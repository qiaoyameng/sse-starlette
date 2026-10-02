"""Lightweight in-process broadcaster for fan-out Server-Sent Events.

Multiple pages (SSE connections) often subscribe to the same background
task. This module provides a small fan-out hub whose subscriptions plug
directly into :class:`sse_starlette.EventSourceResponse` as its ``content``.

Design notes
------------

* **Slow subscribers never block the source.** ``publish()`` is a plain
  synchronous method that never awaits: each subscription owns a bounded
  buffer, so a page on a bad network only fills its own buffer.

* **Backlog overflow is terminal and visible.** When a subscription's
  buffer is full, the subscription is marked *lagged*. It still delivers
  the events it already buffered (in publish order) and then raises
  :class:`LaggedError` — an identifiable "you fell behind" result carrying
  the sequence numbers needed to resync — instead of silently dropping
  intermediate events and pretending the stream is continuous.

* **Consistent ordering.** Every published event gets a monotonically
  increasing sequence number and is offered to every subscriber in the
  same order, so all healthy subscribers observe the identical event
  order.

* **Isolated teardown.** Cancellation, send failure (``aclose()`` — which
  ``EventSourceResponse`` calls on its body iterator after a send
  timeout) and explicit ``unsubscribe()`` release only that
  subscription's resources; peers are unaffected.

* **Explicit source ownership.** ``when_empty`` is a required
  creation-time choice: ``"close"`` means the broadcaster owns the event
  source — when the last subscriber leaves, the broadcaster closes
  permanently and fires ``on_close`` (e.g. to cancel the producer task).
  ``"keep_open"`` means the caller owns the source — the broadcaster
  stays open and later subscribers can still join.

A broadcaster is bound to the event loop its subscriptions are consumed
on; ``publish()`` must be called from that same loop/thread.
"""

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Optional, Set

logger = logging.getLogger(__name__)


class BroadcasterClosedError(RuntimeError):
    """Raised when publishing to or subscribing to a closed broadcaster."""


class LaggedError(Exception):
    """Terminal result of a subscription that fell too far behind.

    Raised from ``Subscription.__anext__`` after the already-buffered
    events have been delivered. Carries everything a consumer needs to
    explain (and repair) the gap:

    * ``first_missed_seq`` — sequence number of the first event that no
      longer fit into the backlog; the delivered stream is continuous up
      to ``first_missed_seq - 1``.
    * ``missed_count`` — how many events were missed in total.
    * ``last_delivered_seq`` — sequence number of the last event this
      subscription actually delivered (``None`` if it delivered none).
    """

    def __init__(
        self,
        *,
        first_missed_seq: int,
        missed_count: int,
        last_delivered_seq: Optional[int] = None,
    ) -> None:
        self.first_missed_seq = first_missed_seq
        self.missed_count = missed_count
        self.last_delivered_seq = last_delivered_seq
        super().__init__(str(self))

    def __str__(self) -> str:
        return (
            f"subscription fell behind: missed {self.missed_count} event(s) "
            f"starting at sequence {self.first_missed_seq} "
            f"(last delivered sequence: {self.last_delivered_seq})"
        )


@dataclass(frozen=True)
class _Published:
    """One published event and its broadcaster-wide sequence number."""

    seq: int
    item: Any


class Subscription:
    """One subscriber's view of an :class:`EventBroadcaster`.

    Implements the async iterator protocol plus ``aclose()``, so it can be
    passed directly as ``content`` to
    :class:`sse_starlette.EventSourceResponse`.

    A subscription is single-pass: cancellation of a pending
    ``__anext__``, ``aclose()``, ``unsubscribe()``, a raised
    :class:`LaggedError` and ``StopAsyncIteration`` all release it
    permanently.
    """

    def __init__(self, broadcaster: "EventBroadcaster", max_backlog: int) -> None:
        self._broadcaster = broadcaster
        self._max_backlog = max_backlog
        self._buffer: Deque[_Published] = deque()
        self._wakeup = asyncio.Event()
        self._lagged: Optional[LaggedError] = None
        self._source_closed = False
        self._released = False
        self._last_delivered_seq: Optional[int] = None

    @property
    def last_delivered_seq(self) -> Optional[int]:
        """Sequence number of the most recently delivered event, if any."""
        return self._last_delivered_seq

    @property
    def released(self) -> bool:
        """Whether this subscription has released its broadcaster resources."""
        return self._released

    def __aiter__(self) -> "Subscription":
        return self

    async def __anext__(self) -> Any:
        try:
            while True:
                # Buffered events are delivered first, in publish order,
                # even if the subscription is already lagged or the source
                # has closed.
                if self._buffer:
                    published = self._buffer.popleft()
                    self._last_delivered_seq = published.seq
                    return published.item
                if self._lagged is not None:
                    lagged = self._lagged
                    lagged.last_delivered_seq = self._last_delivered_seq
                    raise lagged
                if self._source_closed or self._released:
                    raise StopAsyncIteration
                await self._wakeup.wait()
                # State is re-checked at the top of the loop, so a publish
                # racing with this clear() is still visible via the buffer
                # or the flags before we wait again.
                self._wakeup.clear()
        except BaseException:
            # BaseException on purpose: asyncio.CancelledError is how
            # EventSourceResponse tears this stream down on client
            # disconnect. Every terminal path releases only this
            # subscription's resources.
            self.unsubscribe()
            raise

    async def aclose(self) -> None:
        """Release this subscription (async, for async-generator parity).

        EventSourceResponse calls ``aclose()`` on its body iterator after
        a send timeout, so a failed send releases exactly this
        subscription.
        """
        self.unsubscribe()

    def unsubscribe(self) -> None:
        """Detach from the broadcaster. Idempotent; peers are unaffected."""
        if self._released:
            return
        self._released = True
        self._buffer.clear()
        self._broadcaster._release(self)
        # Wake a pending __anext__ so it terminates with StopAsyncIteration.
        self._wakeup.set()

    def _offer(self, published: _Published) -> None:
        """Buffer one published event, or mark the subscription lagged."""
        if self._released:
            return
        if self._lagged is not None:
            # Already terminal: keep counting so the raised LaggedError can
            # report the full gap, but buffer nothing further.
            self._lagged.missed_count += 1
            return
        if len(self._buffer) >= self._max_backlog:
            self._lagged = LaggedError(first_missed_seq=published.seq, missed_count=1)
        else:
            self._buffer.append(published)
        self._wakeup.set()

    def _notify_source_closed(self) -> None:
        """Signal that the broadcaster closed; buffered events still drain."""
        self._source_closed = True
        self._wakeup.set()


class EventBroadcaster:
    """Fan-out hub publishing one event stream to multiple subscriptions.

    Args:
        when_empty: Required, explicit ownership/stop rule for the event
            source. ``"close"``: the broadcaster owns the source — when
            the last subscriber leaves, the broadcaster closes permanently
            and fires ``on_close``. ``"keep_open"``: the caller owns the
            source — the broadcaster stays open and new subscribers can
            still join.
        on_close: Optional zero-argument callable fired exactly once when
            the broadcaster closes (e.g. ``producer_task.cancel``).
    """

    KEEP_OPEN = "keep_open"
    CLOSE = "close"

    def __init__(
        self,
        *,
        when_empty: str,
        on_close: Optional[Callable[[], None]] = None,
    ) -> None:
        if when_empty not in (self.KEEP_OPEN, self.CLOSE):
            raise ValueError(
                f"when_empty must be {self.KEEP_OPEN!r} or {self.CLOSE!r}, "
                f"got: {when_empty!r}"
            )
        self._when_empty = when_empty
        self._on_close = on_close
        self._subscribers: Set[Subscription] = set()
        self._next_seq = 0
        self._closed = False

    @property
    def closed(self) -> bool:
        """Whether the broadcaster has been closed permanently."""
        return self._closed

    @property
    def subscriber_count(self) -> int:
        """Number of currently attached subscriptions."""
        return len(self._subscribers)

    def subscribe(self, *, max_backlog: int) -> Subscription:
        """Attach a new subscriber with its own backlog limit.

        ``max_backlog`` is the maximum number of undelivered events
        buffered for this subscriber; exceeding it ends the subscription
        with :class:`LaggedError`.
        """
        if self._closed:
            raise BroadcasterClosedError("cannot subscribe to a closed broadcaster")
        if max_backlog < 1:
            raise ValueError(f"max_backlog must be >= 1, got: {max_backlog}")
        subscription = Subscription(self, max_backlog)
        self._subscribers.add(subscription)
        return subscription

    def publish(self, item: Any) -> int:
        """Publish one event to all current subscribers. Never blocks.

        Returns the event's sequence number. Events published while there
        are no subscribers are not retained.
        """
        if self._closed:
            raise BroadcasterClosedError("cannot publish to a closed broadcaster")
        seq = self._next_seq
        self._next_seq += 1
        published = _Published(seq=seq, item=item)
        for subscription in list(self._subscribers):
            subscription._offer(published)
        return seq

    def close(self) -> None:
        """Close permanently. Idempotent.

        Active subscriptions drain their buffered events and then end
        normally (``StopAsyncIteration``); ``on_close`` fires exactly
        once.
        """
        if self._closed:
            return
        self._closed = True
        for subscription in list(self._subscribers):
            subscription._notify_source_closed()
        if self._on_close is not None:
            self._on_close()

    def _release(self, subscription: Subscription) -> None:
        """Detach one subscription; apply the ``when_empty`` stop rule."""
        self._subscribers.discard(subscription)
        if not self._subscribers and self._when_empty == self.CLOSE:
            self.close()
