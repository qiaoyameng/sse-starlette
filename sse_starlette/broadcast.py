from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
import enum
import json
from typing import Any, AsyncIterator, Optional, Union

import anyio

from sse_starlette.event import ServerSentEvent

Content = Union[str, bytes, dict, ServerSentEvent, Any]
_UNSET = object()


class LastSubscriberPolicy(enum.Enum):
    """Source behaviour after the final subscriber unsubscribes."""

    STOP_SOURCE = "stop_source"
    KEEP_SOURCE = "keep_source"

class BroadcasterClosedError(RuntimeError):
    """Raised when subscribing or publishing to a closed broadcaster."""


class SubscriptionLagged(ServerSentEvent):
    """Terminal SSE event explaining that a subscriber fell behind."""

    def __init__(
        self,
        *,
        last_sequence: Optional[int],
        first_missing_sequence: int,
        latest_sequence: int,
        backlog: int,
        event: str = "subscription_lagged",
    ) -> None:
        payload = {
            "reason": "lagged",
            "last_sequence": last_sequence,
            "first_missing_sequence": first_missing_sequence,
            "latest_sequence": latest_sequence,
            "backlog": backlog,
        }
        super().__init__(
            data=json.dumps(payload, separators=(",", ":")),
            event=event,
            id=str(first_missing_sequence),
        )
        self.last_sequence = last_sequence
        self.first_missing_sequence = first_missing_sequence
        self.latest_sequence = latest_sequence
        self.backlog = backlog

    def as_dict(self) -> dict[str, object]:
        return {
            "reason": "lagged",
            "last_sequence": self.last_sequence,
            "first_missing_sequence": self.first_missing_sequence,
            "latest_sequence": self.latest_sequence,
            "backlog": self.backlog,
        }


@dataclass(frozen=True)
class _Envelope:
    sequence: int
    event: Content


@dataclass
class _Done:
    error: Optional[BaseException] = None


@dataclass(eq=False)
class _Subscriber:
    next_delivery: int
    last_delivered: Optional[int]
    queue: "asyncio.Queue[Union[_Envelope, SubscriptionLagged, _Done]]"
    backlog: int
    terminal: bool = False


class EventBroadcaster:
    """Broadcast in-process events to independent bounded subscribers."""

    def __init__(
        self,
        source: Optional[AsyncIterator[Content]] = None,
        *,
        on_last_subscriber: Any = _UNSET,
    ) -> None:
        if source is not None and on_last_subscriber is _UNSET:
            raise TypeError(
                "on_last_subscriber is required when a source is supplied"
            )
        if on_last_subscriber is _UNSET:
            on_last_subscriber = LastSubscriberPolicy.STOP_SOURCE
        if not isinstance(on_last_subscriber, LastSubscriberPolicy):
            raise TypeError("on_last_subscriber must be a LastSubscriberPolicy")
        self._source = source
        self._on_last_subscriber = on_last_subscriber
        self._subscribers: set[_Subscriber] = set()
        self._lock = asyncio.Lock()
        self._last_sequence: Optional[int] = None
        self._closed = False
        self._source_task: Optional[asyncio.Task[None]] = None
        self._source_closed = False

    @classmethod
    def from_async_iterable(
        cls,
        source: Any,
        *,
        on_last_subscriber: LastSubscriberPolicy,
    ) -> "EventBroadcaster":
        if not hasattr(source, "__aiter__"):
            raise TypeError("source must be an async iterable")
        return cls(source.__aiter__(), on_last_subscriber=on_last_subscriber)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    @property
    def closed(self) -> bool:
        return self._closed

    def subscribe(self, *, backlog: int = 100) -> BroadcastSubscription:
        if isinstance(backlog, bool) or not isinstance(backlog, int):
            raise TypeError("backlog must be an integer")
        if backlog < 0:
            raise ValueError("backlog must be >= 0")
        if self._closed:
            raise BroadcasterClosedError("broadcaster is closed")
        if self._source_task is not None and self._source_task.done():
            raise BroadcasterClosedError("broadcaster source has finished")

        next_delivery = 0 if self._last_sequence is None else self._last_sequence + 1
        subscriber = _Subscriber(
            next_delivery=next_delivery,
            last_delivered=None,
            queue=asyncio.Queue(maxsize=backlog + 1),
            backlog=backlog,
        )
        self._subscribers.add(subscriber)

        if self._source is not None and self._source_task is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError as exc:
                self._subscribers.remove(subscriber)
                raise RuntimeError(
                    "subscribing to a source requires a running loop"
                ) from exc
            self._source_task = loop.create_task(self._pump_source())

        return BroadcastSubscription(self, subscriber)

    async def publish(self, event: Content) -> int:
        async with self._lock:
            return self._publish_locked(event)

    def publish_nowait(self, event: Content) -> int:
        if self._source is not None:
            raise RuntimeError("sourced broadcasters receive events from their source")
        if self._lock.locked():
            raise RuntimeError("a broadcast is already in progress")
        return self._publish_locked(event)

    def _publish_locked(self, event: Content) -> int:
        if self._closed:
            raise BroadcasterClosedError("broadcaster is closed")
        if self._source is not None:
            raise RuntimeError("sourced broadcasters receive events from their source")

        sequence = 0 if self._last_sequence is None else self._last_sequence + 1
        self._last_sequence = sequence
        self._dispatch_locked(sequence, event)
        return sequence

    async def _pump_source(self) -> None:
        assert self._source is not None
        try:
            async for event in self._source:
                should_stop = False
                async with self._lock:
                    if self._closed:
                        return
                    sequence = (
                        0 if self._last_sequence is None else self._last_sequence + 1
                    )
                    self._last_sequence = sequence
                    self._dispatch_locked(sequence, event)
                    should_stop = self._should_stop_source_locked()
                    if should_stop:
                        self._closed = True

                if should_stop:
                    await self._stop_owned_source()
                    return

            async with self._lock:
                self._finish_locked(None)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            async with self._lock:
                self._finish_locked(exc)

    def _dispatch_locked(self, sequence: int, event: Content) -> None:
        for subscriber in list(self._subscribers):
            if subscriber.terminal:
                continue

            queued = subscriber.queue.qsize()
            if queued >= subscriber.backlog:
                lag = self._build_lag(subscriber, sequence, queued)
                subscriber.terminal = True
                self._subscribers.discard(subscriber)
                subscriber.queue.put_nowait(lag)
            else:
                subscriber.queue.put_nowait(_Envelope(sequence, event))

    @staticmethod
    def _build_lag(
        subscriber: _Subscriber, sequence: int, queued: int
    ) -> SubscriptionLagged:
        if subscriber.last_delivered is None and queued == 0:
            last_sequence = None
        elif subscriber.last_delivered is None:
            last_sequence = subscriber.next_delivery + queued - 1
        else:
            last_sequence = subscriber.last_delivered + queued

        return SubscriptionLagged(
            last_sequence=last_sequence,
            first_missing_sequence=subscriber.next_delivery + queued,
            latest_sequence=sequence,
            backlog=subscriber.backlog,
        )

    def _finish_locked(self, error: Optional[BaseException]) -> None:
        self._closed = True
        done = _Done(error)
        for subscriber in list(self._subscribers):
            if not subscriber.terminal:
                subscriber.terminal = True
                subscriber.queue.put_nowait(done)
        self._subscribers.clear()

    def _should_stop_source_locked(self) -> bool:
        return (
            self._source is not None
            and not self._subscribers
            and not self._closed
            and self._on_last_subscriber is LastSubscriberPolicy.STOP_SOURCE
        )

    async def aclose(self, *, close_source: Optional[bool] = None) -> None:
        async with self._lock:
            self._finish_locked(None)
            task = self._source_task
            should_close_source = (
                self._on_last_subscriber is LastSubscriberPolicy.STOP_SOURCE
                if close_source is None
                else close_source
            )

        if task is not None and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        if self._source is not None and should_close_source and not self._source_closed:
            self._source_closed = True
            aclose = getattr(self._source, "aclose", None)
            if aclose is not None:
                await aclose()

    async def _remove_subscription(self, subscriber: _Subscriber) -> bool:
        with anyio.CancelScope(shield=True):
            async with self._lock:
                if subscriber not in self._subscribers:
                    return False
                subscriber.terminal = True
                self._subscribers.remove(subscriber)
                should_stop = self._should_stop_source_locked()
                if should_stop:
                    self._closed = True

        if should_stop:
            await self._stop_owned_source()
        return True

    async def _stop_owned_source(self) -> None:
        task = self._source_task
        if task is not None and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        if self._source is not None and not self._source_closed:
            self._source_closed = True
            aclose = getattr(self._source, "aclose", None)
            if aclose is not None:
                await aclose()

    async def __aenter__(self) -> "EventBroadcaster":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()


class BroadcastSubscription:
    """Bounded async iterator returned by :meth:`EventBroadcaster.subscribe`."""

    _sse_close_on_stream_error = True

    def __init__(
        self, broadcaster: EventBroadcaster, subscriber: _Subscriber
    ) -> None:
        self._broadcaster = broadcaster
        self._subscriber = subscriber
        self._closed = False

    @property
    def next_sequence(self) -> int:
        return self._subscriber.next_delivery

    def __aiter__(self) -> "BroadcastSubscription":
        return self

    async def __anext__(self) -> Union[Content, SubscriptionLagged]:
        return await self.receive()

    async def receive(self) -> Union[Content, SubscriptionLagged]:
        if self._closed:
            raise StopAsyncIteration
        try:
            envelope = await self._subscriber.queue.get()
        except BaseException:
            await self.aclose()
            raise
        return self._handle(envelope)

    def receive_nowait(self) -> Union[Content, SubscriptionLagged]:
        if self._closed:
            raise StopAsyncIteration
        envelope = self._subscriber.queue.get_nowait()
        return self._handle(envelope)

    async def get(self) -> Union[Content, SubscriptionLagged]:
        return await self.receive()

    def get_nowait(self) -> Union[Content, SubscriptionLagged]:
        return self.receive_nowait()

    def _handle(
        self, envelope: Union[_Envelope, SubscriptionLagged, _Done]
    ) -> Union[Content, SubscriptionLagged]:
        if isinstance(envelope, SubscriptionLagged):
            self._closed = True
            return envelope
        if isinstance(envelope, _Done):
            self._closed = True
            if envelope.error is not None:
                raise envelope.error
            raise StopAsyncIteration

        self._subscriber.last_delivered = envelope.sequence
        self._subscriber.next_delivery = envelope.sequence + 1
        return envelope.event

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        with anyio.CancelScope(shield=True):
            await self._broadcaster._remove_subscription(self._subscriber)


__all__ = [
    "BroadcastSubscription",
    "BroadcasterClosedError",
    "EventBroadcaster",
    "LastSubscriberPolicy",
    "SubscriptionLagged",
]
