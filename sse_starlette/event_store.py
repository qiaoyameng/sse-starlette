"""In-process bounded event store with Last-Event-ID resume support.

This module adds two building blocks on top of ``EventSourceResponse``:

``EventStore``
    A process-local, bounded buffer of published events. Every published
    event gets an id assigned by the store; ids are unique within the store
    and strictly increasing, so "events after id X" is well defined. The
    retention window is bounded by both event count and encoded bytes.

``EventStoreResponse``
    A response adapter that validates the client's ``Last-Event-ID`` cursor
    *before* deciding the HTTP status, then streams missed history followed
    by live events. Events published during the replay-to-live handoff are
    neither lost nor delivered twice.

Cursor validation outcomes (``CursorStatus``):

- ``EMPTY``: no cursor supplied -> 200, live events only.
- ``REPLAY``: cursor is inside the retained window -> 200, replay then live.
- ``UP_TO_DATE``: cursor is valid and no newer events exist -> 200, live only.
- ``TOO_OLD``: cursor was evicted from the window -> 410 Gone. The client
  must re-sync its full state; we never silently restart from the latest
  event after a 200.
- ``UNKNOWN``: cursor was never issued by this store -> 400 Bad Request.

Concurrency notes: the store is in-process and single-event-loop. The
replay-to-live handoff is race-free because a subscriber queue is registered
*before* the replay snapshot is taken, and live events whose sequence was
already replayed are skipped by sequence comparison.
"""

import enum
import logging
from dataclasses import dataclass
from typing import Any, AsyncIterator, List, Optional, Set

import anyio
from starlette.responses import PlainTextResponse, Response
from starlette.types import Receive, Scope, Send

from sse_starlette.event import ServerSentEvent
from sse_starlette.sse import EventSourceResponse

logger = logging.getLogger(__name__)


class CursorStatus(enum.Enum):
    """Outcome of validating a ``Last-Event-ID`` cursor against the store."""

    EMPTY = "empty"  # no cursor supplied: stream live events only
    REPLAY = "replay"  # cursor valid, older events retained: replay then live
    UP_TO_DATE = "up_to_date"  # cursor valid, no newer events: live only
    TOO_OLD = "too_old"  # cursor evicted from the retention window
    UNKNOWN = "unknown"  # cursor never issued by this store


@dataclass(frozen=True)
class CursorValidation:
    """Result of ``EventStore.validate_cursor``."""

    status: CursorStatus
    cursor: Optional[str]


class InvalidCursorError(RuntimeError):
    """Base error raised when streaming from an invalid cursor."""


class CursorTooOldError(InvalidCursorError):
    """The cursor refers to an event already evicted from the window."""


class UnknownCursorError(InvalidCursorError):
    """The cursor was never issued by this store."""


@dataclass(frozen=True)
class _StoredEvent:
    seq: int
    event: ServerSentEvent
    size: int  # length of the encoded event in bytes


class _Subscription:
    """Bounded per-subscriber queue feeding the live phase of a stream."""

    def __init__(self, max_buffer: int) -> None:
        self._send, self._receive = anyio.create_memory_object_stream(max_buffer)

    def deliver(self, entry: _StoredEvent) -> bool:
        """Queue an event; return False if the subscriber fell too far behind.

        On overflow the send side is closed, so the receiver drains what is
        buffered and then gets ``EndOfStream`` -- the stream ends visibly and
        the client reconnects with its last event id to resync.
        """
        try:
            self._send.send_nowait(entry)
            return True
        except anyio.WouldBlock:
            self._send.close()
            return False

    async def receive(self) -> _StoredEvent:
        return await self._receive.receive()

    def close(self) -> None:
        self._send.close()


class EventStore:
    """Bounded in-process store of SSE events with resumable ids.

    Args:
        max_events: Maximum number of retained events.
        max_bytes: Maximum total size of retained events, measured on the
            encoded (wire-format) bytes of each event.
        sep: Line separator used when encoding events (must match the
            separator used by the streaming response for byte accounting
            to be exact).
        subscriber_buffer: Per-subscriber live queue size. A subscriber that
            falls more than this many events behind is evicted and its stream
            ends, so a slow consumer cannot grow memory without bound.
    """

    def __init__(
        self,
        max_events: int = 1024,
        max_bytes: int = 1 << 20,
        *,
        sep: str = "\r\n",
        subscriber_buffer: int = 256,
    ) -> None:
        if max_events <= 0:
            raise ValueError("max_events must be greater than zero")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be greater than zero")
        if subscriber_buffer <= 0:
            raise ValueError("subscriber_buffer must be greater than zero")
        self._max_events = max_events
        self._max_bytes = max_bytes
        self._sep = sep
        self._subscriber_buffer = subscriber_buffer
        self._events: List[_StoredEvent] = []
        self._bytes = 0
        self._seq = 0
        self._subscribers: Set[_Subscription] = set()

    @property
    def latest_id(self) -> Optional[str]:
        """Id of the most recently published event, or None if store is empty."""
        return str(self._seq) if self._seq else None

    @property
    def oldest_id(self) -> Optional[str]:
        """Oldest retained id, or None if the window is empty."""
        return str(self._events[0].seq) if self._events else None

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def __len__(self) -> int:
        return len(self._events)

    def snapshot(self) -> List[ServerSentEvent]:
        """Currently retained events, oldest first."""
        return [entry.event for entry in self._events]

    def publish(
        self,
        data: Optional[Any] = None,
        *,
        event: Optional[str] = None,
        comment: Optional[str] = None,
        retry: Optional[int] = None,
    ) -> ServerSentEvent:
        """Publish an event: assign the next id, retain it, notify subscribers.

        The id is assigned by the store and is unique and strictly increasing,
        so event order is fully determined by id. Returns the stored event.
        """
        self._seq += 1
        sse = ServerSentEvent(
            data=data,
            event=event,
            id=str(self._seq),
            retry=retry,
            comment=comment,
            sep=self._sep,
        )
        entry = _StoredEvent(seq=self._seq, event=sse, size=len(sse.encode()))
        self._events.append(entry)
        self._bytes += entry.size
        self._evict()
        for subscription in list(self._subscribers):
            if not subscription.deliver(entry):
                logger.debug("evicting slow subscriber (queue full)")
                self._subscribers.discard(subscription)
        return sse

    def _evict(self) -> None:
        # Always keep the newest event, even if it alone exceeds max_bytes.
        while len(self._events) > 1 and (
            len(self._events) > self._max_events or self._bytes > self._max_bytes
        ):
            self._bytes -= self._events.pop(0).size

    def validate_cursor(self, cursor: Optional[str]) -> CursorValidation:
        """Classify a ``Last-Event-ID`` cursor against the retained window."""
        if cursor is None:
            return CursorValidation(CursorStatus.EMPTY, None)
        cursor = str(cursor).strip()
        if not cursor:
            return CursorValidation(CursorStatus.EMPTY, None)
        if not cursor.isdigit() or not self._events:
            return CursorValidation(CursorStatus.UNKNOWN, cursor)
        seq = int(cursor)
        oldest = self._events[0].seq
        newest = self._events[-1].seq
        if seq < oldest:
            return CursorValidation(CursorStatus.TOO_OLD, cursor)
        if seq > newest:
            return CursorValidation(CursorStatus.UNKNOWN, cursor)
        # Ids are contiguous and eviction only removes a prefix, so any seq
        # within [oldest, newest] is retained.
        if seq == newest:
            return CursorValidation(CursorStatus.UP_TO_DATE, cursor)
        return CursorValidation(CursorStatus.REPLAY, cursor)

    def _subscribe(self) -> _Subscription:
        subscription = _Subscription(self._subscriber_buffer)
        self._subscribers.add(subscription)
        return subscription

    async def stream(
        self, cursor: Optional[str] = None
    ) -> AsyncIterator[ServerSentEvent]:
        """Stream events after ``cursor``: retained history, then live events.

        The subscriber queue is registered before the replay snapshot is
        taken, so events published during the handoff land in the queue and
        are delivered exactly once (sequence numbers guard against
        duplicates). Raises ``CursorTooOldError``/``UnknownCursorError`` if
        the cursor is invalid -- callers streaming over HTTP should validate
        first via ``validate_cursor`` (see ``EventStoreResponse``) so the
        failure becomes a status code instead of a broken 200 stream.
        """
        subscription = self._subscribe()
        try:
            validation = self.validate_cursor(cursor)
            if validation.status is CursorStatus.TOO_OLD:
                raise CursorTooOldError(
                    f"cursor {validation.cursor!r} is older than the retained "
                    f"window (oldest retained id: {self.oldest_id})"
                )
            if validation.status is CursorStatus.UNKNOWN:
                raise UnknownCursorError(
                    f"cursor {validation.cursor!r} was never issued by this store"
                )
            last_seq = 0
            if validation.status in (CursorStatus.REPLAY, CursorStatus.UP_TO_DATE):
                cursor_seq = int(validation.cursor)  # type: ignore[arg-type]
                last_seq = cursor_seq
                for entry in list(self._events):
                    if entry.seq > cursor_seq:
                        last_seq = entry.seq
                        yield entry.event
            while True:
                try:
                    entry = await subscription.receive()
                except anyio.EndOfStream:
                    # Evicted as a slow subscriber: end visibly so the client
                    # reconnects with its last event id and resyncs.
                    return
                if entry.seq <= last_seq:
                    continue
                last_seq = entry.seq
                yield entry.event
        finally:
            subscription.close()
            self._subscribers.discard(subscription)


class EventStoreResponse(Response):
    """Response adapter streaming events from an ``EventStore`` after a cursor.

    Validates the ``Last-Event-ID`` cursor up front:

    - valid cursor (or none) -> delegates to ``EventSourceResponse`` (200),
      replaying missed history before switching to live events;
    - evicted cursor -> 410 Gone (never a silent 200 restarted at latest);
    - unrecognized cursor -> 400 Bad Request.

    Extra keyword arguments are forwarded to ``EventSourceResponse``
    (e.g. ``ping``, ``send_timeout``). Heartbeat pings are generated by the
    response and never enter the store, so they cannot move the cursor.
    """

    TOO_OLD_STATUS_CODE = 410
    UNKNOWN_STATUS_CODE = 400

    def __init__(
        self,
        store: EventStore,
        last_event_id: Optional[str] = None,
        **stream_kwargs: Any,
    ) -> None:
        self.store = store
        self.validation = store.validate_cursor(last_event_id)
        self._stream_kwargs = stream_kwargs
        super().__init__(content=None)

    @classmethod
    def from_request(
        cls, store: EventStore, request: Any, **kwargs: Any
    ) -> "EventStoreResponse":
        """Build from a Starlette/FastAPI request's ``Last-Event-ID`` header."""
        return cls(store, request.headers.get("last-event-id"), **kwargs)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        delegate: Response
        if self.validation.status is CursorStatus.TOO_OLD:
            delegate = PlainTextResponse(
                f"Last-Event-ID {self.validation.cursor!r} is too old: the "
                "event has been evicted from the store window. Re-sync "
                "without a cursor.",
                status_code=self.TOO_OLD_STATUS_CODE,
            )
        elif self.validation.status is CursorStatus.UNKNOWN:
            delegate = PlainTextResponse(
                f"Last-Event-ID {self.validation.cursor!r} is not recognized "
                "by this event store.",
                status_code=self.UNKNOWN_STATUS_CODE,
            )
        else:
            delegate = EventSourceResponse(
                self.store.stream(self.validation.cursor),
                **self._stream_kwargs,
            )
        await delegate(scope, receive, send)
