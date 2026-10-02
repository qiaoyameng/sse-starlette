from __future__ import annotations

import itertools
import threading
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    Deque,
    Mapping,
    Optional,
    Set,
    Union,
)

import anyio

from sse_starlette.event import ServerSentEvent, ensure_bytes


EventContent = Union[ServerSentEvent, Mapping[str, object]]
BeforeLiveHook = Callable[[], Awaitable[None]]


class CursorStatus(str, Enum):
    EMPTY = "empty"
    REPLAY = "replay"
    CURRENT = "current"
    STALE = "stale"
    UNKNOWN = "unknown"


ResumeStatus = CursorStatus


class StaleCursorError(ValueError):
    """The supplied cursor refers to an event which has left the window."""


class UnknownCursorError(ValueError):
    """The supplied cursor does not refer to any published event."""


@dataclass(frozen=True)
class StoredEvent:
    id: Optional[str]
    sequence: int
    encoded: bytes


class EventSubscription:
    def __init__(
        self,
        store: BoundedEventStore,
        *,
        status: CursorStatus,
        cursor: Optional[str],
        history: list[StoredEvent],
        send_stream: anyio.MemoryObjectSendStream[StoredEvent],
        receive_stream: anyio.MemoryObjectReceiveStream[StoredEvent],
        before_live: Optional[BeforeLiveHook] = None,
    ) -> None:
        self._store = store
        self.status = status
        self.cursor = cursor
        self._history = history
        self._send_stream = send_stream
        self._receive_stream = receive_stream
        self._before_live = before_live

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            for event in self._history:
                yield event.encoded

            if self._before_live is not None:
                await self._before_live()

            async with self._receive_stream:
                async for event in self._receive_stream:
                    yield event.encoded
        finally:
            self._store._unsubscribe(self._send_stream)
            self._send_stream.close()


class BoundedEventStore:
    """An in-process, bounded, append-only SSE event history.

    Event ids are dense positive integers. They are unique for the lifetime of
    the store and determine publication order. A subscriber is registered in
    the same critical section which selects its historical events, which makes
    the history-to-live transition exact once publication is serialized.
    """

    def __init__(
        self,
        *,
        max_events: int,
        max_bytes: int,
        sep: str = ServerSentEvent.DEFAULT_SEPARATOR,
    ) -> None:
        if not isinstance(max_events, int) or isinstance(max_events, bool):
            raise TypeError("max_events must be an int")
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool):
            raise TypeError("max_bytes must be an int")
        if max_events <= 0:
            raise ValueError("max_events must be > 0")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be > 0")
        if sep not in ("\r\n", "\r", "\n"):
            raise ValueError("sep must be one of: \\r\\n, \\r, \\n")

        self.max_events = max_events
        self.max_bytes = max_bytes
        self.sep = sep
        self._events: Deque[StoredEvent] = deque()
        self._subscribers: Set[anyio.MemoryObjectSendStream[StoredEvent]] = set()
        self._sequences = itertools.count(1)
        self._next_id = 1
        self._latest_id: Optional[int] = None
        self._evicted_up_to = 0
        self._total_bytes = 0
        self._state_lock = threading.Lock()
        self._publish_lock = anyio.Lock()

    @property
    def latest_event_id(self) -> Optional[str]:
        if self._latest_id is None:
            return None
        return str(self._latest_id)

    @property
    def oldest_event_id(self) -> Optional[str]:
        if not self._events:
            return None
        return self._events[0].id

    def __len__(self) -> int:
        return len(self._events)

    async def publish(self, event: EventContent) -> Optional[StoredEvent]:
        async with self._publish_lock:
            with self._state_lock:
                sse, is_heartbeat = self._prepare_event(event)

                if is_heartbeat:
                    encoded = ensure_bytes(sse, self.sep)
                    subscribers = list(self._subscribers)
                else:
                    event_id = self._next_id
                    self._next_id += 1
                    sse.id = str(event_id)
                    encoded = ensure_bytes(sse, self.sep)
                    sequence = next(self._sequences)
                    stored = StoredEvent(
                        id=str(event_id), sequence=sequence, encoded=encoded
                    )

                    self._latest_id = event_id
                    self._events.append(stored)
                    self._total_bytes += len(encoded)
                    self._evict_locked()
                    subscribers = list(self._subscribers)

            self._send_to_subscribers(
                subscribers,
                StoredEvent(
                    id=None,
                    sequence=0,
                    encoded=encoded,
                )
                if is_heartbeat
                else stored,
            )

            if is_heartbeat:
                return None
            return stored

    def _send_to_subscribers(
        self,
        subscribers: list[anyio.MemoryObjectSendStream[StoredEvent]],
        event: StoredEvent,
    ) -> None:
        for subscriber in subscribers:
            try:
                subscriber.send_nowait(event)
            except anyio.WouldBlock:
                self._unsubscribe(subscriber)
                subscriber.close()
            except (anyio.BrokenResourceError, anyio.ClosedResourceError):
                self._unsubscribe(subscriber)

    def subscribe(
        self,
        last_event_id: Optional[object] = None,
        *,
        before_live: Optional[BeforeLiveHook] = None,
    ) -> EventSubscription:
        cursor_value, cursor = self._parse_cursor(last_event_id)

        with self._state_lock:
            if cursor_value is None:
                status = CursorStatus.EMPTY
                start_index = 0
            else:
                if self._latest_id is None or cursor_value > self._latest_id:
                    raise UnknownCursorError(str(cursor_value))
                if cursor_value <= self._evicted_up_to:
                    raise StaleCursorError(str(cursor_value))

                try:
                    start_index = next(
                        index
                        for index, event in enumerate(self._events)
                        if int(event.id) == cursor_value
                    ) + 1
                except StopIteration:
                    raise UnknownCursorError(str(cursor_value)) from None

                status = (
                    CursorStatus.CURRENT
                    if start_index == len(self._events)
                    else CursorStatus.REPLAY
                )

            history = list(
                itertools.islice(self._events, start_index, len(self._events))
            )
            send_stream, receive_stream = anyio.create_memory_object_stream[
                StoredEvent
            ](self.max_events)
            self._subscribers.add(send_stream)

        return EventSubscription(
            self,
            status=status,
            cursor=cursor,
            history=history,
            send_stream=send_stream,
            receive_stream=receive_stream,
            before_live=before_live,
        )

    def _evict_locked(self) -> None:
        while (
            len(self._events) > self.max_events
            or self._total_bytes > self.max_bytes
        ):
            evicted = self._events.popleft()
            self._total_bytes -= len(evicted.encoded)
            self._evicted_up_to = int(evicted.id)
            if self._total_bytes < 0:
                self._total_bytes = 0

    def _prepare_event(
        self, event: EventContent
    ) -> tuple[ServerSentEvent, bool]:
        if isinstance(event, ServerSentEvent):
            sse = ServerSentEvent(
                data=event.data,
                event=event.event,
                id=event.id,
                retry=event.retry,
                comment=event.comment,
                sep=self.sep,
            )
        elif isinstance(event, Mapping):
            if "sep" in event:
                raise ValueError("event sep is controlled by the event store")
            try:
                sse = ServerSentEvent(sep=self.sep, **dict(event))
            except TypeError as exc:
                raise ValueError("invalid SSE event fields") from exc
        else:
            raise TypeError("event must be a ServerSentEvent or a mapping")

        is_heartbeat = (
            sse.comment is not None
            and sse.data is None
            and sse.event is None
            and sse.retry is None
        )
        if is_heartbeat:
            if sse.id is not None:
                raise ValueError("heartbeat comments cannot have an event id")
            return sse, True

        sse.id = None
        return sse, False

    @staticmethod
    def _parse_cursor(value: Optional[object]) -> tuple[Optional[int], Optional[str]]:
        if value is None:
            return None, None
        if isinstance(value, bool):
            raise UnknownCursorError(str(value))

        text = str(value).strip()
        if not text:
            return None, None

        try:
            cursor = int(text)
        except ValueError as exc:
            raise UnknownCursorError(text) from exc
        if isinstance(value, str) and str(cursor) != text:
            raise UnknownCursorError(text)
        if cursor <= 0:
            raise UnknownCursorError(text)
        return cursor, str(cursor)

    def _unsubscribe(
        self, send_stream: anyio.MemoryObjectSendStream[StoredEvent]
    ) -> None:
        with self._state_lock:
            self._subscribers.discard(send_stream)


InMemoryEventStore = BoundedEventStore
EventStore = BoundedEventStore
