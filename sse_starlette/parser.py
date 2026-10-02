"""Incremental parser for Server-Sent Events byte streams."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Optional

from sse_starlette.event import ServerSentEvent


_BOM = b"\xef\xbb\xbf"
_DIGITS = frozenset("0123456789")


class ServerSentEventParseError(ValueError):
    """Raised when an SSE byte stream cannot be decoded or exceeds a limit."""

    def __init__(
        self,
        message: str,
        *,
        byte_offset: int,
        line_number: int,
        column: int,
        line_bytes: bytes = b"",
        state: Optional[dict[str, Any]] = None,
        reason: str = "parse_error",
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.byte_offset = byte_offset
        self.offset = byte_offset
        self.line_number = line_number
        self.line = line_number
        self.column = column
        self.line_bytes = bytes(line_bytes)
        self.state = dict(state or {})


@dataclass(frozen=True)
class EventStreamParserState:
    """Public snapshot of an :class:`EventStreamParser`'s control state."""

    last_event_id: str
    retry: Optional[int]
    events_dispatched: int
    bytes_processed: int
    line_number: int
    pending_line_bytes: bytes
    pending_event_bytes: int
    pending_event_type: str
    pending_data: str
    at_stream_start: bool
    cr_pending: bool
    finished: bool


def parse_server_sent_events(
    chunks: Iterable[bytes],
    *,
    max_line_bytes: Optional[int] = None,
    max_event_bytes: Optional[int] = None,
) -> Iterator[ServerSentEvent]:
    """Yield structured SSE events from an iterable of bytes chunks."""
    parser = EventStreamParser(
        max_line_bytes=max_line_bytes,
        max_event_bytes=max_event_bytes,
    )
    for chunk in chunks:
        yield from parser.feed(chunk)
    yield from parser.finish()


class EventStreamParser:
    """Parse SSE bytes delivered in arbitrary-sized chunks.

    Completed events are returned from :meth:`feed` as soon as their blank
    terminating line is received. ``last_event_id`` and ``retry`` are kept as
    parser control state. Limits are measured in UTF-8 bytes after removing a
    leading stream BOM and do not include line terminators.
    """

    def __init__(
        self,
        *,
        max_line_bytes: Optional[int] = None,
        max_event_bytes: Optional[int] = None,
    ) -> None:
        if max_line_bytes is not None and max_line_bytes < 0:
            raise ValueError("max_line_bytes must be non-negative")
        if max_event_bytes is not None and max_event_bytes < 0:
            raise ValueError("max_event_bytes must be non-negative")
        self.max_line_bytes = max_line_bytes
        self.max_event_bytes = max_event_bytes

        self._last_event_id = ""
        self._retry: Optional[int] = None
        self._events_dispatched = 0
        self._bytes_processed = 0
        self._line_number = 1
        self._line_start = 0

        self._line = bytearray()
        self._cr_pending = False
        self._at_stream_start = True
        self._finished = False

        self._event_type = ""
        self._data_parts: list[str] = []
        self._event_bytes = 0

    @property
    def last_event_id(self) -> str:
        return self._last_event_id

    @property
    def retry(self) -> Optional[int]:
        return self._retry

    @property
    def events_dispatched(self) -> int:
        return self._events_dispatched

    def state(self) -> EventStreamParserState:
        return self._state_snapshot()

    def feed(self, chunk: bytes) -> list[ServerSentEvent]:
        """Process one bytes chunk and return newly completed events."""
        if self._finished:
            raise RuntimeError("cannot feed a finished EventStreamParser")
        data = bytes(chunk)
        events: list[ServerSentEvent] = []
        pos = 0

        if self._cr_pending and data:
            if data[:1] == b"\n":
                pos = 1
                self._bytes_processed += 1
                self._line_start = self._bytes_processed
                self._cr_pending = False

        while pos < len(data):
            lf = data.find(b"\n", pos)
            cr = data.find(b"\r", pos)

            if cr == -1 and lf == -1:
                self._line.extend(data[pos:])
                self._bytes_processed += len(data) - pos
                break

            if cr != -1 and (lf == -1 or cr < lf):
                self._line.extend(data[pos:cr])
                line = bytes(self._line)
                self._line.clear()
                self._bytes_processed += cr - pos + 1
                events.extend(self._process_line(line))
                self._line_start = self._bytes_processed

                if cr + 1 == len(data):
                    self._cr_pending = True
                    pos = len(data)
                elif data[cr + 1] == 10:
                    pos = cr + 2
                    self._bytes_processed += 1
                    self._line_start = self._bytes_processed
                else:
                    pos = cr + 1
            else:
                assert lf != -1
                self._line.extend(data[pos:lf])
                line = bytes(self._line)
                self._line.clear()
                self._bytes_processed += lf - pos + 1
                events.extend(self._process_line(line))
                self._line_start = self._bytes_processed
                pos = lf + 1

        return events

    def finish(self) -> list[ServerSentEvent]:
        """Finish input and validate a trailing partial line.

        A completed field without the blank line required by SSE remains
        pending and is intentionally not returned as a dispatched event.
        """
        if self._finished:
            return []
        self._cr_pending = False
        pending = bytes(self._line)
        if pending:
            try:
                pending.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise self._parse_error(
                    "incomplete or invalid UTF-8 sequence",
                    offset=self._line_start + exc.start,
                    column=exc.start,
                    line=pending,
                    reason="invalid_utf8",
                ) from exc
        self._finished = True
        return []

    def _process_line(self, raw: bytes) -> list[ServerSentEvent]:
        bom_length = (
            len(_BOM) if self._at_stream_start and raw.startswith(_BOM) else 0
        )
        line_bytes = raw[bom_length:]
        self._at_stream_start = False

        if self.max_line_bytes is not None and len(line_bytes) > self.max_line_bytes:
            raise self._parse_error(
                f"SSE line exceeds {self.max_line_bytes} byte limit",
                offset=self._line_start + bom_length + len(line_bytes),
                column=len(line_bytes),
                line=line_bytes,
                reason="line_too_long",
            )

        try:
            line = line_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise self._parse_error(
                "invalid UTF-8 sequence in SSE line",
                offset=self._line_start + bom_length + exc.start,
                column=exc.start,
                line=line_bytes,
                reason="invalid_utf8",
            ) from exc

        if line == "":
            events = self._dispatch_event()
            self._event_type = ""
            self._data_parts.clear()
            self._event_bytes = 0
            self._line_number += 1
            return events

        if line.startswith(":"):
            self._line_number += 1
            return []

        colon = line.find(":")
        if colon == -1:
            name = line
            value = ""
        else:
            name = line[:colon]
            value = line[colon + 1 :]
            if value.startswith(" "):
                value = value[1:]

        if name in {"data", "event", "id", "retry"}:
            if (
                self.max_event_bytes is not None
                and self._event_bytes + len(line_bytes) > self.max_event_bytes
            ):
                raise self._parse_error(
                    f"SSE event exceeds {self.max_event_bytes} byte limit",
                    offset=self._line_start + bom_length + len(line_bytes),
                    column=len(line_bytes),
                    line=line_bytes,
                    reason="event_too_large",
                )
            self._event_bytes += len(line_bytes)

        if name == "data":
            self._data_parts.append(value + "\n")
        elif name == "event":
            self._event_type = value
        elif name == "id":
            if "\x00" not in value:
                self._last_event_id = value
        elif name == "retry":
            if value and all(char in _DIGITS for char in value):
                self._retry = int(value)

        self._line_number += 1
        return []

    def _dispatch_event(self) -> list[ServerSentEvent]:
        if not self._data_parts:
            return []
        data = "".join(self._data_parts)[:-1]
        event_type = self._event_type or "message"
        event = ServerSentEvent(
            data=data,
            event=event_type,
            id=self._last_event_id,
        )
        self._events_dispatched += 1
        return [event]

    def _parse_error(
        self,
        message: str,
        *,
        offset: int,
        column: int,
        line: bytes,
        reason: str,
    ) -> ServerSentEventParseError:
        return ServerSentEventParseError(
            message,
            byte_offset=offset,
            line_number=self._line_number,
            column=column,
            line_bytes=line,
            state=self._state_dict(),
            reason=reason,
        )

    def _state_snapshot(self) -> EventStreamParserState:
        return EventStreamParserState(**self._state_dict())

    def _state_dict(self) -> dict[str, Any]:
        return {
            "last_event_id": self._last_event_id,
            "retry": self._retry,
            "events_dispatched": self._events_dispatched,
            "bytes_processed": self._bytes_processed,
            "line_number": self._line_number,
            "pending_line_bytes": bytes(self._line),
            "pending_event_bytes": self._event_bytes,
            "pending_event_type": self._event_type,
            "pending_data": "".join(self._data_parts),
            "at_stream_start": self._at_stream_start,
            "cr_pending": self._cr_pending,
            "finished": self._finished,
        }
