"""Incremental parser for Server-Sent Events (SSE) byte streams.

Complements :class:`sse_starlette.event.ServerSentEvent` (the encoder) with a
chunk-oriented decoder, suitable for gateways that filter and forward an
upstream SSE stream: feed arbitrary ``bytes`` blocks, get structured events
back.

The parser follows the WHATWG "Server-sent events" interpretation rules:

* lines are terminated by CR, LF or CRLF, even when a terminator (or a
  multi-byte UTF-8 character) is split across chunk boundaries;
* a UTF-8 BOM is stripped once, at the very start of the stream;
* multiple ``data:`` lines accumulate into a single event payload;
* comment lines (starting with ``:``) and unknown fields are ignored;
* ``id:`` with an empty value resets the last event id;
* ``retry:`` values that are not plain ASCII digits are ignored;
* events without any ``data:`` line are not dispatched;
* at end of input, an event that was not terminated by a blank line is
  discarded, never dispatched.
"""

import codecs
from dataclasses import dataclass
from typing import List, Optional, Union

from sse_starlette.event import ServerSentEvent

__all__ = ["SSEParser", "SSEParseError", "ParserStateSnapshot"]


@dataclass(frozen=True)
class ParserStateSnapshot:
    """Internal parser state, captured when an error is raised."""

    offset: int  # bytes consumed from the stream so far
    line: int  # 1-based number of the line being processed
    partial_line_length: int  # chars buffered for the current, unfinished line
    buffered_data_lines: int  # data lines accumulated for the pending event
    buffered_data_length: int  # chars accumulated for the pending event payload
    event_type: str  # event type buffered for the pending event
    last_event_id: Optional[str]


class SSEParseError(ValueError):
    """Raised when the stream violates parser limits or the API contract.

    Carries a :class:`ParserStateSnapshot` with enough position and state
    information to locate the failure in the upstream stream.
    """

    def __init__(self, message: str, *, snapshot: ParserStateSnapshot) -> None:
        self.message = message
        self.snapshot = snapshot
        self.offset = snapshot.offset
        self.line = snapshot.line
        super().__init__(
            f"{message} (byte offset={snapshot.offset}, line={snapshot.line})"
        )


class SSEParser:
    """Incremental SSE parser.

    Usage::

        parser = SSEParser()
        for chunk in upstream_byte_chunks:
            for event in parser.feed(chunk):
                ...  # forward or filter a ServerSentEvent
        for event in parser.close():
            ...  # events completed by the final bytes

    Control state that the SSE spec keeps outside of dispatched events is
    exposed as attributes: ``last_event_id`` and ``reconnection_time``.
    """

    DEFAULT_MAX_LINE_LENGTH = 1 << 16  # 64 Ki chars per line
    DEFAULT_MAX_EVENT_SIZE = 1 << 20  # 1 Mi chars per event block

    def __init__(
        self,
        *,
        max_line_length: int = DEFAULT_MAX_LINE_LENGTH,
        max_event_size: int = DEFAULT_MAX_EVENT_SIZE,
    ) -> None:
        if max_line_length < 1:
            raise ValueError("max_line_length must be >= 1")
        if max_event_size < 1:
            raise ValueError("max_event_size must be >= 1")
        self._max_line_length = max_line_length
        self._max_event_size = max_event_size
        # utf-8-sig strips the BOM once, at the very start of the stream, and
        # the incremental decoder tolerates multi-byte chars split across feeds.
        self._decoder = codecs.getincrementaldecoder("utf-8-sig")("replace")
        self._buffer = ""  # decoded chars not yet terminated by a line ending
        self._consumed = 0  # bytes fed so far
        self._line_no = 0  # 1-based number of the line being processed
        self._data_lines: List[str] = []
        self._data_length = 0
        self._event_type = ""
        self._event_size = 0  # chars accumulated for the current event block
        self._closed = False
        self._discarded = ""
        self.last_event_id: Optional[str] = None
        self.reconnection_time: Optional[int] = None

    @property
    def discarded(self) -> str:
        """Unterminated trailing line dropped by ``close()`` ("" if none)."""
        return self._discarded

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def pending_event(self) -> bool:
        """True while un-dispatched event data is buffered.

        A pending event is only dispatched by a following blank line; if the
        stream ends first, it is discarded rather than dispatched.
        """
        return bool(self._data_lines) or bool(self._event_type)

    def feed(
        self, data: Union[bytes, bytearray, memoryview]
    ) -> List[ServerSentEvent]:
        """Consume a chunk of the stream, returning newly completed events."""
        if self._closed:
            raise self._error("feed() called after close()")
        chunk = bytes(data)
        self._consumed += len(chunk)
        self._buffer += self._decoder.decode(chunk)
        events: List[ServerSentEvent] = []
        for line in self._pop_lines(final=False):
            self._process_line(line, events)
        self._check_partial_line()
        return events

    def close(self) -> List[ServerSentEvent]:
        """Signal end of input and flush the decoder.

        Per the SSE spec, any pending data at end of input is discarded: an
        event that was not terminated by a blank line is never dispatched.
        The dropped unterminated line (if any) is available as ``discarded``.
        """
        if self._closed:
            raise self._error("close() called twice")
        self._closed = True
        self._buffer += self._decoder.decode(b"", final=True)
        events: List[ServerSentEvent] = []
        for line in self._pop_lines(final=True):
            self._process_line(line, events)
        self._discarded = self._buffer
        self._buffer = ""
        return events

    def _pop_lines(self, final: bool) -> List[str]:
        lines: List[str] = []
        buf = self._buffer
        start = 0
        i = 0
        n = len(buf)
        while i < n:
            ch = buf[i]
            if ch == "\n":
                lines.append(buf[start:i])
                i += 1
                start = i
            elif ch == "\r":
                if i + 1 < n:
                    lines.append(buf[start:i])
                    i += 2 if buf[i + 1] == "\n" else 1
                    start = i
                elif final:
                    lines.append(buf[start:i])
                    i += 1
                    start = i
                else:
                    # CR at a chunk boundary: wait to see whether LF follows.
                    break
            else:
                i += 1
        self._buffer = buf[start:]
        return lines

    def _process_line(self, line: str, events: List[ServerSentEvent]) -> None:
        self._line_no += 1
        if len(line) > self._max_line_length:
            raise self._error(
                f"line exceeds max_line_length ({self._max_line_length} chars)"
            )
        if not line:
            self._dispatch(events)
            return
        if line.startswith(":"):
            return  # comment lines carry no event state
        self._event_size += len(line) + 1
        if self._event_size > self._max_event_size:
            raise self._error(
                f"event block exceeds max_event_size ({self._max_event_size} chars)"
            )
        field, sep, value = line.partition(":")
        if sep and value.startswith(" "):
            value = value[1:]
        if field == "data":
            self._data_lines.append(value)
            self._data_length += len(value) + 1
        elif field == "event":
            self._event_type = value
        elif field == "id":
            if "\x00" not in value:
                # An empty value is valid and resets the last event id.
                self.last_event_id = value
        elif field == "retry":
            if value and value.isascii() and value.isdigit():
                self.reconnection_time = int(value)
            # Non-digit retry values are ignored, not an error.
        # Unknown fields are ignored per spec.

    def _dispatch(self, events: List[ServerSentEvent]) -> None:
        self._event_size = 0
        if not self._data_lines:
            # Events without data are not dispatched.
            self._event_type = ""
            return
        events.append(
            ServerSentEvent(
                data="\n".join(self._data_lines),
                event=self._event_type or None,
                id=self.last_event_id,
            )
        )
        self._data_lines = []
        self._data_length = 0
        self._event_type = ""

    def _check_partial_line(self) -> None:
        pending = self._buffer[:-1] if self._buffer.endswith("\r") else self._buffer
        if len(pending) > self._max_line_length:
            raise self._error(
                f"line exceeds max_line_length ({self._max_line_length} chars)",
                line=self._line_no + 1,
            )

    def _error(self, message: str, *, line: Optional[int] = None) -> SSEParseError:
        return SSEParseError(
            message,
            snapshot=ParserStateSnapshot(
                offset=self._consumed,
                line=self._line_no if line is None else line,
                partial_line_length=len(self._buffer),
                buffered_data_lines=len(self._data_lines),
                buffered_data_length=self._data_length,
                event_type=self._event_type,
                last_event_id=self.last_event_id,
            ),
        )
