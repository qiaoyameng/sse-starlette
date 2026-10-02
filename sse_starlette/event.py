import io
import re
import json
from typing import AsyncIterable, AsyncIterator, Optional, Any, Union


class ServerSentEvent:
    """
    Helper class to format data for Server-Sent Events (SSE).
    """

    _LINE_SEP_EXPR = re.compile(r"\r\n|\r|\n")
    DEFAULT_SEPARATOR = "\r\n"

    def __init__(
        self,
        data: Optional[Any] = None,
        *,
        event: Optional[str] = None,
        id: Optional[str] = None,
        retry: Optional[int] = None,
        comment: Optional[str] = None,
        sep: Optional[str] = None,
    ) -> None:
        self.data = data
        self.event = event
        self.id = id
        self.retry = retry
        self.comment = comment
        self._sep = sep if sep is not None else self.DEFAULT_SEPARATOR

    def encode(self) -> bytes:
        buffer = io.StringIO()
        if self.comment is not None:
            for chunk in self._LINE_SEP_EXPR.split(str(self.comment)):
                buffer.write(f": {chunk}{self._sep}")

        if self.id is not None:
            # Clean newlines in the event id
            buffer.write("id: " + self._LINE_SEP_EXPR.sub("", self.id) + self._sep)

        if self.event is not None:
            # Clean newlines in the event name
            buffer.write(
                "event: " + self._LINE_SEP_EXPR.sub("", self.event) + self._sep
            )

        if self.data is not None:
            # Break multi-line data into multiple data: lines
            for chunk in self._LINE_SEP_EXPR.split(str(self.data)):
                buffer.write(f"data: {chunk}{self._sep}")

        if self.retry is not None:
            if not isinstance(self.retry, int):
                raise TypeError("retry argument must be int")
            buffer.write(f"retry: {self.retry}{self._sep}")

        buffer.write(self._sep)
        return buffer.getvalue().encode("utf-8")


class JSONServerSentEvent(ServerSentEvent):
    """
    Helper class to format JSON data for Server-Sent Events (SSE).
    """

    def __init__(
        self,
        data: Optional[Any] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(
            json.dumps(
                data,
                ensure_ascii=False,
                allow_nan=False,
                indent=None,
                separators=(",", ":"),
            )
            if data is not None
            else None,
            *args,
            **kwargs,
        )


class StreamingServerSentEvent(ServerSentEvent):
    """
    Server-Sent Event whose ``data`` is an async source of text chunks.

    Unlike :class:`ServerSentEvent`, the payload is never joined into one
    string: chunks are pulled lazily (downstream backpressure limits how fast
    the source is read), split into logical lines across chunk boundaries
    (CR, LF, CRLF), and encoded as ``data: `` lines as they become available.

    Commit boundary: the head lines (comment/id/event) are emitted first, but
    the terminating blank line that makes the client dispatch the event is
    only emitted after the source completes successfully. If the source
    fails, the partial event is left unterminated so a half event is never
    disguised as a complete one.

    The event takes ownership of the source and closes it (via ``aclose``
    when available) once encoding finishes, fails, or is abandoned early
    (e.g. client disconnect).
    """

    def __init__(
        self,
        data: Optional[AsyncIterable[str]] = None,
        *,
        event: Optional[str] = None,
        id: Optional[str] = None,
        retry: Optional[int] = None,
        comment: Optional[str] = None,
        sep: Optional[str] = None,
    ) -> None:
        super().__init__(
            None, event=event, id=id, retry=retry, comment=comment, sep=sep
        )
        self.data = data

    def encode(self) -> bytes:
        raise TypeError(
            "StreamingServerSentEvent cannot be encoded synchronously; "
            "use astream_encode() and send the chunks as they are produced"
        )

    def _encode_head(self) -> bytes:
        buffer = io.StringIO()
        if self.comment is not None:
            for chunk in self._LINE_SEP_EXPR.split(str(self.comment)):
                buffer.write(f": {chunk}{self._sep}")
        if self.id is not None:
            # Clean newlines in the event id
            buffer.write("id: " + self._LINE_SEP_EXPR.sub("", self.id) + self._sep)
        if self.event is not None:
            # Clean newlines in the event name
            buffer.write(
                "event: " + self._LINE_SEP_EXPR.sub("", self.event) + self._sep
            )
        return buffer.getvalue().encode("utf-8")

    def _encode_tail(self) -> bytes:
        buffer = io.StringIO()
        if self.retry is not None:
            if not isinstance(self.retry, int):
                raise TypeError("retry argument must be int")
            buffer.write(f"retry: {self.retry}{self._sep}")
        buffer.write(self._sep)
        return buffer.getvalue().encode("utf-8")

    async def astream_encode(self) -> AsyncIterator[bytes]:
        try:
            head = self._encode_head()
            if head:
                yield head
            if self.data is not None:
                pending = ""
                async for chunk in self.data:
                    pending += str(chunk)
                    # A trailing CR may be the first half of a CRLF split
                    # across chunks; hold it back until more input arrives or
                    # the source ends.
                    hold = "\r" if pending.endswith("\r") else ""
                    parts = self._LINE_SEP_EXPR.split(
                        pending[: len(pending) - len(hold)]
                    )
                    for line in parts[:-1]:
                        yield f"data: {line}{self._sep}".encode("utf-8")
                    pending = parts[-1] + hold
                # End of source: a held CR is now a definite separator and the
                # remainder is the final logical line (re.split parity).
                for line in self._LINE_SEP_EXPR.split(pending):
                    yield f"data: {line}{self._sep}".encode("utf-8")
            yield self._encode_tail()
        finally:
            aclose = getattr(self.data, "aclose", None)
            if aclose is not None:
                await aclose()


def ensure_bytes(data: Union[bytes, dict, ServerSentEvent, Any], sep: str) -> bytes:
    if isinstance(data, (bytes, memoryview)):
        return bytes(data)
    if isinstance(data, ServerSentEvent):
        return data.encode()
    if isinstance(data, dict):
        data["sep"] = sep
        return ServerSentEvent(**data).encode()
    return ServerSentEvent(str(data), sep=sep).encode()
