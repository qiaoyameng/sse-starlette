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
    SSE event whose ``data`` is an async iterable of text chunks.

    Unlike :class:`ServerSentEvent`, the chunks are encoded incrementally via
    ``encode_stream()`` instead of being joined into one string first, so a
    single event can be arbitrarily long without buffering it in memory.
    ``ServerSentEvent`` itself is unchanged; use this type only when the data
    source is asynchronous.

    Commit boundary: the head (comment/id/event lines) and each ``data:``
    line are emitted as they are encoded, but the event is only *committed*
    by the final blank line, which is emitted solely after the source
    finishes without error. If the source fails, the client has merely seen
    an unterminated event, which the SSE spec discards -- a half event is
    never disguised as a complete one. Consequently a sent ``id:`` line only
    takes effect on the client once the terminating blank line was sent.

    Ownership: the event owns its source for the duration of one
    ``encode_stream()`` pass and closes it (via ``aclose()`` when available)
    when the pass ends, fails, or is aborted (e.g. client disconnect).
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
            "use encode_stream() and send the chunks incrementally."
        )

    async def encode_stream(self) -> AsyncIterator[bytes]:
        """Yield the encoded event chunk by chunk.

        Byte-for-byte identical (when concatenated) to what
        ``ServerSentEvent.encode()`` produces for the joined text, but the
        source is only read as fast as the caller consumes the output.
        """
        if self.retry is not None and not isinstance(self.retry, int):
            raise TypeError("retry argument must be int")

        head = io.StringIO()
        if self.comment is not None:
            for chunk in self._LINE_SEP_EXPR.split(str(self.comment)):
                head.write(f": {chunk}{self._sep}")
        if self.id is not None:
            # Clean newlines in the event id
            head.write("id: " + self._LINE_SEP_EXPR.sub("", self.id) + self._sep)
        if self.event is not None:
            # Clean newlines in the event name
            head.write(
                "event: " + self._LINE_SEP_EXPR.sub("", self.event) + self._sep
            )
        if head.tell() > 0:
            yield head.getvalue().encode("utf-8")

        sep_bytes = self._sep.encode("utf-8")
        try:
            async for line in self._iter_data_lines():
                yield b"data: " + line.encode("utf-8") + sep_bytes
        finally:
            # Close the source owned by this pass: on success, on source
            # failure, and on abort (GeneratorExit from aclose()).
            aclose = getattr(self.data, "aclose", None)
            if aclose is not None:
                await aclose()

        # Tail (retry + committing blank line) is only reached when the
        # source completed without error.
        tail = io.StringIO()
        if self.retry is not None:
            tail.write(f"retry: {self.retry}{self._sep}")
        tail.write(self._sep)
        yield tail.getvalue().encode("utf-8")

    async def _iter_data_lines(self) -> AsyncIterator[str]:
        """Split the chunk stream into logical lines.

        Handles CR, LF and CRLF across chunk boundaries and yields exactly
        the pieces ``re.split(r"\\r\\n|\\r|\\n", text)`` would produce for
        the joined text, including trailing empty pieces.
        """
        if self.data is None:
            return
        pending = ""
        async for chunk in self.data:
            pending += chunk if isinstance(chunk, str) else str(chunk)
            start = 0
            while True:
                match = self._LINE_SEP_EXPR.search(pending, start)
                if match is None:
                    break
                if match.group() == "\r" and match.end() == len(pending):
                    # A trailing CR may be the first half of a CRLF split
                    # across chunks; decide once more input arrives.
                    break
                yield pending[start : match.start()]
                start = match.end()
            pending = pending[start:]
        if pending.endswith("\r"):
            yield pending[:-1]
            yield ""
        else:
            yield pending


def ensure_bytes(data: Union[bytes, dict, ServerSentEvent, Any], sep: str) -> bytes:
    if isinstance(data, (bytes, memoryview)):
        return bytes(data)
    if isinstance(data, ServerSentEvent):
        return data.encode()
    if isinstance(data, dict):
        data["sep"] = sep
        return ServerSentEvent(**data).encode()
    return ServerSentEvent(str(data), sep=sep).encode()
