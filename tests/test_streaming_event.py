"""Tests for StreamingServerSentEvent: incremental encoding of async text sources."""

import anyio
import pytest

from sse_starlette import EventSourceResponse, ServerSentEvent, StreamingServerSentEvent


async def collect(event: StreamingServerSentEvent) -> bytes:
    return b"".join([chunk async for chunk in event.encode_stream()])


class ChunkSource:
    """Counted async chunk source with optional failure and close tracking."""

    def __init__(self, chunks, fail_at=None):
        self.chunks = list(chunks)
        self.fail_at = fail_at
        self.reads = 0
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.reads >= len(self.chunks):
            raise StopAsyncIteration
        if self.fail_at is not None and self.reads == self.fail_at:
            raise RuntimeError("source failed")
        chunk = self.chunks[self.reads]
        self.reads += 1
        return chunk

    async def aclose(self):
        self.closed = True


TEXTS = [
    "foo\r\nbar\rx\nyz",
    "a\rb",
    "a\r\nb\r\n",
    "trailing\r",
    "trailing\n",
    "",
    "\n",
    "\r",
    "\r\n",
    "x\ry\n\r\nz\r\r\n",
    "multi-byte: héllo\r\n末\n",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("text", TEXTS)
async def test_encode_stream_matches_joined_encode_at_every_split(text):
    """Splitting the same text at every position must not change the output."""
    expected = ServerSentEvent(text).encode()
    for i in range(len(text) + 1):
        source = ChunkSource([text[:i], text[i:]])
        result = await collect(StreamingServerSentEvent(source))
        assert result == expected, f"split at {i}: {result!r} != {expected!r}"
        assert source.closed


@pytest.mark.asyncio
async def test_encode_stream_matches_joined_encode_with_all_fields():
    text = "foo\r\nbar\rx\nyz"
    kwargs = dict(event="update", id="42", retry=500, comment="a comment")
    expected = ServerSentEvent(text, **kwargs).encode()
    for i in range(len(text) + 1):
        source = ChunkSource([text[:i], text[i:]])
        result = await collect(StreamingServerSentEvent(source, **kwargs))
        assert result == expected, f"split at {i}: {result!r} != {expected!r}"


@pytest.mark.asyncio
@pytest.mark.parametrize("sep", ["\n", "\r", "\r\n"], ids=("LF", "CR", "CR+LF"))
async def test_encode_stream_custom_sep(sep):
    text = "foo\r\nbar\rx\nyz"
    expected = ServerSentEvent(text, sep=sep).encode()
    for i in range(len(text) + 1):
        source = ChunkSource([text[:i], text[i:]])
        result = await collect(StreamingServerSentEvent(source, sep=sep))
        assert result == expected, f"split at {i}: {result!r} != {expected!r}"


@pytest.mark.asyncio
async def test_encode_stream_no_data_matches_plain_event():
    expected = ServerSentEvent(event="update", id="7").encode()
    result = await collect(StreamingServerSentEvent(event="update", id="7"))
    assert result == expected


@pytest.mark.asyncio
async def test_encode_is_not_available_synchronously():
    with pytest.raises(TypeError, match="encode_stream"):
        StreamingServerSentEvent(ChunkSource(["x"])).encode()


@pytest.mark.asyncio
async def test_encode_stream_retry_must_be_int():
    event = StreamingServerSentEvent(ChunkSource(["x"]), retry="ten")
    with pytest.raises(TypeError, match="retry argument must be int"):
        await collect(event)


@pytest.mark.asyncio
async def test_source_failure_never_commits_event():
    """A failing source must not let a half event look complete."""
    source = ChunkSource(["partial\n", "more\n"], fail_at=1)
    response = EventSourceResponse(
        iter([StreamingServerSentEvent(source, id="42", event="update")]), ping=0
    )

    sent = []

    async def send(message):
        sent.append(message)

    async def receive():
        await anyio.sleep_forever()

    scope = {"type": "http", "method": "GET", "headers": []}
    with pytest.raises(RuntimeError, match="source failed"):
        await response(scope, receive, send)

    body = b"".join(
        m["body"] for m in sent if m["type"] == "http.response.body"
    )
    # Head and first data line went out, but the committing blank line did not.
    assert body == b"id: 42\r\nevent: update\r\ndata: partial\r\n"
    assert source.closed


@pytest.mark.asyncio
async def test_first_chunk_sent_early_and_no_unbounded_readahead():
    """Downstream backpressure must limit how far the source is read."""
    chunks = [f"line{i}\n" for i in range(10)]
    source = ChunkSource(chunks)
    response = EventSourceResponse(iter([StreamingServerSentEvent(source)]), ping=0)

    sent = []
    first_body = anyio.Event()
    release = anyio.Event()

    async def send(message):
        sent.append(message)
        if message["type"] == "http.response.body" and message["body"]:
            first_body.set()
            await release.wait()

    async def receive():
        await anyio.sleep_forever()

    scope = {"type": "http", "method": "GET", "headers": []}
    async with anyio.create_task_group() as tg:
        tg.start_soon(response, scope, receive, send)
        with anyio.fail_after(5):
            await first_body.wait()
        # First encoded chunk went out long before the source was exhausted.
        assert source.reads <= 2
        # While the send is blocked, reading must not run ahead.
        await anyio.sleep(0.1)
        assert source.reads <= 2
        # The send lock is held for the whole event (no interleaving possible).
        assert response._send_lock.locked()
        release.set()

    assert source.closed
    body = b"".join(
        m["body"] for m in sent if m["type"] == "http.response.body"
    )
    assert body == ServerSentEvent("".join(chunks)).encode()


@pytest.mark.asyncio
async def test_ping_cannot_interleave_into_streaming_event():
    source = ChunkSource(["a\n", "b\n"])
    response = EventSourceResponse(
        iter([StreamingServerSentEvent(source)]), ping=0.05
    )

    sent = []
    blocked_send = anyio.Event()
    release = anyio.Event()

    async def send(message):
        sent.append(message)
        if message["type"] == "http.response.body" and message["body"]:
            blocked_send.set()
            await release.wait()

    async def receive():
        await anyio.sleep_forever()

    scope = {"type": "http", "method": "GET", "headers": []}
    async with anyio.create_task_group() as tg:
        tg.start_soon(response, scope, receive, send)
        with anyio.fail_after(5):
            await blocked_send.wait()
        # Several ping intervals pass while the event is mid-flight.
        await anyio.sleep(0.2)
        pings = [
            m
            for m in sent
            if m["type"] == "http.response.body"
            and m["body"].startswith(b": ping")
        ]
        assert pings == []
        release.set()


@pytest.mark.asyncio
async def test_source_closed_on_client_disconnect():
    source = ChunkSource([f"line{i}\n" for i in range(100)])
    response = EventSourceResponse(iter([StreamingServerSentEvent(source)]), ping=0)

    first_body = anyio.Event()
    blocker = anyio.Event()

    async def send(message):
        if message["type"] == "http.response.body" and message["body"]:
            first_body.set()
            await blocker.wait()  # never released; disconnect cancels us

    async def receive():
        await first_body.wait()
        return {"type": "http.disconnect"}

    scope = {"type": "http", "method": "GET", "headers": []}
    with anyio.fail_after(5):
        await response(scope, receive, send)

    assert source.closed
