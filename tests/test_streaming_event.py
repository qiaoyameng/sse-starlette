"""Tests for StreamingServerSentEvent: incremental encoding of async text sources."""

import anyio
import pytest

from sse_starlette import EventSourceResponse, ServerSentEvent, StreamingServerSentEvent


async def _achunks(chunks):
    for chunk in chunks:
        yield chunk


async def _collect(event):
    parts = []
    async for chunk in event.astream_encode():
        parts.append(chunk)
    return b"".join(parts)


def _scope():
    return {"type": "http", "method": "GET", "headers": []}


TEXT = "foo\r\nbar\rbaz\nqux\r\n"
FIELDS = dict(event="evt", id="42", retry=3, comment="note")


@pytest.mark.parametrize("cut", range(len(TEXT) + 1))
def test_astream_encode_whenCutAtAnyPosition_thenMatchesPlainEncode(cut):
    """Splitting the same text at every position (incl. inside CRLF pairs)
    must produce byte-identical output to ServerSentEvent.encode()."""
    chunks = [TEXT[:cut], TEXT[cut:]]
    expected = ServerSentEvent(TEXT, **FIELDS).encode()
    streamed = anyio.run(_collect, StreamingServerSentEvent(_achunks(chunks), **FIELDS))
    assert streamed == expected


@pytest.mark.parametrize("sep", ["\n", "\r", "\r\n"], ids=("LF", "CR", "CR+LF"))
def test_astream_encode_whenCustomSep_thenMatchesPlainEncode(sep):
    chunks = ["a\r", "\nb", "\r", "c\n"]
    expected = ServerSentEvent("a\r\nb\rc\n", sep=sep).encode()
    streamed = anyio.run(
        _collect, StreamingServerSentEvent(_achunks(chunks), sep=sep)
    )
    assert streamed == expected


def test_astream_encode_whenNoData_thenMatchesPlainEncode():
    expected = ServerSentEvent(event="evt", id="1").encode()
    streamed = anyio.run(_collect, StreamingServerSentEvent(event="evt", id="1"))
    assert streamed == expected


def test_encode_whenCalledSynchronously_thenRaises():
    with pytest.raises(TypeError, match="astream_encode"):
        StreamingServerSentEvent(_achunks(["x"])).encode()


@pytest.mark.asyncio
async def test_astream_encode_whenSourceFails_thenEventLeftUnterminated():
    """A failing source must not let a half event look like a complete one:
    no terminating blank line is emitted after the partial data."""

    async def source():
        yield "partial\n"
        yield "more"
        raise RuntimeError("boom")

    event = StreamingServerSentEvent(source(), id="1")
    chunks = []
    with pytest.raises(RuntimeError, match="boom"):
        async for chunk in event.astream_encode():
            chunks.append(chunk)

    body = b"".join(chunks)
    # "more" was never newline-terminated, so it stays unflushed; only the
    # head and the complete line went out, with no terminating blank line.
    assert body == b"id: 1\r\ndata: partial\r\n"
    assert not body.endswith(b"\r\n\r\n")


@pytest.mark.asyncio
async def test_response_whenSourceFails_thenBareExceptionAndNoTerminator():
    async def source():
        yield "partial\n"
        raise RuntimeError("boom")

    async def content():
        yield StreamingServerSentEvent(source(), id="1")

    response = EventSourceResponse(content(), ping=0)
    sent = []

    async def send(message):
        sent.append(message)

    async def receive():
        await anyio.sleep_forever()

    with pytest.raises(RuntimeError, match="boom"):
        await response(_scope(), receive, send)

    body = b"".join(
        m["body"] for m in sent if m["type"] == "http.response.body"
    )
    assert body == b"id: 1\r\ndata: partial\r\n"


@pytest.mark.asyncio
async def test_response_whenSlowCountedSource_thenFirstChunkEarlyAndBoundedPrefetch():
    """Downstream backpressure must limit read progress: while the first send
    is blocked, the source is not read to completion (no unbounded prefetch),
    and the first encoded chunk goes out before the source is exhausted."""
    produced = 0
    first_body_sent = anyio.Event()
    release_backpressure = anyio.Event()

    async def source():
        nonlocal produced
        for i in range(10):
            produced += 1
            yield f"line{i}\n"

    async def content():
        yield StreamingServerSentEvent(source())

    response = EventSourceResponse(content(), ping=0)
    sent = []

    async def send(message):
        sent.append(message)
        if message["type"] == "http.response.body":
            first_body_sent.set()
            await release_backpressure.wait()

    async def receive():
        await anyio.sleep_forever()

    async with anyio.create_task_group() as tg:
        tg.start_soon(response, _scope(), receive, send)
        await first_body_sent.wait()
        # The first chunk is out while the source has barely been read.
        assert 0 < produced <= 2
        release_backpressure.set()

    assert produced == 10
    body = b"".join(
        m["body"] for m in sent if m["type"] == "http.response.body"
    )
    expected = ServerSentEvent("".join(f"line{i}\n" for i in range(10))).encode()
    assert body == expected


@pytest.mark.asyncio
async def test_response_whenClientDisconnects_thenOwnedSourceIsClosed():
    closed = False
    parked = anyio.Event()

    async def source():
        nonlocal closed
        try:
            yield "chunk0\n"
            parked.set()
            await anyio.sleep_forever()
        finally:
            closed = True

    async def content():
        yield StreamingServerSentEvent(source())

    response = EventSourceResponse(content(), ping=0)

    async def send(message):
        pass

    async def receive():
        # Only report the disconnect once the stream is parked inside the
        # source, so the close must come from the cancellation path.
        await parked.wait()
        return {"type": "http.disconnect"}

    await response(_scope(), receive, send)
    assert closed


@pytest.mark.asyncio
async def test_response_whenPingFiresDuringEvent_thenPingNotInterleaved():
    """Heartbeats must not land inside a streaming event: the event's chunks
    occupy a contiguous run of body messages."""

    async def source():
        yield "a\n"
        await anyio.sleep(0.05)
        yield "b\n"

    async def content():
        yield StreamingServerSentEvent(source())

    ping_event = ServerSentEvent(comment="ping")
    ping_bytes = ping_event.encode()
    response = EventSourceResponse(
        content(), ping=0.01, ping_message_factory=lambda: ping_event
    )
    bodies = []

    async def send(message):
        if message["type"] == "http.response.body":
            bodies.append(message["body"])

    async def receive():
        await anyio.sleep_forever()

    await response(_scope(), receive, send)

    # "a\n" + "b\n" splits into lines "a", "b" and a final empty segment.
    event_chunks = [b"data: a\r\n", b"data: b\r\n", b"data: \r\n", b"\r\n"]
    indices = [i for i, b in enumerate(bodies) if b in event_chunks]
    assert [bodies[i] for i in indices] == event_chunks
    assert indices == list(range(indices[0], indices[0] + len(indices)))
    assert ping_bytes in bodies  # heartbeats still flow around the event
