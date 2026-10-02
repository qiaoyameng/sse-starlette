import random

import pytest

from sse_starlette import SSEParseError, SSEParser, ServerSentEvent


def parse_all(chunks, **parser_kwargs):
    """Feed chunks through a parser and close it; return (parser, events)."""
    parser = SSEParser(**parser_kwargs)
    events = []
    for chunk in chunks:
        events.extend(parser.feed(chunk))
    events.extend(parser.close())
    return parser, events


def event_key(event):
    return (event.data, event.event, event.id)


def snapshot(parser, events):
    """Dispatched events plus control state kept outside of events."""
    return (
        [event_key(e) for e in events],
        parser.last_event_id,
        parser.reconnection_time,
        parser.discarded,
    )


def test_single_event():
    _, events = parse_all([b"data: hello\n\n"])
    assert [event_key(e) for e in events] == [("hello", None, None)]
    assert isinstance(events[0], ServerSentEvent)


def test_multiline_data_accumulates_with_lf_separator():
    _, events = parse_all([b"data: line1\ndata: line2\ndata: line3\n\n"])
    assert [e.data for e in events] == ["line1\nline2\nline3"]


@pytest.mark.parametrize("newline", [b"\n", b"\r\n", b"\r"])
def test_line_endings(newline):
    payload = b"data: a" + newline + newline + b"data: b" + newline + newline
    _, events = parse_all([payload])
    assert [e.data for e in events] == ["a", "b"]


def test_field_without_colon_and_extra_space_stripping():
    # "data" w/o colon -> empty value; only ONE leading space is stripped.
    _, events = parse_all([b"data\ndata:  padded\n\n"])
    assert [e.data for e in events] == ["\n padded"]


def test_comment_lines_are_ignored():
    _, events = parse_all([b": keepalive\ndata: a\n: another\n\n"])
    assert [e.data for e in events] == ["a"]


def test_unknown_fields_are_ignored():
    _, events = parse_all([b"foo: bar\ndata: a\n\n"])
    assert [e.data for e in events] == ["a"]


def test_event_type_and_id():
    _, events = parse_all([b"event: greeting\nid: 42\ndata: hi\n\n"])
    assert [event_key(e) for e in events] == [("hi", "greeting", "42")]


def test_empty_id_resets_last_event_id():
    parser, events = parse_all([b"id: 1\ndata: a\n\nid:\ndata: b\n\n"])
    assert [event_key(e) for e in events] == [("a", None, "1"), ("b", None, "")]
    assert parser.last_event_id == ""


def test_id_with_null_char_is_ignored():
    parser, events = parse_all([b"id: 1\nid: a\x00b\ndata: x\n\n"])
    assert [event_key(e) for e in events] == [("x", None, "1")]
    assert parser.last_event_id == "1"


@pytest.mark.parametrize(
    "retry_line, expected",
    [
        (b"retry: 2500\n", 2500),
        (b"retry: 007\n", 7),
        (b"retry: nope\n", None),  # not digits -> ignored
        (b"retry: -1\n", None),  # sign is not a digit -> ignored
        (b"retry: 12.5\n", None),
        (b"retry:\n", None),  # empty -> ignored
    ],
)
def test_retry_handling(retry_line, expected):
    parser, _ = parse_all([retry_line + b"data: x\n\n"])
    assert parser.reconnection_time == expected


def test_events_without_data_are_not_dispatched():
    _, events = parse_all([b"event: typed\n\n\n\n: comment\n\n"])
    assert events == []


def test_empty_data_line_still_dispatches():
    _, events = parse_all([b"data:\n\n"])
    assert [e.data for e in events] == [""]


def test_bom_stripped_once_at_stream_start():
    _, events = parse_all(["\ufeffdata: a\n\n".encode(), "data: \ufeffb\n\n".encode()])
    assert [e.data for e in events] == ["a", "\ufeffb"]


def test_bom_split_across_chunks():
    payload = "\ufeffdata: a\n\n".encode()
    _, events = parse_all([payload[:1], payload[1:2], payload[2:]])
    assert [e.data for e in events] == ["a"]


def test_multibyte_utf8_split_across_chunks():
    payload = "data: héllo wörld ✓\n\n".encode()
    parser = SSEParser()
    events = []
    for byte in payload:
        events.extend(parser.feed(bytes([byte])))
    events.extend(parser.close())
    assert [e.data for e in events] == ["héllo wörld ✓"]


def test_crlf_split_across_chunks():
    # CR held at a chunk boundary, LF arrives with the next chunk.
    _, events = parse_all([b"data: a\r", b"\ndata: b\r", b"\n\r", b"\n"])
    assert [e.data for e in events] == ["a\nb"]


def test_cr_followed_by_non_lf_in_next_chunk():
    _, events = parse_all([b"data: a\r", b"data: b\n\n"])
    assert [e.data for e in events] == ["a\nb"]


def test_eof_discards_event_without_terminating_blank_line():
    parser, events = parse_all([b"data: complete\n\ndata: incomplete\n"])
    assert [e.data for e in events] == ["complete"]
    assert parser.pending_event  # buffered data was dropped, not dispatched
    assert parser.discarded == ""


def test_eof_discards_unterminated_line():
    parser, events = parse_all([b"data: a\n\ndata: unterminated"])
    assert [e.data for e in events] == ["a"]
    assert parser.discarded == "data: unterminated"


def test_eof_with_pending_cr_terminates_line():
    parser, events = parse_all([b"data: a\r\r"])
    assert [e.data for e in events] == ["a"]
    assert parser.discarded == ""


def test_feed_after_close_raises():
    parser, _ = parse_all([b"data: a\n\n"])
    with pytest.raises(SSEParseError):
        parser.feed(b"data: b\n\n")
    with pytest.raises(SSEParseError):
        parser.close()


def test_line_length_limit():
    parser = SSEParser(max_line_length=10)
    with pytest.raises(SSEParseError) as exc_info:
        parser.feed(b"data: " + b"x" * 100)
    err = exc_info.value
    assert err.offset == 106  # all bytes fed so far
    assert err.line == 1
    assert err.snapshot.partial_line_length == 106
    assert "max_line_length" in str(err)


def test_line_length_limit_on_completed_line():
    parser = SSEParser(max_line_length=3)
    with pytest.raises(SSEParseError) as exc_info:
        parser.feed(b"data: ab\n")
    assert exc_info.value.line == 1


def test_event_size_limit():
    parser = SSEParser(max_event_size=20)
    with pytest.raises(SSEParseError) as exc_info:
        parser.feed(b"data: " + b"a" * 10 + b"\ndata: " + b"b" * 10 + b"\n")
    err = exc_info.value
    assert err.line == 2
    assert err.snapshot.buffered_data_lines == 1
    assert "max_event_size" in str(err)


def test_comment_lines_do_not_count_towards_event_size():
    # keepalive comments must never trip the per-event limit
    parser, events = parse_all(
        [b": " + b"x" * 50 + b"\n"] * 10 + [b"data: a\n\n"], max_event_size=20
    )
    assert [e.data for e in events] == ["a"]


def test_invalid_limit_configuration():
    with pytest.raises(ValueError):
        SSEParser(max_line_length=0)
    with pytest.raises(ValueError):
        SSEParser(max_event_size=0)


def test_roundtrip_through_server_sent_event_encoder():
    sources = [
        ServerSentEvent(data="line1\nline2", event="upd", id="7"),
        ServerSentEvent(data="plain"),
        ServerSentEvent(data="with id", id="xyz"),
    ]
    payload = b"".join(src.encode() for src in sources)
    _, events = parse_all([payload])
    # Per the SSE spec the last event id persists stream-wide: events encoded
    # without an id still carry the most recently seen one when parsed back.
    assert [event_key(e) for e in events] == [
        ("line1\nline2", "upd", "7"),
        ("plain", None, "7"),
        ("with id", None, "xyz"),
    ]


# A short byte string exercising every cross-chunk hazard at once: BOM,
# comments, CRLF / CR / LF endings, multi-byte UTF-8, multi-line data,
# valid + invalid retry, empty id reset, and a discarded trailing line.
CHUNKING_PAYLOAD = (
    "\ufeff: hello\r\n"
    "event: greeting\r\n"
    "id: 42\n"
    "data: héllo \n"
    "data: wörld\r"
    "retry: 2500\r\n"
    "retry: nope\n"
    "\n"
    "data: second ✓\n"
    "id:\n"
    "\n"
    "data: incomplete"
).encode("utf-8")

CHUNKING_EXPECTED_EVENTS = [
    ("héllo \nwörld", "greeting", "42"),
    ("second ✓", None, ""),
]


def test_chunking_baseline():
    parser, events = parse_all([CHUNKING_PAYLOAD])
    assert snapshot(parser, events) == (
        CHUNKING_EXPECTED_EVENTS,
        "",
        2500,
        "data: incomplete",
    )


def test_every_single_split_point_is_equivalent():
    expected = snapshot(*parse_all([CHUNKING_PAYLOAD]))
    for i in range(len(CHUNKING_PAYLOAD) + 1):
        result = snapshot(*parse_all([CHUNKING_PAYLOAD[:i], CHUNKING_PAYLOAD[i:]]))
        assert result == expected, f"split at byte {i}"


def test_random_chunking_is_equivalent():
    expected = snapshot(*parse_all([CHUNKING_PAYLOAD]))
    rng = random.Random(20261003)
    for trial in range(100):
        chunks = []
        i = 0
        while i < len(CHUNKING_PAYLOAD):
            step = rng.randint(1, 11)
            chunks.append(CHUNKING_PAYLOAD[i : i + step])
            i += step
        result = snapshot(*parse_all(chunks))
        assert result == expected, f"trial {trial}: chunks={chunks!r}"
