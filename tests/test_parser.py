from __future__ import annotations

import random
from dataclasses import asdict

import pytest

from sse_starlette.event import ServerSentEvent
from sse_starlette.parser import (
    EventStreamParser,
    EventStreamParserState,
    ServerSentEventParseError,
    parse_server_sent_events,
)


def event_tuple(event: ServerSentEvent) -> tuple[object, ...]:
    return (event.data, event.event, event.id, event.retry, event.comment)


def result_tuple(
    events: list[ServerSentEvent], state: EventStreamParserState
) -> tuple[object, ...]:
    return (
        [event_tuple(event) for event in events],
        asdict(state)
        | {"pending_line_bytes": bytes(state.pending_line_bytes)},
    )


def consume(parser: EventStreamParser, chunks: list[bytes]) -> tuple[object, ...]:
    events: list[ServerSentEvent] = []
    for chunk in chunks:
        events.extend(parser.feed(chunk))
    events.extend(parser.finish())
    return result_tuple(events, parser.state())


def test_feed_whenUtf8AndDelimitersSplit_thenParsesEvent():
    parser = EventStreamParser()

    assert parser.feed(b"id: 42\reven") == []
    assert parser.feed(b"t: update\rdata: \xe4") == []
    events = parser.feed(b"\xbd\xa0\r\r")

    assert len(events) == 1
    assert event_tuple(events[0]) == ("你", "update", "42", None, None)


def test_cr_only_delimiters_and_null_id_are_supported():
    parser = EventStreamParser()

    parser.feed(b"id: bad\0id\rid: good\rd")
    events = parser.feed(b"ata: x\r\r")

    assert event_tuple(events[0]) == ("x", "message", "good", None, None)


def test_parse_server_sent_events_from_iterable():
    events = list(
        parse_server_sent_events(
            [b"data: a\n\n", b"event: ", b"update\ndata: b\n\n"]
        )
    )

    assert event_tuple(events[0]) == ("a", "message", "", None, None)
    assert event_tuple(events[1]) == ("b", "update", "", None, None)


def test_multiline_data_joins_with_newlines():
    events = EventStreamParser().feed(b"data: one\ndata:\ndata: three\n\n")

    assert len(events) == 1
    assert event_tuple(events[0]) == ("one\n\nthree", "message", "", None, None)


def test_comments_and_empty_blocks_do_not_dispatch():
    parser = EventStreamParser()

    assert parser.feed(b": keep-alive\n\n:another comment\n") == []
    assert parser.events_dispatched == 0


def test_empty_id_invalid_retry_and_empty_event():
    parser = EventStreamParser()

    events = parser.feed(
        b"id: old\nretry: 100\nid:\nretry: nope\ndata: x\nevent:\n\n"
    )

    assert len(events) == 1
    assert event_tuple(events[0]) == ("x", "message", "", None, None)
    assert parser.last_event_id == ""
    assert parser.retry == 100


def test_leading_bom_is_stripped_once_even_when_split():
    parser = EventStreamParser()

    assert parser.feed(b"\xef\xbb") == []
    events = parser.feed(b"\xbfdata: bom\n\n")

    assert len(events) == 1
    assert events[0].data == "bom"
    assert parser.feed(b"data: \xef\xbb\xbfsecond\n\n")[0].data == "\ufeffsecond"


def test_finish_with_unterminated_event_does_not_dispatch():
    parser = EventStreamParser()

    assert parser.feed(b"id: x\ndata: unfinished\n") == []
    assert parser.finish() == []
    assert parser.events_dispatched == 0
    assert parser.state().pending_data == "unfinished\n"


def test_invalid_utf8_error_includes_position_and_state():
    parser = EventStreamParser()
    parser.feed(b"id: 7\n")

    with pytest.raises(ServerSentEventParseError) as exc_info:
        parser.feed(b"data: \xff\n\n")

    error = exc_info.value
    assert error.reason == "invalid_utf8"
    assert error.byte_offset == 12
    assert error.line_number == 2
    assert error.column == 6
    assert error.state["last_event_id"] == "7"
    assert error.state["pending_line_bytes"] == b""


def test_incomplete_utf8_at_end_is_reported_by_finish():
    parser = EventStreamParser()

    parser.feed(b"data: \xe4")
    with pytest.raises(ServerSentEventParseError) as exc_info:
        parser.finish()

    assert exc_info.value.reason == "invalid_utf8"
    assert exc_info.value.line_number == 1
    assert exc_info.value.column == 6


def test_line_and_event_limits():
    line_parser = EventStreamParser(max_line_bytes=4)
    with pytest.raises(ServerSentEventParseError) as line_error:
        line_parser.feed(b"data: long\n")
    assert line_error.value.reason == "line_too_long"
    assert line_error.value.byte_offset == 10

    event_parser = EventStreamParser(max_event_bytes=14)
    with pytest.raises(ServerSentEventParseError) as event_error:
        event_parser.feed(b"data: 12345\ndata: 6\n\n")
    assert event_error.value.reason == "event_too_large"
    assert event_error.value.state["pending_event_bytes"] == 11
    assert event_error.value.line_bytes == b"data: 6"


def test_all_single_cut_points_produce_identical_results():
    payload = (
        b"\xef\xbb\xbf: connected\r\n"
        b"id: 1\r\nretry: 500\r\ndata: one\r\ndata: two\r\n\r\n"
        b"id:\r\nretry: bad\r\nevent:\r\ndata: \xe4\xbd\xa0\r\n\r"
    )
    control = consume(EventStreamParser(), [payload])

    for cut in range(1, len(payload)):
        assert consume(EventStreamParser(), [payload[:cut], payload[cut:]]) == control


@pytest.mark.parametrize("seed", range(20))
def test_random_chunking_produces_identical_results(seed):
    payload = (
        b":start\n\nid: 1\nretry: 250\ndata: first\n\n"
        b"id:\ndata: \xf0\x9f\x9a\x80\nevent: custom\r\n\r\n"
        b"unterminated:nope\n"
    )
    control = consume(EventStreamParser(), [payload])
    rng = random.Random(seed)
    chunks: list[bytes] = []
    start = 0

    while start < len(payload):
        end = min(len(payload), start + rng.randint(1, 7))
        chunks.append(payload[start:end])
        start = end

    assert consume(EventStreamParser(), chunks) == control


def test_empty_chunk_does_not_change_pending_crlf_state():
    parser = EventStreamParser()

    assert parser.feed(b"data: x\r") == []
    assert parser.feed(b"") == []
    assert parser.feed(b"\n\r\n")[0].data == "x"
