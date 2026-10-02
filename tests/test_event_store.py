import anyio
import pytest
from starlette.requests import Request

from sse_starlette.event_store import (
    CursorStatus,
    CursorTooOldError,
    EventStore,
    EventStoreResponse,
    UnknownCursorError,
)


def make_scope():
    return {
        "type": "http",
        "method": "GET",
        "path": "/events",
        "headers": [],
        "query_string": b"",
    }


async def run_response(response, messages, stop_when, timeout=5.0):
    """Run an ASGI response, collecting sent messages until stop_when()."""

    async def send(message):
        messages.append(message)

    async def receive():
        await anyio.sleep(3600)  # client never disconnects; cancelled instead
        return {"type": "http.disconnect"}  # pragma: no cover

    with anyio.move_on_after(timeout):
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(lambda: response(make_scope(), receive, send))
            with anyio.move_on_after(timeout):
                while not stop_when():
                    await anyio.sleep(0.01)
            task_group.cancel_scope.cancel()


def body_bytes(messages):
    return b"".join(
        message.get("body", b"")
        for message in messages
        if message["type"] == "http.response.body"
    )


def status_of(messages):
    for message in messages:
        if message["type"] == "http.response.start":
            return message["status"]
    return None


class TestEventStoreIds:
    def test_publish_assigns_unique_strictly_increasing_ids(self):
        store = EventStore()
        events = [store.publish(f"data-{i}") for i in range(10)]
        ids = [event.id for event in events]
        assert len(set(ids)) == 10
        assert [int(i) for i in ids] == sorted(int(i) for i in ids)
        assert store.latest_id == ids[-1]
        assert store.oldest_id == ids[0]

    def test_publish_returns_event_with_encoded_id_line(self):
        store = EventStore()
        event = store.publish("hello", event="greeting")
        assert event.encode() == b"id: 1\r\nevent: greeting\r\ndata: hello\r\n\r\n"


class TestEventStoreWindow:
    def test_window_bounded_by_event_count(self):
        store = EventStore(max_events=3)
        for i in range(1, 6):
            store.publish(f"data-{i}")
        assert len(store) == 3
        # Eviction drops the oldest events but never renumbers the rest.
        assert [e.id for e in store.snapshot()] == ["3", "4", "5"]
        assert store.oldest_id == "3"
        assert store.latest_id == "5"

    def test_window_bounded_by_encoded_bytes(self):
        event = EventStore().publish("x" * 10)
        event_size = len(event.encode())
        store = EventStore(max_events=1000, max_bytes=2 * event_size)
        for _ in range(5):
            store.publish("x" * 10)
        retained = store.snapshot()
        assert len(retained) == 2
        assert sum(len(e.encode()) for e in retained) <= 2 * event_size
        assert [e.id for e in retained] == ["4", "5"]

    def test_single_oversized_event_is_retained(self):
        store = EventStore(max_events=10, max_bytes=4)
        store.publish("x" * 100)
        assert len(store) == 1
        assert store.latest_id == "1"

    def test_invalid_limits_rejected(self):
        with pytest.raises(ValueError):
            EventStore(max_events=0)
        with pytest.raises(ValueError):
            EventStore(max_bytes=0)


class TestCursorValidation:
    @pytest.fixture
    def store(self):
        store = EventStore(max_events=3)
        for i in range(1, 6):  # retained window: ids 3, 4, 5
            store.publish(f"data-{i}")
        return store

    @pytest.mark.parametrize("cursor", [None, "", "   "])
    def test_empty_cursor(self, store, cursor):
        assert store.validate_cursor(cursor).status is CursorStatus.EMPTY

    def test_replay_cursor(self, store):
        validation = store.validate_cursor("3")
        assert validation.status is CursorStatus.REPLAY
        assert validation.cursor == "3"

    def test_up_to_date_cursor(self, store):
        assert store.validate_cursor("5").status is CursorStatus.UP_TO_DATE

    def test_too_old_cursor(self, store):
        assert store.validate_cursor("2").status is CursorStatus.TOO_OLD

    @pytest.mark.parametrize("cursor", ["bogus", "1.5", "6", "999", "-1"])
    def test_unknown_cursor(self, store, cursor):
        assert store.validate_cursor(cursor).status is CursorStatus.UNKNOWN

    def test_any_cursor_is_unknown_while_store_is_empty(self):
        assert (
            EventStore().validate_cursor("1").status is CursorStatus.UNKNOWN
        )


class TestEventStoreStream:
    async def test_replay_then_live(self):
        store = EventStore()
        for i in range(1, 4):
            store.publish(f"data-{i}")
        received = []
        async with anyio.create_task_group() as task_group:

            async def consume():
                async for event in store.stream("1"):
                    received.append((event.id, event.data))
                    if len(received) == 3:
                        task_group.cancel_scope.cancel()

            task_group.start_soon(consume)
            await anyio.sleep(0.05)  # let the consumer finish replaying
            store.publish("data-4")
        assert received == [("2", "data-2"), ("3", "data-3"), ("4", "data-4")]

    async def test_empty_cursor_streams_live_only(self):
        store = EventStore()
        store.publish("old")
        received = []
        async with anyio.create_task_group() as task_group:

            async def consume():
                async for event in store.stream(None):
                    received.append(event.data)
                    task_group.cancel_scope.cancel()

            task_group.start_soon(consume)
            await anyio.sleep(0.05)
            store.publish("new")
        assert received == ["new"]

    async def test_up_to_date_cursor_yields_nothing_until_next_publish(self):
        store = EventStore()
        store.publish("data-1")
        received = []
        async with anyio.create_task_group() as task_group:

            async def consume():
                async for event in store.stream("1"):
                    received.append(event.data)
                    task_group.cancel_scope.cancel()

            task_group.start_soon(consume)
            await anyio.sleep(0.05)
            assert received == []  # no new events in the window
            store.publish("data-2")
        assert received == ["data-2"]

    async def test_event_published_at_end_of_replay_is_delivered_exactly_once(
        self,
    ):
        """Controlled pause point: publish while replay is about to finish.

        The generator is paused inside the replay loop (one snapshot event
        left) when the new event is published. The resumed sequence must
        contain it exactly once, in order, with nothing lost.
        """
        store = EventStore()
        for i in range(1, 6):
            store.publish(f"event-{i}")
        stream = store.stream("2")
        received = [await anext(stream), await anext(stream)]  # replay: 3, 4
        # Pause point: replay snapshot holds only event 5; publish now so the
        # new event arrives mid-handoff and must come via the live queue.
        store.publish("event-6")
        received.append(await anext(stream))  # replay: 5
        received.append(await anext(stream))  # live handoff: 6
        await stream.aclose()
        assert [e.id for e in received] == ["3", "4", "5", "6"]
        assert [e.data for e in received] == [
            "event-3",
            "event-4",
            "event-5",
            "event-6",
        ]

    async def test_too_old_cursor_raises(self):
        store = EventStore(max_events=2)
        for i in range(3):
            store.publish(f"data-{i}")
        with pytest.raises(CursorTooOldError):
            async for _ in store.stream("1"):
                pass  # pragma: no cover

    async def test_unknown_cursor_raises(self):
        store = EventStore()
        store.publish("data-1")
        with pytest.raises(UnknownCursorError):
            async for _ in store.stream("bogus"):
                pass  # pragma: no cover

    async def test_slow_subscriber_is_evicted_and_stream_ends_visibly(self):
        store = EventStore(subscriber_buffer=1)
        received = []
        async with anyio.create_task_group() as task_group:

            async def consume():
                async for event in store.stream(None):
                    received.append(event.data)

            task_group.start_soon(consume)
            while store.subscriber_count == 0:
                await anyio.sleep(0.01)
            store.publish("a")
            store.publish("b")
            # "a" is handed to the waiting receiver, "b" fills the buffer of
            # 1, so "c" overflows it -> subscriber evicted.
            store.publish("c")
        # The stream drained what it had and then ended (EndOfStream),
        # instead of buffering unboundedly or blocking publishers.
        assert received == ["a", "b"]
        assert store.subscriber_count == 0


class TestEventStoreResponse:
    async def test_valid_cursor_gets_200_with_replay_then_live(self):
        store = EventStore()
        for i in range(1, 4):
            store.publish(f"data-{i}")
        response = EventStoreResponse(store, "1", ping=0)
        messages = []
        await run_response(response, messages, lambda: b"id: 3" in body_bytes(messages))
        assert status_of(messages) == 200
        body = body_bytes(messages)
        assert body.find(b"id: 2") < body.find(b"id: 3")
        assert b"data: data-1" not in body  # cursor excluded, as per SSE spec

    async def test_empty_cursor_gets_200_without_replay(self):
        store = EventStore()
        store.publish("old")
        response = EventStoreResponse(store, None, ping=0)
        messages = []

        async def publish_live():
            while store.subscriber_count == 0:
                await anyio.sleep(0.01)
            store.publish("live")

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(publish_live)
            await run_response(
                response, messages, lambda: b"data: live" in body_bytes(messages)
            )
        assert status_of(messages) == 200
        body = body_bytes(messages)
        assert b"data: live" in body
        assert b"data: old" not in body

    async def test_too_old_cursor_gets_410_not_silent_200(self):
        store = EventStore(max_events=2)
        for i in range(3):
            store.publish(f"data-{i}")
        response = EventStoreResponse(store, "1", ping=0)
        messages = []
        await run_response(response, messages, lambda: status_of(messages) is not None)
        assert status_of(messages) == 410
        assert b"too old" in body_bytes(messages)
        # Crucially: no event payload was streamed after a 200.
        assert b"data:" not in body_bytes(messages)

    async def test_unknown_cursor_gets_400(self):
        store = EventStore()
        store.publish("data-1")
        response = EventStoreResponse(store, "bogus", ping=0)
        messages = []
        await run_response(response, messages, lambda: status_of(messages) is not None)
        assert status_of(messages) == 400
        assert b"not recognized" in body_bytes(messages)

    async def test_heartbeat_comments_do_not_move_business_cursor(self):
        store = EventStore()
        store.publish("data-1")
        response = EventStoreResponse(store, None, ping=0.05)
        messages = []

        async def publish_live():
            while store.subscriber_count == 0:
                await anyio.sleep(0.01)
            store.publish("data-2")

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(publish_live)
            await run_response(
                response,
                messages,
                lambda: body_bytes(messages).count(b": ping") >= 2
                and b"id: 2" in body_bytes(messages),
            )
        body = body_bytes(messages)
        assert b": ping" in body  # heartbeat comments flowed
        # Heartbeats carry no id and never entered the store...
        assert body.count(b"id: ") == 1
        assert store.latest_id == "2"
        assert len(store) == 2
        # ...so the business cursor still resolves exactly as before.
        assert store.validate_cursor("1").status is CursorStatus.REPLAY

    def test_eviction_does_not_change_business_cursor(self):
        store = EventStore(max_events=3)
        for i in range(1, 6):
            store.publish(f"data-{i}")
        # Ids are stable across eviction: a cursor still inside the window
        # resumes from exactly the same business event.
        assert store.validate_cursor("3").status is CursorStatus.REPLAY
        assert store.validate_cursor("2").status is CursorStatus.TOO_OLD
        assert [e.id for e in store.snapshot()] == ["3", "4", "5"]

    def test_from_request_reads_last_event_id_header(self):
        store = EventStore(max_events=2)
        for i in range(3):
            store.publish(f"data-{i}")

        def make_request(raw_headers):
            return Request(
                {
                    "type": "http",
                    "method": "GET",
                    "path": "/events",
                    "headers": raw_headers,
                    "query_string": b"",
                }
            )

        response = EventStoreResponse.from_request(
            store, make_request([(b"last-event-id", b"3")]), ping=0
        )
        assert response.validation.status is CursorStatus.UP_TO_DATE

        response = EventStoreResponse.from_request(
            store, make_request([(b"last-event-id", b"2")]), ping=0
        )
        assert response.validation.status is CursorStatus.REPLAY

        response = EventStoreResponse.from_request(
            store, make_request([(b"last-event-id", b"1")]), ping=0
        )
        assert response.validation.status is CursorStatus.TOO_OLD

        response = EventStoreResponse.from_request(
            store, make_request([(b"last-event-id", b"nope")]), ping=0
        )
        assert response.validation.status is CursorStatus.UNKNOWN

        response = EventStoreResponse.from_request(store, make_request([]), ping=0)
        assert response.validation.status is CursorStatus.EMPTY
