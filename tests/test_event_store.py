import re

import anyio
import pytest
from starlette.requests import Request

from sse_starlette import (
    BoundedEventStore,
    CursorStatus,
    ResumableEventSourceResponse,
    ServerSentEvent,
    StaleCursorError,
    UnknownCursorError,
)


def sse_ids(payload: bytes) -> list[bytes]:
    return [match.rstrip(b"\r") for match in re.findall(rb"(?m)^id: (.*)$", payload)]


def sse_data(payload: bytes) -> list[str]:
    return [
        match.rstrip(b"\r").decode()
        for match in re.findall(rb"(?m)^data: (.*)$", payload)
    ]


async def collect_live(subscription, count: int) -> list[bytes]:
    messages = []
    async for message in subscription:
        messages.append(message)
        if len(messages) == count:
            break
    return messages


async def drain(subscription) -> bytes:
    messages = []
    with anyio.move_on_after(0.05):
        async for message in subscription:
            messages.append(message)
    return b"".join(messages)


@pytest.mark.anyio
async def test_publish_assigns_unique_ordered_ids_and_bounds_by_events():
    store = BoundedEventStore(max_events=2, max_bytes=10_000)

    first = await store.publish({"data": "one"})
    second = await store.publish({"data": "two"})
    third = await store.publish({"data": "three"})

    assert [first.id, second.id, third.id] == ["1", "2", "3"]
    assert [event.id for event in store._events] == ["2", "3"]
    assert store.oldest_event_id == "2"
    assert store.latest_event_id == "3"

    with pytest.raises(StaleCursorError):
        store.subscribe(first.id)

    subscription = store.subscribe(second.id)
    assert subscription.status is CursorStatus.REPLAY
    payload = await drain(subscription)
    assert sse_data(payload) == ["three"]


@pytest.mark.anyio
async def test_window_also_bounds_encoded_bytes():
    store = BoundedEventStore(max_events=10, max_bytes=64)
    first = await store.publish({"data": "x" * 40})
    second = await store.publish({"data": "y" * 40})

    assert len(store) == 1
    assert store.oldest_event_id == second.id
    assert store.latest_event_id == second.id

    with pytest.raises(StaleCursorError):
        store.subscribe(first.id)


@pytest.mark.anyio
async def test_oversized_event_is_delivered_live_then_evicted():
    store = BoundedEventStore(max_events=10, max_bytes=64)
    subscription = store.subscribe(None)

    async def publish_later():
        await anyio.sleep(0)
        await store.publish({"data": "x" * 100})

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(publish_later)
        payload = await drain(subscription)
        task_group.cancel_scope.cancel()

    assert b"x" * 100 in payload
    assert len(store) == 0
    assert store.latest_event_id == "1"
    assert store.oldest_event_id is None

    with pytest.raises(StaleCursorError):
        store.subscribe("1")


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("cursor", "error"),
    [
        ("unknown", UnknownCursorError),
        ("99", UnknownCursorError),
        ("0", UnknownCursorError),
        ("-1", UnknownCursorError),
        ("1abc", UnknownCursorError),
        (True, UnknownCursorError),
    ],
)
async def test_invalid_cursors_are_rejected_before_streaming(cursor, error):
    store = BoundedEventStore(max_events=2, max_bytes=10_000)
    await store.publish({"data": "one"})

    with pytest.raises(error):
        store.subscribe(cursor)


@pytest.mark.anyio
async def test_empty_cursor_replays_full_history_then_live_events():
    store = BoundedEventStore(max_events=10, max_bytes=10_000)
    await store.publish({"data": "one"})
    await store.publish({"data": "two"})

    subscription = store.subscribe(None)
    assert subscription.status is CursorStatus.EMPTY

    async def publish_later():
        await anyio.sleep(0)
        await store.publish({"data": "three"})

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(publish_later)
        payload = b"".join(await collect_live(subscription, 3))
        task_group.cancel_scope.cancel()

    assert sse_data(payload) == ["one", "two", "three"]


@pytest.mark.anyio
async def test_empty_string_cursor_is_treated_as_initial_request():
    store = BoundedEventStore(max_events=10, max_bytes=10_000)
    await store.publish({"data": "one"})

    subscription = store.subscribe("")
    assert subscription.status is CursorStatus.EMPTY
    assert subscription.cursor is None

    async def publish_later():
        await anyio.sleep(0)
        await store.publish({"data": "two"})

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(publish_later)
        payload = await drain(subscription)
        task_group.cancel_scope.cancel()

    assert sse_data(payload) == ["one", "two"]


@pytest.mark.anyio
async def test_current_cursor_has_no_replay_and_receives_new_events():
    store = BoundedEventStore(max_events=10, max_bytes=10_000)
    event = await store.publish({"data": "one"})
    subscription = store.subscribe(event.id)

    assert subscription.status is CursorStatus.CURRENT
    assert subscription.cursor == event.id

    async def publish_later():
        await anyio.sleep(0)
        await store.publish({"data": "two"})

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(publish_later)
        payload = await drain(subscription)
        task_group.cancel_scope.cancel()

    assert sse_data(payload) == ["two"]


@pytest.mark.anyio
async def test_event_published_at_replay_end_is_delivered_exactly_once():
    store = BoundedEventStore(max_events=10, max_bytes=10_000)
    await store.publish({"data": "one"})
    await store.publish({"data": "two"})
    release_handoff = anyio.Event()
    handoff_started = anyio.Event()

    async def before_live():
        handoff_started.set()
        await release_handoff.wait()

    async def publish_during_handoff():
        await handoff_started.wait()
        await store.publish({"data": "handoff"})
        release_handoff.set()

    subscription = store.subscribe("1", before_live=before_live)

    async def consume():
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(publish_during_handoff)
            messages = await collect_live(subscription, 2)
            task_group.cancel_scope.cancel()
            return b"".join(messages)

    payload = await consume()

    assert sse_data(payload) == ["two", "handoff"]
    assert sse_ids(payload) == [b"2", b"3"]


@pytest.mark.anyio
async def test_heartbeat_comment_is_live_only_and_does_not_advance_cursor():
    store = BoundedEventStore(max_events=10, max_bytes=10_000)
    business_event = await store.publish({"data": "one"})
    subscription = store.subscribe(business_event.id)

    async def publish_heartbeat_then_event():
        await anyio.sleep(0)
        assert await store.publish(ServerSentEvent(comment="ping")) is None
        await store.publish({"data": "two"})

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(publish_heartbeat_then_event)
        payload = await drain(subscription)
        task_group.cancel_scope.cancel()

    assert b": ping" in payload
    assert sse_data(payload) == ["two"]
    assert sse_ids(payload) == [b"2"]
    assert store.latest_event_id == "2"
    assert len(store) == 2

    replay = store.subscribe(business_event.id)
    replay_payload = await drain(replay)
    assert b": ping" not in replay_payload


@pytest.mark.anyio
async def test_unsubscribed_receiver_does_not_stop_publication():
    store = BoundedEventStore(max_events=10, max_bytes=10_000)
    await store.publish({"data": "one"})
    subscription = store.subscribe(None)

    async def consume_one():
        async for _ in subscription:
            return

    await consume_one()

    second = await store.publish({"data": "two"})
    third = await store.publish({"data": "three"})

    assert second.id == "2"
    assert third.id == "3"


@pytest.mark.anyio
async def test_slow_subscriber_buffer_is_bounded_and_other_subscribers_continue():
    store = BoundedEventStore(max_events=2, max_bytes=10_000)
    slow = store.subscribe(None)
    healthy = store.subscribe(None)

    async def publish_all():
        for number in range(3):
            await anyio.sleep(0)
            await store.publish({"data": number})

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(publish_all)
        messages = await collect_live(healthy, 3)
        task_group.cancel_scope.cancel()

    assert slow._send_stream not in store._subscribers
    assert healthy._send_stream in store._subscribers
    assert len(messages) == 3


@pytest.mark.anyio
async def test_store_owns_event_ids_to_keep_them_unique_and_ordered():
    store = BoundedEventStore(max_events=10, max_bytes=10_000)
    first = await store.publish(ServerSentEvent(data="one", id="bad"))

    assert first.id == "1"

    second = await store.publish(ServerSentEvent(data="two", id="ignored"))
    assert second.id == "2"


@pytest.mark.anyio
async def test_adapter_returns_409_before_200_for_stale_cursor():
    store = BoundedEventStore(max_events=1, max_bytes=10_000)
    await store.publish({"data": "one"})
    await store.publish({"data": "two"})
    response = ResumableEventSourceResponse.from_cursor(
        store, last_event_id="1"
    )
    messages = []

    async def send(message):
        messages.append(message)

    async def receive():
        await anyio.lowlevel.checkpoint()
        return {"type": "http.disconnect"}

    await response(
        {"type": "http", "method": "GET", "headers": []},
        receive,
        send,
    )

    assert messages[0]["status"] == 409
    headers = dict(messages[0]["headers"])
    assert headers[b"content-type"] == b"text/plain; charset=utf-8"


@pytest.mark.anyio
async def test_adapter_returns_400_for_unknown_cursor():
    store = BoundedEventStore(max_events=10, max_bytes=10_000)
    response = ResumableEventSourceResponse.from_cursor(
        store, last_event_id="unknown"
    )
    messages = []

    async def send(message):
        messages.append(message)

    async def receive():
        await anyio.lowlevel.checkpoint()
        return {"type": "http.disconnect"}

    await response(
        {"type": "http", "method": "GET", "headers": []},
        receive,
        send,
    )

    assert messages[0]["status"] == 400
    assert (b"cache-control", b"no-store") in messages[0]["headers"]


@pytest.mark.anyio
async def test_adapter_reads_last_event_id_request_header():
    store = BoundedEventStore(max_events=10, max_bytes=10_000)
    await store.publish({"data": "one"})
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "headers": [(b"last-event-id", b"1")],
        }
    )

    response = ResumableEventSourceResponse.from_request(
        store, request, ping=0
    )

    assert response.resume_status is CursorStatus.CURRENT
    assert response.resume_cursor == "1"


@pytest.mark.anyio
async def test_adapter_current_cursor_returns_200_without_replayed_body():
    store = BoundedEventStore(max_events=10, max_bytes=10_000)
    await store.publish({"data": "one"})
    response = ResumableEventSourceResponse.from_cursor(
        store, last_event_id="1", ping=0
    )
    messages = []

    async def send(message):
        messages.append(message)

    async def receive():
        await anyio.sleep(0.01)
        return {"type": "http.disconnect"}

    await response(
        {"type": "http", "method": "GET", "headers": []},
        receive,
        send,
    )

    assert response.status_code == 200
    assert response.resume_status is CursorStatus.CURRENT
    assert messages[0]["status"] == 200
    assert [message["body"] for message in messages if message.get("body")] == []


@pytest.mark.anyio
async def test_adapter_empty_cursor_replays_history_with_200():
    store = BoundedEventStore(max_events=10, max_bytes=10_000)
    await store.publish({"data": "one"})
    response = ResumableEventSourceResponse.from_cursor(store, ping=0)
    messages = []

    async def send(message):
        messages.append(message)

    async def receive():
        await anyio.sleep(0.01)
        return {"type": "http.disconnect"}

    await response(
        {"type": "http", "method": "GET", "headers": []},
        receive,
        send,
    )

    assert response.status_code == 200
    assert response.resume_status is CursorStatus.EMPTY
    assert messages[0]["status"] == 200
    assert b"data: one" in b"".join(
        message["body"] for message in messages if message.get("body")
    )
