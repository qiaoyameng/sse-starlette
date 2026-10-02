import asyncio

import pytest

from sse_starlette import (
    BroadcasterClosedError,
    EventBroadcaster,
    EventSourceResponse,
    LaggedError,
)


async def test_publish_whenOneSubscriberBlocks_thenFastGetsAllAndSlowKnowsTheGap():
    # The scenario from the requirements: one immediate ("send") consumer
    # and one blocked consumer read the same batch of numbered events.
    broadcaster = EventBroadcaster(when_empty="keep_open")
    fast = broadcaster.subscribe(max_backlog=16)
    slow = broadcaster.subscribe(max_backlog=2)

    fast_received = []

    async def consume_fast():
        async for item in fast:
            fast_received.append(item)

    unblock_slow = asyncio.Event()
    slow_received = []
    slow_lagged = None

    async def consume_slow():
        nonlocal slow_lagged
        await unblock_slow.wait()  # stays blocked while events are published
        try:
            async for item in slow:
                slow_received.append(item)
        except LaggedError as exc:
            slow_lagged = exc

    fast_task = asyncio.create_task(consume_fast())
    slow_task = asyncio.create_task(consume_slow())
    await asyncio.sleep(0)

    total = 10
    for i in range(total):
        broadcaster.publish(i)
        await asyncio.sleep(0)  # let the fast consumer drain each event

    unblock_slow.set()
    broadcaster.close()
    await asyncio.gather(fast_task, slow_task)

    # The fast consumer received the complete, ordered batch.
    assert fast_received == list(range(total))
    assert fast.last_delivered_seq == total - 1

    # The slow consumer received its buffered prefix, then a terminal
    # LaggedError that explains exactly where it fell behind.
    assert slow_received == [0, 1]
    assert slow_lagged is not None
    assert slow_lagged.first_missed_seq == 2
    assert slow_lagged.missed_count == total - 2
    assert slow_lagged.last_delivered_seq == 1
    assert "2" in str(slow_lagged)


async def test_publish_whenMultipleHealthySubscribers_thenAllSeeIdenticalOrder():
    broadcaster = EventBroadcaster(when_empty="keep_open")
    subscriptions = [broadcaster.subscribe(max_backlog=32) for _ in range(3)]
    received = [[] for _ in subscriptions]

    async def consume(subscription, out):
        async for item in subscription:
            out.append(item)

    tasks = [
        asyncio.create_task(consume(subscription, out))
        for subscription, out in zip(subscriptions, received)
    ]
    await asyncio.sleep(0)

    seqs = [broadcaster.publish(i) for i in range(20)]
    broadcaster.close()
    await asyncio.gather(*tasks)

    assert seqs == list(range(20))
    for subscription, out in zip(subscriptions, received):
        assert out == list(range(20))
        assert subscription.last_delivered_seq == 19


async def test_unsubscribe_whenWaiting_thenStopsAndPeersUnaffected():
    broadcaster = EventBroadcaster(when_empty="keep_open")
    leaving = broadcaster.subscribe(max_backlog=4)
    survivor = broadcaster.subscribe(max_backlog=4)

    pending = asyncio.create_task(anext(leaving))
    await asyncio.sleep(0)
    leaving.unsubscribe()

    with pytest.raises(StopAsyncIteration):
        await pending
    assert leaving.released
    assert broadcaster.subscriber_count == 1

    broadcaster.publish("still here")
    assert await asyncio.wait_for(anext(survivor), timeout=1) == "still here"


async def test_anext_whenCancelled_thenOnlyThatSubscriptionIsReleased():
    # Cancellation is how EventSourceResponse tears down a stream on client
    # disconnect; it must release only the cancelled subscription.
    broadcaster = EventBroadcaster(when_empty="keep_open")
    cancelled = broadcaster.subscribe(max_backlog=4)
    survivor = broadcaster.subscribe(max_backlog=4)

    pending = asyncio.create_task(anext(cancelled))
    await asyncio.sleep(0)
    pending.cancel()

    with pytest.raises(asyncio.CancelledError):
        await pending
    assert cancelled.released
    assert broadcaster.subscriber_count == 1

    broadcaster.publish("after cancel")
    assert await asyncio.wait_for(anext(survivor), timeout=1) == "after cancel"


async def test_aclose_whenSendFails_thenOnlyThatSubscriptionIsReleased():
    # EventSourceResponse calls aclose() on its body iterator after a send
    # timeout; the failed send must not disturb other subscribers.
    broadcaster = EventBroadcaster(when_empty="keep_open")
    failed = broadcaster.subscribe(max_backlog=4)
    survivor = broadcaster.subscribe(max_backlog=4)

    await failed.aclose()
    await failed.aclose()  # idempotent

    assert failed.released
    assert broadcaster.subscriber_count == 1

    broadcaster.publish("after failure")
    assert await asyncio.wait_for(anext(survivor), timeout=1) == "after failure"


async def test_lastSubscriberLeaves_whenPolicyClose_thenBroadcasterStopsSource():
    on_close_calls = []
    broadcaster = EventBroadcaster(
        when_empty="close", on_close=lambda: on_close_calls.append(1)
    )
    first = broadcaster.subscribe(max_backlog=2)
    second = broadcaster.subscribe(max_backlog=2)

    first.unsubscribe()
    assert not broadcaster.closed
    assert on_close_calls == []

    second.unsubscribe()
    assert broadcaster.closed
    assert on_close_calls == [1]

    broadcaster.close()  # idempotent: on_close must not fire twice
    assert on_close_calls == [1]

    with pytest.raises(BroadcasterClosedError):
        broadcaster.subscribe(max_backlog=1)
    with pytest.raises(BroadcasterClosedError):
        broadcaster.publish("too late")


async def test_lastSubscriberLags_whenPolicyClose_thenBroadcasterStopsSource():
    # A lag-driven release is still "the last subscriber left".
    on_close_calls = []
    broadcaster = EventBroadcaster(
        when_empty="close", on_close=lambda: on_close_calls.append(1)
    )
    subscription = broadcaster.subscribe(max_backlog=1)

    broadcaster.publish(0)
    broadcaster.publish(1)  # overflows the backlog of 1

    assert await anext(subscription) == 0
    with pytest.raises(LaggedError):
        await anext(subscription)

    assert broadcaster.closed
    assert on_close_calls == [1]


async def test_lastSubscriberLeaves_whenPolicyKeepOpen_thenBroadcasterStaysOpen():
    broadcaster = EventBroadcaster(when_empty="keep_open")
    first = broadcaster.subscribe(max_backlog=2)
    assert broadcaster.publish("a") == 0
    first.unsubscribe()

    assert not broadcaster.closed
    assert broadcaster.subscriber_count == 0

    # Late subscribers can still join; the sequence keeps increasing.
    late = broadcaster.subscribe(max_backlog=2)
    assert broadcaster.publish("b") == 1
    assert await asyncio.wait_for(anext(late), timeout=1) == "b"


async def test_close_whenSubscribersWaiting_thenStreamsDrainAndEndNormally():
    broadcaster = EventBroadcaster(when_empty="keep_open")
    subscription = broadcaster.subscribe(max_backlog=4)

    broadcaster.publish("a")
    broadcaster.close()

    assert [item async for item in subscription] == ["a"]


def test_init_whenWhenEmptyInvalid_thenRaisesValueError():
    with pytest.raises(ValueError, match="when_empty"):
        EventBroadcaster(when_empty="sometimes")


@pytest.mark.parametrize("max_backlog", [0, -1])
def test_subscribe_whenMaxBacklogNotPositive_thenRaisesValueError(max_backlog):
    # max_backlog=0 would mean unbounded in queue terms, reintroducing the
    # unbounded-memory problem the backlog limit exists to prevent.
    broadcaster = EventBroadcaster(when_empty="keep_open")
    with pytest.raises(ValueError, match="max_backlog"):
        broadcaster.subscribe(max_backlog=max_backlog)


async def test_subscription_whenUsedAsEventSourceResponseContent_thenStreamsEvents():
    broadcaster = EventBroadcaster(when_empty="keep_open")
    subscription = broadcaster.subscribe(max_backlog=8)
    broadcaster.publish({"data": "hello", "event": "greeting"})
    broadcaster.publish({"data": "world"})
    broadcaster.close()

    sent = []

    async def send(message):
        sent.append(message)

    async def receive():
        await asyncio.sleep(3600)  # client never disconnects

    scope = {"type": "http", "method": "GET", "path": "/", "headers": []}
    # ping=0: keep the asserted body independent of heartbeat timing
    response = EventSourceResponse(subscription, ping=0)
    await asyncio.wait_for(response(scope, receive, send), timeout=5)

    start = next(m for m in sent if m["type"] == "http.response.start")
    assert start["status"] == 200
    body = b"".join(
        m.get("body", b"") for m in sent if m["type"] == "http.response.body"
    )
    assert b"event: greeting" in body
    assert b"data: hello" in body
    assert b"data: world" in body
