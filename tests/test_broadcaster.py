import asyncio
import json
from contextlib import suppress

import anyio
import pytest

from sse_starlette import (
    EventBroadcaster,
    EventSourceResponse,
    LastSubscriberPolicy,
    SubscriptionLagged,
)
from tests.anyio_compat import collapse_excgroups


class CountingSource:
    def __init__(self, count: int):
        self.count = count
        self.started = False
        self.closed = False

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        self.started = True
        try:
            for sequence in range(self.count):
                yield sequence
        finally:
            self.closed = True


async def test_publish_whenSubscribersHaveDifferentBacklogs_thenOnlySlowOneLagged():
    broadcaster = EventBroadcaster()
    slow = broadcaster.subscribe(backlog=1)
    fast = broadcaster.subscribe(backlog=1)

    slow_first = asyncio.create_task(slow.get())
    await asyncio.sleep(0)
    received = []
    for sequence in range(3):
        await broadcaster.publish(sequence)
        received.append(await fast.get())
        if sequence == 0:
            assert await slow_first == 0

    assert received == [0, 1, 2]
    assert await slow.get() == 1
    lag = await slow.get()

    assert isinstance(lag, SubscriptionLagged)
    assert lag.as_dict() == {
        "reason": "lagged",
        "last_sequence": 1,
        "first_missing_sequence": 2,
        "latest_sequence": 2,
        "backlog": 1,
    }
    assert broadcaster.subscriber_count == 1


def test_subscribe_whenBacklogInvalid_thenRaises():
    broadcaster = EventBroadcaster()

    with pytest.raises(ValueError, match="backlog"):
        broadcaster.subscribe(backlog=-1)


async def test_receive_nowait_whenNoEvent_thenRaisesQueueEmpty():
    subscription = EventBroadcaster().subscribe()

    with pytest.raises(asyncio.QueueEmpty):
        subscription.receive_nowait()


async def test_get_nowait_and_blocking_get_consumeSameNumberedBatch():
    broadcaster = EventBroadcaster()
    blocking = broadcaster.subscribe(backlog=2)
    immediate = broadcaster.subscribe(backlog=2)

    for sequence in (0, 1):
        assert broadcaster.publish_nowait(sequence) == sequence

    assert blocking.get_nowait() == 0
    assert immediate.get_nowait() == 0
    assert immediate.get_nowait() == 1
    assert await blocking.get() == 1

    broadcaster.publish_nowait(2)
    assert await blocking.get() == 2
    assert immediate.get_nowait() == 2


async def test_cancel_whenWaitingWithoutSource_thenOnlyThatSubscriptionRemoved():
    broadcaster = EventBroadcaster()
    first = broadcaster.subscribe()
    second = broadcaster.subscribe()
    healthy = broadcaster.subscribe()
    waiting = asyncio.create_task(first.get())
    await asyncio.sleep(0)

    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    assert broadcaster.subscriber_count == 2
    assert not broadcaster.closed
    await second.aclose()
    await healthy.aclose()
    assert broadcaster.subscriber_count == 0


async def test_unsubscribe_whenLastSubscriberLeavesWithStopPolicy_thenSourceCloses():
    source = CountingSource(10)
    broadcaster = EventBroadcaster.from_async_iterable(
        source,
        on_last_subscriber=LastSubscriberPolicy.STOP_SOURCE,
    )
    subscription = broadcaster.subscribe(backlog=5)
    await asyncio.sleep(0)

    assert source.started
    await subscription.aclose()
    await asyncio.sleep(0)

    assert broadcaster.closed
    assert source.closed


async def test_lag_whenOnlySubscriberFallsBehindWithStopPolicy_thenSourceCloses():
    source = CountingSource(10)
    broadcaster = EventBroadcaster.from_async_iterable(
        source,
        on_last_subscriber=LastSubscriberPolicy.STOP_SOURCE,
    )
    subscription = broadcaster.subscribe(backlog=0)
    await asyncio.sleep(0)

    lag = await subscription.get()

    assert isinstance(lag, SubscriptionLagged)
    assert broadcaster.subscriber_count == 0
    await asyncio.sleep(0)
    assert broadcaster.closed
    assert source.closed


async def test_unsubscribe_whenLastSubscriberLeavesWithKeepPolicy_thenSourceSurvives():
    resumed = asyncio.Event()

    async def source():
        yield 0
        resumed.set()
        await anyio.sleep_forever()

    broadcaster = EventBroadcaster(
        source(),
        on_last_subscriber=LastSubscriberPolicy.KEEP_SOURCE,
    )
    subscription = broadcaster.subscribe(backlog=5)
    assert await subscription.get() == 0
    await resumed.wait()

    await subscription.aclose()
    await asyncio.sleep(0)

    assert not broadcaster.closed
    await broadcaster.aclose(close_source=False)
    assert broadcaster.closed

async def test_source_whenCompletes_thenSubscriptionsEndInOrder():
    async def source():
        for event in ("a", "b", "c"):
            yield event

    broadcaster = EventBroadcaster(
        source(),
        on_last_subscriber=LastSubscriberPolicy.KEEP_SOURCE,
    )
    first = broadcaster.subscribe()
    second = broadcaster.subscribe()

    assert [await first.get() for _ in range(3)] == ["a", "b", "c"]
    assert [await second.get() for _ in range(3)] == ["a", "b", "c"]
    with pytest.raises(StopAsyncIteration):
        await first.get()
    with pytest.raises(StopAsyncIteration):
        await second.get()


async def test_source_whenRaises_thenSubscriberReceivesSameError():
    async def source():
        yield "before"
        raise RuntimeError("source failed")

    broadcaster = EventBroadcaster(
        source(),
        on_last_subscriber=LastSubscriberPolicy.KEEP_SOURCE,
    )
    subscription = broadcaster.subscribe()
    assert await subscription.get() == "before"

    with pytest.raises(RuntimeError, match="source failed"):
        await subscription.get()


async def test_source_whenPolicyOmitted_thenRequiresExplicitChoice():
    async def source():
        if False:
            yield

    with pytest.raises(TypeError, match="on_last_subscriber"):
        EventBroadcaster(source())


async def test_event_source_response_whenSendFails_thenReleasesSubscription():
    broadcaster = EventBroadcaster()
    subscription = broadcaster.subscribe(backlog=5)
    healthy = broadcaster.subscribe(backlog=5)
    await broadcaster.publish("queued")

    async def send(message):
        if message["type"] == "http.response.start":
            return
        raise RuntimeError("client send failed")

    async def receive():
        await anyio.sleep_forever()

    response = EventSourceResponse(subscription, ping=0)
    with pytest.raises(RuntimeError, match="client send failed"):
        with collapse_excgroups():
            await response(
                {"type": "http", "method": "GET", "headers": []}, receive, send
            )

    assert broadcaster.subscriber_count == 1
    assert await healthy.get() == "queued"
    assert broadcaster.publish_nowait("still-delivered") == 1
    assert await healthy.get() == "still-delivered"
    await healthy.aclose()
    assert broadcaster.subscriber_count == 0


async def test_event_source_responses_withBlockingAndInstantSends():
    first_dispatched = asyncio.Event()
    slow_is_blocked = asyncio.Event()
    release_slow = asyncio.Event()
    fast_acknowledged = [asyncio.Event() for _ in range(3)]

    async def source():
        yield 0
        first_dispatched.set()
        await slow_is_blocked.wait()
        await fast_acknowledged[0].wait()
        yield 1
        await fast_acknowledged[1].wait()
        yield 2
        release_slow.set()
        await fast_acknowledged[2].wait()
        yield 3
        yield 4

    broadcaster = EventBroadcaster(
        source(),
        on_last_subscriber=LastSubscriberPolicy.KEEP_SOURCE,
    )
    slow_subscription = broadcaster.subscribe(backlog=1)
    fast_subscription = broadcaster.subscribe(backlog=4)
    slow_bodies = []
    fast_bodies = []

    async def instant_send(message):
        if message["type"] == "http.response.body" and message["body"]:
            fast_bodies.append(message["body"])
            if message["body"].startswith(b"data: "):
                sequence = int(message["body"].split(b"\r\n", 1)[0][6:])
                if sequence < len(fast_acknowledged):
                    fast_acknowledged[sequence].set()

    async def blocking_send(message):
        if message["type"] == "http.response.body" and message["body"]:
            slow_bodies.append(message["body"])
            if b"event: subscription_lagged" in message["body"]:
                return
            if len(slow_bodies) == 1:
                slow_is_blocked.set()
                await release_slow.wait()

    async def never_receives():
        await anyio.sleep_forever()

    scope = {"type": "http", "method": "GET", "headers": []}
    fast_response = EventSourceResponse(fast_subscription, ping=0)
    slow_response = EventSourceResponse(slow_subscription, ping=0)

    async def run_fast():
        await fast_response(scope, never_receives, instant_send)

    async def run_slow():
        await first_dispatched.wait()
        await slow_response(scope, never_receives, blocking_send)

    fast_task = asyncio.create_task(run_fast())
    slow_task = asyncio.create_task(run_slow())
    await slow_is_blocked.wait()

    with anyio.fail_after(1):
        while len(fast_bodies) < 5:
            await asyncio.sleep(0)
    await fast_task
    with anyio.fail_after(1):
        while len(slow_bodies) < 2:
            await asyncio.sleep(0)

    slow_task.cancel()
    with suppress(anyio.get_cancelled_exc_class()):
        await slow_task

    assert len(fast_bodies) == 5
    assert all(f"data: {index}" in fast_bodies[index].decode() for index in range(5))
    assert len(slow_bodies) == 3
    assert b"data: 0\r\n\r\n" in slow_bodies[0]
    assert b"data: 1\r\n\r\n" in slow_bodies[1]
    assert b"event: subscription_lagged" in slow_bodies[2]
    lag_payload = json.loads(
        slow_bodies[2]
        .decode()
        .split("data: ", 1)[1]
        .removesuffix("\r\n\r\n")
    )
    assert lag_payload == {
        "reason": "lagged",
        "last_sequence": 1,
        "first_missing_sequence": 2,
        "latest_sequence": 2,
        "backlog": 1,
    }
    assert broadcaster.subscriber_count == 0
