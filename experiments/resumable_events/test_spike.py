from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack

import pytest
from conftest import blocked_connections, eventually
from redis.asyncio.connection import Connection
from redis.exceptions import ResponseError
from relay import Relay, SlowConsumer
from store import Conflict, Expired, HistoryUnavailable, Inconsistent, Sealed, Store


def names(store, *, each=1):
    found = {bucket: [] for bucket in range(store.buckets)}
    for n in range(10000):
        stream = f"chat:{n}"
        bucket = store.bucket(stream)
        if len(found[bucket]) < each:
            found[bucket].append(stream)
        if all(len(values) == each for values in found.values()):
            return found
    raise AssertionError("Could not populate test buckets")


async def append(store, stream, n, *, final=False):
    method = store.finish if final else store.append
    return await method(
        stream,
        event="done" if final else "message",
        data={"n": n},
        producer="writer",
        idempotency_key=str(n),
    )


async def collect(iterator, count):
    return [await asyncio.wait_for(anext(iterator), timeout=5) for _ in range(count)]


async def test_create_concurrently_with_fixed_deadline(deployment):
    store, *_ = deployment
    await asyncio.gather(*(store.create("chat:1", ttl=60) for _ in range(8)))
    key, meta = store.keys("chat:1")
    original = await store.client.hget(meta, "deadline")
    assert 0 < await store.client.pttl(key) <= 60000
    await store.create("chat:1", ttl=60)
    assert await store.client.hget(meta, "deadline") == original
    with pytest.raises(Conflict):
        await store.create("chat:1", ttl=120)


async def test_creation_rejects_invalid_and_partial_state(deployment):
    store, *_ = deployment
    with pytest.raises(ValueError):
        await store.create("invalid", ttl=0)
    key, meta = store.keys("partial")
    await store.client.xadd(key, {"bootstrap": "1"})
    with pytest.raises(Inconsistent):
        await store.create("partial", ttl=60)
    assert not await store.client.exists(meta)


async def test_native_dedup_concurrent_retry_and_finish(deployment):
    store, *_ = deployment
    await store.create("chat:1", ttl=60)
    ids = await asyncio.gather(*(append(store, "chat:1", 1) for _ in range(8)))
    assert len(set(ids)) == 1
    assert len((await store.read("chat:1")).events) == 1
    key, meta = store.keys("chat:1")
    deadline = await store.client.hget(meta, "deadline")
    finals = await asyncio.gather(
        *(append(store, "chat:1", 2, final=True) for _ in range(8))
    )
    assert len(set(finals)) == 1
    batch = await store.read("chat:1")
    assert batch.sealed and [e.final for e in batch.events] == [False, True]
    assert await store.client.hget(meta, "deadline") == deadline
    assert 0 < await store.client.pttl(key) <= 60000
    with pytest.raises(Sealed):
        await append(store, "chat:1", 3)


async def test_finish_races_append_and_rejects_reused_identity(deployment):
    store, *_ = deployment
    await store.create("chat:1", ttl=60)
    original = await append(store, "chat:1", 0)
    with pytest.raises(Conflict):
        await append(store, "chat:1", 0, final=True)
    assert not (await store.read("chat:1")).sealed
    results = await asyncio.gather(
        append(store, "chat:1", 1),
        append(store, "chat:1", 2, final=True),
        return_exceptions=True,
    )
    assert isinstance(results[1], str)
    assert isinstance(results[0], (str, Sealed))
    batch = await store.read("chat:1")
    assert batch.events[0].id == original
    assert batch.events[-1].final
    assert sum(e.final for e in batch.events) == 1


async def test_replay_retention_floor_and_atomic_trim(deployment):
    store, *_ = deployment
    await store.create("chat:1", ttl=60)
    ids = [await append(store, "chat:1", n) for n in range(6)]
    assert [e.id for e in (await store.read("chat:1", ids[2])).events] == ids[3:]
    await store.trim_through("chat:1", ids[2])
    with pytest.raises(HistoryUnavailable):
        await store.read("chat:1", ids[1])
    assert [e.id for e in (await store.read("chat:1", ids[2])).events] == ids[3:]
    assert await append(store, "chat:1", 0) == ids[0]
    # Dedup may return a cursor for an already trimmed payload; no resurrection.
    assert [e.id for e in (await store.read("chat:1", ids[2])).events] == ids[3:]


async def test_expiry_is_distinct_from_missing_payload(deployment):
    store, *_ = deployment
    await store.create("expiring", ttl=1)
    key, _ = store.keys("expiring")

    async def gone():
        return not await store.client.exists(key)

    await eventually(gone, timeout=3)
    with pytest.raises(Expired):
        await store.read("expiring")
    with pytest.raises(Expired):
        await append(store, "expiring", 1)
    await store.create("lost", ttl=60)
    await store.client.delete(store.keys("lost")[0])
    with pytest.raises(Inconsistent):
        await store.read("lost")


async def test_exec_error_does_not_rollback_other_commands(deployment):
    """Evidence for why a separate seal flag cannot rely on EXEC rollback."""
    store, *_ = deployment
    key, meta = store.keys("injected")
    await store.client.set(key, "wrong-type")
    async with store.client.pipeline(transaction=True) as pipe:
        pipe.xadd(key, {"final": "1"})
        pipe.hset(meta, "sealed", "1")
        reply = await pipe.execute(raise_on_error=False)
    assert isinstance(reply[0], ResponseError)
    assert await store.client.hget(meta, "sealed") == "1"
    with pytest.raises(Inconsistent):
        await store.read("injected")


async def test_bucket_placement_and_layout_mismatch(deployment, topology):
    store, *_ = deployment
    samples = [values[0] for values in names(store).values()]
    samples += ["tenant:{a}:chat:1", "tenant:a:chat:{1}", "chat:é"]
    keys = [store.keys(name)[0] for name in samples]
    assert len(keys) == len(set(keys))
    if topology == "cluster":
        slots = [await store.client.cluster_keyslot(key) for key in keys[:4]]
        assert len(set(slots)) == 4
        nodes = [store.client.get_node_from_key(key).name for key in keys[:4]]
        assert len(set(nodes)) >= 2
    other = Store(store.client, store.prefix, buckets=8)
    with pytest.raises(Conflict):
        await other.initialize()


async def test_fixed_blocking_connections_and_cleanup(deployment):
    store, blocking, ports, relay_name = deployment
    streams = [s for group in names(store, each=2).values() for s in group]
    for stream in streams:
        await store.create(stream, ttl=60)

    async def all_blocked():
        return await blocked_connections(store, ports, relay_name) == store.buckets

    async with Relay(store, blocking) as relay:
        async with AsyncExitStack() as stack:
            first = {}
            for stream in streams[::2]:
                first[stream] = await stack.enter_async_context(relay.subscribe(stream))
            await eventually(all_blocked)
            # More clients AND more streams, across already active buckets.
            readers = []
            for stream in streams:
                for _ in range(10):
                    readers.append(
                        (
                            stream,
                            await stack.enter_async_context(relay.subscribe(stream)),
                        )
                    )
            await eventually(all_blocked)
            # Independent command pool remains able to publish while all reads block.
            ids = {stream: await append(store, stream, 1) for stream in streams}
            for stream, reader in readers:
                assert (await collect(reader, 1))[0].id == ids[stream]
            assert await blocked_connections(store, ports, relay_name) <= store.buckets

    async def none_blocked():
        return await blocked_connections(store, ports, relay_name) == 0

    await eventually(none_blocked)
    assert all(task.done() for task in relay.tasks)


async def test_handoff_and_gateway_reconnect_have_no_gaps(deployment):
    store, blocking, *_ = deployment
    stream = "chat:handoff"
    await store.create(stream, ttl=60)
    initial = [await append(store, stream, n) for n in range(40)]
    async with Relay(store, blocking) as relay:
        async with relay.subscribe(stream) as reader:
            # Registration occurred, but replay has not begun.
            concurrent = [await append(store, stream, n) for n in range(40, 50)]
            events = await collect(reader, 50)
            assert [e.id for e in events] == initial + concurrent
            saved = events[20].id
    offline = [await append(store, stream, n) for n in range(50, 55)]
    # Fresh process-local registry; no shared high-water mark.
    async with Relay(store, blocking) as another_gateway:
        async with another_gateway.subscribe(stream, after=saved) as reader:
            remaining = initial[21:] + concurrent + offline
            assert [e.id for e in await collect(reader, len(remaining))] == remaining


async def test_sealed_stream_replay_ends(deployment):
    store, blocking, *_ = deployment
    await store.create("chat:sealed", ttl=60)
    await append(store, "chat:sealed", 1)
    final = await append(store, "chat:sealed", 2, final=True)
    async with Relay(store, blocking) as relay:
        async with relay.subscribe("chat:sealed") as reader:
            events = await asyncio.wait_for(_all(reader), timeout=5)
            assert len(events) == 2 and events[-1].final
        async with relay.subscribe("chat:sealed", after=final) as reader:
            assert await asyncio.wait_for(_all(reader), timeout=5) == []


async def _all(reader):
    return [event async for event in reader]


async def test_slow_subscriber_does_not_block_same_bucket(deployment):
    store, blocking, *_ = deployment
    hot, quiet = names(store, each=2)[0]
    for stream in (hot, quiet):
        await store.create(stream, ttl=60)
    async with Relay(store, blocking, queue_events=2) as relay:
        async with relay.subscribe(hot) as slow, relay.subscribe(quiet) as healthy:
            for n in range(8):
                await append(store, hot, n)
            expected = await append(store, quiet, 9)
            assert (await collect(healthy, 1))[0].id == expected
            with pytest.raises(SlowConsumer):
                await asyncio.wait_for(anext(slow), timeout=5)


async def test_trim_during_registered_replay_reports_gap(deployment):
    store, blocking, *_ = deployment
    await store.create("chat:trim", ttl=60)
    ids = [await append(store, "chat:trim", n) for n in range(6)]
    async with Relay(store, blocking) as relay:
        async with relay.subscribe("chat:trim") as reader:
            await store.trim_through("chat:trim", ids[2])
            with pytest.raises(HistoryUnavailable):
                await asyncio.wait_for(anext(reader), timeout=5)


async def test_expiry_isolated_from_healthy_stream_in_same_bucket(deployment):
    store, blocking, *_ = deployment
    expiring, healthy = names(store, each=2)[0]
    await store.create(expiring, ttl=1)
    await store.create(healthy, ttl=60)
    async with Relay(store, blocking) as relay:
        async with (
            relay.subscribe(expiring) as expired,
            relay.subscribe(healthy) as reader,
        ):
            with pytest.raises(Expired):
                await asyncio.wait_for(anext(expired), timeout=4)
            expected = await append(store, healthy, 1)
            assert (await collect(reader, 1))[0].id == expected


async def test_byte_limit_disconnects_and_replay_recovers(deployment):
    store, blocking, *_ = deployment
    await store.create("large", ttl=60)
    async with Relay(store, blocking, queue_bytes=64) as relay:
        async with relay.subscribe("large") as reader:
            expected = await append(store, "large", 1)
            with pytest.raises(SlowConsumer):
                await asyncio.wait_for(anext(reader), timeout=5)
        # Payload persisted despite overflow; no loss on a new subscription.
        async with relay.subscribe("large") as reader:
            assert (await collect(reader, 1))[0].id == expected


async def test_replay_from_floor_when_entire_open_history_was_trimmed(deployment):
    store, blocking, *_ = deployment
    await store.create("trim-all", ttl=60)
    last = await append(store, "trim-all", 1)
    await store.trim_through("trim-all", last)
    assert (await store.read("trim-all", last)).events == []
    async with Relay(store, blocking) as relay:
        async with relay.subscribe("trim-all", after=last) as reader:
            expected = await append(store, "trim-all", 2)
            assert (await collect(reader, 1))[0].id == expected


async def test_disconnect_before_exec_and_lost_finish_reply(deployment, topology):
    store, _, ports, _ = deployment
    if not ports:
        pytest.skip("Raw socket fault injection requires a local owned Redis node")
    await store.create("lost-reply", ttl=60)
    key, meta = store.keys("lost-reply")
    port = (
        store.client.get_node_from_key(key).port if topology == "cluster" else ports[0]
    )

    async def queued_finish():
        connection = Connection(host="127.0.0.1", port=port, decode_responses=True)
        await connection.connect()
        await connection.send_command("WATCH", key, meta)
        assert await connection.read_response() == "OK"
        await connection.send_command("MULTI")
        assert await connection.read_response() == "OK"
        await connection.send_command(
            "XADD",
            key,
            "NOMKSTREAM",
            "IDMP",
            "writer",
            "finish",
            "*",
            "event",
            "done",
            "data",
            "{}",
            "producer",
            "writer",
            "iid",
            "finish",
            "final",
            "1",
        )
        assert await connection.read_response() == "QUEUED"
        return connection

    before = await queued_finish()
    await before.disconnect()
    assert not (await store.read("lost-reply")).events

    after = await queued_finish()
    try:
        await after.send_command("EXEC")

        async def committed():
            return (await store.read("lost-reply")).sealed

        await eventually(committed)
        # Drop the connection without reading EXEC's committed response.
    finally:
        await after.disconnect()
    result = await store.finish(
        "lost-reply",
        event="done",
        data={},
        producer="writer",
        idempotency_key="finish",
    )
    events = (await store.read("lost-reply")).events
    assert len(events) == 1 and events[0].id == result and events[0].final


async def test_hot_stream_cannot_starve_same_bucket_with_one_entry_budget(deployment):
    store, blocking, *_ = deployment
    hot, quiet = names(store, each=2)[0]
    for stream in (hot, quiet):
        await store.create(stream, ttl=60)
    async with Relay(store, blocking, maxcount=1, queue_events=256) as relay:
        async with relay.subscribe(hot), relay.subscribe(quiet) as reader:
            # Commit a backlog at once, so the relay cannot drain it while it
            # is being produced. Both keys have the same bucket hash tag.
            async with store.client.pipeline(transaction=True) as pipe:
                for n in range(100):
                    pipe.xadd(
                        store.keys(hot)[0],
                        {
                            "event": "message",
                            "data": "{}",
                            "producer": "writer",
                            "iid": str(n),
                            "final": "0",
                        },
                        nomkstream=True,
                        idmp=("writer", str(n)),
                    )
                pipe.xadd(
                    store.keys(quiet)[0],
                    {
                        "event": "message",
                        "data": "{}",
                        "producer": "writer",
                        "iid": "quiet",
                        "final": "0",
                    },
                    nomkstream=True,
                    idmp=("writer", "quiet"),
                )
                written = await pipe.execute()
            assert (await collect(reader, 1))[0].id == written[-1]
            assert relay.streams[hot].position != written[-2]
            response = await store.client.execute_command(
                "XREAD",
                "COUNT",
                1,
                "MAXCOUNT",
                1,
                "MAXSIZE",
                65536,
                "STREAMS",
                store.keys(hot)[0],
                store.keys(quiet)[0],
                "0-1",
                "0-1",
            )
            assert sum(len(rows) for _, rows in response) == 1
