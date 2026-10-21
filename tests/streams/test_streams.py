"""Public storage, relay and HTTP contracts against disposable real Redis."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from starlette.requests import ClientDisconnect

from redis_fastapi import (
    FastAPIRedis,
    InvalidStreamRequest,
    RedisSettings,
    StreamBackend,
    StreamBackendDep,
    StreamCapacityExceeded,
    StreamConflict,
    StreamCursorAhead,
    StreamError,
    StreamExpired,
    StreamHistoryUnavailable,
    StreamNotFound,
    StreamSlowConsumer,
    get_stream_backend,
    sse_response,
)
from redis_fastapi.streams import _cursor

from .conftest import blocked_connections, eventually

pytestmark = pytest.mark.integration


async def publish(backend, stream, n, final=False):
    method = backend.finish if final else backend.append
    return await method(
        stream,
        event="done" if final else "message",
        data={"n": n},
        producer="worker",
        idempotency_key=str(n),
    )


async def collect(events, count):
    return [await asyncio.wait_for(anext(events), 5) for _ in range(count)]


def names(backend, each=1):
    found = {i: [] for i in range(backend.settings.stream_bucket_count)}
    for n in range(10000):
        stream = f"chat:{n}"
        bucket = backend._store.bucket(stream)
        if len(found[bucket]) < each:
            found[bucket].append(stream)
        if all(len(values) == each for values in found.values()):
            return found
    raise AssertionError("Unable to populate buckets")


async def test_create_retry_expiry_and_configuration(deployment):
    backend, *_ = deployment
    await asyncio.gather(*(backend.create("chat", ttl=60) for _ in range(8)))
    key, meta = backend._store.keys("chat")
    before = await backend._store.client.pexpiretime(key)
    await backend.create("chat", ttl=60)
    assert await backend._store.client.pexpiretime(key) == before
    with pytest.raises(StreamConflict):
        await backend.create("chat", ttl=120)
    assert await backend._store.client.hget(meta, "ready") == "1"


async def test_append_retry_and_finish_race(deployment):
    backend, *_ = deployment
    await backend.create("chat", ttl=60)
    ids = await asyncio.gather(*(publish(backend, "chat", 1) for _ in range(8)))
    assert len(set(ids)) == 1
    with pytest.raises(StreamConflict):
        await publish(backend, "chat", 1, final=True)
    result = await asyncio.gather(
        publish(backend, "chat", 2),
        publish(backend, "chat", 3, final=True),
        return_exceptions=True,
    )
    assert isinstance(result[1], str)
    assert isinstance(result[0], (str, StreamConflict))
    assert await publish(backend, "chat", 3, final=True) == result[1]
    with pytest.raises(StreamConflict):
        await publish(backend, "chat", 4)
    async with backend, backend.subscribe("chat") as events:
        batch = [event async for event in events]
    assert batch[-1].final and sum(e.final for e in batch) == 1
    assert batch[0].id == ids[0]


async def test_retention_and_duplicate_of_trimmed_event(deployment):
    backend, *_ = deployment
    backend.settings.stream_max_events = 3
    await backend.create("chat", ttl=60)
    ids = [await publish(backend, "chat", n) for n in range(7)]
    with pytest.raises(StreamHistoryUnavailable):
        async with backend, backend.subscribe("chat"):
            pass
    assert await publish(backend, "chat", 0) == ids[0]
    await publish(backend, "chat", 7, final=True)
    async with backend, backend.subscribe("chat", after=ids[4]) as events:
        assert [e.id async for e in events][:-1] == ids[5:]


async def test_expiry_missing_and_partial_initialization(deployment):
    backend, *_ = deployment
    with pytest.raises(StreamNotFound):
        await backend.append(
            "absent", event="x", data={}, producer="p", idempotency_key="i"
        )
    await backend.create("expiring", ttl=1)
    key, meta = backend._store.keys("expiring")

    async def gone():
        return not await backend._store.client.exists(key)

    await eventually(gone, timeout=3)
    with pytest.raises(StreamExpired):
        await publish(backend, "expiring", 1)
    await backend._store.client.delete(meta)
    with pytest.raises(StreamNotFound):
        await publish(backend, "expiring", 1)
    for broken in ("ready", "ttl", "idmp"):
        await backend.create(broken, ttl=60)
        key, meta = backend._store.keys(broken)
        if broken == "ready":
            await backend._store.client.hset(meta, "ready", "0")
        elif broken == "ttl":
            await backend._store.client.persist(key)
        else:
            await backend._store.client.execute_command(
                "XCFGSET", key, "IDMP-DURATION", 10
            )
        with pytest.raises(StreamError):
            await backend.create(
                broken, ttl=60
            ) if broken != "ready" else await publish(backend, broken, 1)
    await backend.create("lost", ttl=60)
    await backend._store.client.delete(backend._store.keys("lost")[0])
    with pytest.raises(StreamError):
        await publish(backend, "lost", 1)


async def test_replay_handoff_and_fresh_gateway(deployment):
    backend, blocking, *_ = deployment
    await backend.create("chat", ttl=60)
    ids = [await publish(backend, "chat", n) for n in range(40)]
    async with backend:
        async with backend.subscribe("chat", after=ids[4]) as events:
            first = await collect(events, 5)
        while_disconnected = await publish(backend, "chat", 40)
    second = StreamBackend(
        backend._store.client, settings=backend.settings, blocking_client=blocking
    )
    async with second:
        async with second.subscribe("chat", after=first[-1].id) as events:
            # Publish between replay registration and consumption.
            final = await publish(backend, "chat", 41, final=True)
            received = [e.id async for e in events]
    assert received == ids[10:] + [while_disconnected, final]


async def test_slow_subscriber_does_not_block_healthy_client(deployment):
    backend, *_ = deployment
    backend.settings.stream_queue_events = 2
    await backend.create("chat", ttl=60)
    async with backend:
        async with (
            backend.subscribe("chat") as slow,
            backend.subscribe("chat") as healthy,
        ):
            for n in range(4):
                waiting = asyncio.create_task(anext(healthy))
                await publish(backend, "chat", n)
                assert (await asyncio.wait_for(waiting, 5)).data == {"n": n}
            with pytest.raises(StreamSlowConsumer):
                await asyncio.wait_for(anext(slow), 5)


async def test_bucket_connection_bound_and_pool_independence(deployment):
    backend, _, ports, relay_name = deployment
    groups = names(backend, each=2)
    streams = [stream for values in groups.values() for stream in values]
    for stream in streams:
        await backend.create(stream, ttl=60)
    async with backend:
        async with AsyncExitStack() as stack:
            for stream in streams:
                await stack.enter_async_context(backend.subscribe(stream))

            async def four():
                return await blocked_connections(backend._store, ports, relay_name) == 4

            await eventually(four)
            for n in range(80):
                await stack.enter_async_context(
                    backend.subscribe(streams[n % len(streams)])
                )
            await eventually(four)
            assert await publish(backend, streams[0], 1)

    async def zero():
        return await blocked_connections(backend._store, ports, relay_name) == 0

    await eventually(zero)


async def test_admission_and_layout_conflicts(deployment):
    backend, *_ = deployment
    backend.settings.stream_max_subscribers = 1
    await backend.create("chat", ttl=60)
    async with backend, backend.subscribe("chat"):
        with pytest.raises(StreamCapacityExceeded):
            async with backend.subscribe("chat"):
                pass
    other = RedisSettings(prefix=backend.settings.prefix, stream_bucket_count=5)
    with pytest.raises(StreamConflict):
        await StreamBackend(backend._store.client, settings=other).initialize()


def request(query=b"", headers=()):
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/events",
            "query_string": query,
            "headers": headers,
        }
    )


def test_cursor_precedence_and_validation():
    assert _cursor(request(b"after=1-0", [(b"last-event-id", b"2-0")]), None) == "2-0"
    assert _cursor(request(b"after=1-0", [(b"last-event-id", b"2-0")]), "3-0") == "3-0"
    for req in [
        request(b"after=1-0&after=2-0"),
        request(b"after="),
        request(b"after=$"),
        request(b"after=bad", [(b"last-event-id", b"2-0")]),
        request(headers=[(b"last-event-id", b"1-0"), (b"last-event-id", b"2-0")]),
    ]:
        with pytest.raises(InvalidStreamRequest):
            _cursor(req, None)


async def test_http_authorization_resume_and_final_204(deployment):
    backend, *_ = deployment
    await backend.create("chat", ttl=60)
    ids = [await publish(backend, "chat", n, final=n == 2) for n in range(3)]
    app = FastAPI()
    FastAPIRedis(app).streams()

    async def access(request: Request):
        if request.headers.get("authorization") != "Bearer accepted":
            raise HTTPException(401, headers={"WWW-Authenticate": "Bearer"})
        return "chat"

    async def dependency():
        return backend

    app.dependency_overrides[get_stream_backend] = dependency

    @app.get(
        "/events",
        response_class=StreamingResponse,
        responses={200: {"content": {"text/event-stream": {}}}},
    )
    async def events(
        request: Request, streams: StreamBackendDep, stream=Depends(access)
    ):
        return await sse_response(request, backend=streams, stream=stream)

    async with (
        backend,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client,
    ):
        denied = await client.get("/events?after=bad")
        assert (
            denied.status_code == 401 and denied.headers["www-authenticate"] == "Bearer"
        )
        headers = {"Authorization": "Bearer accepted", "Last-Event-ID": ids[0]}
        resumed = await client.get("/events?after=0-0", headers=headers)
        assert (
            resumed.status_code == 200
            and resumed.headers["cache-control"] == "no-store"
        )
        assert f"id: {ids[0]}" not in resumed.text
        assert all(f"id: {value}" in resumed.text for value in ids[1:])
        assert "event: done" in resumed.text
        assert (
            await client.get("/events", headers={**headers, "Last-Event-ID": ids[-1]})
        ).status_code == 204
        assert (
            await client.get(
                "/events?after=bad", headers={"Authorization": "Bearer accepted"}
            )
        ).status_code == 422
        assert (
            "text/event-stream"
            in app.openapi()["paths"]["/events"]["get"]["responses"]["200"]["content"]
        )


async def test_prepared_response_deadlines_and_cleanup(deployment):
    backend, *_ = deployment
    backend.settings.stream_prepare_timeout = 0.05
    backend.settings.stream_keepalive = 0.01
    await backend.create("chat", ttl=60)
    async with backend:
        response = await sse_response(request(), backend=backend, stream="chat")
        assert backend._relay.subscribers == 1

        async def released():
            return backend._relay.subscribers == 0

        await eventually(released)
        assert response.closed
        response = await sse_response(
            request(),
            backend=backend,
            stream="chat",
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=0.05),
        )
        body = b"".join([part async for part in response.body_iterator])
        await response.aclose()
        assert b": keepalive" in body and b'"action":"reauthenticate"' in body
        assert b"id:" not in body
        assert backend._relay.subscribers == 0


async def test_unknown_expired_and_future_cursor_http_errors(deployment):
    backend, *_ = deployment
    async with backend:
        with pytest.raises(StreamNotFound):
            await sse_response(request(), backend=backend, stream="absent")
        await backend.create("chat", ttl=60)
        with pytest.raises(StreamCursorAhead):
            await sse_response(
                request(b"after=999999999999999999-0"), backend=backend, stream="chat"
            )
        assert backend._relay.subscribers == 0


async def test_byte_overflow_reconnect_and_stream_local_expiry(deployment):
    backend, *_ = deployment
    backend.settings.stream_queue_bytes = 64
    expiring, healthy = names(backend, each=2)[0]
    await backend.create(expiring, ttl=1)
    await backend.create(healthy, ttl=60)
    async with backend:
        async with backend.subscribe(healthy) as events:
            expected = await publish(backend, healthy, 1)
            with pytest.raises(StreamSlowConsumer):
                await asyncio.wait_for(anext(events), 5)
        async with backend.subscribe(healthy) as events:
            assert (await collect(events, 1))[0].id == expected
        backend.settings.stream_queue_bytes = 1048576
        async with (
            backend.subscribe(expiring) as expired,
            backend.subscribe(healthy, after=expected) as events,
        ):
            with pytest.raises(StreamExpired):
                await asyncio.wait_for(anext(expired), 4)
            last = await publish(backend, healthy, 2)
            assert (await collect(events, 1))[0].id == last


async def test_transport_failure_and_disconnect_release_reservation(deployment):
    backend, *_ = deployment
    await backend.create("chat", ttl=60)
    await publish(backend, "chat", 1)
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/events",
        "query_string": b"",
        "headers": [],
        "asgi": {"spec_version": "2.4"},
    }

    async def receive():
        await asyncio.Event().wait()

    async def failed_send(message):
        if message["type"] == "http.response.body":
            raise OSError("connection closed")

    async with backend:
        response = await sse_response(request(), backend=backend, stream="chat")
        with pytest.raises(ClientDisconnect):
            await response(scope, receive, failed_send)
        assert backend._relay.subscribers == 0
        backend.settings.stream_send_timeout = 0.02
        response = await sse_response(request(), backend=backend, stream="chat")

        async def blocked_send(message):
            await asyncio.Event().wait()

        with pytest.raises((ClientDisconnect, TimeoutError, asyncio.TimeoutError)):
            await response(scope, receive, blocked_send)
        assert backend._relay.subscribers == 0


async def test_finish_committed_reply_lost_and_pre_exec_disconnect(
    deployment, topology
):
    from redis.asyncio.connection import Connection

    backend, _, ports, _ = deployment
    if not ports:
        pytest.skip("Raw socket fault injection requires owned nodes")
    await backend.create("reply", ttl=60)
    store = backend._store
    key, meta = store.keys("reply")
    port = (
        store.client.get_node_from_key(key).port if topology == "cluster" else ports[0]
    )

    async def queued():
        conn = Connection(host="127.0.0.1", port=port, decode_responses=True)
        await conn.connect()
        await conn.send_command("WATCH", key, meta)
        assert await conn.read_response() == "OK"
        await conn.send_command("MULTI")
        assert await conn.read_response() == "OK"
        await conn.send_command(
            "XADD",
            key,
            "NOMKSTREAM",
            "IDMP",
            "worker",
            "final",
            "*",
            "event",
            "done",
            "data",
            "{}",
            "producer",
            "worker",
            "iid",
            "final",
            "final",
            "1",
        )
        assert await conn.read_response() == "QUEUED"
        return conn

    before = await queued()
    await before.disconnect()
    assert not (await store.read("reply")).events
    after = await queued()
    try:
        await after.send_command("EXEC")

        async def committed():
            return (await store.read("reply")).sealed

        await eventually(committed)
    finally:
        await after.disconnect()
    final = await backend.finish(
        "reply", event="done", data={}, producer="worker", idempotency_key="final"
    )
    batch = await store.read("reply")
    assert (
        len(batch.events) == 1 and batch.events[0].id == final and batch.events[0].final
    )


async def test_command_error_during_create_never_publishes_ready(
    deployment, topology, monkeypatch
):
    backend, *_ = deployment
    if topology != "standalone":
        pytest.skip(
            "Runtime command injection uses the standalone pipeline; slot behavior is tested separately"
        )
    client = backend._store.client
    original = client.pipeline
    injected = False

    def pipeline(*args, **kwargs):
        pipe = original(*args, **kwargs)
        execute = pipe.execute

        async def fail_config(*args, **kwargs):
            nonlocal injected
            for index, (command, options) in enumerate(pipe.command_stack):
                if command[0] == "XCFGSET" and not injected:
                    # Valid command syntax, server-side execution error. Other
                    # queued commands still execute on the real Redis server.
                    pipe.command_stack[index] = (
                        ("XCFGSET", command[1], "IDMP-DURATION", -1),
                        options,
                    )
                    injected = True
            return await execute(*args, **kwargs)

        pipe.execute = fail_config
        return pipe

    monkeypatch.setattr(client, "pipeline", pipeline)
    with pytest.raises(StreamError):
        await backend.create("partial", ttl=60)
    key, meta = backend._store.keys("partial")
    assert await client.hget(meta, "ready") == "0"
    assert await client.pttl(key) > 0
    with pytest.raises(StreamError):
        await publish(backend, "partial", 1)
    with pytest.raises(StreamError):
        await backend.create("partial", ttl=60)


async def test_deleted_floor_survives_failed_metadata_write(
    deployment, topology, monkeypatch
):
    backend, *_ = deployment
    if topology != "standalone":
        pytest.skip("Runtime command injection uses the standalone pipeline")
    backend.settings.stream_max_events = 2
    await backend.create("chat", ttl=60)
    first = await publish(backend, "chat", 1)
    await publish(backend, "chat", 2)
    client = backend._store.client
    original = client.pipeline

    def pipeline(*args, **kwargs):
        pipe = original(*args, **kwargs)
        execute = pipe.execute

        async def fail_floor(*args, **kwargs):
            for index, (command, options) in enumerate(pipe.command_stack):
                if command[0] == "HSET" and command[2] == "floor":
                    pipe.command_stack[index] = (("INCR", command[1]), options)
            return await execute(*args, **kwargs)

        pipe.execute = fail_floor
        return pipe

    monkeypatch.setattr(client, "pipeline", pipeline)
    with pytest.raises(StreamError):
        await publish(backend, "chat", 3)
    assert await client.hget(backend._store.keys("chat")[1], "floor") == "0-0"
    with pytest.raises(StreamHistoryUnavailable):
        await backend._store.read("chat")
    assert len((await backend._store.read("chat", first)).events) == 2


async def test_lifespan_composition_repeat_setup_and_rollback(deployment, monkeypatch):
    from contextlib import asynccontextmanager

    from redis_fastapi.config import get_settings

    backend, _, ports, _ = deployment
    if not ports or backend.settings.cluster:
        pytest.skip(
            "Application setup exercised on standalone; backend Cluster path tested separately"
        )
    # deployment.settings deliberately doesn't configure topology: use the
    # client's type rather than its fixture settings for this setup scenario.
    from redis.asyncio import RedisCluster

    if isinstance(backend._store.client, RedisCluster):
        pytest.skip("Standalone application lifespan scenario")
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_URL", f"redis://127.0.0.1:{ports[0]}")
    monkeypatch.setenv("REDIS_PREFIX", backend.settings.prefix + ":app")
    observed = []

    @asynccontextmanager
    async def existing(app):
        observed.append("start")
        yield
        observed.append("stop")

    app = FastAPI(lifespan=existing)
    try:
        FastAPIRedis(app).lifespan().streams(stream_bucket_count=2)
        FastAPIRedis(app).lifespan().streams(stream_bucket_count=2)
        with pytest.raises(ValueError):
            FastAPIRedis(app).streams(stream_bucket_count=3)
        async with app.router.lifespan_context(app):
            shared = await get_stream_backend(Request({"type": "http", "app": app}))
            await shared.create("chat", ttl=60)
            assert shared._relay.store.buckets == 2
            await publish(shared, "chat", 1, final=True)
            response = await sse_response(request(), backend=shared, stream="chat")
            assert b"event: done" in b"".join(
                [chunk async for chunk in response.body_iterator]
            )
            await response.aclose()
        assert observed == ["start", "stop"]
        assert not hasattr(app.state, "_redis_stream_backend")
    finally:
        get_settings.cache_clear()


async def test_invalid_inputs_before_mutation(deployment):
    backend, *_ = deployment
    await backend.create("chat", ttl=60)
    values = [
        ("redis.stream.control", {}),
        ("bad\nname", {}),
        ("ok", float("nan")),
        ("ok", {"large": "x" * 70000}),
    ]
    for event, data in values:
        with pytest.raises(InvalidStreamRequest):
            await backend.append(
                "chat", event=event, data=data, producer="p", idempotency_key="i"
            )
    assert not (await backend._store.read("chat")).events


async def test_canceled_preparation_releases_resources(deployment, monkeypatch):
    backend, *_ = deployment
    await backend.create("chat", ttl=60)
    async with backend:
        entered = asyncio.Event()
        original = backend._store.read

        async def paused(*args, **kwargs):
            entered.set()
            await asyncio.Event().wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(backend._store, "read", paused)
        preparing = asyncio.create_task(
            sse_response(request(), backend=backend, stream="chat")
        )
        await asyncio.wait_for(entered.wait(), 2)
        preparing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await preparing
        assert backend._relay.subscribers == 0


async def test_hot_stream_fairness_and_membership_during_block(deployment):
    backend, _, ports, relay_name = deployment
    hot, quiet, joining = names(backend, each=3)[0]
    backend.settings.stream_read_maxcount = 1
    for stream in (hot, quiet, joining):
        await backend.create(stream, ttl=60)
    async with backend:
        async with backend.subscribe(hot), backend.subscribe(quiet) as events:
            if ports:

                async def blocked():
                    return (
                        await blocked_connections(backend._store, ports, relay_name)
                        == 1
                    )

                await eventually(blocked)
            # A quiet log must make progress while the same bucket has backlog.
            async with backend._store.client.pipeline(transaction=True) as pipe:
                for n in range(80):
                    pipe.execute_command(
                        "XADD",
                        backend._store.keys(hot)[0],
                        "NOMKSTREAM",
                        "IDMP",
                        "worker",
                        str(n),
                        "*",
                        "event",
                        "message",
                        "data",
                        "{}",
                        "producer",
                        "worker",
                        "iid",
                        str(n),
                        "final",
                        "0",
                    )
                pipe.execute_command(
                    "XADD",
                    backend._store.keys(quiet)[0],
                    "NOMKSTREAM",
                    "IDMP",
                    "worker",
                    "quiet",
                    "*",
                    "event",
                    "message",
                    "data",
                    "{}",
                    "producer",
                    "worker",
                    "iid",
                    "quiet",
                    "final",
                    "0",
                )
                written = await pipe.execute()
            assert (await collect(events, 1))[0].id == written[-1]
            assert backend._relay.streams[hot].position != written[-2]
            async with backend.subscribe(joining) as new_events:
                expected = await publish(backend, joining, 1)
                assert (await collect(new_events, 1))[0].id == expected
            assert joining not in backend._relay.streams


async def test_failure_between_consumer_reads_is_not_lost(deployment):
    backend, *_ = deployment
    backend.settings.stream_queue_events = 1
    await backend.create("chat", ttl=60)
    async with backend, backend.subscribe("chat") as events:
        await publish(backend, "chat", 1)
        assert (await collect(events, 1))[0].data == {"n": 1}
        await publish(backend, "chat", 2)
        await publish(backend, "chat", 3)

        async def overflowed():
            return any(
                sub.failure for sub in backend._relay.streams["chat"].subscribers
            )

        await eventually(overflowed)
        with pytest.raises(StreamSlowConsumer):
            await asyncio.wait_for(anext(events), 1)
