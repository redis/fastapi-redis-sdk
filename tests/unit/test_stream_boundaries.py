"""Input, setup and wire encoding boundaries independent of Redis behavior."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import Mock

import pytest
from fastapi import FastAPI, Request

from redis_fastapi import (
    FastAPIRedis,
    InvalidStreamRequest,
    RedisSettings,
    StreamBackend,
    StreamError,
    get_stream_backend,
    sse_response,
)
from redis_fastapi.stream_backend import StreamEvent, _normalize, parse_cursor
from redis_fastapi.streams import _cursor, _frame
from redis_fastapi.telemetry import disable_telemetry, record_stream


@pytest.mark.parametrize(
    "value",
    [
        "",
        "$",
        "+",
        "1",
        "-1-0",
        "1-",
        "1-0\n",
        "a-0",
        "1-" + "0" * 40,
        f"{2**64}-0",
        None,
        1,
    ],
)
def test_invalid_cursor(value):
    with pytest.raises(InvalidStreamRequest):
        parse_cursor(value)


@pytest.mark.parametrize("value", ["0-0", "000-00", "1-2", f"{2**64 - 1}-{2**64 - 1}"])
def test_numeric_cursor(value):
    assert parse_cursor(value) == tuple(map(int, value.split("-")))


def request(query=b"", headers=()):
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": query,
            "headers": headers,
        }
    )


def test_cursor_selection_validates_all_sources():
    assert _cursor(request(), None) == "0-0"
    assert _cursor(request(b"after=01-02"), None) == "1-2"
    assert _cursor(request(b"after=1-0", [(b"last-event-id", b"2-0")]), "3-0") == "3-0"
    for req in (
        request(b"after="),
        request(b"after=1-0&after=2-0"),
        request(b"after=bad", [(b"last-event-id", b"2-0")]),
        request(headers=[(b"last-event-id", b"1-0"), (b"last-event-id", b"2-0")]),
    ):
        with pytest.raises(InvalidStreamRequest):
            _cursor(req, None)


def test_utf8_normalization_and_framing():
    assert _normalize({b"a": [(b"1-0", {b"data": b"{}"})]}) == {
        "a": [["1-0", {"data": "{}"}]]
    }
    body = _frame(event="message", data={"text": "one\ntwo é"}, event_id="1-0")
    assert body.startswith(b"id: 1-0\nevent: message\ndata: ")
    assert body.count(b"\n") == 4 and b"one\\ntwo" in body
    assert not _frame(
        event="redis.stream.control", data={"action": "reload"}
    ).startswith(b"id:")


def test_event_decode_and_corruption():
    event = StreamEvent._parse(
        ["1-0", {"event": "message", "data": '{"n":1}', "final": "1"}]
    )
    assert event.final and event.data == {"n": 1} and event._size() > 0
    for fields in (
        {},
        {"event": "message", "data": "{}", "final": "broken"},
        {"event": "message", "data": "broken", "final": "0"},
    ):
        with pytest.raises(StreamError):
            StreamEvent._parse(["1-0", fields])


def test_setup_overrides_repeat_and_invalid_options():
    app = FastAPI()
    setup = FastAPIRedis(app)
    assert setup.streams(stream_bucket_count=4) is setup
    setup.streams(stream_bucket_count=4)
    assert app.state._redis_stream_settings.stream_bucket_count == 4
    with pytest.raises(ValueError):
        setup.streams(stream_bucket_count=5)
    with pytest.raises(ValueError):
        FastAPIRedis(FastAPI()).streams(unknown=True)
    with pytest.raises(ValueError):
        FastAPIRedis(FastAPI()).streams(stream_replay_bytes=2048)
    with pytest.raises(ValueError):
        FastAPIRedis(FastAPI()).streams(stream_bucket_count=0)


def test_key_encoding_is_injective_and_slot_mapping_deterministic():
    client = Mock()
    settings = RedisSettings(prefix="test")
    a = StreamBackend(client, settings=settings)._store
    b = StreamBackend(client, settings=settings)._store
    names = ["a:b", "a/b", "a{b}", "é", "e", "租户:聊天"]
    assert len({a.keys(name)[0] for name in names}) == len(names)
    assert all(a.keys(name) == b.keys(name) for name in names)
    assert all(
        a.keys(name)[0].split("{")[1].split("}")[0]
        == a.keys(name)[1].split("{")[1].split("}")[0]
        for name in names
    )
    for name in ("", "x" * 513, None):
        with pytest.raises(InvalidStreamRequest):
            a.keys(name)
    with pytest.raises(InvalidStreamRequest):
        StreamBackend(client, settings=RedisSettings(prefix="{invalid}"))
    with pytest.raises(InvalidStreamRequest):
        StreamBackend(client, settings=RedisSettings(stream_replay_bytes=2048))


async def test_unstarted_runtime_and_credential_validation():
    backend = StreamBackend(Mock())
    with pytest.raises(StreamError):
        async with backend.subscribe("chat"):
            pass
    with pytest.raises(StreamError):
        await get_stream_backend(Request({"type": "http", "app": FastAPI()}))
    with pytest.raises(InvalidStreamRequest):
        await sse_response(
            request(), backend=backend, stream="chat", expires_at=datetime(2020, 1, 1)
        )
    with pytest.raises(StreamError):
        await sse_response(request(), backend=backend, stream="chat")


def test_disabled_telemetry_is_inert():
    disable_telemetry()
    record_stream("operations", operation="append", stream="private")
    record_stream("unknown", 1)


async def test_tls_relay_construction_and_url_pool_limits():
    from redis.asyncio.connection import SSLConnection

    from redis_fastapi.streams import _blocking_client

    client = _blocking_client(RedisSettings(ssl=True))
    try:
        assert client.connection_pool.connection_class is SSLConnection
    finally:
        await client.aclose()
    client = _blocking_client(RedisSettings(cluster=True, ssl=True))
    await client.aclose()
    client = _blocking_client(
        RedisSettings(
            url="redis://localhost:6379?max_connections=999", stream_bucket_count=4
        )
    )
    try:
        assert client.connection_pool.max_connections == 4
    finally:
        await client.aclose()
    with pytest.raises(ValueError):
        _blocking_client(
            RedisSettings(url="redis://localhost:6379?socket_timeout=0.001")
        )


def test_explicit_settings_defaults_are_environment_free(monkeypatch, tmp_path):
    from redis_fastapi import StreamSettings, add_redis_streams

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "REDIS_PREFIX=unwanted\nREDIS_STREAM_BUCKET_COUNT=invalid\n"
    )
    monkeypatch.setenv("REDIS_STREAM_BUCKET_COUNT", "invalid")
    settings = StreamSettings(prefix="explicit")
    assert settings.stream_bucket_count == 16
    assert StreamBackend(Mock()).settings.prefix == "redis:fastapi"
    backend = StreamBackend(Mock(), settings=settings)
    app = FastAPI()
    add_redis_streams(app, backend=backend)
    with pytest.raises(ValueError):
        add_redis_streams(FastAPI(), backend=backend, stream_bucket_count=2)
    with pytest.raises(ValueError):
        add_redis_streams(FastAPI(), manage_lifespan=False)
    with pytest.raises(ValueError):
        StreamSettings(unknown=True)


async def test_shared_command_and_blocking_pool_is_rejected():
    from redis.asyncio import ConnectionPool, Redis

    pool = ConnectionPool()
    command, blocking = Redis(connection_pool=pool), Redis(connection_pool=pool)
    try:
        backend = StreamBackend(command, blocking_client=blocking)
        with pytest.raises(ValueError, match="separate blocking"):
            async with backend:
                pass
    finally:
        await command.aclose()
        await blocking.aclose()
        await pool.aclose()


def test_live_read_budget_must_fit_one_maximum_event():
    from redis_fastapi import StreamSettings

    with pytest.raises(InvalidStreamRequest, match="Live read budget"):
        StreamBackend(Mock(), settings=StreamSettings(stream_read_maxbytes=1024))
    with pytest.raises(ValueError, match="Live read budget"):
        FastAPIRedis(FastAPI()).streams(stream_read_maxbytes=1024)


def test_long_key_prefix_is_charged_to_live_read_budget():
    from redis_fastapi import StreamSettings

    with pytest.raises(InvalidStreamRequest, match="key and framing"):
        StreamBackend(
            Mock(),
            settings=StreamSettings(
                prefix="x" * 2000, stream_max_event_bytes=64, stream_read_maxbytes=576
            ),
        )
