"""Unit tests for resumable streams: the parts that need no Redis server.

Cursor parsing and selection, name and event-type validation, SSE framing,
``session_expiry()``, the support probe's reply handling, the settings
checks, setup, the error handlers, and the SSE response's reader-slot and
send-timeout rules.  Redis behaviour - IDMP, ``DELEX``, ``SET ... IFEQ`` - is
not in fakeredis, so it is covered by the integration suite.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.crc import key_slot
from redis.exceptions import ConnectionError as RedisConnectionError
from starlette.requests import Request

from redis_fastapi import (
    FastAPIRedis,
    StreamBackend,
    StreamConfigurationError,
    StreamInvalidCursor,
    StreamUnsupported,
    add_redis_streams,
    get_stream_backend,
    session_expiry,
)
from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import (
    StreamBusy,
    StreamCredentialExpired,
    StreamCursorAhead,
    StreamExpired,
    StreamFenced,
    StreamFinished,
    StreamNotFound,
    StreamStoreError,
)
from redis_fastapi.session_backend import SessionState
from redis_fastapi.stream_backend import (
    _keys,
    _StreamCapabilities,
    _xread_entries,
    check_event_type,
    check_name,
    parse_cursor,
    probe_streams_support,
)
from redis_fastapi.streams import (
    _SSEResponse,
    check_stream_settings,
    encode_frame,
    select_cursor,
    stream_error_handler,
)


@pytest.fixture()
def settings_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """Set ``REDIS_*`` variables for one test, with a fresh settings cache."""

    def apply(**values: Any) -> None:
        for name, value in values.items():
            monkeypatch.setenv(f"REDIS_{name.upper()}", str(value))
        get_settings.cache_clear()

    get_settings.cache_clear()
    yield apply
    get_settings.cache_clear()


def _request(headers: list[tuple[str, str]] = (), query: str = "") -> Request:  # type: ignore[assignment]
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(k.encode(), v.encode()) for k, v in headers],
            "query_string": query.encode(),
        }
    )


# ---------------------------------------------------------------------------
# Cursors
# ---------------------------------------------------------------------------


class TestParseCursor:
    def test_parses_into_integers(self) -> None:
        assert parse_cursor("1700000000000-3") == (1700000000000, 3)
        assert parse_cursor("0-0") == (0, 0)

    @pytest.mark.parametrize(
        "value", ["$", "+", "-", "123", "", "1-", "-1", "a-1", "1-1-1", " 1-1"]
    )
    def test_rejects_special_and_malformed_ids(self, value: str) -> None:
        with pytest.raises(StreamInvalidCursor):
            parse_cursor(value)

    def test_rejects_parts_above_64_bits(self) -> None:
        with pytest.raises(StreamInvalidCursor):
            parse_cursor(f"{2**64}-0")
        with pytest.raises(StreamInvalidCursor):
            parse_cursor(f"0-{2**64}")
        assert parse_cursor(f"{2**64 - 1}-{2**64 - 1}")[0] == 2**64 - 1

    def test_compares_as_numbers_not_text(self) -> None:
        assert "999-0" > "1000-0"  # the text comparison is wrong
        assert parse_cursor("999-0") < parse_cursor("1000-0")
        assert parse_cursor("5-9") < parse_cursor("5-10")

    def test_invalid_cursor_is_a_value_error(self) -> None:
        with pytest.raises(ValueError):
            parse_cursor("$")


class TestSelectCursor:
    def test_handler_value_wins(self) -> None:
        request = _request([("last-event-id", "5-0")], "after=3-0")
        assert select_cursor(request, "1-0") == "1-0"

    def test_header_beats_query(self) -> None:
        request = _request([("last-event-id", "5-0")], "after=3-0")
        assert select_cursor(request) == "5-0"

    def test_query_when_alone(self) -> None:
        assert select_cursor(_request(query="after=3-0")) == "3-0"

    def test_none_when_absent(self) -> None:
        assert select_cursor(_request()) is None

    def test_repeated_header_is_refused(self) -> None:
        request = _request([("last-event-id", "1-0"), ("last-event-id", "2-0")])
        with pytest.raises(StreamInvalidCursor):
            select_cursor(request)

    def test_repeated_query_is_refused(self) -> None:
        with pytest.raises(StreamInvalidCursor):
            select_cursor(_request(query="after=1-0&after=2-0"))

    def test_a_losing_cursor_is_still_validated(self) -> None:
        request = _request([("last-event-id", "junk")], "after=1-0")
        with pytest.raises(StreamInvalidCursor):
            select_cursor(request, "2-0")


# ---------------------------------------------------------------------------
# Names, event types and framing
# ---------------------------------------------------------------------------


class TestNames:
    @pytest.mark.parametrize("scope", ["chat:42", "a", "x" * 200, "t.e_s-t:1"])
    def test_valid_scopes(self, scope: str) -> None:
        check_name(scope, "run_1-A")

    @pytest.mark.parametrize(
        "scope", ["", "x" * 201, "chat:{42}", "a b", "a\nb", "ä", "a/b"]
    )
    def test_invalid_scopes(self, scope: str) -> None:
        with pytest.raises(ValueError):
            check_name(scope)

    @pytest.mark.parametrize("stream_id", ["", "x" * 65, "a:b", "a{b", "a.b", "a b"])
    def test_invalid_ids(self, stream_id: str) -> None:
        with pytest.raises(ValueError):
            check_name("chat:1", stream_id)

    def test_scope_and_id_pairs_never_share_a_key(self) -> None:
        # ":" is allowed in a scope but not in an ID, so the split is unique.
        assert _keys("a:b", "c").stream != _keys("a", "b-c").stream

    def test_keys_share_a_cluster_slot(self) -> None:
        keys = _keys("chat:42", "run1")
        slots = {key_slot(k.encode()) for k in (keys.stream, keys.meta, keys.alive)}
        assert len(slots) == 1

    def test_keys_use_the_prefix_setting(self, settings_env: Any) -> None:
        settings_env(prefix="myapp")
        keys = _keys("chat:1", "r")
        assert keys.stream == "myapp:stream:{chat:1:r}"
        assert keys.meta == "myapp:stream-meta:{chat:1:r}"
        assert keys.alive == "myapp:stream-alive:{chat:1:r}"
        assert keys.active == "myapp:stream-active:chat:1"


class TestEventTypes:
    @pytest.mark.parametrize("event_type", ["delta", "x" * 128, "tool.call", "a:b"])
    def test_valid(self, event_type: str) -> None:
        check_event_type(event_type)

    @pytest.mark.parametrize(
        "event_type",
        ["", "x" * 129, "a\nid: 9-0", "a\rb", "a\0b", "stream.end", "stream.x"],
    )
    def test_invalid_or_reserved(self, event_type: str) -> None:
        with pytest.raises(ValueError):
            check_event_type(event_type)


class TestEncodeFrame:
    def test_single_line(self) -> None:
        assert encode_frame("t", '{"a": 1}', "1-0") == (
            'id: 1-0\nevent: t\ndata: {"a": 1}\n\n'
        )

    def test_no_id_for_the_readers_own_events(self) -> None:
        assert encode_frame("stream.gap", "{}") == "event: stream.gap\ndata: {}\n\n"

    def test_line_breaks_become_several_data_lines(self) -> None:
        frame = encode_frame("t", "one\ntwo\r\nthree\rfour")
        assert frame == ("event: t\ndata: one\ndata: two\ndata: three\ndata: four\n\n")


# ---------------------------------------------------------------------------
# Session deadline
# ---------------------------------------------------------------------------


class TestSessionExpiry:
    def test_none_without_session_middleware(self) -> None:
        assert session_expiry(_request()) is None

    def test_reads_the_absolute_deadline(self) -> None:
        from redis_fastapi.sessions import STATE_SCOPE_KEY

        request = _request()
        request.scope[STATE_SCOPE_KEY] = SessionState(data={}, absolute_remaining=60)
        deadline = session_expiry(request)
        assert deadline is not None
        left = (deadline - datetime.now(timezone.utc)).total_seconds()
        assert 58 <= left <= 61

    def test_none_without_a_deadline(self) -> None:
        from redis_fastapi.sessions import STATE_SCOPE_KEY

        request = _request()
        request.scope[STATE_SCOPE_KEY] = SessionState(data={})
        assert session_expiry(request) is None


# ---------------------------------------------------------------------------
# Replies and the support probe
# ---------------------------------------------------------------------------


class TestXreadEntries:
    def test_resp2(self) -> None:
        reply = [[b"s", [(b"1-0", {b"t": b"x"})]]]
        assert _xread_entries(reply) == [(b"1-0", {b"t": b"x"})]

    def test_resp3(self) -> None:
        reply = {b"s": [[(b"1-0", {b"t": b"x"})]]}
        assert _xread_entries(reply) == [(b"1-0", {b"t": b"x"})]

    @pytest.mark.parametrize("reply", [None, [], {}])
    def test_empty(self, reply: Any) -> None:
        assert _xread_entries(reply) == []


class TestProbe:
    async def test_both_commands_present(self) -> None:
        client = MagicMock()
        client.execute_command = AsyncMock(return_value={"xcfgset": [1], "delex": [1]})
        assert await probe_streams_support(client) is True

    async def test_list_reply(self) -> None:
        client = MagicMock()
        client.execute_command = AsyncMock(return_value=[[1], [1]])
        assert await probe_streams_support(client) is True

    async def test_a_missing_command(self) -> None:
        client = MagicMock()
        client.execute_command = AsyncMock(return_value=[[1], None])
        assert await probe_streams_support(client) is False

    async def test_redis_py_type_error_means_unsupported(self) -> None:
        client = MagicMock()
        client.execute_command = AsyncMock(side_effect=TypeError("nil entry"))
        assert await probe_streams_support(client) is False

    async def test_unreachable_is_unknown(self) -> None:
        client = MagicMock()
        client.execute_command = AsyncMock(side_effect=RedisConnectionError())
        assert await probe_streams_support(client) is None

    async def test_cluster_asks_a_random_node(self) -> None:
        client = MagicMock(spec=AsyncRedisCluster)
        client.execute_command = AsyncMock(return_value={"a": [1], "b": [1]})
        assert await probe_streams_support(client) is True
        _, kwargs = client.execute_command.call_args
        assert kwargs["target_nodes"] == AsyncRedisCluster.RANDOM


# ---------------------------------------------------------------------------
# Settings, setup and handlers
# ---------------------------------------------------------------------------


class TestCheckStreamSettings:
    def test_defaults_pass(self, settings_env: Any) -> None:
        settings_env()
        check_stream_settings()

    def test_prefix_with_braces(self, settings_env: Any) -> None:
        settings_env(prefix="app:{x}")
        with pytest.raises(StreamConfigurationError, match="prefix"):
            check_stream_settings()

    def test_heartbeat_key_must_outlive_the_heartbeat(self, settings_env: Any) -> None:
        settings_env(stream_heartbeat_seconds=10, stream_alive_ttl_seconds=10)
        with pytest.raises(StreamConfigurationError, match="alive"):
            check_stream_settings()

    def test_socket_timeout_must_exceed_the_block(self, settings_env: Any) -> None:
        settings_env(socket_timeout=5)
        with pytest.raises(StreamConfigurationError, match="socket_timeout"):
            check_stream_settings()

    def test_other_features_never_see_the_checks(self, settings_env: Any) -> None:
        settings_env(socket_timeout=5)
        assert get_settings().socket_timeout == 5  # RedisSettings itself accepts it


class TestSetup:
    def test_add_redis_streams_is_idempotent(self, settings_env: Any) -> None:
        settings_env()
        app = FastAPI()
        first = add_redis_streams(app)
        assert add_redis_streams(app) is first
        assert isinstance(first, StreamBackend)

    def test_builder_registers_the_backend(self, settings_env: Any) -> None:
        settings_env()
        app = FastAPI()
        FastAPIRedis(app).lifespan().streams().streams()
        assert isinstance(app.state._redis_streams, StreamBackend)
        assert StreamNotFound in app.exception_handlers

    def test_bad_settings_fail_at_setup(self, settings_env: Any) -> None:
        settings_env(prefix="{bad}")
        with pytest.raises(StreamConfigurationError):
            FastAPIRedis(FastAPI()).streams()

    async def test_dependency_without_setup(self) -> None:
        app = FastAPI()
        request = Request({"type": "http", "app": app, "headers": []})
        with pytest.raises(StreamConfigurationError, match="streams"):
            await get_stream_backend(request)


class TestErrorHandler:
    @pytest.mark.parametrize(
        ("exc", "status"),
        [
            (StreamNotFound("x"), 404),
            (StreamExpired("x"), 410),
            (StreamBusy("r1"), 409),
            (StreamCursorAhead("x"), 409),
            (StreamFinished("x"), 409),
            (StreamFenced("x"), 409),
            (StreamInvalidCursor("x"), 422),
            (StreamCredentialExpired("x"), 401),
            (StreamUnsupported("x"), 503),
            (StreamStoreError("x"), 503),
        ],
    )
    async def test_status(self, exc: Exception, status: int) -> None:
        response = await stream_error_handler(_request(), exc)
        assert response.status_code == status
        if status == 503:
            assert response.headers["Retry-After"]

    def test_busy_names_the_run(self) -> None:
        assert StreamBusy("r1").stream_id == "r1"


# ---------------------------------------------------------------------------
# The SSE response: reader slots and the send timeout
# ---------------------------------------------------------------------------


class _Backend:
    """Stands in for a StreamBackend's reader accounting."""

    def __init__(self, limit: int = 1) -> None:
        self.limit = limit
        self.readers = 0

    def _reserve_reader(self) -> bool:
        if self.readers >= self.limit:
            return False
        self.readers += 1
        return True

    def _release_reader(self) -> None:
        self.readers -= 1


_SCOPE = {"type": "http", "asgi": {"spec_version": "2.4"}}


async def _never() -> Any:
    await asyncio.sleep(3600)


def _response(backend: _Backend, body: AsyncIterator[str], timeout: float = 5) -> Any:
    return _SSEResponse(
        body,
        backend=backend,  # type: ignore[arg-type]
        send_timeout=timeout,
        media_type="text/event-stream",
    )


async def _frames(count: int, delay: float = 0) -> AsyncIterator[str]:
    for i in range(count):
        if delay:
            await asyncio.sleep(delay)
        yield f"data: {i}\n\n"


class TestReaderSlots:
    async def test_a_response_never_sent_takes_no_slot(self) -> None:
        backend = _Backend()
        for _ in range(5):
            _response(backend, _frames(1))
        assert backend.readers == 0

    async def test_slot_held_while_streaming_and_released_after(self) -> None:
        backend = _Backend()
        seen: list[int] = []

        async def send(message: Any) -> None:
            seen.append(backend.readers)

        await _response(backend, _frames(3))(_SCOPE, _never, send)
        assert seen and all(count == 1 for count in seen)
        assert backend.readers == 0

    async def test_over_the_limit_is_a_503_before_any_byte(self) -> None:
        backend = _Backend(limit=0)
        sent: list[Any] = []

        async def send(message: Any) -> None:
            sent.append(message)

        await _response(backend, _frames(1))(_SCOPE, _never, send)
        assert sent[0]["status"] == 503
        assert (b"retry-after", b"5") in sent[0]["headers"]
        assert backend.readers == 0

    async def test_cancellation_releases_the_slot(self) -> None:
        backend = _Backend()

        async def send(message: Any) -> None:
            return None

        task = asyncio.create_task(
            _response(backend, _frames(100, delay=0.05))(_SCOPE, _never, send)
        )
        await asyncio.sleep(0.1)
        assert backend.readers == 1
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert backend.readers == 0

    async def test_an_error_releases_the_slot(self) -> None:
        backend = _Backend()

        async def failing() -> AsyncIterator[str]:
            yield "data: 1\n\n"
            raise RuntimeError("boom")

        async def send(message: Any) -> None:
            return None

        with pytest.raises(RuntimeError):
            await _response(backend, failing())(_SCOPE, _never, send)
        assert backend.readers == 0


class TestSendTimeout:
    @pytest.mark.parametrize("spec_version", ["2.4", "2.0"])
    async def test_a_client_that_stops_reading_is_closed(
        self, spec_version: str
    ) -> None:
        backend = _Backend()
        sent: list[Any] = []
        closed: list[bool] = []

        async def body() -> AsyncIterator[str]:
            try:
                for i in range(100):
                    yield f"data: {i}\n\n"
            finally:
                closed.append(True)

        async def send(message: Any) -> None:
            sent.append(message)
            if len(sent) >= 3:
                await asyncio.sleep(3600)  # the client stopped reading

        scope = {"type": "http", "asgi": {"spec_version": spec_version}}
        await asyncio.wait_for(
            _response(backend, body(), timeout=0.1)(scope, _never, send), 2
        )
        assert backend.readers == 0
        assert closed == [True]


# ---------------------------------------------------------------------------
# A server without support
# ---------------------------------------------------------------------------


class TestUnsupported:
    @pytest.fixture()
    def backend(self, settings_env: Any) -> StreamBackend:
        settings_env()
        backend = StreamBackend(lambda: MagicMock())
        backend._start(_StreamCapabilities(supported=False))
        return backend

    async def test_every_entry_point_refuses(self, backend: StreamBackend) -> None:
        with pytest.raises(StreamUnsupported):
            await backend.open(scope="chat:1")
        with pytest.raises(StreamUnsupported):
            await backend.attach("r", scope="chat:1")
        with pytest.raises(StreamUnsupported):
            await backend.cancel("r", scope="chat:1")
        with pytest.raises(StreamUnsupported):
            await backend.active("chat:1")
        with pytest.raises(StreamUnsupported):
            await backend.sse("r", request=_request(), scope="chat:1")
        with pytest.raises(StreamUnsupported):
            async for _ in backend.follow("r", scope="chat:1"):
                pass

    async def test_detection_runs_once(self, settings_env: Any) -> None:
        settings_env()
        client = MagicMock()
        client.execute_command = AsyncMock(return_value=[None, [1]])
        backend = StreamBackend(lambda: client)
        assert await backend.detect() is False
        assert await backend.detect() is False
        assert client.execute_command.await_count == 1
        assert backend.supported is False

    async def test_unknown_is_asked_again(self, settings_env: Any) -> None:
        settings_env()
        client = MagicMock()
        client.execute_command = AsyncMock(
            side_effect=[RedisConnectionError(), [[1], [1]]]
        )
        backend = StreamBackend(lambda: client)
        assert await backend.detect() is None
        assert await backend.detect() is True

    async def test_no_pool_yet_is_unknown(self, settings_env: Any) -> None:
        settings_env()

        def no_pool() -> Any:
            raise RuntimeError("no lifespan")

        assert await StreamBackend(no_pool).detect() is None


class TestWriterValidation:
    async def test_invalid_names_fail_before_redis(self, settings_env: Any) -> None:
        settings_env()
        backend = StreamBackend(lambda: MagicMock())
        backend._start(_StreamCapabilities(supported=True))
        with pytest.raises(ValueError):
            await backend.open(scope="bad scope")
        with pytest.raises(ValueError):
            await backend.open(scope="ok", stream_id="bad:id")

    async def test_unknown_id_shapes_are_not_found(self, settings_env: Any) -> None:
        settings_env()
        backend = StreamBackend(lambda: MagicMock())
        backend._start(_StreamCapabilities(supported=True))
        with pytest.raises(StreamNotFound):
            await backend.cancel("../x", scope="chat:1")

    def test_oversized_event(self, settings_env: Any) -> None:
        settings_env(stream_max_event_bytes=10)
        backend = StreamBackend(lambda: MagicMock())
        with pytest.raises(ValueError, match="stream_max_event_bytes"):
            backend._encode({"text": "a long payload"})

    def test_undecodable_payload_passes_as_text(self, settings_env: Any) -> None:
        settings_env()
        assert StreamBackend(lambda: MagicMock())._decode(b"not json") == "not json"


class TestSuspendSeconds:
    def test_timedelta_and_int(self) -> None:
        from redis_fastapi.stream_backend import _seconds

        assert _seconds(timedelta(minutes=2)) == 120
        assert _seconds(30) == 30
        assert _seconds(0) == 1


class TestHistoryLost:
    """The check before replay: was an entry after the cursor removed?"""

    @staticmethod
    def _info(**fields: Any) -> dict[str, Any]:
        base = {
            "length": 5,
            "entries-added": 5,
            "max-deleted-entry-id": "0-0",
            "last-generated-id": "50-0",
            "first-entry": ("10-0", {}),
        }
        base.update(fields)
        return base

    def test_nothing_removed(self) -> None:
        from redis_fastapi.stream_backend import _history_lost

        assert not _history_lost(self._info(), (0, 0), own_present=False)
        assert not _history_lost(self._info(), (20, 0), own_present=True)

    def test_a_deleted_entry_after_the_cursor(self) -> None:
        from redis_fastapi.stream_backend import _history_lost

        info = self._info(**{"max-deleted-entry-id": "30-0", "length": 4})
        assert _history_lost(info, (20, 0), own_present=True)
        assert not _history_lost(info, (30, 0), own_present=True)

    def test_trimming_before_a_missing_cursor(self) -> None:
        from redis_fastapi.stream_backend import _history_lost

        info = self._info(**{"entries-added": 9})
        assert _history_lost(info, (5, 0), own_present=False)
        # The cursor's own entry survived, so nothing after it was trimmed.
        assert not _history_lost(info, (12, 0), own_present=True)

    def test_no_cursor_fails_after_any_removal(self) -> None:
        from redis_fastapi.stream_backend import _history_lost

        assert _history_lost(
            self._info(**{"entries-added": 6}), (0, 0), own_present=False
        )

    def test_everything_trimmed(self) -> None:
        from redis_fastapi.stream_backend import _history_lost

        info = self._info(length=0, **{"entries-added": 5, "first-entry": None})
        assert _history_lost(info, (20, 0), own_present=False)
        assert not _history_lost(info, (50, 0), own_present=False)

    def test_no_stream_yet(self) -> None:
        from redis_fastapi.stream_backend import _history_lost

        assert not _history_lost(None, (0, 0), own_present=False)
