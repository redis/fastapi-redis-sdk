"""Unit tests for resumable streams: the parts that need no Redis server.

Cursor parsing and selection, name and event-type validation, SSE framing,
``session_expiry()``, the settings checks, setup, the error handlers, and the
SSE response's reader-slot and send-timeout rules.  No test here makes a Redis
call: every one that would runs against a real server in the integration
suite.  A backend here gets a client factory that fails the test if called.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from collections.abc import AsyncIterator, Callable, Iterator
from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi import FastAPI
from pydantic import ValidationError
from redis.crc import key_slot
from starlette.requests import Request

from redis_fastapi import (
    FastAPIRedis,
    StreamBackend,
    StreamConfigurationError,
    StreamError,
    StreamEvent,
    StreamInvalidCursor,
    StreamInvalidName,
    StreamUnsupported,
    add_redis_streams,
    get_stream_backend,
    session_expiry,
)
from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import (
    StreamCredentialExpired,
    StreamCursorAhead,
    StreamExpired,
    StreamFinished,
    StreamNotFound,
    StreamStoreError,
)
from redis_fastapi.session_backend import SessionState
from redis_fastapi.stream_backend import (
    StreamWriter,
    _key,
    _StreamCapabilities,
    _xadd,
    _xread_entries,
    check_event_type,
    check_name,
    parse_cursor,
)
from redis_fastapi.streams import (
    _retry_ms,
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


def _no_redis() -> Any:
    """A client factory for tests that must never reach Redis."""
    raise AssertionError("this test must not make a Redis call")


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

    def test_rejects_non_ascii_digits(self) -> None:
        # \d would accept these, and int() would read them as 12-0.
        with pytest.raises(StreamInvalidCursor):
            parse_cursor("\u0661\u0662-\u0660")

    def test_rejects_huge_numbers_as_invalid_not_as_a_crash(self) -> None:
        # Past 4,300 digits int() raises a plain ValueError, which would be a
        # 500; the length bound refuses it first.
        with pytest.raises(StreamInvalidCursor):
            parse_cursor("9" * 5000 + "-0")
        with pytest.raises(StreamInvalidCursor):
            parse_cursor("1" + "0" * 20 + "-0")


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
        with pytest.raises(StreamInvalidName):
            check_name(scope)

    @pytest.mark.parametrize("stream_id", ["", "x" * 65, "a:b", "a{b", "a.b", "a b"])
    def test_invalid_ids(self, stream_id: str) -> None:
        with pytest.raises(StreamInvalidName):
            check_name("chat:1", stream_id)

    def test_an_invalid_name_is_a_value_error_and_a_422(self) -> None:
        assert issubclass(StreamInvalidName, ValueError)
        assert StreamInvalidName.status_code == 422

    def test_scope_and_id_pairs_never_share_a_key(self) -> None:
        # ":" is allowed in a scope but not in an ID, so the split is unique.
        assert _key("a:b", "c") != _key("a", "b-c")

    def test_the_key_uses_the_prefix_setting(self, settings_env: Any) -> None:
        settings_env(prefix="myapp")
        assert _key("chat:1", "r") == "myapp:stream:chat:1:r"

    def test_runs_of_one_scope_spread_over_cluster_slots(self) -> None:
        # One key per run needs no hash tag, so a busy scope is not one shard.
        slots = {key_slot(_key("chat:42", f"run{n}").encode()) for n in range(20)}
        assert len(slots) > 1


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

    def test_retry_hint_is_short_and_spread(self) -> None:
        values = {_retry_ms() for _ in range(200)}
        assert all(1000 <= value <= 3000 for value in values)
        assert len(values) > 1


class TestEventShape:
    def test_only_the_documented_fields(self) -> None:
        names = [f.name for f in dataclasses.fields(StreamEvent)]
        assert names == ["id", "type", "data", "final"]

    def test_replace_keeps_the_new_data(self) -> None:
        event = dataclasses.replace(StreamEvent("1-0", "t", {"a": 1}), data={"b": 2})
        assert dataclasses.asdict(event)["data"] == {"b": 2}


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

    @pytest.mark.parametrize("done_ttl", [60, 120])
    def test_a_finished_run_must_be_shorter_than_a_running_one(
        self, settings_env: Any, done_ttl: int
    ) -> None:
        settings_env(stream_max_run=60, stream_done_ttl=done_ttl)
        with pytest.raises(StreamConfigurationError, match="stream_done_ttl"):
            check_stream_settings()

    def test_no_write_trims_a_run(self) -> None:
        # Trimming could remove the final entry once writes land after it.
        for args in (
            _xadd("k", "delta", "{}", idmp=("w", "1"), seq=1),
            _xadd("k", "stream.end", "{}", idmp=("end", "final"), final=True),
            _xadd("k", "stream.start", "null", idmp=None, create=True),
        ):
            assert "MAXLEN" not in args and "MINID" not in args

    def test_a_run_must_fit_the_idempotency_window(self, settings_env: Any) -> None:
        settings_env(stream_max_run=86_401)
        with pytest.raises(StreamConfigurationError, match="86400"):
            check_stream_settings()

    def test_silence_must_outlast_two_heartbeats(self, settings_env: Any) -> None:
        settings_env(stream_heartbeat_interval=10, stream_silence_timeout=20)
        with pytest.raises(StreamConfigurationError, match="silence"):
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
        assert app.exception_handlers[StreamError] is stream_error_handler
        # One handler for the family, not one per subclass.
        assert StreamNotFound not in app.exception_handlers

    def test_an_application_handler_is_kept(self, settings_env: Any) -> None:
        settings_env()

        async def own(request: Request, exc: Exception) -> Any:
            return None

        app = FastAPI(exception_handlers={StreamError: own})
        add_redis_streams(app)
        assert app.exception_handlers[StreamError] is own

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
            (StreamCursorAhead("x"), 409),
            (StreamFinished("x"), 409),
            (StreamInvalidCursor("x"), 422),
            (StreamInvalidName("x"), 422),
            (StreamCredentialExpired("x"), 401),
            (StreamUnsupported("x"), 503),
            (StreamStoreError("x"), 503),
            (StreamConfigurationError("x"), 500),
        ],
    )
    async def test_status(self, exc: Exception, status: int) -> None:
        response = await stream_error_handler(_request(), exc)
        assert response.status_code == status
        if status == 503:
            assert response.headers["Retry-After"]

    @pytest.mark.parametrize(
        ("exc", "code"),
        [
            (StreamNotFound("x"), "stream_not_found"),
            (StreamExpired("x"), "stream_expired"),
            (StreamCursorAhead("x"), "cursor_ahead"),
            (StreamInvalidCursor("x"), "invalid_cursor"),
            (StreamInvalidName("x"), "invalid_stream_name"),
            (StreamStoreError("x"), "stream_unavailable"),
        ],
    )
    async def test_body_has_a_stable_code(self, exc: Exception, code: str) -> None:
        response = await stream_error_handler(_request(), exc)
        assert json.loads(bytes(response.body)) == {"detail": str(exc), "code": code}


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
        assert json.loads(sent[1]["body"])["code"] == "stream_capacity_exceeded"
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
        backend = StreamBackend(_no_redis)
        backend._start(_StreamCapabilities(supported=False))
        return backend

    async def test_every_entry_point_refuses(self, backend: StreamBackend) -> None:
        with pytest.raises(StreamUnsupported):
            await backend.open(scope="chat:1")
        with pytest.raises(StreamUnsupported):
            await backend.cancel("r", scope="chat:1")
        with pytest.raises(StreamUnsupported):
            await backend.sse("r", request=_request(), scope="chat:1")
        with pytest.raises(StreamUnsupported):
            await backend.follow("r", scope="chat:1")
        with pytest.raises(StreamUnsupported):
            await backend.start(_noop, scope="chat:1")

    async def test_no_pool_yet_is_unknown(self, settings_env: Any) -> None:
        settings_env()

        def no_pool() -> Any:
            raise RuntimeError("no lifespan")

        assert await StreamBackend(no_pool).detect() is None


class TestWriterValidation:
    async def test_invalid_names_fail_before_redis(self, settings_env: Any) -> None:
        settings_env()
        backend = StreamBackend(_no_redis)
        backend._start(_StreamCapabilities(supported=True))
        with pytest.raises(StreamInvalidName):
            await backend.open(scope="bad scope")
        with pytest.raises(StreamInvalidName):
            await backend.open(scope="ok", stream_id="bad:id")

    async def test_invalid_names_on_every_path_are_the_same_error(
        self, settings_env: Any
    ) -> None:
        settings_env()
        backend = StreamBackend(_no_redis)
        backend._start(_StreamCapabilities(supported=True))
        with pytest.raises(StreamInvalidName):
            await backend.cancel("../x", scope="chat:1")
        with pytest.raises(StreamInvalidName):
            await backend.follow("../x", scope="chat:1")

    def test_oversized_event(self, settings_env: Any) -> None:
        settings_env(stream_max_event_bytes=10)
        backend = StreamBackend(_no_redis)
        with pytest.raises(ValueError, match="stream_max_event_bytes"):
            backend._encode({"text": "a long payload"})

    def test_undecodable_payload_passes_as_text(self, settings_env: Any) -> None:
        settings_env()
        assert StreamBackend(_no_redis)._decode(b"not json") == "not json"


class TestDurations:
    def test_settings_take_whole_seconds_only(
        self, settings_env: Callable[..., None]
    ) -> None:
        settings_env(stream_heartbeat_interval=0.5)
        with pytest.raises(ValidationError):
            get_settings()


async def _noop(writer: StreamWriter) -> None:
    return None


class TestSpawnGuards:
    def _writer(self, backend: StreamBackend, *, created: bool) -> StreamWriter:
        return StreamWriter(
            backend, scope="chat:1", stream_id="r1", writer_id="w", created=created
        )

    async def test_a_retry_handle_is_refused(self, settings_env: Any) -> None:
        settings_env()
        backend = StreamBackend(_no_redis)
        with pytest.raises(ValueError, match="created"):
            backend.spawn(self._writer(backend, created=False), _noop)
        assert not backend._spawned

    async def test_a_writer_is_spawned_once(self, settings_env: Any) -> None:
        settings_env()
        backend = StreamBackend(_no_redis)
        writer = self._writer(backend, created=True)
        writer._task = asyncio.get_running_loop().create_future()  # type: ignore[assignment]
        with pytest.raises(ValueError, match="already runs"):
            backend.spawn(writer, _noop)

    async def test_a_retry_handle_cannot_write(self, settings_env: Any) -> None:
        settings_env()
        backend = StreamBackend(_no_redis)
        handle = self._writer(backend, created=False)
        with pytest.raises(ValueError, match="retried start"):
            await handle.append("delta", {})


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


# ---------------------------------------------------------------------------
# Lifecycle checks that run before any Redis call
# ---------------------------------------------------------------------------


class TestLifecycle:
    async def test_no_run_starts_while_closing(
        self, settings_env: Callable[..., None]
    ) -> None:
        settings_env()
        backend = StreamBackend(_no_redis)
        backend._start(_StreamCapabilities(supported=True))
        backend._closing = True
        with pytest.raises(StreamStoreError, match="shutting down"):
            await backend.open(scope="chat:1")
