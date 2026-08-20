from __future__ import annotations

import contextlib
import logging
from importlib import import_module
from unittest.mock import patch

import fakeredis.aioredis
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request as StarletteRequest

import redis_fastapi.telemetry as telemetry
from redis_fastapi.cache import cache, default_key_builder
from redis_fastapi.config import get_settings
from redis_fastapi.deps import get_async_redis
from redis_fastapi.setup import FastAPIRedis


def _make_request(path: str, query: str = "") -> StarletteRequest:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": query.encode(),
        "headers": [],
    }
    return StarletteRequest(scope)


class TestCorruptedEntryDoubleTelemetry:
    async def test_corrupt_entry_only_records_miss(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        recorded_results: list[str] = []

        def spy_record(*, result: str, eviction_group: str = "") -> None:
            recorded_results.append(result)

        app = FastAPI()
        FastAPIRedis(app).caching()

        get_settings.cache_clear()
        settings = get_settings()

        @app.get("/items", dependencies=[Depends(cache(ttl=300))])
        async def ep() -> dict:
            return {"value": 1}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        cache_module = import_module("redis_fastapi.cache")

        key = default_key_builder(
            _make_request("/items"), prefix=settings.pattern_prefix("cache")
        )
        await fake_async_redis.set(key, "not-valid-json{{{")

        with patch.object(cache_module, "record_cache_request", side_effect=spy_record):
            with TestClient(app) as c:
                r = c.get("/items")
                assert r.status_code == 200

        assert "hit" not in recorded_results, (
            f"FIXED: corrupt entry should NOT record 'hit'. Got: {recorded_results}"
        )
        assert recorded_results == ["miss"], (
            f"FIXED: corrupt entry should only record 'miss'. Got: {recorded_results}"
        )
        get_settings.cache_clear()


class _RecordingSpan:
    """Minimal span stand-in that records every attribute change."""

    def __init__(self, attributes: dict[str, object] | None = None) -> None:
        self.attributes: dict[str, object] = dict(attributes or {})
        self.set_sequence: list[tuple[str, object]] = []

    def set_attribute(self, key: str, value: object) -> None:
        self.attributes[key] = value
        self.set_sequence.append((key, value))


class TestSpanAttributeInconsistencyOnCorruptData:
    async def test_span_hit_never_marked_true_on_corrupt_entry(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        get_settings.cache_clear()
        try:
            settings = get_settings()
            spans: dict[str, _RecordingSpan] = {}

            def fake_cache_span(
                name: str, attributes: dict[str, object] | None = None
            ) -> contextlib.AbstractContextManager[_RecordingSpan]:
                spans[name] = _RecordingSpan(attributes)
                return contextlib.nullcontext(spans[name])

            app = FastAPI()
            FastAPIRedis(app).caching()

            @app.get("/items", dependencies=[Depends(cache(ttl=300))])
            async def ep() -> dict:
                return {"value": 1}

            async def _fake() -> fakeredis.aioredis.FakeRedis:
                return fake_async_redis

            app.dependency_overrides[get_async_redis] = _fake
            cache_module = import_module("redis_fastapi.cache")

            key = default_key_builder(
                _make_request("/items"), prefix=settings.pattern_prefix("cache")
            )
            await fake_async_redis.set(key, "not-valid-json{{{")

            with patch.object(cache_module, "cache_span", side_effect=fake_cache_span):
                with TestClient(app) as c:
                    r = c.get("/items")
                    assert r.status_code == 200

            get_span = spans["cache.get"]
            hit_sets = [
                value
                for key_name, value in get_span.set_sequence
                if key_name == "cache.hit"
            ]
            assert hit_sets == [False], (
                "FIXED: span for a corrupt entry must only set cache.hit to False, "
                f"never True. Got sequence: {hit_sets}"
            )
        finally:
            get_settings.cache_clear()


class TestTelemetryExceptionLogLevel:
    def test_failure_logs_warning_once_then_debug(self) -> None:
        logger = logging.getLogger("redis_fastapi.telemetry")
        telemetry.disable_telemetry()
        telemetry.enable_telemetry()
        telemetry._reset_log_once()
        if not telemetry.is_enabled():
            pytest.skip("opentelemetry not installed")

        def _boom(*args: object, **kwargs: object) -> None:
            raise RuntimeError("metric recording failed")

        telemetry._state.cache_requests.add = _boom  # type: ignore[attr-defined]

        try:
            with patch.object(logger, "warning") as mock_warning:
                with patch.object(logger, "debug") as mock_debug:
                    telemetry.record_cache_request(result="hit")
                    telemetry.record_cache_request(result="hit")

            assert mock_warning.call_count == 1
            assert mock_debug.call_count == 1
        finally:
            telemetry.disable_telemetry()

    def test_rate_limit_failure_logs_warning_once_then_debug(self) -> None:
        logger = logging.getLogger("redis_fastapi.telemetry")
        telemetry.disable_telemetry()
        telemetry.enable_telemetry()
        telemetry._reset_log_once()
        if not telemetry.is_enabled():
            pytest.skip("opentelemetry not installed")

        def _boom(*args: object, **kwargs: object) -> None:
            raise RuntimeError("metric recording failed")

        telemetry._state.ratelimit_requests.add = _boom  # type: ignore[attr-defined]

        try:
            with patch.object(logger, "warning") as mock_warning:
                with patch.object(logger, "debug") as mock_debug:
                    telemetry.record_rate_limit_request(result="allowed")
                    telemetry.record_rate_limit_request(result="allowed")

            assert mock_warning.call_count == 1
            assert mock_debug.call_count == 1
        finally:
            telemetry.disable_telemetry()
