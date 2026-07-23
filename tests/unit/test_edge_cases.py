from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import fakeredis.aioredis
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request as StarletteRequest

from redis_fastapi.cache import (
    CacheHitException,
    CachePending,
    _build_hit_response,
    _cache_control_value,
    _is_stale_for_client,
    _parse_cache_control,
    _read_cache_entry,
    cache,
    default_key_builder,
)
from redis_fastapi.config import CACHE_STATUS_HEADER, get_settings
from redis_fastapi.deps import _PoolState, get_async_redis
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
    @pytest.mark.asyncio
    async def test_corrupt_entry_only_records_miss(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        from redis_fastapi.cache import record_cache_request as cache_record

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

        key = default_key_builder(
            _make_request("/items"), prefix=settings.pattern_prefix("cache")
        )
        await fake_async_redis.set(key, "not-valid-json{{{")

        with patch("redis_fastapi.cache.record_cache_request", side_effect=spy_record):
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


class TestSpanAttributeInconsistencyOnCorruptData:
    @pytest.mark.asyncio
    async def test_span_attributes_on_corrupt_entry(self) -> None:
        from redis_fastapi import telemetry as tel
        tel.disable_telemetry()
        tel.enable_telemetry()

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        settings = get_settings()
        prefix = settings.pattern_prefix("cache")
        key = default_key_builder(_make_request("/test"), prefix=prefix)
        await fake.set(key, "corrupt-json{{{")

        cc = {}
        cached_data, remaining_ttl = await _read_cache_entry(
            fake, key, 300, cc, force_refresh=False
        )
        assert cached_data is not None
        assert cached_data == "corrupt-json{{{"

        with pytest.raises(json.JSONDecodeError):
            _build_hit_response(
                cached_data, remaining_ttl, _make_request("/test"), private=False
            )
        tel.disable_telemetry()


class TestTelemetryExceptionLogLevel:
    def test_telemetry_exceptions_logged_at_warning(self) -> None:
        import logging
        from redis_fastapi.telemetry import record_cache_request, enable_telemetry, disable_telemetry

        disable_telemetry()
        enable_telemetry()

        logger = logging.getLogger("redis_fastapi.telemetry")
        original_level = logger.level
        logger.setLevel(logging.DEBUG)

        try:
            with patch.object(logger, "warning") as mock_warning:
                with patch.object(logger, "debug") as mock_debug:
                    record_cache_request(result="hit")
        finally:
            logger.setLevel(original_level)
            disable_telemetry()
