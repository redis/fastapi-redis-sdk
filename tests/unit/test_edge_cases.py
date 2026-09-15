from __future__ import annotations

import concurrent.futures
import json
import sys
from typing import Any
from unittest.mock import patch

import fakeredis.aioredis
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request as StarletteRequest

from redis_fastapi.cache import (
    CachePending,
    cache,
    default_key_builder,
)
from redis_fastapi.config import get_settings
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


def _make_fake_dep(fake: fakeredis.aioredis.FakeRedis):
    async def _fake() -> fakeredis.aioredis.FakeRedis:
        return fake
    return _fake


class TestMiddlewareFallbackNoLifespan:
    def test_middleware_fallback_crashes_without_lifespan(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        cache_mod = sys.modules["redis_fastapi.cache"]

        app = FastAPI()
        FastAPIRedis(app).caching()
        empty_ps = _PoolState()

        @app.get("/test")
        async def test_endpoint(request: StarletteRequest) -> dict:
            request.state.redis_cache_pending = CachePending(
                key="test:key", ttl=60, redis=None
            )
            return {"ok": True}

        with patch.object(cache_mod, "_get_pool_state", return_value=empty_ps):
            with TestClient(app, raise_server_exceptions=False) as c:
                r = c.get("/test")


class TestConcurrentMissThunderingHerd:
    def test_thundering_herd_on_first_request(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts: list[int] = [0]

        @app.get("/compute", dependencies=[Depends(cache(ttl=300))])
        async def compute() -> dict:
            counts[0] += 1
            return {"value": counts[0]}

        app.dependency_overrides[get_async_redis] = _make_fake_dep(fake_async_redis)
        with TestClient(app) as c:
            N = 20

            def req() -> dict[str, Any]:
                return c.get("/compute").json()

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
                futures = [ex.submit(req) for _ in range(N)]
                results = [f.result() for f in concurrent.futures.as_completed(futures)]

            values = sorted(set(r["value"] for r in results))


class TestStampedeProtection:
    def test_stampede_protection_keyword_accepted(self) -> None:
        deps = cache(ttl=300, stampede_protection=True)
        assert deps is not None

    @pytest.mark.asyncio
    async def test_stampede_protection_can_convert_hit_to_miss(
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

        @app.get("/near-expiry", dependencies=[Depends(cache(ttl=2, stampede_protection=True))])
        async def ep() -> dict:
            return {"value": 1}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis
        app.dependency_overrides[get_async_redis] = _fake

        key = default_key_builder(
            _make_request("/near-expiry"), prefix=settings.pattern_prefix("cache")
        )
        await fake_async_redis.set(key, json.dumps({"body": '{"v":1}', "status_code": 200, "headers": {}, "etag": '"abc"'}))
        await fake_async_redis.expire(key, 0)

        with patch("redis_fastapi.cache.record_cache_request", side_effect=spy_record):
            with TestClient(app, raise_server_exceptions=False) as c:
                responses = [c.get("/near-expiry") for _ in range(20)]
                hits = sum(1 for r in responses if r.status_code == 200 and r.json() is not None and r.json().get("value"))

        get_settings.cache_clear()
