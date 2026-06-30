"""Tests for Redis-backed rate limiting: rate_limit() dependency.

Uses fakeredis for all Redis operations. fakeredis does not support
INCREX, so the Lua fallback path is exercised in every test that uses
a real fake Redis client. An explicit INCREX test mocks the probe to
verify the INCREX code path.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import fakeredis.aioredis
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError
from starlette.status import HTTP_429_TOO_MANY_REQUESTS

from redis_fastapi.deps import _get_pool_state, get_async_redis
from redis_fastapi.rate_limit import (
    DEFAULT_RATE_LIMIT_PREFIX,
    rate_limit,
)

# ---------------------------------------------------------------------------
# Helpers: create test apps with rate-limited endpoints
# ---------------------------------------------------------------------------


def _make_app(
    fake: fakeredis.aioredis.FakeRedis,
    limit: int = 5,
    window: int = 60,
    **kwargs: object,
) -> tuple[FastAPI, list[int]]:
    app = FastAPI()
    counts: list[int] = [0]

    @app.get(
        "/limited",
        dependencies=[Depends(rate_limit(limit=limit, window=window, **kwargs))],
    )
    async def limited_endpoint() -> dict:
        counts[0] += 1
        return {"value": counts[0]}

    async def _fake() -> fakeredis.aioredis.FakeRedis:
        return fake

    app.dependency_overrides[get_async_redis] = _fake
    return app, counts


# ===================================================================
# Basic allow / block
# ===================================================================


@pytest.mark.unit
class TestRateLimitAllowBlock:
    def test_requests_under_limit_are_allowed(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        app, counts = _make_app(fake_async_redis, limit=3, window=60)
        with TestClient(app) as c:
            for _ in range(3):
                resp = c.get("/limited")
                assert resp.status_code == 200, resp.text
                assert resp.json()["value"] == counts[0]

        assert counts[0] == 3

    def test_request_over_limit_returns_429(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        app, counts = _make_app(fake_async_redis, limit=3, window=60)
        with TestClient(app) as c:
            for _ in range(3):
                resp = c.get("/limited")
                assert resp.status_code == 200

            resp = c.get("/limited")
            assert resp.status_code == HTTP_429_TOO_MANY_REQUESTS

        assert counts[0] == 3

    def test_retry_after_header_present(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        app, _ = _make_app(fake_async_redis, limit=2, window=60)
        with TestClient(app) as c:
            c.get("/limited")
            c.get("/limited")
            resp = c.get("/limited")

        assert resp.status_code == HTTP_429_TOO_MANY_REQUESTS
        assert "retry-after" in resp.headers

    def test_endpoint_not_called_when_limited(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        app, counts = _make_app(fake_async_redis, limit=1, window=60)
        with TestClient(app) as c:
            resp1 = c.get("/limited")
            assert resp1.status_code == 200
            assert counts[0] == 1

            resp2 = c.get("/limited")
            assert resp2.status_code == HTTP_429_TOO_MANY_REQUESTS
            assert counts[0] == 1

    def test_window_expiry_allows_again(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        app, counts = _make_app(fake_async_redis, limit=2, window=1)
        with TestClient(app) as c:
            c.get("/limited")
            c.get("/limited")
            resp = c.get("/limited")
            assert resp.status_code == HTTP_429_TOO_MANY_REQUESTS

        import time as _time

        _time.sleep(1.1)

        with TestClient(app) as c:
            resp = c.get("/limited")
            assert resp.status_code == 200
            assert resp.json()["value"] == counts[0]

    def test_different_routes_have_independent_counters(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        app = FastAPI()
        call_count: list[int] = [0]

        @app.get("/a", dependencies=[Depends(rate_limit(limit=1, window=60))])
        async def a_endpoint() -> dict:
            call_count[0] += 1
            return {"value": call_count[0]}

        @app.get("/b", dependencies=[Depends(rate_limit(limit=1, window=60))])
        async def b_endpoint() -> dict:
            call_count[0] += 1
            return {"value": call_count[0]}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake

        with TestClient(app) as c:
            resp = c.get("/a")
            assert resp.status_code == 200
            resp = c.get("/b")
            assert resp.status_code == 200
            resp = c.get("/a")
            assert resp.status_code == HTTP_429_TOO_MANY_REQUESTS
            resp = c.get("/b")
            assert resp.status_code == HTTP_429_TOO_MANY_REQUESTS


# ===================================================================
# Value errors
# ===================================================================


@pytest.mark.unit
class TestRateLimitValidation:
    def test_invalid_limit_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="limit must be >= 1"):
            rate_limit(limit=0, window=60)  # type: ignore[arg-type]

    def test_invalid_window_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="window must be >= 1"):
            rate_limit(limit=10, window=0)  # type: ignore[arg-type]


# ===================================================================
# Custom key builder
# ===================================================================


@pytest.mark.unit
class TestCustomKeyBuilder:
    def test_custom_key_builder_separates_buckets(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        app = FastAPI()
        call_count: list[int] = [0]

        @app.get(
            "/a",
            dependencies=[
                Depends(
                    rate_limit(limit=2, window=60, key_builder=lambda r: "bucket-a")
                )
            ],
        )
        async def endpoint_a() -> dict:
            call_count[0] += 1
            return {"value": call_count[0]}

        @app.get(
            "/b",
            dependencies=[
                Depends(
                    rate_limit(limit=2, window=60, key_builder=lambda r: "bucket-b")
                )
            ],
        )
        async def endpoint_b() -> dict:
            call_count[0] += 1
            return {"value": call_count[0]}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake

        with TestClient(app) as c:
            c.get("/a")
            c.get("/a")
            resp = c.get("/a")
            assert resp.status_code == HTTP_429_TOO_MANY_REQUESTS

            resp = c.get("/b")
            assert resp.status_code == 200

    def test_async_key_builder_supported(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        app = FastAPI()
        call_count: list[int] = [0]

        async def async_builder(request: object) -> str:
            return "async-bucket"

        @app.get(
            "/limited",
            dependencies=[
                Depends(rate_limit(limit=1, window=60, key_builder=async_builder))
            ],
        )
        async def limited_endpoint() -> dict:
            call_count[0] += 1
            return {"value": call_count[0]}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake

        with TestClient(app) as c:
            resp = c.get("/limited")
            assert resp.status_code == 200
            resp = c.get("/limited")
            assert resp.status_code == HTTP_429_TOO_MANY_REQUESTS


# ===================================================================
# Sync endpoint support
# ===================================================================


@pytest.mark.unit
class TestSyncEndpoint:
    def test_sync_endpoint_supported(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        app = FastAPI()
        call_count: list[int] = [0]

        @app.get(
            "/sync-limited", dependencies=[Depends(rate_limit(limit=2, window=60))]
        )
        def sync_endpoint() -> dict:
            call_count[0] += 1
            return {"value": call_count[0]}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake

        with TestClient(app) as c:
            c.get("/sync-limited")
            c.get("/sync-limited")
            resp = c.get("/sync-limited")
            assert resp.status_code == HTTP_429_TOO_MANY_REQUESTS

        assert call_count[0] == 2


# ===================================================================
# Lua fallback (INCREX unsupported)
# ===================================================================


@pytest.mark.unit
class TestLuaFallback:
    def test_lua_fallback_used_when_increx_unsupported(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        """fakeredis does not support INCREX, so Lua fallback is used automatically."""
        app, _ = _make_app(fake_async_redis, limit=3, window=60)
        pool_state = _get_pool_state(app)
        assert pool_state.increx_supported == "unknown"

        with TestClient(app) as c:
            for _ in range(3):
                resp = c.get("/limited")
                assert resp.status_code == 200

            resp = c.get("/limited")
            assert resp.status_code == HTTP_429_TOO_MANY_REQUESTS

        assert pool_state.increx_supported == "unsupported"

    def test_increx_unsupported_detected_once(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        """The INCREX probe executes exactly once — on the first request."""
        app, _ = _make_app(fake_async_redis, limit=5, window=60)
        pool_state = _get_pool_state(app)

        with patch.object(
            fake_async_redis, "execute_command", wraps=fake_async_redis.execute_command
        ) as mock_exec:
            with TestClient(app) as c:
                c.get("/limited")

            increx_calls = [
                call for call in mock_exec.call_args_list if call[0][0] == "INCREX"
            ]
            assert len(increx_calls) == 1

        pool_state.increx_supported = "unsupported"

        with patch.object(
            fake_async_redis, "execute_command", wraps=fake_async_redis.execute_command
        ) as mock_exec:
            with TestClient(app) as c:
                c.get("/limited")

            increx_calls = [
                call for call in mock_exec.call_args_list if call[0][0] == "INCREX"
            ]
            assert len(increx_calls) == 0


# ===================================================================
# INCREX supported path (mocked)
# ===================================================================


@pytest.mark.unit
class TestIncrexPath:
    def test_increx_supported_path(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        """When INCREX is supported, the INCREX code path is exercised."""
        app, _ = _make_app(fake_async_redis, limit=3, window=60)
        pool_state = _get_pool_state(app)

        original_exec = fake_async_redis.execute_command

        async def fake_increx(command: str, *args: object, **kwargs: object) -> object:
            if command == "INCREX":
                if args and str(args[0]) == "__ratelimit_probe__":
                    return [0, 0]
                key = str(args[0])
                existing = await fake_async_redis.get(key)
                current = int(existing) if existing else 0
                # args: [key, "BYINT", 1, "UBOUND", limit, "EX", window, "ENX"]
                limit_val = int(args[4])  # type: ignore[arg-type]
                if current >= limit_val:
                    return [current, 0]
                new_val = current + 1
                await fake_async_redis.set(key, str(new_val))
                window_secs = int(args[6])  # type: ignore[arg-type]
                await fake_async_redis.expire(key, window_secs)
                return [new_val, 1]
            return await original_exec(command, *args, **kwargs)

        fake_async_redis.execute_command = fake_increx  # type: ignore[method-assign]

        with TestClient(app) as c:
            resp = c.get("/limited")
            assert resp.status_code == 200
            assert pool_state.increx_supported == "supported"

            c.get("/limited")
            c.get("/limited")
            resp = c.get("/limited")
            assert resp.status_code == HTTP_429_TOO_MANY_REQUESTS


# ===================================================================
# Redis error policy (fail open)
# ===================================================================


@pytest.mark.unit
class TestRedisErrorPolicy:
    def test_redis_error_fails_open(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        """When Redis raises an error, the request succeeds (fail open)."""
        app = FastAPI()

        @app.get("/limited", dependencies=[Depends(rate_limit(limit=5, window=60))])
        async def limited_endpoint() -> dict:
            return {"ok": True}

        async def broken_redis() -> AsyncMock:
            client = AsyncMock(spec=fake_async_redis)
            client.execute_command = AsyncMock(
                side_effect=RedisConnectionError("Redis down")
            )
            return client

        app.dependency_overrides[get_async_redis] = broken_redis

        with TestClient(app) as c:
            resp = c.get("/limited")
            assert resp.status_code == 200


# ===================================================================
# Default key builder
# ===================================================================


@pytest.mark.unit
class TestDefaultKeyBuilder:
    async def test_default_key_prefix(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        app, _ = _make_app(fake_async_redis, limit=5, window=60)
        with TestClient(app) as c:
            c.get("/limited")

        keys = await fake_async_redis.keys(f"{DEFAULT_RATE_LIMIT_PREFIX}:*")
        assert len(keys) >= 1
        key = keys[0].decode() if isinstance(keys[0], bytes) else str(keys[0])
        assert key.startswith(DEFAULT_RATE_LIMIT_PREFIX)
        assert "GET" in key
        assert "limited" in key

    async def test_custom_prefix_overrides_default(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        app = FastAPI()

        @app.get(
            "/limited",
            dependencies=[
                Depends(rate_limit(limit=5, window=60, prefix="myapp:ratelimit"))
            ],
        )
        async def limited_endpoint() -> dict:
            return {"ok": True}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake

        with TestClient(app) as c:
            c.get("/limited")

        keys = await fake_async_redis.keys("myapp:ratelimit:*")
        assert len(keys) >= 1
