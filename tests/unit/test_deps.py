"""Tests for AsyncRedisDep dependency injection."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import fakeredis
import fakeredis.aioredis
import pytest
from fastapi import FastAPI, Request, WebSocket
from fastapi.testclient import TestClient

from redis_fastapi.config import RedisSettings
from redis_fastapi.deps import (
    AsyncRedisDep,
    _get_pool_state,
    _PoolState,
    get_async_redis,
)


def _mock_request(app: FastAPI | None = None) -> Request:
    """Create a minimal mock Request pointing at *app*."""
    if app is None:
        app = FastAPI()
    req = MagicMock(spec=Request)
    req.app = app
    return req


@pytest.mark.unit
class TestAsyncDep:
    def test_async_redis_dep_injection(self) -> None:
        app = FastAPI()
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)

        @app.get("/ping")
        async def ping(redis: AsyncRedisDep) -> dict:
            await redis.set("key", "val")
            return {"result": await redis.get("key")}

        async def override() -> fakeredis.aioredis.FakeRedis:
            return fake

        app.dependency_overrides[get_async_redis] = override

        with TestClient(app) as client:
            r = client.get("/ping")
            assert r.status_code == 200
            assert r.json()["result"] == "val"

        app.dependency_overrides.clear()

    def test_async_redis_dep_in_websocket(self) -> None:
        app = FastAPI()
        pool = MagicMock()
        redis = MagicMock(connection_pool=pool)
        state = _get_pool_state(app)
        state.async_pool = pool
        state._async_client = redis

        @app.websocket("/ws")
        async def websocket_endpoint(
            websocket: WebSocket,
            injected: AsyncRedisDep,
        ) -> None:
            await websocket.accept()
            await websocket.send_json({"same_client": injected is redis})

        with TestClient(app) as client:
            with client.websocket_connect("/ws") as websocket:
                assert websocket.receive_json() == {"same_client": True}


@pytest.mark.unit
class TestBuildAsyncPoolKVMode:
    """Cover _PoolState.build_async_pool KV path."""

    async def test_kv_mode_creates_pool(self) -> None:
        from redis.asyncio import ConnectionPool as AsyncConnectionPool

        s = RedisSettings(host="localhost", port=6379, db=0)
        with patch("redis_fastapi.deps.get_settings", return_value=s):
            pool = _PoolState.build_async_pool()
            assert isinstance(pool, AsyncConnectionPool)
            await pool.disconnect()


@pytest.mark.unit
class TestBuildCluster:
    """Cover _PoolState.build_async_cluster.

    We mock AsyncRedisCluster constructors since we don't
    have a real cluster to connect to.
    """

    def test_build_async_cluster_url(self) -> None:
        s = RedisSettings(url="redis://cluster:7000", cluster=True)
        mock_cls = MagicMock()
        with (
            patch("redis_fastapi.deps.get_settings", return_value=s),
            patch("redis_fastapi.deps.AsyncRedisCluster", mock_cls),
        ):
            _PoolState.build_async_cluster()
            mock_cls.from_url.assert_called_once()

    def test_build_async_cluster_kv(self) -> None:
        s = RedisSettings(host="cluster-node", port=7001, cluster=True)
        mock_cls = MagicMock()
        with (
            patch("redis_fastapi.deps.get_settings", return_value=s),
            patch("redis_fastapi.deps.AsyncRedisCluster", mock_cls),
        ):
            _PoolState.build_async_cluster()
            mock_cls.assert_called_once()
            call_kw = mock_cls.call_args[1]
            assert call_kw["host"] == "cluster-node"


@pytest.mark.unit
class TestGetAsyncRedisNoLifespan:
    """get_async_redis raises RuntimeError when no lifespan has run."""

    @pytest.mark.asyncio
    async def test_raises_without_lifespan_pool(self) -> None:
        with pytest.raises(RuntimeError, match="no lifespan"):
            await get_async_redis(_mock_request())


@pytest.mark.unit
class TestGetAsyncRedisClusterBranch:
    """Cover get_async_redis cluster=True branch."""

    @pytest.mark.asyncio
    async def test_get_async_redis_returns_cluster_client(self) -> None:
        s = RedisSettings(cluster=True)
        mock_cluster = MagicMock()
        with patch("redis_fastapi.deps.get_settings", return_value=s):
            request = _mock_request()
            ps = _get_pool_state(request.app)
            ps.async_cluster = mock_cluster
            result = await get_async_redis(request)
            assert result is mock_cluster

    @pytest.mark.asyncio
    async def test_get_async_redis_raises_without_lifespan_cluster(self) -> None:
        s = RedisSettings(cluster=True)
        with patch("redis_fastapi.deps.get_settings", return_value=s):
            with pytest.raises(RuntimeError, match="no lifespan"):
                await get_async_redis(_mock_request())


@pytest.mark.unit
class TestCacheBackend:
    """Unit tests for CacheBackend get/set/delete/delete_group."""

    @pytest.mark.asyncio
    async def test_get_set_round_trip(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        await backend.set("k1", {"a": 1}, ttl=60)
        result = await backend.get("k1")
        assert result == {"a": 1}

    @pytest.mark.asyncio
    async def test_get_miss_returns_none(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        result = await backend.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_without_ttl(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        await backend.set("k1", "hello")
        result = await backend.get("k1")
        assert result == "hello"
        # No TTL means key persists indefinitely
        ttl = await fake.ttl(backend._build_key("k1"))
        assert ttl == -1  # -1 means no expiry

    @pytest.mark.asyncio
    async def test_delete_existing_key(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        await backend.set("k1", "val")
        deleted = await backend.delete("k1")
        assert deleted is True
        assert await backend.get("k1") is None

    @pytest.mark.asyncio
    async def test_delete_missing_key(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        deleted = await backend.delete("nonexistent")
        assert deleted is False

    @pytest.mark.asyncio
    async def test_delete_group(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        await backend.set("k1", "v1")
        await backend.set("k2", "v2")
        await backend.set("k3", "v3")

        deleted = await backend.delete_group()
        assert deleted == 3
        assert await backend.get("k1") is None

    @pytest.mark.asyncio
    async def test_eviction_group_override_per_call(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="default")

        await backend.set("k1", "in-default")
        await backend.set("k1", "in-other", eviction_group="other")

        assert await backend.get("k1") == "in-default"
        assert await backend.get("k1", eviction_group="other") == "in-other"

    @pytest.mark.asyncio
    async def test_eviction_group_isolation_on_delete(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns1")

        await backend.set("k1", "v1", eviction_group="ns1")
        await backend.set("k1", "v2", eviction_group="ns2")

        await backend.delete_group("ns1")
        assert await backend.get("k1", eviction_group="ns1") is None
        assert await backend.get("k1", eviction_group="ns2") == "v2"

    @pytest.mark.asyncio
    async def test_delete_group_empty_wipes_all(self) -> None:
        """delete_group() with no eviction group deletes ALL cache keys."""
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        # No instance-level eviction group
        backend = CacheBackend(fake, eviction_group="")

        # Seed keys across different groups
        await backend.set("k1", "v1", eviction_group="ns1")
        await backend.set("k2", "v2", eviction_group="ns2")
        await backend.set("k3", "v3", eviction_group="ns1")

        # Verify all exist
        assert await backend.get("k1", eviction_group="ns1") == "v1"
        assert await backend.get("k2", eviction_group="ns2") == "v2"
        assert await backend.get("k3", eviction_group="ns1") == "v3"

        # Wipe all — no eviction group provided, instance group is ""
        deleted = await backend.delete_group()
        assert deleted == 3

        # Everything is gone
        assert await backend.get("k1", eviction_group="ns1") is None
        assert await backend.get("k2", eviction_group="ns2") is None
        assert await backend.get("k3", eviction_group="ns1") is None

    @pytest.mark.asyncio
    async def test_delete_group_empty_logs_warning(self) -> None:
        """delete_group() with no eviction group emits a warning log."""

        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="")

        with patch("redis_fastapi.cache_backend.logger") as mock_logger:
            await backend.delete_group()
            mock_logger.warning.assert_called_once()
            assert "ALL cache keys" in mock_logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    async def test_get_handles_redis_error(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        with patch.object(fake, "get", side_effect=ConnectionError("down")):
            result = await backend.get("k1")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_handles_redis_error(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        with patch.object(fake, "set", side_effect=ConnectionError("down")):
            # Should not raise
            await backend.set("k1", "val", ttl=60)

    @pytest.mark.asyncio
    async def test_delete_handles_redis_error(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        with patch.object(fake, "delete", side_effect=ConnectionError("down")):
            result = await backend.delete("k1")
        assert result is False

    @pytest.mark.asyncio
    async def test_get_handles_decode_error(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        # Write invalid JSON directly
        full_key = backend._build_key("k1")
        await fake.set(full_key, "not-valid-json{{{")
        result = await backend.get("k1")
        assert result is None

    # ---- has() ----

    @pytest.mark.asyncio
    async def test_has_returns_true_for_existing_key(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        await backend.set("k1", "val")
        assert await backend.has("k1") is True

    @pytest.mark.asyncio
    async def test_has_returns_false_for_missing_key(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        assert await backend.has("nonexistent") is False

    @pytest.mark.asyncio
    async def test_has_respects_eviction_group(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        await backend.set("k1", "val", eviction_group="a")
        assert await backend.has("k1", eviction_group="a") is True
        assert await backend.has("k1", eviction_group="b") is False

    @pytest.mark.asyncio
    async def test_has_handles_redis_error(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        with patch.object(fake, "exists", side_effect=ConnectionError("down")):
            assert await backend.has("k1") is False

    # ---- default param on get() ----

    @pytest.mark.asyncio
    async def test_get_returns_none_by_default_on_miss(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        result = await backend.get("missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_returns_custom_default_on_miss(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        result = await backend.get("missing", default="fallback")
        assert result == "fallback"

    @pytest.mark.asyncio
    async def test_get_returns_custom_default_on_redis_error(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        with patch.object(fake, "get", side_effect=ConnectionError("down")):
            result = await backend.get("k1", default=42)
        assert result == 42

    @pytest.mark.asyncio
    async def test_get_returns_custom_default_on_decode_error(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        full_key = backend._build_key("k1")
        await fake.set(full_key, "not-json{{{")
        result = await backend.get("k1", default={"empty": True})
        assert result == {"empty": True}

    @pytest.mark.asyncio
    async def test_get_ignores_default_on_hit(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        await backend.set("k1", "real-value")
        result = await backend.get("k1", default="fallback")
        assert result == "real-value"

    # ---- timedelta TTL support ----

    @pytest.mark.asyncio
    async def test_set_with_timedelta_ttl(self) -> None:
        from datetime import timedelta

        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        await backend.set("k1", "val", ttl=timedelta(minutes=5))
        result = await backend.get("k1")
        assert result == "val"

        # TTL should be close to 300 seconds
        full_key = backend._build_key("k1")
        actual_ttl = await fake.ttl(full_key)
        assert 298 <= actual_ttl <= 300

    @pytest.mark.asyncio
    async def test_set_with_timedelta_fractional_seconds(self) -> None:
        from datetime import timedelta

        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        # 90.5 seconds should truncate to 90 seconds (int conversion)
        await backend.set("k1", "val", ttl=timedelta(seconds=90.5))
        result = await backend.get("k1")
        assert result == "val"

        full_key = backend._build_key("k1")
        actual_ttl = await fake.ttl(full_key)
        assert 88 <= actual_ttl <= 90

    @pytest.mark.asyncio
    async def test_set_with_int_ttl_still_works(self) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        await backend.set("k1", "val", ttl=120)
        full_key = backend._build_key("k1")
        actual_ttl = await fake.ttl(full_key)
        assert 118 <= actual_ttl <= 120
