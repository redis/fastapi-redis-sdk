"""Tests for Redis Sentinel mode: settings, pool construction and lifespan.

None of these connect to Redis.  Building a ``Sentinel`` and its pool opens
no socket, so the lifespan can run as long as no endpoint sends a command.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.sentinel import (
    Sentinel,
    SentinelConnectionPool,
    SentinelManagedConnection,
    SentinelManagedSSLConnection,
)

from redis_fastapi.config import DRIVER_INFO, RedisSettings
from redis_fastapi.deps import _get_pool_state, _PoolState, get_async_redis
from redis_fastapi.setup import FastAPIRedis


def _sentinel_settings(**overrides: object) -> RedisSettings:
    kw: dict[str, object] = {
        "sentinel": True,
        "sentinel_nodes": ["s1:26379", "s2:26380"],
    }
    kw.update(overrides)
    return RedisSettings(**kw)  # type: ignore[arg-type]


@pytest.mark.unit
class TestSentinelSettings:
    def test_defaults(self) -> None:
        s = RedisSettings()
        assert s.sentinel is False
        assert s.sentinel_master_name == "mymaster"
        assert s.sentinel_nodes == []
        assert s.sentinel_username is None
        assert s.sentinel_password is None

    def test_from_env(self) -> None:
        env = {
            "REDIS_SENTINEL": "true",
            "REDIS_SENTINEL_MASTER_NAME": "cache-primary",
            "REDIS_SENTINEL_NODES": "s1:26379, s2:26380 ,,s3",
            "REDIS_SENTINEL_USERNAME": "sentinel-user",
            "REDIS_SENTINEL_PASSWORD": "sentinel-secret",
        }
        with patch.dict(os.environ, env, clear=True):
            s = RedisSettings()
        assert s.sentinel is True
        assert s.sentinel_master_name == "cache-primary"
        assert s.sentinel_nodes == ["s1:26379", "s2:26380", "s3"]
        assert s.sentinel_username == "sentinel-user"
        assert s.sentinel_password is not None
        assert s.sentinel_password.get_secret_value() == "sentinel-secret"

    def test_addresses_default_port(self) -> None:
        s = _sentinel_settings(sentinel_nodes=["s1:1234", "s2", "::1:26381"])
        assert s.sentinel_addresses() == [
            ("s1", 1234),
            ("s2", 26379),
            ("::1", 26381),
        ]

    def test_requires_nodes(self) -> None:
        with pytest.raises(ValidationError, match="at least one"):
            RedisSettings(sentinel=True)

    def test_rejects_cluster(self) -> None:
        with pytest.raises(ValidationError, match="mutually exclusive"):
            _sentinel_settings(cluster=True)

    def test_rejects_url(self) -> None:
        with pytest.raises(ValidationError, match="'url' is not supported"):
            _sentinel_settings(url="redis://localhost:6379/0")

    @pytest.mark.parametrize("node", [":26379", "s1:", "s1:port", "s1:0", "s1:70000"])
    def test_rejects_malformed_node(self, node: str) -> None:
        with pytest.raises(ValidationError, match="Invalid sentinel node"):
            _sentinel_settings(sentinel_nodes=[node])

    def test_nodes_ignored_when_sentinel_off(self) -> None:
        # Nodes are only validated in sentinel mode.
        s = RedisSettings(sentinel_nodes=["s1:bad"])
        assert s.sentinel is False


@pytest.mark.unit
class TestSentinelKwargs:
    def test_sentinel_kwargs_minimal(self) -> None:
        assert _sentinel_settings().sentinel_kwargs() == {"driver_info": DRIVER_INFO}

    def test_sentinel_kwargs_full(self) -> None:
        s = _sentinel_settings(
            socket_timeout=1.5,
            socket_connect_timeout=0.5,
            ssl=True,
            ssl_ca_certs="/ca.pem",
            sentinel_username="sentinel-user",
            sentinel_password="sentinel-secret",
            # The primary's credentials must not reach the Sentinels.
            username="app",
            password="app-secret",
        )
        kw = s.sentinel_kwargs()
        assert kw["socket_timeout"] == 1.5
        assert kw["socket_connect_timeout"] == 0.5
        assert kw["ssl"] is True
        assert kw["ssl_ca_certs"] == "/ca.pem"
        assert "connection_class" not in kw
        assert kw["username"] == "sentinel-user"
        assert kw["password"] == "sentinel-secret"

    def test_connection_kwargs_for_primary(self) -> None:
        s = _sentinel_settings(
            db=2,
            username="app",
            password="app-secret",
            max_connections=10,
            sentinel_password="sentinel-secret",
        )
        kw = s.sentinel_connection_kwargs()
        assert "host" not in kw
        assert "port" not in kw
        assert kw["db"] == 2
        assert kw["username"] == "app"
        assert kw["password"] == "app-secret"
        assert kw["max_connections"] == 10
        assert kw["driver_info"] is DRIVER_INFO

    def test_connection_kwargs_tls_uses_ssl_flag(self) -> None:
        kw = _sentinel_settings(ssl=True).sentinel_connection_kwargs()
        assert kw["ssl"] is True
        assert "connection_class" not in kw


@pytest.mark.unit
class TestBuildAsyncSentinel:
    async def test_builds_sentinel_and_pool(self) -> None:
        s = _sentinel_settings(
            sentinel_master_name="cache-primary",
            sentinel_password="sentinel-secret",
            password="app-secret",
        )
        with patch("redis_fastapi.deps.get_settings", return_value=s):
            sentinel, pool = _PoolState.build_async_sentinel()

        assert isinstance(sentinel, Sentinel)
        addresses = [
            (
                n.connection_pool.connection_kwargs["host"],
                n.connection_pool.connection_kwargs["port"],
            )
            for n in sentinel.sentinels
        ]
        assert addresses == [("s1", 26379), ("s2", 26380)]
        for node in sentinel.sentinels:
            assert node.connection_pool.connection_kwargs["password"] == (
                "sentinel-secret"
            )

        assert isinstance(pool, SentinelConnectionPool)
        assert pool.service_name == "cache-primary"
        assert pool.is_master is True
        assert pool.sentinel_manager is sentinel
        assert pool.connection_class is SentinelManagedConnection
        assert pool.connection_kwargs["password"] == "app-secret"

        await pool.aclose()
        for node in sentinel.sentinels:
            await node.aclose()

    async def test_tls_uses_sentinel_managed_ssl_connection(self) -> None:
        s = _sentinel_settings(ssl=True)
        with patch("redis_fastapi.deps.get_settings", return_value=s):
            sentinel, pool = _PoolState.build_async_sentinel()
        assert pool.connection_class is SentinelManagedSSLConnection
        await pool.aclose()
        for node in sentinel.sentinels:
            await node.aclose()


@pytest.mark.unit
class TestSentinelLifespan:
    def test_lifespan_opens_and_closes_sentinel(self) -> None:
        s = _sentinel_settings()
        app = FastAPI()
        FastAPIRedis(app).lifespan()
        seen: dict[str, object] = {}

        @app.get("/ping")
        async def ping() -> dict[str, bool]:
            return {"ok": True}

        with (
            patch("redis_fastapi.lifespan.get_settings", return_value=s),
            patch("redis_fastapi.deps.get_settings", return_value=s),
            patch.object(AsyncRedis, "aclose", autospec=True) as aclose,
        ):
            with TestClient(app) as client:
                ps = _get_pool_state(app)
                seen["sentinel"] = ps.async_sentinel
                seen["pool"] = ps.async_pool
                assert client.get("/ping").status_code == 200
                aclose.assert_not_called()

            # One aclose() per Sentinel node; the primary's pool closes itself.
            assert aclose.call_count == 2

        assert isinstance(seen["sentinel"], Sentinel)
        assert isinstance(seen["pool"], SentinelConnectionPool)
        ps = _get_pool_state(app)
        assert ps.async_sentinel is None
        assert ps.async_pool is None
        assert ps.async_cluster is None

    async def test_get_async_redis_uses_sentinel_pool(self) -> None:
        s = _sentinel_settings()
        app = FastAPI()
        with patch("redis_fastapi.deps.get_settings", return_value=s):
            ps = _get_pool_state(app)
            ps.async_sentinel, ps.async_pool = _PoolState.build_async_sentinel()

            request = MagicMock(spec=Request)
            request.app = app
            client = await get_async_redis(request)

        assert isinstance(client, AsyncRedis)
        assert client.connection_pool is ps.async_pool
        await ps.async_pool.aclose()
        for node in ps.async_sentinel.sentinels:
            await node.aclose()
