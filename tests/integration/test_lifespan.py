"""Integration tests for lifespan pool management and CLIENT SETINFO.

Scenarios: #12 lifespan opens/closes pools, #13 CLIENT SETINFO LIB-NAME.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import patch

import pytest
import redis as sync_redis
from fastapi import FastAPI
from fastapi.testclient import TestClient

from redis_fastapi.config import LIB_NAME, LIB_VERSION
from redis_fastapi.deps import (
    AsyncRedisDep,
    _get_pool_state,
    _PoolState,
)
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import requires_redis


@requires_redis
@pytest.mark.integration
class TestLifespanPoolManagement:
    """Scenario #12: Lifespan opens and closes connection pools."""

    def test_pools_created_and_destroyed(self) -> None:
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.get("/ping")
        async def ping() -> dict:
            return {"pong": True}

        # Before lifespan, state not yet attached
        assert getattr(app.state, "_redis", None) is None

        with TestClient(app) as client:
            ps = _get_pool_state(app)
            # During lifespan, pool is set
            assert ps.async_pool is not None

            r = client.get("/ping")
            assert r.status_code == 200

        # After lifespan, pool is cleared
        ps = _get_pool_state(app)
        assert ps.async_pool is None

    async def test_client_aclose_releases_no_pooled_connection(
        self, real_redis: sync_redis.Redis
    ) -> None:
        """``clear()`` has no connection to close; the pool owns them all.

        Measured on the server with ``INFO clients``, because only the server
        knows what is still open - and asserted while the event loop is still
        running, since loop teardown closes every socket on it and would mask
        the difference.

        The cached client is built with ``connection_pool=`` passed in, so
        ``auto_close_connection_pool`` is ``False`` and ``Redis.aclose()``
        disconnects nothing.  ``ConnectionPool.aclose()`` is what frees the
        connections, and the lifespan already calls it.  That is why
        ``_PoolState.clear()`` stays synchronous and closes no client.
        """
        fanout = 5

        def connected() -> int:
            return int(real_redis.info("clients")["connected_clients"])

        def settle(target: int) -> int:
            # The server reaps closed sockets asynchronously.
            for _ in range(40):
                if connected() <= target:
                    break
                time.sleep(0.05)
            return connected()

        baseline = connected()

        ps = _PoolState()
        ps.async_pool = _PoolState.build_async_pool()
        client = ps.get_async_client()
        assert client.auto_close_connection_pool is False

        try:
            # Concurrent commands force the pool to open several connections.
            await asyncio.gather(*(client.ping() for _ in range(fanout)))
            peak = connected()
            assert peak >= baseline + fanout, (
                f"expected {fanout} new connections, baseline {baseline}, now {peak}"
            )

            # Closing the *client* frees none of them.
            await client.aclose()
            assert connected() == peak, (
                "Redis.aclose() disconnected pooled connections; the premise "
                "that clear() need not close the client no longer holds"
            )

            # Closing the *pool* frees all of them.
            ps.clear()
            assert ps._async_client is None
            await ps.async_pool.aclose()
            assert settle(baseline) <= baseline, (
                f"pool.aclose() left connections open: baseline {baseline}, "
                f"peak {peak}, now {connected()}"
            )
        finally:
            if ps.async_pool is not None:
                await ps.async_pool.aclose()
                ps.async_pool = None

    def test_deps_use_lifespan_pools(self) -> None:
        """AsyncRedisDep should use the lifespan-managed pool."""
        app = FastAPI()
        FastAPIRedis(app).lifespan()
        captured_pools: dict = {}

        @app.get("/async")
        async def async_ep(r: AsyncRedisDep) -> dict:
            captured_pools["async"] = r.connection_pool
            await r.ping()
            return {"ok": True}

        with TestClient(app) as client:
            client.get("/async")

            ps = _get_pool_state(app)
            assert captured_pools["async"] is ps.async_pool

    def test_deps_error_without_lifespan(self) -> None:
        """Without lifespan, deps raise RuntimeError."""
        app = FastAPI()  # no lifespan

        @app.get("/async")
        async def async_ep(r: AsyncRedisDep) -> dict:
            await r.ping()
            return {"ok": True}

        with TestClient(app, raise_server_exceptions=False) as client:
            r = client.get("/async")
            assert r.status_code == 500


@requires_redis
@pytest.mark.integration
class TestClientSetInfo:
    """Scenario #13: CLIENT SETINFO LIB-NAME is set on connect."""

    def test_lib_name_set_via_lifespan(self, real_redis: sync_redis.Redis) -> None:
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.get("/touch")
        async def touch(r: AsyncRedisDep) -> dict:
            await r.ping()
            return {"ok": True}

        with TestClient(app) as client:
            client.get("/touch")

            # Check CLIENT LIST for our lib-name
            clients = real_redis.client_list()
            lib_names = [c.get("lib-name", "") for c in clients]
            assert any(LIB_NAME in name for name in lib_names), (
                f"Expected '{LIB_NAME}' in client lib-names, got: {lib_names}"
            )

    def test_lib_version_reported_in_lib_name(
        self, real_redis: sync_redis.Redis
    ) -> None:
        """LIB-NAME includes the upstream driver version (e.g. fastapi-redis-sdk_v0.1.0)."""
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.get("/touch")
        async def touch(r: AsyncRedisDep) -> dict:
            await r.ping()
            return {"ok": True}

        with TestClient(app) as client:
            client.get("/touch")

            # DriverInfo embeds the upstream version in LIB-NAME, not LIB-VER.
            # e.g. "redis-py(fastapi-redis-sdk_v0.1.0)"
            clients = real_redis.client_list()
            lib_names = [c.get("lib-name", "") for c in clients]
            expected = f"{LIB_NAME}_v{LIB_VERSION}"
            assert any(expected in name for name in lib_names), (
                f"Expected '{expected}' in client lib-names, got: {lib_names}"
            )


@requires_redis
@pytest.mark.integration
class TestLifespanPoolSettings:
    """Pool settings (max_connections, timeouts) applied to real pools."""

    def test_max_connections_applied(self) -> None:
        from redis_fastapi.config import RedisSettings

        custom = RedisSettings(
            url="redis://localhost:6379/0",
            max_connections=3,
        )
        with (
            patch("redis_fastapi.deps.get_settings", return_value=custom),
            patch("redis_fastapi.lifespan.get_settings", return_value=custom),
        ):
            app = FastAPI()
            FastAPIRedis(app).lifespan()

            @app.get("/ping")
            async def ping(r: AsyncRedisDep) -> dict:
                await r.ping()
                return {"ok": True}

            with TestClient(app) as client:
                r = client.get("/ping")
                assert r.status_code == 200
                ps = _get_pool_state(app)
                assert ps.async_pool is not None
                assert ps.async_pool.max_connections == 3

    def test_socket_timeout_applied(self) -> None:
        from redis_fastapi.config import RedisSettings

        custom = RedisSettings(
            url="redis://localhost:6379/0",
            socket_timeout=7.5,
            socket_connect_timeout=2.5,
        )
        with (
            patch("redis_fastapi.deps.get_settings", return_value=custom),
            patch("redis_fastapi.lifespan.get_settings", return_value=custom),
        ):
            app = FastAPI()
            FastAPIRedis(app).lifespan()

            @app.get("/ping")
            async def ping(r: AsyncRedisDep) -> dict:
                await r.ping()
                return {"ok": True}

            with TestClient(app) as client:
                r = client.get("/ping")
                assert r.status_code == 200
                kw = _get_pool_state(app).async_pool.connection_kwargs
                assert kw["socket_timeout"] == 7.5
                assert kw["socket_connect_timeout"] == 2.5

    def test_kv_mode_lifespan(self) -> None:
        """Lifespan works with KV-mode settings (no URL)."""
        from redis_fastapi.config import RedisSettings

        custom = RedisSettings(host="localhost", port=6379, db=0)
        with (
            patch("redis_fastapi.deps.get_settings", return_value=custom),
            patch("redis_fastapi.lifespan.get_settings", return_value=custom),
        ):
            app = FastAPI()
            FastAPIRedis(app).lifespan()

            @app.get("/ping")
            async def ping(r: AsyncRedisDep) -> dict:
                await r.ping()
                return {"ok": True}

            with TestClient(app) as client:
                r = client.get("/ping")
                assert r.status_code == 200


@requires_redis
@pytest.mark.integration
class TestLifespanClusterBranch:
    """Cover lifespan cluster branch via mocking (no real cluster)."""

    def test_cluster_lifespan_creates_and_destroys(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        from redis_fastapi.config import RedisSettings

        custom = RedisSettings(cluster=True, url="redis://localhost:6379/0")
        mock_async = MagicMock()
        mock_async.aclose = AsyncMock()

        with (
            patch("redis_fastapi.lifespan.get_settings", return_value=custom),
            patch.object(
                _PoolState,
                "build_async_cluster",
                return_value=mock_async,
            ),
        ):
            app = FastAPI()
            FastAPIRedis(app).lifespan()

            @app.get("/ping")
            async def ping() -> dict:
                return {"ok": True}

            with TestClient(app) as client:
                ps = _get_pool_state(app)
                assert ps.async_cluster is mock_async
                r = client.get("/ping")
                assert r.status_code == 200

            # After shutdown
            ps = _get_pool_state(app)
            assert ps.async_cluster is None
            mock_async.aclose.assert_called_once()


@requires_redis
@pytest.mark.integration
class TestManualLifespanComposition:
    """Verify the manual-composition alternative documented in architecture.md.

    Users who need explicit lifespan ordering can skip ``FastAPIRedis(app).lifespan()``
    and compose ``redis_lifespan`` themselves.
    """

    def test_manual_lifespan_with_caching(self) -> None:
        """redis_lifespan composed manually + FastAPIRedis(app).caching() works end-to-end."""
        from collections.abc import AsyncIterator
        from contextlib import asynccontextmanager

        from fastapi import Depends

        from redis_fastapi import redis_lifespan
        from redis_fastapi.cache import cache

        startup_order: list[str] = []

        @asynccontextmanager
        async def other_lifespan(app: FastAPI) -> AsyncIterator[None]:
            startup_order.append("other:start")
            yield
            startup_order.append("other:stop")

        @asynccontextmanager
        async def composed_lifespan(app: FastAPI) -> AsyncIterator[None]:
            async with redis_lifespan(app):
                startup_order.append("redis:start")
                async with other_lifespan(app):
                    yield
                startup_order.append("redis:stop")

        app = FastAPI(lifespan=composed_lifespan)
        FastAPIRedis(app).caching()  # no .lifespan() - user owns it

        call_count: list[int] = [0]

        @app.get("/items", dependencies=[Depends(cache(ttl=300))])
        async def get_items() -> dict:
            call_count[0] += 1
            return {"v": call_count[0]}

        with TestClient(app) as client:
            # Pools are available - DI-based caching works
            r1 = client.get("/items")
            assert r1.status_code == 200
            assert r1.headers["X-Redis-Cache"] == "MISS"

            r2 = client.get("/items")
            assert r2.headers["X-Redis-Cache"] == "HIT"
            assert r2.json() == r1.json()

            # Verify startup ordering
            assert startup_order == ["redis:start", "other:start"]

        # Verify both lifespans shut down
        assert "other:stop" in startup_order
        assert "redis:stop" in startup_order

    def test_manual_lifespan_pools_managed(self) -> None:
        """Pools are created and destroyed when redis_lifespan is composed manually."""
        from collections.abc import AsyncIterator
        from contextlib import asynccontextmanager

        from redis_fastapi import redis_lifespan

        @asynccontextmanager
        async def my_lifespan(app: FastAPI) -> AsyncIterator[None]:
            async with redis_lifespan(app):
                yield

        app = FastAPI(lifespan=my_lifespan)

        @app.get("/ping")
        async def ping(r: AsyncRedisDep) -> dict:
            await r.ping()
            return {"ok": True}

        assert getattr(app.state, "_redis", None) is None
        with TestClient(app) as client:
            assert _get_pool_state(app).async_pool is not None
            r = client.get("/ping")
            assert r.status_code == 200
        assert _get_pool_state(app).async_pool is None
