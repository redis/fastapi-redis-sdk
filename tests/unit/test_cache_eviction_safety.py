"""Unit tests for the startup cache eviction-safety check in lifespan.py."""

from __future__ import annotations

import importlib
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError, ResponseError

from redis_fastapi.config import RedisSettings
from redis_fastapi.lifespan import _check_cache_eviction_safety
from redis_fastapi.setup import FastAPIRedis

# `redis_fastapi.cache` the attribute is the factory function, not the module,
# so patch.object() on the real module rather than a dotted-string target -
# mock's string resolver walks getattr and lands on the function on py3.10.
_CACHE_MOD = importlib.import_module("redis_fastapi.cache")

MEMORY_LIMIT = 1_000_000


def _pool_state(info: Any) -> MagicMock:
    """A _PoolState stand-in whose client returns *info* from INFO memory."""
    client = MagicMock()
    client.info = AsyncMock(return_value=info)
    ps = MagicMock()
    ps.get_async_client.return_value = client
    return ps


def _failing_pool_state(exc: Exception) -> MagicMock:
    client = MagicMock()
    client.info = AsyncMock(side_effect=exc)
    ps = MagicMock()
    ps.get_async_client.return_value = client
    return ps


async def _run(
    caplog: pytest.LogCaptureFixture,
    ps: MagicMock,
    *,
    default_ttl: int = 0,
    warn: bool = True,
    relies: bool = True,
) -> list[str]:
    """Run the check and return the WARNING messages it emitted."""
    settings = RedisSettings(default_ttl=default_ttl, warn_unbounded_cache=warn)
    with (
        patch("redis_fastapi.lifespan.get_settings", return_value=settings),
        patch.object(_CACHE_MOD, "relies_on_default_ttl", return_value=relies),
        caplog.at_level(logging.WARNING, logger="redis_fastapi.lifespan"),
    ):
        caplog.clear()
        await _check_cache_eviction_safety(ps)
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


# ===================================================================
# Server configurations that cannot evict un-expiring keys
# ===================================================================


@pytest.mark.unit
class TestUnsafeServerConfigurations:
    async def test_warns_when_no_memory_limit(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        ps = _pool_state({"maxmemory": 0, "maxmemory_policy": "noeviction"})
        warnings = await _run(caplog, ps)

        assert len(warnings) == 1
        assert "no memory limit" in warnings[0]
        assert "REDIS_DEFAULT_TTL" in warnings[0]

    @pytest.mark.parametrize(
        "policy",
        ["volatile-lru", "volatile-lfu", "volatile-ttl", "volatile-random"],
    )
    async def test_warns_on_volatile_policies(
        self, caplog: pytest.LogCaptureFixture, policy: str
    ) -> None:
        """volatile-* is the default on ElastiCache, Azure, and Memorystore.

        It evicts only keys that carry a TTL, so entries cached without one
        can never be reclaimed and writes eventually fail with OOM.
        """
        ps = _pool_state({"maxmemory": MEMORY_LIMIT, "maxmemory_policy": policy})
        warnings = await _run(caplog, ps)

        assert len(warnings) == 1
        assert policy in warnings[0]
        assert "OOM" in warnings[0]

    async def test_warns_on_noeviction_with_a_limit(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        ps = _pool_state({"maxmemory": MEMORY_LIMIT, "maxmemory_policy": "noeviction"})
        warnings = await _run(caplog, ps)

        assert len(warnings) == 1
        assert "OOM" in warnings[0]

    async def test_no_memory_limit_outranks_an_allkeys_policy(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """allkeys-lru never fires without a limit, so growth is still unbounded."""
        ps = _pool_state({"maxmemory": 0, "maxmemory_policy": "allkeys-lru"})
        warnings = await _run(caplog, ps)

        assert len(warnings) == 1
        assert "no memory limit" in warnings[0]

    async def test_reads_cluster_shaped_info(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Cluster clients answer per node; any primary's view will do."""
        ps = _pool_state(
            {"node-1:6379": {"maxmemory": 0, "maxmemory_policy": "noeviction"}}
        )
        warnings = await _run(caplog, ps)

        assert len(warnings) == 1


# ===================================================================
# Configurations that must stay silent
# ===================================================================


@pytest.mark.unit
class TestSafeConfigurationsStaySilent:
    @pytest.mark.parametrize("policy", ["allkeys-lru", "allkeys-lfu", "allkeys-random"])
    async def test_silent_when_policy_evicts_any_key(
        self, caplog: pytest.LogCaptureFixture, policy: str
    ) -> None:
        ps = _pool_state({"maxmemory": MEMORY_LIMIT, "maxmemory_policy": policy})

        assert await _run(caplog, ps) == []

    async def test_silent_when_a_default_ttl_is_configured(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        ps = _pool_state({"maxmemory": 0, "maxmemory_policy": "noeviction"})

        assert await _run(caplog, ps, default_ttl=300) == []

    async def test_silent_when_nothing_relies_on_the_default_ttl(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Apps that pass an explicit ttl on every route are not warned."""
        ps = _pool_state({"maxmemory": 0, "maxmemory_policy": "noeviction"})

        assert await _run(caplog, ps, relies=False) == []
        ps.get_async_client.assert_not_called()

    async def test_silent_when_suppressed_by_setting(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        ps = _pool_state({"maxmemory": 0, "maxmemory_policy": "noeviction"})

        assert await _run(caplog, ps, warn=False) == []
        ps.get_async_client.assert_not_called()


# ===================================================================
# The probe is best-effort and must never break startup
# ===================================================================


@pytest.mark.unit
class TestProbeIsBestEffort:
    @pytest.mark.parametrize(
        "exc",
        [
            ResponseError("unknown command 'info'"),
            ConnectionError("Redis unreachable"),
            OSError("socket closed"),
        ],
    )
    async def test_probe_failure_is_swallowed(
        self, caplog: pytest.LogCaptureFixture, exc: Exception
    ) -> None:
        assert await _run(caplog, _failing_pool_state(exc)) == []

    @pytest.mark.parametrize(
        "info",
        [
            {},
            {"maxmemory": "not-an-int", "maxmemory_policy": "noeviction"},
            {"maxmemory": 0},  # policy absent
            {"maxmemory": 0, "maxmemory_policy": None},
            {"maxmemory_policy": "noeviction"},  # maxmemory absent
        ],
    )
    async def test_unparseable_info_is_inconclusive(
        self, caplog: pytest.LogCaptureFixture, info: dict[str, Any]
    ) -> None:
        assert await _run(caplog, _pool_state(info)) == []


# ===================================================================
# Wiring
# ===================================================================


@pytest.mark.unit
class TestCachingMarker:
    def test_add_redis_caching_marks_the_app(self) -> None:
        """The lifespan gates the probe on this marker."""
        from redis_fastapi.cache import add_redis_caching

        app = FastAPI()
        assert getattr(app.state, "_redis_caching", False) is False

        add_redis_caching(app)
        assert app.state._redis_caching is True

    def test_explicit_ttl_does_not_flag_default_reliance(self) -> None:
        cache_mod = _CACHE_MOD

        original = cache_mod._default_ttl_in_use
        try:
            cache_mod._default_ttl_in_use = False
            cache_mod.cache(ttl=60)
            assert cache_mod.relies_on_default_ttl() is False

            cache_mod.cache()
            assert cache_mod.relies_on_default_ttl() is True
        finally:
            cache_mod._default_ttl_in_use = original


@pytest.mark.unit
class TestLifespanGate:
    """The probe must run only when caching is wired."""

    def test_probe_runs_when_caching_is_wired(self) -> None:
        app = FastAPI()
        FastAPIRedis(app).lifespan().caching()

        with patch(
            "redis_fastapi.lifespan._check_cache_eviction_safety", new=AsyncMock()
        ) as probe:
            with TestClient(app):
                pass

        probe.assert_awaited_once()

    def test_probe_skipped_without_caching(self) -> None:
        app = FastAPI()
        FastAPIRedis(app).lifespan()  # no .caching()

        with patch(
            "redis_fastapi.lifespan._check_cache_eviction_safety", new=AsyncMock()
        ) as probe:
            with TestClient(app):
                pass

        probe.assert_not_called()
