"""Unit tests for lifespan.py OpenTelemetry init/shutdown paths."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from redis_fastapi.config import RedisSettings
from redis_fastapi.deps import _PoolState
from redis_fastapi.setup import FastAPIRedis

# ===================================================================
# _init_redis_otel()
# ===================================================================


@pytest.mark.unit
class TestInitRedisOtel:
    def test_returns_otel_instance_when_available(self):
        from redis_fastapi.lifespan import _init_redis_otel

        mock_otel = MagicMock()
        mock_get = MagicMock(return_value=mock_otel)
        mock_config = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "redis.observability": MagicMock(
                    get_observability_instance=mock_get,
                    OTelConfig=mock_config,
                )
            },
        ):
            result = _init_redis_otel()
            assert result is mock_otel
            mock_otel.init.assert_called_once()

    def test_returns_none_when_import_fails(self):
        from redis_fastapi.lifespan import _init_redis_otel

        with patch.dict(
            "sys.modules",
            {
                "redis.observability": None,
                "redis.observability.config": None,
                "redis.observability.providers": None,
            },
        ):
            result = _init_redis_otel()
            assert result is None

    def test_returns_none_on_generic_exception(self):
        from redis_fastapi.lifespan import _init_redis_otel

        mock_mod = MagicMock()
        mock_mod.get_observability_instance.side_effect = RuntimeError("boom")

        with patch.dict("sys.modules", {"redis.observability": mock_mod}):
            result = _init_redis_otel()
            assert result is None


# ===================================================================
# _shutdown_redis_otel()
# ===================================================================


@pytest.mark.unit
class TestShutdownRedisOtel:
    def test_noop_when_none(self):
        from redis_fastapi.lifespan import _shutdown_redis_otel

        _shutdown_redis_otel(None)  # should not raise

    def test_calls_shutdown(self):
        from redis_fastapi.lifespan import _shutdown_redis_otel

        mock_otel = MagicMock()
        _shutdown_redis_otel(mock_otel)
        mock_otel.shutdown.assert_called_once()

    def test_swallows_exception(self):
        from redis_fastapi.lifespan import _shutdown_redis_otel

        mock_otel = MagicMock()
        mock_otel.shutdown.side_effect = RuntimeError("boom")
        _shutdown_redis_otel(mock_otel)  # should not raise


# ===================================================================
# Lifespan with otel_enabled (cache-layer telemetry)
# ===================================================================


@pytest.mark.unit
class TestLifespanCacheOtelEnabled:
    def test_enable_telemetry_called_when_otel_enabled(self):
        """Lifespan calls enable_telemetry when otel_enabled=True."""
        custom = RedisSettings(otel_enabled=True)

        with (
            patch("redis_fastapi.lifespan.get_settings", return_value=custom),
            patch("redis_fastapi.deps.get_settings", return_value=custom),
            patch("redis_fastapi.telemetry.enable_telemetry") as mock_enable,
        ):
            app = FastAPI()
            FastAPIRedis(app).lifespan()

            @app.get("/ping")
            async def ping() -> dict:
                return {"ok": True}

            with TestClient(app) as client:
                client.get("/ping")
                mock_enable.assert_called_once()

    def test_enable_telemetry_not_called_when_disabled(self):
        """Lifespan does not call enable_telemetry when otel_enabled=False."""
        custom = RedisSettings(otel_enabled=False)

        with (
            patch("redis_fastapi.lifespan.get_settings", return_value=custom),
            patch("redis_fastapi.deps.get_settings", return_value=custom),
            patch("redis_fastapi.telemetry.enable_telemetry") as mock_enable,
        ):
            app = FastAPI()
            FastAPIRedis(app).lifespan()

            @app.get("/ping")
            async def ping() -> dict:
                return {"ok": True}

            with TestClient(app) as client:
                client.get("/ping")
                mock_enable.assert_not_called()

    def test_otel_enabled_activates_is_enabled_on_startup(self):
        """REDIS_OTEL_ENABLED / otel_enabled=True enables cache telemetry at startup."""
        import redis_fastapi.telemetry as tel

        custom = RedisSettings(otel_enabled=True)
        original = tel._state
        tel.disable_telemetry()
        try:
            with (
                patch("redis_fastapi.lifespan.get_settings", return_value=custom),
                patch("redis_fastapi.deps.get_settings", return_value=custom),
            ):
                app = FastAPI()
                # Builder only — no explicit .otel(); matches issue #22 reproduction.
                FastAPIRedis(app).lifespan().caching()
                assert tel.is_enabled() is False

                with TestClient(app):
                    assert tel.is_enabled() is True
        finally:
            tel._state = original


# ===================================================================
# Lifespan with otel_redis_enabled
# ===================================================================


@pytest.mark.unit
class TestLifespanOtelIntegration:
    def test_otel_init_called_when_enabled(self):
        """Lifespan calls _init_redis_otel when otel_redis_enabled=True."""
        custom = RedisSettings(otel_redis_enabled=True)

        mock_init = MagicMock(return_value=MagicMock())
        mock_shutdown = MagicMock()

        with (
            patch("redis_fastapi.lifespan.get_settings", return_value=custom),
            patch("redis_fastapi.deps.get_settings", return_value=custom),
            patch("redis_fastapi.lifespan._init_redis_otel", mock_init),
            patch("redis_fastapi.lifespan._shutdown_redis_otel", mock_shutdown),
        ):
            app = FastAPI()
            FastAPIRedis(app).lifespan()

            @app.get("/ping")
            async def ping() -> dict:
                return {"ok": True}

            with TestClient(app) as client:
                client.get("/ping")
                mock_init.assert_called_once()

            mock_shutdown.assert_called_once_with(mock_init.return_value)

    def test_otel_not_called_when_disabled(self):
        """Lifespan does NOT call _init_redis_otel when otel_redis_enabled=False."""
        custom = RedisSettings(otel_redis_enabled=False)

        mock_init = MagicMock()

        with (
            patch("redis_fastapi.lifespan.get_settings", return_value=custom),
            patch("redis_fastapi.deps.get_settings", return_value=custom),
            patch("redis_fastapi.lifespan._init_redis_otel", mock_init),
        ):
            app = FastAPI()
            FastAPIRedis(app).lifespan()

            @app.get("/ping")
            async def ping() -> dict:
                return {"ok": True}

            with TestClient(app) as client:
                client.get("/ping")
                mock_init.assert_not_called()

    def test_cluster_branch_calls_otel_shutdown(self):
        """Cluster lifespan also shuts down OTel."""
        custom = RedisSettings(cluster=True, otel_redis_enabled=True)

        mock_init = MagicMock(return_value=MagicMock())
        mock_shutdown = MagicMock()
        mock_async_cluster = MagicMock()
        mock_async_cluster.aclose = AsyncMock()

        with (
            patch("redis_fastapi.lifespan.get_settings", return_value=custom),
            patch("redis_fastapi.lifespan._init_redis_otel", mock_init),
            patch("redis_fastapi.lifespan._shutdown_redis_otel", mock_shutdown),
            patch.object(
                _PoolState,
                "build_async_cluster",
                return_value=mock_async_cluster,
            ),
        ):
            app = FastAPI()
            FastAPIRedis(app).lifespan()

            @app.get("/ping")
            async def ping() -> dict:
                return {"ok": True}

            with TestClient(app):
                mock_init.assert_called_once()

            mock_shutdown.assert_called_once()


# ===================================================================
# Startup INCREX capability probe
# ===================================================================


@pytest.mark.unit
class TestLifespanRateLimitProbe:
    """INCREX support is settled once at pool init, not per request."""

    def _app(self, *, rate_limiting: bool) -> FastAPI:
        app = FastAPI()
        builder = FastAPIRedis(app).lifespan()
        if rate_limiting:
            builder.rate_limiting()

        @app.get("/ping")
        async def ping() -> dict:
            return {"ok": True}

        return app

    def test_probe_runs_at_startup_when_rate_limiting_is_wired(self):
        app = self._app(rate_limiting=True)
        with patch(
            "redis_fastapi.ratelimit_backend.probe_increx_support",
            new=AsyncMock(return_value=True),
        ) as probe:
            with TestClient(app) as client:
                client.get("/ping")
                # Read before shutdown — teardown clears the pool state.
                caps = app.state._redis.ratelimit_capabilities
                assert caps is not None and caps.supports_increx is True

        probe.assert_awaited_once()

    def test_cache_only_app_pays_no_probe(self):
        # A startup round trip nobody needs also means a boot-time connect to a
        # Redis that a cache-only app may be happy to reach lazily.
        app = self._app(rate_limiting=False)
        with patch(
            "redis_fastapi.ratelimit_backend.probe_increx_support",
            new=AsyncMock(return_value=True),
        ) as probe:
            with TestClient(app) as client:
                client.get("/ping")

        probe.assert_not_awaited()

    def test_unreachable_redis_at_boot_does_not_break_startup(self):
        # The probe is best-effort: detection is deferred, never fatal.
        app = self._app(rate_limiting=True)
        with patch(
            "redis_fastapi.ratelimit_backend.probe_increx_support",
            new=AsyncMock(side_effect=OSError("no route to host")),
        ):
            with TestClient(app) as client:
                assert client.get("/ping").status_code == 200
                # Nothing cached, so the first hit re-probes rather than
                # inheriting a guess made while Redis was down.
                assert app.state._redis.ratelimit_capabilities is None
