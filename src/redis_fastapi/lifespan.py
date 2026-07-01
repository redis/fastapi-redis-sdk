"""Lifespan context manager for Redis connection pool management."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from redis_fastapi.config import get_settings
from redis_fastapi.deps import _PoolState

logger = logging.getLogger(__name__)


def _init_redis_otel() -> Any:
    """Initialize redis-py native OTel.  Returns the instance or None."""
    try:
        # redis-py >=7.4 re-exports from redis.observability directly;
        # 7.x keeps them in submodules.
        try:
            from redis.observability import (
                OTelConfig,
                get_observability_instance,
            )
        except ImportError:
            from redis.observability.config import OTelConfig
            from redis.observability.providers import (
                get_observability_instance,
            )

        otel = get_observability_instance()
        otel.init(OTelConfig())
        logger.info("redis-py native OpenTelemetry instrumentation enabled")
        return otel
    except ImportError:
        logger.warning(
            "redis[otel] not installed; skipping redis-py OTel init.  "
            "Install with: pip install redis[otel]"
        )
        return None
    except (RuntimeError, TypeError, ValueError):
        logger.warning("Failed to initialize redis-py OTel", exc_info=True)
        return None


def _shutdown_redis_otel(otel: Any) -> None:
    """Shut down redis-py native OTel if it was initialised."""
    if otel is None:
        return
    try:
        otel.shutdown()
    except Exception:
        logger.debug("Error shutting down redis-py OTel", exc_info=True)


@asynccontextmanager
async def redis_lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage Redis connection pools across the application lifecycle.

    Usage::

        app = FastAPI(lifespan=redis_lifespan)

    Supports both standalone and OSS Cluster modes based on
    ``get_settings().cluster``.

    When ``settings.otel_redis_enabled`` is ``True``, also initializes
    redis-py's native OpenTelemetry integration on startup and shuts it
    down on teardown.
    """
    settings = get_settings()

    # -- redis-py native OTel (optional) -----------------------------------
    otel_instance: Any = None
    if settings.otel_redis_enabled:
        otel_instance = _init_redis_otel()

    ps = _PoolState()
    app.state._redis = ps

    if settings.cluster:
        ps.async_cluster = _PoolState.build_async_cluster()
    else:
        ps.async_pool = _PoolState.build_async_pool()

    try:
        yield
    finally:
        ps.clear()
        if settings.cluster:
            await ps.async_cluster.aclose()  # type: ignore[union-attr]
            ps.async_cluster = None
        else:
            await ps.async_pool.aclose()  # type: ignore[union-attr]
            ps.async_pool = None
        _shutdown_redis_otel(otel_instance)
