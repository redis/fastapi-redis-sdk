"""OpenTelemetry instrumentation for fastapi-redis-sdk cache operations.

Provides spans and metrics for cache(), cache_evict(), cache_put(),
and CacheBackend operations.  All OTel imports are guarded - when the
``opentelemetry`` packages are not installed every helper is a silent
no-op.

Enable via::

    FastAPIRedis(app).lifespan().caching().otel()

Or by setting ``REDIS_OTEL_ENABLED=true``.

Requires: ``pip install fastapi-redis-sdk[otel]``
"""

from __future__ import annotations

import contextlib
import dataclasses
import logging
import time
from collections.abc import Iterator
from typing import Any

logger = logging.getLogger(__name__)

TRACER_NAME = "fastapi-redis-sdk"
METER_NAME = "fastapi-redis-sdk"


# ---------------------------------------------------------------------------
# Telemetry state - populated by enable_telemetry()
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _OTelState:
    """Groups all mutable OTel state into a single object."""

    enabled: bool = False
    tracer: Any = None  # opentelemetry.trace.Tracer | None
    meter: Any = None  # opentelemetry.metrics.Meter | None

    # Cache metric instruments
    cache_requests: Any = None
    cache_evictions: Any = None
    cache_writes: Any = None
    cache_latency: Any = None


_state = _OTelState()


def _try_import_otel() -> tuple[Any, Any] | None:
    """Return ``(trace, metrics)`` modules or ``None`` if not installed."""
    try:
        from opentelemetry import metrics, trace

        return trace, metrics
    except ImportError:
        return None


def enable_telemetry() -> None:
    """Activate OTel instrumentation for cache operations.

    Safe to call multiple times - subsequent calls are no-ops.
    If the ``opentelemetry`` packages are not installed a warning is
    logged and telemetry remains disabled.
    """
    if _state.enabled:
        return

    otel = _try_import_otel()
    if otel is None:
        logger.warning(
            "opentelemetry-api / opentelemetry-sdk not installed; "
            "cache telemetry will be disabled.  "
            "Install with: pip install fastapi-redis-sdk[otel]"
        )
        return

    trace, metrics = otel

    _state.tracer = trace.get_tracer(TRACER_NAME)
    _state.meter = metrics.get_meter(METER_NAME)

    _state.cache_requests = _state.meter.create_counter(
        name="redis_fastapi.cache.requests",
        description="Total cache lookups",
        unit="1",
    )
    _state.cache_evictions = _state.meter.create_counter(
        name="redis_fastapi.cache.evictions",
        description="Cache invalidations",
        unit="1",
    )
    _state.cache_writes = _state.meter.create_counter(
        name="redis_fastapi.cache.writes",
        description="Cache writes",
        unit="1",
    )
    _state.cache_latency = _state.meter.create_histogram(
        name="redis_fastapi.cache.latency",
        description="Cache operation duration",
        unit="s",
    )

    _state.enabled = True
    logger.info("fastapi-redis-sdk OpenTelemetry instrumentation enabled")


def disable_telemetry() -> None:
    """Deactivate OTel instrumentation and reset all state.

    Primarily useful in tests to restore a clean slate between runs.
    """
    global _state  # noqa: PLW0603
    _state = _OTelState()


def is_enabled() -> bool:
    """Return whether telemetry is currently active."""
    return _state.enabled


# ---------------------------------------------------------------------------
# Span helper
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def cache_span(
    name: str,
    attributes: dict[str, Any] | None = None,
) -> Iterator[Any]:
    """Create a span for a cache operation.

    Uses ``start_as_current_span`` so that child spans (e.g. from
    redis-py's native OTel instrumentation) are correctly nested.

    No-op context manager when OTel is disabled or not installed.
    Caller exceptions propagate; the span records them automatically.
    """
    if not _state.enabled or _state.tracer is None:
        yield None
        return
    with _state.tracer.start_as_current_span(
        name,
        attributes=attributes or {},
    ) as span:
        yield span


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------


def record_cache_request(
    *,
    result: str,
    eviction_group: str = "",
) -> None:
    """Record a cache lookup (hit / miss / bypass)."""
    if not _state.enabled or _state.cache_requests is None:
        return
    try:
        _state.cache_requests.add(
            1, {"result": result, "eviction_group": eviction_group}
        )
    except Exception:
        logger.warning("Error recording cache request metric", exc_info=True)


def record_cache_eviction(
    *,
    evict_type: str,
    eviction_group: str = "",
) -> None:
    """Record a cache eviction (key or group)."""
    if not _state.enabled or _state.cache_evictions is None:
        return
    try:
        _state.cache_evictions.add(
            1, {"type": evict_type, "eviction_group": eviction_group}
        )
    except Exception:
        logger.warning("Error recording cache eviction metric", exc_info=True)


def record_cache_write(
    *,
    write_type: str,
    eviction_group: str = "",
) -> None:
    """Record a cache write (miss_fill or write_through)."""
    if not _state.enabled or _state.cache_writes is None:
        return
    try:
        _state.cache_writes.add(
            1, {"type": write_type, "eviction_group": eviction_group}
        )
    except Exception:
        logger.warning("Error recording cache write metric", exc_info=True)


def record_cache_latency(
    *,
    duration: float,
    operation: str,
    eviction_group: str = "",
) -> None:
    """Record cache operation latency in seconds."""
    if not _state.enabled or _state.cache_latency is None:
        return
    try:
        _state.cache_latency.record(
            duration, {"operation": operation, "eviction_group": eviction_group}
        )
    except Exception:
        logger.warning("Error recording cache latency metric", exc_info=True)


@contextlib.contextmanager
def timed_operation(operation: str, eviction_group: str = "") -> Iterator[None]:
    """Context manager that records latency for a cache operation."""
    start = time.monotonic()
    try:
        yield
    finally:
        duration = time.monotonic() - start
        record_cache_latency(
            duration=duration, operation=operation, eviction_group=eviction_group
        )
