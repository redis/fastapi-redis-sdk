"""Tests: OTel spans and metrics emitted during rate-limit checks.

Uses OTel in-memory exporters to verify spans/metrics without a real collector.
"""

from __future__ import annotations

import fakeredis.aioredis
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from starlette.status import HTTP_429_TOO_MANY_REQUESTS

import redis_fastapi.telemetry as tel
from redis_fastapi.deps import get_async_redis
from redis_fastapi.rate_limit import rate_limit

# ---------------------------------------------------------------------------
# In-memory span exporter
# ---------------------------------------------------------------------------


class InMemorySpanExporter(SpanExporter):
    """Collects finished spans in memory for test assertions."""

    def __init__(self) -> None:
        self._spans: list = []

    def export(self, spans):  # type: ignore[override]
        self._spans.extend(spans)
        return SpanExportResult.SUCCESS

    def get_finished_spans(self) -> list:
        return list(self._spans)

    def clear(self) -> None:
        self._spans.clear()

    def shutdown(self) -> None:
        pass


# ---------------------------------------------------------------------------
# OTel test fixtures - module-scoped providers, per-test reset
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _otel_cleanup(request: pytest.FixtureRequest) -> None:
    """Reset telemetry module state before each test."""
    orig = tel._state
    tel.disable_telemetry()

    yield

    tel._state = orig


@pytest.fixture()
def span_exporter() -> InMemorySpanExporter:
    return InMemorySpanExporter()


@pytest.fixture()
def metric_reader() -> InMemoryMetricReader:
    return InMemoryMetricReader()


def _make_otel_app(
    fake: fakeredis.aioredis.FakeRedis,
) -> tuple[FastAPI, list[int]]:
    app = FastAPI()
    counts: list[int] = [0]

    @app.get("/limited", dependencies=[Depends(rate_limit(limit=3, window=60))])
    async def limited_endpoint() -> dict:
        counts[0] += 1
        return {"value": counts[0]}

    async def _fake() -> fakeredis.aioredis.FakeRedis:
        return fake

    app.dependency_overrides[get_async_redis] = _fake
    return app, counts


def _setup_test_otel(
    span_exporter: InMemorySpanExporter,
    metric_reader: InMemoryMetricReader,
) -> None:
    """Configure OTel for a single test using dedicated in-memory exporters.

    Bypasses the global tracer / meter provider so that different test
    files can each use their own exporters without conflicting.
    """
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    tracer = tracer_provider.get_tracer(tel.TRACER_NAME)

    meter_provider = MeterProvider(metric_readers=[metric_reader])
    meter = meter_provider.get_meter(tel.METER_NAME)

    tel._state.tracer = tracer
    tel._state.meter = meter
    tel._state.enabled = True
    tel._state.rate_limit_requests = meter.create_counter(
        name="redis_fastapi.rate_limit.requests",
        description="Total rate limit checks",
        unit="1",
    )


# ===================================================================
# Rate-limit spans
# ===================================================================


@pytest.mark.unit
class TestRateLimitSpans:
    def test_allowed_request_emits_rate_limit_span(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
        span_exporter: InMemorySpanExporter,
    ) -> None:
        _setup_test_otel(span_exporter, InMemoryMetricReader())
        app, _ = _make_otel_app(fake_async_redis)
        with TestClient(app) as c:
            c.get("/limited")

        spans = span_exporter.get_finished_spans()
        rate_spans = [s for s in spans if s.name == "rate_limit.check"]
        assert len(rate_spans) >= 1
        span = rate_spans[0]
        assert span.attributes.get("rate_limit.allowed") is True
        assert span.attributes.get("rate_limit.remaining") is not None
        assert span.attributes.get("rate_limit.backend") is not None

    def test_blocked_request_emits_rate_limit_span(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
        span_exporter: InMemorySpanExporter,
    ) -> None:
        _setup_test_otel(span_exporter, InMemoryMetricReader())
        app, _ = _make_otel_app(fake_async_redis)
        with TestClient(app) as c:
            c.get("/limited")
            c.get("/limited")
            c.get("/limited")
            span_exporter.clear()
            resp = c.get("/limited")

        assert resp.status_code == HTTP_429_TOO_MANY_REQUESTS

        spans = span_exporter.get_finished_spans()
        rate_spans = [s for s in spans if s.name == "rate_limit.check"]
        assert len(rate_spans) >= 1
        span = rate_spans[0]
        assert span.attributes.get("rate_limit.allowed") is False


# ===================================================================
# Rate-limit metrics
# ===================================================================


@pytest.mark.unit
class TestRateLimitMetrics:
    def test_rate_limit_requests_metric_recorded(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
        metric_reader: InMemoryMetricReader,
    ) -> None:
        _setup_test_otel(InMemorySpanExporter(), metric_reader)
        app, _ = _make_otel_app(fake_async_redis)
        with TestClient(app) as c:
            c.get("/limited")  # allowed
            c.get("/limited")  # allowed
            c.get("/limited")  # allowed

        data = metric_reader.get_metrics_data()
        metrics_by_name = {}
        for resource_metrics in data.resource_metrics:
            for scope_metrics in resource_metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    metrics_by_name[metric.name] = metric

        assert "redis_fastapi.rate_limit.requests" in metrics_by_name
