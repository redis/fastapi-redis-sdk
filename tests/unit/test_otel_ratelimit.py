"""OTel spans for rate-limit checks — verifies the ``ratelimit.backend`` attribute.

Uses an in-memory span exporter to confirm that the execution tier that served
a check (increx / lua) is surfaced on the span for observability.
"""

from __future__ import annotations

import fakeredis.aioredis
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)

import redis_fastapi.telemetry as tel
from redis_fastapi.deps import get_rate_limit_backend
from redis_fastapi.ratelimit import add_redis_rate_limiting, rate_limit
from redis_fastapi.ratelimit_backend import RateLimitBackend


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


@pytest.fixture()
def span_exporter() -> InMemorySpanExporter:
    """Wire telemetry to a dedicated in-memory tracer for this test.

    Bypasses the process-global tracer provider (which is set-once and shared
    across test modules) by assigning ``tel._state.tracer`` directly.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer(tel.TRACER_NAME)

    orig = tel._state
    tel.disable_telemetry()
    tel._state.tracer = tracer
    tel._state.enabled = True
    try:
        yield exporter
    finally:
        tel._state = orig


def _make_app(fake: fakeredis.aioredis.FakeRedis) -> FastAPI:
    app = FastAPI()

    @app.get("/limited", dependencies=[Depends(rate_limit(limit=3, window=60))])
    async def limited() -> dict:
        return {"ok": True}

    async def _fake_backend() -> RateLimitBackend:
        return RateLimitBackend(fake)

    add_redis_rate_limiting(app)
    app.dependency_overrides[get_rate_limit_backend] = _fake_backend
    return app


@pytest.mark.unit
class TestOtelRateLimitBackendAttribute:
    def test_hit_span_reports_backend(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
        span_exporter: InMemorySpanExporter,
    ) -> None:
        app = _make_app(fake_async_redis)
        with TestClient(app) as c:
            assert c.get("/limited").status_code == 200

        spans = span_exporter.get_finished_spans()
        hit_spans = [s for s in spans if s.name == "ratelimit.hit"]
        assert len(hit_spans) >= 1
        # fakeredis rejects INCREX but runs Lua (via lupa), so the real Lua tier
        # serves the check.
        assert hit_spans[0].attributes.get("ratelimit.backend") == "lua"
