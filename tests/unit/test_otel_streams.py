"""OTel coverage of resumable streams: the emitted set, not a sample of it.

The operation and result labels ``telemetry.record_stream_operation``
documents are compared with the ones the stream modules send, as whole sets,
so a label added to the docstring without a caller fails here, and so does
the reverse.  The reader gauge, the latency histogram, the spans and the
slow-consumer counter are driven without Redis: each is a few lines of the
SDK with the Redis call behind a stub.
"""

from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import AsyncIterator
from typing import Any

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)

import redis_fastapi.stream_backend as stream_backend
import redis_fastapi.streams as streams
import redis_fastapi.telemetry as tel
from redis_fastapi.stream_backend import StreamBackend, _StreamCapabilities
from redis_fastapi.streams import _SSEResponse

pytestmark = pytest.mark.unit

# The vocabulary ``record_stream_operation`` documents.
DOCUMENTED_OPERATIONS = {
    "open",
    "append",
    "finish",
    "cancel",
    "read",
    "gap",
    "producer_lost",
    "slow_consumer",
}
DOCUMENTED_RESULTS = {
    "ok",
    "created",
    "retry",
    "expired",
    "error",
    "started",
    "resumed",
    "finished",
    "not_found",
    "ahead",
}


def _emitted(pattern: str) -> set[str]:
    source = inspect.getsource(stream_backend) + inspect.getsource(streams)
    return set(re.findall(pattern, source))


class _Recorder:
    """Stands in for a counter, a histogram or a gauge."""

    def __init__(self) -> None:
        self.calls: list[tuple[Any, dict[str, Any]]] = []

    def add(self, amount: Any, attributes: dict[str, Any] | None = None) -> None:
        self.calls.append((amount, dict(attributes or {})))

    def record(self, value: Any, attributes: dict[str, Any] | None = None) -> None:
        self.calls.append((value, dict(attributes or {})))

    def labels(self) -> list[dict[str, Any]]:
        return [attributes for _, attributes in self.calls]


@pytest.fixture()
def metrics(monkeypatch: pytest.MonkeyPatch) -> tuple[_Recorder, _Recorder, _Recorder]:
    """Enable telemetry with recording instruments in place of real ones."""
    operations, latency, readers = _Recorder(), _Recorder(), _Recorder()
    monkeypatch.setattr(tel._state, "enabled", True)
    monkeypatch.setattr(tel._state, "stream_operations", operations)
    monkeypatch.setattr(tel._state, "stream_latency", latency)
    monkeypatch.setattr(tel._state, "stream_readers", readers)
    return operations, latency, readers


class _Exporter(SpanExporter):
    def __init__(self) -> None:
        self.spans: list[Any] = []

    def export(self, spans: Any) -> SpanExportResult:
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        pass


@pytest.fixture()
def spans() -> AsyncIterator[_Exporter]:  # type: ignore[misc]
    """A dedicated in-memory tracer, bypassing the set-once global provider."""
    exporter = _Exporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    original = tel._state
    tel.disable_telemetry()
    tel._state.tracer = provider.get_tracer(tel.TRACER_NAME)
    tel._state.enabled = True
    try:
        yield exporter  # type: ignore[misc]
    finally:
        tel._state = original


def _no_redis() -> Any:
    raise AssertionError("this test must not make a Redis call")


# ---------------------------------------------------------------------------
# The documented set is the emitted set
# ---------------------------------------------------------------------------


def test_every_documented_operation_is_emitted_and_vice_versa() -> None:
    emitted = _emitted(r'record_stream_operation\(\s*operation="(\w+)"')
    assert emitted == DOCUMENTED_OPERATIONS


def test_every_documented_result_is_emitted_and_vice_versa() -> None:
    # ``open`` and ``read`` pick their result with a conditional expression,
    # so the ``else`` arm is a bare string literal: the second pattern.
    emitted = _emitted(r'result="(\w+)"') | _emitted(
        r'result="\w+" if .*? else "(\w+)"'
    )
    assert emitted == DOCUMENTED_RESULTS


def test_latency_is_recorded_for_the_timed_writes_only() -> None:
    assert _emitted(r'timed_stream\("(\w+)"\)') == {"open", "append", "finish"}


def test_span_names_follow_the_other_features() -> None:
    names = _emitted(r'stream_span\("([\w.]+)"\)')
    assert names == {
        "redis_fastapi.streams.open",
        "redis_fastapi.streams.append",
        "redis_fastapi.streams.finish",
    }


# ---------------------------------------------------------------------------
# The instruments, driven without Redis
# ---------------------------------------------------------------------------


def test_an_operation_is_counted_with_both_labels(
    metrics: tuple[_Recorder, _Recorder, _Recorder],
) -> None:
    operations, _, _ = metrics
    tel.record_stream_operation(operation="producer_lost", result="ok")
    assert operations.calls == [(1, {"operation": "producer_lost", "result": "ok"})]


def test_timed_stream_records_latency_under_the_operation(
    metrics: tuple[_Recorder, _Recorder, _Recorder],
) -> None:
    _, latency, _ = metrics
    with tel.timed_stream("append"):
        pass
    assert len(latency.calls) == 1
    duration, labels = latency.calls[0]
    assert duration >= 0 and labels == {"operation": "append"}


def test_the_reader_gauge_follows_the_slots(
    metrics: tuple[_Recorder, _Recorder, _Recorder],
) -> None:
    _, _, readers = metrics
    backend = StreamBackend(_no_redis)
    backend._start(_StreamCapabilities(supported=True))
    assert backend._reserve_reader()
    backend._release_reader()
    assert [amount for amount, _ in readers.calls] == [1, -1]
    assert all(labels == {} for _, labels in readers.calls)  # no stream ID


async def test_a_slow_consumer_is_counted(
    metrics: tuple[_Recorder, _Recorder, _Recorder],
) -> None:
    operations, _, _ = metrics
    backend = StreamBackend(_no_redis)
    backend._start(_StreamCapabilities(supported=True))

    async def body() -> AsyncIterator[str]:
        for i in range(3):
            yield f"data: {i}\n\n"

    async def stalled(message: Any) -> None:
        if message["type"] == "http.response.body":
            await asyncio.sleep(3600)  # the client stopped reading

    async def never() -> Any:
        await asyncio.sleep(3600)

    response = _SSEResponse(
        body(), backend=backend, send_timeout=0.1, media_type="text/event-stream"
    )
    scope = {"type": "http", "asgi": {"spec_version": "2.4"}}
    await asyncio.wait_for(response(scope, never, stalled), 2)
    assert operations.labels() == [{"operation": "slow_consumer", "result": "ok"}]


def test_spans_carry_no_scope_or_stream_id(spans: _Exporter) -> None:
    with tel.stream_span("redis_fastapi.streams.append"):
        pass
    assert [span.name for span in spans.spans] == ["redis_fastapi.streams.append"]
    assert dict(spans.spans[0].attributes or {}) == {}


def test_everything_is_a_no_op_when_disabled() -> None:
    tel.disable_telemetry()
    tel.record_stream_operation(operation="append", result="ok")
    tel.record_stream_latency(duration=0.1, operation="append")
    tel.add_stream_readers(1)
    with (
        tel.timed_stream("open"),
        tel.stream_span("redis_fastapi.streams.open") as span,
    ):
        assert span is None
