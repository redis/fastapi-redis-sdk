"""OTel coverage of the session store — F-18, "every store operation".

Before this file the store emitted spans and metrics for ``load`` and ``save``
only, while ``telemetry.record_session_operation`` documented eight labels.
Half of them were never sent, so a dashboard filtering on them showed a
permanently empty series — which reads as "nothing is happening" rather than
"nothing is measured". Nothing in the suite noticed, because nothing asserted
on the emitted set.

These tests assert the set, not a sample of it: the documented labels and the
emitted labels are compared as whole sets, so adding a label to the docstring
without emitting it fails here, and so does the reverse.
"""

from __future__ import annotations

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)

import redis_fastapi.telemetry as tel
from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import SessionStoreError
from redis_fastapi.session_backend import RedisSessionStore, SessionState

# Every operation the store instruments, and how. Kept here as data because
# the point of these tests is the set, not any one member of it.
COUNTED = {
    "load",
    "create",
    "save",
    "touch",
    "rotate",
    "revoke",
    "revoke_id",
    "revoke_all",
    "list",
    "count",
}
# ``touch`` is one command: a counter confirms the setting does something,
# and a span would cost more than it tells anyone.
TIMED = COUNTED - {"touch"}
# Always part of another operation. Instrumenting them double-counts it.
NEVER = {"delete", "index"}


class _Recorder:
    """Stands in for a counter and a histogram at once."""

    def __init__(self) -> None:
        self.labels: list[dict] = []

    def add(self, amount, attributes=None) -> None:
        self.labels.append(dict(attributes or {}))

    def record(self, value, attributes=None) -> None:
        self.labels.append(dict(attributes or {}))

    def operations(self) -> set[str]:
        return {row["operation"] for row in self.labels}

    def results(self, operation: str) -> list[str]:
        return [
            row["result"]
            for row in self.labels
            if row["operation"] == operation and "result" in row
        ]


@pytest.fixture()
def metrics(monkeypatch) -> tuple[_Recorder, _Recorder]:
    """Enable telemetry with recording instruments in place of real ones."""
    operations, latency = _Recorder(), _Recorder()
    monkeypatch.setattr(tel._state, "enabled", True)
    monkeypatch.setattr(tel._state, "session_operations", operations)
    monkeypatch.setattr(tel._state, "session_latency", latency)
    return operations, latency


class _Exporter(SpanExporter):
    def __init__(self) -> None:
        self.spans: list = []

    def export(self, spans):  # type: ignore[override]
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        pass


@pytest.fixture()
def spans() -> _Exporter:
    """A dedicated in-memory tracer, bypassing the set-once global provider."""
    exporter = _Exporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    original = tel._state
    tel.disable_telemetry()
    tel._state.tracer = provider.get_tracer(tel.TRACER_NAME)
    tel._state.enabled = True
    try:
        yield exporter
    finally:
        tel._state = original


@pytest.fixture()
def store(fake_async_redis) -> RedisSessionStore:
    get_settings.cache_clear()
    return RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)


async def _exercise_every_operation(store: RedisSessionStore) -> None:
    """Call each of the ten instrumented operations exactly once."""
    record = store.new_record({"user_id": "42"})

    first = store.new_id()
    await store.create(first, record)
    await store.load(first)
    await store.save(first, record)
    await store.touch(first)
    await store.index("42", first, record, absolute_remaining=600)

    await store.list_for_subject("42")
    await store.count_for_subject("42")

    rotated = SessionState(data={"user_id": "42"}, session_id=first, subject="42")
    await store.rotate(rotated, subject="42")
    await store.revoke_id(rotated.session_id or "", subject="42")

    second = store.new_id()
    await store.create(second, record)
    await store.index("42", second, record, absolute_remaining=600)
    await store.revoke(SessionState(data={}, session_id=second, subject="42"))

    third = store.new_id()
    await store.create(third, record)
    await store.index("42", third, record, absolute_remaining=600)
    await store.revoke_all("42")


@pytest.mark.unit
class TestEveryOperationIsCounted:
    async def test_the_emitted_labels_are_exactly_the_documented_ones(
        self, store: RedisSessionStore, metrics
    ) -> None:
        operations, _ = metrics
        await _exercise_every_operation(store)
        assert operations.operations() == COUNTED

    async def test_the_docstring_lists_what_is_emitted(self) -> None:
        """The finding was a docstring promising labels nothing sent."""
        documented = tel.record_session_operation.__doc__ or ""
        for operation in COUNTED:
            assert f"``{operation}``" in documented, f"{operation} is undocumented"
        for operation in NEVER:
            assert f"``{operation}`` and" in documented or (
                f"and ``{operation}``" in documented
            ), f"{operation} should be documented as deliberately absent"

    async def test_latency_covers_every_operation_but_touch(
        self, store: RedisSessionStore, metrics
    ) -> None:
        _, latency = metrics
        await _exercise_every_operation(store)
        assert latency.operations() == TIMED
        assert "touch" not in latency.operations()


@pytest.mark.unit
class TestCreateAndSaveReportSeparately:
    """A create is the sign-in rate; a save is an update of one that exists.

    They shared ``_save`` and therefore shared the ``save`` label, which
    understated create and polluted save.
    """

    async def test_a_create_is_not_counted_as_a_save(
        self, store: RedisSessionStore, metrics
    ) -> None:
        operations, latency = metrics
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": "42"}))
        assert operations.operations() == {"create"}
        assert latency.operations() == {"create"}

    async def test_a_save_is_not_counted_as_a_create(
        self, store: RedisSessionStore, metrics
    ) -> None:
        operations, _ = metrics
        sid = store.new_id()
        record = store.new_record({"user_id": "42"})
        await store.create(sid, record)
        operations.labels.clear()
        await store.save(sid, record)
        assert operations.operations() == {"save"}

    async def test_a_rotation_reports_its_own_create(
        self, store: RedisSessionStore, metrics
    ) -> None:
        """The §5.1 backstop needs both: sign-ins with no rotations is the
        anomaly, so creates and rotations must be separately visible."""
        operations, _ = metrics
        sid = store.new_id()
        record = store.new_record({"user_id": "42"})
        await store.create(sid, record)
        operations.labels.clear()
        await store.rotate(SessionState(data={"user_id": "42"}, session_id=sid))
        assert operations.operations() == {"rotate", "create"}


@pytest.mark.unit
class TestOutcomeLabels:
    async def test_a_refused_revoke_id_is_a_miss_not_a_hit(
        self, store: RedisSessionStore, metrics
    ) -> None:
        """A run of these is a broken UI or somebody probing identifiers."""
        operations, _ = metrics
        assert await store.revoke_id(store.new_id(), subject="42") is False
        assert operations.results("revoke_id") == ["miss"]

    async def test_a_malformed_id_never_reaches_redis_and_still_counts(
        self, store: RedisSessionStore, metrics
    ) -> None:
        operations, latency = metrics
        assert await store.revoke_id("not-an-id", subject="42") is False
        assert operations.results("revoke_id") == ["miss"]
        assert latency.operations() == set(), "no round trip, no latency"

    async def test_revoking_a_session_never_written_is_a_miss(
        self, store: RedisSessionStore, metrics
    ) -> None:
        operations, _ = metrics
        await store.revoke(SessionState(data={}))
        assert operations.results("revoke") == ["miss"]

    async def test_an_empty_listing_and_count_are_a_miss_and_a_hit(
        self, store: RedisSessionStore, metrics
    ) -> None:
        operations, _ = metrics
        assert await store.list_for_subject("nobody") == []
        assert await store.count_for_subject("nobody") == 0
        assert operations.results("list") == ["miss"]
        assert operations.results("count") == ["hit"], "zero is an answer"

    async def test_a_failed_composite_counts_itself_and_the_inner_failure(
        self, store: RedisSessionStore, metrics, monkeypatch
    ) -> None:
        """Both series matter: which operation the user lost, and where."""
        operations, _ = metrics

        async def _fail(*args, **kwargs):
            raise ConnectionError("down")

        monkeypatch.setattr(store, "_write", _fail)
        with pytest.raises(SessionStoreError):
            await store.rotate(SessionState(data={"user_id": "42"}))
        assert operations.results("create") == ["error"]
        assert operations.results("rotate") == ["error"]


@pytest.mark.unit
class TestSpans:
    async def test_every_timed_operation_opens_a_span(
        self, store: RedisSessionStore, spans: _Exporter
    ) -> None:
        await _exercise_every_operation(store)
        opened = {span.name for span in spans.spans}
        assert opened == {f"session.{operation}" for operation in TIMED}

    async def test_no_span_carries_an_identifier(
        self, store: RedisSessionStore, spans: _Exporter
    ) -> None:
        """§10: a session ID is a bearer credential and a subject is a user ID.

        Neither may reach a trace, which leaves the vendor's storage far
        outside this store's threat model.
        """
        await _exercise_every_operation(store)
        for span in spans.spans:
            assert not span.attributes, f"{span.name} carried {dict(span.attributes)}"

    async def test_a_rotation_nests_its_create(
        self, store: RedisSessionStore, spans: _Exporter
    ) -> None:
        """The rotate span measures all four round trips, the create span one."""
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": "42"}))
        await store.rotate(SessionState(data={"user_id": "42"}, session_id=sid))

        rotate = next(s for s in spans.spans if s.name == "session.rotate")
        nested = [
            s
            for s in spans.spans
            if s.name == "session.create"
            and s.parent is not None
            and s.parent.span_id == rotate.context.span_id
        ]
        assert nested, "the create inside a rotation was not a child of it"


@pytest.mark.unit
class TestUninstrumentedOnPurpose:
    async def test_delete_and_index_emit_nothing_of_their_own(
        self, store: RedisSessionStore, metrics, spans: _Exporter
    ) -> None:
        """Both are always part of another operation, already inside its span."""
        operations, latency = metrics
        sid = store.new_id()
        record = store.new_record({"user_id": "42"})
        await store.create(sid, record)
        operations.labels.clear()
        latency.labels.clear()
        spans.spans.clear()

        await store.index("42", sid, record, absolute_remaining=600)
        await store.delete(sid)

        assert operations.operations() == set()
        assert latency.operations() == set()
        assert {s.name for s in spans.spans} == set()

    async def test_the_pure_helpers_emit_nothing(
        self, store: RedisSessionStore, metrics
    ) -> None:
        operations, latency = metrics
        record = store.new_record({"user_id": "42"})
        store.is_valid_id(store.new_id())
        store.decode(store.encode(record))
        store.session_id(SessionState(data={}))
        assert operations.labels == []
        assert latency.labels == []
