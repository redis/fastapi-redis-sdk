"""The write path, the process lease and run handoff, on every Redis topology.

Each test runs once per endpoint ``stream_case`` was given: standalone always,
an OSS Cluster and Redis Enterprise when their URLs are set.  Every process in
a test is a separate backend on its own client, as separate processes would
be.
"""

from __future__ import annotations

import asyncio
import gc
from typing import Any

import pytest
from redis.asyncio import Redis

from redis_fastapi import (
    StreamBackend,
    StreamBusy,
    StreamFinished,
    StreamNotFound,
    StreamWriter,
)
from redis_fastapi.config import get_settings
from redis_fastapi.stream_backend import _keys, _lease_key, probe_streams_support
from tests.stream_support import (
    collect,
    command_calls,
    configure,
    crash,
    gated_exec,
    stream_backend,
    stream_client,
)

pytestmark = pytest.mark.integration

Case = tuple[StreamBackend, Any, str]


class _Processes:
    """Extra processes for one test, each a backend on its own client.

    An async context manager the test enters itself, so every client opens
    and closes on the test's own event loop.
    """

    def __init__(self) -> None:
        self._open: list[tuple[StreamBackend, Any]] = []

    def spawn(self) -> tuple[StreamBackend, Any]:
        client = stream_client()
        backend = stream_backend(client)
        self._open.append((backend, client))
        return backend, client

    async def __aenter__(self) -> _Processes:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        for backend, client in self._open:
            await backend.aclose()
            await client.aclose()


async def test_an_append_writes_only_its_entry(stream_case: Case) -> None:
    backend, client, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="r1")
    keys = _keys("chat:1", "r1")
    await client.expire(keys.stream, 2000)
    for n in range(3):
        await writer.append("delta", {"n": n})
    assert 1990 <= await client.ttl(keys.stream) <= 2000  # no TTL write
    await writer.finish("message", {})
    events = await collect(backend, "r1", "chat:1")
    assert [e.data for e in events] == [{"n": 0}, {"n": 1}, {"n": 2}, {}]


async def test_a_crashed_process_is_reported_lost_and_frees_the_scope(
    stream_case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with _Processes() as processes:
        backend, _, _ = stream_case
        configure(monkeypatch, stream_heartbeat_interval=1)
        crashed, crashed_client = processes.spawn()
        writer = await crashed.open(scope="chat:1", stream_id="r1")
        await writer.append("delta", {})
        with pytest.raises(StreamBusy):
            await backend.open(scope="chat:1", stream_id="r2")
        await crash(crashed, crashed_client)
        events = await asyncio.wait_for(collect(backend, "r1", "chat:1"), 5)
        assert events[-1].data == {"reason": "producer_lost"}
        with pytest.raises(StreamFinished, match="reader"):
            await writer.append("delta", {})
        assert (await backend.open(scope="chat:1", stream_id="r2")).created


async def test_a_dropped_writer_is_reported_lost(
    stream_case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with _Processes() as processes:
        backend, _, _ = stream_case
        configure(monkeypatch, stream_heartbeat_interval=1, stream_lease_interval=1)
        owner, _ = processes.spawn()
        writer = await owner.open(scope="chat:1", stream_id="r1")
        await writer.append("delta", {})
        del writer
        gc.collect()
        events = await asyncio.wait_for(collect(backend, "r1", "chat:1"), 5)
        assert events[-1].data == {"reason": "producer_lost"}


async def test_a_cancel_reaches_another_process_during_a_tool_call(
    stream_case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with _Processes() as processes:
        backend, _, _ = stream_case
        configure(monkeypatch, stream_lease_interval=1, stream_lease_ttl=3)
        owner, _ = processes.spawn()

        async def tool_call(writer: StreamWriter) -> None:
            await writer.append("tool_call", {})
            await asyncio.sleep(3600)

        task = (await owner.start(tool_call, scope="chat:1", stream_id="r1"))._task
        assert task is not None
        await asyncio.sleep(0.05)
        assert await backend.cancel("r1", scope="chat:1")
        await asyncio.wait_for(task, 5)
        events = await collect(backend, "r1", "chat:1")
        assert events[-1].data == {"reason": "cancelled"}


async def test_suspend_then_resume_once_in_another_process(stream_case: Case) -> None:
    async with _Processes() as processes:
        backend, client, _ = stream_case
        writer = await backend.open(scope="chat:1", stream_id="r1")
        await writer.append("tool_call", {"n": 1})
        await writer.suspend(600)
        keys = _keys("chat:1", "r1")
        assert await client.hget(keys.meta, "status") == "suspended"
        assert await client.ttl(keys.stream) >= 600
        with pytest.raises(ValueError, match="suspended"):
            await writer.append("delta", {})
        later, _ = processes.spawn()
        again, _ = processes.spawn()
        resumed = await later.resume("r1", scope="chat:1")
        with pytest.raises(StreamBusy):
            await again.resume("r1", scope="chat:1")  # a resume delivered twice
        await resumed.append("tool_result", {"n": 2})
        await resumed.finish("message", {"n": 3})
        events = await collect(backend, "r1", "chat:1")
        assert [(e.type, e.data) for e in events] == [
            ("tool_call", {"n": 1}),
            ("tool_result", {"n": 2}),
            ("message", {"n": 3}),
        ]
        entries = await client.xrange(keys.stream)
        # The sequence goes on without a hole, so no reader sees a gap.
        assert [f.get("q") for _, f in entries] == [None, "1", "2", "3"]


async def test_cancel_ends_a_suspended_run(stream_case: Case) -> None:
    backend, _, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="r1")
    await writer.suspend(86_400)
    assert await backend.cancel("r1", scope="chat:1")
    events = await asyncio.wait_for(collect(backend, "r1", "chat:1"), 5)
    assert events[-1].data == {"reason": "cancelled"}
    assert await backend.active("chat:1") is None
    assert await backend.cancel("r1", scope="chat:1") is False


async def test_a_reader_never_ends_a_run_resumed_before_its_exec(
    stream_case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with _Processes() as processes:
        # The reader decided the run was lost; another process resumes it before
        # the reader's EXEC.  The WATCH on meta aborts the end.
        backend, _, _ = stream_case
        crashed, crashed_client = processes.spawn()
        writer = await crashed.open(scope="chat:1", stream_id="r1")
        await writer.append("delta", {})
        await crash(crashed, crashed_client)
        reader, reader_client = processes.spawn()
        plan = await reader._prepare("r1", scope="chat:1", after=None)
        assert plan is not None
        owner = await reader_client.hget(plan.keys.meta, "owner")
        with gated_exec(reader_client, monkeypatch) as gate:
            ending = asyncio.create_task(reader._end_lost(plan, owner))
            await asyncio.wait_for(gate["entered"].wait(), 3)
            resumed = await backend.resume("r1", scope="chat:1")
            gate["release"].set()
            assert await asyncio.wait_for(ending, 5) == ""  # moved on: left alone
        await resumed.append("delta", {})
        final = await resumed.finish("message", {})
        events = await collect(backend, "r1", "chat:1")
        assert events[-1].id == final and events[-1].type == "message"


async def test_concurrent_starts_admit_one_run(stream_case: Case) -> None:
    backend, client, _ = stream_case
    results = await asyncio.gather(
        *(backend.open(scope="chat:1", stream_id=f"r{i}") for i in range(6)),
        return_exceptions=True,
    )
    created = [r for r in results if isinstance(r, StreamWriter)]
    assert len(created) == 1
    assert all(isinstance(r, StreamBusy) for r in results if r not in created)
    # Only the winner left keys behind: no other run has meta.
    for i in range(6):
        meta = _keys("chat:1", f"r{i}").meta
        assert bool(await client.exists(meta)) == (f"r{i}" == created[0].stream_id)


# ---------------------------------------------------------------------------
# Support detection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("protocol", [2, 3])
async def test_the_probe_reads_either_reply_shape(
    stream_case: Case, protocol: int
) -> None:
    # COMMAND INFO is a list on RESP2 and a map on RESP3.
    client = stream_client(protocol)
    try:
        assert await probe_streams_support(client) is True
    finally:
        await client.aclose()


async def test_detection_runs_once_and_asks_again_after_unknown(
    stream_case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    import redis_fastapi.stream_backend as stream_backend_module

    _, client, _ = stream_case
    probes = {"n": 0}

    async def counted(target: Any) -> bool | None:
        probes["n"] += 1
        return await probe_streams_support(target)

    monkeypatch.setattr(stream_backend_module, "probe_streams_support", counted)
    unreachable = Redis(host="127.0.0.1", port=1, socket_connect_timeout=1)
    clients = iter([unreachable, client])
    backend = StreamBackend(lambda: next(clients))
    try:
        assert await backend.detect() is None  # nothing answered: unknown
        assert await backend.detect() is True
        assert await backend.detect() is True  # cached: no third probe
        assert probes["n"] == 2
    finally:
        await unreachable.aclose()


# ---------------------------------------------------------------------------
# What an append sends, and the process lease
# ---------------------------------------------------------------------------


async def test_an_append_runs_one_xadd_and_one_read(
    stream_case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, client, _ = stream_case
    configure(monkeypatch, stream_lease_interval=3600)  # no renewal mid-test
    writer = await backend.open(scope="chat:1", stream_id="r1")
    await writer.append("delta", {"n": 0})
    before = await command_calls(client)
    await writer.append("delta", {"n": 1})
    after = await command_calls(client)
    sent = {
        name: after[name] - before.get(name, 0)
        for name in after
        if after[name] != before.get(name, 0) and not name.startswith("info")
    }
    assert sent == {"xadd": 1, "hmget": 1}


async def test_lease_renewal_lists_the_runs_and_collects_cancels(
    stream_case: Case,
) -> None:
    backend, client, prefix = stream_case
    writer = await backend.open(scope="chat:1", stream_id="r1")
    lease = _lease_key(backend._proc)
    assert 0 < await client.ttl(lease) <= get_settings().stream_lease_ttl
    # Another process asks this one to cancel r1.
    cancels = f"{prefix}:stream-cancel:{{{backend._proc}}}"
    await client.sadd(cancels, "chat:1:r1")
    await backend._renew_lease()
    assert await client.smembers(lease) == {"", "chat:1:r1"}
    assert writer.cancelled
    assert not await client.exists(cancels)


async def test_a_dropped_writer_leaves_the_lease(stream_case: Case) -> None:
    backend, client, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="r1")
    await writer.append("delta", {})
    del writer
    gc.collect()
    await backend._renew_lease()
    assert await client.smembers(_lease_key(backend._proc)) == {""}


# ---------------------------------------------------------------------------
# What a reader concludes about a quiet run
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("state", "entries", "expected"),
    [
        ("running", "kept", "quiet"),
        ("suspended", "kept", "quiet"),
        ("finished", "kept", "quiet"),
        ("lost", "kept", "ended"),
        ("running", "expired", "gone"),
        ("suspended", "expired", "gone"),
        ("finished", "expired", "gone"),
        ("lost", "expired", "gone"),
    ],
)
async def test_a_quiet_reader_checks_the_producer(
    stream_case: Case, state: str, entries: str, expected: str
) -> None:
    backend, client, _ = stream_case
    keys = _keys("chat:1", "r1")
    async with _Processes() as processes:
        owner, owner_client = processes.spawn()
        writer = await owner.open(scope="chat:1", stream_id="r1")
        await writer.append("delta", {})
        if state == "suspended":
            await writer.suspend(600)
        elif state == "finished":
            await writer.finish("message", {})
        elif state == "lost":
            await crash(owner, owner_client)
        plan = await backend._prepare("r1", scope="chat:1", after=None)
        assert plan is not None
        if entries == "expired":
            await client.delete(keys.stream)
        assert await backend._check_producer(plan) == expected
    if entries == "expired":
        assert not await client.exists(keys.stream)  # never recreated
    if expected == "ended":
        events = await collect(backend, "r1", "chat:1")
        assert events[-1].data == {"reason": "producer_lost"}
        assert await client.ttl(keys.stream) <= get_settings().stream_done_ttl
        assert await client.get(keys.active) is None  # the slot is free


# ---------------------------------------------------------------------------
# Transactions through each client protocol
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("protocol", [2, 3])
async def test_resume_and_read_through_either_protocol(
    stream_case: Case, protocol: int
) -> None:
    # redis-py applies a reply callback per command inside a transaction, and
    # some RESP2 Cluster callbacks take no options: the lease renewal's
    # SMEMBERS and the reads in resume() and follow() all go through them.
    client = stream_client(protocol)
    backend = stream_backend(client)
    try:
        first = await backend.open(scope="chat:1", stream_id="r1")
        await first.append("tool_call", {"n": 1})
        await first.suspend(600)
        resumed = await backend.resume("r1", scope="chat:1")
        await resumed.append("tool_result", {"n": 2})
        await resumed.finish("message", {"n": 3})
        events = await collect(backend, "r1", "chat:1")
        assert [e.data for e in events] == [{"n": 1}, {"n": 2}, {"n": 3}]
        with pytest.raises(StreamNotFound):
            await backend.follow("nope", scope="chat:1")
    finally:
        await backend.aclose()
        await client.aclose()
