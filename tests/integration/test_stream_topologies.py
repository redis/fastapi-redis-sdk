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

from redis_fastapi import StreamBackend, StreamBusy, StreamFinished, StreamWriter
from redis_fastapi.stream_backend import _keys
from tests.stream_support import (
    collect,
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
