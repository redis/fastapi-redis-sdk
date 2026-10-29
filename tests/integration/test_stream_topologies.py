"""The write path, silence, cancels and support detection, on every topology.

Each test runs once per endpoint ``stream_case`` was given: standalone always,
an OSS Cluster and Redis Enterprise when their URLs are set.  Every process in
a test is a separate backend on its own client, as separate processes would
be.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from redis.asyncio import Redis

from redis_fastapi import StreamBackend, StreamNotFound, StreamWriter
from redis_fastapi.config import get_settings
from redis_fastapi.stream_backend import _key, probe_streams_support
from tests.stream_support import (
    collect,
    command_calls,
    configure,
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


async def _sent(client: Any, before: dict[str, int]) -> dict[str, int]:
    """The commands the server ran since *before*, without the INFO reads."""
    after = await command_calls(client)
    return {
        name: after[name] - before.get(name, 0)
        for name in after
        if after[name] != before.get(name, 0) and not name.startswith("info")
    }


def _finals(entries: list[Any]) -> list[dict[str, Any]]:
    return [fields for _, fields in entries if fields.get("f") == "1"]


# ---------------------------------------------------------------------------
# What each operation sends
# ---------------------------------------------------------------------------


async def test_open_is_one_transaction_and_one_config(stream_case: Case) -> None:
    backend, client, _ = stream_case
    before = await command_calls(client)
    writer = await backend.open(scope="chat:1", stream_id="r1")
    assert writer.created
    sent = await _sent(client, before)
    assert sent.pop("xadd") == 1 and sent.pop("expire") == 1
    assert sent.pop("xcfgset") == 1  # only for the call that created the run
    assert set(sent) <= {"multi", "exec"}
    before = await command_calls(client)
    assert not (await backend.open(scope="chat:1", stream_id="r1")).created
    assert "xcfgset" not in await _sent(client, before)
    assert 0 < await client.ttl(_key("chat:1", "r1")) <= get_settings().stream_max_run


async def test_an_append_is_one_xadd(stream_case: Case) -> None:
    backend, client, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="r1")
    await writer.append("delta", {"n": 0})
    before = await command_calls(client)
    await writer.append("delta", {"n": 1})
    assert await _sent(client, before) == {"xadd": 1}


async def test_an_append_leaves_the_lifetime_alone(stream_case: Case) -> None:
    backend, client, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="r1")
    key = _key("chat:1", "r1")
    await client.expire(key, 2000)
    for n in range(3):
        await writer.append("delta", {"n": n})
    assert 1990 <= await client.ttl(key) <= 2000
    await writer.finish("message", {})
    events = await collect(backend, "r1", "chat:1")
    assert [e.data for e in events] == [{"n": 0}, {"n": 1}, {"n": 2}, {}]


async def test_concurrent_appends_store_every_event(stream_case: Case) -> None:
    # Each append takes its own sequence number before it is sent, so two in
    # flight never share an idempotency key, and Redis keeps both.
    backend, client, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="r1")
    first, second = await asyncio.gather(
        writer.append("tool_call", {"n": 1}), writer.append("tool_call", {"n": 2})
    )
    assert first != second
    entries = await client.xrange(_key("chat:1", "r1"))
    assert sorted(f["q"] for _, f in entries if "q" in f) == ["1", "2"]


async def test_ending_twice_never_extends_the_run(stream_case: Case) -> None:
    backend, client, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="r1")
    final = await writer.finish("message", {})
    key = _key("chat:1", "r1")
    await client.expire(key, 30)  # most of stream_done_ttl has passed
    await backend.cancel("r1", scope="chat:1")
    assert await client.ttl(key) <= 30
    assert len(_finals(await client.xrange(key))) == 1
    assert (await collect(backend, "r1", "chat:1"))[-1].id == final


async def test_a_created_run_widens_its_idmp_window_first(stream_case: Case) -> None:
    # Redis's default IDMP window is 100 s.  open() widens it to the run's
    # whole life before anything else writes, because XCFGSET clears the
    # IDMP history: an append made after open() is still deduplicated.
    backend, client, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="r1")
    first = await writer.append("delta", {})
    retried = await backend._command(
        ["XADD", _key("chat:1", "r1"), "NOMKSTREAM", "IDMP", writer._writer_id, "1"]
        + ["*", "t", "delta", "d", "{}", "q", "1"]
    )
    assert retried == first  # the same pair again: deduplicated
    info = await client.xinfo_stream(_key("chat:1", "r1"))
    settings = get_settings()
    assert int(info["idmp-duration"]) == max(
        settings.stream_max_run, settings.stream_done_ttl
    )


async def test_a_retried_start_is_not_created_again(stream_case: Case) -> None:
    backend, client, _ = stream_case
    results = await asyncio.gather(
        *(backend.open(scope="chat:1", stream_id="r1") for _ in range(6))
    )
    assert sum(w.created for w in results) == 1
    entries = await client.xrange(_key("chat:1", "r1"))
    assert {f["t"] for _, f in entries} == {"stream.start"}  # hidden from readers
    writer = next(w for w in results if w.created)
    await writer.finish("message", {})
    assert [e.type for e in await collect(backend, "r1", "chat:1")] == ["message"]


# ---------------------------------------------------------------------------
# Silence: a lost producer, and keepalive()
# ---------------------------------------------------------------------------


async def test_a_silent_producer_is_reported_lost(
    stream_case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, client, _ = stream_case
    configure(monkeypatch, stream_heartbeat_interval=1, stream_silence_timeout=3)
    async with _Processes() as processes:
        crashed, _ = processes.spawn()
        writer = await crashed.open(scope="chat:1", stream_id="r1")
        await writer.append("delta", {})
        # The producer writes nothing more, as after a hard kill.
        events = await asyncio.wait_for(collect(backend, "r1", "chat:1"), 10)
        assert [e.type for e in events] == ["delta", "stream.end"]
        assert events[-1].data == {"reason": "producer_lost"}
        key = _key("chat:1", "r1")
        assert await client.ttl(key) <= get_settings().stream_done_ttl
        # A producer that was only stalled keeps writing, after the end.
        await writer.append("delta", {"late": True})
        assert await writer.finish("message", {}) == events[-1].id
        assert len(_finals(await client.xrange(key))) == 1


async def test_keepalive_keeps_a_quiet_producer_alive(
    stream_case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, _, _ = stream_case
    configure(monkeypatch, stream_heartbeat_interval=1, stream_silence_timeout=3)
    released = asyncio.Event()

    async def slow_tool(writer: StreamWriter) -> None:
        await writer.append("tool_call", {})
        await released.wait()  # quiet for longer than the silence timeout
        await writer.append("tool_result", {})

    async with _Processes() as processes:
        owner, _ = processes.spawn()
        await owner.start(slow_tool, scope="chat:1", stream_id="r1")
        reading = asyncio.create_task(collect(backend, "r1", "chat:1"))
        await asyncio.sleep(5)  # the quiet stretch itself
        assert not reading.done()
        released.set()
        events = await asyncio.wait_for(reading, 10)
    assert [e.type for e in events] == ["tool_call", "tool_result", "stream.end"]
    assert events[-1].data == {"reason": "done"}  # pings never shown


async def test_a_reader_stops_when_the_stream_disappears(
    stream_case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, client, _ = stream_case
    configure(monkeypatch, stream_heartbeat_interval=1, stream_silence_timeout=3)
    await backend.open(scope="chat:1", stream_id="r1")
    reader = await backend.follow("r1", scope="chat:1")  # checks pass first

    async def drain() -> list[Any]:
        async with reader:
            return [event async for event in reader]

    reading = asyncio.create_task(drain())
    await client.delete(_key("chat:1", "r1"))
    assert await asyncio.wait_for(reading, 10) == []
    assert not await client.exists(_key("chat:1", "r1"))  # never recreated


# ---------------------------------------------------------------------------
# Cancels and concurrent ends
# ---------------------------------------------------------------------------


async def _forever(writer: StreamWriter) -> None:
    while True:
        await writer.append("delta", {})
        await asyncio.sleep(0.05)


async def test_a_cancel_in_the_producers_process_stops_it(stream_case: Case) -> None:
    backend, client, _ = stream_case
    writer = await backend.start(_forever, scope="chat:1", stream_id="r1")
    assert writer._task is not None
    await backend.cancel("r1", scope="chat:1")
    await asyncio.wait_for(writer._task, 5)
    events = await collect(backend, "r1", "chat:1")
    assert events[-1].data == {"reason": "cancelled"}
    assert len(_finals(await client.xrange(_key("chat:1", "r1")))) == 1


async def test_a_cancel_from_another_process_ends_the_run_for_readers(
    stream_case: Case,
) -> None:
    backend, client, _ = stream_case
    async with _Processes() as processes:
        owner, _ = processes.spawn()
        writer = await owner.start(_forever, scope="chat:1", stream_id="r1")
        assert writer._task is not None
        await backend.cancel("r1", scope="chat:1")
        events = await asyncio.wait_for(collect(backend, "r1", "chat:1"), 5)
        assert events[-1].data == {"reason": "cancelled"}
        # The producer is not told: it goes on until its own process stops it.
        await asyncio.sleep(0.2)
        assert not writer._task.done()
    key = _key("chat:1", "r1")
    assert len(_finals(await client.xrange(key))) == 1  # its end added nothing


async def test_a_cancel_racing_a_finish_leaves_one_final_entry(
    stream_case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, client, _ = stream_case
    async with _Processes() as processes:
        canceller, canceller_client = processes.spawn()
        writer = await backend.open(scope="chat:1", stream_id="r1")
        await writer.append("delta", {})
        with gated_exec(canceller_client, monkeypatch) as gate:
            cancelling = asyncio.create_task(canceller.cancel("r1", scope="chat:1"))
            await asyncio.wait_for(gate["entered"].wait(), 3)
            final = await writer.finish("message", {"won": True})
            gate["release"].set()
            await asyncio.wait_for(cancelling, 5)
    finals = _finals(await client.xrange(_key("chat:1", "r1")))
    assert [f["t"] for f in finals] == ["message"]
    events = await collect(backend, "r1", "chat:1")
    assert events[-1].id == final


async def test_cancelling_an_unknown_run_is_not_found(stream_case: Case) -> None:
    backend, client, _ = stream_case
    with pytest.raises(StreamNotFound):
        await backend.cancel("nope", scope="chat:1")
    assert not await client.exists(_key("chat:1", "nope"))  # never created


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
# Through each client protocol
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("protocol", [2, 3])
async def test_a_run_through_either_protocol(stream_case: Case, protocol: int) -> None:
    # redis-py applies a reply callback per command inside a transaction, and
    # some RESP2 Cluster callbacks take no options.
    client = stream_client(protocol)
    backend = stream_backend(client)
    try:
        writer = await backend.open(scope="chat:1", stream_id="r1")
        assert writer.created
        assert not (await backend.open(scope="chat:1", stream_id="r1")).created
        await writer.append("delta", {"n": 1})
        await writer.append("delta", {"n": 2})
        await writer.finish("message", {"n": 3})
        events = await collect(backend, "r1", "chat:1")
        assert [e.data for e in events] == [{"n": 1}, {"n": 2}, {"n": 3}]
        await backend.cancel("r1", scope="chat:1")  # after the end: no change
        with pytest.raises(StreamNotFound):
            await backend.follow("nope", scope="chat:1")
    finally:
        await backend.aclose()
        await client.aclose()
