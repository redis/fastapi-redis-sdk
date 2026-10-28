"""Heartbeat retention, extension and ownership boundaries against real Redis."""

import asyncio
import json
import time

import pytest

from redis_fastapi import StreamExpired, StreamFenced, StreamFinished
from redis_fastapi.config import get_settings
from redis_fastapi.stream_backend import _keys
from tests.stream_support import gated_exec

pytestmark = pytest.mark.integration


def policy(monkeypatch, **values):
    for key, value in values.items():
        monkeypatch.setenv("REDIS_" + key.upper(), str(value))
    get_settings.cache_clear()


async def test_spawn_keeps_payload_and_metadata_past_their_original_ttl(
    stream_case, monkeypatch
):
    backend, client, _ = stream_case
    policy(
        monkeypatch,
        stream_active_ttl=2,
        stream_gone_ttl=0,
        stream_heartbeat_interval=1,
        stream_alive_ttl=3,
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def producer(writer):
        await writer.append("delta", {"n": 0})
        started.set()
        await release.wait()
        await writer.append("delta", {"n": 1})
        await writer.finish("message", {})

    writer = await backend.open(scope="chat:1", stream_id="long")
    task = backend.spawn(writer, producer)
    try:
        await asyncio.wait_for(started.wait(), 2)
        keys = _keys("chat:1", "long")
        # Observe all keys repeatedly across the original two-second deadline.
        deadline = time.monotonic() + 2.25
        while time.monotonic() < deadline:
            assert await client.exists(keys.stream)
            assert await client.exists(keys.meta)
            assert (
                await client.execute_command("HPTTL", keys.meta, "FIELDS", 1, "alive")
            )[0] > 0
            await asyncio.sleep(0.02)
        release.set()
        await asyncio.wait_for(task, 2)
        entries = await client.xrange(keys.stream)
        assert [json.loads(fields["d"]) for _, fields in entries[:2]] == [
            {"n": 0},
            {"n": 1},
        ]
        assert entries[-1][1]["f"] == "1"
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_heartbeat_does_not_shorten_explicit_extension(stream_case, monkeypatch):
    backend, client, _ = stream_case
    policy(
        monkeypatch,
        stream_active_ttl=2,
        stream_gone_ttl=2,
        stream_heartbeat_interval=1,
        stream_alive_ttl=3,
    )
    writer = await backend.open(scope="chat:1", stream_id="extend")
    await writer.append("delta", {})
    await writer.extend(30)
    keys = _keys("chat:1", "extend")
    # This is the operation used by the heartbeat; observe its committed TTLs.
    await writer._renew(1000, 200)
    for key in [keys.stream, keys.active]:
        assert 28_000 <= await client.pttl(key) <= 30_000
    assert 30_000 <= await client.pttl(keys.meta) <= 32_000
    assert (
        28_000
        <= (await client.execute_command("HPTTL", keys.meta, "FIELDS", 1, "alive"))[0]
        <= 30_000
    )


@pytest.mark.parametrize("extend", [False, True])
async def test_takeover_before_renew_exec_prevents_stale_ttl_changes(
    stream_case, monkeypatch, extend
):
    backend, client, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="owner")
    await writer.append("delta", {})
    keys = _keys("chat:1", "owner")
    with gated_exec(client, monkeypatch) as gate:
        operation = (
            writer.extend(3600) if extend else writer._renew(3_600_000, 3_600_000)
        )
        pending = asyncio.create_task(operation)
        try:
            await asyncio.wait_for(gate["entered"].wait(), 2)
            await backend.attach("owner", scope="chat:1")
            gate["release"].set()
            with pytest.raises(StreamFenced):
                await asyncio.wait_for(pending, 2)
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
    assert await client.pttl(keys.stream) <= 600_000
    assert (await client.execute_command("HPTTL", keys.meta, "FIELDS", 1, "alive"))[
        0
    ] <= 45_000


async def test_finished_stream_retention_cannot_be_extended(stream_case):
    backend, client, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="finished")
    await writer.finish("message", {})
    keys = _keys("chat:1", "finished")
    before = await client.pttl(keys.stream)
    with pytest.raises(StreamFinished):
        await writer._renew(3_600_000, 3_600_000)
    with pytest.raises(StreamFinished):
        await writer.extend(3600)
    assert 0 < await client.pttl(keys.stream) <= before
    assert await client.hget(keys.meta, "status") == "finished"


async def test_missing_history_is_not_recreated_or_renewed(stream_case):
    backend, client, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="expired")
    await writer.append("delta", {})
    keys = _keys("chat:1", "expired")
    await client.delete(keys.stream)
    before = await client.pttl(keys.meta)
    with pytest.raises(StreamExpired):
        await writer._renew(3_600_000, 3_600_000)
    assert not await client.exists(keys.stream)
    assert await client.pttl(keys.meta) <= before


async def test_finish_before_renew_exec_preserves_terminal_retention(
    stream_case, monkeypatch
):
    backend, client, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="finishing")
    await writer.append("delta", {})
    keys = _keys("chat:1", "finishing")
    with gated_exec(client, monkeypatch) as gate:
        pending = asyncio.create_task(writer._renew(3_600_000, 3_600_000))
        try:
            await asyncio.wait_for(gate["entered"].wait(), 2)
            await writer.finish("message", {})
            before = await client.pttl(keys.stream)
            gate["release"].set()
            with pytest.raises(StreamFinished):
                await asyncio.wait_for(pending, 2)
            assert 0 < await client.pttl(keys.stream) <= before
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


async def test_history_removed_before_renew_exec_is_not_revived(
    stream_case, monkeypatch
):
    backend, client, _ = stream_case
    writer = await backend.open(scope="chat:1", stream_id="disappearing")
    await writer.append("delta", {})
    keys = _keys("chat:1", "disappearing")
    with gated_exec(client, monkeypatch) as gate:
        pending = asyncio.create_task(writer._renew(3_600_000, 3_600_000))
        try:
            await asyncio.wait_for(gate["entered"].wait(), 2)
            await client.delete(keys.stream)
            before = await client.pttl(keys.meta)
            gate["release"].set()
            with pytest.raises(StreamExpired):
                await asyncio.wait_for(pending, 2)
            assert not await client.exists(keys.stream)
            assert await client.pttl(keys.meta) <= before
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
