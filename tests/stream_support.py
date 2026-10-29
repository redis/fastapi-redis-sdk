"""Real-Redis fixtures and bounded transaction synchronization for stream tests.

``stream_case`` runs a test once per topology: standalone at ``REDIS_URL``,
and an OSS Cluster or a Redis Enterprise database when
``STREAMS_TEST_CLUSTER_URL`` or ``STREAMS_TEST_ENTERPRISE_URL`` names one.  A
topology with no endpoint is skipped, never faked.  Every test gets its own
key prefix, removed afterwards.

Adapted from redis/fastapi-redis-sdk#98.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from redis.asyncio import Redis, RedisCluster

from redis_fastapi import StreamBackend
from redis_fastapi.config import get_settings
from redis_fastapi.stream_backend import (
    _lease_key,
    _StreamCapabilities,
    probe_streams_support,
)
from tests.conftest import REDIS_URL


def stream_client() -> Any:
    """A client for the endpoint of the running ``stream_case``.

    For a test that needs a second process's own connections.
    """
    factory = RedisCluster if os.environ.get("REDIS_CLUSTER") == "True" else Redis
    return factory.from_url(
        os.environ["REDIS_URL"], decode_responses=True, socket_timeout=5, protocol=3
    )


def stream_backend(client: Any) -> StreamBackend:
    """A backend on *client*: another process, as Redis sees it."""
    backend = StreamBackend(lambda: client)
    backend._start(_StreamCapabilities(supported=True))
    return backend


def configure(monkeypatch: pytest.MonkeyPatch, **values: Any) -> None:
    """Set ``REDIS_*`` settings for one test, with a fresh settings cache."""
    for name, value in values.items():
        monkeypatch.setenv(f"REDIS_{name.upper()}", str(value))
    get_settings.cache_clear()


async def collect(
    backend: StreamBackend, stream_id: str, scope: str, after: str | None = None
) -> list[Any]:
    """Every event of a stream, read to its end."""
    async with await backend.follow(stream_id, scope=scope, after=after) as reader:
        return [event async for event in reader]


async def crash(backend: StreamBackend, client: Any) -> None:
    """A hard kill: *backend*'s lease stops being renewed and is gone."""
    assert backend._lease_task is not None
    backend._lease_task.cancel()
    await client.delete(_lease_key(backend._proc))


@pytest.fixture(params=["standalone", "cluster", "enterprise"])
async def stream_case(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[tuple[StreamBackend, Any, str]]:
    topology = request.param
    url = (
        REDIS_URL
        if topology == "standalone"
        else os.getenv(f"STREAMS_TEST_{topology.upper()}_URL")
    )
    if not url:
        pytest.skip(f"no explicitly supplied {topology} endpoint")
    prefix = f"stream-regression:{uuid.uuid4().hex}"
    monkeypatch.setenv("REDIS_PREFIX", prefix)
    monkeypatch.setenv("REDIS_URL", url)
    monkeypatch.setenv("REDIS_CLUSTER", str(topology == "cluster"))
    get_settings.cache_clear()
    client = stream_client()
    backend = StreamBackend(lambda: client)
    ready = False
    try:
        try:
            await client.ping()
            ready = True
        except Exception:
            if topology == "standalone":
                pytest.skip("standalone Redis not reachable")
            raise
        if await probe_streams_support(client) is not True:
            pytest.skip(f"{topology} Redis without stream support")
        backend._start(_StreamCapabilities(supported=True))
        yield backend, client, prefix
    finally:
        try:
            await backend.aclose()
        finally:
            try:
                if ready:
                    async for key in client.scan_iter(match=prefix + "*"):
                        await client.delete(key)
            finally:
                await client.aclose()
                get_settings.cache_clear()


class _PipelineGate:
    def __init__(self, pipe: Any, gate: dict[str, Any]) -> None:
        self.pipe = pipe
        self.gate = gate

    def __getattr__(self, name: str) -> Any:
        return getattr(self.pipe, name)

    async def __aenter__(self) -> _PipelineGate:
        await self.pipe.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> Any:
        return await self.pipe.__aexit__(*args)

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        if self.gate["remaining"]:
            self.gate["remaining"] -= 1
            if not self.gate["remaining"]:
                self.gate["entered"].set()
            await asyncio.wait_for(self.gate["release"].wait(), 3)
        return await self.pipe.execute(*args, **kwargs)


@contextmanager
def gated_exec(
    client: Any, monkeypatch: pytest.MonkeyPatch, count: int = 1
) -> Iterator[dict[str, Any]]:
    """Pause *client*'s next *count* pipelines before EXEC; Redis is untouched.

    ``gate["entered"]`` is set once the last of them is waiting; set
    ``gate["release"]`` to let them run.
    """
    original = type(client).pipeline
    gate: dict[str, Any] = {
        "remaining": count,
        "entered": asyncio.Event(),
        "release": asyncio.Event(),
    }

    def pipeline(instance: Any, *args: Any, **kwargs: Any) -> Any:
        pipe = original(instance, *args, **kwargs)
        return _PipelineGate(pipe, gate) if instance is client else pipe

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "pipeline", pipeline)
        try:
            yield gate
        finally:
            gate["release"].set()
