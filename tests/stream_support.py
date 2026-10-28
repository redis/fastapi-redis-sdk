"""Real-Redis fixtures and bounded transaction synchronization for stream regressions."""

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import contextmanager

import pytest
from redis.asyncio import Redis, RedisCluster

from redis_fastapi import StreamBackend
from redis_fastapi.config import get_settings
from redis_fastapi.stream_backend import probe_streams_support
from tests.conftest import REDIS_URL


@pytest.fixture(params=["standalone", "cluster", "enterprise"])
async def stream_case(request, monkeypatch):
    topology = request.param
    url = (
        REDIS_URL
        if topology == "standalone"
        else os.getenv(f"STREAMS_TEST_{topology.upper()}_URL")
    )
    if not url:
        pytest.skip(f"no explicitly supplied {topology} endpoint")
    factory = RedisCluster if topology == "cluster" else Redis
    client = factory.from_url(url, decode_responses=True, socket_timeout=5, protocol=3)
    prefix = f"stream-regression:{uuid.uuid4().hex}"
    monkeypatch.setenv("REDIS_PREFIX", prefix)
    monkeypatch.setenv("REDIS_URL", url)
    monkeypatch.setenv("REDIS_CLUSTER", str(topology == "cluster"))
    get_settings.cache_clear()
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
    def __init__(self, pipe, gate):
        self.pipe = pipe
        self.gate = gate

    def __getattr__(self, name):
        return getattr(self.pipe, name)

    async def __aenter__(self):
        await self.pipe.__aenter__()
        return self

    async def __aexit__(self, *args):
        return await self.pipe.__aexit__(*args)

    async def execute(self, *args, **kwargs):
        if self.gate["remaining"]:
            self.gate["remaining"] -= 1
            if not self.gate["remaining"]:
                self.gate["entered"].set()
            await asyncio.wait_for(self.gate["release"].wait(), 3)
        return await self.pipe.execute(*args, **kwargs)


@contextmanager
def gated_exec(client, monkeypatch, count=1):
    """Pause real transactions before EXEC; leave Redis reads/writes intact."""
    original = type(client).pipeline
    gate = {"remaining": count, "entered": asyncio.Event(), "release": asyncio.Event()}

    def pipeline(instance, *args, **kwargs):
        pipe = original(instance, *args, **kwargs)
        return _PipelineGate(pipe, gate) if instance is client else pipe

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "pipeline", pipeline)
        try:
            yield gate
        finally:
            gate["release"].set()
