"""Real-Redis fixtures and bounded transaction synchronization for stream tests.

``stream_case`` runs a test once per topology: standalone at ``REDIS_URL``,
and an OSS Cluster, a Redis Enterprise database or a Sentinel-managed primary
when ``STREAMS_TEST_CLUSTER_URL``, ``STREAMS_TEST_ENTERPRISE_URL`` or
``STREAMS_TEST_SENTINEL_URL`` names one.  A topology with no endpoint is
skipped, never faked.  Every test gets its own key prefix, removed afterwards.

The Sentinel URL has the form ``redis+sentinel://[:password@]host:port[,host:port]/master``:
the Sentinel nodes, then the primary's name.

Adapted from redis/fastapi-redis-sdk#98.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import contextmanager
from functools import partial
from typing import Any

import pytest
from redis.asyncio import Redis, RedisCluster, Sentinel

from redis_fastapi import StreamBackend
from redis_fastapi.config import get_settings
from redis_fastapi.stream_backend import _StreamCapabilities, probe_streams_support
from tests.conftest import REDIS_URL


def _sentinel_primary(url: str, **options: Any) -> Any:
    """A client for the primary a Sentinel URL names (see the module docstring)."""
    rest = url.split("://", 1)[1]
    password: str | None = None
    if "@" in rest:
        auth, rest = rest.rsplit("@", 1)
        password = auth.split(":", 1)[1] if ":" in auth else auth
    nodes_text, master = rest.rsplit("/", 1)
    nodes = [
        (host, int(port))
        for host, port in (node.rsplit(":", 1) for node in nodes_text.split(","))
    ]
    sentinel = Sentinel(
        nodes, sentinel_kwargs={"password": password}, password=password, **options
    )
    return sentinel.master_for(master)


def stream_client(protocol: int = 3) -> Any:
    """A client for the endpoint of the running ``stream_case``.

    For a test that needs a second process's own connections, or a client
    speaking another protocol.
    """
    options: dict[str, Any] = {
        "decode_responses": True,
        "socket_timeout": 5,
        "protocol": protocol,
    }
    topology = os.environ.get("STREAMS_TEST_TOPOLOGY", "standalone")
    if topology == "sentinel":
        return _sentinel_primary(os.environ["REDIS_URL"], **options)
    factory = RedisCluster if topology == "cluster" else Redis
    return factory.from_url(os.environ["REDIS_URL"], **options)


async def command_calls(client: Any) -> dict[str, int]:
    """How many times the server ran each command, summed over primaries.

    Read it before and after an operation to see what that operation sent.
    """
    if isinstance(client, RedisCluster):
        replies = await client.info("commandstats", target_nodes=RedisCluster.PRIMARIES)
        sections = list(replies.values()) if isinstance(replies, dict) else replies
    else:
        sections = [await client.info("commandstats")]
    calls: dict[str, int] = {}
    for section in sections:
        for name, stats in section.items():
            command = name.removeprefix("cmdstat_")
            calls[command] = calls.get(command, 0) + int(stats["calls"])
    return calls


def stream_backend(client: Any) -> StreamBackend:
    """A backend on *client*: another process, as Redis sees it."""
    backend = StreamBackend(lambda: client)
    backend._start(_StreamCapabilities(supported=True))
    return backend


def configure(monkeypatch: pytest.MonkeyPatch, **values: Any) -> None:
    """Set ``REDIS_*`` settings for one test, with a fresh settings cache.

    Clears the cache on entry only.  ``stream_case`` and ``settings_env``
    clear it again on exit; use one of them with this.
    """
    for name, value in values.items():
        monkeypatch.setenv(f"REDIS_{name.upper()}", str(value))
    get_settings.cache_clear()


@pytest.fixture()
def settings_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[..., None]]:
    """``configure()`` as a fixture: ``settings_env(stream_maxlen=3)``.

    Clears the settings cache before the test and after it, so no value set
    here reaches the next test.  Points ``REDIS_URL`` at the test server.
    """
    monkeypatch.setenv("REDIS_URL", REDIS_URL)
    get_settings.cache_clear()
    yield partial(configure, monkeypatch)
    get_settings.cache_clear()


async def collect(
    backend: StreamBackend, stream_id: str, scope: str, after: str | None = None
) -> list[Any]:
    """Every event of a stream, read to its end."""
    async with await backend.follow(stream_id, scope=scope, after=after) as reader:
        return [event async for event in reader]


@pytest.fixture(params=["standalone", "cluster", "enterprise", "sentinel"])
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
    monkeypatch.setenv("STREAMS_TEST_TOPOLOGY", topology)
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
