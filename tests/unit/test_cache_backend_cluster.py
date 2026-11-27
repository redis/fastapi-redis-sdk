"""CacheBackend.delete_group() against a Redis Cluster client."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.crc import key_slot

from redis_fastapi.cache_backend import CacheBackend


def _cluster(keys: list[str]) -> MagicMock:
    async def scan_iter(**_: Any) -> AsyncIterator[str]:
        for key in keys:
            yield key

    client = MagicMock(spec=AsyncRedisCluster)
    client.eval = AsyncMock(return_value=0)
    client.delete = AsyncMock(return_value=len(keys))
    client.scan_iter = scan_iter
    return client


@pytest.mark.asyncio
async def test_group_eviction_is_routed_to_the_group_slot() -> None:
    client = _cluster([])
    backend = CacheBackend(client)

    await backend.delete_group("users")

    _script, numkeys, routing_key, *_ = client.eval.await_args.args
    assert numkeys == 1
    assert key_slot(routing_key.encode()) == key_slot(
        backend._build_key("42", "users").encode()
    )


@pytest.mark.asyncio
async def test_full_wipe_scans_every_primary() -> None:
    keys = ["a", "b"]
    client = _cluster(keys)

    assert await CacheBackend(client).delete_group() == 2

    client.eval.assert_not_awaited()
    client.delete.assert_awaited_once_with(*keys)
