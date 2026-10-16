"""Integration tests for the problem claimed in PR #28.

A corrupt cache entry falls through to the miss path.  The claim is that the
lookup is then counted twice - once as a hit, once as a miss - and that the
``cache.get`` span is marked ``cache.hit=True`` before it is reset to False.
"""

from __future__ import annotations

import contextlib
import importlib
from collections.abc import Iterator
from typing import Any

import pytest
import redis as sync_redis
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from redis_fastapi.cache import cache
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import requires_redis

# ``redis_fastapi.cache`` the function shadows the module on attribute access.
cache_module = importlib.import_module("redis_fastapi.cache")

CORRUPT_ENTRIES = {
    "invalid_json": b"not-valid-json{{{",
    "missing_keys": b"{}",
    "not_an_object": b"123",
    "not_utf8": b"\x80\x81\x82\x83",
}


class _RecordingSpan:
    def __init__(self) -> None:
        self.hit_values: list[object] = []

    def set_attribute(self, key: str, value: object) -> None:
        if key == "cache.hit":
            self.hit_values.append(value)


@pytest.fixture()
def recorded(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Spy on the lookup metric and the ``cache.get`` span."""
    results: list[str] = []
    spans: list[_RecordingSpan] = []

    def record(*, result: str, eviction_group: str = "") -> None:
        results.append(result)

    @contextlib.contextmanager
    def span(name: str, attributes: object = None) -> Iterator[_RecordingSpan]:
        recording = _RecordingSpan()
        if name == "cache.get":
            spans.append(recording)
        yield recording

    monkeypatch.setattr(cache_module, "record_cache_request", record)
    monkeypatch.setattr(cache_module, "cache_span", span)
    return {"results": results, "spans": spans}


def _build_app(calls: list[int]) -> FastAPI:
    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get("/items", dependencies=[Depends(cache(ttl=300))])
    async def items() -> dict:
        calls[0] += 1
        return {"value": calls[0]}

    return app


@requires_redis
@pytest.mark.integration
@pytest.mark.parametrize("corrupt", CORRUPT_ENTRIES.values(), ids=CORRUPT_ENTRIES)
def test_corrupt_entry_counts_as_one_miss(
    real_redis: sync_redis.Redis, recorded: dict[str, Any], corrupt: bytes
) -> None:
    calls = [0]
    with TestClient(_build_app(calls)) as client:
        assert client.get("/items").headers["X-Redis-Cache"] == "MISS"
        (key,) = real_redis.keys("*")
        real_redis.set(key, corrupt, keepttl=True)
        recorded["results"].clear()
        recorded["spans"].clear()

        response = client.get("/items")

    assert response.status_code == 200
    assert response.headers["X-Redis-Cache"] == "MISS"
    assert calls[0] == 2
    assert recorded["results"] == ["miss"]
    assert [s.hit_values for s in recorded["spans"]] == [[False]]
