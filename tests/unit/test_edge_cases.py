from __future__ import annotations

import logging
from datetime import timedelta

import fakeredis.aioredis
import pytest

from redis_fastapi.cache_backend import CacheBackend


class TestNegativeTTLDocstringMismatch:
    async def test_negative_ttl_does_not_raise_valueerror(self) -> None:
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        try:
            await backend.set("k", "v", ttl=-5)
        except ValueError as exc:
            pytest.fail(f"Unexpected ValueError: {exc}")

        assert await backend.get("k") == "v"
        full_key = backend._build_key("k")
        ttl_val = await fake.ttl(full_key)
        assert ttl_val == -1, "Negative TTL should behave as no expiry"

    async def test_negative_ttl_logs_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        with caplog.at_level(logging.WARNING, logger="redis_fastapi.cache_backend"):
            caplog.clear()
            await backend.set("k", "v", ttl=-5)

        assert any("Negative TTL" in record.message for record in caplog.records)


class TestTimedeltaTTLPrecision:
    async def test_subsecond_ttl_rounded_up_to_one_second(self) -> None:
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        await backend.set("k", "v", ttl=timedelta(milliseconds=500))
        full_key = backend._build_key("k")
        assert await fake.ttl(full_key) == 1

    async def test_timedelta_ttl_one_second(self) -> None:
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        await backend.set("k", "v", ttl=timedelta(seconds=1))
        full_key = backend._build_key("k")
        assert await fake.ttl(full_key) == 1


class TestZeroTTLStorage:
    async def test_zero_ttl_persists_indefinitely(self) -> None:
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")
        await backend.set("k", "v", ttl=0)
        await backend.set("k2", "v2")
        full_key = backend._build_key("k")
        full_key2 = backend._build_key("k2")
        assert await fake.ttl(full_key) == -1
        assert await fake.ttl(full_key2) == -1

    async def test_default_ttl_zero_persists_forever(self) -> None:
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")
        await backend.set("k", "v")
        full_key = backend._build_key("k")
        assert await fake.ttl(full_key) == -1


class TestNoExpiryWarningOnUnboundedServer:
    async def test_no_warning_when_server_has_eviction_policy(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        async def _info(section: str) -> dict[str, object]:
            assert section == "memory"
            return {"maxmemory": 1000, "maxmemory_policy": "allkeys-lru"}

        fake.info = _info  # type: ignore[method-assign]

        with caplog.at_level(logging.WARNING, logger="redis_fastapi.cache_backend"):
            caplog.clear()
            await backend.set("k", "v")

        assert not any("without TTL" in record.message for record in caplog.records)
        assert await fake.ttl(backend._build_key("k")) == -1

    async def test_warning_when_server_has_no_eviction_protection(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        async def _info(section: str) -> dict[str, object]:
            assert section == "memory"
            return {"maxmemory": 0, "maxmemory_policy": "noeviction"}

        fake.info = _info  # type: ignore[method-assign]

        with caplog.at_level(logging.WARNING, logger="redis_fastapi.cache_backend"):
            caplog.clear()
            await backend.set("k", "v")

        assert any("without TTL" in record.message for record in caplog.records)
        assert await fake.ttl(backend._build_key("k")) == -1
