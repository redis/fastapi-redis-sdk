from __future__ import annotations

import pytest
from redis_fastapi.cache_backend import CacheBackend


class TestNegativeTTLDocstringMismatch:
    @pytest.mark.asyncio
    async def test_negative_ttl_does_not_raise_valueerror(self) -> None:
        import fakeredis.aioredis

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


class TestTimedeltaTTLPrecision:
    @pytest.mark.asyncio
    async def test_timedelta_ttl_uses_milliseconds(self) -> None:
        from datetime import timedelta

        import fakeredis.aioredis

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        td = timedelta(milliseconds=500)
        await backend.set("k", "v", ttl=td)
        full_key = backend._build_key("k")
        ttl_val = await fake.pttl(full_key)
        assert 0 < ttl_val <= 500, (
            f"Expected pttl between 1 and 500ms, got {ttl_val}"
        )

    @pytest.mark.asyncio
    async def test_timedelta_ttl_one_second(self) -> None:
        from datetime import timedelta

        import fakeredis.aioredis

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        td = timedelta(seconds=1)
        await backend.set("k", "v", ttl=td)
        full_key = backend._build_key("k")
        ttl_val = await fake.ttl(full_key)
        assert ttl_val == 1, f"Expected ttl=1, got {ttl_val}"


class TestZeroTTLStorage:
    @pytest.mark.asyncio
    async def test_zero_ttl_persists_indefinitely(self) -> None:
        import fakeredis.aioredis

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")
        await backend.set("k", "v", ttl=0)
        await backend.set("k2", "v2")
        full_key = backend._build_key("k")
        full_key2 = backend._build_key("k2")
        assert await fake.ttl(full_key) == -1
        assert await fake.ttl(full_key2) == -1

    @pytest.mark.asyncio
    async def test_default_ttl_zero_persists_forever(self) -> None:
        import fakeredis.aioredis

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")
        await backend.set("k", "v")
        full_key = backend._build_key("k")
        assert await fake.ttl(full_key) == -1
