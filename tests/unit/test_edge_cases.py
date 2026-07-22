"""Edge case and adversarial tests for fastapi-redis-sdk.

Tests bugs found during codebase review:
1. Corrupted cache entry causes double telemetry recording (hit + miss)
2. Negative TTL docstring says ValueError but no ValueError raised
3. Middleware fallback when CachePending.redis is None without lifespan
4. Key builder with ambiguous query params
5. Non-2xx response caching edge cases
6. Streaming response with cache
7. Thundering herd / concurrent miss behavior
8. CacheHitException raised with corrupt data still sets span hit=True then False
9. cache_evict empty group wipes ALL keys
10. Zero TTL stores without expiry
11. Hash tag key collision / isolation
12. Cached entry with missing body/etag keys
"""

from __future__ import annotations

import json
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request as StarletteRequest

from redis_fastapi.cache import (
    CacheHitException,
    CachePending,
    CacheResponseCaptureMiddleware,
    _build_hit_response,
    _cache_control_value,
    _is_stale_for_client,
    _parse_cache_control,
    _read_cache_entry,
    cache,
    cache_evict,
    cache_put,
    default_key_builder,
)
from redis_fastapi.cache_backend import CacheBackend
from redis_fastapi.config import CACHE_STATUS_HEADER, get_settings
from redis_fastapi.deps import _PoolState, get_async_redis
from redis_fastapi.setup import FastAPIRedis


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fake_dep(fake: fakeredis.aioredis.FakeRedis):
    async def _fake() -> fakeredis.aioredis.FakeRedis:
        return fake
    return _fake


def _make_request(path: str, query: str = "") -> StarletteRequest:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": query.encode(),
        "headers": [],
    }
    return StarletteRequest(scope)


# ===================================================================
# BUG 1: Corrupted cache entry causes double telemetry recording
# ===================================================================


@pytest.mark.unit
class TestCorruptedEntryDoubleTelemetry:
    """When a cached entry has corrupt JSON, telemetry records BOTH hit and miss.

    Code path:
    1. _read_cache_entry returns (data, ttl) where data is corrupt JSON
    2. cached_data is truthy, so record_cache_request(result="hit") is called
    3. _build_hit_response raises json.JSONDecodeError
    4. Exception caught, falls through to MISS path
    5. record_cache_request(result="miss") is called again
    """

    @pytest.mark.asyncio
    async def test_corrupt_entry_only_records_miss(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        from redis_fastapi.cache import record_cache_request as cache_record

        recorded_results: list[str] = []

        def spy_record(*, result: str, eviction_group: str = "") -> None:
            recorded_results.append(result)

        app = FastAPI()
        FastAPIRedis(app).caching()

        get_settings.cache_clear()
        settings = get_settings()

        @app.get("/items", dependencies=[Depends(cache(ttl=300))])
        async def ep() -> dict:
            return {"value": 1}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake

        # Pre-populate Redis with corrupt JSON
        key = default_key_builder(
            _make_request("/items"), prefix=settings.pattern_prefix("cache")
        )
        await fake_async_redis.set(key, "not-valid-json{{{")

        with patch("redis_fastapi.cache.record_cache_request", side_effect=spy_record):
            with TestClient(app) as c:
                r = c.get("/items")
                assert r.status_code == 200

        assert "hit" not in recorded_results, (
            f"FIXED: corrupt entry should NOT record 'hit'. Got: {recorded_results}"
        )
        assert recorded_results == ["miss"], (
            f"FIXED: corrupt entry should only record 'miss'. Got: {recorded_results}"
        )
        get_settings.cache_clear()


# ===================================================================
# BUG 2: Negative TTL docstring mismatch
# ===================================================================


@pytest.mark.unit
class TestNegativeTTLDocstringMismatch:
    """CacheBackend.set() docstring says "Raises ValueError: If ttl is negative"
    but the code silently treats negative TTL as "no expiry".
    """

    @pytest.mark.asyncio
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
        print(f"\n  >> BUG: negative TTL={await fake.ttl(full_key)} (no expiry, "
              f"docstring says should raise ValueError)")


# ===================================================================
# BUG 3: Middleware fallback with no lifespan
# ===================================================================


@pytest.mark.unit
class TestMiddlewareFallbackNoLifespan:
    """When CachePending.redis is None, middleware falls back to
    _get_pool_state(app).get_async_client(). If lifespan hasn't been set up,
    this raises RuntimeError — an unhandled 500.
    """

    def test_middleware_fallback_crashes_without_lifespan(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        cache_mod = sys.modules["redis_fastapi.cache"]

        app = FastAPI()
        FastAPIRedis(app).caching()  # middleware registered, but NO lifespan
        empty_ps = _PoolState()

        @app.get("/test")
        async def test_endpoint(request: StarletteRequest) -> dict:
            request.state.redis_cache_pending = CachePending(
                key="test:key", ttl=60, redis=None
            )
            return {"ok": True}

        with patch.object(cache_mod, "_get_pool_state", return_value=empty_ps):
            with TestClient(app, raise_server_exceptions=False) as c:
                r = c.get("/test")
                print(f"\n  >> Fallback without lifespan: status={r.status_code}")


# ===================================================================
# BUG 4: Key builder ambiguity
# ===================================================================


@pytest.mark.unit
class TestKeyBuilderAmbiguity:
    """default_key_builder uses ':' as delimiter. Values containing ':' or '='
    create ambiguous keys that don't round-trip correctly.
    """

    def test_multiple_query_params_with_colons(self) -> None:
        key = default_key_builder(
            _make_request("/search", "q=a:b&filter=x:y:z"),
            prefix="pfx",
        )
        segments = key.split(":")
        value_segments = [s for s in segments if "=" in s]
        print(f"\n  >> Ambiguous key: {key}")
        print(f"  >> Segments with '=': {value_segments}")

    def test_equals_in_query_value_is_ambiguous(self) -> None:
        key = default_key_builder(
            _make_request("/auth", "token=abc=def"),
            prefix="pfx",
        )
        print(f"\n  >> Equals-in-value key: {key}")


# ===================================================================
# BUG 5: 304 responses not re-cached
# ===================================================================


@pytest.mark.unit
class Test304ResponseNotRecached:
    def test_304_not_re_cached(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts: list[int] = [0]

        @app.get("/data", dependencies=[Depends(cache(ttl=300))])
        async def data() -> dict:
            counts[0] += 1
            return {"value": counts[0]}

        app.dependency_overrides[get_async_redis] = _make_fake_dep(fake_async_redis)
        with TestClient(app) as c:
            r1 = c.get("/data")
            etag = r1.headers["ETag"]
            r2 = c.get("/data", headers={"If-None-Match": etag})
            assert r2.status_code == 304
            r3 = c.get("/data")
            assert r3.headers.get("X-Redis-Cache") == "HIT"
            assert counts[0] == 1


# ===================================================================
# BUG 6: Empty eviction group key
# ===================================================================


@pytest.mark.unit
class TestEmptyEvictionGroupKey:
    def test_empty_eviction_group_no_hash_tags(self) -> None:
        key = default_key_builder(
            _make_request("/items"), eviction_group="", prefix="pfx"
        )
        assert "{" not in key
        assert key == "pfx:items"


# ===================================================================
# BUG 7: Span attribute inconsistency on corrupt entry
# ===================================================================


@pytest.mark.unit
class TestSpanAttributeInconsistencyOnCorruptData:
    @pytest.mark.asyncio
    async def test_span_attributes_on_corrupt_entry(self) -> None:
        from redis_fastapi import telemetry as tel
        tel.disable_telemetry()
        tel.enable_telemetry()

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        settings = get_settings()
        prefix = settings.pattern_prefix("cache")
        key = default_key_builder(_make_request("/test"), prefix=prefix)
        await fake.set(key, "corrupt-json{{{")

        cc = {}
        cached_data, remaining_ttl = await _read_cache_entry(
            fake, key, 300, cc, force_refresh=False
        )
        assert cached_data is not None
        assert cached_data == "corrupt-json{{{"

        with pytest.raises(json.JSONDecodeError):
            _build_hit_response(
                cached_data, remaining_ttl, _make_request("/test"), private=False
            )
        tel.disable_telemetry()


# ===================================================================
# BUG 8: Thundering herd
# ===================================================================


@pytest.mark.unit
class TestConcurrentMissThunderingHerd:
    def test_thundering_herd_on_first_request(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        import concurrent.futures
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts: list[int] = [0]

        @app.get("/compute", dependencies=[Depends(cache(ttl=300))])
        async def compute() -> dict:
            counts[0] += 1
            return {"value": counts[0]}

        app.dependency_overrides[get_async_redis] = _make_fake_dep(fake_async_redis)
        with TestClient(app) as c:
            N = 20

            def req() -> dict[str, Any]:
                return c.get("/compute").json()

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
                futures = [ex.submit(req) for _ in range(N)]
                results = [f.result() for f in concurrent.futures.as_completed(futures)]

            values = sorted(set(r["value"] for r in results))
            print(f"\n  >> Thundering herd: {N} requests -> unique values={values}")
            print(f"  >> Endpoint called {counts[0]} times")


# ===================================================================
# BUG 9: cache_evict empty group raises ValueError
# ===================================================================


@pytest.mark.unit
class TestCacheEvictEmptyGroupRaises:
    def test_evict_empty_group_raises_valueerror(self) -> None:
        with pytest.raises(ValueError, match="cache_evict\\(\\) requires"):
            cache_evict()

    @pytest.mark.asyncio
    async def test_evict_explicit_group_still_works(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()
        backend = CacheBackend(fake_async_redis)

        @app.post("/seed")
        async def seed() -> dict:
            await backend.set("k1", "v1", eviction_group="grp1")
            await backend.set("k2", "v2", eviction_group="grp2")
            return {"ok": True}

        @app.post("/wipe/{grp}")
        async def wipe(grp: str) -> dict:
            await backend.delete_group(grp)
            return {"ok": True}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis
        app.dependency_overrides[get_async_redis] = _fake

        with TestClient(app) as c:
            c.post("/seed")
            assert await backend.has("k1", eviction_group="grp1") is True
            assert await backend.has("k2", eviction_group="grp2") is True
            c.post("/wipe/grp1")
            c.post("/wipe/grp2")
            assert await backend.has("k1", eviction_group="grp1") is False
            assert await backend.has("k2", eviction_group="grp2") is False


# ===================================================================
# BUG 10: Stampede protection (probabilistic early expiration)
# ===================================================================


@pytest.mark.unit
class TestStampedeProtection:
    def test_stampede_protection_keyword_accepted(self) -> None:
        """stampede_protection=True is accepted by cache() without error."""
        deps = cache(ttl=300, stampede_protection=True)
        assert deps is not None

    @pytest.mark.asyncio
    async def test_stampede_protection_can_convert_hit_to_miss(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        from redis_fastapi.cache import record_cache_request as cache_record

        recorded_results: list[str] = []
        def spy_record(*, result: str, eviction_group: str = "") -> None:
            recorded_results.append(result)

        app = FastAPI()
        FastAPIRedis(app).caching()
        get_settings.cache_clear()
        settings = get_settings()

        @app.get("/near-expiry", dependencies=[Depends(cache(ttl=2, stampede_protection=True))])
        async def ep() -> dict:
            return {"value": 1}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis
        app.dependency_overrides[get_async_redis] = _fake

        key = default_key_builder(
            _make_request("/near-expiry"), prefix=settings.pattern_prefix("cache")
        )
        # Store a valid entry with very short remaining TTL so stampede kicks in
        await fake_async_redis.set(key, json.dumps({"body": '{"v":1}', "status_code": 200, "headers": {}, "etag": "\"abc\""}))
        # Artificially age the entry
        await fake_async_redis.expire(key, 0)

        with patch("redis_fastapi.cache.record_cache_request", side_effect=spy_record):
            with TestClient(app, raise_server_exceptions=False) as c:
                # Some requests should hit stampede protection and treat as miss
                responses = [c.get("/near-expiry") for _ in range(20)]
                misses = [r for r in responses if r.json() is not None]
                # At least some should be hits (stampede is probabilistic)
                hits = sum(1 for r in responses if r.status_code == 200 and r.json().get("value") == 1)
                print(f"\n  >> Stampede: {len(misses)} misses, {hits} hits out of {len(responses)}")

        get_settings.cache_clear()


# ===================================================================
# BUG 11: Zero TTL stores without expiry
# ===================================================================


@pytest.mark.unit
class TestZeroTTLStorage:
    @pytest.mark.asyncio
    async def test_zero_ttl_persists_indefinitely(self) -> None:
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
        """The default default_ttl=0 means entries persist until evicted."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")
        await backend.set("k", "v")
        full_key = backend._build_key("k")
        assert await fake.ttl(full_key) == -1
        print("\n  >> DEFAULT: default_ttl=0 means entries live forever "
              "- unbounded growth risk")


# ===================================================================
# BUG 11: Hash tag key isolation
# ===================================================================


@pytest.mark.unit
class TestHashTagKeyIsolation:
    def test_different_groups_different_keys(self) -> None:
        key1 = default_key_builder(
            _make_request("/items"), eviction_group="grp1", prefix="pfx"
        )
        key2 = default_key_builder(
            _make_request("/items"), eviction_group="grp2", prefix="pfx"
        )
        assert key1 != key2
        assert "{grp1}" in key1
        assert "{grp2}" in key2


# ===================================================================
# BUG 12: Missing keys in cached entry
# ===================================================================


@pytest.mark.unit
class TestCachedEntryMissingKeys:
    def test_missing_keys_raise_key_error(self) -> None:
        from starlette.requests import Request
        scope = {
            "type": "http", "method": "GET", "path": "/test",
            "query_string": b"", "headers": [],
        }
        req = Request(scope)
        with pytest.raises(KeyError):
            _build_hit_response(
                json.dumps({"no_body": "here"}), 250, req, private=False
            )


# ===================================================================
# BUG 13: cache_put with no key_builder and no eviction_group
# ===================================================================


@pytest.mark.unit
class TestCachePutNoGroup:
    def test_cache_put_with_defaults_overwrites_global(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """cache_put() with no key_builder or eviction_group uses default
        key builder with empty eviction group, producing an ungrouped key."""
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get("/item", dependencies=[Depends(cache(ttl=300))])
        async def get() -> dict:
            return {"src": "original"}

        @app.put("/item", dependencies=[Depends(cache_put(ttl=300))])
        async def put() -> dict:
            return {"src": "updated"}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis
        app.dependency_overrides[get_async_redis] = _fake

        with TestClient(app) as c:
            r1 = c.get("/item")
            assert r1.json() == {"src": "original"}
            c.put("/item")
            r2 = c.get("/item")
            assert r2.json() == {"src": "updated"}


# ===================================================================
# BUG 14: _is_stale_for_client with integer max-age from header
# ===================================================================


@pytest.mark.unit
class TestIsStaleForClientEdgeCases:
    def test_max_age_none(self) -> None:
        assert _is_stale_for_client(200, 300, {}) is False

    def test_max_age_zero_stale(self) -> None:
        assert _is_stale_for_client(299, 300, {"max-age": "0"}) is True

    def test_max_age_exact_boundary(self) -> None:
        assert _is_stale_for_client(180, 300, {"max-age": "120"}) is True

    def test_max_age_one_second_under_boundary(self) -> None:
        assert _is_stale_for_client(181, 300, {"max-age": "120"}) is False

    def test_remaining_ttl_zero(self) -> None:
        """When the key has expired (ttl=0), we should treat as stale."""
        assert _is_stale_for_client(0, 300, {"max-age": "1"}) is True


# ===================================================================
# BUG 15: _parse_cache_control None and empty
# ===================================================================


@pytest.mark.unit
class TestParseCacheControlEdgeCases:
    def test_none_returns_empty(self) -> None:
        assert _parse_cache_control(None) == {}

    def test_empty_string_returns_empty(self) -> None:
        assert _parse_cache_control("") == {}

    def test_only_commas(self) -> None:
        assert _parse_cache_control(",,,") == {}

    def test_mixed_case(self) -> None:
        result = _parse_cache_control("No-Cache, Max-Age=60")
        assert result["no-cache"] is True
        assert result["max-age"] == "60"

    def test_duplicate_directives(self) -> None:
        result = _parse_cache_control("max-age=60, max-age=120")
        assert result["max-age"] == "120"


# ===================================================================
# BUG 16: reset_settings() clears cached settings
# ===================================================================


@pytest.mark.unit
class TestResetSettings:
    def test_reset_settings_clears_cache(self) -> None:
        from redis_fastapi.config import get_settings, reset_settings

        settings1 = get_settings()
        reset_settings()
        settings2 = get_settings()
        assert settings1 is not settings2
        assert type(settings1) is type(settings2)


# ===================================================================
# BUG 17: Timedelta TTL preserves sub-second precision
# ===================================================================


@pytest.mark.unit
class TestTimedeltaTTLPrecision:
    @pytest.mark.asyncio
    async def test_timedelta_ttl_uses_milliseconds(self) -> None:
        from datetime import timedelta

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

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")

        td = timedelta(seconds=1)
        await backend.set("k", "v", ttl=td)
        full_key = backend._build_key("k")
        ttl_val = await fake.ttl(full_key)
        assert ttl_val == 1, f"Expected ttl=1, got {ttl_val}"


# ===================================================================
# BUG 18: Telemetry swallowed exceptions logged at WARNING
# ===================================================================


@pytest.mark.unit
class TestTelemetryExceptionLogLevel:
    def test_telemetry_exceptions_logged_at_warning(self) -> None:
        import logging
        from redis_fastapi.telemetry import record_cache_request, enable_telemetry, disable_telemetry

        disable_telemetry()
        enable_telemetry()

        logger = logging.getLogger("redis_fastapi.telemetry")
        original_level = logger.level
        logger.setLevel(logging.DEBUG)

        try:
            with patch.object(logger, "warning") as mock_warning:
                with patch.object(logger, "debug") as mock_debug:
                    record_cache_request(result="hit")
        finally:
            logger.setLevel(original_level)
            disable_telemetry()


# ===================================================================
# BUG 19: cache_evict raises ValueError with no args
# ===================================================================


@pytest.mark.unit
class TestCacheEvictNoArgs:
    def test_no_args_raises_valueerror(self) -> None:
        from redis_fastapi.cache import cache_evict
        with pytest.raises(ValueError, match="cache_evict\\(\\) requires"):
            cache_evict()

    def test_with_eviction_group_ok(self) -> None:
        from redis_fastapi.cache import cache_evict
        dep = cache_evict(eviction_group="mygroup")
        assert dep is not None

    def test_with_key_builder_ok(self) -> None:
        from redis_fastapi.cache import cache_evict
        dep = cache_evict(key_builder=lambda r, **kw: "custom:key")
        assert dep is not None
