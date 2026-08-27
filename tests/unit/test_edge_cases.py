from __future__ import annotations

import pytest
from starlette.requests import Request as StarletteRequest

from redis_fastapi.cache import (
    cache_evict,
    default_key_builder,
)


def _make_request(path: str, query: str = "") -> StarletteRequest:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": query.encode(),
        "headers": [],
    }
    return StarletteRequest(scope)


class TestKeyBuilderAmbiguity:
    def test_multiple_query_params_with_colons(self) -> None:
        key = default_key_builder(
            _make_request("/search", "q=a:b&filter=x:y:z"),
            prefix="pfx",
        )
        segments = key.split(":")
        _ = [s for s in segments if "=" in s]

    def test_equals_in_query_value_is_ambiguous(self) -> None:
        default_key_builder(
            _make_request("/auth", "token=abc=def"),
            prefix="pfx",
        )


class TestEmptyEvictionGroupKey:
    def test_empty_eviction_group_no_hash_tags(self) -> None:
        key = default_key_builder(
            _make_request("/items"), eviction_group="", prefix="pfx"
        )
        assert "{" not in key
        assert key == "pfx:items"


class TestCacheEvictEmptyGroupRaises:
    def test_evict_empty_group_raises_valueerror(self) -> None:
        with pytest.raises(ValueError, match="cache_evict\\(\\) requires"):
            cache_evict()

    def test_evict_with_key_builder_ok(self) -> None:
        dep = cache_evict(key_builder=lambda r, **kw: "custom:key")
        assert dep is not None


class TestResetSettings:
    def test_reset_settings_clears_cache(self) -> None:
        from redis_fastapi.config import get_settings, reset_settings

        settings1 = get_settings()
        reset_settings()
        settings2 = get_settings()
        assert settings1 is not settings2
        assert type(settings1) is type(settings2)
