"""Tests for default_key_builder key normalisation."""

import pytest
from starlette.requests import Request

from redis_fastapi.cache import default_key_builder


def _make_request(path: str, query: str = "") -> Request:
    """Create a minimal ASGI Request for testing."""
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": query.encode(),
        "headers": [],
    }
    return Request(scope)


@pytest.mark.unit
class TestDefaultKeyBuilder:
    def test_simple_path(self) -> None:
        req = _make_request("/items")
        key = default_key_builder(req, prefix="pfx")
        assert key == "pfx:items"

    def test_nested_path_slashes_to_colons(self) -> None:
        req = _make_request("/api/v1/items")
        key = default_key_builder(req, prefix="pfx")
        assert key == "pfx:api:v1:items"

    def test_query_params_sorted(self) -> None:
        req = _make_request("/items", query="z=2&a=1")
        key = default_key_builder(req, prefix="pfx")
        assert key == "pfx:items:a=1&z=2"

    def test_eviction_group_included(self) -> None:
        req = _make_request("/items")
        key = default_key_builder(req, prefix="pfx", eviction_group="ns")
        assert key == "pfx:{ns}:items"

    def test_no_prefix(self) -> None:
        req = _make_request("/items")
        key = default_key_builder(req)
        assert key == "items"

    def test_root_path(self) -> None:
        req = _make_request("/")
        key = default_key_builder(req, prefix="pfx")
        # root path stripped → only prefix
        assert key == "pfx"

    def test_trailing_slash_stripped(self) -> None:
        req = _make_request("/items/")
        key = default_key_builder(req, prefix="pfx")
        assert key == "pfx:items"

    def test_full_example_from_plan(self) -> None:
        """Scenario 14 from PLAN.md: /api/v1/items?q=x → prefix:api:v1:items:q=x"""
        req = _make_request("/api/v1/items", query="q=x")
        key = default_key_builder(req, prefix="fastapi-cache")
        assert key == "fastapi-cache:api:v1:items:q=x"
