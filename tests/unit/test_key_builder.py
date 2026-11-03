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

    def test_path_segment_does_not_collide_with_query(self) -> None:
        """``/items/page=2`` and ``/items?page=2`` are different resources."""
        path_key = default_key_builder(_make_request("/items/page=2"), prefix="pfx")
        query_key = default_key_builder(
            _make_request("/items", query="page=2"), prefix="pfx"
        )
        assert path_key == "pfx:items:page%3D2"
        assert query_key == "pfx:items:page=2"

    def test_nested_path_does_not_collide_with_query(self) -> None:
        path_key = default_key_builder(_make_request("/files/a/b=1"), prefix="pfx")
        query_key = default_key_builder(
            _make_request("/files/a", query="b=1"), prefix="pfx"
        )
        assert path_key != query_key

    def test_percent_in_path_is_escaped(self) -> None:
        """A decoded ``%3D`` in the path does not collide with a literal ``=``."""
        literal = default_key_builder(_make_request("/items/x=1"), prefix="pfx")
        percent = default_key_builder(_make_request("/items/x%3D1"), prefix="pfx")
        assert literal == "pfx:items:x%3D1"
        assert percent == "pfx:items:x%253D1"
