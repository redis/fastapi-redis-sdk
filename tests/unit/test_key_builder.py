"""Tests for default_key_builder key normalisation."""

from urllib.parse import unquote

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
        """A literal ``%`` in the decoded path is encoded, so ``100%`` and
        ``100%25`` (what the server decodes ``%2525`` to) stay apart."""
        literal = default_key_builder(_make_request("/items/100%"), prefix="pfx")
        encoded = default_key_builder(_make_request("/items/100%25"), prefix="pfx")
        assert literal == "pfx:items:100%25"
        assert encoded == "pfx:items:100%2525"

    def test_colon_in_segment_does_not_collide_with_slash(self) -> None:
        """``/items/a:b`` and ``/items/a/b`` are different resources."""
        colon = default_key_builder(_make_request("/items/a:b"), prefix="pfx")
        slash = default_key_builder(_make_request("/items/a/b"), prefix="pfx")
        assert colon == "pfx:items:a%3Ab"
        assert slash == "pfx:items:a:b"

    def test_decoded_question_mark_in_segment_is_kept(self) -> None:
        """The server decodes ``%3F`` to ``?`` before routing.  The key must
        come from the routed path, not from re-parsing it as a URL."""
        key = default_key_builder(_make_request("/items/a?b=1"), prefix="pfx")
        plain = default_key_builder(_make_request("/items/a"), prefix="pfx")
        assert key == "pfx:items:a%3Fb%3D1"
        assert key != plain

    def test_brace_in_segment_cannot_forge_eviction_group(self) -> None:
        forged = default_key_builder(_make_request("/{grp}/items"), prefix="pfx")
        grouped = default_key_builder(
            _make_request("/items"), prefix="pfx", eviction_group="grp"
        )
        assert forged == "pfx:%7Bgrp%7D:items"
        assert forged != grouped


# ---------------------------------------------------------------------------
# RFC 3986 coverage
#
# Every character class the RFC defines, in every position where it could be
# mistaken for key structure: the ``:`` separator, the path/query boundary,
# the ``%`` escape, and the Redis ``{}`` hash-tag.
# ---------------------------------------------------------------------------


def _from_target(target: str) -> Request:
    """Build the request an ASGI server hands the app for *target*.

    The server splits the request target at the first ``?``, percent-decodes
    the path once, and passes the query string through raw.
    """
    raw_path, _, query = target.partition("?")
    return _make_request(unquote(raw_path), query)


# Request targets that name different resources.  No two may share a key.
DISTINCT_TARGETS: list[str] = [
    # baseline
    "/",
    "/items",
    "/Items",  # paths are case-sensitive
    "/items/a",
    "/items/a/b",
    # unreserved: ALPHA / DIGIT / "-" / "." / "_" / "~" (RFC 3986 §2.3)
    "/items/a-b",
    "/items/a_b",
    "/items/a.b",
    "/items/a~b",
    # sub-delims "=" and "&": a segment must not read as a query string (#103)
    "/items/page=2",
    "/items?page=2",
    "/files/a/b=1",
    "/files/a?b=1",
    "/items/a=1&b=2",
    "/items?a=1&b=2",
    "/items/=",
    "/items?=",
    "/items/a=",
    "/items?a",
    # the other sub-delims, bare in a segment (RFC 3986 §2.2)
    "/items/a!b",
    "/items/a$b",
    "/items/a'b",
    "/items/a(b)",
    "/items/a*b",
    "/items/a,b",
    "/items/a;b",
    # "+" is a space only in a form-encoded query; in a path it is literal
    "/items/a+b",
    "/items/a%20b",
    # gen-delim ":" is a pchar: a segment must not read as a separator
    "/files/a:b",
    "/files/a::b",
    "/files/a//b",
    "/files/a/:b",
    "/files/:",
    # gen-delim "@" is a pchar
    "/files/user@host",
    "/files/user/host",
    # gen-delims the server decodes into the path: "?" "#" "[" "]"
    "/files/a%3Fb=1",
    "/files/a%23b",
    "/files/%5Ba%5D",
    "/files/a%5D",
    # pct-encoded: a literal "%" after decoding must stay apart from what it
    # would decode to (RFC 3986 §2.4: decode once only)
    "/files/100",
    "/files/100%25",
    "/files/100%2525",
    "/files/%253A",
    # query = *( pchar / "/" / "?" ): these are bare in a query, "&" and "="
    # are structural, and an encoded "&" is data
    "/items?q=a:b",
    "/items/q=a:b",
    "/items?q=a/b",
    "/items?q=a?b",
    "/items?a=1%26b=2",
    "/items?a=1&a=2",
    "/items?a=2&a=1",
    # non-ASCII (RFC 3987): NFC and NFD are different strings and routes
    "/items/cafe",
    "/items/caf%C3%A9",
    "/items/%C3%A9",
    "/items/e%CC%81",
    "/items/%E6%97%A5%E6%9C%AC%E8%AA%9E",
    # Redis hash-tag braces are not reserved by the RFC but are key structure
    "/%7Bgrp%7D/items",
    "/items/%7Bgrp%7D",
    # dot segments: the router does not normalise them
    "/items/./a",
    "/items/../a",
]

# Request targets the router cannot tell apart, or that the builder
# normalises on purpose.  Each group must share one key.
EQUIVALENT_TARGETS: list[tuple[str, ...]] = [
    # the server decodes once, so these reach the router as the same path
    ("/items/a%2Fb", "/items/a/b"),
    ("/items/x%3D1", "/items/x=1"),
    ("/items/%2a", "/items/%2A", "/items/*"),  # hex case (RFC 3986 §2.1)
    ("/items/%7E", "/items/~"),  # encoded unreserved (RFC 3986 §2.3)
    ("/items/%E6%97%A5%E6%9C%AC%E8%AA%9E", "/items/日本語"),
    # query normalisation the builder applies on purpose
    ("/items?a=1&b=2", "/items?b=2&a=1"),  # sorted by name
    ("/items?a+b=1", "/items?a%20b=1"),  # "+" is a space in a query
    ("/items?a=b%3Dc", "/items?a=b=c"),  # split at the first "="
    ("/items?a", "/items?a="),
    ("/items?", "/items"),
    # documented: trailing slash is dropped
    ("/items/", "/items"),
]


@pytest.mark.unit
class TestRfc3986Coverage:
    def test_target_list_has_no_duplicates(self) -> None:
        assert len(set(DISTINCT_TARGETS)) == len(DISTINCT_TARGETS)

    def test_distinct_targets_build_distinct_keys(self) -> None:
        by_key: dict[str, list[str]] = {}
        for target in DISTINCT_TARGETS:
            key = default_key_builder(_from_target(target), prefix="pfx")
            by_key.setdefault(key, []).append(target)
        collisions = {k: v for k, v in by_key.items() if len(v) > 1}
        assert not collisions, f"targets sharing a key: {collisions}"

    @pytest.mark.parametrize("targets", EQUIVALENT_TARGETS)
    def test_equivalent_targets_share_a_key(self, targets: tuple[str, ...]) -> None:
        keys = {default_key_builder(_from_target(t), prefix="pfx") for t in targets}
        assert len(keys) == 1, keys
