"""Queries over session hashes: what to ask the Query Engine, and how to read
the answer.

No Redis call in this module.  It builds index definitions and queries with
the search helpers of redis-py, and it turns a reply into rows.  The store
sends what this module builds.

Two indexes cover the same session hashes:

* the built-in index holds system fields only, so no value an application
  writes can remove a session from it;
* the custom index holds the attributes the application declared.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from redis.commands.search.field import (
    Field,
    GeoField,
    NumericField,
    TagField,
    TextField,
)
from redis.commands.search.query import Query

from redis_fastapi.session_layout import (
    ATTR_PREFIX,
    FIELD_CREATED,
    FIELD_DESCRIPTOR,
    FIELD_SEEN,
    FIELD_SUBJECT,
    GEO,
    NUMERIC,
    TAG,
    TEXT,
)

try:  # redis-py 6 and later
    from redis.commands.search.index_definition import IndexDefinition, IndexType
except ImportError:  # pragma: no cover - older redis-py
    from redis.commands.search.indexDefinition import (  # type: ignore[import-not-found,no-redef]
        IndexDefinition,
        IndexType,
    )

# A character no JSON text can hold unescaped, so no TAG value can contain it
# and the engine never splits one value into two tags.
TAG_SEPARATOR = "\x1f"

BUILTIN = "builtin"
CUSTOM = "custom"

# Rows a list returns, and how far a page may reach.  Constants, not settings.
LIST_LIMIT = 1000
PAGE_LIMIT = 10_000
REVOKE_BATCH = 1000
DEFAULT_PAGE = 100

RETURN_FIELDS = (FIELD_CREATED, FIELD_SEEN, FIELD_DESCRIPTOR)

_FIELD_ESCAPE = re.compile(r"([^A-Za-z0-9_])")


@dataclass(frozen=True)
class Filter:
    """One condition on one field of one index."""

    index: str
    field: str
    kind: str
    value: Any = None


def escape_field(name: str) -> str:
    """A field name as a query writes it: ``a:cart.total`` is
    ``a\\:cart\\.total``.

    ``SORTBY`` and ``RETURN`` take the name as it is.
    """
    return _FIELD_ESCAPE.sub(r"\\\1", name)


def tag_clause(value: Any) -> str:
    """The exact-match clause for a TAG value stored as JSON text."""
    text = json.dumps(value, ensure_ascii=False)
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return '{"' + escaped + '"}'


def _number(value: Any, missing: str) -> str:
    return missing if value is None else json.dumps(value)


# Every query carries this clause.  It matches a hash that has ``s:created``,
# so it leaves out a session past its deadline, whose ``s:created`` expired,
# and a hash a late save recreated.  Outside the parentheses of a TEXT
# filter, so the text of a well-formed query cannot remove it.
LIVE_FILTER = f"@{escape_field(FIELD_CREATED)}:[-inf +inf]"


def query_string(flt: Filter) -> str:
    """The query text for *flt*, with the live filter added."""
    name = escape_field(flt.field)
    if flt.kind == TAG:
        clause = f"@{name}:{tag_clause(flt.value)}"
    elif flt.kind == NUMERIC:
        low, high = flt.value
        clause = f"@{name}:[{_number(low, '-inf')} {_number(high, '+inf')}]"
    elif flt.kind == TEXT:
        clause = f"@{name}:({flt.value})"
    else:
        lon, lat, radius = flt.value
        clause = f"@{name}:[{json.dumps(lon)} {json.dumps(lat)} {json.dumps(radius)} m]"
    return f"{clause} {LIVE_FILTER}"


def page_query(flt: Filter, *, limit: int, offset: int) -> Query:
    """A sorted page of sessions, newest access first."""
    return (
        Query(query_string(flt))
        .dialect(2)
        .sort_by(FIELD_SEEN, asc=False)
        .return_fields(*RETURN_FIELDS)
        .paging(offset, limit)
    )


def count_query(flt: Filter) -> Query:
    """How many sessions match.  No rows come back, and nothing is sorted."""
    return Query(query_string(flt)).dialect(2).no_content().paging(0, 0)


def ids_query(flt: Filter, *, limit: int) -> Query:
    """A batch of matching keys with no content and no sort, for a bulk
    revoke."""
    return Query(query_string(flt)).dialect(2).no_content().paging(0, limit)


def builtin_fields() -> list[Field]:
    """The fixed schema of the built-in index.  System fields only."""
    return [
        TagField(FIELD_SUBJECT, separator=TAG_SEPARATOR, case_sensitive=True),
        NumericField(FIELD_SEEN, sortable=True),
        NumericField(FIELD_CREATED),
    ]


def custom_fields(searchable: Mapping[str, str]) -> list[Field]:
    """The schema of the custom index: the declared attributes, ``s:seen``
    for the sort, and ``s:created`` for the live filter."""
    fields: list[Field] = [NumericField(FIELD_SEEN, sortable=True)]
    fields.append(NumericField(FIELD_CREATED))
    for path in sorted(searchable):
        name = ATTR_PREFIX + path
        kind = searchable[path]
        if kind == TAG:
            fields.append(TagField(name, separator=TAG_SEPARATOR, case_sensitive=True))
        elif kind == NUMERIC:
            fields.append(NumericField(name))
        elif kind == TEXT:
            fields.append(TextField(name))
        elif kind == GEO:
            fields.append(GeoField(name))
    return fields


def custom_index_name(base: str, searchable: Mapping[str, str]) -> str:
    """``<base><hash>``, where the hash names the declaration.

    Two stores with the same declaration use one index.  A changed
    declaration gives a new name, so the store never has to compare an index
    with what was declared.
    """
    lines = sorted(f"{path}={kind}" for path, kind in searchable.items())
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()
    return f"{base}{digest[:8]}"


def definition(key_prefix: str) -> IndexDefinition:
    """Index every hash under *key_prefix*."""
    return IndexDefinition(  # type: ignore[no-untyped-call]
        prefix=[key_prefix], index_type=IndexType.HASH
    )


@dataclass(frozen=True)
class SearchReply:
    """A reply in one form, whatever the protocol gave."""

    total: int
    rows: list[tuple[str, dict[str, str]]]
    timed_out: bool


def _str(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _is_timeout(warning: Any) -> bool:
    items = warning if isinstance(warning, (list, tuple)) else [warning]
    return any("timeout" in _str(item).lower() for item in items if item)


def parse_reply(reply: Any) -> SearchReply:
    """Read an ``FT.SEARCH`` reply.

    redis-py returns a ``Result`` object or a plain ``dict``, depending on
    its version and the protocol.  Only a RESP3 reply can say that the query
    timed out; a RESP2 reply has no place for the warning.
    """
    if isinstance(reply, Mapping):
        rows = []
        for row in reply.get("results") or reply.get(b"results") or []:
            fields = row.get("extra_attributes") or row.get(b"extra_attributes") or {}
            rows.append(
                (
                    _str(row.get("id", row.get(b"id"))),
                    {_str(k): _str(v) for k, v in dict(fields).items()},
                )
            )
        total = reply.get("total_results", reply.get(b"total_results", 0))
        warning = reply.get("warning", reply.get(b"warning"))
        return SearchReply(int(total), rows, _is_timeout(warning))
    rows = []
    for doc in getattr(reply, "docs", None) or []:
        fields = {
            _str(key): _str(value)
            for key, value in vars(doc).items()
            if key not in ("id", "payload") and value is not None
        }
        rows.append((_str(doc.id), fields))
    # redis-py builds a ``Result`` from a RESP3 reply too, and keeps what the
    # server put in ``warning`` on it.
    warnings = getattr(reply, "warnings", None)
    return SearchReply(int(getattr(reply, "total", 0)), rows, _is_timeout(warnings))


def error_kind(exc: BaseException) -> str | None:
    """Classify a driver error from a search: ``timeout``, ``syntax``,
    ``missing`` (no such index), ``exists`` or ``None``."""
    text = str(exc).lower()
    if "timeout" in text or "timed out" in text:
        return "timeout"
    if "syntax" in text:
        return "syntax"
    if "already exists" in text:
        return "exists"
    if "no such index" in text or "index not found" in text or "unknown index" in text:
        return "missing"
    return None
