"""How a session maps to the fields of one Redis hash.

No Redis in this module.  It turns the nested value an application keeps in
``request.session`` into flat hash fields and back, and it computes what a
save has to write.  The store and the tests both call it.

One field for each leaf::

    {"role": "admin"}                           a:role
    {"user": {"address": {"city": "Sofia"}}}    a:user.address.city
    {"bikes": ["road", "gravel"]}               a:bikes[0], a:bikes[1]
    {"tags": []}                                a:tags = []

Each field holds JSON text, so ``7`` and ``"7"`` have different stored forms
and each comes back as it went in.  The store needs no model to restore a
type.
"""

from __future__ import annotations

import base64
import json
import math
from collections.abc import Iterable, Mapping
from typing import Any

from redis_fastapi.exceptions import (
    SessionAttributeTypeError,
    SessionConfigurationError,
    SessionStoreError,
)
from redis_fastapi.types import Encryptor

# A field that starts with ``s:`` belongs to the store, and one that starts
# with ``a:`` is an attribute of the application.  The prefixes are what keep
# an attribute named ``created`` from overwriting the store's own field.
SYSTEM_PREFIX = "s:"
ATTR_PREFIX = "a:"

FIELD_SUBJECT = f"{SYSTEM_PREFIX}sub"
FIELD_CREATED = f"{SYSTEM_PREFIX}created"
FIELD_SEEN = f"{SYSTEM_PREFIX}seen"
FIELD_DESCRIPTOR = f"{SYSTEM_PREFIX}desc"

TAG = "tag"
NUMERIC = "numeric"
TEXT = "text"
GEO = "geo"
SEARCHABLE_TYPES = frozenset({TAG, NUMERIC, TEXT, GEO})

# The four characters a path gives a meaning to, and the escape for each.
# ``%`` first on the way in and last on the way out, so an escape is never
# escaped twice.
_ESCAPES = (("%", "%25"), (".", "%2E"), ("[", "%5B"), ("]", "%5D"))


def escape_key(key: str) -> str:
    """Make a dict key safe to use as one segment of a path."""
    for char, escaped in _ESCAPES:
        key = key.replace(char, escaped)
    return key


def unescape_key(segment: str) -> str:
    """The reverse of :func:`escape_key`."""
    for char, escaped in reversed(_ESCAPES):
        segment = segment.replace(escaped, char)
    return segment


def _key_text(key: Any) -> str:
    """A dict key as the string JSON would write for it."""
    if isinstance(key, str):
        return key
    if key is True:
        return "true"
    if key is False:
        return "false"
    if key is None:
        return "null"
    if isinstance(key, (int, float)):
        return json.dumps(key)
    raise TypeError(
        f"keys must be str, int, float, bool or None, not {type(key).__name__}"
    )


def _text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_point(value: Any) -> bool:
    return (
        isinstance(value, (list, tuple))
        and len(value) == 2
        and all(_is_number(part) for part in value)
    )


def _walk(value: Any, path: str, geo: frozenset[str], out: dict[str, str]) -> None:
    if path in geo and _is_point(value):
        # The engine reads a GEO value as ``lon,lat`` with no quotes, so this
        # one leaf is not JSON text and is not flattened into two fields.
        out[ATTR_PREFIX + path] = f"{_text(value[0])},{_text(value[1])}"
    elif isinstance(value, Mapping) and value:
        for key, item in value.items():
            _walk(item, f"{path}.{escape_key(_key_text(key))}", geo, out)
    elif isinstance(value, (list, tuple)) and value:
        for index, item in enumerate(value):
            _walk(item, f"{path}[{index}]", geo, out)
    elif isinstance(value, Mapping):
        out[ATTR_PREFIX + path] = "{}"
    elif isinstance(value, (list, tuple)):
        out[ATTR_PREFIX + path] = "[]"
    else:
        out[ATTR_PREFIX + path] = _text(value)


def flatten(data: Mapping[Any, Any], *, geo: Iterable[str] = ()) -> dict[str, str]:
    """Return ``{field name: stored text}`` for the attributes in *data*.

    *geo* names the paths declared as GEO.  Each is one leaf, in the
    ``lon,lat`` form the engine reads.

    Raises:
        TypeError: If a value has no JSON form, as ``json.dumps`` does.
    """
    out: dict[str, str] = {}
    geo_paths = frozenset(geo)
    for key, value in data.items():
        _walk(value, escape_key(_key_text(key)), geo_paths, out)
    return out


def _tokens(path: str) -> list[str | int]:
    """Split a path into dict keys (``str``) and list indexes (``int``)."""
    tokens: list[str | int] = []
    key, rest = _take_key(path, 0)
    tokens.append(unescape_key(key))
    while rest < len(path):
        if path[rest] == ".":
            key, rest = _take_key(path, rest + 1)
            tokens.append(unescape_key(key))
        else:
            end = path.index("]", rest)
            tokens.append(int(path[rest + 1 : end]))
            rest = end + 1
    return tokens


def _take_key(path: str, start: int) -> tuple[str, int]:
    end = start
    while end < len(path) and path[end] not in ".[":
        end += 1
    return path[start:end], end


class _Node:
    """One position in the value being rebuilt: a leaf or a container."""

    __slots__ = ("children", "has_leaf", "leaf")

    def __init__(self) -> None:
        self.children: dict[str | int, _Node] = {}
        self.has_leaf = False
        self.leaf: Any = None

    def build(self) -> Any:
        if not self.children:
            return self.leaf
        # A container wins over a leaf at the same path.  Two requests that
        # save at once can leave both; the container is the newer shape or
        # the older one, and either is a value the application wrote.
        if all(isinstance(key, int) for key in self.children):
            # Indexes in order, with any gap closed.  A list item whose TTL
            # ended, or a concurrent save, can leave ``[0]`` and ``[2]``.
            return [self.children[key].build() for key in sorted(self.children)]
        return {str(key): node.build() for key, node in self.children.items()}


def unflatten(fields: Mapping[str, str], *, geo: Iterable[str] = ()) -> dict[str, Any]:
    """Rebuild the session from its ``a:`` fields.  Other fields are ignored.

    A set of fields that no single save wrote still rebuilds: a gap in a list
    closes, and a container wins over a leaf at the same path.

    Raises:
        SessionStoreError: If a field name or a value cannot be read.
    """
    geo_paths = frozenset(geo)
    root = _Node()
    for name, raw in fields.items():
        if not name.startswith(ATTR_PREFIX):
            continue
        path = name[len(ATTR_PREFIX) :]
        try:
            if path in geo_paths and not raw.startswith(("[", "n")):
                lon, lat = raw.split(",")
                value: Any = [json.loads(lon), json.loads(lat)]
            else:
                value = json.loads(raw)
            node = root
            for token in _tokens(path):
                node = node.children.setdefault(token, _Node())
        except (ValueError, TypeError) as exc:
            raise SessionStoreError(f"Unreadable session field {name!r}") from exc
        node.has_leaf = True
        node.leaf = value
    return {str(key): node.build() for key, node in root.children.items()}


def diff(
    before: Mapping[str, str], after: Mapping[str, str]
) -> tuple[dict[str, str], list[str]]:
    """Return ``(fields to set, fields to delete)`` that turn *before* into
    *after*.

    A field with an equal value is in neither.  That matters: ``HSET`` removes
    the TTL of a field, so a save must not write a field that did not change.
    """
    to_set = {name: text for name, text in after.items() if before.get(name) != text}
    to_delete = [name for name in before if name not in after]
    return to_set, to_delete


def attribute_of(field: str) -> str | None:
    """The top-level attribute a field belongs to, or ``None`` for a system
    field."""
    if not field.startswith(ATTR_PREFIX):
        return None
    key, _ = _take_key(field[len(ATTR_PREFIX) :], 0)
    return unescape_key(key)


def fields_of(fields: Iterable[str], name: Any) -> list[str]:
    """The fields in *fields* that hold the top-level attribute *name*."""
    wanted = _key_text(name)
    return [field for field in fields if attribute_of(field) == wanted]


def _lookup(data: Mapping[Any, Any], path: str) -> tuple[bool, Any]:
    """Find the value at a declared *path* in *data*: ``(found, value)``."""
    value: Any = data
    for token in _tokens(path):
        if isinstance(token, int):
            if not isinstance(value, (list, tuple)) or token >= len(value):
                return False, None
            value = value[token]
        else:
            if not isinstance(value, Mapping):
                return False, None
            for key in value:
                if _key_text(key) == token:
                    value = value[key]
                    break
            else:
                return False, None
    return True, value


def check_searchable(searchable: Mapping[str, str]) -> dict[str, str]:
    """Validate a ``searchable`` declaration and return it with lower-case
    types.

    Raises:
        SessionConfigurationError: If a type is not one of tag, numeric, text
            or geo, or a path is not a string.
    """
    checked: dict[str, str] = {}
    for path, kind in searchable.items():
        name = str(kind).lower()
        if not isinstance(path, str) or not path or name not in SEARCHABLE_TYPES:
            raise SessionConfigurationError(
                f"searchable[{path!r}] must be one of "
                f"{sorted(SEARCHABLE_TYPES)}, got {kind!r}"
            )
        checked[path] = name
    return checked


def check_types(data: Mapping[Any, Any], searchable: Mapping[str, str]) -> None:
    """Refuse a declared attribute whose value has another type.

    The engine removes a whole session from an index when one indexed field
    has a value it cannot read, and the write that caused it succeeds.  So
    the store checks before it writes.  An absent attribute is allowed.

    Raises:
        SessionAttributeTypeError: For the first value that does not fit.
    """
    for path, kind in searchable.items():
        found, value = _lookup(data, path)
        if not found:
            continue
        if kind == TAG:
            valid = isinstance(value, (str, bool))
        elif kind == NUMERIC:
            valid = _is_number(value)
        elif kind == TEXT:
            valid = isinstance(value, str)
        else:
            valid = _is_point(value)
        if not valid:
            raise SessionAttributeTypeError(
                f"Session attribute {path!r} is declared as {kind} and cannot "
                f"hold a value of type {type(value).__name__}"
            )


def check_ttls(names: Iterable[Any], searchable: Mapping[str, str]) -> None:
    """Refuse a TTL on an attribute that is searchable, or holds a searchable
    path.

    When such a field expired, the session would lack an indexed field in
    normal operation.

    Raises:
        SessionConfigurationError: For the first such attribute.
    """
    for name in names:
        wanted = _key_text(name)
        for path in searchable:
            if unescape_key(_take_key(path, 0)[0]) == wanted:
                raise SessionConfigurationError(
                    f"Session attribute {name!r} cannot have a TTL: "
                    f"{path!r} is declared as searchable"
                )


def is_finite(value: Any) -> bool:
    """Whether *value* is a number a query can hold."""
    return _is_number(value) and math.isfinite(value)


def seal(text: str, encryptor: Encryptor | None) -> str:
    """Encrypt one field value, or return it unchanged with no encryptor.

    Base64, because ciphertext is arbitrary bytes and a hash field value goes
    to redis-py as text.
    """
    if encryptor is None:
        return text
    return base64.b64encode(encryptor.encrypt(text.encode())).decode("ascii")


def unseal(raw: str, encryptor: Encryptor | None) -> str:
    """The reverse of :func:`seal`.

    Raises:
        SessionStoreError: If the value does not decrypt.
    """
    if encryptor is None:
        return raw
    try:
        return encryptor.decrypt(base64.b64decode(raw, validate=True)).decode()
    except Exception as exc:
        raise SessionStoreError(f"Unreadable session record: {exc}") from exc
