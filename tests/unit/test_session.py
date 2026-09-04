"""Unit tests for the :class:`Session` mapping.

Every test here corresponds to a row of the table in Section 8 of
``docs/specs/session-design.md``.  The last four rows exist because the
upstream Starlette session gets them wrong, so each one fails against that
implementation and passes against ours.
"""

from __future__ import annotations

import pytest

from redis_fastapi.exceptions import (
    SessionConfigurationError,
    SessionError,
    SessionStoreError,
)
from redis_fastapi.sessions import Session


class TestFlagsStartClean:
    """Construction is the middleware loading us, not the application."""

    def test_empty_session_is_untouched(self) -> None:
        s = Session()
        assert s.accessed is False
        assert s.modified is False

    def test_populated_session_is_untouched(self) -> None:
        s = Session({"user_id": 42})
        assert s == {"user_id": 42}
        assert s.accessed is False
        assert s.modified is False


class TestRawDoesNotMark:
    """The store and ``principal_of`` must be able to look without touching."""

    def test_raw_leaves_both_flags_clean(self) -> None:
        s = Session({"user_id": 42})
        assert s.raw() == {"user_id": 42}
        assert s.accessed is False
        assert s.modified is False

    def test_raw_is_a_copy_not_a_view(self) -> None:
        s = Session({"a": 1})
        snapshot = s.raw()
        snapshot["a"] = 2
        assert s["a"] == 1
        assert type(snapshot) is dict

    @pytest.mark.parametrize("spelling", [dict, lambda s: s.copy()])
    def test_the_obvious_spellings_do_mark(self, spelling) -> None:
        """Why ``raw()`` exists.

        CPython routes both ``dict(session)`` and ``session.copy()`` through
        the subclass's ``keys()``, so both mark.  If this ever stops being
        true, ``raw()`` can be simplified - but not before.
        """
        s = Session({"a": 1})
        spelling(s)
        assert s.accessed is True


class TestReadsMarkAccessedOnly:
    @pytest.mark.parametrize(
        "read",
        [
            lambda s: s["a"],
            lambda s: s.get("a"),
            lambda s: "a" in s,
            lambda s: list(s),
            lambda s: list(s.keys()),
            lambda s: list(s.values()),
            lambda s: list(s.items()),
        ],
    )
    def test_read_sets_accessed_and_not_modified(self, read) -> None:
        s = Session({"a": 1})
        read(s)
        assert s.accessed is True
        assert s.modified is False

    def test_mark_accessed_is_the_starlette_hook(self) -> None:
        # Starlette calls this by name via hasattr; the name is load-bearing.
        s = Session()
        assert hasattr(s, "mark_accessed")
        s.mark_accessed()
        assert s.accessed is True
        assert s.modified is False


class TestWritesMarkBoth:
    @pytest.mark.parametrize(
        "write",
        [
            lambda s: s.__setitem__("b", 2),
            lambda s: s.__delitem__("a"),
            lambda s: s.clear(),
            lambda s: s.update({"b": 2}),
            lambda s: s.setdefault("b", 2),
        ],
    )
    def test_write_sets_both_flags(self, write) -> None:
        s = Session({"a": 1})
        write(s)
        assert s.modified is True
        assert s.accessed is True, "a modification is also an access"


class TestTheFourUpstreamFaults:
    """Section 8's table.  Each of these is a bug in Starlette's session."""

    def test_popitem_sets_the_flags(self) -> None:
        s = Session({"a": 1})
        key, value = s.popitem()
        assert (key, value) == ("a", 1)
        assert s.modified is True, "upstream popitem() sets no flag"
        assert s.accessed is True

    def test_ior_sets_the_flags(self) -> None:
        s = Session({"a": 1})
        s |= {"b": 2}
        assert s == {"a": 1, "b": 2}
        assert s.modified is True, "dict.__ior__ runs in C and skips update()"
        assert s.accessed is True

    def test_pop_sets_accessed_as_well_as_modified(self) -> None:
        s = Session({"a": 1})
        assert s.pop("a") == 1
        assert s.modified is True
        assert s.accessed is True, "upstream pop() sets modified but not accessed"

    def test_nested_mutation_is_invisible(self) -> None:
        """The documented limit.  No dict subclass can see this."""
        s = Session({"a": {"b": 0}})
        s["a"]["b"] = 1
        assert s.modified is False, (
            "if this ever becomes True the escape route in Section 8 is no "
            "longer needed and the docs must change"
        )
        assert s.accessed is True, "reading s['a'] is still an access"


class TestSetdefaultDoesNotWriteOnHit:
    def test_hit_is_a_read(self) -> None:
        s = Session({"a": 1})
        assert s.setdefault("a", 99) == 1
        assert s.modified is False, "setdefault on an existing key changes nothing"
        assert s.accessed is True

    def test_miss_is_a_write(self) -> None:
        s = Session({"a": 1})
        assert s.setdefault("b", 2) == 2
        assert s.modified is True


class TestPopWithDefault:
    def test_missing_key_with_default_is_a_read(self) -> None:
        s = Session({"a": 1})
        assert s.pop("nope", "fallback") == "fallback"
        assert s.modified is False, "nothing was removed, so nothing to write"
        assert s.accessed is True

    def test_missing_key_without_default_raises(self) -> None:
        s = Session()
        with pytest.raises(KeyError):
            s.pop("nope")


class TestOrReturnsAFreshSession:
    def test_or_does_not_mutate_the_original(self) -> None:
        s = Session({"a": 1})
        merged = s | {"b": 2}
        assert merged == {"a": 1, "b": 2}
        assert s == {"a": 1}
        assert s.modified is False
        assert isinstance(merged, Session)
        assert merged.modified is False, "a new object has not been modified"


class TestExceptionHierarchy:
    def test_one_base_catches_everything(self) -> None:
        assert issubclass(SessionConfigurationError, SessionError)
        assert issubclass(SessionStoreError, SessionError)

    def test_store_error_is_not_a_configuration_error(self) -> None:
        assert not issubclass(SessionStoreError, SessionConfigurationError)
