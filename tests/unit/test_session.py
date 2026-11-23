"""Unit tests for the :class:`Session` mapping.

Every test here corresponds to a feature of the session module.
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


class TestAttributeTtl:
    """``set(name, value, ttl=N)`` asks for a TTL on one attribute.

    ``Session`` only carries the request.  The store reads it at the save,
    with ``pending_ttls()``.
    """

    def test_set_with_a_ttl_writes_the_value_and_the_pending_ttl(self) -> None:
        s = Session()
        s.set("flash", "Settings saved.", ttl=60)
        assert s == {"flash": "Settings saved."}
        assert s.pending_ttls() == {"flash": 60}
        assert s.modified is True
        assert s.accessed is True

    def test_set_with_no_ttl_is_a_plain_write(self) -> None:
        s = Session()
        s.set("flash", "Profile saved.")
        assert s == {"flash": "Profile saved."}
        assert s.pending_ttls() == {}
        assert s.modified is True

    def test_a_new_session_has_no_pending_ttl(self) -> None:
        assert Session({"flash": "x"}).pending_ttls() == {}

    def test_the_last_ttl_for_a_name_wins(self) -> None:
        s = Session()
        s.set("flash", "a", ttl=60)
        s.set("flash", "b", ttl=30)
        s.set("other", 1, ttl=5)
        assert s.pending_ttls() == {"flash": 30, "other": 5}

    @pytest.mark.parametrize(
        "ttl", [0, -1, 1.5, 60.0, "60", True, False, [60], float("inf")]
    )
    def test_a_ttl_that_is_not_a_positive_integer_is_refused(self, ttl) -> None:
        """Test 33a.  At the call, before anything is written."""
        s = Session({"flash": "old"})
        with pytest.raises(ValueError, match="positive integer"):
            s.set("flash", "new", ttl=ttl)
        assert s == {"flash": "old"}
        assert s.pending_ttls() == {}
        assert s.modified is False

    def test_a_refused_ttl_keeps_the_earlier_one(self) -> None:
        s = Session()
        s.set("flash", "a", ttl=60)
        with pytest.raises(ValueError):
            s.set("flash", "b", ttl=0)
        assert s == {"flash": "a"}
        assert s.pending_ttls() == {"flash": 60}

    def test_pending_ttls_does_not_mark_the_session(self) -> None:
        """The store calls it on each save, on sessions nobody touched."""
        s = Session({"flash": "x"})
        s.pending_ttls()
        assert s.accessed is False
        assert s.modified is False

    def test_pending_ttls_is_a_copy(self) -> None:
        s = Session()
        s.set("flash", "a", ttl=60)
        s.pending_ttls()["flash"] = 1
        s.pending_ttls().clear()
        assert s.pending_ttls() == {"flash": 60}

    def test_clear_pending_ttls_keeps_the_data_and_the_flags(self) -> None:
        s = Session()
        s.set("flash", "a", ttl=60)
        s.clear_pending_ttls()
        assert s.pending_ttls() == {}
        assert s == {"flash": "a"}
        assert s.modified is True

    def test_the_pending_ttls_are_not_part_of_the_data(self) -> None:
        """The store writes ``raw()``.  The slot never reaches Redis."""
        s = Session()
        s.set("flash", "a", ttl=60)
        s.persist("flash")
        assert s.raw() == {"flash": "a"}
        assert list(dict.items(s)) == [("flash", "a")]
        assert "_ttls" in Session.__slots__

    def test_a_name_that_is_not_a_string(self) -> None:
        s = Session()
        s.set(7, "x", ttl=5)
        assert s.pending_ttls() == {7: 5}


class TestPersist:
    """``persist(name)`` asks the store to remove the TTL and keep the value."""

    def test_persist_marks_the_name_and_the_session(self) -> None:
        s = Session({"flash": "x"})
        s.persist("flash")
        assert s.pending_ttls() == {"flash": None}
        assert s == {"flash": "x"}
        assert s.modified is True, "the save must run to send the request"
        assert s.accessed is True

    def test_persist_on_an_absent_attribute_does_nothing(self) -> None:
        """No pending entry, and the session is not modified: a request that
        did only this writes nothing."""
        s = Session({"other": 1})
        s.persist("flash")
        assert s.pending_ttls() == {}
        assert s == {"other": 1}
        assert s.modified is False

    def test_persist_replaces_a_pending_ttl(self) -> None:
        s = Session()
        s.set("flash", "a", ttl=60)
        s.persist("flash")
        assert s.pending_ttls() == {"flash": None}

    def test_set_with_a_ttl_after_persist_gives_the_ttl(self) -> None:
        """Test 33c: the last call for a name decides."""
        s = Session({"flash": "x"})
        s.persist("flash")
        s.set("flash", "y", ttl=30)
        assert s.pending_ttls() == {"flash": 30}

    def test_persist_does_not_need_the_value(self) -> None:
        """It reads nothing through an accessor the application overrides."""
        s = Session({"flash": {"big": "value"}})
        s.persist("flash")
        assert s["flash"] == {"big": "value"}


# Each change of a name other than ``set`` with a TTL, and ``persist``.
_OTHER_CHANGES = {
    "setitem": lambda s: s.__setitem__("flash", "new"),
    "set-no-ttl": lambda s: s.set("flash", "new"),
    "set-ttl-none": lambda s: s.set("flash", "new", ttl=None),
    "del": lambda s: s.__delitem__("flash"),
    "pop": lambda s: s.pop("flash"),
    "pop-default": lambda s: s.pop("flash", None),
    "popitem": lambda s: s.popitem(),
    "update-mapping": lambda s: s.update({"flash": "new"}),
    "update-pairs": lambda s: s.update([("flash", "new")]),
    "update-keyword": lambda s: s.update(flash="new"),
    "ior": lambda s: s.__ior__({"flash": "new"}),
    "clear": lambda s: s.clear(),
}


class TestEachOtherChangeRemovesThePendingEntry:
    """The last call for a name decides, with no rule for each pair of
    calls.  Test 33c."""

    @pytest.mark.parametrize("change", list(_OTHER_CHANGES))
    def test_after_set_with_a_ttl(self, change: str) -> None:
        s = Session({"other": 1})
        s.set("other", 2, ttl=99)
        s.set("flash", "old", ttl=60)
        _OTHER_CHANGES[change](s)
        assert "flash" not in s.pending_ttls()

    @pytest.mark.parametrize("change", list(_OTHER_CHANGES))
    def test_after_persist(self, change: str) -> None:
        s = Session({"other": 1, "flash": "old"})
        s.persist("flash")
        _OTHER_CHANGES[change](s)
        assert "flash" not in s.pending_ttls()

    @pytest.mark.parametrize(
        "change", [c for c in _OTHER_CHANGES if c not in ("clear", "popitem")]
    )
    def test_a_change_of_one_name_keeps_the_entry_of_another(self, change) -> None:
        s = Session()
        s.set("other", 2, ttl=99)
        s.set("flash", "old", ttl=60)
        _OTHER_CHANGES[change](s)
        assert s.pending_ttls() == {"other": 99}

    def test_a_pop_then_a_new_write_has_no_ttl(self) -> None:
        s = Session()
        s.set("flash", "old", ttl=60)
        s.pop("flash")
        s["flash"] = "new"
        assert s.pending_ttls() == {}

    def test_setdefault_that_writes_has_no_ttl(self) -> None:
        s = Session()
        s.set("flash", "old", ttl=60)
        del s["flash"]
        assert s.setdefault("flash", "new") == "new"
        assert s.pending_ttls() == {}

    def test_setdefault_on_a_present_name_changes_nothing(self) -> None:
        """A hit is a read, so the pending TTL of the earlier write stays."""
        s = Session()
        s.set("flash", "old", ttl=60)
        assert s.setdefault("flash", "new") == "old"
        assert s.pending_ttls() == {"flash": 60}

    def test_a_pop_that_removes_nothing_changes_nothing(self) -> None:
        s = Session()
        s.set("flash", "old", ttl=60)
        s.pop("absent", None)
        assert s.pending_ttls() == {"flash": 60}

    def test_a_read_changes_nothing(self) -> None:
        s = Session()
        s.set("flash", "old", ttl=60)
        _ = s["flash"], s.get("flash"), "flash" in s, list(s.items())
        assert s.pending_ttls() == {"flash": 60}


class TestUpdateStillWritesEachForm:
    """``update`` and ``|=`` look at the names they change, to drop each
    pending TTL.  That look must not use up the argument."""

    @pytest.mark.parametrize(
        "merge",
        [
            lambda s, pairs: s.update(pairs),
            lambda s, pairs: s.__ior__(pairs),
        ],
        ids=["update", "ior"],
    )
    def test_pairs_from_an_iterator_are_written(self, merge) -> None:
        """``dict.update`` takes an iterable of pairs, and so does this
        signature.  A generator can be read once."""
        s = Session()
        s.set("a", 0, ttl=60)
        merge(s, ((name, 1) for name in "ab"))
        assert s == {"a": 1, "b": 1}
        assert s.pending_ttls() == {}
        assert s.modified is True
