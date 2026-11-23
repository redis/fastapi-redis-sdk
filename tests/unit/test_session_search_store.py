"""Tests for the search path of the store, with a stub of the Query Engine.

``fakeredis`` has no ``FT.*`` command.  ``session_stubs`` gives the fake
client the calls the store makes, so these tests show what the store sends
and how it reads each reply and each error.  The integration suite shows
what a real server does with the same queries.

Design sections 4 to 6; tests 17, 27b, 34 to 37b.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pytest
from fastapi import FastAPI
from redis.commands.search.result import Result
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

import redis_fastapi.session_backend as backend
from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import (
    SessionError,
    SessionSearchError,
    SessionSearchTimeoutError,
    SessionSearchUnavailableError,
    SessionStoreError,
)
from redis_fastapi.lifespan import _ensure_session_indexes
from redis_fastapi.session_backend import (
    RedisSessionStore,
    SessionState,
    _StoreCapabilities,
    probe_hsetex_support,
    probe_search_support,
)
from redis_fastapi.session_layout import FIELD_CREATED, FIELD_SEEN
from redis_fastapi.sessions import add_redis_sessions
from tests.conftest import aexpire_now
from tests.unit.session_stubs import (
    REPLY_FORMS,
    RESP3,
    RESP3_AS_RESULT,
    StubQueryEngine,
    install_query_engine,
    names,
    record_commands,
)

LIVE = r"@s\:created:[-inf +inf]"
SEARCHABLE = {
    "device": "tag",
    "cart.total": "numeric",
    "name": "text",
    "location": "geo",
}
TIMEOUT = "Timeout limit was reached"

# Each method that reads an index, as a call on a store.
SEARCHES = {
    "list_for_subject": lambda store: store.list_for_subject("42"),
    "count_for_subject": lambda store: store.count_for_subject("42"),
    "revoke_all": lambda store: store.revoke_all("42"),
    "list_active": lambda store: store.list_active(60),
    "revoke_idle": lambda store: store.revoke_idle(60),
    "find": lambda store: store.find("device", equals="d-91"),
    "count": lambda store: store.count("device", equals="d-91"),
}


def _caps(**kwargs: Any) -> _StoreCapabilities:
    return _StoreCapabilities(
        **{"supports_search": True, "supports_hsetex": True, **kwargs}
    )


@pytest.fixture(autouse=True)
def _fresh_settings():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(params=REPLY_FORMS)
def engine(request, fake_async_redis) -> StubQueryEngine:
    """A Query Engine on the fake client, once for each reply form."""
    return install_query_engine(fake_async_redis, form=request.param)


@pytest.fixture()
def store(fake_async_redis, engine) -> RedisSessionStore:
    return RedisSessionStore(
        fake_async_redis,
        capabilities=_caps(),
        idle_ttl=60,
        absolute_ttl=600,
        searchable=SEARCHABLE,
    )


async def _make(store: RedisSessionStore, subject: str | None = "42", **data) -> str:
    """Create a session of *subject* and return its ID."""
    sid = store.new_id()
    await store.create(sid, store.new_record(data), subject=subject)
    return sid


async def _seen(store: RedisSessionStore, redis, sid: str, ago: float) -> None:
    """Set the last access of a session to *ago* seconds before now.

    No test depends on the clock for ``s:seen``: it writes the field.
    """
    await redis.hset(store.session_key(sid), FIELD_SEEN, repr(time.time() - ago))


def _ids(infos) -> list[str]:
    return [info.session_id for info in infos]


class TestListing:
    async def test_lists_every_live_session_of_the_subject(
        self, store: RedisSessionStore
    ) -> None:
        first = await _make(store)
        second = await _make(store)
        assert set(_ids(await store.list_for_subject("42"))) == {first, second}

    async def test_a_subject_sees_only_their_own(
        self, store: RedisSessionStore
    ) -> None:
        mine = await _make(store, "42")
        await _make(store, "99")
        await _make(store, "421")
        assert _ids(await store.list_for_subject("42")) == [mine]

    async def test_an_anonymous_session_is_in_no_listing(
        self, store: RedisSessionStore
    ) -> None:
        await _make(store, None)
        assert await store.list_for_subject("") == []
        assert await store.list_for_subject("None") == []

    async def test_an_unknown_subject_lists_nothing(
        self, store: RedisSessionStore
    ) -> None:
        assert await store.list_for_subject("nobody") == []

    async def test_a_row_carries_the_times_and_the_descriptor(
        self, store: RedisSessionStore, monkeypatch
    ) -> None:
        """From the search reply alone: a listing never reads a session."""
        sid = store.new_id()
        record = store.new_record({"user_id": 42}, created=1000.5)
        await store.create(sid, record, subject="42", descriptor={"ua": "Firefox"})

        async def _unreachable(*args, **kwargs):
            raise AssertionError("listing read the session record")

        monkeypatch.setattr(store, "_read", _unreachable)
        (info,) = await store.list_for_subject("42")
        assert info.session_id == sid
        assert info.created == 1000.5
        assert info.last_access == record.metadata.last_access
        assert info.descriptor == {"ua": "Firefox"}

    async def test_a_session_with_no_descriptor_has_an_empty_one(
        self, store: RedisSessionStore
    ) -> None:
        await _make(store)
        (info,) = await store.list_for_subject("42")
        assert info.descriptor == {}

    async def test_the_newest_access_is_first(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        old = await _make(store)
        new = await _make(store)
        middle = await _make(store)
        await _seen(store, fake_async_redis, old, 300)
        await _seen(store, fake_async_redis, middle, 200)
        await _seen(store, fake_async_redis, new, 100)
        assert _ids(await store.list_for_subject("42")) == [new, middle, old]

    async def test_it_asks_the_built_in_index_for_at_most_1000(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        await store.list_for_subject("42")
        (search,) = engine.sent("FT.SEARCH")
        assert search[1] == "rf:si"
        assert search[2] == r'@s\:sub:{"\"42\""} ' + LIVE
        assert search[search.index("LIMIT") + 1 :][:2] == (0, 1000)
        assert search[search.index("SORTBY") + 1 :][:2] == ("s:seen", "DESC")
        assert search[search.index("DIALECT") + 1] == 2

    @pytest.mark.parametrize(
        "subject", ["a,b", "Ana", 'say "hi"', "back\\slash", "Петрова", "a b", "{x}|y"]
    )
    async def test_a_subject_with_a_special_character_is_found(
        self, store: RedisSessionStore, subject: str
    ) -> None:
        """The clause names the text the store wrote in ``s:sub``."""
        mine = await _make(store, subject)
        await _make(store, subject.lower() + "x")
        await _make(store, "a")
        assert _ids(await store.list_for_subject(subject)) == [mine]
        assert await store.count_for_subject(subject) == 1

    async def test_a_subject_that_is_not_a_string_matches_its_text(
        self, store: RedisSessionStore
    ) -> None:
        """``subject_of`` may return a number.  The store writes its text."""
        sid = store.new_id()
        await store.create(sid, store.new_record({}), subject=42)  # type: ignore[arg-type]
        assert _ids(await store.list_for_subject("42")) == [sid]
        assert _ids(await store.list_for_subject(42)) == [sid]  # type: ignore[arg-type]

    async def test_an_unreadable_descriptor_does_not_hide_the_session(
        self, store: RedisSessionStore, fake_async_redis, caplog
    ) -> None:
        """A descriptor is display data."""
        sid = await _make(store)
        await fake_async_redis.hset(store.session_key(sid), "s:desc", "{not json")
        with caplog.at_level(logging.WARNING, logger=backend.__name__):
            (info,) = await store.list_for_subject("42")
        assert info.session_id == sid
        assert info.descriptor == {}
        assert "Unreadable descriptor" in caplog.text


class TestTheLiveFilter:
    """A hash without ``s:created`` is not a session, in a search too."""

    async def test_a_session_past_its_deadline_is_in_no_list_and_no_count(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        live = await _make(store)
        ended = await _make(store)
        key = store.session_key(ended)
        await aexpire_now(fake_async_redis, key, FIELD_CREATED)
        assert await fake_async_redis.exists(key) == 1, "the key is still present"

        assert _ids(await store.list_for_subject("42")) == [live]
        assert await store.count_for_subject("42") == 1
        assert _ids(await store.list_active(60)) == [live]

    async def test_each_query_carries_it(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        for search in SEARCHES.values():
            await search(store)
        await store.find("cart.total", between=(1, 2))
        await store.find("name", text="ana")
        queries = engine.queries()
        assert len(queries) == len(SEARCHES) + 2
        for query in queries:
            assert query.endswith(" " + LIVE), query


class TestCounting:
    async def test_the_count_is_the_number_of_live_sessions(
        self, store: RedisSessionStore
    ) -> None:
        for _ in range(3):
            await _make(store, "42")
        await _make(store, "99")
        assert await store.count_for_subject("42") == 3
        assert await store.count_for_subject("99") == 1
        assert await store.count_for_subject("nobody") == 0

    async def test_a_count_asks_for_no_row_and_no_sort(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        """The count comes from the engine.  It is not the length of a list,
        so it is right for a subject with more than 1,000 sessions."""
        await _make(store)
        await store.count_for_subject("42")
        (search,) = engine.sent("FT.SEARCH")
        assert search[1] == "rf:si"
        assert search[search.index("LIMIT") + 1 :][:2] == (0, 0)
        assert "NOCONTENT" in search
        assert "SORTBY" not in search

    async def test_the_count_is_the_total_of_the_reply_not_its_rows(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        engine.replies.append(Result([5000], False))
        assert await store.count_for_subject("42") == 5000

    async def test_limit_is_accepted_and_does_not_change_the_count(
        self, store: RedisSessionStore
    ) -> None:
        """A caller written for the earlier store passes it."""
        for _ in range(3):
            await _make(store)
        assert await store.count_for_subject("42", limit=2) == 3


class TestRevokeById:
    """Test 17."""

    async def test_it_ends_a_session_of_the_right_subject(
        self, store: RedisSessionStore
    ) -> None:
        sid = await _make(store, "42")
        assert await store.revoke_id(sid, subject="42") is True
        assert await store.load(sid) is None

    async def test_it_refuses_a_session_of_another_subject(
        self, store: RedisSessionStore
    ) -> None:
        """Without the subject check this is a cross-user revocation primitive.

        Any handler taking an ID from a request could end a stranger's
        session, so the subject is required and compared with ``s:sub``.
        """
        theirs = await _make(store, "99")
        assert await store.revoke_id(theirs, subject="42") is False
        assert await store.load(theirs) is not None, "a stranger's session was ended"

    @pytest.mark.parametrize("subject", ["", "None", "null", '"42"', "4"])
    async def test_no_subject_ends_an_anonymous_session(
        self, store: RedisSessionStore, subject: str
    ) -> None:
        anonymous = await _make(store, None)
        mine = await _make(store, "42")
        assert await store.revoke_id(anonymous, subject=subject) is False
        assert await store.revoke_id(mine, subject=subject) is False
        assert await store.load(anonymous) is not None
        assert await store.load(mine) is not None

    async def test_an_unknown_id_is_refused_rather_than_erroring(
        self, store: RedisSessionStore
    ) -> None:
        assert await store.revoke_id(store.new_id(), subject="42") is False

    async def test_a_malformed_id_never_reaches_redis(
        self, store: RedisSessionStore, monkeypatch
    ) -> None:
        async def _unreachable(*args, **kwargs):
            raise AssertionError("a malformed ID reached Redis")

        monkeypatch.setattr(store, "_stored_subject", _unreachable)
        assert await store.revoke_id("not valid", subject="42") is False

    async def test_it_runs_no_search(
        self, store: RedisSessionStore, engine: StubQueryEngine, fake_async_redis
    ) -> None:
        """One ``HGET`` of ``s:sub`` and one ``DEL``."""
        sid = await _make(store, "42")
        other = await _make(store, "99")
        sent = record_commands(fake_async_redis)

        assert await store.revoke_id(sid, subject="42") is True
        assert await store.revoke_id(other, subject="42") is False

        assert engine.commands == []
        assert sent == [
            ("HGET", store.session_key(sid), "s:sub"),
            ("DEL", store.session_key(sid)),
            ("HGET", store.session_key(other), "s:sub"),
        ]

    async def test_it_works_with_no_query_engine(self, fake_async_redis) -> None:
        store = RedisSessionStore(
            fake_async_redis, capabilities=_caps(supports_search=False)
        )
        sid = await _make(store, "42")
        assert await store.revoke_id(sid, subject="99") is False
        assert await store.revoke_id(sid, subject="42") is True
        assert await fake_async_redis.exists(store.session_key(sid)) == 0


class TestRevokeAll:
    async def test_it_ends_every_session_of_a_subject(
        self, store: RedisSessionStore
    ) -> None:
        first = await _make(store)
        second = await _make(store)
        assert await store.revoke_all("42") == 2
        assert await store.load(first) is None
        assert await store.load(second) is None

    async def test_it_leaves_other_subjects_and_anonymous_sessions_alone(
        self, store: RedisSessionStore
    ) -> None:
        mine = await _make(store, "42")
        theirs = await _make(store, "99")
        anonymous = await _make(store, None)
        assert await store.revoke_all("42") == 1
        assert await store.load(mine) is None
        assert await store.load(theirs) is not None
        assert await store.load(anonymous) is not None

    async def test_an_unknown_subject_revokes_nothing(
        self, store: RedisSessionStore
    ) -> None:
        assert await store.revoke_all("nobody") == 0

    async def test_it_leaves_no_key_behind(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The subject is in the session's own hash.  No second key holds it."""
        await _make(store)
        await store.revoke_all("42")
        assert await fake_async_redis.keys("*") == []

    async def test_it_repeats_the_batch_until_the_query_returns_nothing(
        self, store: RedisSessionStore, engine: StubQueryEngine, monkeypatch
    ) -> None:
        """A subject can have more sessions than one batch holds."""
        monkeypatch.setattr(backend, "REVOKE_BATCH", 2)
        for _ in range(5):
            await _make(store)
        await _make(store, "99")

        assert await store.revoke_all("42") == 5

        searches = engine.sent("FT.SEARCH")
        assert len(searches) == 4, "three batches, and one that found nothing"
        for search in searches:
            assert search[search.index("LIMIT") + 1 :][:2] == (0, 2)
            assert "NOCONTENT" in search
            assert "SORTBY" not in search
        assert await store.count_for_subject("42") == 0
        assert await store.count_for_subject("99") == 1

    async def test_a_batch_is_1000_keys(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        await store.revoke_all("42")
        (search,) = engine.sent("FT.SEARCH")
        assert search[search.index("LIMIT") + 1 :][:2] == (0, 1000)

    async def test_it_counts_the_keys_removed_not_the_keys_matched(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        """An index can name a key for a moment after the key is gone."""
        live = await _make(store)
        gone = store.new_id()
        engine.replies.append(
            Result([2, store.session_key(live), store.session_key(gone)], False)
        )
        assert await store.revoke_all("42") == 1

    async def test_an_index_that_keeps_a_deleted_key_does_not_loop(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        """The index lets go of a deleted key in the background.

        Until it does, the query returns the key again.  The store stops
        when a batch holds nothing new.
        """
        sid = await _make(store)
        stale = Result([1, store.session_key(sid)], False)
        engine.replies.extend([stale, stale, stale])

        assert await store.revoke_all("42") == 1
        assert len(engine.sent("FT.SEARCH")) == 2

    async def test_a_failed_delete_raises(
        self, store: RedisSessionStore, monkeypatch
    ) -> None:
        await _make(store)

        async def _fail(ids):
            raise RedisConnectionError("down")

        monkeypatch.setattr(store, "_delete_many", _fail)
        with pytest.raises(SessionStoreError, match="Could not revoke sessions"):
            await store.revoke_all("42")


class TestLastActivity:
    async def test_list_active_returns_the_sessions_seen_in_the_window(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        recent = await _make(store, "42")
        anonymous = await _make(store, None)
        old = await _make(store, "99")
        await _seen(store, fake_async_redis, recent, 50)
        await _seen(store, fake_async_redis, anonymous, 10)
        await _seen(store, fake_async_redis, old, 500)

        assert _ids(await store.list_active(100)) == [anonymous, recent]
        assert _ids(await store.list_active(1000)) == [anonymous, recent, old]
        assert await store.list_active(5) == []

    async def test_limit_and_offset_page_with_no_repeat_and_no_gap(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        ids = [await _make(store) for _ in range(5)]
        for age, sid in enumerate(ids):
            await _seen(store, fake_async_redis, sid, age * 10)

        pages = [
            _ids(await store.list_active(1000, limit=2, offset=offset))
            for offset in (0, 2, 4)
        ]
        assert pages == [ids[0:2], ids[2:4], ids[4:5]]

    async def test_the_query_of_list_active(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        before = time.time()
        await store.list_active(100, limit=7, offset=3)
        after = time.time()

        (search,) = engine.sent("FT.SEARCH")
        assert search[1] == "rf:si"
        low, high = search[2].removeprefix(r"@s\:seen:[").split("]")[0].split(" ")
        assert before - 100 <= float(low) <= after - 100
        assert high == "+inf"
        assert search[search.index("LIMIT") + 1 :][:2] == (3, 7)

    async def test_revoke_idle_ends_the_sessions_not_seen_since(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        recent = await _make(store, "42")
        old = await _make(store, "42")
        older = await _make(store, None)
        await _seen(store, fake_async_redis, recent, 50)
        await _seen(store, fake_async_redis, old, 500)
        await _seen(store, fake_async_redis, older, 900)

        assert await store.revoke_idle(100) == 2

        assert await store.load(recent) is not None
        assert await store.load(old) is None
        assert await store.load(older) is None

    async def test_the_query_of_revoke_idle(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        before = time.time()
        assert await store.revoke_idle(100) == 0
        after = time.time()

        (search,) = engine.sent("FT.SEARCH")
        assert search[1] == "rf:si"
        low, high = search[2].removeprefix(r"@s\:seen:[").split("]")[0].split(" ")
        assert low == "-inf"
        assert before - 100 <= float(high) <= after - 100
        assert "NOCONTENT" in search


class TestCustomSearch:
    """The wiring of ``find`` and ``count``.  What the engine matches is for
    the integration suite."""

    async def test_find_by_a_tag(self, store: RedisSessionStore) -> None:
        mine = await _make(store, device="d-91")
        await _make(store, device="d-92")
        await _make(store)
        assert _ids(await store.find("device", equals="d-91")) == [mine]
        assert await store.count("device", equals="d-91") == 1

    async def test_find_by_a_boolean_tag(self, fake_async_redis, engine) -> None:
        store = RedisSessionStore(
            fake_async_redis, capabilities=_caps(), searchable={"beta": "tag"}
        )
        yes = await _make(store, beta=True)
        await _make(store, beta=False)
        await _make(store, beta="true")
        assert _ids(await store.find("beta", equals=True)) == [yes]

    async def test_find_by_a_range_on_a_nested_path(
        self, store: RedisSessionStore
    ) -> None:
        small = await _make(store, cart={"total": 50})
        large = await _make(store, cart={"total": 150.5})
        assert _ids(await store.find("cart.total", between=(100, None))) == [large]
        assert _ids(await store.find("cart.total", between=(None, 100))) == [small]
        assert await store.count("cart.total", between=(0, 200)) == 2
        assert await store.count("cart.total", between=[50, 50]) == 1

    async def test_find_by_text(self, store: RedisSessionStore) -> None:
        ana = await _make(store, name="Ana Petrova")
        await _make(store, name="Jean Picard")
        assert _ids(await store.find("name", text="petrova")) == [ana]
        assert await store.count("name", text="petrova") == 1

    async def test_find_near_a_place(self, store: RedisSessionStore) -> None:
        sofia = await _make(store, location=[23.32, 42.69])
        await _make(store, location=[24.75, 42.14])
        assert _ids(await store.find("location", near=(23.3, 42.7, 5000))) == [sofia]
        assert await store.count("location", near=[23.3, 42.7, 500_000]) == 2
        loaded = await store.load(sofia)
        assert loaded is not None
        assert loaded.record.data == {"location": [23.32, 42.69]}

    async def test_the_query_goes_to_the_custom_index(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        await store.find("cart.total", between=(100, None), limit=5, offset=10)
        await store.count("location", near=(23.32, 42.69, 5000))
        await store.find("name", text="pet*")

        page, count, text = engine.sent("FT.SEARCH")
        assert page[1] == count[1] == text[1] == store._custom_index
        assert page[1] != "rf:si"
        assert page[2] == r"@a\:cart\.total:[100 +inf] " + LIVE
        assert page[page.index("LIMIT") + 1 :][:2] == (10, 5)
        assert page[page.index("SORTBY") + 1 :][:2] == ("s:seen", "DESC")
        assert count[2] == r"@a\:location:[23.32 42.69 5000 m] " + LIVE
        assert count[count.index("LIMIT") + 1 :][:2] == (0, 0)
        assert text[2] == r"@a\:name:(pet*) " + LIVE

    async def test_find_returns_one_page_of_100_by_default(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        await store.find("device", equals="d-91")
        (search,) = engine.sent("FT.SEARCH")
        assert search[search.index("LIMIT") + 1 :][:2] == (0, 100)

    async def test_text_with_a_syntax_error_raises_value_error(
        self, store: RedisSessionStore
    ) -> None:
        """Test 27a, with the stub in the place of the server."""
        with pytest.raises(ValueError, match="Syntax error"):
            await store.find("name", text="(")


class TestFindArguments:
    """Tests 34, 34a and 27b.  Each call is refused before a command."""

    @pytest.fixture()
    def store(self) -> RedisSessionStore:
        """A store on a client that fails the test when it is used."""
        return RedisSessionStore(
            _NoCommands(), capabilities=_caps(), searchable=SEARCHABLE
        )

    @pytest.mark.parametrize("method", ["find", "count"])
    @pytest.mark.parametrize(
        ("name", "filters"),
        [
            pytest.param("role", {"equals": "admin"}, id="not-declared"),
            pytest.param("cart", {"between": (1, 2)}, id="parent-of-a-declared-path"),
            pytest.param("device", {}, id="no-filter"),
            pytest.param("device", {"equals": "a", "text": "a"}, id="two-filters"),
            pytest.param(
                "cart.total", {"between": (1, 2), "equals": "1"}, id="two-filters-2"
            ),
            pytest.param("device", {"between": (1, 2)}, id="tag-with-between"),
            pytest.param("device", {"text": "d-91"}, id="tag-with-text"),
            pytest.param("cart.total", {"equals": "5"}, id="numeric-with-equals"),
            pytest.param("name", {"equals": "ana"}, id="text-with-equals"),
            pytest.param("location", {"between": (1, 2)}, id="geo-with-between"),
            pytest.param("device", {"equals": 5}, id="tag-value-number"),
            pytest.param("device", {"equals": None}, id="tag-value-none"),
            pytest.param("cart.total", {"between": 5}, id="between-not-a-pair"),
            pytest.param("cart.total", {"between": (1, 2, 3)}, id="between-three"),
            pytest.param("cart.total", {"between": ("1", "2")}, id="between-strings"),
            pytest.param("cart.total", {"between": (True, 2)}, id="between-bool"),
            pytest.param("name", {"text": 5}, id="text-value-number"),
            pytest.param("location", {"near": (23.3, 42.7)}, id="near-two"),
            pytest.param("location", {"near": "23.3,42.7,5"}, id="near-string"),
            pytest.param("location", {"near": (23.3, 42.7, None)}, id="near-none"),
        ],
    )
    async def test_a_call_that_does_not_fit_the_declaration_is_refused(
        self, store: RedisSessionStore, method: str, name: str, filters: dict
    ) -> None:
        with pytest.raises(ValueError):
            await getattr(store, method)(name, **filters)

    @pytest.mark.parametrize("method", ["find", "count"])
    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(lambda bad: ("cart.total", {"between": (bad, 5)}), id="low"),
            pytest.param(lambda bad: ("cart.total", {"between": (1, bad)}), id="high"),
            pytest.param(lambda bad: ("location", {"near": (bad, 42, 5)}), id="lon"),
            pytest.param(lambda bad: ("location", {"near": (23, bad, 5)}), id="lat"),
            pytest.param(
                lambda bad: ("location", {"near": (23, 42, bad)}), id="radius"
            ),
        ],
    )
    async def test_a_number_that_is_not_finite_is_refused(
        self, store: RedisSessionStore, method: str, bad: float, build
    ) -> None:
        """Test 27b."""
        name, filters = build(bad)
        with pytest.raises(ValueError, match="finite"):
            await getattr(store, method)(name, **filters)

    async def test_a_store_with_no_declaration_refuses_each_name(self) -> None:
        store = RedisSessionStore(_NoCommands(), capabilities=_caps())
        with pytest.raises(ValueError, match="not a searchable"):
            await store.find("device", equals="d-91")
        with pytest.raises(ValueError, match="not a searchable"):
            await store.count("device", equals="d-91")

    @pytest.mark.parametrize(
        ("page", "message"),
        [
            pytest.param({"limit": 0}, "limit", id="limit-zero"),
            pytest.param({"limit": -1}, "limit", id="limit-negative"),
            pytest.param({"limit": 1.5}, "limit", id="limit-float"),
            pytest.param({"limit": True}, "limit", id="limit-bool"),
            pytest.param({"limit": None}, "limit", id="limit-none"),
            pytest.param({"offset": -1}, "offset", id="offset-negative"),
            pytest.param({"offset": "0"}, "offset", id="offset-string"),
            pytest.param({"limit": 10_001}, "10000", id="limit-over"),
            pytest.param({"limit": 100, "offset": 9_901}, "10000", id="sum-over"),
            pytest.param({"limit": 1, "offset": 10_000}, "10000", id="offset-at-end"),
        ],
    )
    async def test_a_page_out_of_range_is_refused(
        self, store: RedisSessionStore, page: dict, message: str
    ) -> None:
        """Test 34a."""
        with pytest.raises(ValueError, match=message):
            await store.find("device", equals="d-91", **page)
        with pytest.raises(ValueError, match=message):
            await store.list_active(60, **page)

    async def test_the_last_page_the_store_allows(
        self, fake_async_redis, engine: StubQueryEngine
    ) -> None:
        store = RedisSessionStore(
            fake_async_redis, capabilities=_caps(), searchable=SEARCHABLE
        )
        assert await store.find("device", equals="d", limit=100, offset=9_900) == []
        assert await store.list_active(60, limit=10_000) == []
        first, second = engine.sent("FT.SEARCH")
        assert first[first.index("LIMIT") + 1 :][:2] == (9_900, 100)
        assert second[second.index("LIMIT") + 1 :][:2] == (0, 10_000)


class _NoCommands:
    """A client that fails the test when the store uses it."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"the store used redis.{name}")


class TestASearchThatTimedOut:
    """Test 36.  A partial answer is worse than none.

    A bulk revoke that ends a part of the matches and reports success is a
    security defect, so a timeout is an error and never a short result.
    """

    @pytest.mark.parametrize("method", list(SEARCHES))
    @pytest.mark.parametrize("message", [TIMEOUT, "Query timed out"])
    async def test_a_server_timeout_error_raises(
        self,
        store: RedisSessionStore,
        engine: StubQueryEngine,
        method: str,
        message: str,
    ) -> None:
        sid = await _make(store, device="d-91")
        engine.errors["FT.SEARCH"] = [ResponseError(message)]

        with pytest.raises(SessionSearchTimeoutError, match="timed out"):
            await SEARCHES[method](store)

        assert await store.load(sid) is not None, "a timed-out search ended a session"

    @staticmethod
    async def _partial(redis, form: str) -> tuple[RedisSessionStore, list[str]]:
        """Three old sessions on a server that returns one, with a warning."""
        engine = install_query_engine(redis, form=form)
        store = RedisSessionStore(
            redis,
            capabilities=_caps(),
            idle_ttl=60,
            absolute_ttl=600,
            searchable=SEARCHABLE,
        )
        ids = [await _make(store, device="d-91") for _ in range(3)]
        for sid in ids:
            await _seen(store, redis, sid, 600)
        engine.warning = [TIMEOUT]
        engine.truncate = 1
        return store, ids

    @pytest.mark.parametrize("method", list(SEARCHES))
    async def test_a_resp3_reply_with_a_timeout_warning_raises(
        self, fake_async_redis, method: str
    ) -> None:
        """The server answered with the rows it had, and a warning."""
        store, ids = await self._partial(fake_async_redis, RESP3)

        with pytest.raises(SessionSearchTimeoutError, match="partial"):
            await SEARCHES[method](store)

        for sid in ids:
            assert await store.load(sid) is not None, (
                "a search that timed out ended a session"
            )

    @pytest.mark.skipif(
        RESP3_AS_RESULT not in REPLY_FORMS,
        reason="this redis-py returns a dict on RESP3, not a Result",
    )
    async def test_a_timeout_warning_on_a_result_object_raises(
        self, fake_async_redis
    ) -> None:
        """The same reply, in the form redis-py 8 gives by default.

        From version 8 the client talks RESP3 and still returns a ``Result``
        object.  The warning of the server is in ``Result.warnings``.  A
        ``revoke_all`` that reads it as a full answer ends a part of the
        sessions and reports success.
        """
        store, ids = await self._partial(fake_async_redis, RESP3_AS_RESULT)

        with pytest.raises(SessionSearchTimeoutError, match="partial"):
            await store.revoke_all("42")

        for sid in ids:
            assert await store.load(sid) is not None, (
                "a search that timed out ended a session"
            )

    async def test_a_warning_that_is_not_a_timeout_is_not_an_error(
        self, fake_async_redis
    ) -> None:
        engine = install_query_engine(fake_async_redis, form=RESP3)
        store = RedisSessionStore(fake_async_redis, capabilities=_caps())
        sid = await _make(store)
        engine.warning = ["Max prefix expansions limit was reached"]
        assert _ids(await store.list_for_subject("42")) == [sid]

    def test_the_error_is_in_the_hierarchy_of_the_design(self) -> None:
        """A handler for ``SessionStoreError`` catches both search errors."""
        for error in (SessionSearchTimeoutError, SessionSearchUnavailableError):
            assert issubclass(error, SessionSearchError)
            assert issubclass(error, SessionStoreError)
            assert issubclass(error, SessionError)


class TestSearchErrors:
    @pytest.mark.parametrize("method", ["find", "count"])
    async def test_a_syntax_error_raises_value_error_with_the_message(
        self, store: RedisSessionStore, engine: StubQueryEngine, method: str
    ) -> None:
        engine.errors["FT.SEARCH"] = [ResponseError("Syntax error at offset 12 near x")]
        with pytest.raises(ValueError, match="Syntax error at offset 12 near x"):
            await getattr(store, method)("name", text="x) | (y")

    @pytest.mark.parametrize("method", list(SEARCHES))
    @pytest.mark.parametrize(
        "error",
        [
            ResponseError("WRONGTYPE something else"),
            RedisConnectionError("Redis is down"),
            OSError("connection reset by peer"),
        ],
        ids=["response", "connection", "oserror"],
    )
    async def test_any_other_failure_raises_a_search_error(
        self,
        store: RedisSessionStore,
        engine: StubQueryEngine,
        method: str,
        error: BaseException,
    ) -> None:
        """Never an empty answer.  For a count, ``0`` is the permissive one:
        a cap on concurrent sessions would let each sign-in through."""
        engine.errors["FT.SEARCH"] = [error]
        with pytest.raises(SessionSearchError, match="Could not search sessions"):
            await SEARCHES[method](store)

    async def test_a_search_error_is_not_a_timeout(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        engine.errors["FT.SEARCH"] = [RedisConnectionError("Redis is down")]
        with pytest.raises(SessionSearchError) as raised:
            await store.list_for_subject("42")
        assert not isinstance(raised.value, SessionSearchTimeoutError)
        assert not isinstance(raised.value, SessionSearchUnavailableError)


class TestEnsureIndexes:
    async def test_it_creates_the_built_in_index_over_the_session_hashes(
        self, fake_async_redis, engine: StubQueryEngine
    ) -> None:
        store = RedisSessionStore(fake_async_redis, capabilities=_caps())
        await store.ensure_indexes()

        assert engine.indexes == {"rf:si": "rf:s:"}
        assert engine.schemas["rf:si"] == [
            ["s:sub", "TAG", "SEPARATOR", "\x1f", "CASESENSITIVE"],
            ["s:seen", "NUMERIC", "SORTABLE"],
            ["s:created", "NUMERIC"],
        ]

    async def test_with_a_declaration_it_creates_the_custom_index_too(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        await store.ensure_indexes()

        custom = store._custom_index
        assert custom is not None
        assert engine.indexes == {"rf:si": "rf:s:", custom: "rf:s:"}
        assert [line[:2] for line in engine.schemas[custom]] == [
            ["s:seen", "NUMERIC"],
            ["s:created", "NUMERIC"],
            ["a:cart.total", "NUMERIC"],
            ["a:device", "TAG"],
            ["a:location", "GEO"],
            ["a:name", "TEXT"],
        ]

    async def test_the_names_follow_the_key_prefix(
        self, fake_async_redis, engine: StubQueryEngine
    ) -> None:
        store = RedisSessionStore(
            fake_async_redis,
            capabilities=_caps(),
            key_prefix="app",
            searchable={"role": "tag"},
        )
        await store.ensure_indexes()
        assert set(engine.indexes.values()) == {"app:s:"}
        assert sorted(engine.indexes) == [
            "app:sa:" + store._custom_index[-8:],
            "app:si",
        ]

    async def test_index_already_exists_counts_as_success(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        """Test 37b.  Each worker runs this at startup, and one of them wins."""
        await store.ensure_indexes()
        before = dict(engine.indexes)

        await store.ensure_indexes()

        assert engine.indexes == before
        assert len(engine.sent("FT.CREATE")) == 4
        assert engine.sent("FT.DROPINDEX") == []

    @pytest.mark.parametrize(
        "error",
        [
            ResponseError("WRONGTYPE not an index"),
            RedisConnectionError("Redis is down"),
        ],
        ids=["response", "connection"],
    )
    async def test_any_other_create_error_raises(
        self, store: RedisSessionStore, engine: StubQueryEngine, error
    ) -> None:
        engine.errors["FT.CREATE"] = [error]
        with pytest.raises(SessionSearchError, match="Could not create"):
            await store.ensure_indexes()
        assert store._caps.ready_indexes == set()

    async def test_a_changed_declaration_drops_the_old_custom_index(
        self, fake_async_redis, engine: StubQueryEngine, caplog
    ) -> None:
        old = RedisSessionStore(
            fake_async_redis, capabilities=_caps(), searchable={"device": "tag"}
        )
        await old.ensure_indexes()
        new = RedisSessionStore(
            fake_async_redis, capabilities=_caps(), searchable={"device": "text"}
        )
        assert new._custom_index != old._custom_index

        with caplog.at_level(logging.WARNING, logger=backend.__name__):
            await new.ensure_indexes()

        assert sorted(engine.indexes) == sorted(["rf:si", new._custom_index])
        assert engine.sent("FT.DROPINDEX") == [
            ("FT.DROPINDEX", old._custom_index, False)
        ], "one drop, of the old custom index, that keeps the sessions"
        assert f"Dropping the session attribute index {old._custom_index}" in (
            caplog.text
        )

    async def test_the_same_declaration_drops_nothing(
        self, fake_async_redis, engine: StubQueryEngine, caplog
    ) -> None:
        for _ in range(2):
            store = RedisSessionStore(
                fake_async_redis, capabilities=_caps(), searchable={"device": "tag"}
            )
            with caplog.at_level(logging.WARNING, logger=backend.__name__):
                await store.ensure_indexes()
        assert engine.sent("FT.DROPINDEX") == []
        assert caplog.text == ""

    async def test_with_no_declaration_each_custom_index_is_dropped(
        self, fake_async_redis, engine: StubQueryEngine
    ) -> None:
        declared = RedisSessionStore(
            fake_async_redis, capabilities=_caps(), searchable={"device": "tag"}
        )
        await declared.ensure_indexes()
        plain = RedisSessionStore(fake_async_redis, capabilities=_caps())

        await plain.ensure_indexes()

        assert engine.indexes == {"rf:si": "rf:s:"}

    async def test_it_drops_no_index_it_does_not_own(
        self, fake_async_redis, engine: StubQueryEngine
    ) -> None:
        """Not the built-in index, not an index of another prefix, and not
        an index of the application."""
        others = {"app:sa:12345678": "app:s:", "products": "p:", "rf:sax": "x:"}
        engine.indexes.update(others)
        engine.schemas.update(dict.fromkeys(others, []))
        store = RedisSessionStore(
            fake_async_redis, capabilities=_caps(), searchable={"device": "tag"}
        )

        await store.ensure_indexes()
        await store.ensure_indexes()

        assert engine.sent("FT.DROPINDEX") == []
        assert set(engine.indexes) == {"rf:si", store._custom_index, *others}

    async def test_index_not_found_on_a_drop_counts_as_success(
        self, fake_async_redis, engine: StubQueryEngine
    ) -> None:
        """Test 37b.  Another worker dropped it first."""
        old = RedisSessionStore(
            fake_async_redis, capabilities=_caps(), searchable={"device": "tag"}
        )
        await old.ensure_indexes()
        new = RedisSessionStore(
            fake_async_redis, capabilities=_caps(), searchable={"device": "text"}
        )
        engine.errors["FT.DROPINDEX"] = [ResponseError("Index not found")]

        await new.ensure_indexes()

        assert len(engine.sent("FT.DROPINDEX")) == 1
        assert new._custom_index in new._caps.ready_indexes

    async def test_a_listed_index_that_is_gone_by_the_drop_is_no_error(
        self, fake_async_redis, engine: StubQueryEngine
    ) -> None:
        """The same case with no injected error: the list names an index
        that is gone by the time of the drop."""
        new = RedisSessionStore(
            fake_async_redis, capabilities=_caps(), searchable={"device": "text"}
        )

        async def stale_list() -> list[bytes]:
            return [b"rf:si", b"rf:sa:00000000"]

        engine.list_indexes = stale_list  # type: ignore[method-assign]
        await new.ensure_indexes()
        assert engine.sent("FT.DROPINDEX") == [
            ("FT.DROPINDEX", "rf:sa:00000000", False)
        ]

    async def test_any_other_drop_error_raises(
        self, fake_async_redis, engine: StubQueryEngine
    ) -> None:
        old = RedisSessionStore(
            fake_async_redis, capabilities=_caps(), searchable={"device": "tag"}
        )
        await old.ensure_indexes()
        new = RedisSessionStore(
            fake_async_redis, capabilities=_caps(), searchable={"device": "text"}
        )
        engine.errors["FT.DROPINDEX"] = [RedisConnectionError("Redis is down")]
        with pytest.raises(SessionSearchError, match="Could not create"):
            await new.ensure_indexes()

    async def test_a_failed_list_raises(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        engine.errors["FT._LIST"] = [RedisConnectionError("Redis is down")]
        with pytest.raises(SessionSearchError, match="Could not create"):
            await store.ensure_indexes()

    async def test_the_first_search_ensures_the_indexes_once(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        """A store that the lifespan did not start still finds its index."""
        await store.list_for_subject("42")
        await store.count_for_subject("42")
        await store.find("device", equals="d-91")

        assert len(engine.sent("FT.CREATE")) == 2
        assert len(engine.sent("FT._LIST")) == 1
        assert names(engine.commands)[:3] == ["FT.CREATE", "FT.CREATE", "FT._LIST"]

    async def test_stores_that_share_capabilities_ensure_once(
        self, fake_async_redis, engine: StubQueryEngine
    ) -> None:
        """The store is built for each request.  The pool keeps what is
        ready."""
        caps = _caps()
        for _ in range(3):
            store = RedisSessionStore(fake_async_redis, capabilities=caps)
            await store.list_for_subject("42")
        assert len(engine.sent("FT.CREATE")) == 1
        assert caps.ready_indexes == {"rf:si"}

    async def test_with_no_query_engine_it_does_nothing(self) -> None:
        store = RedisSessionStore(
            _NoCommands(),
            capabilities=_caps(supports_search=False),
            searchable=SEARCHABLE,
        )
        await store.ensure_indexes()
        assert store._caps.ready_indexes == set()


class TestAnIndexThatWentMissing:
    """A flush, or a failover to an empty node, takes the index with it."""

    async def test_the_store_creates_it_again_and_asks_once_more(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        sid = await _make(store)
        assert _ids(await store.list_for_subject("42")) == [sid]
        engine.indexes.clear()
        engine.schemas.clear()
        engine.commands.clear()

        assert _ids(await store.list_for_subject("42")) == [sid]

        assert names(engine.commands) == [
            "FT.SEARCH",
            "FT.CREATE",
            "FT.CREATE",
            "FT._LIST",
            "CONFIG GET",
            "FT.SEARCH",
        ]
        assert "rf:si" in engine.indexes

    @pytest.mark.parametrize("method", list(SEARCHES))
    async def test_each_search_method_recovers(
        self, store: RedisSessionStore, engine: StubQueryEngine, method: str
    ) -> None:
        await store.ensure_indexes()
        engine.errors["FT.SEARCH"] = [ResponseError("rf:si: no such index")]
        await SEARCHES[method](store)

    async def test_an_index_that_stays_missing_raises_after_one_retry(
        self, store: RedisSessionStore, engine: StubQueryEngine
    ) -> None:
        await store.ensure_indexes()
        engine.errors["FT.SEARCH"] = [
            ResponseError("No such index rf:si"),
            ResponseError("No such index rf:si"),
            ResponseError("No such index rf:si"),
        ]
        with pytest.raises(SessionSearchError, match="Could not search sessions"):
            await store.count_for_subject("42")
        assert len(engine.sent("FT.SEARCH")) == 2


class TestNoQueryEngine:
    """Tests 35 and 35a."""

    @pytest.mark.parametrize("method", list(SEARCHES))
    async def test_each_search_method_raises_and_sends_no_command(
        self, method: str
    ) -> None:
        store = RedisSessionStore(
            _NoCommands(),
            capabilities=_caps(supports_search=False),
            searchable=SEARCHABLE,
        )
        with pytest.raises(SessionSearchUnavailableError, match="no Query Engine"):
            await SEARCHES[method](store)

    async def test_load_save_rotate_and_revoke_work(self, fake_async_redis) -> None:
        engine = install_query_engine(fake_async_redis)
        sent = record_commands(fake_async_redis)
        store = RedisSessionStore(
            fake_async_redis,
            capabilities=_caps(supports_search=False),
            idle_ttl=60,
            absolute_ttl=600,
        )
        sid = store.new_id()
        fields = await store.create(sid, store.new_record({"n": 1}), subject="42")

        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"n": 1}

        await store.save(sid, store.new_record({"n": 2}), baseline=fields, subject="42")
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"n": 2}

        state = SessionState(
            data={"n": 2}, session_id=sid, subject="42", fields=loaded.fields
        )
        new_id = await store.rotate(state)
        assert new_id != sid
        assert await store.load(sid) is None
        assert (await store.load(new_id)).record.data == {"n": 2}  # type: ignore[union-attr]

        assert await store.revoke_id(new_id, subject="99") is False
        assert await store.revoke_id(new_id, subject="42") is True

        other = store.new_id()
        await store.create(other, store.new_record({}))
        await store.touch(other)
        await store.revoke(SessionState(data={}, session_id=other))
        assert await fake_async_redis.keys("*") == []

        assert engine.commands == [], "a search command went to a server without one"
        assert "HSETEX" not in names(sent)

    async def test_the_warning_is_logged_once_for_a_pool(
        self, fake_async_redis, caplog
    ) -> None:
        """Test 35a.  The pool shares one capabilities object, and builds a
        store for each request."""
        caps = _StoreCapabilities(supports_search=False, supports_hsetex=True)
        with caplog.at_level(logging.WARNING, logger=backend.__name__):
            for _ in range(3):
                store = RedisSessionStore(
                    fake_async_redis, capabilities=caps, searchable=SEARCHABLE
                )
                sid = await _make(store)
                for _ in range(3):
                    await store.load(sid)
                    await store.touch(sid)
                await store.ensure_indexes()
                for search in SEARCHES.values():
                    with pytest.raises(SessionSearchUnavailableError):
                        await search(store)

        warnings = [r for r in caplog.records if "no Query Engine" in r.getMessage()]
        assert len(warnings) == 1
        assert warnings[0].levelno == logging.WARNING
        assert "session indexing is unavailable" in warnings[0].getMessage()

    async def test_a_server_with_a_query_engine_logs_no_warning(
        self, store: RedisSessionStore, caplog
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=backend.__name__):
            sid = await _make(store)
            await store.load(sid)
            await store.list_for_subject("42")
        assert "no Query Engine" not in caplog.text

    async def test_the_store_asks_the_server_once(
        self, fake_async_redis, monkeypatch, caplog
    ) -> None:
        """``fakeredis`` has no ``FT.SEARCH``, so the real probe says no."""
        asked: list[str] = []
        original = backend.probe_search_support

        async def counting(client):
            asked.append("search")
            return await original(client)

        monkeypatch.setattr(backend, "probe_search_support", counting)
        caps = _StoreCapabilities()
        with caplog.at_level(logging.WARNING, logger=backend.__name__):
            for _ in range(2):
                store = RedisSessionStore(fake_async_redis, capabilities=caps)
                with pytest.raises(SessionSearchUnavailableError):
                    await store.list_for_subject("42")
                await store.load(store.new_id())

        assert asked == ["search"]
        assert caps.supports_search is False
        assert caplog.text.count("no Query Engine") == 1

    async def test_no_answer_is_not_cached_and_not_logged(
        self, fake_async_redis, monkeypatch, caplog
    ) -> None:
        """While the capability check has no answer, a search is refused and
        the load does not write ``s:seen``.  The next call asks again."""
        answers: list[bool | None] = [None, None, True]

        async def probe(client):
            return answers.pop(0)

        monkeypatch.setattr(backend, "probe_search_support", probe)
        engine = install_query_engine(fake_async_redis)
        sent = record_commands(fake_async_redis)
        caps = _StoreCapabilities(supports_hsetex=True)
        store = RedisSessionStore(fake_async_redis, capabilities=caps)
        sid = await _make(store)

        with caplog.at_level(logging.WARNING, logger=backend.__name__):
            with pytest.raises(SessionSearchUnavailableError):
                await store.list_for_subject("42")
            assert caps.supports_search is None
            await store.load(sid)
            assert "HSETEX" not in names(sent)
            assert engine.commands == []

            assert _ids(await store.list_for_subject("42")) == [sid]

        assert caps.supports_search is True
        assert "no Query Engine" not in caplog.text


class TestTheTimeoutPolicyWarning:
    """Test 36b.  With a policy other than ``fail``, a server answers a
    query that timed out with a partial result and no error."""

    async def test_a_policy_other_than_fail_is_logged_once(
        self, fake_async_redis, engine: StubQueryEngine, caplog
    ) -> None:
        engine.policy = "return"
        caps = _caps()
        with caplog.at_level(logging.WARNING, logger=backend.__name__):
            for _ in range(3):
                store = RedisSessionStore(fake_async_redis, capabilities=caps)
                await store.ensure_indexes()
                await store.ensure_indexes()
                await store.list_for_subject("42")

        warnings = [r for r in caplog.records if "search-on-timeout" in r.getMessage()]
        assert len(warnings) == 1
        assert "'return'" in warnings[0].getMessage()
        assert "partial result" in warnings[0].getMessage()
        assert engine.sent("CONFIG GET")[0] == ("CONFIG GET", "search-on-timeout")

    @pytest.mark.parametrize("policy", ["fail", "FAIL"])
    async def test_fail_logs_nothing(
        self, fake_async_redis, engine: StubQueryEngine, caplog, policy: str
    ) -> None:
        engine.policy = policy
        store = RedisSessionStore(fake_async_redis, capabilities=_caps())
        with caplog.at_level(logging.WARNING, logger=backend.__name__):
            await store.ensure_indexes()
        assert "search-on-timeout" not in caplog.text
        assert len(engine.sent("CONFIG GET")) == 1

    async def test_a_server_that_refuses_config_get_does_not_stop_the_start(
        self, fake_async_redis, engine: StubQueryEngine, caplog
    ) -> None:
        """Managed platforms commonly refuse ``CONFIG``."""
        engine.policy = None
        store = RedisSessionStore(fake_async_redis, capabilities=_caps())
        with caplog.at_level(logging.WARNING, logger=backend.__name__):
            await store.ensure_indexes()
        assert "search-on-timeout" not in caplog.text
        assert await store.list_for_subject("42") == []

    async def test_a_reply_from_a_client_that_does_not_decode(
        self, fake_async_redis, engine: StubQueryEngine, caplog
    ) -> None:
        async def config_get(pattern: str) -> dict[bytes, bytes]:
            return {b"search-on-timeout": b"return"}

        fake_async_redis.config_get = config_get
        store = RedisSessionStore(fake_async_redis, capabilities=_caps())
        with caplog.at_level(logging.WARNING, logger=backend.__name__):
            await store.ensure_indexes()
        assert caplog.text.count("search-on-timeout is 'return'") == 1


class _CommandInfo:
    """A client that answers ``COMMAND INFO`` with one reply."""

    def __init__(self, reply: Any) -> None:
        self._reply = reply
        self.commands: list[tuple[Any, ...]] = []

    async def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        self.commands.append(args)
        if isinstance(self._reply, BaseException):
            raise self._reply
        return self._reply


class TestTheProbes:
    """``probe_search_support`` gives the three answers of
    ``probe_hsetex_support``."""

    @pytest.mark.parametrize(
        ("reply", "answer"),
        [
            pytest.param([["ft.search", -3, [], 0, 0, 0]], True, id="resp2-known"),
            pytest.param([None], False, id="resp2-unknown"),
            pytest.param([], False, id="empty"),
            pytest.param({"ft.search": {"arity": -3}}, True, id="resp3-known"),
            pytest.param({"ft.search": None}, False, id="resp3-unknown"),
            pytest.param({}, False, id="resp3-empty"),
            pytest.param(TypeError("'NoneType' is not subscriptable"), False, id="nil"),
            pytest.param(RedisConnectionError("down"), None, id="no-answer"),
            pytest.param(OSError("reset"), None, id="no-answer-oserror"),
        ],
    )
    async def test_the_answer_for_each_reply(self, reply, answer) -> None:
        assert await probe_search_support(_CommandInfo(reply)) is answer
        assert await probe_hsetex_support(_CommandInfo(reply)) is answer

    async def test_each_probe_asks_for_its_command(self) -> None:
        search, hsetex = _CommandInfo([None]), _CommandInfo([None])
        await probe_search_support(search)
        await probe_hsetex_support(hsetex)
        assert search.commands == [("COMMAND", "INFO", "FT.SEARCH")]
        assert hsetex.commands == [("COMMAND", "INFO", "HSETEX")]


class _PoolState:
    """The part of the pool state that the index setup reads."""

    def __init__(self, client: Any, capabilities: _StoreCapabilities | None) -> None:
        self._client = client
        self.session_capabilities = capabilities

    def get_async_client(self) -> Any:
        return self._client


class TestTheLifespanEnsuresTheIndexes:
    """The lifespan creates the indexes once, at pool init, so the first
    search of a worker does not pay for it."""

    async def test_it_creates_the_indexes_of_the_declaration(
        self, fake_async_redis, engine: StubQueryEngine
    ) -> None:
        app = FastAPI()
        add_redis_sessions(app, searchable={"device": "tag"}, key_prefix="app")
        pool = _PoolState(fake_async_redis, _caps())

        await _ensure_session_indexes(app, pool)

        assert len(engine.indexes) == 2
        assert "app:si" in engine.indexes
        assert pool.session_capabilities.ready_indexes == set(engine.indexes)

    async def test_the_requests_share_what_the_start_learned(
        self, fake_async_redis, engine: StubQueryEngine
    ) -> None:
        """The setup makes the capabilities object of the pool when there is
        none, and ``fakeredis`` then answers that it has no Query Engine."""
        app = FastAPI()
        add_redis_sessions(app)
        pool = _PoolState(fake_async_redis, None)

        await _ensure_session_indexes(app, pool)

        assert pool.session_capabilities is not None
        assert pool.session_capabilities.supports_search is False
        assert engine.commands == []

    @pytest.mark.parametrize("wired", ["no-sessions", "store", "store-factory"])
    async def test_it_leaves_an_application_store_alone(
        self, fake_async_redis, engine: StubQueryEngine, wired: str
    ) -> None:
        """A store that the application supplies owns its indexes."""
        app = FastAPI()
        own = RedisSessionStore(fake_async_redis, capabilities=_caps())
        if wired == "store":
            add_redis_sessions(app, store=own)
        elif wired == "store-factory":
            add_redis_sessions(app, store_factory=lambda request: own)
        pool = _PoolState(_NoCommands(), None)

        await _ensure_session_indexes(app, pool)

        assert engine.commands == []
        assert pool.session_capabilities is None

    async def test_a_failure_does_not_stop_the_start(
        self, fake_async_redis, engine: StubQueryEngine
    ) -> None:
        """Best effort.  The store creates a missing index on the first
        search."""
        app = FastAPI()
        add_redis_sessions(app)
        pool = _PoolState(fake_async_redis, _caps())
        engine.errors["FT.CREATE"] = [RedisConnectionError("Redis is down")]

        await _ensure_session_indexes(app, pool)

        assert engine.indexes == {}
        store = RedisSessionStore(
            fake_async_redis, capabilities=pool.session_capabilities
        )
        assert await store.list_for_subject("42") == []
        assert "rf:si" in engine.indexes
