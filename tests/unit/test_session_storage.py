"""Tests for what the store writes to Redis, and what it reads back.

One hash for each session, one field for each leaf.  These tests run the
store on ``fakeredis`` and read the hash.  Each one runs with and without an
``Encryptor``: the layout is the same, and only the values differ.

Design section 2; tests 1 to 7a and 41 to 43.
"""

from __future__ import annotations

import base64
import itertools
import json
from typing import Any

import pytest
from pydantic import BaseModel

from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import SessionStoreError
from redis_fastapi.session_backend import (
    RedisSessionStore,
    SessionState,
    _StoreCapabilities,
)
from redis_fastapi.session_layout import flatten
from tests.conftest import about
from tests.unit.session_stubs import names, record_commands

EVERY_TYPE = {
    "name": "Ana",
    "visits": 7,
    "ratio": 0.25,
    "admin": True,
    "nickname": None,
    "tags": [],
    "meta": {},
    "user": {"address": {"city": "Sofia"}},
    "bikes": [{"kind": "road"}, {"kind": "gravel", "gears": [1, 2]}],
}
SYSTEM_FIELDS = {"s:created", "s:seen", "s:sub", "s:desc"}


class Invert:
    """A cipher with the two properties of a real one that matter here.

    Most of its output is above 0x7F, and it starts with a nonce, so a
    second encryption of one value gives another ciphertext.
    """

    def __init__(self) -> None:
        self._nonce = itertools.count()

    def encrypt(self, data: bytes) -> bytes:
        nonce = next(self._nonce) % 256
        return bytes([nonce]) + bytes(b ^ 0xFF for b in data)

    def decrypt(self, data: bytes) -> bytes:
        return bytes(b ^ 0xFF for b in data[1:])


@pytest.fixture(autouse=True)
def _fresh_settings():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(params=[None, Invert], ids=["plain", "encrypted"])
def encryptor(request) -> Invert | None:
    return request.param() if request.param else None


@pytest.fixture()
def store(fake_async_redis, encryptor) -> RedisSessionStore:
    return RedisSessionStore(
        fake_async_redis,
        capabilities=_StoreCapabilities(supports_search=False, supports_hsetex=True),
        encryptor=encryptor,
        idle_ttl=60,
        absolute_ttl=600,
    )


async def _hash(redis, store: RedisSessionStore, sid: str) -> dict[str, str]:
    raw = await redis.hgetall(store.session_key(sid))
    return {k.decode(): v.decode() for k, v in raw.items()}


def _plain(store: RedisSessionStore, stored: str) -> str:
    """The plaintext of one stored ``a:`` value."""
    if store._encryptor is None:
        return stored
    return store._encryptor.decrypt(base64.b64decode(stored, validate=True)).decode()


async def _round_trip(store: RedisSessionStore, data: dict[Any, Any]) -> dict:
    sid = store.new_id()
    await store.create(sid, store.new_record(data))
    loaded = await store.load(sid)
    assert loaded is not None
    return loaded.record.data


class TestRoundTrip:
    async def test_each_type_loads_equal(self, store: RedisSessionStore) -> None:
        """Test 1."""
        assert await _round_trip(store, EVERY_TYPE) == EVERY_TYPE

    async def test_a_number_and_its_text_load_with_their_types(
        self, store: RedisSessionStore
    ) -> None:
        """Test 2."""
        data = await _round_trip(store, {"n": 7, "s": "7", "t": True, "one": 1})
        assert data == {"n": 7, "s": "7", "t": True, "one": 1}
        assert type(data["n"]) is int
        assert type(data["s"]) is str
        assert data["t"] is True
        assert type(data["one"]) is int

    @pytest.mark.parametrize("key", ["a.b", "a[0]", "]", "100%", "%2E", ""])
    async def test_a_key_with_a_path_character_loads_unchanged(
        self, store: RedisSessionStore, key: str
    ) -> None:
        """Test 3."""
        data = {key: "top", "nested": {key: [{key: 1}]}}
        assert await _round_trip(store, data) == data

    async def test_a_key_that_is_not_a_string_loads_as_a_string(
        self, store: RedisSessionStore
    ) -> None:
        """Test 3.  As JSON does."""
        data = await _round_trip(store, {1: "a", "d": {2: "b", None: "c", True: "d"}})
        assert data == {"1": "a", "d": {"2": "b", "null": "c", "true": "d"}}

    async def test_text_outside_ascii(self, store: RedisSessionStore) -> None:
        data = {"имя": "Ана Петрова", "emoji": "\U0001f6b2"}
        assert await _round_trip(store, data) == data

    async def test_an_empty_session(self, store: RedisSessionStore) -> None:
        assert await _round_trip(store, {}) == {}

    async def test_a_model_dump_loads_equal_and_validates(
        self, store: RedisSessionStore
    ) -> None:
        """Test 41."""

        class Item(BaseModel):
            sku: str
            quantity: int
            price: float

        class Cart(BaseModel):
            owner: str | None
            items: list[Item]
            gift: bool
            notes: dict[str, str]

        cart = Cart(
            owner=None,
            items=[
                Item(sku="b-1", quantity=2, price=9.5),
                Item(sku="b-2", quantity=1, price=3.0),
            ],
            gift=False,
            notes={},
        )
        written = {"cart": cart.model_dump(mode="json")}

        loaded = await _round_trip(store, written)

        assert loaded == written
        assert Cart.model_validate(loaded["cart"]) == cart

    async def test_a_value_with_no_json_form_is_a_store_error(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        with pytest.raises(SessionStoreError, match="Could not serialize"):
            await store.create(sid, store.new_record({"when": object()}))
        assert await fake_async_redis.keys("*") == []


class TestOneFieldForEachLeaf:
    async def test_the_field_names_of_a_session(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 4, and test 7: the same fields with and without an encryptor."""
        sid = store.new_id()
        await store.create(
            sid,
            store.new_record(EVERY_TYPE),
            subject="42",
            descriptor={"ua": "Firefox"},
        )

        stored = await _hash(fake_async_redis, store, sid)

        assert set(stored) == {
            "a:name",
            "a:visits",
            "a:ratio",
            "a:admin",
            "a:nickname",
            "a:tags",
            "a:meta",
            "a:user.address.city",
            "a:bikes[0].kind",
            "a:bikes[1].kind",
            "a:bikes[1].gears[0]",
            "a:bikes[1].gears[1]",
            *SYSTEM_FIELDS,
        }
        assert "d" not in stored

    async def test_each_value_is_the_json_text_of_its_leaf(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        await store.create(sid, store.new_record(EVERY_TYPE))

        stored = await _hash(fake_async_redis, store, sid)

        attributes = {
            name: _plain(store, value)
            for name, value in stored.items()
            if name.startswith("a:")
        }
        assert attributes == flatten(EVERY_TYPE)
        assert attributes["a:name"] == '"Ana"'
        assert attributes["a:visits"] == "7"
        assert attributes["a:admin"] == "true"
        assert attributes["a:nickname"] == "null"
        assert attributes["a:tags"] == "[]"
        assert attributes["a:meta"] == "{}"

    async def test_the_system_fields(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Never encrypted: the built-in index reads them."""
        sid = store.new_id()
        record = store.new_record({"user_id": 42})
        await store.create(sid, record, subject="42", descriptor={"ua": "Firefox"})

        stored = await _hash(fake_async_redis, store, sid)

        assert stored["s:sub"] == '"42"'
        assert json.loads(stored["s:desc"]) == {"ua": "Firefox"}
        assert float(stored["s:created"]) == record.metadata.created
        assert float(stored["s:seen"]) == record.metadata.last_access

    async def test_create_returns_the_plaintext_fields(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The baseline of the next save, with or without an encryptor."""
        sid = store.new_id()
        fields = await store.create(sid, store.new_record({"role": "admin"}))
        assert fields["a:role"] == '"admin"'
        assert set(fields) == {"a:role", "s:created", "s:seen"}
        assert set(await _hash(fake_async_redis, store, sid)) == set(fields)


class TestSystemFieldsAreApart:
    async def test_an_attribute_named_like_a_system_field_changes_none(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 5."""
        sid = store.new_id()
        data = {
            "s:sub": "evil",
            "s:created": 0,
            "created": 1,
            "seen": 2,
            "sub": "x",
            "desc": {"ua": "evil"},
        }
        record = store.new_record(data)
        await store.create(sid, record, subject="42", descriptor={"ua": "Firefox"})

        stored = await _hash(fake_async_redis, store, sid)
        assert stored["s:sub"] == '"42"'
        assert float(stored["s:created"]) == record.metadata.created
        assert json.loads(stored["s:desc"]) == {"ua": "Firefox"}
        assert {"a:s:sub", "a:s:created", "a:created", "a:seen", "a:sub"} <= set(stored)

        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == data
        assert loaded.record.metadata.created == record.metadata.created
        assert about(loaded.absolute_remaining, 600)

    async def test_no_system_field_is_in_the_session(
        self, store: RedisSessionStore
    ) -> None:
        sid = store.new_id()
        await store.create(
            sid, store.new_record({"n": 1}), subject="42", descriptor={"ua": "x"}
        )
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"n": 1}


class TestASaveWritesTheDifference:
    async def _saved(
        self, store: RedisSessionStore, redis, before: dict, after: dict, **kwargs
    ) -> tuple[str, list[tuple[str, ...]]]:
        """Create *before*, load it, save *after*; return the commands of
        the save."""
        sid = store.new_id()
        await store.create(sid, store.new_record(before), **kwargs)
        loaded = await store.load(sid)
        assert loaded is not None
        sent = record_commands(redis)
        await store.save(
            sid,
            store.new_record(after, created=loaded.record.metadata.created),
            baseline=loaded.fields,
            **kwargs,
        )
        return sid, sent

    async def test_a_changed_attribute_writes_that_field_only(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Tests 6 and 7a."""
        before = {"a": 1, "b": {"c": 2, "d": 3}, "e": [1, 2]}
        after = {"a": 1, "b": {"c": 2, "d": 4}, "e": [1, 2]}

        sid, sent = await self._saved(store, fake_async_redis, before, after)

        key = store.session_key(sid)
        assert names(sent) == ["HSET", "EXPIRE"]
        hset = sent[0]
        written = dict(zip(hset[2::2], hset[3::2], strict=True))
        assert set(written) == {"a:b.d", "s:seen"}
        assert _plain(store, written["a:b.d"]) == "4"
        assert sent[1] == ("EXPIRE", key, "60")
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == after

    async def test_a_shorter_list_removes_the_fields_of_the_removed_items(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 6."""
        before = {"bikes": ["road", "gravel", "city"], "n": 1}
        after = {"bikes": ["road"], "n": 1}

        sid, sent = await self._saved(store, fake_async_redis, before, after)

        key = store.session_key(sid)
        assert ("HDEL", key, "a:bikes[1]", "a:bikes[2]") in sent
        hset = next(command for command in sent if command[0] == "HSET")
        assert hset[2::2] == ("s:seen",), "no attribute field was written"
        stored = await _hash(fake_async_redis, store, sid)
        assert {n for n in stored if n.startswith("a:")} == {"a:bikes[0]", "a:n"}
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == after

    async def test_a_save_with_no_change_writes_no_attribute_field(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 7a.  A new encryption of an equal value gives another
        ciphertext, so the store compares plaintext."""
        sid, sent = await self._saved(
            store, fake_async_redis, EVERY_TYPE, dict(EVERY_TYPE)
        )

        assert names(sent) == ["HSET", "EXPIRE"]
        assert sent[0][2::2] == ("s:seen",)

    async def test_an_unchanged_stored_value_keeps_its_bytes(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The proof that the save did not write it.  The encryptor gives a
        new ciphertext each time, so a second write would change the bytes."""
        sid = store.new_id()
        fields = await store.create(sid, store.new_record({"a": 1, "b": 2}))
        before = await _hash(fake_async_redis, store, sid)

        await store.save(sid, store.new_record({"a": 1, "b": 3}), baseline=fields)

        after = await _hash(fake_async_redis, store, sid)
        assert after["a:a"] == before["a:a"]
        assert after["a:b"] != before["a:b"]

    async def test_a_removed_attribute_removes_each_of_its_fields(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        before = {"cart": {"total": 5, "items": [1, 2]}, "n": 1}
        sid, sent = await self._saved(store, fake_async_redis, before, {"n": 1})

        hdel = next(command for command in sent if command[0] == "HDEL")
        assert set(hdel[2:]) == {"a:cart.total", "a:cart.items[0]", "a:cart.items[1]"}
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"n": 1}

    async def test_the_subject_and_the_descriptor_are_written_only_on_a_change(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The store calls ``subject_of`` and ``descriptor_of`` on each save.
        It writes a field when the value differs from the value at load."""
        sid, sent = await self._saved(
            store,
            fake_async_redis,
            {"n": 1},
            {"n": 2},
            subject="42",
            descriptor={"ua": "Firefox"},
        )
        hset = next(command for command in sent if command[0] == "HSET")
        assert set(hset[2::2]) == {"a:n", "s:seen"}

    async def test_a_changed_subject_and_descriptor_are_written(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        fields = await store.create(
            sid, store.new_record({"n": 1}), subject="42", descriptor={"ua": "a"}
        )

        await store.save(
            sid,
            store.new_record({"n": 1}),
            baseline=fields,
            subject="43",
            descriptor={"ua": "b"},
        )

        stored = await _hash(fake_async_redis, store, sid)
        assert stored["s:sub"] == '"43"'
        assert json.loads(stored["s:desc"]) == {"ua": "b"}

    async def test_a_session_that_became_anonymous_loses_its_subject(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        fields = await store.create(
            sid, store.new_record({"n": 1}), subject="42", descriptor={"ua": "a"}
        )

        await store.save(sid, store.new_record({"n": 1}), baseline=fields)

        stored = await _hash(fake_async_redis, store, sid)
        assert "s:sub" not in stored
        assert "s:desc" not in stored

    async def test_save_returns_the_fields_the_session_now_holds(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        created = await store.create(sid, store.new_record({"a": 1, "b": [1, 2]}))

        saved = await store.save(
            sid, store.new_record({"a": 2, "b": [1]}), baseline=created
        )

        assert {n: v for n, v in saved.items() if n.startswith("a:")} == {
            "a:a": "2",
            "a:b[0]": "1",
        }
        assert saved["s:created"] == created["s:created"]
        assert set(saved) == set(await _hash(fake_async_redis, store, sid))

    async def test_a_save_with_no_baseline_writes_each_field(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        await store.create(sid, store.new_record({"a": 1, "gone": 2}))
        sent = record_commands(fake_async_redis)

        await store.save(sid, store.new_record({"a": 1, "b": 2}))

        hset = next(command for command in sent if command[0] == "HSET")
        assert set(hset[2::2]) == {"a:a", "a:b", "s:seen"}
        assert "HDEL" not in names(sent), "with no baseline the store removes none"


class TestWithAnEncryptor:
    """Test 7: the layout does not change, and no value is readable."""

    @pytest.fixture()
    def encryptor(self) -> Invert:
        return Invert()

    async def test_no_attribute_field_holds_its_plaintext(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        await store.create(
            sid, store.new_record(EVERY_TYPE), subject="42", descriptor={"ua": "x"}
        )

        stored = await _hash(fake_async_redis, store, sid)

        plain = flatten(EVERY_TYPE)
        assert {n for n in stored if n.startswith("a:")} == set(plain)
        for name, text in plain.items():
            assert stored[name] != text, f"{name} is stored in the clear"
            assert text not in stored[name]
            assert _plain(store, stored[name]) == text
        raw = await fake_async_redis.hvals(store.session_key(sid))
        assert not any(b"Sofia" in value or b"gravel" in value for value in raw)

    async def test_the_same_fields_as_with_no_encryptor(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        plain_store = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        first, second = store.new_id(), store.new_id()
        for target, sid in ((store, first), (plain_store, second)):
            await target.create(
                sid, target.new_record(EVERY_TYPE), subject="42", descriptor={"u": 1}
            )

        assert set(await _hash(fake_async_redis, store, first)) == set(
            await _hash(fake_async_redis, plain_store, second)
        )

    async def test_the_system_fields_stay_in_plaintext(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        record = store.new_record({"n": 1})
        await store.create(sid, record, subject="42", descriptor={"ua": "x"})

        stored = await _hash(fake_async_redis, store, sid)

        assert stored["s:sub"] == '"42"'
        assert stored["s:desc"] == '{"ua": "x"}'
        assert float(stored["s:created"]) == record.metadata.created
        assert float(stored["s:seen"]) == record.metadata.last_access

    async def test_the_load_gives_the_save_plaintext_to_compare(
        self, store: RedisSessionStore
    ) -> None:
        sid = store.new_id()
        await store.create(sid, store.new_record({"role": "admin"}))
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.fields["a:role"] == '"admin"'

    async def test_a_field_that_does_not_decrypt_is_a_failed_read(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """One field makes the record unreadable.  The record is kept, so
        the right key brings the session back."""
        sid = store.new_id()
        await store.create(sid, store.new_record({"a": 1, "b": 2}))
        await fake_async_redis.hset(store.session_key(sid), "a:b", "not base64!")

        assert await store.load_with_status(sid) == (None, True)
        assert await fake_async_redis.exists(store.session_key(sid)) == 1

    async def test_a_session_written_in_plaintext_does_not_load_as_one(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Turning encryption on does not read old values as ciphertext."""
        plain_store = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        sid = plain_store.new_id()
        await plain_store.create(sid, plain_store.new_record({"role": "admin"}))

        assert await store.load_with_status(sid) == (None, True)

    async def test_a_rotation_keeps_the_values_encrypted(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        fields = await store.create(sid, store.new_record({"role": "admin"}))
        state = SessionState(data={"role": "admin"}, session_id=sid, fields=fields)

        new_id = await store.rotate(state)

        stored = await _hash(fake_async_redis, store, new_id)
        assert stored["a:role"] != '"admin"'
        assert _plain(store, stored["a:role"]) == '"admin"'
        assert state.fields is not None
        assert state.fields["a:role"] == '"admin"'


class TestAnonymousSession:
    """Test 42."""

    async def test_it_has_no_subject_field_and_it_loads_and_saves(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        fields = await store.create(sid, store.new_record({"basket": [1]}))
        assert "s:sub" not in await _hash(fake_async_redis, store, sid)

        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"basket": [1]}

        await store.save(sid, store.new_record({"basket": [1, 2]}), baseline=fields)

        assert "s:sub" not in await _hash(fake_async_redis, store, sid)
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"basket": [1, 2]}

    @pytest.mark.parametrize("subject", [None, ""])
    async def test_an_empty_subject_is_no_subject(
        self, store: RedisSessionStore, fake_async_redis, subject
    ) -> None:
        sid = store.new_id()
        await store.create(sid, store.new_record({}), subject=subject)
        assert "s:sub" not in await _hash(fake_async_redis, store, sid)

    async def test_a_sign_in_adds_the_subject(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        fields = await store.create(sid, store.new_record({}))
        await store.save(
            sid, store.new_record({"user_id": 42}), baseline=fields, subject="42"
        )
        assert (await _hash(fake_async_redis, store, sid))["s:sub"] == '"42"'


class TestTheEarlierLayout:
    """Test 43.  An upgrade signs each user out once."""

    async def test_a_hash_with_field_d_loads_as_no_session_and_is_deleted(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        envelope = json.dumps({"m": {"c": 1.0, "a": 2.0}, "d": {"user_id": 42}})
        await fake_async_redis.hset(key, "d", envelope)
        await fake_async_redis.execute_command("HEXPIRE", key, 60, "FIELDS", 1, "d")
        await fake_async_redis.expire(key, 600)

        assert await store.load_with_status(sid) == (None, False)
        assert await fake_async_redis.exists(key) == 0

    async def test_a_hash_with_attributes_and_no_deadline_field_is_deleted(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """What a save leaves when it lands after the session ended."""
        sid = store.new_id()
        key = store.session_key(sid)
        await fake_async_redis.hset(key, mapping={"a:user_id": "42", "s:seen": "1.0"})
        await fake_async_redis.expire(key, 60)

        assert await store.load(sid) is None
        assert await fake_async_redis.exists(key) == 0
