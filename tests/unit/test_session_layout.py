"""Tests for how a session maps to the fields of one hash.

``session_layout`` has no Redis in it, so these tests give it a value and
check the fields.  Design sections 2.1 to 2.3, tests 1 to 7a.
"""

from __future__ import annotations

import base64

import pytest
from pydantic import BaseModel

from redis_fastapi.exceptions import (
    SessionAttributeTypeError,
    SessionConfigurationError,
    SessionStoreError,
)
from redis_fastapi.session_layout import (
    ATTR_PREFIX,
    FIELD_CREATED,
    FIELD_DESCRIPTOR,
    FIELD_SEEN,
    FIELD_SUBJECT,
    attribute_of,
    check_searchable,
    check_ttls,
    check_types,
    diff,
    escape_key,
    fields_of,
    flatten,
    is_finite,
    seal,
    unescape_key,
    unflatten,
    unseal,
)

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


class Xor:
    """A reversible cipher that turns each byte into another one."""

    def encrypt(self, data: bytes) -> bytes:
        return bytes(b ^ 0xFF for b in data)

    def decrypt(self, data: bytes) -> bytes:
        return bytes(b ^ 0xFF for b in data)


class TestFieldNames:
    def test_the_system_fields_and_the_attribute_prefix(self) -> None:
        """The names are stored in each session, so they are part of the
        format.  A change orphans every stored session."""
        assert FIELD_SUBJECT == "s:sub"
        assert FIELD_CREATED == "s:created"
        assert FIELD_SEEN == "s:seen"
        assert FIELD_DESCRIPTOR == "s:desc"
        assert ATTR_PREFIX == "a:"

    @pytest.mark.parametrize(
        ("value", "fields"),
        [
            ({"role": "admin"}, {"a:role": '"admin"'}),
            (
                {"user": {"address": {"city": "Sofia"}}},
                {"a:user.address.city": '"Sofia"'},
            ),
            (
                {"bikes": ["road", "gravel"]},
                {"a:bikes[0]": '"road"', "a:bikes[1]": '"gravel"'},
            ),
            ({"tags": []}, {"a:tags": "[]"}),
            ({"meta": {}}, {"a:meta": "{}"}),
            (
                {"rows": [{"n": 1}, [2, 3]]},
                {"a:rows[0].n": "1", "a:rows[1][0]": "2", "a:rows[1][1]": "3"},
            ),
        ],
    )
    def test_one_field_for_each_leaf(self, value, fields) -> None:
        """Test 4: the table of section 2.1."""
        assert flatten(value) == fields

    def test_no_field_holds_the_whole_session(self) -> None:
        """Test 4: the earlier layout kept the session in one field, ``d``."""
        fields = flatten(EVERY_TYPE)
        assert "d" not in fields
        assert all(name.startswith(ATTR_PREFIX) for name in fields)

    def test_an_empty_session_has_no_field(self) -> None:
        assert flatten({}) == {}
        assert unflatten({}) == {}


class TestValues:
    @pytest.mark.parametrize(
        ("value", "stored"),
        [
            (7, "7"),
            ("7", '"7"'),
            (True, "true"),
            (None, "null"),
            (0.5, "0.5"),
            ("Петрова", '"Петрова"'),
        ],
    )
    def test_each_value_is_json_text(self, value, stored) -> None:
        """Section 2.2.  Text outside ASCII is stored as it is, so a search
        can match it."""
        assert flatten({"x": value}) == {"a:x": stored}

    def test_a_number_and_its_text_keep_their_types(self) -> None:
        """Test 2."""
        fields = flatten({"n": 7, "s": "7"})
        assert fields["a:n"] != fields["a:s"]
        back = unflatten(fields)
        assert back == {"n": 7, "s": "7"}
        assert type(back["n"]) is int
        assert type(back["s"]) is str

    def test_a_value_with_no_json_form_raises(self) -> None:
        with pytest.raises(TypeError):
            flatten({"when": object()})

    def test_a_key_with_no_json_form_raises(self) -> None:
        with pytest.raises(TypeError, match="keys must be"):
            flatten({("a", "b"): 1})


class TestRoundTrip:
    def test_each_type_loads_equal(self) -> None:
        """Test 1."""
        assert unflatten(flatten(EVERY_TYPE)) == EVERY_TYPE

    def test_the_types_survive(self) -> None:
        """Equal is not enough: ``True == 1`` and ``1.0 == 1``."""
        back = unflatten(flatten({"flag": True, "one": 1, "real": 1.0}))
        assert back["flag"] is True
        assert type(back["one"]) is int
        assert type(back["real"]) is float

    def test_a_tuple_loads_as_a_list(self) -> None:
        """As JSON does: the stored form has one kind of sequence."""
        assert unflatten(flatten({"pair": (1, 2)})) == {"pair": [1, 2]}

    @pytest.mark.parametrize(
        "key", ["a.b", "a[0]", "a]", "100%", "%2E", "a.b[1]%25", ".", "[", ""]
    )
    def test_a_key_with_a_path_character_loads_unchanged(self, key: str) -> None:
        """Test 3.  A ``.`` in a key must not read as a nested dict."""
        value = {key: 1, "outer": {key: [1, {key: "x"}]}}
        assert unflatten(flatten(value)) == value

    def test_the_escape_of_each_path_character(self) -> None:
        assert escape_key("a.b[0]%") == "a%2Eb%5B0%5D%25"
        assert unescape_key("a%2Eb%5B0%5D%25") == "a.b[0]%"
        assert flatten({"a.b": {"c": 1}}) == {"a:a%2Eb.c": "1"}

    def test_an_escape_is_not_escaped_twice(self) -> None:
        """``%`` goes first on the way in and last on the way out."""
        assert unescape_key(escape_key("%2E")) == "%2E"
        assert escape_key("%2E") == "%252E"

    @pytest.mark.parametrize(
        ("key", "text"),
        [(1, "1"), (1.5, "1.5"), (True, "true"), (False, "false"), (None, "null")],
    )
    def test_a_key_that_is_not_a_string_loads_as_a_string(self, key, text) -> None:
        """Test 3.  The string is the one JSON writes for the key."""
        assert unflatten(flatten({key: "v", "d": {key: "w"}})) == {
            text: "v",
            "d": {text: "w"},
        }

    def test_a_model_dump_loads_equal_and_validates(self) -> None:
        """Test 41.  The store needs no model to give each type back."""

        class Address(BaseModel):
            city: str
            zip: str | None = None

        class Profile(BaseModel):
            name: str
            age: int
            score: float
            active: bool
            address: Address
            tags: list[str]
            history: list[Address]

        profile = Profile(
            name="Ana",
            age=41,
            score=9.5,
            active=True,
            address=Address(city="Sofia"),
            tags=[],
            history=[Address(city="Plovdiv", zip="4000")],
        )
        written = {"profile": profile.model_dump(mode="json")}

        loaded = unflatten(flatten(written))

        assert loaded == written
        assert Profile.model_validate(loaded["profile"]) == profile


class TestSystemFieldsAreApart:
    def test_an_attribute_cannot_take_the_name_of_a_system_field(self) -> None:
        """Test 5.  The prefix is what keeps the two apart."""
        fields = flatten({"s:sub": "evil", "created": 1, "s:created": 2, "seen": 3})
        assert set(fields) == {"a:s:sub", "a:created", "a:s:created", "a:seen"}
        for system in (FIELD_SUBJECT, FIELD_CREATED, FIELD_SEEN, FIELD_DESCRIPTOR):
            assert system not in fields

    def test_unflatten_ignores_the_system_fields(self) -> None:
        fields = {
            **flatten({"s:sub": "evil", "role": "admin"}),
            FIELD_SUBJECT: '"42"',
            FIELD_CREATED: "1.5",
            FIELD_SEEN: "2.5",
            FIELD_DESCRIPTOR: '{"ua": "x"}',
        }
        assert unflatten(fields) == {"s:sub": "evil", "role": "admin"}


class TestFieldsNoSingleSaveWrote:
    """Two requests can save at once, and a field can expire on its own."""

    def test_a_gap_in_a_list_closes(self) -> None:
        """A list item whose TTL ended leaves ``[0]`` and ``[2]``."""
        assert unflatten({"a:x[0]": "1", "a:x[2]": "3"}) == {"x": [1, 3]}

    def test_list_items_come_back_in_index_order(self) -> None:
        fields = {f"a:x[{i}]": str(i) for i in (10, 2, 0, 1)}
        assert unflatten(fields) == {"x": [0, 1, 2, 10]}

    def test_a_container_wins_over_a_leaf_at_the_same_path(self) -> None:
        assert unflatten({"a:x": "1", "a:x.y": "2"}) == {"x": {"y": 2}}

    @pytest.mark.parametrize(
        "fields",
        [{"a:x": "{not json"}, {"a:x[oops]": "1"}, {"a:x[0": "1"}],
    )
    def test_a_field_that_cannot_be_read_raises(self, fields) -> None:
        """Not an empty session: the caller must see a failed read."""
        with pytest.raises(SessionStoreError, match="Unreadable session field"):
            unflatten(fields)


class TestGeo:
    def test_a_geo_attribute_is_one_leaf_in_the_form_the_engine_reads(self) -> None:
        fields = flatten({"location": [23.32, 42.69]}, geo=["location"])
        assert fields == {"a:location": "23.32,42.69"}

    def test_it_loads_as_lon_lat(self) -> None:
        fields = flatten({"location": [23.32, 42.69]}, geo=["location"])
        assert unflatten(fields, geo=["location"]) == {"location": [23.32, 42.69]}

    def test_a_nested_geo_path(self) -> None:
        value = {"home": {"at": [23, 42.5]}}
        fields = flatten(value, geo=["home.at"])
        assert fields == {"a:home.at": "23,42.5"}
        assert unflatten(fields, geo=["home.at"]) == value

    def test_a_list_that_is_not_declared_stays_a_list(self) -> None:
        assert flatten({"location": [23.32, 42.69]}) == {
            "a:location[0]": "23.32",
            "a:location[1]": "42.69",
        }

    def test_an_empty_or_null_geo_value_still_loads(self) -> None:
        """The type check refuses these on a save.  A load must not fail on
        one that is already stored."""
        for value in ([], None):
            fields = flatten({"location": value}, geo=["location"])
            assert unflatten(fields, geo=["location"]) == {"location": value}


class TestDiff:
    def test_a_changed_attribute_writes_that_field_only(self) -> None:
        """Test 6."""
        before = flatten({"a": 1, "b": {"c": 2, "d": 3}})
        after = flatten({"a": 1, "b": {"c": 2, "d": 4}})
        assert diff(before, after) == ({"a:b.d": "4"}, [])

    def test_a_shorter_list_removes_the_fields_of_the_removed_items(self) -> None:
        """Test 6."""
        before = flatten({"bikes": ["road", "gravel", "city"]})
        after = flatten({"bikes": ["road"]})
        assert diff(before, after) == ({}, ["a:bikes[1]", "a:bikes[2]"])

    def test_a_list_that_becomes_empty_swaps_its_fields_for_one_leaf(self) -> None:
        before = flatten({"bikes": ["road"]})
        after = flatten({"bikes": []})
        assert diff(before, after) == ({"a:bikes": "[]"}, ["a:bikes[0]"])

    def test_no_change_writes_nothing(self) -> None:
        """``HSET`` removes the TTL of a field, so an equal value must not be
        written."""
        fields = flatten(EVERY_TYPE)
        assert diff(fields, dict(fields)) == ({}, [])

    def test_a_new_and_a_removed_attribute(self) -> None:
        assert diff(flatten({"old": 1}), flatten({"new": 2})) == (
            {"a:new": "2"},
            ["a:old"],
        )

    def test_a_changed_type_is_a_change(self) -> None:
        assert diff(flatten({"n": 7}), flatten({"n": "7"})) == ({"a:n": '"7"'}, [])


class TestFieldsOfAnAttribute:
    FIELDS = flatten(
        {"cart": {"total": 5, "items": [1, 2]}, "cart_id": 9, "a.b": {"c": 1}}
    )

    def test_each_field_of_a_nested_attribute(self) -> None:
        assert fields_of(self.FIELDS, "cart") == [
            "a:cart.total",
            "a:cart.items[0]",
            "a:cart.items[1]",
        ]

    def test_a_name_that_is_a_prefix_of_another_is_not_matched(self) -> None:
        assert fields_of(self.FIELDS, "cart_id") == ["a:cart_id"]

    def test_a_name_with_a_path_character(self) -> None:
        assert fields_of(self.FIELDS, "a.b") == ["a:a%2Eb.c"]
        assert fields_of(self.FIELDS, "a") == []

    def test_a_system_field_belongs_to_no_attribute(self) -> None:
        assert fields_of([FIELD_SUBJECT, FIELD_CREATED, "a:x"], "s") == []
        assert attribute_of(FIELD_CREATED) is None

    @pytest.mark.parametrize(
        ("field", "name"),
        [
            ("a:role", "role"),
            ("a:cart.total", "cart"),
            ("a:bikes[0].kind", "bikes"),
            ("a:a%2Eb.c", "a.b"),
        ],
    )
    def test_the_attribute_of_a_field(self, field: str, name: str) -> None:
        assert attribute_of(field) == name

    def test_a_name_that_is_not_a_string(self) -> None:
        assert fields_of(flatten({1: "x"}), 1) == ["a:1"]


class TestSearchableDeclaration:
    def test_types_are_lowered(self) -> None:
        assert check_searchable({"a": "TAG", "b": "Numeric"}) == {
            "a": "tag",
            "b": "numeric",
        }

    @pytest.mark.parametrize(
        "declaration", [{"a": "vector"}, {"a": None}, {"": "tag"}, {1: "tag"}]
    )
    def test_an_unknown_type_or_a_bad_path_is_refused(self, declaration) -> None:
        with pytest.raises(SessionConfigurationError, match="must be one of"):
            check_searchable(declaration)


SEARCHABLE = {
    "device": "tag",
    "cart.total": "numeric",
    "name": "text",
    "location": "geo",
}


class TestTypeRules:
    """Test 32: each rule of design section 4."""

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param({"cart": {"total": "12"}}, id="numeric-string"),
            pytest.param({"cart": {"total": True}}, id="numeric-bool"),
            pytest.param({"cart": {"total": None}}, id="numeric-none"),
            pytest.param({"device": 7}, id="tag-int"),
            pytest.param({"device": 1.5}, id="tag-float"),
            pytest.param({"device": ["a"]}, id="tag-list"),
            pytest.param({"device": None}, id="tag-none"),
            pytest.param({"name": 7}, id="text-int"),
            pytest.param({"name": True}, id="text-bool"),
            pytest.param({"name": None}, id="text-none"),
            pytest.param({"location": [23.3]}, id="geo-one-number"),
            pytest.param({"location": [1, 2, 3]}, id="geo-three-numbers"),
            pytest.param({"location": ["23.3", "42.7"]}, id="geo-strings"),
            pytest.param({"location": [True, False]}, id="geo-bools"),
            pytest.param({"location": "23.3,42.7"}, id="geo-string"),
            pytest.param({"location": {"lon": 1, "lat": 2}}, id="geo-dict"),
            pytest.param({"location": None}, id="geo-none"),
        ],
    )
    def test_a_value_of_another_type_is_refused(self, data) -> None:
        with pytest.raises(SessionAttributeTypeError, match="is declared as"):
            check_types(data, SEARCHABLE)

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param({}, id="all-absent"),
            pytest.param({"other": 1}, id="another-attribute"),
            pytest.param({"cart": {}}, id="parent-without-the-leaf"),
            pytest.param({"cart": "not a dict"}, id="parent-is-a-leaf"),
            pytest.param({"device": True}, id="tag-bool"),
            pytest.param({"device": "d-91"}, id="tag-string"),
            pytest.param({"cart": {"total": 5}}, id="numeric-int"),
            pytest.param({"cart": {"total": 5.5}}, id="numeric-float"),
            pytest.param({"name": "Ana"}, id="text-string"),
            pytest.param({"location": [23.3, 42]}, id="geo-list"),
            pytest.param({"location": (23.3, 42)}, id="geo-tuple"),
        ],
    )
    def test_a_fitting_value_or_an_absent_attribute_raises_nothing(self, data) -> None:
        check_types(data, SEARCHABLE)

    def test_the_error_is_a_session_error_and_a_type_error(self) -> None:
        from redis_fastapi.exceptions import SessionError

        with pytest.raises(TypeError):
            check_types({"device": 7}, SEARCHABLE)
        with pytest.raises(SessionError):
            check_types({"device": 7}, SEARCHABLE)

    def test_a_path_through_a_list(self) -> None:
        searchable = {"rows[1].n": "numeric"}
        check_types({"rows": [{"n": "skipped"}]}, searchable)
        check_types({"rows": [{}, {"n": 2}]}, searchable)
        check_types({"rows": {"n": 2}}, searchable)
        with pytest.raises(SessionAttributeTypeError):
            check_types({"rows": [{}, {"n": "2"}]}, searchable)

    def test_a_key_that_is_not_a_string_is_found(self) -> None:
        with pytest.raises(SessionAttributeTypeError):
            check_types({1: "x"}, {"1": "numeric"})


class TestTtlRules:
    """Test 33b: an attribute with an indexed field cannot expire in parts."""

    def test_a_searchable_attribute_cannot_have_a_ttl(self) -> None:
        with pytest.raises(SessionConfigurationError, match="cannot have a TTL"):
            check_ttls(["device"], SEARCHABLE)

    def test_nor_can_an_attribute_that_holds_a_searchable_path(self) -> None:
        with pytest.raises(SessionConfigurationError, match="'cart.total'"):
            check_ttls(["cart"], SEARCHABLE)

    def test_another_attribute_can(self) -> None:
        check_ttls(["flash", "cart_total", "car"], SEARCHABLE)
        check_ttls([], SEARCHABLE)

    def test_a_name_with_a_path_character_is_compared_whole(self) -> None:
        check_ttls(["a"], {"a%2Eb": "tag"})
        with pytest.raises(SessionConfigurationError):
            check_ttls(["a.b"], {"a%2Eb": "tag"})


class TestFiniteNumbers:
    @pytest.mark.parametrize("value", [0, -3, 2.5])
    def test_a_number_is_finite(self, value) -> None:
        assert is_finite(value) is True

    @pytest.mark.parametrize(
        "value", [float("nan"), float("inf"), float("-inf"), True, "1", None]
    )
    def test_anything_else_is_not(self, value) -> None:
        assert is_finite(value) is False


class TestSeal:
    def test_with_no_encryptor_the_text_is_unchanged(self) -> None:
        assert seal('"admin"', None) == '"admin"'
        assert unseal('"admin"', None) == '"admin"'

    def test_a_sealed_value_is_base64_and_not_the_plaintext(self) -> None:
        """Test 7.  Ciphertext is bytes; a hash value goes to the driver as
        text."""
        sealed = seal('"admin"', Xor())
        assert "admin" not in sealed
        assert base64.b64decode(sealed, validate=True) == Xor().encrypt(b'"admin"')
        assert sealed.isascii()

    def test_it_round_trips_text_outside_ascii(self) -> None:
        assert unseal(seal('"Петрова"', Xor()), Xor()) == '"Петрова"'

    @pytest.mark.parametrize("raw", ["not base64!", '"admin"', "7"])
    def test_a_value_that_is_not_ciphertext_raises(self, raw: str) -> None:
        with pytest.raises(SessionStoreError, match="Unreadable session record"):
            unseal(raw, Xor())

    def test_a_value_that_does_not_decrypt_raises(self) -> None:
        class Refuses(Xor):
            def decrypt(self, data: bytes) -> bytes:
                raise ValueError("bad tag")

        with pytest.raises(SessionStoreError, match="bad tag"):
            unseal(seal("1", Xor()), Refuses())

    def test_each_field_seals_on_its_own(self) -> None:
        """Test 7: the same fields with and without an encryptor."""
        plain = flatten(EVERY_TYPE)
        sealed = {name: seal(text, Xor()) for name, text in plain.items()}
        assert set(sealed) == set(plain)
        assert all(sealed[name] != plain[name] for name in plain)
        opened = {name: unseal(raw, Xor()) for name, raw in sealed.items()}
        assert unflatten(opened) == EVERY_TYPE
