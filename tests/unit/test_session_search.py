"""Tests for the search code that has no Redis in it.

``session_search`` builds each query and reads each reply.  These tests give
it input and check the output, so the unit job covers the search code on
each system and Python version.  Design section 4.1, tests 38 to 40.
"""

from __future__ import annotations

import pytest
from redis.commands.search.result import Result
from redis.exceptions import ResponseError

from redis_fastapi.session_search import (
    BUILTIN,
    CUSTOM,
    LIST_LIMIT,
    LIVE_FILTER,
    PAGE_LIMIT,
    REVOKE_BATCH,
    TAG_SEPARATOR,
    Filter,
    SearchReply,
    builtin_fields,
    count_query,
    custom_fields,
    custom_index_name,
    definition,
    error_kind,
    escape_field,
    ids_query,
    page_query,
    parse_reply,
    query_string,
    tag_clause,
)

LIVE = r"@s\:created:[-inf +inf]"
BY_SUBJECT = Filter(BUILTIN, "s:sub", "tag", "42")


def _schema(fields) -> list[list[str]]:
    return [[str(arg) for arg in field.redis_args()] for field in fields]


class TestLimits:
    def test_the_constants_of_the_design(self) -> None:
        """Section 5.5: constants of the store, not settings."""
        assert LIST_LIMIT == 1000
        assert PAGE_LIMIT == 10_000
        assert REVOKE_BATCH == 1000


class TestFieldNames:
    @pytest.mark.parametrize(
        ("name", "escaped"),
        [
            ("s:sub", r"s\:sub"),
            ("a:cart.total", r"a\:cart\.total"),
            ("a:cart_total", r"a\:cart_total"),
            ("a:bikes[0].kind", r"a\:bikes\[0\]\.kind"),
            ("a:a%2Eb", r"a\:a\%2Eb"),
            ("a:first name", r"a\:first\ name"),
            ("a:x-y", r"a\:x\-y"),
            ("a:имя", "a\\:\\и\\м\\я"),
            ("abc_123", "abc_123"),
        ],
    )
    def test_each_character_but_a_letter_a_digit_or_underscore_is_escaped(
        self, name: str, escaped: str
    ) -> None:
        assert escape_field(name) == escaped

    def test_two_declared_paths_never_share_a_name(self) -> None:
        """``cart.total`` and ``cart_total`` can both be searchable."""
        assert escape_field("a:cart.total") != escape_field("a:cart_total")


class TestQueryStrings:
    """Test 38: one clause for the filter, and the live filter after it."""

    def test_the_live_filter(self) -> None:
        assert LIVE_FILTER == LIVE

    @pytest.mark.parametrize(
        ("flt", "query"),
        [
            pytest.param(
                BY_SUBJECT,
                r'@s\:sub:{"\"42\""} ' + LIVE,
                id="tag-subject",
            ),
            pytest.param(
                Filter(CUSTOM, "a:device_id", "tag", "d-91"),
                r'@a\:device_id:{"\"d-91\""} ' + LIVE,
                id="tag-attribute",
            ),
            pytest.param(
                Filter(CUSTOM, "a:beta", "tag", True),
                r'@a\:beta:{"true"} ' + LIVE,
                id="tag-bool",
            ),
            pytest.param(
                Filter(CUSTOM, "a:cart.total", "numeric", (100, 250.5)),
                r"@a\:cart\.total:[100 250.5] " + LIVE,
                id="numeric-both-limits",
            ),
            pytest.param(
                Filter(CUSTOM, "a:cart.total", "numeric", (100, None)),
                r"@a\:cart\.total:[100 +inf] " + LIVE,
                id="numeric-no-upper-limit",
            ),
            pytest.param(
                Filter(BUILTIN, "s:seen", "numeric", (None, 1700000000.5)),
                r"@s\:seen:[-inf 1700000000.5] " + LIVE,
                id="numeric-no-lower-limit",
            ),
            pytest.param(
                Filter(CUSTOM, "a:name", "text", "ana petrova"),
                r"@a\:name:(ana petrova) " + LIVE,
                id="text",
            ),
            pytest.param(
                Filter(CUSTOM, "a:location", "geo", (23.32, 42.69, 5000)),
                r"@a\:location:[23.32 42.69 5000 m] " + LIVE,
                id="geo",
            ),
        ],
    )
    def test_the_query_of_each_filter_type(self, flt: Filter, query: str) -> None:
        assert query_string(flt) == query

    @pytest.mark.parametrize("text", ["pet*", "%petrva%", "petrova|picard", "-petrova"])
    def test_text_is_passed_unchanged(self, text: str) -> None:
        """Section 4.2: the text is Redis query syntax.  The store does not
        escape, split or limit it."""
        query = query_string(Filter(CUSTOM, "a:name", "text", text))
        assert query == rf"@a\:name:({text}) " + LIVE

    def test_the_live_filter_stays_outside_the_parenthesis(self) -> None:
        """Text that closes the parenthesis cannot remove the live filter
        from the end of the query."""
        query = query_string(Filter(CUSTOM, "a:name", "text", "x) | (y"))
        assert query.endswith(") " + LIVE)


class TestTagValues:
    """Test 39: a TAG value is the JSON text of the value, in quotes, with
    ``\\`` and ``"`` escaped."""

    @pytest.mark.parametrize(
        ("value", "clause"),
        [
            pytest.param("admin", r'{"\"admin\""}', id="plain"),
            pytest.param('say "hi"', r'{"\"say \\\"hi\\\"\""}', id="quote"),
            pytest.param("a\\b", r'{"\"a\\\\b\""}', id="backslash"),
            pytest.param("a,b", r'{"\"a,b\""}', id="comma"),
            pytest.param("{a}", r'{"\"{a}\""}', id="braces"),
            pytest.param("a|b", r'{"\"a|b\""}', id="pipe"),
            pytest.param("Петрова", '{"\\"Петрова\\""}', id="non-ascii"),
            pytest.param("a b", r'{"\"a b\""}', id="space"),
            pytest.param("", r'{"\"\""}', id="empty"),
            pytest.param(True, '{"true"}', id="true"),
            pytest.param(False, '{"false"}', id="false"),
        ],
    )
    def test_the_clause_of_a_value(self, value, clause: str) -> None:
        assert tag_clause(value) == clause

    @pytest.mark.parametrize(
        "value", ['say "hi"', "a\\b", 'a\\"b', "a,b", "{a}", "a|b", "Петрова", '"}']
    )
    def test_the_clause_names_the_stored_text_and_nothing_else(self, value) -> None:
        """Undo the two escapes and the stored JSON text is back, whole.

        So no character of the value can close the quotes or the braces.
        """
        import json
        import re

        clause = tag_clause(value)
        assert clause.startswith('{"')
        assert clause.endswith('"}')
        inner = clause[2:-2]
        assert not re.search(r'(?<!\\)(?:\\\\)*"', inner), "an unescaped quote"
        stored = re.sub(r"\\(.)", r"\1", inner)
        assert stored == json.dumps(value, ensure_ascii=False)

    def test_a_string_and_a_bool_differ(self) -> None:
        assert tag_clause("true") != tag_clause(True)

    def test_the_separator_cannot_be_in_a_stored_value(self) -> None:
        """JSON text escapes each control character, so the engine never
        splits one value into two tags."""
        import json

        assert TAG_SEPARATOR == "\x1f"
        assert TAG_SEPARATOR not in json.dumps("a\x1fb", ensure_ascii=False)
        assert TAG_SEPARATOR not in tag_clause("a\x1fb")


class TestQueryObjects:
    def test_a_page_is_sorted_returns_three_fields_and_has_a_limit(self) -> None:
        args = page_query(BY_SUBJECT, limit=25, offset=50).get_args()
        assert args[0] == query_string(BY_SUBJECT)
        assert _after(args, "SORTBY", 2) == ["s:seen", "DESC"]
        assert _after(args, "RETURN", 4) == [3, "s:created", "s:seen", "s:desc"]
        assert _after(args, "LIMIT", 2) == [50, 25]
        assert _after(args, "DIALECT", 1) == [2]
        assert "NOCONTENT" not in args

    def test_sortby_and_return_take_the_name_with_no_escape(self) -> None:
        args = page_query(BY_SUBJECT, limit=1, offset=0).get_args()
        assert all("\\" not in str(arg) for arg in args[1:])

    def test_a_count_returns_no_row_and_does_not_sort(self) -> None:
        args = count_query(BY_SUBJECT).get_args()
        assert args[0] == query_string(BY_SUBJECT)
        assert _after(args, "LIMIT", 2) == [0, 0]
        assert _after(args, "DIALECT", 1) == [2]
        assert "NOCONTENT" in args
        assert "SORTBY" not in args

    def test_a_revoke_batch_returns_keys_only_and_does_not_sort(self) -> None:
        args = ids_query(BY_SUBJECT, limit=REVOKE_BATCH).get_args()
        assert args[0] == query_string(BY_SUBJECT)
        assert _after(args, "LIMIT", 2) == [0, 1000]
        assert _after(args, "DIALECT", 1) == [2]
        assert "NOCONTENT" in args
        assert "SORTBY" not in args


def _after(args: list, word: str, count: int) -> list:
    at = args.index(word)
    return args[at + 1 : at + 1 + count]


class TestSchemas:
    def test_the_built_in_index_holds_system_fields_only(self) -> None:
        """No value from the application can remove a session from it."""
        assert _schema(builtin_fields()) == [
            ["s:sub", "TAG", "SEPARATOR", "\x1f", "CASESENSITIVE"],
            ["s:seen", "NUMERIC", "SORTABLE"],
            ["s:created", "NUMERIC"],
        ]

    def test_the_custom_index_holds_the_declared_attributes(self) -> None:
        schema = _schema(
            custom_fields(
                {
                    "name": "text",
                    "device_id": "tag",
                    "location": "geo",
                    "cart.total": "numeric",
                }
            )
        )
        assert schema[:2] == [
            ["s:seen", "NUMERIC", "SORTABLE"],
            ["s:created", "NUMERIC"],
        ]
        assert [line[:2] for line in schema[2:]] == [
            ["a:cart.total", "NUMERIC"],
            ["a:device_id", "TAG"],
            ["a:location", "GEO"],
            ["a:name", "TEXT"],
        ]
        tag = schema[3]
        assert tag[tag.index("SEPARATOR") + 1] == "\x1f"
        assert "CASESENSITIVE" in tag

    def test_no_field_has_an_alias(self) -> None:
        fields = builtin_fields() + custom_fields({"cart.total": "numeric"})
        for line in _schema(fields):
            assert "AS" not in line

    def test_the_custom_index_does_not_hold_the_subject(self) -> None:
        names = [line[0] for line in _schema(custom_fields({"role": "tag"}))]
        assert "s:sub" not in names

    def test_an_index_covers_the_hashes_under_the_session_prefix(self) -> None:
        args = [str(arg) for arg in definition("rf:s:").args]
        assert args[args.index("ON") + 1] == "HASH"
        assert args[args.index("PREFIX") + 1 : args.index("PREFIX") + 3] == [
            "1",
            "rf:s:",
        ]


class TestCustomIndexName:
    DECLARATION = {"device_id": "tag", "cart.total": "numeric"}

    def test_it_is_the_base_and_eight_hex_characters(self) -> None:
        name = custom_index_name("rf:sa:", self.DECLARATION)
        assert name.startswith("rf:sa:")
        suffix = name[len("rf:sa:") :]
        assert len(suffix) == 8
        assert set(suffix) <= set("0123456789abcdef")

    def test_two_stores_with_one_declaration_use_one_name(self) -> None:
        """Whatever the order of the declaration."""
        reordered = dict(reversed(list(self.DECLARATION.items())))
        assert list(reordered) != list(self.DECLARATION)
        assert custom_index_name("rf:sa:", reordered) == custom_index_name(
            "rf:sa:", self.DECLARATION
        )

    def test_the_name_does_not_change_between_releases(self) -> None:
        """A new name drops the index of each deployment and builds it again.

        The value below is the name of this declaration.  A change to how
        the name is computed must be a decision, not an accident.
        """
        assert custom_index_name("rf:sa:", {"device_id": "tag"}) == "rf:sa:c10d268b"

    @pytest.mark.parametrize(
        "changed",
        [
            pytest.param({"device_id": "text", "cart.total": "numeric"}, id="type"),
            pytest.param({"device": "tag", "cart.total": "numeric"}, id="path"),
            pytest.param({"device_id": "tag"}, id="one-fewer"),
            pytest.param(
                {"device_id": "tag", "cart.total": "numeric", "name": "text"},
                id="one-more",
            ),
        ],
    )
    def test_a_changed_declaration_gives_a_new_name(self, changed) -> None:
        assert custom_index_name("rf:sa:", changed) != custom_index_name(
            "rf:sa:", self.DECLARATION
        )


ROWS = [
    (
        "rf:s:first",
        {"s:created": "100.5", "s:seen": "200.5", "s:desc": '{"ua": "Firefox"}'},
    ),
    ("rf:s:second", {"s:created": "90.0", "s:seen": "150.0"}),
]


def _resp2(rows=ROWS, total: int = 7) -> Result:
    flat: list = [total]
    for key, fields in rows:
        flat.append(key.encode())
        flat.append([part.encode() for pair in fields.items() for part in pair])
    return Result(flat, True)


def _resp3(rows=ROWS, total: int = 7, warning=(), binary: bool = False) -> dict:
    def enc(text: str):
        return text.encode() if binary else text

    return {
        enc("attributes"): [],
        enc("format"): enc("STRING"),
        enc("total_results"): total,
        enc("results"): [
            {
                enc("id"): enc(key),
                enc("extra_attributes"): {enc(k): enc(v) for k, v in fields.items()},
                enc("values"): [],
            }
            for key, fields in rows
        ],
        enc("warning"): [enc(item) for item in warning],
    }


class TestParseReply:
    """Test 40: one answer, whatever the protocol gave."""

    def test_a_resp2_reply(self) -> None:
        assert parse_reply(_resp2()) == SearchReply(7, ROWS, False)

    def test_a_resp3_reply_gives_the_same_rows_and_total(self) -> None:
        assert parse_reply(_resp3()) == parse_reply(_resp2())

    def test_a_resp3_reply_from_a_client_that_does_not_decode(self) -> None:
        assert parse_reply(_resp3(binary=True)) == parse_reply(_resp2())

    def test_the_two_forms_of_a_count(self) -> None:
        resp2 = Result([12], False)
        resp3 = {"total_results": 12, "results": [], "warning": []}
        assert parse_reply(resp2) == SearchReply(12, [], False)
        assert parse_reply(resp3) == SearchReply(12, [], False)

    def test_the_two_forms_of_a_reply_with_no_content(self) -> None:
        """What a bulk revoke asks for: keys only."""
        resp2 = Result([2, b"rf:s:first", b"rf:s:second"], False)
        resp3 = {
            "total_results": 2,
            "results": [
                {"id": "rf:s:first", "values": []},
                {"id": "rf:s:second", "extra_attributes": None, "values": []},
            ],
            "warning": [],
        }
        expected = SearchReply(2, [("rf:s:first", {}), ("rf:s:second", {})], False)
        assert parse_reply(resp2) == expected
        assert parse_reply(resp3) == expected

    def test_an_empty_reply(self) -> None:
        assert parse_reply(Result([0], True)) == SearchReply(0, [], False)
        assert parse_reply({"total_results": 0, "results": []}) == SearchReply(
            0, [], False
        )

    @pytest.mark.parametrize(
        "warning",
        [
            ["Timeout limit was reached"],
            [b"Timeout limit was reached"],
            ["another warning", "Timeout limit was reached"],
            "Timeout limit was reached",
        ],
    )
    def test_a_resp3_timeout_warning_is_reported(self, warning) -> None:
        reply = {
            "total_results": 7,
            "results": _resp3()["results"],
            "warning": warning,
        }
        parsed = parse_reply(reply)
        assert parsed.timed_out is True
        assert parsed.rows == ROWS

    def test_a_binary_resp3_timeout_warning_is_reported(self) -> None:
        reply = _resp3(warning=["Timeout limit was reached"], binary=True)
        assert parse_reply(reply).timed_out is True

    @pytest.mark.parametrize("warning", [[], None, [""], ["Max prefix expansions"]])
    def test_another_warning_or_none_is_not_a_timeout(self, warning) -> None:
        reply = {"total_results": 0, "results": [], "warning": warning}
        assert parse_reply(reply).timed_out is False

    @pytest.mark.skipif(
        not hasattr(Result, "from_resp3"),
        reason="this redis-py returns a dict on RESP3, not a Result",
    )
    def test_a_timeout_warning_on_a_result_object_is_reported(self) -> None:
        """redis-py 8 talks RESP3 by default and still returns a ``Result``.

        It builds the object with ``Result.from_resp3`` and puts the
        ``warning`` entry of the server in ``Result.warnings``.  A timeout
        there is the same partial result as in the ``dict`` form.
        """
        reply = Result.from_resp3(_resp3(warning=["Timeout limit was reached"]))
        assert reply.warnings == ["Timeout limit was reached"]

        parsed = parse_reply(reply)

        assert parsed.rows == ROWS
        assert parsed.timed_out is True


class TestErrorKind:
    @pytest.mark.parametrize(
        ("message", "kind"),
        [
            ("Timeout limit was reached", "timeout"),
            ("Query timed out", "timeout"),
            ("Syntax error at offset 1 near (", "syntax"),
            ("Index already exists", "exists"),
            ("rf:si: no such index", "missing"),
            ("No such index rf:si", "missing"),
            ("Index not found", "missing"),
            ("Unknown Index name", "missing"),
            ("Unknown index name (or name is an alias itself)", "missing"),
            ("unknown command 'FT.SEARCH'", None),
            ("WRONGTYPE Operation against a key", None),
            ("", None),
        ],
    )
    def test_the_kind_of_a_server_error(self, message: str, kind) -> None:
        assert error_kind(ResponseError(message)) == kind

    def test_a_dropped_connection_is_not_a_search_timeout(self) -> None:
        """A search that timed out gave a partial answer.  A connection that
        failed gave none, and the caller may try again."""
        assert error_kind(ConnectionError("Connection reset by peer")) is None
