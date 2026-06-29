"""Unit tests for the fluent rate definition (rate.py)."""

import pytest

from redis_fastapi.rate import Rate, parse_rate


@pytest.mark.unit
class TestParseRate:
    @pytest.mark.parametrize(
        ("spec", "limit", "window"),
        [
            ("100/minute", 100, 60),
            ("5/second", 5, 1),
            ("1000/hour", 1000, 3600),
            ("10/day", 10, 86_400),
            ("10/15seconds", 10, 15),
            ("100 per 2 minutes", 100, 120),
            ("50/hr", 50, 3600),
            ("10/s", 10, 1),
        ],
    )
    def test_parse_strings(self, spec: str, limit: int, window: int) -> None:
        rate = parse_rate(spec)
        assert rate.limit == limit
        assert rate.window == window

    @pytest.mark.parametrize(
        ("spec", "limit", "window"),
        [
            # leading / trailing whitespace is stripped
            ("  100/minute  ", 100, 60),
            ("\t5/second\n", 5, 1),
            # whitespace around the "/" separator
            ("100 / minute", 100, 60),
            ("100/ minute", 100, 60),
            ("100 /minute", 100, 60),
            ("100\t/\tminute", 100, 60),
            # whitespace around (and collapsing of) the "per" separator
            ("100  per  minute", 100, 60),
            ("100per minute", 100, 60),
            # whitespace mixed with a multiple
            ("10 / 15 seconds", 10, 15),
            ("100/ 2 minutes", 100, 120),
            ("  10/2hours  ", 10, 7200),
        ],
    )
    def test_parse_whitespace_variations(
        self, spec: str, limit: int, window: int
    ) -> None:
        rate = parse_rate(spec)
        assert rate.limit == limit
        assert rate.window == window

    @pytest.mark.parametrize(
        ("spec", "limit", "window"),
        [
            ("100/MINUTE", 100, 60),
            ("5/Second", 5, 1),
            ("50/HR", 50, 3600),
            ("10/S", 10, 1),
            ("1000/Hours", 1000, 3600),
        ],
    )
    def test_parse_case_insensitive(self, spec: str, limit: int, window: int) -> None:
        rate = parse_rate(spec)
        assert rate.limit == limit
        assert rate.window == window

    @pytest.mark.parametrize(
        ("spec", "limit", "window"),
        [
            # plural spellings (trailing "s" is optional in the grammar)
            ("100/minutes", 100, 60),
            ("5/seconds", 5, 1),
            ("1000/hours", 1000, 3600),
            ("10/days", 10, 86_400),
            ("50/hrs", 50, 3600),
            # short / abbreviated unit spellings
            ("10/m", 10, 60),
            ("10/h", 10, 3600),
            ("10/d", 10, 86_400),
            ("10/sec", 10, 1),
            ("10/min", 10, 60),
        ],
    )
    def test_parse_plural_and_short_units(
        self, spec: str, limit: int, window: int
    ) -> None:
        rate = parse_rate(spec)
        assert rate.limit == limit
        assert rate.window == window

    @pytest.mark.parametrize(
        ("spec", "limit", "window"),
        [
            ("100/2minutes", 100, 120),
            ("1/2hours", 1, 7200),
            ("3 per 5 days", 3, 432_000),
            ("100per2minutes", 100, 120),
        ],
    )
    def test_parse_with_multiple(self, spec: str, limit: int, window: int) -> None:
        rate = parse_rate(spec)
        assert rate.limit == limit
        assert rate.window == window

    def test_parse_tuple(self) -> None:
        assert parse_rate((7, 30)) == Rate(7, 30)

    def test_parse_rate_passthrough(self) -> None:
        rate = Rate(3, 5)
        assert parse_rate(rate) is rate

    @pytest.mark.parametrize(
        "bad",
        [
            "abc",
            "/minute",
            "100/",
            "",
            "   ",  # whitespace only
            "-5/minute",  # sign is not part of the \d+ grammar
            "1.5/minute",  # floats are not accepted
            "100//minute",  # doubled separator
            "100 minute",  # missing "/" or "per" separator
            "per minute",  # missing limit
            "100 per minute extra",  # trailing garbage
        ],
    )
    def test_invalid_strings_raise(self, bad: str) -> None:
        with pytest.raises(ValueError):
            parse_rate(bad)

    @pytest.mark.parametrize("bad", ["100/fortnight", "100/weeks", "100/parsec"])
    def test_unknown_unit_raises(self, bad: str) -> None:
        # Parses as a rate spec, but the unit is not a known time unit.
        with pytest.raises(ValueError, match="unknown time unit"):
            parse_rate(bad)

    @pytest.mark.parametrize("bad", ["0/minute", "5/0seconds", "0 per 2 hours"])
    def test_string_with_nonpositive_value_raises(self, bad: str) -> None:
        # Matches the grammar but resolves to a non-positive limit/window,
        # rejected by Rate.__post_init__.
        with pytest.raises(ValueError, match="must be positive"):
            parse_rate(bad)

    def test_invalid_type_raises(self) -> None:
        with pytest.raises(TypeError):
            parse_rate(3.5)  # type: ignore[arg-type]


@pytest.mark.unit
class TestRate:
    def test_constructors(self) -> None:
        assert Rate.per_second(1) == Rate(1, 1)
        assert Rate.per_minute(2) == Rate(2, 60)
        assert Rate.per_hour(3) == Rate(3, 3600)
        assert Rate.per_day(4) == Rate(4, 86_400)

    @pytest.mark.parametrize(
        ("rate", "expected"),
        [
            (Rate(100, 60), "100/minute"),
            (Rate(5, 1), "5/second"),
            (Rate(1, 3600), "1/hour"),
            (Rate(1, 86_400), "1/day"),
            (Rate(10, 15), "10/15seconds"),
            # windows with no named unit fall back to a seconds count
            (Rate(5, 30), "5/30seconds"),
            (Rate(2, 7200), "2/7200seconds"),
        ],
    )
    def test_str_roundtrip(self, rate: Rate, expected: str) -> None:
        assert str(rate) == expected
        # canonical strings re-parse to the same rate
        assert parse_rate(str(rate)) == rate

    @pytest.mark.parametrize(("limit", "window"), [(0, 60), (-1, 60), (5, 0), (5, -2)])
    def test_non_positive_rejected(self, limit: int, window: int) -> None:
        with pytest.raises(ValueError):
            Rate(limit, window)
