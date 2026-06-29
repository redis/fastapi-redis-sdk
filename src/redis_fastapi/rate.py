"""Fluent rate definitions for fastapi-redis-sdk rate limiting.

A :class:`Rate` couples a request ``limit`` with a time ``window`` (in
seconds).  Rates can be written in a compact string form
(``"100/minute"``, ``"5/second"``, ``"10/15seconds"``) and parsed via
:func:`parse_rate`, which also accepts an existing :class:`Rate` or a plain
``(limit, window)`` tuple.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Canonical seconds-per-unit.  Both the full and short spellings map here.
_UNIT_SECONDS: dict[str, int] = {
    "second": 1,
    "sec": 1,
    "s": 1,
    "minute": 60,
    "min": 60,
    "m": 60,
    "hour": 3600,
    "hr": 3600,
    "h": 3600,
    "day": 86_400,
    "d": 86_400,
}

# Preferred singular unit name for each window length, used by ``Rate.__str__``.
_SECONDS_UNIT: dict[int, str] = {1: "second", 60: "minute", 3600: "hour", 86_400: "day"}

# "<limit> [per|/] [<multiple>] <unit>" e.g. "100/minute", "10/15seconds",
# "100 per 2 minutes".
_RATE_RE = re.compile(
    r"^\s*(?P<limit>\d+)\s*(?:/|per)\s*(?P<multiple>\d+)?\s*(?P<unit>[a-zA-Z]+?)s?\s*$"
)


@dataclass(frozen=True)
class Rate:
    """A request ``limit`` allowed within a ``window`` of seconds."""

    limit: int
    window: int

    def __post_init__(self) -> None:
        if self.limit <= 0:
            raise ValueError(f"rate limit must be positive, got {self.limit}")
        if self.window <= 0:
            raise ValueError(f"rate window must be positive, got {self.window}")

    @classmethod
    def per_second(cls, limit: int) -> Rate:
        return cls(limit, 1)

    @classmethod
    def per_minute(cls, limit: int) -> Rate:
        return cls(limit, 60)

    @classmethod
    def per_hour(cls, limit: int) -> Rate:
        return cls(limit, 3600)

    @classmethod
    def per_day(cls, limit: int) -> Rate:
        return cls(limit, 86_400)

    def __str__(self) -> str:
        """Round-trip to the canonical spec, e.g. ``"100/minute"``.

        Windows that are an exact multiple of a known unit render as
        ``"<limit>/<multiple><unit>s"`` (``"10/15seconds"``); a window that
        matches a unit exactly drops the multiple (``"100/minute"``); anything
        else falls back to seconds (``"5/30seconds"``).
        """
        unit = _SECONDS_UNIT.get(self.window)
        if unit is not None:
            return f"{self.limit}/{unit}"
        return f"{self.limit}/{self.window}seconds"


# Accepted inputs anywhere a rate is specified.
RateLike = "str | Rate | tuple[int, int]"


def parse_rate(spec: str | Rate | tuple[int, int]) -> Rate:
    """Coerce *spec* into a :class:`Rate`.

    Accepts:

    * a :class:`Rate` (returned as-is),
    * a ``(limit, window_seconds)`` tuple,
    * a string such as ``"100/minute"``, ``"5/second"``, ``"1000/hour"``,
      ``"10/15seconds"`` or ``"100 per 2 minutes"``.

    Raises:
        ValueError: If the string cannot be parsed or the unit is unknown.
    """
    if isinstance(spec, Rate):
        return spec
    if isinstance(spec, tuple):
        limit, window = spec
        return Rate(int(limit), int(window))
    if isinstance(spec, str):
        match = _RATE_RE.match(spec)
        if match is None:
            raise ValueError(
                f"invalid rate spec {spec!r}; expected e.g. '100/minute' or "
                "'10/15seconds'"
            )
        unit = match.group("unit").lower()
        if unit not in _UNIT_SECONDS:
            raise ValueError(
                f"unknown time unit {unit!r} in rate spec {spec!r}; "
                f"expected one of second/minute/hour/day"
            )
        multiple = int(match.group("multiple") or 1)
        window = _UNIT_SECONDS[unit] * multiple
        return Rate(int(match.group("limit")), window)
    raise TypeError(f"cannot parse rate from {type(spec).__name__}: {spec!r}")
