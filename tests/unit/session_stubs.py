"""Test doubles for the session store: a Query Engine, and a command log.

``fakeredis`` has no ``FT.*`` command.  :func:`install_query_engine` gives a
fake client the few calls the session store makes: ``ft(name).create_index``,
``.dropindex`` and ``.search``, ``FT._LIST``, and
``CONFIG GET search-on-timeout``.

The stub reads the hashes in ``fakeredis`` and answers the queries the store
builds: one TAG, NUMERIC, TEXT or GEO clause, and the live filter.  It is not a
query engine.  It shows that the store sends the right query to the right
index and reads the reply; the integration suite shows what a real server
does with that query.
"""

from __future__ import annotations

import math
import re
from collections.abc import Awaitable, Callable
from typing import Any

from redis.commands.search.result import Result
from redis.exceptions import ResponseError

# The forms ``ft().search`` returns a reply in.  redis-py gives a ``Result``
# object on RESP2 and a ``dict`` on RESP3.  From version 8 its default is
# RESP3 on the wire with the ``Result`` object of RESP2, which it builds with
# ``Result.from_resp3``.
RESP2 = "resp2"
RESP3 = "resp3"
RESP3_AS_RESULT = "resp3-as-result"
REPLY_FORMS = [RESP2, RESP3]
if hasattr(Result, "from_resp3"):
    REPLY_FORMS.append(RESP3_AS_RESULT)

# The clause the store adds to each query (design, section 4.1).
LIVE_FILTER = r"@s\:created:[-inf +inf]"

_CLAUSE = re.compile(r"^@(?P<field>(?:\\.|\w)+):(?P<body>.+)$", re.DOTALL)
_ESCAPED = re.compile(r"\\(.)", re.DOTALL)


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _bound(text: str) -> float:
    return float(text.replace("inf", "Infinity"))


def _meters(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """The distance between two places on the Earth (haversine)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    half_lat = (phi2 - phi1) / 2
    half_lon = math.radians(lon2 - lon1) / 2
    a = (
        math.sin(half_lat) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(half_lon) ** 2
    )
    return 2 * 6_371_000 * math.asin(math.sqrt(a))


def _matcher(clause: str) -> Callable[[dict[str, str]], bool]:
    """Turn one clause of a query into a test on the fields of a hash."""
    found = _CLAUSE.match(clause)
    if found is None:
        raise ResponseError(f"Syntax error at offset 0 near {clause[:10]}")
    name = _ESCAPED.sub(r"\1", found["field"])
    body = found["body"]

    if body.startswith('{"') and body.endswith('"}'):
        wanted = _ESCAPED.sub(r"\1", body[2:-2])
        return lambda fields: fields.get(name) == wanted

    if body.startswith("[") and body.endswith(" m]"):
        lon, lat, radius = (float(part) for part in body[1:-3].split(" "))

        def is_near(fields: dict[str, str]) -> bool:
            try:
                there = [float(part) for part in fields[name].split(",")]
            except (KeyError, ValueError):
                return False
            return len(there) == 2 and _meters(lon, lat, *there) <= radius

        return is_near

    if body.startswith("[") and body.endswith("]"):
        low, high = (_bound(part) for part in body[1:-1].split(" "))

        def in_range(fields: dict[str, str]) -> bool:
            try:
                return low <= float(fields[name]) <= high
            except (KeyError, ValueError):
                return False

        return in_range

    if body.startswith("(") and body.endswith(")"):
        text = body[1:-1]
        if not text or text.count("(") != text.count(")"):
            raise ResponseError(f"Syntax error at offset 1 near {text[:10]}")
        words = text.lower().split()

        def has_words(fields: dict[str, str]) -> bool:
            stored = re.findall(r"\w+", fields.get(name, "").lower())
            return all(word in stored for word in words)

        return has_words

    raise ResponseError(f"Syntax error at offset 0 near {body[:10]}")


class _Index:
    """What ``client.ft(name)`` returns."""

    def __init__(self, engine: StubQueryEngine, name: str) -> None:
        self._engine = engine
        self._name = name

    async def create_index(self, fields: Any, *, definition: Any = None) -> str:
        self._engine._called("FT.CREATE", self._name)
        if self._name in self._engine.indexes:
            raise ResponseError("Index already exists")
        args = list(getattr(definition, "args", []))
        prefix = args[args.index("PREFIX") + 2] if "PREFIX" in args else ""
        self._engine.indexes[self._name] = prefix
        self._engine.schemas[self._name] = [
            [_text(arg) for arg in field.redis_args()] for field in fields
        ]
        return "OK"

    async def dropindex(self, delete_documents: bool = False) -> str:
        self._engine._called("FT.DROPINDEX", self._name, delete_documents)
        if self._name not in self._engine.indexes:
            raise ResponseError("Unknown index name (or name is an alias itself)")
        del self._engine.indexes[self._name]
        del self._engine.schemas[self._name]
        return "OK"

    async def search(self, query: Any) -> Any:
        self._engine._called("FT.SEARCH", self._name, *query.get_args())
        if self._name not in self._engine.indexes:
            raise ResponseError(f"No such index {self._name}")
        return await self._engine._search(self._engine.indexes[self._name], query)


class StubQueryEngine:
    """Answers the search calls of one fake client.

    Attributes:
        indexes: Each index that exists, as ``{name: key prefix}``.
        schemas: The schema lines each index was created with.
        commands: Each search command the client received, in order, as a
            tuple that starts with the command name.
        errors: ``{command name: [error, ...]}``.  A call to that command
            raises the first error in its list, once.
        replies: Replies for the next searches, in order.  While the list
            has an entry, a search returns it and reads no hash.
        warning: The ``warning`` entry of each reply.  The RESP2 form has no
            such entry, so there it is lost, as on a real server.
        truncate: With a number, a reply holds at most that many rows, as a
            server that stopped at its timeout does.
        policy: The value of ``search-on-timeout``, or ``None`` for a server
            that refuses ``CONFIG GET``.
    """

    def __init__(self, redis: Any, *, form: str = RESP2) -> None:
        # A second client on the same data.  The reads of the stub then stay
        # out of the command log of the client under test.
        self._redis = type(redis)(connection_pool=redis.connection_pool)
        self._form = form
        self.indexes: dict[str, str] = {}
        self.schemas: dict[str, list[list[str]]] = {}
        self.commands: list[tuple[Any, ...]] = []
        self.errors: dict[str, list[BaseException]] = {}
        self.replies: list[Any] = []
        self.warning: list[str] = []
        self.truncate: int | None = None
        self.policy: str | None = "fail"

    def _called(self, *command: Any) -> None:
        self.commands.append(command)
        pending = self.errors.get(command[0])
        if pending:
            raise pending.pop(0)

    def sent(self, name: str) -> list[tuple[Any, ...]]:
        """The calls to one command."""
        return [command for command in self.commands if command[0] == name]

    def queries(self) -> list[str]:
        """The query string of each ``FT.SEARCH``."""
        return [command[2] for command in self.sent("FT.SEARCH")]

    def ft(self, name: str = "idx") -> _Index:
        return _Index(self, name)

    async def list_indexes(self) -> list[bytes]:
        self._called("FT._LIST")
        return [name.encode() for name in self.indexes]

    async def config_get(self, pattern: str = "*") -> dict[str, str]:
        self._called("CONFIG GET", pattern)
        if self.policy is None:
            raise ResponseError("unknown command 'CONFIG'")
        return {pattern: self.policy}

    async def _search(self, prefix: str, query: Any) -> Any:
        if self.replies:
            return self.replies.pop(0)
        text = query.query_string()
        live = text.endswith(" " + LIVE_FILTER)
        matches = _matcher(text[: -len(LIVE_FILTER) - 1] if live else text)

        found: list[tuple[str, dict[str, str]]] = []
        for raw_key in await self._redis.keys(prefix + "*"):
            key = _text(raw_key)
            stored = await self._redis.hgetall(key)
            fields = {_text(k): _text(v) for k, v in stored.items()}
            if live and "s:created" not in fields:
                continue
            if matches(fields):
                found.append((key, fields))

        total = len(found)
        sort = getattr(query, "_sortby", None)
        if sort is not None:
            name, direction = sort.args
            found.sort(
                key=lambda row: float(row[1].get(name, 0)),
                reverse=direction == "DESC",
            )
        page = found[query._offset : query._offset + query._num]
        if self.truncate is not None:
            page = page[: self.truncate]
        content = not query._no_content
        rows = [
            (key, {n: fields[n] for n in query._return_fields if n in fields})
            for key, fields in page
        ]

        if self._form != RESP2:
            reply = {
                "attributes": [],
                "format": "STRING",
                "total_results": total,
                "results": [
                    {"id": key, "values": [], "extra_attributes": fields}
                    if content
                    else {"id": key, "values": []}
                    for key, fields in rows
                ],
                "warning": list(self.warning),
            }
            return reply if self._form == RESP3 else Result.from_resp3(reply)
        flat: list[Any] = [total]
        for key, fields in rows:
            flat.append(key)
            if content:
                flat.append([part for pair in fields.items() for part in pair])
        return Result(flat, content)


def install_query_engine(redis: Any, *, form: str = RESP2) -> StubQueryEngine:
    """Give the fake client *redis* a Query Engine, and return the stub.

    *form* picks the reply form of a search, one of :data:`REPLY_FORMS`.
    """
    engine = StubQueryEngine(redis, form=form)
    execute: Callable[..., Awaitable[Any]] = redis.execute_command

    async def execute_command(*args: Any, **kwargs: Any) -> Any:
        if args and args[0] == "FT._LIST":
            return await engine.list_indexes()
        return await execute(*args, **kwargs)

    redis.ft = engine.ft
    redis.execute_command = execute_command
    redis.config_get = engine.config_get
    return engine


def record_commands(redis: Any) -> list[tuple[str, ...]]:
    """Log each command the fake client *redis* sends from now on.

    Returns the log: one tuple of text for each command, in order, from a
    pipeline or not.  A test reads it to show which commands a request sent,
    and which it did not.

    The log is live, and a read by the test goes into it too.  Copy it with
    ``list(sent)`` straight after the call under test.
    """
    sent: list[tuple[str, ...]] = []
    make_pipeline = redis.pipeline
    execute = redis.execute_command

    def pipeline(*args: Any, **kwargs: Any) -> Any:
        pipe = make_pipeline(*args, **kwargs)
        run = pipe.execute

        async def logged(*run_args: Any, **run_kwargs: Any) -> Any:
            sent.extend(
                tuple(_text(arg) for arg in command[0])
                for command in pipe.command_stack
            )
            return await run(*run_args, **run_kwargs)

        pipe.execute = logged
        return pipe

    async def execute_command(*args: Any, **kwargs: Any) -> Any:
        sent.append(tuple(_text(arg) for arg in args))
        return await execute(*args, **kwargs)

    redis.pipeline = pipeline
    redis.execute_command = execute_command
    return sent


def names(commands: list[tuple[str, ...]]) -> list[str]:
    """The name of each command in a log."""
    return [command[0].upper() for command in commands]
