"""Resumable streams: a Redis-backed event log a client can resume.

One Redis Stream holds one run - one agent turn, for example - and it is the
only key a run has::

    <prefix>:stream:<scope>:<id>

The entry ID is the cursor and the SSE ``id:``, so a client that reconnects
with ``Last-Event-ID`` gets exactly the entries after the last one it saw.
The run is decoupled from the HTTP request: a disconnect never stops it.

The scope is part of the key, so a stream is reachable only through the scope
the application authorized.  Everything about a run is an entry in its
stream: ``stream.start`` when it opens, the events, ``stream.ping`` while an
idle producer waits, and one final entry.  Readers stop at the final entry,
so anything written after it is never read.

The run's lifetime is set once, when it opens (``stream_max_run``), and
shortened to ``stream_done_ttl`` when it ends.  A reader that sees nothing for
``stream_silence_timeout`` ends the run as ``producer_lost``.

Entry fields: ``t`` (type), ``d`` (payload), ``q`` (writer sequence number) and
``f`` (``1`` on the final entry only).

The HTTP side - the SSE response, cursor selection and the exception
handlers - lives in :mod:`redis_fastapi.streams`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import secrets
import time
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypeAlias, TypeVar

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import NoPermissionError, RedisClusterException, RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import (
    StreamCursorAhead,
    StreamError,
    StreamExpired,
    StreamFinished,
    StreamInvalidCursor,
    StreamInvalidName,
    StreamNotFound,
    StreamStoreError,
    StreamUnsupported,
)
from redis_fastapi.telemetry import (
    add_stream_readers,
    record_stream_operation,
    stream_span,
    timed_stream,
)
from redis_fastapi.types import Coder, JsonableCoder

if TYPE_CHECKING:
    from starlette.requests import Request
    from starlette.responses import Response

logger = logging.getLogger(__name__)

AsyncClient: TypeAlias = AsyncRedis | AsyncRedisCluster

# Every driver failure the feature interprets.  ``RedisClusterException`` is
# not a ``RedisError``; see the same tuple in ``session_backend``.
STORE_ERRORS: tuple[type[BaseException], ...] = (
    RedisError,
    RedisClusterException,
    OSError,
)

# Failures after which Redis may or may not have applied a write.  Every write
# here is safe to repeat (IDMP, EXPIRE), so these are retried.
_AMBIGUOUS_ERRORS: tuple[type[BaseException], ...] = (
    RedisConnectionError,
    RedisTimeoutError,
    OSError,
)
_WRITE_ATTEMPTS = 3

# Shutdown: how long aclose() waits for spawned producers to write their final
# entries, and how often it repeats a cancel that did not arrive.
_SHUTDOWN_GRACE_SECONDS = 5.0
_SHUTDOWN_RETRY_SECONDS = 0.25

# What ending a stream from ``spawn()`` may fail with, logged and not raised.
_END_ERRORS: tuple[type[BaseException], ...] = (StreamError, *STORE_ERRORS)

_T = TypeVar("_T")

# Entry fields.
F_TYPE = "t"
F_DATA = "d"
F_SEQ = "q"
F_FINAL = "f"

# Types starting with this prefix belong to the SDK.
RESERVED_PREFIX = "stream."
START_TYPE = "stream.start"
PING_TYPE = "stream.ping"
END_TYPE = "stream.end"
GAP_TYPE = "stream.gap"
ERROR_TYPE = "stream.error"
# The SDK's own entries, never shown to a reader.
_HIDDEN_TYPES = frozenset({START_TYPE, PING_TYPE})

# The one IDMP pair every final entry shares, so a stream holds one final
# entry even when a writer, a cancel and a reader end it at once.
_END_PRODUCER = "end"
_END_ID = "final"

# No ":" in an ID, so "<scope>:<id>" splits in exactly one way and two pairs
# never share a key.  No braces, so no value can make a hash tag.
_SCOPE_RE = re.compile(r"[A-Za-z0-9_.:-]{1,200}")
_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")
# ASCII digits only, and at most 20 of them: 2**64 - 1 has 20 digits, and a
# longer run would only reach int() to be refused there.
_CURSOR_RE = re.compile(r"[0-9]{1,20}-[0-9]{1,20}")
_MAX_PART = 2**64 - 1
MAX_TYPE_LENGTH = 128

Cursor: TypeAlias = tuple[int, int]
_START: Cursor = (0, 0)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _text(value: Any) -> str | None:
    """A reply value as text, whatever ``decode_responses`` is set to."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def parse_cursor(value: str) -> Cursor:
    """Parse an entry ID into a pair of integers.

    Cursors compare as numbers: as strings ``"999-0"`` would sort after
    ``"1000-0"``.  The special IDs ``$``, ``+`` and ``-`` and a bare ``<ms>``
    are refused, as is a part above 2**64 - 1.

    Raises:
        StreamInvalidCursor: If *value* is not a valid entry ID.
    """
    if not _CURSOR_RE.fullmatch(value):
        raise StreamInvalidCursor(f"invalid cursor {value!r}")
    ms_text, seq_text = value.split("-")
    ms, seq = int(ms_text), int(seq_text)
    if ms > _MAX_PART or seq > _MAX_PART:
        raise StreamInvalidCursor(f"cursor out of range: {value!r}")
    return ms, seq


def _format(cursor: Cursor) -> str:
    return f"{cursor[0]}-{cursor[1]}"


def check_name(scope: str, stream_id: str | None = None) -> None:
    """Reject a scope or stream ID outside the allowed characters.

    Raises:
        StreamInvalidName: If *scope* or *stream_id* has characters outside
            the allowed set, or is too long.  It is a ``ValueError`` too.
    """
    if not isinstance(scope, str) or not _SCOPE_RE.fullmatch(scope):
        raise StreamInvalidName(
            f"invalid scope {scope!r}: use 1-200 of A-Z a-z 0-9 _ . : -"
        )
    if stream_id is not None and (
        not isinstance(stream_id, str) or not _ID_RE.fullmatch(stream_id)
    ):
        raise StreamInvalidName(
            f"invalid stream ID {stream_id!r}: use 1-64 of A-Z a-z 0-9 _ -"
        )


def check_event_type(event_type: str) -> None:
    """Reject a type that could break an SSE frame, or that the SDK reserves.

    The type goes into the SSE ``event:`` line as it is, so a line break would
    end the frame and could start a forged one with its own ``id:``.

    Raises:
        ValueError: If the type is empty, longer than 128 characters, contains
            CR, LF or NUL, or starts with ``stream.``.
    """
    if (
        not isinstance(event_type, str)
        or not event_type
        or len(event_type) > MAX_TYPE_LENGTH
        or any(c in event_type for c in "\r\n\0")
    ):
        raise ValueError(f"invalid event type {event_type!r}")
    if event_type.startswith(RESERVED_PREFIX):
        raise ValueError(
            f"invalid event type {event_type!r}: types starting with "
            f"{RESERVED_PREFIX!r} are reserved"
        )


def _key(scope: str, stream_id: str) -> str:
    """The run's one key."""
    return f"{get_settings().prefix}:stream:{scope}:{stream_id}"


def _xadd(
    key: str,
    type: str,
    payload: str,
    *,
    idmp: tuple[str, str] | None,
    seq: int | None = None,
    final: bool = False,
    create: bool = False,
) -> list[Any]:
    """The ``XADD`` for one entry, shared by every path that writes one.

    ``NOMKSTREAM`` unless *create*: only ``open()`` creates the stream, so a
    write never recreates one that expired.  No ``MAXLEN``: Redis never trims
    a run, so its final entry can never be trimmed away by writes that land
    after it.  The writer caps the entries instead (``stream_maxlen``).
    """
    args: list[Any] = ["XADD", key]
    if not create:
        args.append("NOMKSTREAM")
    if idmp is not None:
        args += ["IDMP", *idmp]
    args.append("*")
    args += [F_TYPE, type, F_DATA, payload]
    if seq is not None:
        args += [F_SEQ, str(seq)]
    if final:
        args += [F_FINAL, "1"]
    return args


def _queue_end(pipe: Any, key: str, type: str, payload: str, seq: int | None) -> None:
    """Queue the final entry and the TTL of a finished run.

    Every final entry shares one IDMP pair, so a late one - a writer finishing
    after a cancel - returns the existing entry instead of adding another.
    ``LT`` lets ending only shorten the run's lifetime: a second end never
    keeps a finished run around longer.
    """
    pipe.execute_command(
        *_xadd(key, type, payload, idmp=(_END_PRODUCER, _END_ID), seq=seq, final=True)
    )
    pipe.expire(key, get_settings().stream_done_ttl, lt=True)


# How long a stream remembers IDMP pairs, in seconds: the most Redis allows,
# which ``check_stream_settings`` keeps above any run's lifetime.  A constant,
# because XCFGSET with a changed value clears the IDMP history, while the same
# value - sent again by a retried open() - leaves it alone.
IDMP_WINDOW = 86_400


def _xread_entries(reply: Any) -> list[tuple[Any, dict[Any, Any]]]:
    """Normalize an ``XREAD`` reply for one stream, RESP2 or RESP3."""
    if not reply:
        return []
    if isinstance(reply, dict):  # RESP3: {name: [entries]}
        streams = list(reply.values())
        if not streams:
            return []
        first = streams[0]
        if first and isinstance(first[0], list):
            return list(first[0])
        return list(first)
    return list(reply[0][1])  # RESP2: [[name, entries]]


# ---------------------------------------------------------------------------
# Support detection
# ---------------------------------------------------------------------------


@dataclass
class _StreamCapabilities:
    """Process-lifetime cache of server support, shared on the pool state.

    ``None`` means "not yet known" and leaves detection to a later attempt
    rather than caching a guess, as the rate limiter and the session store do.
    """

    supported: bool | None = None


async def probe_streams_support(client: AsyncClient) -> bool | None:
    """Ask the server whether it has ``XCFGSET`` and ``DELEX``.

    ``XCFGSET`` arrived in Redis 8.6 together with ``XADD ... IDMP``, which
    this feature needs; ``DELEX`` arrived in 8.4.  Like
    :func:`~redis_fastapi.ratelimit_backend.probe_increx_support` this asks
    ``COMMAND INFO`` rather than sending the commands and reading an error: on
    a cluster, redis-py rejects an unknown command client-side.

    The probe cannot tell Redis 8.6.0 or 8.6.1, which have the commands but
    carry IDMP defects, from 8.6.2.  The feature requires 8.6.2.

    Returns:
        ``True`` when both commands exist, ``False`` when either is missing,
        and ``None`` when the question could not be answered.
    """
    try:
        if isinstance(client, AsyncRedisCluster):
            reply = await client.execute_command(
                "COMMAND",
                "INFO",
                "XCFGSET",
                "DELEX",
                target_nodes=AsyncRedisCluster.RANDOM,
            )
        else:
            reply = await client.execute_command(  # type: ignore[no-untyped-call]
                "COMMAND", "INFO", "XCFGSET", "DELEX"
            )
    except TypeError:
        # redis-py parses the COMMAND reply eagerly and indexes into a nil
        # entry for a command the server does not know: "unsupported".
        return False
    except STORE_ERRORS:
        logger.debug("Stream capability probe failed; will retry", exc_info=True)
        return None
    if isinstance(reply, dict):
        entries = list(reply.values())
    elif isinstance(reply, (list, tuple)):
        entries = list(reply)
    else:
        return False
    return len(entries) == 2 and all(entries)


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StreamEvent:
    """One event read from a stream.

    Attributes:
        id: The Redis entry ID, which is the cursor.  ``None`` for the
            reader's own events (``stream.gap``, ``stream.error``).
        type: The event type the writer chose, or a ``stream.`` type.
        data: The decoded payload.
        final: ``True`` on the stream's final entry.
    """

    id: str | None
    type: str
    data: Any
    final: bool = False


@dataclass(frozen=True)
class _Entry:
    """One event as the read loop produces it, with its payload still encoded.

    The SSE response sends *raw* as it is; :class:`StreamReader` decodes it
    into a :class:`StreamEvent`.  The reader's own events (``stream.gap``,
    ``stream.error``) have no ID, and carry their *data* already decoded.
    """

    id: str | None
    type: str
    raw: str | None
    final: bool = False
    data: Any = None

    @staticmethod
    def own(type: str, data: dict[str, Any]) -> _Entry:
        return _Entry(None, type, json.dumps(data), data=data)


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


class StreamWriter:
    """Appends to one stream.  Made by :meth:`StreamBackend.open`.

    Attributes:
        stream_id: The stream's ID within its scope.
        scope: The scope the stream belongs to.
        created: ``True`` when :meth:`~StreamBackend.open` created the run,
            ``False`` when it found the same run already started - a retried
            start request.  Start a producer only when this is ``True``.
    """

    def __init__(
        self,
        backend: StreamBackend,
        *,
        scope: str,
        stream_id: str,
        writer_id: str,
        created: bool,
    ) -> None:
        self._backend = backend
        self.scope = scope
        self.stream_id = stream_id
        self.created = created
        self._key = _key(scope, stream_id)
        self._writer_id = writer_id
        self._seq = 0
        # One append in flight per writer: a retried append then never has
        # other appends from this writer pass it, so Redis's IDMP history,
        # 100 pairs per writer by default, always still holds its pair.
        self._lock = asyncio.Lock()
        self._last_write = time.monotonic()
        self._final_id: str | None = None
        # Set when spawn() starts writing the final entry, so a repeated
        # cancel at shutdown never interrupts that write.
        self._ending = False
        # A handle for a run that already exists (a retried start) writes
        # nothing: the run's own producer does.
        self._read_only = not created
        self._task: asyncio.Task[None] | None = None

    @property
    def finished(self) -> bool:
        """Whether this writer has written the final entry."""
        return self._final_id is not None

    async def append(self, type: str, data: Any) -> str:
        """Append one event and return its entry ID.

        One ``XADD``.  A timeout or a dropped connection is retried with the
        same idempotency key, so a retried append never stores the event
        twice.  Appends from one writer go out one at a time, in order.  An
        append after a cancel from another process still lands, after the
        final entry, where no reader looks.

        Raises:
            ValueError: If *type* is invalid or reserved, the default coder cannot
                encode *data*, the encoded event is larger than
                ``stream_max_event_bytes``, this writer already wrote
                ``stream_maxlen`` events, or it is a retried start's handle.
            StreamFinished: If this writer already wrote the final entry.
            StreamExpired: If the run outlived ``stream_max_run``.
            StreamStoreError: If Redis cannot be reached.
        """
        check_event_type(type)
        self._check_writable()
        if self._backend._closing and self._is_own_task():
            # The application is shutting down.  Stop here rather than rely
            # on the cancel alone, which a concurrent await can lose.
            raise asyncio.CancelledError
        payload = self._backend._encode(data)
        async with self._lock:
            self._check_writable()  # a finish() may have gone first
            limit = get_settings().stream_maxlen
            if self._seq >= limit:
                raise ValueError(
                    f"stream {self.stream_id} already holds {limit} events "
                    "(stream_maxlen)"
                )
            # Kept if the append fails: a number is never sent twice, or
            # Redis would keep the first event under it and drop the second.
            # One that never landed shows readers a stream.gap, the truth.
            self._seq = seq = self._seq + 1
            args = _xadd(
                self._key, type, payload, idmp=(self._writer_id, str(seq)), seq=seq
            )
            with stream_span("redis_fastapi.streams.append"), timed_stream("append"):
                entry_id = _text(await self._backend._command(args))
        if entry_id is None:
            record_stream_operation(operation="append", result="expired")
            raise StreamExpired(f"stream {self.stream_id} has expired")
        self._last_write = time.monotonic()
        record_stream_operation(operation="append", result="ok")
        return entry_id

    async def finish(self, type: str, data: Any = None) -> str:
        """Write the final event and end the stream.

        The final event is an ordinary event of your chosen type, marked as
        the end.  Readers stop after it, and a reconnect after it gets 204.
        If the run was already ended - by a cancel, or by a reader that found
        it silent - this returns that final entry's ID and adds nothing.

        Returns:
            The final entry's ID.
        """
        check_event_type(type)
        return await self._end(type, data)

    def keepalive(self) -> AbstractAsyncContextManager[None]:
        """Keep a quiet run from looking lost while a block runs.

        A reader that sees no entry for ``stream_silence_timeout`` ends the
        run as ``producer_lost``.  While the block runs, an entry nobody sees
        (``stream.ping``) is written whenever the producer has been quiet long
        enough that no reader goes more than half that timeout without an
        entry.  :meth:`StreamBackend.spawn` runs it for you.
        """
        return self._keepalive()

    @asynccontextmanager
    async def _keepalive(self) -> AsyncIterator[None]:
        pinger = asyncio.create_task(self._ping())
        try:
            yield
        finally:
            pinger.cancel()
            await asyncio.gather(pinger, return_exceptions=True)

    async def _ping(self) -> None:
        settings = get_settings()
        # Looked at every heartbeat, so the longest gap a reader sees is
        # quiet + heartbeat: half the silence timeout.
        quiet = max(
            0.0,
            settings.stream_silence_timeout / 2 - settings.stream_heartbeat_interval,
        )
        while self._final_id is None:
            await asyncio.sleep(settings.stream_heartbeat_interval)
            if time.monotonic() - self._last_write < quiet:
                continue
            try:
                reply = await self._backend._command(
                    _xadd(self._key, PING_TYPE, "null", idmp=None)
                )
            except StreamStoreError:
                logger.debug("Stream ping failed; will retry", exc_info=True)
                continue
            if reply is None:
                return  # the run expired; the next append says so
            self._last_write = time.monotonic()

    def _is_own_task(self) -> bool:
        task = self._task
        return task is not None and asyncio.current_task() is task

    def _check_writable(self) -> None:
        if self._read_only:
            raise ValueError(
                f"this writer is a retried start's handle for stream "
                f"{self.stream_id}; the run's own producer writes to it"
            )
        if self._final_id is not None:
            raise StreamFinished(f"stream {self.stream_id} already ended")

    async def _end(self, type: str, data: Any) -> str:
        if self._final_id is not None:
            return self._final_id
        self._check_writable()
        payload = self._backend._encode(data)
        async with self._lock:  # after any append still in flight
            if self._final_id is not None:
                return self._final_id
            self._seq = seq = self._seq + 1
            with stream_span("redis_fastapi.streams.finish"), timed_stream("finish"):
                replies = await self._backend._pipeline(
                    lambda pipe: _queue_end(pipe, self._key, type, payload, seq),
                    transaction=True,
                )
        entry_id = _text(replies[0])
        if entry_id is None:
            record_stream_operation(operation="finish", result="expired")
            raise StreamExpired(f"stream {self.stream_id} has expired")
        self._final_id = entry_id
        record_stream_operation(operation="finish", result="ok")
        return entry_id


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------


@dataclass
class _ReadPlan:
    """What the reader learned before the first byte."""

    key: str
    stream_id: str
    cursor: Cursor
    last_seq: int | None
    first: list[tuple[Any, dict[Any, Any]]] = field(default_factory=list)


def _to_entry(raw_id: Any, raw_fields: dict[Any, Any]) -> tuple[_Entry, int | None]:
    fields = {_text(k): v for k, v in raw_fields.items()}
    seq_text = _text(fields.get(F_SEQ))
    entry = _Entry(
        id=_text(raw_id),
        type=_text(fields.get(F_TYPE)) or "",
        raw=_text(fields.get(F_DATA)),
        final=_text(fields.get(F_FINAL)) == "1",
    )
    return entry, int(seq_text) if seq_text else None


class StreamReader:
    """The events of one stream.  Made by :meth:`StreamBackend.follow`.

    An async iterator of :class:`StreamEvent`, and an async context manager
    that closes it.  It reads nothing until the first iteration, so a reader
    that is never iterated holds nothing.  Close one you leave early, with
    ``async with`` or :meth:`aclose`, so its read stops at once.
    """

    def __init__(self, backend: StreamBackend, plan: _ReadPlan | None) -> None:
        self._backend = backend
        self._plan = plan
        self._pages: AsyncGenerator[list[_Entry], None] | None = None
        self._buffer: deque[StreamEvent] = deque()

    def __aiter__(self) -> StreamReader:
        return self

    async def __anext__(self) -> StreamEvent:
        while not self._buffer:
            if self._plan is None:
                raise StopAsyncIteration
            if self._pages is None:
                self._pages = self._backend._pages(self._plan)
            try:
                page = await self._pages.__anext__()
            except StopAsyncIteration:
                self._plan = None
                raise
            self._buffer.extend(self._backend._event(entry) for entry in page)
        return self._buffer.popleft()

    async def aclose(self) -> None:
        """Stop reading.  Safe to call more than once."""
        self._plan = None
        self._buffer.clear()
        pages, self._pages = self._pages, None
        if pages is not None:
            await pages.aclose()

    async def __aenter__(self) -> StreamReader:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------


class StreamBackend:
    """Resumable streams backed by Redis.

    There is one instance per process.  ``FastAPIRedis(app).streams()``
    creates it, and :data:`~redis_fastapi.deps.StreamBackendDep` returns it.
    It holds state for the whole process: the tasks started by :meth:`spawn`,
    the live reader count and the server-support result.

    Args:
        client: A callable returning the async Redis client, so the backend
            follows the application's pool.
        coder: Serializer for event payloads.  Defaults to
            :class:`~redis_fastapi.types.JsonableCoder`.
        capabilities: Shared server-support cache.
    """

    def __init__(
        self,
        client: Callable[[], AsyncClient],
        *,
        coder: type[Coder] | None = None,
        capabilities: _StreamCapabilities | None = None,
    ) -> None:
        self._client = client
        self._coder: type[Coder] = coder or JsonableCoder
        self._caps = capabilities if capabilities is not None else _StreamCapabilities()
        self._tasks: set[asyncio.Task[None]] = set()
        self._spawned: dict[tuple[str, str], asyncio.Task[None]] = {}
        self._writers: dict[asyncio.Task[None], StreamWriter] = {}
        self._readers = 0
        self._closing = False

    # -- support -------------------------------------------------------------

    @property
    def supported(self) -> bool | None:
        """Whether the server supports the feature; ``None`` if not yet known."""
        return self._caps.supported

    async def detect(self) -> bool | None:
        """Detect server support once, and log one warning when it is missing."""
        if self._caps.supported is not None:
            return self._caps.supported
        try:
            result = await probe_streams_support(self._client())
        except RuntimeError:
            return None  # no pool yet
        if result is False:
            logger.warning(
                "Resumable streams are off: this Redis has no XCFGSET / DELEX.  "
                "They need Redis 8.6.2 or later; the rest of the application "
                "is unaffected."
            )
        elif result is True:
            logger.info("Resumable streams: server support detected")
        self._caps.supported = result
        return result

    async def _require_support(self) -> None:
        if self._caps.supported is None:
            await self.detect()
        if self._caps.supported is False:
            raise StreamUnsupported("resumable streams need Redis 8.6.2 or later")

    # -- plumbing ------------------------------------------------------------

    def _encode(self, data: Any) -> str:
        payload = self._coder.encode(data)
        size = len(payload.encode())
        limit = get_settings().stream_max_event_bytes
        if size > limit:
            raise ValueError(
                f"event of {size} bytes exceeds stream_max_event_bytes ({limit})"
            )
        return payload

    def _decode(self, raw: Any) -> Any:
        text = _text(raw)
        if text is None:
            return None
        try:
            return self._coder.decode(text)
        except Exception:
            logger.warning("Undecodable stream payload; passing it as text")
            return text

    async def _retry(self, attempt: Callable[[], Awaitable[_T]]) -> _T:
        """Run *attempt*, repeating it after a timeout or a dropped connection.

        Redis may or may not have applied a write before such a failure, and
        every write the feature sends is safe to repeat, so it is repeated
        with backoff.  Any other driver error ends it.

        The error text never carries Redis's own message, which can name the
        ACL user and the key.

        Raises:
            StreamStoreError: If Redis cannot be reached or refused a command.
        """
        for n in range(_WRITE_ATTEMPTS):
            try:
                return await attempt()
            except _AMBIGUOUS_ERRORS as exc:
                if n == _WRITE_ATTEMPTS - 1:
                    raise StreamStoreError("Redis did not answer") from exc
                await asyncio.sleep(0.05 * 2**n)
            except STORE_ERRORS as exc:
                raise StreamStoreError("Redis refused a stream command") from exc
        raise AssertionError("unreachable")  # pragma: no cover

    async def _command(self, args: list[Any]) -> Any:
        """Send one command under :meth:`_retry`."""
        return await self._retry(lambda: self._client().execute_command(*args))

    async def _pipeline(
        self, build: Callable[[Any], None], *, transaction: bool
    ) -> list[Any]:
        """Run a pipeline under :meth:`_retry`."""

        async def attempt() -> list[Any]:
            async with self._client().pipeline(transaction=transaction) as pipe:
                build(pipe)
                return list(await pipe.execute())

        return await self._retry(attempt)

    def _check_open(self) -> None:
        if self._closing:
            raise StreamStoreError("the application is shutting down")

    # -- writer side ---------------------------------------------------------

    async def open(self, *, scope: str, stream_id: str | None = None) -> StreamWriter:
        """Start a run, or return the same run on a retried start request.

        One transaction writes the run's first entry, sets how long the stream
        remembers idempotency keys, and gives the stream its lifetime,
        ``stream_max_run``.  A client-supplied *stream_id* - for
        example an idempotency key the browser generated - makes a retried
        ``POST`` safe: the second call finds the same run and returns a writer
        whose ``created`` is ``False``.  Start a producer only for a writer
        whose ``created`` is ``True``, or use :meth:`start`, which does both.

        Raises:
            StreamInvalidName: If *scope* or *stream_id* is invalid.
            StreamUnsupported: If the server lacks support.
            StreamStoreError: If Redis cannot be reached, or the key holds
                something other than a stream.
        """
        await self._require_support()
        check_name(scope, stream_id)
        self._check_open()
        sid = stream_id or secrets.token_urlsafe(12)
        key = _key(scope, sid)
        lifetime = get_settings().stream_max_run

        def build(pipe: Any) -> None:
            # No IDMP on the start entry: on a new stream the XCFGSET below
            # clears the IDMP history.  A retried start adds a second start
            # entry, which readers skip.
            pipe.execute_command(
                *_xadd(key, START_TYPE, "null", idmp=None, create=True)
            )
            # In the same transaction, so nothing - a cancel from another
            # process, say - can write an IDMP pair that this then clears.
            # A retried start sends the same value, which clears nothing.
            pipe.execute_command("XCFGSET", key, "IDMP-DURATION", IDMP_WINDOW)
            # NX sets a TTL only on a key that has none.  Every run gets its
            # TTL here, so "had none" means this call created the stream.
            pipe.expire(key, lifetime, nx=True)

        with stream_span("redis_fastapi.streams.open"), timed_stream("open"):
            try:
                replies = await self._pipeline(build, transaction=True)
            except StreamStoreError:
                record_stream_operation(operation="open", result="error")
                raise
        created = bool(replies[2])
        record_stream_operation(
            operation="open", result="created" if created else "retry"
        )
        return StreamWriter(
            self, scope=scope, stream_id=sid, writer_id=_new_id(), created=created
        )

    async def start(
        self,
        fn: Callable[[StreamWriter], Awaitable[None]],
        *,
        scope: str,
        stream_id: str | None = None,
    ) -> StreamWriter:
        """Open a run and :meth:`spawn` *fn* on it, unless it already exists.

        The usual start route in one call.  On a retried start, which finds
        the run already going, nothing is spawned, and the writer returned has
        ``created`` set to ``False``.  Return its ``stream_id`` either way.

        Raises:
            The same as :meth:`open`.
        """
        writer = await self.open(scope=scope, stream_id=stream_id)
        if writer.created:
            self.spawn(writer, fn)
        return writer

    def spawn(
        self,
        writer: StreamWriter,
        fn: Callable[[StreamWriter], Awaitable[None]],
    ) -> asyncio.Task[None]:
        """Run *fn* as a task the backend owns, outside the request.

        A disconnect never stops it.  :meth:`StreamWriter.keepalive` runs
        around it.  When *fn* returns, raises or is cancelled without having
        called :meth:`StreamWriter.finish`, the task writes a final
        ``stream.end`` event whose data gives the reason:
        ``{"reason": "done"}``, ``{"reason": "error", "code": ...}`` or
        ``{"reason": "cancelled"}``.  On application shutdown the remaining
        tasks are cancelled.

        Appends from one writer go out one at a time, and the final entry
        after them.  Await every append *fn* starts: one still running in a
        task of its own holds up the end, and at shutdown can outlast the
        grace period, so readers see ``producer_lost`` instead.

        Raises:
            ValueError: If *writer* is a handle from a retried start
                (``created`` is ``False``), whose run already has a producer,
                or if *writer* was already spawned.
        """
        if writer._read_only:
            raise ValueError(
                f"stream {writer.stream_id} already has a producer: spawn only a "
                "writer whose created is True, or use start()"
            )
        if writer._task is not None:
            raise ValueError(f"the writer of stream {writer.stream_id} already runs")

        async def end(data: dict[str, Any]) -> None:
            # From here on a repeated cancel, from cancel() or shutdown, must
            # not interrupt the final write.
            writer._ending = True
            if writer.finished:
                return
            try:
                await writer._end(END_TYPE, data)
            except _END_ERRORS:
                logger.warning(
                    "Could not end stream %s; readers will see it as lost",
                    writer.stream_id,
                    exc_info=True,
                )

        async def run() -> None:
            try:
                async with writer.keepalive():
                    await fn(writer)
            except asyncio.CancelledError:
                await end({"reason": "cancelled"})
                return
            except Exception as exc:
                logger.exception("Stream producer for %s failed", writer.stream_id)
                code = getattr(exc, "code", None)
                await end(
                    {
                        "reason": "error",
                        "code": code if isinstance(code, str) else "producer_error",
                    }
                )
                return
            await end({"reason": "done"})

        task = asyncio.create_task(run())
        writer._task = task
        key = (writer.scope, writer.stream_id)
        self._tasks.add(task)
        self._spawned[key] = task
        self._writers[task] = writer

        def forget(done: asyncio.Task[None]) -> None:
            self._tasks.discard(done)
            self._writers.pop(done, None)
            if self._spawned.get(key) is done:
                del self._spawned[key]

        task.add_done_callback(forget)
        return task

    async def cancel(self, stream_id: str, *, scope: str) -> None:
        """End the run now, with a ``stream.end`` whose reason is ``cancelled``.

        Readers stop at once.  A producer started by :meth:`spawn` in this
        process is cancelled too; one in another process is not told, and
        whatever it writes afterwards lands after the final entry, where no
        reader looks.  Cancelling a run that already ended changes nothing.

        Raises:
            StreamInvalidName: If *scope* or *stream_id* is invalid.
            StreamNotFound: If there is no such stream in this scope.
            StreamStoreError: If Redis cannot be reached.
        """
        await self._require_support()
        check_name(scope, stream_id)
        key = _key(scope, stream_id)
        payload = self._encode({"reason": "cancelled"})
        replies = await self._pipeline(
            lambda pipe: _queue_end(pipe, key, END_TYPE, payload, None),
            transaction=True,
        )
        if _text(replies[0]) is None:
            raise StreamNotFound(f"no stream {stream_id} in this scope")
        task = self._spawned.get((scope, stream_id))
        if task is not None and not task.done():
            self._cancel_unless_ending({task})
        record_stream_operation(operation="cancel", result="ok")

    # -- reader side ---------------------------------------------------------

    async def follow(
        self,
        stream_id: str,
        *,
        scope: str,
        after: str | None = None,
    ) -> StreamReader:
        """Check the stream and the cursor, and return a reader for it.

        The transport-neutral core: :meth:`sse` wraps it for HTTP.  Every
        check runs here, before the first event, so a WebSocket handler can
        refuse the connection with a code before it accepts it::

            reader = await streams.follow(stream_id, scope=scope, after=cursor)
            await websocket.accept()
            async with reader:
                async for event in reader:
                    await websocket.send_json(...)

        With no *after* it reads the whole stream, and fails if anything was
        trimmed.  The reader replays the events after *after*, follows the
        live stream, and stops after the final event; it is empty when
        *after* is already the final event.  It holds nothing in Redis until
        iterated, and closes on ``async with`` exit or
        :meth:`StreamReader.aclose`.

        Unlike :meth:`sse` it takes no reader slot, so
        ``stream_max_readers`` does not count it.  Bound such readers yourself.

        Raises:
            StreamInvalidName: *scope* or *stream_id* is invalid.
            StreamNotFound: No such stream in this scope: unknown, expired,
                or another scope's.
            StreamExpired: Entries after *after* were trimmed.
            StreamCursorAhead: *after* is past the stream's last entry.
            StreamInvalidCursor: *after* is not a valid entry ID.
        """
        await self._require_support()
        plan = await self._prepare(stream_id, scope=scope, after=after)
        return StreamReader(self, plan)

    async def sse(
        self,
        stream_id: str,
        *,
        request: Request,
        scope: str,
        after: str | None = None,
        expires_at: datetime | None = None,
    ) -> Response:
        """An SSE response for the stream.  See :func:`redis_fastapi.streams.sse`."""
        from redis_fastapi.streams import sse

        return await sse(
            self,
            stream_id,
            request=request,
            scope=scope,
            after=after,
            expires_at=expires_at,
        )

    async def _prepare(
        self, stream_id: str, *, scope: str, after: str | None
    ) -> _ReadPlan | None:
        """Check the stream and the cursor, and read the first page.

        Returns ``None`` when the cursor is the final entry.  Runs as one
        transaction, so the checks and the first page are one snapshot and no
        trim can fall between them.
        """
        check_name(scope, stream_id)
        cursor = parse_cursor(after) if after is not None else _START
        key = _key(scope, stream_id)
        page = get_settings().stream_replay_page

        def build(pipe: Any) -> None:
            pipe.exists(key)
            pipe.xinfo_stream(key)
            if cursor != _START:
                pipe.xrange(key, _format(cursor), _format(cursor))
            # XRANGE with an exclusive start, not XREAD: redis-py cannot
            # queue XREAD, whose keys move, in an async Cluster transaction.
            pipe.xrange(key, f"({_format(cursor)}", "+", count=page)

        try:
            async with self._client().pipeline(transaction=True) as pipe:
                build(pipe)
                replies = list(await pipe.execute(raise_on_error=False))
        except STORE_ERRORS as exc:
            raise StreamStoreError("could not read the stream") from exc
        if isinstance(replies[0], Exception):
            raise StreamStoreError("could not read the stream") from replies[0]
        if not replies[0]:
            record_stream_operation(operation="read", result="not_found")
            raise StreamNotFound(f"no stream {stream_id} in this scope")
        info = replies[1]
        if isinstance(info, Exception):
            raise StreamStoreError("could not read the stream") from info

        last_generated = _info_id(info, "last-generated-id")
        if cursor != _START and (last_generated is None or cursor > last_generated):
            record_stream_operation(operation="read", result="ahead")
            raise StreamCursorAhead(f"cursor {_format(cursor)} is ahead of the stream")
        own = replies[2] if cursor != _START else None
        own_fields: dict[str | None, str | None] = {}
        if own and not isinstance(own, Exception):
            own_fields = {_text(k): _text(v) for k, v in own[0][1].items()}
        if own_fields.get(F_FINAL) == "1":
            record_stream_operation(operation="read", result="finished")
            return None  # the cursor is the final entry
        if _history_lost(info, cursor, own_present=bool(own_fields)):
            record_stream_operation(operation="read", result="expired")
            raise StreamExpired(
                "entries after the cursor were removed; reload from the source"
            )
        first_reply = replies[-1]
        first = [] if isinstance(first_reply, Exception) else list(first_reply)

        last_seq: int | None = 0 if cursor == _START else None
        if own_fields:
            seq_text = own_fields.get(F_SEQ)
            last_seq = int(seq_text) if seq_text else None
        record_stream_operation(
            operation="read", result="started" if cursor == _START else "resumed"
        )
        return _ReadPlan(
            key=key,
            stream_id=stream_id,
            cursor=cursor,
            last_seq=last_seq,
            first=first,
        )

    async def _pages(
        self, plan: _ReadPlan, *, deadline: float | None = None
    ) -> AsyncGenerator[list[_Entry], None]:
        """The read loop.  Yields each read's entries as one page.

        An empty page is a keepalive: a quiet heartbeat, a read retried after
        a Redis failure, or *deadline* (a ``time.monotonic()`` value) reached.
        No blocking read runs past *deadline*, so a caller that stops there
        stops on time.  The page with the final entry is the last.

        Silence is measured here, on this process's clock: from the start of
        the read, and from every entry it receives.  Past
        ``stream_silence_timeout`` the reader ends the run as
        ``producer_lost``.
        """
        settings = get_settings()
        heartbeat_ms = settings.stream_heartbeat_interval * 1000  # BLOCK is in ms
        cursor = plan.cursor
        last_seq = plan.last_seq
        pending = plan.first
        last_seen = time.monotonic()
        failing_since: float | None = None
        while True:
            if pending:
                last_seen = time.monotonic()
                page: list[_Entry] = []
                for raw_id, raw_fields in pending:
                    entry, seq = _to_entry(raw_id, raw_fields)
                    if entry.type in _HIDDEN_TYPES:
                        continue  # the SDK's own markers, never shown
                    if seq is not None:
                        if last_seq is not None and seq > last_seq + 1:
                            record_stream_operation(operation="gap", result="ok")
                            page.append(
                                _Entry.own(GAP_TYPE, {"missing": seq - last_seq - 1})
                            )
                        # The highest seen: appends in flight together can be
                        # stored out of order, and a late lower number must
                        # not make the next entry look like a second gap.
                        last_seq = seq if last_seq is None else max(last_seq, seq)
                    page.append(entry)
                    if entry.final:
                        yield page
                        return
                cursor = parse_cursor(_text(pending[-1][0]) or "0-0")
                pending = []
                if page:
                    yield page
            if self._closing:
                return  # shutting down: close, and the client reconnects
            block_ms = heartbeat_ms
            if deadline is not None:
                left_ms = int((deadline - time.monotonic()) * 1000)
                if left_ms <= 0:
                    yield []
                    continue
                block_ms = min(block_ms, left_ms)
            try:
                reply = await self._client().xread(
                    {plan.key: _format(cursor)},
                    count=settings.stream_replay_page,
                    block=max(1, block_ms),  # BLOCK 0 would wait forever
                )
                if failing_since is not None:
                    # Redis was out, so silence proved nothing: the producer
                    # gets a full timeout of reachable Redis again.
                    last_seen = time.monotonic()
                failing_since = None
            except STORE_ERRORS:
                now = time.monotonic()
                failing_since = failing_since or now
                if now - failing_since > settings.stream_read_retry_window:
                    raise StreamStoreError("Redis stayed unreachable") from None
                logger.debug("Stream read failed; retrying", exc_info=True)
                await asyncio.sleep(min(1.0, block_ms / 1000))
                yield []
                continue
            pending = _xread_entries(reply)
            if pending:
                continue
            if time.monotonic() - last_seen < settings.stream_silence_timeout:
                yield []
                continue
            state, final_id = await self._end_lost(plan)
            if state == "ended" and final_id and parse_cursor(final_id) <= cursor:
                return  # the run ended before this cursor: nothing will follow
            if state == "gone":
                return  # the stream itself is gone: the reconnect gets 404
            if state == "lost_read_only":
                yield [_Entry.own(ERROR_TYPE, {"code": "producer_lost"})]
                return
            if state == "retry":
                last_seen = time.monotonic()  # Redis failed; ask again later
            # "ended": the next read returns the final entry

    def _event(self, entry: _Entry) -> StreamEvent:
        data = entry.data if entry.id is None else self._decode(entry.raw)
        return StreamEvent(entry.id, entry.type, data, entry.final)

    async def _end_lost(self, plan: _ReadPlan) -> tuple[str, str | None]:
        """Write the final entry a silent producer never wrote.

        The same entry and the same idempotency key as a writer's final
        entry, so a producer that was only slow and ends the run a moment
        later adds nothing, and a run that already ended returns its final
        entry instead.

        Returns:
            ``("ended", final entry ID)``, ``("gone", None)`` when the stream
            no longer exists, ``("lost_read_only", None)`` when this client
            may not write, or ``("retry", None)`` when Redis failed.
        """
        payload = self._encode({"reason": "producer_lost"})
        try:
            replies = await self._pipeline(
                lambda pipe: _queue_end(pipe, plan.key, END_TYPE, payload, None),
                transaction=True,
            )
        except StreamStoreError as exc:
            if isinstance(exc.__cause__, NoPermissionError):
                return "lost_read_only", None
            logger.debug("Could not end a silent producer's stream", exc_info=True)
            return "retry", None
        final_id = _text(replies[0])
        if final_id is None:
            return "gone", None
        record_stream_operation(operation="producer_lost", result="ok")
        return "ended", final_id

    # -- readers and lifecycle -----------------------------------------------

    def _start(self, capabilities: _StreamCapabilities) -> None:
        """Bind to a freshly built pool.  Called by the lifespan at startup."""
        self._caps = capabilities
        self._closing = False

    def _reserve_reader(self) -> bool:
        if self._closing or self._readers >= get_settings().stream_max_readers:
            return False
        self._readers += 1
        add_stream_readers(1)
        return True

    def _release_reader(self) -> None:
        self._readers -= 1
        add_stream_readers(-1)

    async def aclose(self) -> None:
        """Stop the feature: close live readers and cancel spawned producers.

        Each cancelled producer writes a ``stream.end`` with reason
        ``cancelled``, so its readers do not hang.  Called by the lifespan
        before the connection pool closes.
        """
        self._closing = True
        # Let producers spawned a moment ago start first: a task cancelled
        # before it runs never writes its final entry.
        await asyncio.sleep(0)
        pending = set(self._tasks)
        self._cancel_unless_ending(pending)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _SHUTDOWN_GRACE_SECONDS
        while pending:
            _, pending = await asyncio.wait(pending, timeout=_SHUTDOWN_RETRY_SECONDS)
            if not pending:
                break
            if loop.time() >= deadline:
                logger.warning(
                    "%d stream producer(s) did not stop within %.0f s of "
                    "shutdown; their readers will see producer_lost",
                    len(pending),
                    _SHUTDOWN_GRACE_SECONDS,
                )
                break
            # On Python 3.10 and 3.11 asyncio.wait_for, which redis-py uses
            # for socket writes, can lose a cancel that arrives as the write
            # completes.  Repeat it.
            self._cancel_unless_ending(pending)

    def _cancel_unless_ending(self, tasks: set[asyncio.Task[None]]) -> None:
        """Cancel spawned producers, except one already writing its final entry.

        A cancel there would abort that write, and readers would wait out the
        silence timeout to see ``producer_lost`` instead of the real reason.
        """
        for task in tasks:
            writer = self._writers.get(task)
            if writer is None or not writer._ending:
                task.cancel()


def _new_id() -> str:
    return secrets.token_urlsafe(9)


def _history_lost(info: Any, cursor: Cursor, *, own_present: bool) -> bool:
    """Whether an entry after *cursor* was removed, from ``XINFO STREAM``.

    ``XDEL`` raises ``max-deleted-entry-id``, so a deleted entry newer than
    the cursor is exact to detect.  Trimming does not raise that field: it
    shows only as ``entries-added`` exceeding ``length``, with the oldest
    entries gone.  So once anything was removed, a cursor whose own entry is
    gone and that is older than the first retained entry counts as a gap.
    That is conservative: a client whose last entry was exactly the newest
    one trimmed also gets 410, although it missed nothing.  And with no
    cursor (``0-0``, the whole stream) any removal is a gap.
    """
    if not isinstance(info, dict):
        return False
    max_deleted = _info_id(info, "max-deleted-entry-id")
    if max_deleted is not None and cursor < max_deleted:
        return True
    try:
        removed = int(_text(info.get("entries-added")) or 0) - int(
            _text(info.get("length")) or 0
        )
    except ValueError:
        return False
    if removed <= 0:
        return False
    if cursor == _START:
        return True
    if own_present:
        return False  # trimming removes the oldest entries, and this one stayed
    first = info.get("first-entry")
    if first:
        first_id = _text(first[0])
        return first_id is not None and cursor < parse_cursor(first_id)
    last = _info_id(info, "last-generated-id")
    return last is not None and cursor < last


def _info_id(info: Any, name: str) -> Cursor | None:
    if not isinstance(info, dict):
        return None
    value = _text(info.get(name))
    if value is None:
        return None
    try:
        return parse_cursor(value)
    except StreamInvalidCursor:
        return None
