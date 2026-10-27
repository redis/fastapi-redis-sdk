"""Resumable streams: a Redis-backed event log a client can resume.

One Redis Stream holds one run - one agent turn, for example.  The entry ID is
the cursor and the SSE ``id:``, so a client that reconnects with
``Last-Event-ID`` gets exactly the entries after the last one it saw.  The run
is decoupled from the HTTP request: a disconnect never stops it.

Keys, all built from the SDK's ``prefix`` setting::

    <prefix>:stream:{<scope>}:<id>        Stream   entries
    <prefix>:stream-meta:{<scope>}:<id>   Hash     status, cancel, writer, started,
                                                   alive (a field with its own TTL)
    <prefix>:stream-active:{<scope>}      String   the scope's active run

The scope is part of every stream key, so a stream is reachable only through
the scope the application authorized, and the ``{<scope>}`` hash tag keeps a
scope's keys in one Cluster slot.

Entry fields: ``t`` (type), ``d`` (payload), ``q`` (writer sequence number) and
``f`` (``1`` on the final entry only).

The HTTP side - the SSE response, cursor selection and the exception
handlers - lives in :mod:`redis_fastapi.streams`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import secrets
import time
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, TypeAlias

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import (
    NoPermissionError,
    RedisClusterException,
    RedisError,
    WatchError,
)
from redis.exceptions import TimeoutError as RedisTimeoutError

from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import (
    StreamBusy,
    StreamCursorAhead,
    StreamError,
    StreamExpired,
    StreamFenced,
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
from redis_fastapi.types import Coder, JsonCoder

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
# here is safe to repeat (IDMP, EXPIRE, HSET, SET), so these are retried.
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

# Entry fields.
F_TYPE = "t"
F_DATA = "d"
F_SEQ = "q"
F_FINAL = "f"

# Meta fields.
M_STATUS = "status"
M_CANCEL = "cancel"
M_WRITER = "writer"
M_STARTED = "started"
# The producer heartbeat: a field with its own TTL, refreshed by every append
# and heartbeat.  A meta hash holding only this field is the remnant of an
# expired run (a late heartbeat recreated it), and counts as no meta.
M_ALIVE = "alive"

STATUS_RUNNING = "running"
STATUS_FINISHED = "finished"

# Types starting with this prefix belong to the SDK.
RESERVED_PREFIX = "stream."
END_TYPE = "stream.end"
GAP_TYPE = "stream.gap"
ERROR_TYPE = "stream.error"

# The one IDMP pair every final entry shares, so a stream holds at most one
# final entry even when two writers, or a writer and a reader, end it at once.
_END_PRODUCER = "end"
_END_ID = "final"

# No braces, so no value can break out of the hash tag.  No ":" in an ID, so
# "<scope>:<id>" splits in exactly one way and two pairs never share a key.
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
    """Reject a scope or stream ID that could break a key's hash tag.

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


def _ms(value: float | timedelta) -> int:
    """A duration in whole milliseconds, at least 1, for ``PX`` and ``PEXPIRE``."""
    if isinstance(value, timedelta):
        value = value.total_seconds()
    return max(1, int(value * 1000))


@dataclass(frozen=True)
class _Keys:
    stream: str
    meta: str
    active: str


def _active_key(scope: str) -> str:
    return f"{get_settings().prefix}:stream-active:{{{scope}}}"


def _keys(scope: str, stream_id: str) -> _Keys:
    # Every key of a run, and the scope's active slot, hash on the scope, so
    # they share one Cluster slot and one transaction can cover them all.
    prefix = get_settings().prefix
    tag = f"{{{scope}}}:{stream_id}"
    return _Keys(
        stream=f"{prefix}:stream:{tag}",
        meta=f"{prefix}:stream-meta:{tag}",
        active=_active_key(scope),
    )


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

    ``XCFGSET`` arrived in Redis 8.6 together with ``XADD ... IDMP``, and
    ``DELEX`` (with ``SET ... IFEQ``) in 8.4, so both present means the server
    has the commands this feature uses.  Like
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
        exclusive: bool,
        created: bool,
        seq: int = 0,
        started: bool = False,
    ) -> None:
        self._backend = backend
        self.scope = scope
        self.stream_id = stream_id
        self.created = created
        self._keys = _keys(scope, stream_id)
        self._writer_id = writer_id
        self._exclusive = exclusive
        self._seq = seq
        self._started = started
        self._final_id: str | None = None
        self._cancelled = False
        self._fenced = False
        # Set when spawn() starts writing the final entry, so a repeated
        # cancel at shutdown never interrupts that write.
        self._ending = False
        # A handle for a run that already exists (a retried start) writes
        # nothing: the run's own producer does.
        self._read_only = False
        self._task: asyncio.Task[None] | None = None

    @property
    def cancelled(self) -> bool:
        """Whether a stop was requested, as last seen by an append or heartbeat."""
        return self._cancelled

    @property
    def finished(self) -> bool:
        """Whether this writer has written the final entry."""
        return self._final_id is not None

    async def append(self, type: str, data: Any) -> str:
        """Append one event and return its entry ID.

        One transaction writes the entry, refreshes the stream's TTL and the
        producer heartbeat, and reads the cancel flag into :attr:`cancelled`.
        A producer started by :meth:`StreamBackend.spawn` is cancelled for
        you; one you run yourself checks :attr:`cancelled` and calls
        :meth:`finish`.  A timeout or a dropped connection is retried with the
        same idempotency key, so a retried append never stores the event
        twice.

        Raises:
            ValueError: If *type* is invalid or reserved, or the encoded event
                is larger than ``stream_max_event_bytes``.
            StreamFinished: If the final entry was already written.
            StreamExpired: If the stream expired, for example after a pause
                longer than its TTL.
            StreamFenced: If this writer does not own the stream: a handle
                from a retried start, or a writer that
                :meth:`StreamBackend.attach` replaced.
            StreamStoreError: If Redis cannot be reached.
        """
        check_event_type(type)
        self._check_writable()
        if self._final_id is not None:
            raise StreamFinished(f"stream {self.stream_id} already ended")
        if self._backend._closing and self._is_own_task():
            # The application is shutting down.  Stop here rather than rely
            # on the cancel alone, which a concurrent await can lose.
            raise asyncio.CancelledError
        payload = self._backend._encode(data)
        with stream_span("redis_fastapi.streams.append"), timed_stream("append"):
            entry_id, cancelled = await self._write(
                type, payload, self._seq + 1, final=False
            )
        record_stream_operation(operation="append", result="ok")
        if cancelled:
            self._cancelled = True
            current = asyncio.current_task()
            if current is not None and current is self._task:
                current.cancel()
                await asyncio.sleep(0)  # raises CancelledError here
        return entry_id

    async def finish(self, type: str, data: Any = None) -> str:
        """Write the final event and end the stream.

        The final event is an ordinary event of your chosen type, marked as
        the end.  Readers stop after it, and a reconnect after it gets 204.
        Every final entry shares one idempotency key, so a stream has at most
        one: if another writer ended it first, this returns that entry's ID.

        Returns:
            The final entry's ID.

        Raises:
            StreamFenced: If this writer does not own the stream.  A writer
                that :meth:`StreamBackend.attach` replaced cannot end the new
                writer's run.
        """
        check_event_type(type)
        return await self._end(type, data)

    async def extend(self, ttl: float | timedelta) -> None:
        """Keep the stream, its heartbeat and its active slot for *ttl*.

        For a run that waits on a slow tool, or that another process will
        continue with :meth:`StreamBackend.attach`.  *ttl* is in seconds.

        Raises:
            StreamFenced: If this writer does not own the stream.
            StreamFinished: If the final entry was already written.
            StreamStoreError: If Redis cannot be reached.
        """
        self._check_writable()
        if self._final_id is not None:
            raise StreamFinished(f"stream {self.stream_id} already ended")
        ms = _ms(ttl)
        gone = _ms(get_settings().stream_gone_ttl_seconds)
        client = self._backend._client()
        try:
            pipe = client.pipeline(transaction=True)
            pipe.pexpire(self._keys.stream, ms)
            pipe.pexpire(self._keys.meta, ms + gone)
            _set_alive(pipe, self._keys, ms)
            if self._exclusive:
                _refresh_slot(pipe, self._keys, self.stream_id, ms)
            await pipe.execute()
        except STORE_ERRORS as exc:
            raise StreamStoreError("could not extend the stream") from exc

    def keepalive(self) -> AbstractAsyncContextManager[None]:
        """Refresh the producer heartbeat while a block runs.

        Every ``stream_heartbeat_seconds`` it refreshes the heartbeat and the
        active slot, and reads the cancel flag.  It covers long tool calls
        during which no append happens.  :meth:`StreamBackend.spawn` runs it
        for you.
        """
        return self._keepalive()

    @asynccontextmanager
    async def _keepalive(self) -> AsyncIterator[None]:
        beat = asyncio.create_task(self._beat())
        try:
            yield
        finally:
            beat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await beat

    async def _beat(self) -> None:
        settings = get_settings()
        while True:
            await asyncio.sleep(settings.stream_heartbeat_seconds)
            client = self._backend._client()
            try:
                pipe = client.pipeline(transaction=True)
                _set_alive(pipe, self._keys, _ms(settings.stream_alive_ttl_seconds))
                if self._exclusive:
                    _refresh_slot(
                        pipe,
                        self._keys,
                        self.stream_id,
                        _ms(settings.stream_active_ttl_seconds),
                    )
                pipe.hmget(self._keys.meta, [M_CANCEL, M_WRITER])
                replies = await pipe.execute()
            except STORE_ERRORS:
                logger.debug("Stream heartbeat failed; will retry", exc_info=True)
                continue
            cancel, writer = (_text(v) for v in replies[-1])
            if writer is not None and writer != self._writer_id:
                logger.info("Stream %s was handed to another writer", self.stream_id)
                self._fenced = True
                if self._task is not None:
                    self._task.cancel()
                return
            if cancel == "1":
                self._cancelled = True
                if self._task is not None:
                    self._task.cancel()
                return

    def _is_own_task(self) -> bool:
        task = self._task
        return task is not None and asyncio.current_task() is task

    def _check_writable(self) -> None:
        if self._read_only or self._fenced:
            raise StreamFenced(
                f"this writer does not own stream {self.stream_id}; "
                "its own producer writes to it"
            )

    async def _end(self, type: str, data: Any) -> str:
        self._check_writable()
        if self._final_id is not None:
            return self._final_id
        payload = self._backend._encode(data)
        with stream_span("redis_fastapi.streams.finish"), timed_stream("finish"):
            entry_id, _ = await self._write(type, payload, self._seq + 1, final=True)
            self._final_id = entry_id
        record_stream_operation(operation="finish", result="ok")
        return entry_id

    async def _write(
        self, type: str, payload: str, seq: int, *, final: bool
    ) -> tuple[str, bool]:
        settings = get_settings()
        keys = self._keys
        ttl = (
            settings.stream_done_ttl_seconds
            if final
            else settings.stream_active_ttl_seconds
        )
        args = _xadd(
            keys,
            type,
            payload,
            idmp=(_END_PRODUCER, _END_ID) if final else (self._writer_id, str(seq)),
            seq=seq,
            final=final,
            nomkstream=self._started,
        )

        def build(pipe: Any) -> None:
            _build_write(
                pipe,
                args,
                keys,
                self.stream_id,
                _ms(ttl),
                final=final,
                started=self._started,
                exclusive=self._exclusive,
            )

        # An append checks the fence after the fact, in the same transaction:
        # at most that one entry lands after a takeover.  The final entry is
        # checked first, under WATCH, because a stale one would end the new
        # writer's run.  It is written once per run, so the extra round trip
        # costs nothing that matters.
        owner = (keys.meta, self._writer_id) if final else None
        try:
            replies = await self._backend._transaction(build, owner=owner)
        except StreamFenced:
            self._fenced = True
            record_stream_operation(operation="finish", result="fenced")
            raise
        entry_id = _text(replies[0])
        if entry_id is None:
            record_stream_operation(operation="append", result="expired")
            raise StreamExpired(f"stream {self.stream_id} has expired")
        cancel, writer = (_text(v) for v in replies[-1])
        self._seq = seq
        self._started = True
        if writer is None:
            raise StreamExpired(f"stream {self.stream_id} has expired")
        if writer != self._writer_id:
            self._fenced = True
            record_stream_operation(operation="append", result="fenced")
            raise StreamFenced(f"stream {self.stream_id} was handed to another writer")
        return entry_id, cancel == "1"


def _xadd(
    keys: _Keys,
    type: str,
    payload: str,
    *,
    idmp: tuple[str, str],
    seq: int | None,
    final: bool,
    nomkstream: bool,
) -> list[Any]:
    """The ``XADD`` for one entry, shared by every path that writes one.

    *nomkstream* once the stream has started: an append must never recreate a
    stream that expired.  *seq* is ``None`` only for the final entry a reader
    writes for a lost producer, since no reader knows the writer's sequence.
    """
    args: list[Any] = ["XADD", keys.stream]
    if nomkstream:
        args.append("NOMKSTREAM")
    args += ["IDMP", *idmp]
    args += ["MAXLEN", "~", str(get_settings().stream_maxlen), "*"]
    args += [F_TYPE, type, F_DATA, payload]
    if seq is not None:
        args += [F_SEQ, str(seq)]
    if final:
        args += [F_FINAL, "1"]
    return args


def _set_alive(pipe: Any, keys: _Keys, ms: int) -> None:
    """Queue a refresh of the producer heartbeat: meta's ``alive`` field.

    ``HSETEX`` gives the field its own TTL and leaves meta's TTL alone.
    """
    pipe.execute_command("HSETEX", keys.meta, "PX", ms, "FIELDS", 1, M_ALIVE, "1")


def _refresh_slot(pipe: Any, keys: _Keys, stream_id: str, ms: int) -> None:
    """Queue an extension of the active slot, only while it names *stream_id*."""
    pipe.execute_command("SET", keys.active, stream_id, "IFEQ", stream_id, "PX", ms)


def _release_slot(pipe: Any, keys: _Keys, stream_id: str) -> None:
    """Queue freeing the active slot, only if it names *stream_id*.

    A stale writer must not free the slot of a newer run.
    """
    pipe.execute_command("DELEX", keys.active, "IFEQ", stream_id)


def _build_write(
    pipe: Any,
    args: list[Any],
    keys: _Keys,
    stream_id: str,
    ms: int,
    *,
    final: bool,
    started: bool,
    exclusive: bool,
) -> None:
    settings = get_settings()
    gone = _ms(settings.stream_gone_ttl_seconds)
    pipe.execute_command(*args)
    if not started:
        pipe.hset(keys.meta, M_STARTED, "1")
    if final:
        pipe.hset(keys.meta, M_STATUS, STATUS_FINISHED)
    pipe.pexpire(keys.stream, ms)
    pipe.pexpire(keys.meta, ms + gone)
    if final:
        # No need to clear the heartbeat: once status is finished, nothing
        # reads it, and the field expires by itself.
        _release_slot(pipe, keys, stream_id)
    else:
        _set_alive(pipe, keys, _ms(settings.stream_alive_ttl_seconds))
        if exclusive:
            # A run that appends keeps its slot even without keepalive().
            _refresh_slot(pipe, keys, stream_id, ms)
    pipe.hmget(keys.meta, [M_CANCEL, M_WRITER])


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------


@dataclass
class _ReadPlan:
    """What the reader learned before the first byte."""

    keys: _Keys
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
        coder: Serializer for event payloads.  Defaults to :class:`JsonCoder`.
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
        self._coder: type[Coder] = coder or JsonCoder
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
        limit = get_settings().stream_max_event_bytes
        if len(payload.encode()) > limit:
            raise ValueError(
                f"event of {len(payload.encode())} bytes exceeds "
                f"stream_max_event_bytes ({limit})"
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

    async def _transaction(
        self,
        build: Callable[[Any], None],
        *,
        owner: tuple[str, str] | None = None,
    ) -> list[Any]:
        """Run a transaction, retrying the ambiguous failures.

        Every command the feature sends in a transaction is safe to repeat, so
        a timeout or a dropped connection, after which Redis may or may not
        have applied the writes, is retried with backoff.

        With *owner*, a ``(meta key, writer ID)`` pair, the transaction runs
        only while meta still names that writer: it ``WATCH``es meta, checks
        the writer, and repeats if meta changed before ``EXEC``.

        The error text never carries Redis's own message, which can name the
        ACL user and the key.

        Raises:
            StreamFenced: If meta names another writer.
            StreamExpired: If meta is gone.
            StreamStoreError: If Redis cannot be reached.
        """
        for attempt in range(_WRITE_ATTEMPTS):
            try:
                if owner is None:
                    pipe = self._client().pipeline(transaction=True)
                    build(pipe)
                    return list(await pipe.execute())
                return await self._owned_transaction(build, *owner)
            except WatchError:
                continue  # meta changed under the WATCH: check the owner again
            except _AMBIGUOUS_ERRORS as exc:
                if attempt == _WRITE_ATTEMPTS - 1:
                    raise StreamStoreError("Redis did not answer") from exc
                await asyncio.sleep(0.05 * 2**attempt)
            except STORE_ERRORS as exc:
                raise StreamStoreError("Redis refused a stream write") from exc
        raise StreamStoreError("the stream kept changing; retry")

    async def _owned_transaction(
        self, build: Callable[[Any], None], meta: str, writer_id: str
    ) -> list[Any]:
        client = self._client()
        async with client.pipeline(transaction=True) as pipe:
            await pipe.watch(meta)
            # Read on another connection, as _create does: EXEC still aborts
            # if meta changes after the WATCH, whoever changes it.
            current = _text(await client.hget(meta, M_WRITER))
            if current is None:
                raise StreamExpired("the stream has expired")
            if current != writer_id:
                raise StreamFenced("the stream was handed to another writer")
            pipe.multi()  # type: ignore[no-untyped-call]
            build(pipe)
            return list(await pipe.execute())

    async def _set_meta(self, keys: _Keys, mapping: dict[Any, Any]) -> None:
        """Write meta fields without ever leaving meta without a TTL.

        If meta expired a moment ago, ``HSET`` would create it again with no
        expiry.  ``EXPIRE ... NX`` sets a TTL only on a key that has none, so
        it repairs exactly that case and leaves a live meta's TTL alone.
        """
        settings = get_settings()
        pipe = self._client().pipeline(transaction=True)
        pipe.hset(keys.meta, mapping=mapping)
        pipe.pexpire(
            keys.meta,
            _ms(settings.stream_done_ttl_seconds + settings.stream_gone_ttl_seconds),
            nx=True,
        )
        await pipe.execute()

    # -- writer side ---------------------------------------------------------

    async def open(
        self,
        *,
        scope: str,
        stream_id: str | None = None,
        exclusive: bool = True,
    ) -> StreamWriter:
        """Start a run, or return the same run on a retried start request.

        With *exclusive* (the default) the run takes the scope's active slot,
        so a scope holds one active run at a time.  A client-supplied
        *stream_id* - for example an idempotency key the browser generated -
        makes a retried ``POST`` safe: the second call finds the same run and
        returns a writer whose ``created`` is ``False``.  Start a producer only
        for a writer whose ``created`` is ``True``, or use :meth:`start`,
        which does both.

        Raises:
            StreamInvalidName: If *scope* or *stream_id* is invalid.
            StreamBusy: If the scope's slot names another run.
            StreamUnsupported: If the server lacks support.
            StreamStoreError: If Redis cannot be reached.
        """
        await self._require_support()
        check_name(scope, stream_id)
        sid = stream_id or secrets.token_urlsafe(12)
        keys = _keys(scope, sid)
        writer_id = _new_writer_id()
        with stream_span("redis_fastapi.streams.open"), timed_stream("open"):
            try:
                created = await self._create(keys, sid, writer_id, exclusive)
            except STORE_ERRORS as exc:
                record_stream_operation(operation="open", result="error")
                raise StreamStoreError("could not open the stream") from exc
        if not created:
            record_stream_operation(operation="open", result="retry")
            return self._handle(scope, sid, exclusive)
        record_stream_operation(operation="open", result="created")
        return StreamWriter(
            self,
            scope=scope,
            stream_id=sid,
            writer_id=writer_id,
            exclusive=exclusive,
            created=True,
        )

    async def start(
        self,
        fn: Callable[[StreamWriter], Awaitable[None]],
        *,
        scope: str,
        stream_id: str | None = None,
        exclusive: bool = True,
    ) -> StreamWriter:
        """Open a run and :meth:`spawn` *fn* on it, unless it already exists.

        The usual start route in one call.  On a retried start, which finds
        the run already going, nothing is spawned, and the writer returned has
        ``created`` set to ``False``.  Return its ``stream_id`` either way.

        Raises:
            The same as :meth:`open`.
        """
        writer = await self.open(scope=scope, stream_id=stream_id, exclusive=exclusive)
        if writer.created:
            self.spawn(writer, fn)
        return writer

    async def _create(
        self, keys: _Keys, stream_id: str, writer_id: str, exclusive: bool
    ) -> bool:
        """Create the run unless it exists, in one optimistic transaction.

        ``WATCH`` the slot and meta, read them, then take the slot and create
        meta in one ``MULTI``.  If another start changes either key in
        between, ``EXEC`` aborts and the read repeats.  So the slot and meta
        appear together or not at all, and no start is ever left half made.

        The run's meta, not the slot, marks a retried start: the slot is freed
        when a run ends, but meta outlives it.  A slot that names this run
        while meta is gone (expired or evicted) is taken over; the old
        producer is fenced at its next append or heartbeat.

        Returns:
            ``True`` if this call created the run, ``False`` if the run
            already existed.

        Raises:
            StreamBusy: If the slot names another run.
            StreamStoreError: If the keys kept changing between attempts.
        """
        settings = get_settings()
        client = self._client()
        watched = [keys.active, keys.meta] if exclusive else [keys.meta]
        for attempt in range(_WRITE_ATTEMPTS):
            try:
                async with client.pipeline(transaction=True) as pipe:
                    await pipe.watch(*watched)
                    # Read on another connection: EXEC still aborts if either
                    # key changes after the WATCH, whoever changes it.
                    read = client.pipeline(transaction=True)
                    read.hmget(keys.meta, [M_STATUS, M_WRITER])
                    read.get(keys.active)
                    (status, writer), holder = await read.execute()
                    if _text(writer) == writer_id:
                        return True  # this call's EXEC applied; its reply was lost
                    if status is not None:
                        return False
                    holder = _text(holder)
                    if exclusive and holder is not None and holder != stream_id:
                        record_stream_operation(operation="open", result="busy")
                        raise StreamBusy(holder)
                    pipe.multi()  # type: ignore[no-untyped-call]
                    active_ms = _ms(settings.stream_active_ttl_seconds)
                    if exclusive:
                        pipe.set(keys.active, stream_id, px=active_ms)
                    pipe.hset(
                        keys.meta,
                        mapping={M_STATUS: STATUS_RUNNING, M_WRITER: writer_id},
                    )
                    pipe.pexpire(
                        keys.meta,
                        active_ms + _ms(settings.stream_gone_ttl_seconds),
                    )
                    _set_alive(pipe, keys, _ms(settings.stream_alive_ttl_seconds))
                    await pipe.execute()
                    return True
            except WatchError:
                continue  # another start changed the keys: read them again
            except _AMBIGUOUS_ERRORS:
                if attempt == _WRITE_ATTEMPTS - 1:
                    raise
                await asyncio.sleep(0.05 * 2**attempt)
        raise StreamStoreError("the run kept changing while it started; retry")

    def _handle(self, scope: str, stream_id: str, exclusive: bool) -> StreamWriter:
        """A writer for a run that already exists, returned on a retried start.

        It holds no writer ID of its own, so an append through it is fenced:
        the run's own producer keeps writing.
        """
        handle = StreamWriter(
            self,
            scope=scope,
            stream_id=stream_id,
            writer_id=_new_writer_id(),
            exclusive=exclusive,
            created=False,
            started=True,
        )
        handle._read_only = True
        return handle

    async def attach(self, stream_id: str, *, scope: str) -> StreamWriter:
        """Continue a run from another process of the same application.

        The new writer continues the sequence numbers and takes over: the old
        writer is fenced, and stops at its next append or heartbeat.

        Raises:
            StreamInvalidName: If *scope* or *stream_id* is invalid.
            StreamNotFound: If there is no such stream.
            StreamFinished: If the stream already has its final entry.
        """
        await self._require_support()
        check_name(scope, stream_id)
        keys = _keys(scope, stream_id)
        client = self._client()
        try:
            pipe = client.pipeline(transaction=True)
            pipe.hgetall(keys.meta)
            pipe.xrevrange(keys.stream, count=1)
            pipe.get(keys.active)
            meta_raw, last, holder = await pipe.execute()
            meta = {_text(k): _text(v) for k, v in (meta_raw or {}).items()}
            if M_STATUS not in meta:
                raise StreamNotFound(f"no stream {stream_id} in this scope")
            if meta.get(M_STATUS) == STATUS_FINISHED:
                raise StreamFinished(f"stream {stream_id} already ended")
            seq = 0
            if last:
                fields = {_text(k): _text(v) for k, v in last[0][1].items()}
                seq = int(fields.get(F_SEQ) or 0)
            writer_id = _new_writer_id()
            await self._set_meta(keys, {M_WRITER: writer_id})
        except STORE_ERRORS as exc:
            raise StreamStoreError("could not attach to the stream") from exc
        record_stream_operation(operation="attach", result="ok")
        return StreamWriter(
            self,
            scope=scope,
            stream_id=stream_id,
            writer_id=writer_id,
            exclusive=_text(holder) == stream_id,
            created=False,
            seq=seq,
            started=bool(last) or meta.get(M_STARTED) == "1",
        )

    def spawn(
        self,
        writer: StreamWriter,
        fn: Callable[[StreamWriter], Awaitable[None]],
    ) -> asyncio.Task[None]:
        """Run *fn* as a task the backend owns, outside the request.

        A disconnect never stops it.  When *fn* returns, raises or is
        cancelled without having called :meth:`StreamWriter.finish`, the task
        writes a final ``stream.end`` event whose data gives the reason:
        ``{"reason": "done"}``, ``{"reason": "error", "code": ...}`` or
        ``{"reason": "cancelled"}``.  The heartbeat runs around *fn*.  On
        application shutdown the remaining tasks are cancelled.

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
            if writer.finished or writer._fenced:
                return
            try:
                await writer._end(END_TYPE, data)
            except StreamFenced:
                pass  # another writer owns the stream now, and ends it
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
            except StreamFenced:
                return  # another writer owns the stream now
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

    async def cancel(self, stream_id: str, *, scope: str) -> bool:
        """Ask the run to stop.

        Sets the cancel flag.  The producer sees it on its next append or
        heartbeat; a producer started by :meth:`spawn` in this process is
        cancelled at once.

        Returns:
            ``True`` if the run was still going, ``False`` if it had ended.

        Raises:
            StreamInvalidName: If *scope* or *stream_id* is invalid.
            StreamNotFound: If there is no such stream in this scope.
        """
        await self._require_support()
        check_name(scope, stream_id)
        keys = _keys(scope, stream_id)
        try:
            status = _text(await self._client().hget(keys.meta, M_STATUS))
            if status is None:
                raise StreamNotFound(f"no stream {stream_id} in this scope")
            if status == STATUS_FINISHED:
                return False
            await self._set_meta(keys, {M_CANCEL: "1"})
        except STORE_ERRORS as exc:
            raise StreamStoreError("could not cancel the stream") from exc
        task = self._spawned.get((scope, stream_id))
        if task is not None and not task.done():
            self._cancel_unless_ending({task})
        record_stream_operation(operation="cancel", result="ok")
        return True

    async def active(self, scope: str) -> str | None:
        """Return the ID of the scope's active run, or ``None``.

        Raises:
            StreamInvalidName: If *scope* is invalid.
        """
        await self._require_support()
        check_name(scope)
        try:
            return _text(await self._client().get(_active_key(scope)))
        except STORE_ERRORS as exc:
            raise StreamStoreError("could not read the active run") from exc

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
        *after* is already at or past the final event.  It holds nothing in
        Redis until iterated, and closes on ``async with`` exit or
        :meth:`StreamReader.aclose`.

        Unlike :meth:`sse` it takes no reader slot, so
        ``stream_max_readers`` does not count it.  Bound such readers yourself.

        Raises:
            StreamInvalidName: *scope* or *stream_id* is invalid.
            StreamNotFound: Unknown stream, or one from another scope.
            StreamExpired: The stream expired, or entries after *after* were
                trimmed.
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

        Returns ``None`` when the cursor is at or past the final entry.  Runs
        as one transaction, so the checks and the first page are one snapshot
        and no trim can fall between them.
        """
        check_name(scope, stream_id)
        cursor = parse_cursor(after) if after is not None else _START
        keys = _keys(scope, stream_id)
        page = get_settings().stream_replay_page

        def build(pipe: Any) -> None:
            pipe.hgetall(keys.meta)
            pipe.exists(keys.stream)
            pipe.xinfo_stream(keys.stream)
            if cursor != _START:
                pipe.xrange(keys.stream, _format(cursor), _format(cursor))
            # XRANGE with an exclusive start, not XREAD: redis-py cannot
            # queue XREAD, whose keys move, in an async Cluster transaction.
            pipe.xrange(keys.stream, f"({_format(cursor)}", "+", count=page)

        try:
            pipe = self._client().pipeline(transaction=True)
            build(pipe)
            replies = list(await pipe.execute(raise_on_error=False))
        except STORE_ERRORS as exc:
            raise StreamStoreError("could not read the stream") from exc
        for reply in (replies[0], replies[1]):
            if isinstance(reply, Exception):
                raise StreamStoreError("could not read the stream") from reply

        meta = {_text(k): _text(v) for k, v in (replies[0] or {}).items()}
        if M_STATUS not in meta:
            record_stream_operation(operation="read", result="not_found")
            raise StreamNotFound(f"no stream {stream_id} in this scope")
        exists = bool(replies[1])
        if meta.get(M_STARTED) == "1" and not exists:
            record_stream_operation(operation="read", result="expired")
            raise StreamExpired(f"stream {stream_id} has expired")

        info = replies[2] if not isinstance(replies[2], Exception) else None
        last_generated = _info_id(info, "last-generated-id")
        if cursor != _START and (last_generated is None or cursor > last_generated):
            record_stream_operation(operation="read", result="ahead")
            raise StreamCursorAhead(f"cursor {_format(cursor)} is ahead of the stream")
        own = replies[3] if cursor != _START else None
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
        if (
            meta.get(M_STATUS) == STATUS_FINISHED
            and len(first) < page
            and not any(_to_entry(i, f)[0].final for i, f in first)
        ):
            # The run ended, and everything after the cursor fits in one page
            # with no final entry in it: the cursor is past the final entry,
            # where at most a stale writer's one entry can follow.
            record_stream_operation(operation="read", result="finished")
            return None

        last_seq: int | None = 0 if cursor == _START else None
        if own_fields:
            seq_text = own_fields.get(F_SEQ)
            last_seq = int(seq_text) if seq_text else None
        record_stream_operation(
            operation="read", result="started" if cursor == _START else "resumed"
        )
        return _ReadPlan(
            keys=keys,
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
        """
        settings = get_settings()
        heartbeat_ms = _ms(settings.stream_heartbeat_seconds)
        cursor = plan.cursor
        last_seq = plan.last_seq
        pending = plan.first
        failing_since: float | None = None
        while True:
            if pending:
                page: list[_Entry] = []
                for raw_id, raw_fields in pending:
                    entry, seq = _to_entry(raw_id, raw_fields)
                    if seq is not None:
                        if last_seq is not None and seq > last_seq + 1:
                            record_stream_operation(operation="gap", result="ok")
                            page.append(
                                _Entry.own(GAP_TYPE, {"missing": seq - last_seq - 1})
                            )
                        last_seq = seq
                    page.append(entry)
                    if entry.final:
                        yield page
                        return
                cursor = parse_cursor(page[-1].id or "0-0")
                yield page
                pending = []
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
                    {plan.keys.stream: _format(cursor)},
                    count=settings.stream_replay_page,
                    block=max(1, block_ms),  # BLOCK 0 would wait forever
                )
                failing_since = None
            except STORE_ERRORS:
                now = time.monotonic()
                failing_since = failing_since or now
                if now - failing_since > settings.stream_read_retry_seconds:
                    raise StreamStoreError("Redis stayed unreachable") from None
                logger.debug("Stream read failed; retrying", exc_info=True)
                await asyncio.sleep(min(1.0, block_ms / 1000))
                yield []
                continue
            pending = _xread_entries(reply)
            if pending:
                continue
            if block_ms < heartbeat_ms:
                yield []  # cut short by the deadline, not a quiet heartbeat
                continue
            state = await self._check_producer(plan)
            if state == "gone":
                return
            if state == "lost_read_only":
                yield [_Entry.own(ERROR_TYPE, {"code": "producer_lost"})]
                return
            if state == "ended":
                continue  # the next read returns the final entry
            yield []

    def _event(self, entry: _Entry) -> StreamEvent:
        data = entry.data if entry.id is None else self._decode(entry.raw)
        return StreamEvent(entry.id, entry.type, data, entry.final)

    async def _check_producer(self, plan: _ReadPlan) -> str:
        """On a quiet heartbeat: is the producer alive, and does the stream exist?

        A producer whose heartbeat lapsed with no final entry will never end
        the stream, so the reader writes the final entry it never wrote.
        """
        try:
            status, started, alive = await self._client().hmget(
                plan.keys.meta, [M_STATUS, M_STARTED, M_ALIVE]
            )
        except STORE_ERRORS:
            return "quiet"
        status = _text(status)
        if status is None:
            return "gone"  # meta expired or evicted mid-read
        if status == STATUS_FINISHED or alive:
            return "quiet"
        try:
            final_id = await self._end_lost(plan, started=_text(started) == "1")
        except NoPermissionError:
            return "lost_read_only"
        except STORE_ERRORS:
            logger.debug("Could not end a lost producer's stream", exc_info=True)
            return "quiet"
        # No final entry means the stream itself is gone: stop, and the
        # reconnect gets 410.
        return "ended" if final_id is not None else "gone"

    async def _end_lost(self, plan: _ReadPlan, *, started: bool) -> str | None:
        """Write the final entry a lost producer never wrote, and return its ID.

        The same ``XADD`` and the same transaction as a writer's final entry.
        Once the run has started, ``NOMKSTREAM`` keeps an expired stream
        expired, so a late reconnect still gets 410.  A run that never
        appended has no stream yet, and gets one holding only the end.
        Returns ``None`` when the stream is gone.
        """
        settings = get_settings()
        keys = plan.keys
        payload = self._encode({"reason": "producer_lost"})
        args = _xadd(
            keys,
            END_TYPE,
            payload,
            idmp=(_END_PRODUCER, _END_ID),
            seq=None,
            final=True,
            nomkstream=started,
        )
        pipe = self._client().pipeline(transaction=True)
        _build_write(
            pipe,
            args,
            keys,
            plan.stream_id,
            _ms(settings.stream_done_ttl_seconds),
            final=True,
            started=started,
            exclusive=False,
        )
        replies = await pipe.execute()
        final_id = _text(replies[0])
        if final_id is None:
            return None
        record_stream_operation(operation="producer_lost", result="ok")
        return final_id

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
        heartbeat to see ``producer_lost`` instead of the real reason.
        """
        for task in tasks:
            writer = self._writers.get(task)
            if writer is None or not writer._ending:
                task.cancel()


def _new_writer_id() -> str:
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
        return False  # no stream yet, so nothing was removed
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
