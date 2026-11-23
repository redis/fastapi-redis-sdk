"""Redis-backed session store.

One hash for each session, and no other key::

    rf:s:<sid>     key TTL = idle
      s:created    <unix seconds>   field TTL = absolute
      s:seen       <unix seconds>   last access
      s:sub        <subject>        absent for an anonymous session
      s:desc       <descriptor>     absent with no descriptor_of
      a:<path>     <value>          one field for each leaf of the session

Each clock has its own TTL.  This module sets both TTLs and Redis enforces
them: no code here reads a timestamp to decide whether a session has
expired.  The idle clock is the key's TTL, and every pipeline that writes to
the key restarts it.  The absolute clock is the TTL of ``s:created``, which
is written once and never again - ``HSET`` removes a field's TTL, so writing
it again would remove the deadline.  A hash without ``s:created`` is not a
session.

``session_layout`` holds how a session maps to fields, and ``session_search``
holds the queries.  Neither talks to Redis.
"""

from __future__ import annotations

import contextlib
import json
import logging
import secrets
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator, Mapping, MutableMapping
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum, auto
from typing import Any, Protocol, runtime_checkable

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.exceptions import RedisClusterException, RedisError

from redis_fastapi.config import KEY_SEGMENTS, get_settings
from redis_fastapi.exceptions import (
    SessionConfigurationError,
    SessionSearchError,
    SessionSearchTimeoutError,
    SessionSearchUnavailableError,
    SessionStoreError,
)
from redis_fastapi.session_layout import (
    ATTR_PREFIX,
    FIELD_CREATED,
    FIELD_DESCRIPTOR,
    FIELD_SEEN,
    FIELD_SUBJECT,
    GEO,
    NUMERIC,
    TAG,
    TEXT,
    check_searchable,
    check_ttls,
    check_types,
    diff,
    fields_of,
    flatten,
    is_finite,
    seal,
    unflatten,
    unseal,
)
from redis_fastapi.session_search import (
    BUILTIN,
    CUSTOM,
    DEFAULT_PAGE,
    LIST_LIMIT,
    PAGE_LIMIT,
    REVOKE_BATCH,
    Filter,
    builtin_fields,
    count_query,
    custom_fields,
    custom_index_name,
    definition,
    error_kind,
    ids_query,
    page_query,
    parse_reply,
)
from redis_fastapi.telemetry import (
    record_session_operation,
    session_span,
    timed_session,
)
from redis_fastapi.types import Coder, Encryptor, JsonCoder

logger = logging.getLogger(__name__)

# Every driver failure this store is willing to interpret.
#
# ``RedisClusterException`` is **not** a subclass of ``RedisError`` - it
# descends straight from ``Exception`` - and its subclass
# ``SlotNotCoveredError`` is raised on the ordinary command path during a
# resharding or a failover.  Catching only ``RedisError`` therefore lets the
# commonest cluster failure escape every policy in this module: the read would
# not fail open, and a raw driver exception would reach application code that
# was told ``SessionError`` catches the whole feature.
STORE_ERRORS: tuple[type[BaseException], ...] = (
    RedisError,
    RedisClusterException,
    OSError,
)

# What a *composite* operation can fail with: a driver error from a primitive
# it calls directly, or this module's own wrapper around one raised deeper
# down.  ``_observe`` counts both as one failed operation.
OBSERVED_ERRORS: tuple[type[BaseException], ...] = (SessionStoreError, *STORE_ERRORS)

# The payload's hash field name.  One character, and identical in every


class Deadline(Enum):
    """What ``_read`` says when the absolute deadline is not a number.

    A backend reports a live deadline as an ``int`` of seconds remaining, and
    anything else as one of these.  The two cases are not interchangeable and
    the base class branches on both, so neither may be smuggled through as a
    negative integer.

    An earlier version passed Redis's own ``TTL`` sentinels - ``-2`` and
    ``-1`` - straight through, which quietly made "reproduce this Redis
    encoding" part of the contract every other backend had to satisfy, without
    the ``_read`` docstring ever saying so.
    """

    ABSENT = auto()
    """No key at all.  The session does not exist."""

    UNBOUNDED = auto()
    """The key exists with no expiry.  A create always sets one, so this is a
    key recreated by a write that landed after the session ended - a save
    racing the deadline or a revocation - or one written by something else.
    Either way the session is over."""


# Redis's own ``TTL`` answers, mapped to the above by ``_ttl``.
_TTL_NO_KEY = -2
_TTL_NO_EXPIRY = -1

# Characters a session ID may contain: the alphabet of ``secrets.token_urlsafe``.
_ID_ALPHABET = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
)
# 128 bits is the OWASP recommendation for a custom identifier; 32 bytes gives
# 256 and costs nothing.
_ID_BYTES = 32
_ID_MIN_LENGTH = 22

# The default for ``rotate(subject=...)``: keep the session's current subject.
# ``None`` cannot mean that, because ``None`` is how the middleware says the
# session is now anonymous.
_UNSET: Any = object()


def _seconds(value: int | timedelta) -> int:
    """Normalise a TTL setting to whole seconds.

    Accepts ``timedelta`` because ``cache()`` already does, and other session
    libraries accept it too, which eases migration.
    """
    if isinstance(value, timedelta):
        return int(value.total_seconds())
    return int(value)


@dataclass(frozen=True)
class SessionMetadata:
    """Timestamps kept beside the payload, never mixed into it.

    Some session libraries store their metadata under a ``__metadata__`` key
    *inside* the session, so it shows up in the application's own
    ``request.session``.  We keep it in fields of its own, ``s:created`` and
    ``s:seen``, beside the fields that hold the session.
    A third field, ``lifetime``, is not stored: it is the store's own
    ``absolute_seconds``, the same for every session.
    """

    created: float
    last_access: float


@dataclass(frozen=True)
class SessionRecord:
    """A session as the application sees it: the payload and its metadata."""

    data: dict[str, Any]
    metadata: SessionMetadata


@dataclass(frozen=True)
class SessionInfo:
    """One row of a "you are signed in on these devices" listing.

    Built from the index alone, so listing a subject's sessions never reads the
    session records themselves.
    """

    session_id: str
    created: float
    last_access: float
    descriptor: dict[str, Any]


@dataclass
class _StoreCapabilities:
    """Process-lifetime cache of server capability detection.

    Shared by every per-request store, in the same way
    ``_BackendCapabilities`` is shared by the rate limiter, so the question is
    asked once per pool rather than once per request.  ``None`` means "not yet
    known" and leaves detection to a later attempt rather than caching a guess.

    Attributes:
        supports_hsetex: Whether the server has ``HSETEX``, which arrived in
            Redis 8.0.  The load uses its ``FXX`` form to record the last
            access without creating a key for a cookie that names no session.
        supports_search: Whether the server has the Query Engine.  Without it
            no session index exists, and each method that reads one raises.
        ready_indexes: The index names this process has already ensured.
        warned_no_search: The "session indexing is unavailable" warning went
            out.  It is logged once for a pool, not once for a request.
        warned_timeout_policy: The same, for the warning about a server that
            answers a timed-out query with a partial result.
    """

    supports_hsetex: bool | None = None
    supports_search: bool | None = None
    ready_indexes: set[str] = field(default_factory=set)
    warned_no_search: bool = False
    warned_timeout_policy: bool = False


async def _probe_command(
    client: AsyncRedis | AsyncRedisCluster, command: str
) -> bool | None:
    try:
        if isinstance(client, AsyncRedisCluster):
            reply = await client.execute_command(
                "COMMAND", "INFO", command, target_nodes=AsyncRedisCluster.RANDOM
            )
        else:
            reply = await client.execute_command(  # type: ignore[no-untyped-call]
                "COMMAND", "INFO", command
            )
    except TypeError:
        # redis-py parses the COMMAND reply eagerly and indexes into a nil
        # entry for a command the server does not know.  That is the
        # "unsupported" answer, not a broken probe.
        return False
    except STORE_ERRORS:
        return None
    if not reply:
        return False
    if isinstance(reply, Mapping):
        return any(value is not None for value in reply.values())
    first = reply[0] if isinstance(reply, (list, tuple)) else reply
    return first is not None


async def probe_hsetex_support(
    client: AsyncRedis | AsyncRedisCluster,
) -> bool | None:
    """Ask the server whether it implements ``HSETEX``.

    Returns ``True`` when the server advertises the command, ``False`` when it
    does not, and ``None`` when the question could not be answered, which
    leaves detection to a later attempt.

    This mirrors :func:`~redis_fastapi.ratelimit_backend.probe_increx_support`
    deliberately, including the reason it asks ``COMMAND INFO`` rather than
    sending the command and reading the error: on a cluster, redis-py builds
    its command map from the server's own ``COMMAND`` table and rejects an
    unknown command client-side, so error text is not a usable signal.
    """
    return await _probe_command(client, "HSETEX")


async def probe_search_support(
    client: AsyncRedis | AsyncRedisCluster,
) -> bool | None:
    """Ask the server whether it has the Query Engine.

    The same three answers as :func:`probe_hsetex_support`, from
    ``COMMAND INFO FT.SEARCH``.  The Query Engine is part of Redis from 8.0;
    Redis 7.4 open source and some managed services do not have it.
    """
    return await _probe_command(client, "FT.SEARCH")


@dataclass
class SessionState:
    """Everything about one session that is not its data.

    The store reads and writes this; the middleware owns it and puts it on
    ``request.state``.  Keeping it separate is what lets ``Session`` go back to
    being a ``dict`` with a few flags - Starlette's contract and little more -
    and it is why this module imports nothing from ``sessions``.

    The type of ``data`` is a plain ``MutableMapping`` on purpose: the store
    has no business knowing about a web session object, and a second carrier
    (an agent or MCP session, say) can reuse the store without one.

    Attributes:
        data: The payload the application sees.
        session_id: Where it is stored, or ``None`` before the first write.
        subject: What it is indexed under.  Captured at load time, because by
            the time ``revoke`` runs the payload it would be derived from is
            already gone.
        created: The original creation time.
        absolute_remaining: What the server says is left of the deadline.
        fields: The stored fields as the last load or write left them, in
            plaintext.  A save compares the session with this and writes only
            the difference.
        revoked: The store ended this session; the middleware owes a clearing
            cookie.
        rotated: A handler asked for a rotation the middleware still owes.
    """

    data: MutableMapping[str, Any]
    session_id: str | None = None
    subject: str | None = None
    created: float | None = None
    absolute_remaining: int | None = None
    fields: dict[str, str] | None = None
    revoked: bool = False
    rotated: bool = False


@dataclass(frozen=True)
class LoadedSession:
    """A live session, plus the deadline Redis is counting down for it.

    ``absolute_remaining`` comes from ``HTTL`` on ``s:created`` - the server's
    own number, never one this process computed.  The cookie's ``max-age`` is
    sized from it, so the cookie cannot expire before the record.

    ``fields`` is what the hash held, in plaintext.  The middleware keeps it
    on the session state so the next save has something to compare with.
    """

    record: SessionRecord
    absolute_remaining: int
    fields: dict[str, str] = field(default_factory=dict)


@runtime_checkable
class SessionStoreProtocol(Protocol):
    """The surface the middleware and the dependencies actually call.

    Much smaller than :class:`SessionStore`, and structural rather than
    inherited, so a test double or another package can supply a store without
    subclassing anything.  :class:`SessionStore` satisfies it by construction.

    Implement this when you want to *substitute* a store; inherit
    :class:`SessionStore` when you want to *write* one, because the base class
    already owns the lifecycle rules that are a security control.

    ``create`` and ``save`` return the fields they left in the store.  The
    middleware passes that back as *baseline* on the next save.  *ttls* maps
    the name of an attribute to the TTL the application gave it, or to
    ``None`` when the application asked for the TTL to be removed.
    """

    @property
    def idle_seconds(self) -> int: ...  # pragma: no cover

    @property
    def absolute_seconds(self) -> int: ...  # pragma: no cover

    def is_valid_id(self, value: str) -> bool: ...  # pragma: no cover

    def new_id(self) -> str: ...  # pragma: no cover

    def new_record(
        self,
        data: dict[str, Any],
        *,
        created: float | None = ...,
    ) -> SessionRecord: ...  # pragma: no cover

    def session_id(self, state: SessionState) -> str | None: ...  # pragma: no cover

    async def load(
        self, session_id: str, *, refresh: bool = ...
    ) -> LoadedSession | None: ...  # pragma: no cover

    async def create(
        self,
        session_id: str,
        record: SessionRecord,
        *,
        subject: str | None = ...,
        descriptor: Mapping[str, Any] | None = ...,
        ttls: Mapping[Any, int | None] | None = ...,
    ) -> dict[str, str]: ...  # pragma: no cover

    async def save(
        self,
        session_id: str,
        record: SessionRecord,
        *,
        baseline: Mapping[str, str] | None = ...,
        subject: str | None = ...,
        descriptor: Mapping[str, Any] | None = ...,
        ttls: Mapping[Any, int | None] | None = ...,
    ) -> dict[str, str]: ...  # pragma: no cover

    async def touch(self, session_id: str) -> None: ...  # pragma: no cover

    async def delete(self, session_id: str) -> None: ...  # pragma: no cover

    async def rotate(
        self,
        state: SessionState,
        *,
        subject: str | None = ...,
        descriptor: Mapping[str, Any] | None = ...,
        ttls: Mapping[Any, int | None] | None = ...,
    ) -> str: ...  # pragma: no cover

    async def revoke(
        self, state: SessionState, *, subject: str | None = ...
    ) -> None: ...  # pragma: no cover


class SessionStore(ABC):
    """Owns the session lifecycle; a subclass owns only the storage.

    The lifecycle is a security control, so it is written once here rather
    than once per backend.  A new backend implements the abstract primitives
    at the bottom of this class and inherits the identifier rules, the
    two-clock policy, the field layout and the error handling.

    **The underscore on those primitives means "applications never call this",
    not "do not override this".**  The prefix is what separates policy from
    mechanism - ``delete()`` applies the failure policy, ``_delete()`` removes
    the key - and the two could not share a name.  Overriding them is
    supported and is how this class is meant to be extended; it is simply not
    a documented, supported product surface, so the published guide does not
    describe it and no compatibility promise attaches to it.

    This is an abstract base class, not a bare protocol, so subclasses share
    the field layout and key handling.  ``SessionStoreProtocol`` describes the much
    smaller surface the middleware calls, for a caller who wants no
    inheritance.
    """

    def __init__(
        self,
        *,
        coder: type[Coder] = JsonCoder,
        encryptor: Encryptor | None = None,
        idle_ttl: int | timedelta | None = None,
        absolute_ttl: int | timedelta | None = None,
        gc_ttl: int | timedelta | None = None,
        id_factory: Callable[[], str] | None = None,
        searchable: Mapping[str, str] | None = None,
    ) -> None:
        settings = get_settings()
        # Accepted so that a caller written against the earlier store still
        # constructs one. Each field holds JSON text, so the coder has no
        # part in how a session is stored.
        self._coder = coder
        self._encryptor = encryptor
        self._searchable = check_searchable(searchable or {})
        if self._searchable and encryptor is not None:
            raise SessionConfigurationError(
                "searchable cannot be combined with an encryptor: the Query "
                "Engine cannot search an encrypted value."
            )
        self._geo = frozenset(
            path for path, kind in self._searchable.items() if kind == GEO
        )
        self._id_factory = id_factory
        self._idle_ttl = _seconds(
            idle_ttl if idle_ttl is not None else settings.session_idle_ttl
        )
        self._absolute_ttl = _seconds(
            absolute_ttl if absolute_ttl is not None else settings.session_absolute_ttl
        )
        self._gc_ttl = _seconds(
            gc_ttl if gc_ttl is not None else settings.session_gc_ttl
        )
        for name, value in (
            ("session_idle_ttl", self._idle_ttl),
            ("session_absolute_ttl", self._absolute_ttl),
            ("session_gc_ttl", self._gc_ttl),
        ):
            if value < 0:
                raise SessionConfigurationError(
                    f"{name} must not be negative, got {value}"
                )
        if self._gc_ttl <= 0:
            raise SessionConfigurationError(
                "session_gc_ttl must be positive: it is the backstop that lets "
                "Redis collect a key whose real deadline is unknown, and a "
                "session must never be left without any expiry at all."
            )

    # -- the two clocks ------------------------------------------------------

    @property
    def idle_seconds(self) -> int:
        """TTL for the session key, restarted by every load and every write.

        ``session_idle_ttl=0`` disables the idle clock.  The key then takes
        the absolute length, so every command still sends a positive TTL and
        no read can end a session the idle clock was told to leave alone.
        The deadline on ``s:created`` still ends the session on time.
        ``gc_ttl`` here would act as a hidden idle clock whenever
        ``absolute_ttl`` is longer than it.
        """
        return self._idle_ttl or self.absolute_seconds

    @property
    def absolute_seconds(self) -> int:
        """TTL for ``s:created``, set once at creation and never refreshed.

        ``session_absolute_ttl=0`` disables the absolute clock, and the field
        then falls back to ``gc_ttl``.  The load reads a hash whose
        ``s:created`` has no TTL as a dead session, so the field must always
        carry one.
        """
        return self._absolute_ttl or self._gc_ttl

    # -- identifiers ---------------------------------------------------------

    @staticmethod
    def is_valid_id(value: str) -> bool:
        """Whether *value* is shaped like an identifier this store issues.

        Applied to the incoming cookie as well as to whatever ``id_factory``
        returns.  On the cookie this is not cosmetic: the value is written back
        into a ``Set-Cookie`` header, so an unvalidated one is a
        header-injection vector.
        """
        return (
            isinstance(value, str)
            and len(value) >= _ID_MIN_LENGTH
            and not (set(value) - _ID_ALPHABET)
        )

    def new_id(self) -> str:
        """Generate an identifier, and validate it before returning it.

        A seam that supplies a security-critical value has to be checked, not
        trusted.  A factory returning an identifier with a separator in it once
        produced a key that collided with the index key; the prefixes now make
        that collision structurally impossible, but a short or oddly-charactered
        identifier is still a defect this package refuses rather than stores.

        Raises:
            SessionConfigurationError: If a supplied ``id_factory`` returns a
                value that is too short or uses characters outside
                ``[A-Za-z0-9_-]``.
        """
        if self._id_factory is None:
            return secrets.token_urlsafe(_ID_BYTES)
        value: str = self._id_factory()
        if not self.is_valid_id(value):
            raise SessionConfigurationError(
                "id_factory returned an unusable session ID. It must be at "
                f"least {_ID_MIN_LENGTH} characters of [A-Za-z0-9_-]; "
                f"got {value!r}."
            )
        return value

    def new_record(
        self, data: dict[str, Any], *, created: float | None = None
    ) -> SessionRecord:
        """Build a record to write.

        *created* carries the original creation time forward on an update.
        Omitting it stamps "now", which is correct only for a session that
        does not exist yet: leave it out on a create, pass the loaded value on
        every subsequent write.  Without it ``created`` silently becomes "time
        of last write", and a device listing reports every active session as
        having been signed in seconds ago.
        """
        now = time.time()
        return SessionRecord(
            data=data,
            metadata=SessionMetadata(
                created=created if created is not None else now,
                last_access=now,
            ),
        )

    # -- instrumentation ------------------------------------------------------

    @contextlib.contextmanager
    def _observe(self, operation: str) -> Generator[None, None, None]:
        """Span, latency and the ``error`` count for one store operation.

        The outcome count is the caller's, because only the caller knows
        whether nothing-to-do is a ``miss`` or a ``hit``.  Failure is not:
        every operation that raises counts the same way, and doing it here is
        what keeps the ``error`` series from depending on someone remembering
        an ``except`` arm.

        A composite operation records **twice** on failure, once for itself
        and once for the inner operation that failed - a rotation whose
        ``create`` fails is both a failed create and a failed rotation, and a
        dashboard wants to see both.  The spans nest for the same reason.
        """
        with session_span(f"session.{operation}"), timed_session(operation):
            try:
                yield
            except OBSERVED_ERRORS:
                record_session_operation(operation=operation, result="error")
                raise

    # -- fields ---------------------------------------------------------------

    def _fields_for(
        self,
        record: SessionRecord,
        subject: str | None,
        descriptor: Mapping[str, Any] | None,
        *,
        created: bool,
    ) -> dict[str, str]:
        """The plaintext fields a session should hold.

        ``s:created`` is in the result only for a create.  A save must never
        write it: ``HSET`` removes a field's TTL, and that TTL is the absolute
        deadline.
        """
        try:
            fields = flatten(record.data, geo=self._geo)
            if descriptor:
                fields[FIELD_DESCRIPTOR] = json.dumps(
                    dict(descriptor), ensure_ascii=False
                )
        except (TypeError, ValueError) as exc:
            raise SessionStoreError(f"Could not serialize the session: {exc}") from exc
        if subject:
            fields[FIELD_SUBJECT] = json.dumps(str(subject), ensure_ascii=False)
        fields[FIELD_SEEN] = repr(record.metadata.last_access)
        if created:
            fields[FIELD_CREATED] = repr(record.metadata.created)
        return fields

    def _check(
        self, data: Mapping[Any, Any], ttls: Mapping[Any, int | None] | None
    ) -> None:
        """Refuse a write the index could not hold, before anything is sent."""
        if not self._searchable:
            return
        check_types(data, self._searchable)
        if ttls:
            check_ttls([name for name, ttl in ttls.items() if ttl], self._searchable)

    @staticmethod
    def _field_ttls(
        fields: Mapping[str, str], ttls: Mapping[Any, int | None] | None
    ) -> tuple[dict[str, int], list[str]]:
        """Turn per-attribute TTLs into per-field ones: ``(expire, persist)``.

        A TTL covers each field of the attribute, changed or not.
        """
        expire: dict[str, int] = {}
        persist: list[str] = []
        for name, ttl in (ttls or {}).items():
            for field_name in fields_of(fields, name):
                if ttl:
                    expire[field_name] = int(ttl)
                else:
                    persist.append(field_name)
        return expire, persist

    def _sealed(self, fields: Mapping[str, str]) -> dict[str, str]:
        """Encrypt the value of each attribute field; leave system fields."""
        if self._encryptor is None:
            return dict(fields)
        return {
            name: seal(text, self._encryptor) if name.startswith(ATTR_PREFIX) else text
            for name, text in fields.items()
        }

    def _opened(self, stored: Mapping[str, str]) -> dict[str, str]:
        """The reverse of :meth:`_sealed`."""
        if self._encryptor is None:
            return dict(stored)
        return {
            name: unseal(raw, self._encryptor) if name.startswith(ATTR_PREFIX) else raw
            for name, raw in stored.items()
        }

    # -- lifecycle -----------------------------------------------------------

    async def load(
        self, session_id: str, *, refresh: bool = True
    ) -> LoadedSession | None:
        """Read a session, and restart its idle clock in the same round trip.

        See :meth:`load_with_status` for the contract; this is the same call
        without the second answer.
        """
        loaded, _ = await self.load_with_status(session_id, refresh=refresh)
        return loaded

    async def load_with_status(
        self, session_id: str, *, refresh: bool = True
    ) -> tuple[LoadedSession | None, bool]:
        """:meth:`load`, plus whether the read itself failed.

        The second value is ``True`` only when Redis could not be read, or the
        record could not be decoded, and ``session_fail_closed`` let the request
        continue.  Without it, an
        outage looks exactly like an unknown identifier, and
        ``valid_session()`` would send users to sign in when it should say
        "try again".

        The first value is ``None`` for every way a session can fail to
        exist: never created, idle-expired, past its absolute deadline, or
        revoked.  The caller cannot tell those apart, and must not: to an
        application they are all "no session".

        *refresh* false makes this a plain read, for
        ``session_refresh_on_load=False``.  The idle clock then advances only
        when the response writes.

        **A hash without ``s:created`` is not a session.**  That one rule
        covers a session past its deadline, whose ``s:created`` expired, a
        hash a late save recreated after the session ended, and a record in
        an older layout.  Anything of the kind is deleted on the way past.

        Raises:
            SessionStoreError: If the read failed, or the record exists but
                cannot be decoded, and ``session_fail_closed`` is set.
        """
        if not self.is_valid_id(session_id):
            return None, False
        with session_span("session.load"), timed_session("load"):
            try:
                stored, deadline = await self._read(
                    session_id,
                    refresh_idle=self.idle_seconds if refresh else None,
                    seen=time.time() if refresh else None,
                )
            except STORE_ERRORS as exc:
                record_session_operation(operation="load", result="error")
                self._read_failed(exc)
                return None, True

        alive_until = deadline if isinstance(deadline, int) else 0
        if FIELD_CREATED not in stored or alive_until <= 0:
            if stored:
                await self._safe_delete(session_id)
            record_session_operation(
                operation="load", result="expired" if stored else "miss"
            )
            return None, False

        try:
            fields = self._opened(stored)
            data = unflatten(fields, geo=self._geo)
            created = float(fields[FIELD_CREATED])
            seen = float(fields.get(FIELD_SEEN, created))
        except (SessionStoreError, ValueError) as exc:
            # Handled like a failed read, and the record is kept: when every
            # record fails at once - an encryption key rotated or deployed by
            # mistake - deleting them would make the mistake irreversible.
            record_session_operation(operation="load", result="error")
            self._read_failed(exc)
            return None, True

        record_session_operation(operation="load", result="hit")
        record = SessionRecord(
            data=data, metadata=SessionMetadata(created=created, last_access=seen)
        )
        return (
            LoadedSession(record=record, absolute_remaining=alive_until, fields=fields),
            False,
        )

    async def create(
        self,
        session_id: str,
        record: SessionRecord,
        *,
        subject: str | None = None,
        descriptor: Mapping[str, Any] | None = None,
        ttls: Mapping[Any, int | None] | None = None,
    ) -> dict[str, str]:
        """Write a session that does not exist yet, starting both clocks.

        The only method that ever writes ``s:created``, and so the only one
        that sets the absolute deadline.  Use it for a first write and for
        the new half of a rotation; use :meth:`save` for every later write.

        Returns:
            The fields the session now holds, in plaintext.  Pass them to the
            next :meth:`save` as *baseline*.

        Raises:
            SessionAttributeTypeError: If a searchable attribute holds a value
                of another type.  Nothing is written.
            SessionStoreError: On any store failure.
        """
        self._check(record.data, ttls)
        fields = self._fields_for(record, subject, descriptor, created=True)
        expire, _ = self._field_ttls(fields, ttls)
        with session_span("session.create"), timed_session("create"):
            try:
                await self._create(
                    session_id,
                    self._sealed(fields),
                    idle=self.idle_seconds,
                    absolute=self.absolute_seconds,
                    field_ttls=expire,
                )
            except STORE_ERRORS as exc:
                record_session_operation(operation="create", result="error")
                raise SessionStoreError(f"Could not save session: {exc}") from exc
        record_session_operation(operation="create", result="hit")
        return fields

    async def save(
        self,
        session_id: str,
        record: SessionRecord,
        *,
        baseline: Mapping[str, str] | None = None,
        subject: str | None = None,
        descriptor: Mapping[str, Any] | None = None,
        ttls: Mapping[Any, int | None] | None = None,
    ) -> dict[str, str]:
        """Write what changed in an existing session, and nothing else.

        The store compares the session with *baseline* - the fields the last
        load or write returned - and sends only the difference.  Two rules
        follow from ``HSET`` removing a field's TTL:

        * a field that did not change is not written, so it keeps its TTL;
        * ``s:created`` is never written, so an update cannot move the
          absolute deadline and cannot bring it back after it has passed.

        A write that lands after the session ended recreates the hash with no
        ``s:created``.  The next load deletes it, and until then the key's
        TTL is the idle timeout.

        With no *baseline* every field is written and none is removed.

        Returns:
            The fields the session now holds, in plaintext.

        Raises:
            SessionAttributeTypeError: If a searchable attribute holds a value
                of another type.  Nothing is written.
            SessionConfigurationError: If *ttls* gives a TTL to a searchable
                attribute.  Nothing is written.
            SessionStoreError: On any store failure.  A write never fails
                quietly, whatever ``session_fail_closed`` says - losing a login
                or a rotation is the worst outcome in this design.
        """
        self._check(record.data, ttls)
        after = self._fields_for(record, subject, descriptor, created=False)
        before = {
            name: text
            for name, text in (baseline or {}).items()
            if name != FIELD_CREATED
        }
        to_set, to_delete = diff(before, after)
        expire, persist = self._field_ttls(after, ttls)
        with session_span("session.save"), timed_session("save"):
            try:
                await self._apply(
                    session_id,
                    self._sealed(to_set),
                    to_delete,
                    field_ttls=expire,
                    persist=persist,
                    idle=self.idle_seconds,
                )
            except STORE_ERRORS as exc:
                record_session_operation(operation="save", result="error")
                raise SessionStoreError(f"Could not save session: {exc}") from exc
        record_session_operation(operation="save", result="hit")
        if baseline and FIELD_CREATED in baseline:
            after[FIELD_CREATED] = baseline[FIELD_CREATED]
        return after

    async def touch(self, session_id: str) -> None:
        """Restart the idle clock without writing the session.

        Only needed under ``session_refresh_on_load=False``; the default load
        already refreshed in the same round trip as the read.

        A counter and no span: one command is not worth a span, but the count
        is the only way to confirm that ``refresh_on_load=False`` is doing
        anything at all.
        """
        try:
            await self._expire(session_id, self.idle_seconds, seen=time.time())
        except STORE_ERRORS as exc:
            record_session_operation(operation="touch", result="error")
            raise SessionStoreError(f"Could not refresh session: {exc}") from exc
        record_session_operation(operation="touch", result="hit")

    async def delete(self, session_id: str) -> None:
        """Remove a session outright.

        Raises:
            SessionStoreError: On any store failure.  Deletion is revocation,
                so a failure here must reach the caller.
        """
        try:
            await self._delete(session_id)
        except STORE_ERRORS as exc:
            raise SessionStoreError(f"Could not delete session: {exc}") from exc

    # -- rotation and revocation ---------------------------------------------
    def session_age(self, state: SessionState) -> int | None:
        """Seconds since this session's ID was issued, or ``None``.

        A new ID is a new key, and a new key starts its absolute clock - the
        TTL of ``s:created`` - at full length, so the age is that length minus
        what is left.  Both numbers come from Redis, so a skewed container
        clock cannot make an old session look recent.

        ``None`` when the request has no stored session, or when the session
        has more time left than the configured length allows - one created
        before ``session_absolute_ttl`` was lowered.  A caller must treat
        ``None`` as "not recent": that is what keeps a configuration change
        from passing old sessions off as new.
        """
        return issued_ago(self.absolute_seconds, state)

    def session_id(self, state: SessionState) -> str | None:
        """The identifier this session is stored under, if it has one yet.

        ``None`` for a session that has never been written.  Useful for
        marking "this device" in a listing.
        """
        return state.session_id

    async def rotate(
        self,
        state: SessionState,
        *,
        subject: str | None = _UNSET,
        descriptor: Mapping[str, Any] | None = None,
        ttls: Mapping[Any, int | None] | None = None,
    ) -> str:
        """Issue a new identifier for *state*, deleting the old key first.

        This is the defence against session fixation, and the **middleware
        normally drives it** when the principal changes.  It is public for
        the rare handler that must force one - a step-up that leaves no trace
        in session state.

        **The order is the security control, not an implementation detail.**
        The old key goes first, so an interrupted rotation signs the user out
        rather than leaving two identifiers that both work.  Signed out is
        recoverable; two live identifiers after a privilege change is the
        fixation this design exists to prevent.

        The new session starts a fresh absolute clock, which is correct:
        rotation follows authentication or a privilege change, so the deadline
        should run from that moment rather than from whenever the anonymous
        session began.

        A rotation is not a write by the application, so an attribute this
        request did not change keeps the time left on its TTL.  ``HTTL`` answers in whole seconds and
        rounds up, so that TTL can move later by up to one second.

        Omit *subject* to keep the session's current one.  ``None`` makes the
        new session anonymous: no search by subject finds it.

        Returns:
            The new session ID.

        Raises:
            SessionAttributeTypeError: If a searchable attribute holds a value
                of another type.  Nothing is written or deleted.
            SessionStoreError: On any store failure.
        """
        with self._observe("rotate"):
            return await self._rotate(
                state, subject=subject, descriptor=descriptor, ttls=ttls
            )

    async def _rotate(
        self,
        state: SessionState,
        *,
        subject: str | None,
        descriptor: Mapping[str, Any] | None,
        ttls: Mapping[Any, int | None] | None,
    ) -> str:
        """Body of :meth:`rotate`, so the span wraps both trips."""
        if subject is _UNSET:
            subject = state.subject
        record = self.new_record(dict(state.data))
        # Before the old key goes: a refused write must not sign the user out.
        self._check(record.data, ttls)
        fields = self._fields_for(record, subject, descriptor, created=True)

        carried: dict[str, int] = {}
        old_id = state.session_id
        if old_id is not None:
            attributes = [
                name for name in (state.fields or {}) if name.startswith(ATTR_PREFIX)
            ]
            try:
                carried = await self._retire(old_id, attributes)
            except STORE_ERRORS as exc:
                raise SessionStoreError(f"Could not delete session: {exc}") from exc

        # Only for a field whose value this request did not change. A changed
        # value loses its TTL in a rotation as it does in a save.
        before = state.fields or {}
        expire = {
            name: ttl
            for name, ttl in carried.items()
            if name in fields and fields[name] == before.get(name)
        }
        given, persist = self._field_ttls(fields, ttls)
        expire.update(given)
        for name in persist:
            expire.pop(name, None)

        new_id = self.new_id()
        with session_span("session.create"), timed_session("create"):
            try:
                await self._create(
                    new_id,
                    self._sealed(fields),
                    idle=self.idle_seconds,
                    absolute=self.absolute_seconds,
                    field_ttls=expire,
                )
            except STORE_ERRORS as exc:
                record_session_operation(operation="create", result="error")
                raise SessionStoreError(f"Could not save session: {exc}") from exc
        record_session_operation(operation="create", result="hit")

        state.session_id = new_id
        state.created = record.metadata.created
        state.absolute_remaining = self.absolute_seconds
        state.fields = fields
        # Cleared, not set. ``rotated`` means "the middleware still owes this
        # session a rotation"; we have just performed one. Leaving it set made
        # the middleware rotate a second time at response start, which threw
        # away the key written here and turned the ID returned to the caller
        # into a stale value. The middleware notices the new ID by comparing it
        # with the one it loaded, so the cookie still goes out.
        state.rotated = False
        state.revoked = False
        state.subject = subject
        # The runtime backstop for a misconfigured principal resolver: sign-ins
        # with no rotations is a visible anomaly on a dashboard, and a silent
        # misconfiguration is the one genuine cost of detecting rather than
        # being told.
        record_session_operation(operation="rotate", result="hit")
        return new_id

    async def reauthenticate(self, state: SessionState) -> None:
        """Force a rotation on the way out of this request.

        For a privilege change the principal cannot see - an impersonation
        that leaves no trace in session state, or a step-up before a sensitive
        action.  The middleware performs the rotation at
        ``http.response.start`` so it lands in the same response as the new
        cookie.
        """
        state.rotated = True

    async def revoke(self, state: SessionState, *, subject: str | None = None) -> None:
        """End the session in hand and clear its cookie.

        Safe to call on a session that was never written.

        *subject* is accepted for callers written against the earlier store
        and is not used: the subject lives in the session's own hash, so it
        goes when the key goes.
        """
        old_id = state.session_id
        with self._observe("revoke"):
            if old_id is not None:
                await self.delete(old_id)
            state.data.clear()
            state.session_id = None
            state.subject = None
            state.fields = None
            state.revoked = True
            state.rotated = False
        # ``miss`` is a session that was never written: there was no key to
        # delete and no cookie to replace, so nothing reached Redis.
        record_session_operation(
            operation="revoke", result="hit" if old_id is not None else "miss"
        )

    async def revoke_id(self, session_id: str, *, subject: str) -> bool:
        """End one session by ID, and refuse an ID that is not *subject*'s.

        The subject is **required** and checked, so a caller holding an
        arbitrary identifier cannot end a stranger's session.  Without it this
        method would be a cross-user revocation primitive reachable from any
        handler that takes an ID from a request.

        Reads the subject from the session's own hash.  It runs no search, so
        it works on a server without the Query Engine.

        Returns:
            True if a session was ended, False if that ID is not this
            subject's - including when it has already expired.
        """
        if not self.is_valid_id(session_id):
            record_session_operation(operation="revoke_id", result="miss")
            return False
        with self._observe("revoke_id"):
            try:
                stored = await self._stored_subject(session_id)
            except STORE_ERRORS as exc:
                raise SessionStoreError(f"Could not read the session: {exc}") from exc
            if stored is None or stored != json.dumps(str(subject), ensure_ascii=False):
                # Worth its own label: a run of these is either a broken UI or
                # somebody trying identifiers that are not theirs.
                record_session_operation(operation="revoke_id", result="miss")
                return False
            await self.delete(session_id)
            record_session_operation(operation="revoke_id", result="hit")
            return True

    # -- search: the built-in index --------------------------------------------

    @staticmethod
    def _by_subject(subject: str) -> Filter:
        return Filter(BUILTIN, FIELD_SUBJECT, TAG, str(subject))

    async def revoke_all(self, subject: str) -> int:
        """End every session belonging to *subject*.

        Reads the built-in index, which holds only fields the store writes.
        No value an application puts in a session can hide it from this call.

        Returns:
            How many session keys were removed, counted from the server's own
            ``DEL`` replies.

        Raises:
            SessionSearchUnavailableError: If the server has no Query Engine.
            SessionSearchTimeoutError: If the search timed out.  Sessions
                found before that are already ended.
        """
        with self._observe("revoke_all"):
            removed = await self._revoke_matching(self._by_subject(subject))
            record_session_operation(
                operation="revoke_all", result="hit" if removed else "miss"
            )
            return removed

    async def list_for_subject(self, subject: str) -> list[SessionInfo]:
        """Live sessions for *subject*, newest access first.

        At most 1,000.  For a subject with more, :meth:`count_for_subject`
        gives the full number.

        Raises:
            SessionSearchUnavailableError: If the server has no Query Engine.
            SessionSearchTimeoutError: If the search timed out.
        """
        with self._observe("list"):
            infos = await self._search(
                self._by_subject(subject), limit=LIST_LIMIT, offset=0
            )
            record_session_operation(
                operation="list", result="hit" if infos else "miss"
            )
            return infos

    async def count_for_subject(self, subject: str, *, limit: int | None = None) -> int:
        """How many live sessions *subject* has.

        One query that returns no rows and sorts nothing, so it is cheap on
        every login for a "cap concurrent sessions" check.

        *limit* is accepted for callers written against the earlier store and
        is not used: the count is exact with or without it.

        **This one does not fail open.**  The store *is* the answer here, and
        ``0`` is the permissive one - a concurrent-session cap would wave
        every login through exactly when Redis is unhealthy.

        Raises:
            SessionSearchUnavailableError: If the server has no Query Engine.
            SessionSearchTimeoutError: If the search timed out.
            SessionStoreError: If the count could not be established.
        """
        with self._observe("count"):
            total = await self._count(self._by_subject(subject))
            record_session_operation(operation="count", result="hit")
            return total

    async def list_active(
        self, within: int, *, limit: int = DEFAULT_PAGE, offset: int = 0
    ) -> list[SessionInfo]:
        """Sessions used in the last *within* seconds, newest access first.

        One page.  A sorted page costs about as much as sorting every match,
        so a page deep in a large result is expensive; for "how many", use a
        count.

        Raises:
            ValueError: If *limit* or *offset* is out of range.
            SessionSearchUnavailableError: If the server has no Query Engine.
            SessionSearchTimeoutError: If the search timed out.
        """
        _check_page(limit, offset)
        flt = Filter(BUILTIN, FIELD_SEEN, NUMERIC, (time.time() - within, None))
        with self._observe("list"):
            infos = await self._search(flt, limit=limit, offset=offset)
            record_session_operation(
                operation="list", result="hit" if infos else "miss"
            )
            return infos

    async def revoke_idle(self, older_than: int) -> int:
        """End every session not used for *older_than* seconds.

        The idle timeout already ends an unused session; this is for a cutoff
        shorter than that.

        Returns:
            How many session keys were removed.

        Raises:
            SessionSearchUnavailableError: If the server has no Query Engine.
            SessionSearchTimeoutError: If the search timed out.
        """
        flt = Filter(BUILTIN, FIELD_SEEN, NUMERIC, (None, time.time() - older_than))
        with self._observe("revoke_all"):
            removed = await self._revoke_matching(flt)
            record_session_operation(
                operation="revoke_all", result="hit" if removed else "miss"
            )
            return removed

    # -- search: the custom index ------------------------------------------------

    def _custom_filter(
        self, name: str, equals: Any, between: Any, text: Any, near: Any
    ) -> Filter:
        """Turn the one filter of a ``find`` or ``count`` call into a query
        condition, or refuse the call."""
        kind = self._searchable.get(name)
        if kind is None:
            raise ValueError(f"{name!r} is not a searchable session attribute")
        given = {
            TAG: equals,
            NUMERIC: between,
            TEXT: text,
            GEO: near,
        }
        used = [key for key, value in given.items() if value is not _UNSET]
        if used != [kind]:
            raise ValueError(
                f"{name!r} is declared as {kind}: pass exactly one filter, "
                f"{_FILTER_ARGUMENT[kind]}="
            )
        value = given[kind]
        if kind == TAG and not isinstance(value, (str, bool)):
            raise ValueError("equals= takes a str or a bool")
        if kind == NUMERIC:
            if not isinstance(value, (tuple, list)) or len(value) != 2:
                raise ValueError("between= takes (low, high); either may be None")
            if any(part is not None and not is_finite(part) for part in value):
                raise ValueError("between= takes finite numbers")
            value = tuple(value)
        if kind == TEXT and not isinstance(value, str):
            raise ValueError("text= takes a str")
        if kind == GEO:
            if not isinstance(value, (tuple, list)) or len(value) != 3:
                raise ValueError("near= takes (lon, lat, radius in meters)")
            if not all(is_finite(part) for part in value):
                raise ValueError("near= takes finite numbers")
            value = tuple(value)
        return Filter(CUSTOM, ATTR_PREFIX + name, kind, value)

    async def find(
        self,
        name: str,
        *,
        equals: Any = _UNSET,
        between: Any = _UNSET,
        text: Any = _UNSET,
        near: Any = _UNSET,
        limit: int = DEFAULT_PAGE,
        offset: int = 0,
    ) -> list[SessionInfo]:
        """Sessions whose searchable attribute *name* matches one filter.

        Pass the filter that fits the declared type: ``equals=`` for a tag,
        ``between=(low, high)`` for a number, ``text=`` for text, and
        ``near=(lon, lat, meters)`` for a place.

        **``text=`` is Redis query syntax, sent as it is.**  ``pet*`` is a
        prefix, ``a|b`` is either word, and ``-a`` is every session without
        the word.  Text from an end user can change the meaning of the query,
        so the application must sanitize it.

        Raises:
            ValueError: If *name* is not declared, the filter does not fit
                its type, the text has a syntax error, or *limit* or *offset*
                is out of range.
            SessionSearchUnavailableError: If the server has no Query Engine.
            SessionSearchTimeoutError: If the search timed out.
        """
        _check_page(limit, offset)
        flt = self._custom_filter(name, equals, between, text, near)
        with self._observe("list"):
            infos = await self._search(flt, limit=limit, offset=offset)
            record_session_operation(
                operation="list", result="hit" if infos else "miss"
            )
            return infos

    async def count(
        self,
        name: str,
        *,
        equals: Any = _UNSET,
        between: Any = _UNSET,
        text: Any = _UNSET,
        near: Any = _UNSET,
    ) -> int:
        """How many sessions :meth:`find` would match, with no rows returned.

        Raises:
            ValueError: As :meth:`find`.
            SessionSearchUnavailableError: If the server has no Query Engine.
            SessionSearchTimeoutError: If the search timed out.
        """
        flt = self._custom_filter(name, equals, between, text, near)
        with self._observe("count"):
            total = await self._count(flt)
            record_session_operation(operation="count", result="hit")
            return total

    # -- failure policy ------------------------------------------------------

    def _read_failed(self, exc: BaseException) -> None:
        """Apply the read half of the fail-open / fail-closed policy.

        The default is asymmetric on purpose.  A failed read yields no session,
        so the caller looks anonymous and the application's own authorization
        dependency returns a login page or a 401 - a protected route stays
        protected, because it never depended on this read succeeding.

        Raises:
            SessionStoreError: If ``session_fail_closed`` is set, for a
                deployment that would rather return 503 than an anonymous page.
        """
        if get_settings().session_fail_closed:
            raise SessionStoreError(f"Could not load session: {exc}") from exc
        logger.warning("Session load failed, continuing without a session: %s", exc)

    async def _safe_delete(self, session_id: str) -> None:
        """Delete a key we already know is dead; never raise for it.

        This runs on the tidy-up path of a *read*.  Failing the request because
        we could not clean up a session that is already gone would turn a
        cosmetic problem into an outage.
        """
        try:
            await self._delete(session_id)
        except STORE_ERRORS as exc:
            logger.warning("Could not remove a dead session key: %s", exc)

    # -- the whole surface a new backend implements --------------------------
    #
    # Underscore-prefixed because no application calls them, *not* because a
    # backend may not override them - overriding them is the only way to write
    # one, and Python refuses to instantiate a subclass that leaves one out.
    # Each docstring below carries the rule that is easy to get wrong, because
    # these docstrings are the only statement of the contract: it is
    # deliberately not described in the published docs.

    @abstractmethod
    async def _read(
        self, session_id: str, *, refresh_idle: int | None, seen: float | None
    ) -> tuple[dict[str, str], int | Deadline]:
        """Return ``(the stored fields, the absolute deadline)``.

        The fields are as stored - an attribute value is still encrypted when
        an encryptor is set.  An empty dict means no key.

        The deadline is the number of seconds left on ``s:created``, or a
        :class:`Deadline` member when it is not a number.  Do not invent
        negative sentinels - the base class does not interpret them.

        *refresh_idle* is the idle window in seconds, or ``None`` to read
        without refreshing.  *seen* is the time to record as the last access,
        where the backend records one on a read.  **No command here may
        create the key**: a cookie that names no session must leave nothing
        behind.
        """

    @abstractmethod
    async def _create(
        self,
        session_id: str,
        fields: Mapping[str, str],
        *,
        idle: int,
        absolute: int,
        field_ttls: Mapping[str, int],
    ) -> None:
        """Write a new session: every field, both clocks, and the TTL of each
        field in *field_ttls*.

        *absolute* is the TTL of ``s:created`` and *idle* the TTL of the key.
        """

    @abstractmethod
    async def _apply(
        self,
        session_id: str,
        to_set: Mapping[str, str],
        to_delete: list[str],
        *,
        field_ttls: Mapping[str, int],
        persist: list[str],
        idle: int,
    ) -> None:
        """Write a difference to an existing session and restart its idle
        clock.

        Set the TTL of each field in *field_ttls*, and remove the TTL of each
        field in *persist*.  ``s:created`` is never in *to_set*, and its TTL
        must be left completely alone.
        """

    @abstractmethod
    async def _expire(self, session_id: str, idle: int, *, seen: float) -> None:
        """Restart the idle clock, leaving the fields and the deadline alone.

        Record *seen* as the last access where the backend records one.  As
        with ``_read``, nothing here may create the key.
        """

    @abstractmethod
    async def _delete(self, session_id: str) -> None:
        """Remove the session entirely."""

    @abstractmethod
    async def _delete_many(self, session_ids: list[str]) -> int:
        """Remove many sessions in one round trip; return how many existed."""

    @abstractmethod
    async def _retire(self, session_id: str, fields: list[str]) -> dict[str, int]:
        """Remove a session that is being rotated, and return the seconds
        left on each of *fields* that has a TTL."""

    @abstractmethod
    async def _stored_subject(self, session_id: str) -> str | None:
        """Return the stored value of ``s:sub``, or ``None``."""

    @abstractmethod
    async def _search(
        self, flt: Filter, *, limit: int, offset: int
    ) -> list[SessionInfo]:
        """Return one page of the live sessions that match *flt*, newest
        access first."""

    @abstractmethod
    async def _count(self, flt: Filter) -> int:
        """Return how many live sessions match *flt*."""

    @abstractmethod
    async def _revoke_matching(self, flt: Filter) -> int:
        """End every live session that matches *flt*; return how many."""


class RedisSessionStore(SessionStore):
    """The Redis implementation of the storage primitives.

    One pipelined round trip for each operation on a session.  The key is
    flat, with no hash tag, for the reason ``ratelimit_backend.py`` already
    gives for rate-limit keys: a hash tag would send every session of one
    deployment to a single Cluster slot and create a hot shard.  Nothing in
    this class is a multi-key command, so nothing needs co-location.
    """

    def __init__(
        self,
        redis: AsyncRedis | AsyncRedisCluster,
        *,
        capabilities: _StoreCapabilities | None = None,
        key_prefix: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._redis = redis
        self._caps = capabilities if capabilities is not None else _StoreCapabilities()
        settings = get_settings()
        base = key_prefix if key_prefix is not None else settings.prefix
        self._session_prefix = f"{base}:{KEY_SEGMENTS['session']}:"
        # Index names, not keys: the Query Engine keeps them in a namespace
        # of its own, so the built-in one needs no trailing separator.
        self._builtin_index = f"{base}:{KEY_SEGMENTS['session-index']}"
        self._custom_base = f"{base}:{KEY_SEGMENTS['session-attr-index']}:"
        self._custom_index = (
            custom_index_name(self._custom_base, self._searchable)
            if self._searchable
            else None
        )

    # -- keys ----------------------------------------------------------------

    def session_key(self, session_id: str) -> str:
        """``<prefix>:s:<sid>``."""
        return f"{self._session_prefix}{session_id}"

    # -- capability ----------------------------------------------------------

    async def _has_hsetex(self) -> bool:
        """Whether the server has ``HSETEX``.  Probed once, then cached."""
        if self._caps.supports_hsetex is None:
            self._caps.supports_hsetex = await probe_hsetex_support(self._redis)
        return bool(self._caps.supports_hsetex)

    async def _has_search(self) -> bool:
        """Whether the server has the Query Engine.  Probed once, then cached.

        An unknown answer reads as "no" for this call and is asked again on
        the next one.  A definite "no" is logged once for the pool.
        """
        if self._caps.supports_search is None:
            self._caps.supports_search = await probe_search_support(self._redis)
        if self._caps.supports_search is False and not self._caps.warned_no_search:
            self._caps.warned_no_search = True
            logger.warning(
                "The Redis server has no Query Engine: session indexing is "
                "unavailable. Sessions load and save by ID; searches by "
                "subject, by last activity and by attribute raise "
                "SessionSearchUnavailableError."
            )
        return bool(self._caps.supports_search)

    async def _records_access(self) -> bool:
        """Whether a read should record the last access.

        Only a search reads ``s:seen``, so a read writes it only where a
        search can run.  It also needs ``HSETEX ... FXX``, which writes only
        to a field that exists: a plain ``HSET`` would create a key for a
        cookie that names no session.
        """
        return await self._has_search() and await self._has_hsetex()

    def _pipeline(self) -> Any:
        """One atomic pipeline for one key, where the client allows it.

        ``MULTI`` keeps a reader from seeing half of a write.  A cluster
        client gets a plain pipeline: every command names one key, so they
        reach one node in order.
        """
        return self._redis.pipeline(
            transaction=not isinstance(self._redis, AsyncRedisCluster)
        )

    # -- the primitives ------------------------------------------------------

    async def _read(
        self, session_id: str, *, refresh_idle: int | None, seen: float | None
    ) -> tuple[dict[str, str], int | Deadline]:
        key = self.session_key(session_id)
        record_access = (
            refresh_idle is not None
            and seen is not None
            and await self._records_access()
        )
        pipe = self._redis.pipeline(transaction=False)
        pipe.hgetall(key)
        pipe.execute_command("HTTL", key, "FIELDS", 1, FIELD_CREATED)
        if record_access:
            pipe.execute_command(
                "HSETEX", key, "FXX", "FIELDS", 1, FIELD_SEEN, repr(seen)
            )
        if refresh_idle is not None:
            # On a missing key this does nothing, like every command above.
            pipe.execute_command("EXPIRE", key, refresh_idle)
        replies = await pipe.execute()
        return _text_map(replies[0]), _deadline(replies[1])

    @staticmethod
    def _expire_fields(pipe: Any, key: str, field_ttls: Mapping[str, int]) -> None:
        """One ``HEXPIRE`` for each distinct TTL."""
        by_ttl: dict[int, list[str]] = {}
        for name, ttl in field_ttls.items():
            by_ttl.setdefault(ttl, []).append(name)
        for ttl, names in by_ttl.items():
            pipe.execute_command("HEXPIRE", key, ttl, "FIELDS", len(names), *names)

    async def _create(
        self,
        session_id: str,
        fields: Mapping[str, str],
        *,
        idle: int,
        absolute: int,
        field_ttls: Mapping[str, int],
    ) -> None:
        key = self.session_key(session_id)
        pipe = self._pipeline()
        pipe.hset(key, mapping=dict(fields))
        pipe.execute_command("HEXPIRE", key, absolute, "FIELDS", 1, FIELD_CREATED)
        self._expire_fields(pipe, key, field_ttls)
        pipe.execute_command("EXPIRE", key, idle)
        await pipe.execute()

    async def _apply(
        self,
        session_id: str,
        to_set: Mapping[str, str],
        to_delete: list[str],
        *,
        field_ttls: Mapping[str, int],
        persist: list[str],
        idle: int,
    ) -> None:
        key = self.session_key(session_id)
        pipe = self._pipeline()
        if to_set:
            pipe.hset(key, mapping=dict(to_set))
        if to_delete:
            pipe.hdel(key, *to_delete)
        self._expire_fields(pipe, key, field_ttls)
        if persist:
            pipe.execute_command("HPERSIST", key, "FIELDS", len(persist), *persist)
        # Every pipeline that writes ends with this. A save that lands after
        # the session ended recreates the hash, and without a TTL that hash
        # would stay for ever.
        pipe.execute_command("EXPIRE", key, idle)
        await pipe.execute()

    async def _expire(self, session_id: str, idle: int, *, seen: float) -> None:
        key = self.session_key(session_id)
        pipe = self._redis.pipeline(transaction=False)
        if await self._records_access():
            pipe.execute_command(
                "HSETEX", key, "FXX", "FIELDS", 1, FIELD_SEEN, repr(seen)
            )
        pipe.execute_command("EXPIRE", key, idle)
        await pipe.execute()

    async def _delete(self, session_id: str) -> None:
        await self._redis.delete(self.session_key(session_id))

    async def _delete_many(self, session_ids: list[str]) -> int:
        if not session_ids:
            return 0
        pipe = self._redis.pipeline(transaction=False)
        for session_id in session_ids:
            pipe.delete(self.session_key(session_id))
        # One DEL per key rather than one variadic DEL: on a cluster the keys
        # span slots, and redis-py fans a pipeline out per node while a single
        # multi-key DEL would be refused. Still one round trip per node.
        return sum(int(reply or 0) for reply in await pipe.execute())

    async def _retire(self, session_id: str, fields: list[str]) -> dict[str, int]:
        key = self.session_key(session_id)
        pipe = self._redis.pipeline(transaction=False)
        if fields:
            pipe.execute_command("HTTL", key, "FIELDS", len(fields), *fields)
        pipe.delete(key)
        replies = await pipe.execute()
        if not fields or not isinstance(replies[0], (list, tuple)):
            return {}
        left: dict[str, int] = {}
        for name, reply in zip(fields, replies[0], strict=False):
            try:
                seconds = int(reply)
            except (TypeError, ValueError):
                continue
            if seconds > 0:
                left[name] = seconds
        return left

    async def _stored_subject(self, session_id: str) -> str | None:
        raw = await self._redis.hget(self.session_key(session_id), FIELD_SUBJECT)
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else str(raw)

    # -- the indexes -----------------------------------------------------------

    async def ensure_indexes(self) -> None:
        """Create the session indexes this store needs, if they are missing.

        Safe to call from every worker at once: "Index already exists" on a
        create, and "index not found" on a drop, both count as success.

        The built-in index is never dropped.  The custom index carries a hash
        of its declaration in its name, so a changed declaration creates a
        new index, and each older one is dropped here.  Redis fills a new
        index in the background, and a search in that time sees a part of
        the sessions.

        Does nothing on a server without the Query Engine.

        Raises:
            SessionSearchError: If an index could not be created.
        """
        if not await self._has_search():
            return
        try:
            await self._create_index(self._builtin_index, builtin_fields())
            if self._custom_index is not None:
                await self._create_index(
                    self._custom_index, custom_fields(self._searchable)
                )
            for name in await self._redis.execute_command("FT._LIST"):
                text = name.decode() if isinstance(name, bytes) else str(name)
                if text.startswith(self._custom_base) and text != self._custom_index:
                    logger.warning(
                        "Dropping the session attribute index %s: the "
                        "searchable declaration changed",
                        text,
                    )
                    await self._drop_index(text)
        except STORE_ERRORS as exc:
            raise SessionSearchError(
                f"Could not create the session index: {exc}"
            ) from exc
        self._caps.ready_indexes.add(self._builtin_index)
        if self._custom_index is not None:
            self._caps.ready_indexes.add(self._custom_index)
        await self._warn_on_timeout_policy()

    async def _create_index(self, name: str, fields: list[Any]) -> None:
        try:
            await self._redis.ft(name).create_index(
                fields, definition=definition(self._session_prefix)
            )
        except STORE_ERRORS as exc:
            if error_kind(exc) != "exists":
                raise

    async def _drop_index(self, name: str) -> None:
        try:
            await self._redis.ft(name).dropindex(delete_documents=False)
        except STORE_ERRORS as exc:
            if error_kind(exc) != "missing":
                raise

    async def _warn_on_timeout_policy(self) -> None:
        """Say once when the server answers a timed-out query with a partial
        result and no error.

        Best-effort: managed platforms commonly refuse ``CONFIG GET``, and a
        refusal must not stop a search.
        """
        if self._caps.warned_timeout_policy:
            return
        try:
            reply = await self._redis.config_get("search-on-timeout")
        except Exception:
            logger.debug("Could not read search-on-timeout", exc_info=True)
            return
        policy = _config_value(reply, "search-on-timeout")
        if policy is not None and policy.lower() != "fail":
            self._caps.warned_timeout_policy = True
            logger.warning(
                "search-on-timeout is %r: a session search that times out can "
                "return a partial result. Set it to 'fail' so that the store "
                "raises SessionSearchTimeoutError instead.",
                policy,
            )

    def _index_for(self, flt: Filter) -> str:
        if flt.index == BUILTIN:
            return self._builtin_index
        if self._custom_index is None:
            raise ValueError("No searchable session attribute is declared")
        return self._custom_index

    async def _query(self, flt: Filter, query: Any) -> Any:
        """Send one search and return the parsed reply.

        The one place that checks for the Query Engine, ensures the index,
        and turns a timeout or a syntax error into its own exception.
        """
        if not await self._has_search():
            raise SessionSearchUnavailableError(
                "The Redis server has no Query Engine, so sessions cannot be searched"
            )
        name = self._index_for(flt)
        if name not in self._caps.ready_indexes:
            await self.ensure_indexes()
        for attempt in (1, 2):
            try:
                reply = parse_reply(await self._redis.ft(name).search(query))
                break
            except STORE_ERRORS as exc:
                kind = error_kind(exc)
                if kind == "timeout":
                    raise SessionSearchTimeoutError(
                        f"The session search timed out: {exc}"
                    ) from exc
                if kind == "syntax":
                    raise ValueError(f"The search text is not valid: {exc}") from exc
                if kind == "missing" and attempt == 1:
                    # The index went with a flush or a failover to an empty
                    # node. Create it again and ask once more.
                    self._caps.ready_indexes.discard(name)
                    await self.ensure_indexes()
                    continue
                raise SessionSearchError(f"Could not search sessions: {exc}") from exc
        if reply.timed_out:
            raise SessionSearchTimeoutError(
                "The session search timed out and returned a partial result"
            )
        return reply

    def _to_info(self, key: str, fields: Mapping[str, str]) -> SessionInfo:
        """Build a listing row from what a search returned for one session."""
        try:
            created = float(fields.get(FIELD_CREATED, 0.0))
            seen = float(fields.get(FIELD_SEEN, created))
        except (TypeError, ValueError):
            created, seen = 0.0, 0.0
        descriptor: dict[str, Any] = {}
        if FIELD_DESCRIPTOR in fields:
            try:
                descriptor = dict(json.loads(fields[FIELD_DESCRIPTOR]))
            except (TypeError, ValueError):
                # A descriptor is display data. A malformed one must not hide
                # a live session from the list.
                logger.warning("Unreadable descriptor for a live session")
        return SessionInfo(
            session_id=key[len(self._session_prefix) :],
            created=created,
            last_access=seen,
            descriptor=descriptor,
        )

    async def _search(
        self, flt: Filter, *, limit: int, offset: int
    ) -> list[SessionInfo]:
        reply = await self._query(flt, page_query(flt, limit=limit, offset=offset))
        return [self._to_info(key, fields) for key, fields in reply.rows]

    async def _count(self, flt: Filter) -> int:
        return int((await self._query(flt, count_query(flt))).total)

    async def _revoke_matching(self, flt: Filter) -> int:
        removed = 0
        seen: set[str] = set()
        while True:
            reply = await self._query(flt, ids_query(flt, limit=REVOKE_BATCH))
            ids = [key[len(self._session_prefix) :] for key, _ in reply.rows]
            fresh = [session_id for session_id in ids if session_id not in seen]
            if not fresh:
                # Nothing, or only keys already deleted that the index has
                # not let go of yet. Either way there is no more to do.
                return removed
            seen.update(fresh)
            try:
                removed += await self._delete_many(fresh)
            except STORE_ERRORS as exc:
                raise SessionStoreError(f"Could not revoke sessions: {exc}") from exc


def issued_ago(absolute_seconds: int, state: SessionState) -> int | None:
    """:meth:`SessionStore.session_age`, for any store with an absolute clock.

    A module function so the session gate can measure a store that implements
    only :class:`SessionStoreProtocol`, which has ``absolute_seconds`` but no
    ``session_age``.
    """
    if state.session_id is None or state.absolute_remaining is None:
        return None
    age = absolute_seconds - state.absolute_remaining
    return age if age >= 0 else None


_FILTER_ARGUMENT = {TAG: "equals", NUMERIC: "between", TEXT: "text", GEO: "near"}


def _check_page(limit: int, offset: int) -> None:
    """Refuse a page the store will not ask Redis for."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError(f"limit must be a positive integer, got {limit!r}")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise ValueError(f"offset must not be negative, got {offset!r}")
    if offset + limit > PAGE_LIMIT:
        raise ValueError(
            f"offset + limit must not exceed {PAGE_LIMIT}, got {offset + limit}"
        )


def _text_map(raw: Any) -> dict[str, str]:
    """A hash reply as ``{str: str}``, whatever the client decoded."""
    if not raw:
        return {}
    return {
        (k.decode() if isinstance(k, bytes) else str(k)): (
            v.decode() if isinstance(v, bytes) else str(v)
        )
        for k, v in dict(raw).items()
    }


def _config_value(reply: Any, name: str) -> str | None:
    """Read one setting out of a ``CONFIG GET`` reply."""
    if isinstance(reply, Mapping):
        for key, value in reply.items():
            text = key.decode() if isinstance(key, bytes) else str(key)
            if text == name:
                return value.decode() if isinstance(value, bytes) else str(value)
    return None


def _deadline(reply: Any) -> int | Deadline:
    """Map the ``HTTL`` reply for ``s:created`` onto the deadline contract.

    This is the only place that knows what ``-1`` and ``-2`` mean, which is
    the point: the encoding stays inside the Redis backend.
    """
    value = reply[0] if isinstance(reply, (list, tuple)) and reply else reply
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return Deadline.ABSENT
    if seconds == _TTL_NO_EXPIRY:
        return Deadline.UNBOUNDED
    if seconds == _TTL_NO_KEY:
        return Deadline.ABSENT
    return seconds


class SyncSessionStore:
    """Synchronous facade over :class:`SessionStore`.

    Every method delegates to the async store via
    :func:`anyio.from_thread.run`, mirroring ``SyncCacheBackend`` and
    ``SyncRateLimitBackend``.  Only usable from FastAPI-managed worker threads
    - a plain ``def`` endpoint or a ``def`` dependency.  Calling it from the
    main thread raises ``RuntimeError``.

    Only the methods a handler calls are exposed.  The lifecycle
    (``load``/``save``/``touch``) belongs to the middleware, which is async
    and needs no bridge.
    """

    def __init__(self, store: SessionStore) -> None:
        self._store = store

    @staticmethod
    def _run(func: Any) -> Any:
        import anyio.from_thread

        return anyio.from_thread.run(func)

    def session_id(self, state: SessionState) -> str | None:
        """The identifier this session is stored under, if any (no I/O)."""
        return self._store.session_id(state)

    def rotate(
        self,
        state: SessionState,
        *,
        subject: str | None = _UNSET,
        descriptor: dict[str, Any] | None = None,
    ) -> str:
        """Issue a new identifier, deleting the old key first (blocking)."""
        result: str = self._run(
            lambda: self._store.rotate(state, subject=subject, descriptor=descriptor)
        )
        return result

    def reauthenticate(self, state: SessionState) -> None:
        """Force a rotation on the way out of this request.

        No I/O, so no bridge: the async original only sets a flag the
        middleware reads at response time.  Routing that through
        ``anyio.from_thread.run`` burnt a worker thread and made the call
        raise outside one, for two attribute writes.
        """
        state.rotated = True

    def revoke(self, state: SessionState, *, subject: str | None = None) -> None:
        """End the session in hand and clear its cookie (blocking)."""
        self._run(lambda: self._store.revoke(state, subject=subject))

    def revoke_id(self, session_id: str, *, subject: str) -> bool:
        """End one session by ID, scoped to *subject* (blocking)."""
        result: bool = self._run(
            lambda: self._store.revoke_id(session_id, subject=subject)
        )
        return result

    def revoke_all(self, subject: str) -> int:
        """End every session belonging to *subject* (blocking)."""
        result: int = self._run(lambda: self._store.revoke_all(subject))
        return result

    def list_for_subject(self, subject: str) -> list[SessionInfo]:
        """Live, verified sessions for *subject* (blocking)."""
        result: list[SessionInfo] = self._run(
            lambda: self._store.list_for_subject(subject)
        )
        return result

    def count_for_subject(self, subject: str, *, limit: int | None = None) -> int:
        """How many sessions *subject* has (blocking)."""
        result: int = self._run(
            lambda: self._store.count_for_subject(subject, limit=limit)
        )
        return result

    def list_active(
        self, within: int, *, limit: int = DEFAULT_PAGE, offset: int = 0
    ) -> list[SessionInfo]:
        """Sessions used in the last *within* seconds (blocking)."""
        result: list[SessionInfo] = self._run(
            lambda: self._store.list_active(within, limit=limit, offset=offset)
        )
        return result

    def revoke_idle(self, older_than: int) -> int:
        """End every session not used for *older_than* seconds (blocking)."""
        result: int = self._run(lambda: self._store.revoke_idle(older_than))
        return result

    def find(self, name: str, **filters: Any) -> list[SessionInfo]:
        """Sessions whose searchable attribute matches one filter (blocking)."""
        result: list[SessionInfo] = self._run(lambda: self._store.find(name, **filters))
        return result

    def count(self, name: str, **filters: Any) -> int:
        """How many sessions :meth:`find` would match (blocking)."""
        result: int = self._run(lambda: self._store.count(name, **filters))
        return result
