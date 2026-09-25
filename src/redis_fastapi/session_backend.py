"""Redis-backed session store.

Two keys, both hashes::

    redis:fastapi:session:<sid>          key TTL = absolute
                                         field "d" = <envelope> TTL = idle
    redis:fastapi:sessions-of:<subject>  field <sid> = <descriptor>
                                                                TTL = absolute

Each clock has its own TTL, and Redis enforces both: this module computes
neither.  At the absolute deadline Redis deletes the whole key, so no reader
can serve a session past it, whatever it asks for.  Writing the payload
touches field ``d`` alone, so no number of writes can extend the key's TTL.

See ``docs/specs/session-design.md`` Sections 3 and 4.
"""

from __future__ import annotations

import contextlib
import logging
import secrets
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, MutableMapping
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum, auto
from typing import Any, Protocol, runtime_checkable

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.exceptions import RedisClusterException, RedisError

from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import (
    SessionConfigurationError,
    SessionStoreError,
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
# session key.  Section 13.2: a uniform schema is what a future compact-hash
# encoding would reward, and a short name is fewer bytes on the wire meanwhile.
FIELD_DATA = "d"


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


def _seconds(value: int | timedelta) -> int:
    """Normalise a TTL setting to whole seconds.

    Accepts ``timedelta`` because ``cache()`` already does, and Section 9.4 of
    ``session-mgmt.md`` requires it for the ``starsessions`` migration.
    """
    if isinstance(value, timedelta):
        return int(value.total_seconds())
    return int(value)


@dataclass(frozen=True)
class SessionMetadata:
    """Timestamps kept beside the payload, never mixed into it.

    ``starsessions`` stores its metadata under a ``__metadata__`` key *inside*
    the session, so it shows up in the application's own ``request.session``.
    We keep it in a sibling slot of the envelope instead.  The three names
    match theirs, so their accessors port across as a rename.
    """

    created: float
    last_access: float
    lifetime: int


@dataclass(frozen=True)
class SessionRecord:
    """What field ``d`` holds once decoded: the payload and its metadata."""

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
        supports_hsetex: Whether the server has ``HSETEX``/``HGETEX``, which
            arrived in Redis 8.0.  Against 7.4 the store falls back to
            ``HSET`` + ``HEXPIRE`` and ``HGET`` + ``HEXPIRE``, pipelined.  That
            costs one extra command, never a change in behaviour.
    """

    supports_hsetex: bool | None = None


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

    Unlike the ``INCREX`` case there is no correctness cliff behind this - both
    paths write the same fields with the same expirations.
    """
    try:
        if isinstance(client, AsyncRedisCluster):
            reply = await client.execute_command(
                "COMMAND", "INFO", "HSETEX", target_nodes=AsyncRedisCluster.RANDOM
            )
        else:
            reply = await client.execute_command(  # type: ignore[no-untyped-call]
                "COMMAND", "INFO", "HSETEX"
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
    first = reply[0] if isinstance(reply, (list, tuple)) else reply
    return first is not None


@dataclass
class SessionState:
    """Everything about one session that is not its data.

    The store reads and writes this; the middleware owns it and puts it on
    ``request.state``.  Keeping it separate is what lets ``Session`` go back to
    being a ``dict`` with two flags - exactly Starlette's contract and nothing
    more - and it is why this module imports nothing from ``sessions``.

    The type of ``data`` is a plain ``MutableMapping`` on purpose: the store
    has no business knowing about a web session object, and a second carrier
    (an agent or MCP session, say) can reuse the store without one.

    Attributes:
        data: The payload the application sees.
        session_id: Where it is stored, or ``None`` before the first write.
        subject: What it is indexed under.  Captured at load time, because by
            the time ``revoke`` runs the payload it would be derived from is
            already gone.
        created: The original creation time, carried forward so a write does
            not restamp it.
        absolute_remaining: What the server says is left of the deadline.
        revoked: The store ended this session; the middleware owes a clearing
            cookie.
        rotated: A handler asked for a rotation the middleware still owes.
    """

    data: MutableMapping[str, Any]
    session_id: str | None = None
    subject: str | None = None
    created: float | None = None
    absolute_remaining: int | None = None
    revoked: bool = False
    rotated: bool = False


@dataclass(frozen=True)
class LoadedSession:
    """A live session, plus the deadline Redis is counting down for it.

    ``absolute_remaining`` comes from ``TTL`` on the session key - the server's
    own number, never one this process computed.  Section 6 needs it to size
    the cookie's ``max-age``, so the cookie cannot expire before the record.
    """

    record: SessionRecord
    absolute_remaining: int


@runtime_checkable
class SessionStoreProtocol(Protocol):
    """The surface the middleware and the dependencies actually call.

    Much smaller than :class:`SessionStore`, and structural rather than
    inherited, so a test double or another package can supply a store without
    subclassing anything.  :class:`SessionStore` satisfies it by construction.

    Implement this when you want to *substitute* a store; inherit
    :class:`SessionStore` when you want to *write* one, because the base class
    already owns the lifecycle rules that are a security control.
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
        self, session_id: str, record: SessionRecord
    ) -> None: ...  # pragma: no cover

    async def save(
        self, session_id: str, record: SessionRecord
    ) -> None: ...  # pragma: no cover

    async def touch(self, session_id: str) -> None: ...  # pragma: no cover

    async def delete(self, session_id: str) -> None: ...  # pragma: no cover

    async def rotate(
        self,
        state: SessionState,
        *,
        subject: str | None = ...,
        descriptor: dict[str, Any] | None = ...,
    ) -> str: ...  # pragma: no cover

    async def revoke(
        self, state: SessionState, *, subject: str | None = ...
    ) -> None: ...  # pragma: no cover

    async def index(
        self,
        subject: str,
        session_id: str,
        record: SessionRecord,
        *,
        absolute_remaining: int,
        extra: dict[str, Any] | None = ...,
    ) -> None: ...  # pragma: no cover


class SessionStore(ABC):
    """Owns the session lifecycle; a subclass owns only the storage.

    The lifecycle is a security control, so it is written once here rather
    than once per backend.  A new backend implements the abstract primitives
    at the bottom of this class - eleven of them, listed there - and inherits
    the identifier rules, the two-clock policy, the envelope format and the
    error handling.

    **The underscore on those eleven means "applications never call this",
    not "do not override this".**  The prefix is what separates policy from
    mechanism - ``delete()`` applies the failure policy, ``_delete()`` removes
    the key - and the two could not share a name.  Overriding them is
    supported and is how this class is meant to be extended; it is simply not
    a documented, supported product surface, so the published guide does not
    describe it and no compatibility promise attaches to it.

    Section 7 of ``session-mgmt.md`` gives the reason this is an abstract base
    class and not a bare protocol.  ``SessionStoreProtocol`` describes the much
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
    ) -> None:
        settings = get_settings()
        self._coder = coder
        self._encryptor = encryptor
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
        """TTL for field ``d``.

        ``session_idle_ttl=0`` disables the idle clock, and the field then
        falls back to ``gc_ttl`` rather than being left unexpiring, so Redis
        can always collect an abandoned key.
        """
        return self._idle_ttl or self._gc_ttl

    @property
    def absolute_seconds(self) -> int:
        """TTL for the session key, set once at creation and never refreshed.

        ``session_absolute_ttl=0`` disables the absolute clock, and the key
        then falls back to ``gc_ttl`` for the same reason as above.
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

    # -- the envelope --------------------------------------------------------

    def encode(self, record: SessionRecord) -> str:
        """Serialize the envelope that field ``d`` holds.

        The metadata lives in a sibling slot ``m``, never inside the payload
        ``d``, so it can never appear in the application's ``request.session``.
        """
        encoded: str = self._coder.encode(
            {
                "m": {
                    "created": record.metadata.created,
                    "last_access": record.metadata.last_access,
                    "lifetime": record.metadata.lifetime,
                },
                "d": record.data,
            }
        )
        if self._encryptor is not None:
            return self._encryptor.encrypt(encoded.encode()).decode("latin-1")
        return encoded

    def decode(self, raw: str | bytes) -> SessionRecord:
        """Parse the envelope; raise :class:`SessionStoreError` if it is junk.

        A corrupted record is treated as unreadable rather than as an empty
        session, because the two mean different things: the caller decides
        whether to sign the user out or to fail the request.
        """
        if self._encryptor is not None:
            blob = raw.encode("latin-1") if isinstance(raw, str) else raw
            try:
                text = self._encryptor.decrypt(blob).decode()
            except Exception as exc:
                raise SessionStoreError(f"Unreadable session record: {exc}") from exc
        else:
            text = raw if isinstance(raw, str) else raw.decode()
        try:
            envelope = self._coder.decode(text)
            meta = envelope["m"]
            return SessionRecord(
                data=dict(envelope["d"]),
                metadata=SessionMetadata(
                    created=float(meta["created"]),
                    last_access=float(meta["last_access"]),
                    lifetime=int(meta["lifetime"]),
                ),
            )
        except Exception as exc:
            raise SessionStoreError(f"Unreadable session record: {exc}") from exc

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

        ``lifetime`` records the deadline actually in force, so a session with
        the absolute clock disabled reports the ``gc_ttl`` backstop rather
        than a misleading zero.
        """
        now = time.time()
        return SessionRecord(
            data=data,
            metadata=SessionMetadata(
                created=created if created is not None else now,
                last_access=now,
                lifetime=self.absolute_seconds,
            ),
        )

    # -- instrumentation ------------------------------------------------------

    @contextlib.contextmanager
    def _observe(self, operation: str) -> Iterator[None]:
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

        The second value is ``True`` only when Redis could not be read and
        ``session_fail_closed`` let the request continue.  Without it, an
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

        The session is alive only when **both** answers say so: the payload is
        present and the key has time left.  Anything else that finds a key
        deletes it, so the index entry can follow.

        Raises:
            SessionStoreError: If the record exists but cannot be decoded, or
                if the read failed and ``session_fail_closed`` is set.
        """
        if not self.is_valid_id(session_id):
            return None, False
        with session_span("session.load"), timed_session("load"):
            try:
                raw, absolute_ttl = await self._read(
                    session_id, refresh_idle=self.idle_seconds if refresh else None
                )
            except STORE_ERRORS as exc:
                record_session_operation(operation="load", result="error")
                self._read_failed(exc)
                return None, True

        alive_until = absolute_ttl if isinstance(absolute_ttl, int) else 0
        if raw is None or alive_until <= 0:
            # A key that exists but is not a live session: a payload with no
            # deadline, which a save recreated after the session ended, or a
            # deadline with no payload. Delete it so the index entry can
            # follow, rather than leaving a candidate that every later
            # verification has to reject. No key at all is simply absent.
            half_dead = raw is not None or absolute_ttl is not Deadline.ABSENT
            if half_dead:
                await self._safe_delete(session_id)
            record_session_operation(
                operation="load", result="expired" if half_dead else "miss"
            )
            return None, False

        record_session_operation(operation="load", result="hit")
        loaded = LoadedSession(record=self.decode(raw), absolute_remaining=alive_until)
        return loaded, False

    async def create(self, session_id: str, record: SessionRecord) -> None:
        """Write a session that does not exist yet, starting both clocks.

        The only method that ever sets the key's TTL.  Use it for a first
        write and for the new half of a rotation; use :meth:`save` for every
        subsequent write.

        Raises:
            SessionStoreError: On any store failure.
        """
        await self._save(session_id, record, absolute=self.absolute_seconds)

    async def save(self, session_id: str, record: SessionRecord) -> None:
        """Update an existing session's payload, and only its payload.

        **This is N-6, and the method split is what makes it structural.**
        There is no argument to this method that could set the key's TTL, so
        an update cannot extend the absolute deadline and - the case that
        matters - cannot bring it back after it has expired.

        A write that lands after the key expired, or after it was revoked,
        recreates the key with ``d`` and no TTL.  The next load reads that as
        :attr:`Deadline.UNBOUNDED` and deletes it.  The session ends, which is
        the correct outcome: its deadline passed.

        Raises:
            SessionStoreError: On any store failure.  A write never fails
                quietly, whatever ``session_fail_closed`` says - losing a login
                or a rotation is the worst outcome in this design.
        """
        await self._save(session_id, record, absolute=None)

    async def _save(
        self, session_id: str, record: SessionRecord, *, absolute: int | None
    ) -> None:
        """Shared body of :meth:`create` and :meth:`save`.

        **This is N-6, and the flag is what makes it structural.**  An update
        writes field ``d`` and nothing else, so it cannot extend the key's TTL
        and - the case that matters - it cannot bring it back after it has
        expired.

        *absolute* is the deadline TTL on a create, and ``None`` on an update.
        It also names the operation for telemetry: the two report separately
        because a create is a session that did not exist a moment ago - the
        sign-in rate, and the most useful single number a session dashboard
        can show - while a save is a session being updated.  Reporting both
        under ``save`` understated one and polluted the other.
        """
        operation = "create" if absolute is not None else "save"
        with session_span(f"session.{operation}"), timed_session(operation):
            try:
                await self._write(
                    session_id,
                    self.encode(record),
                    idle=self.idle_seconds,
                    absolute=absolute,
                )
            except STORE_ERRORS as exc:
                record_session_operation(operation=operation, result="error")
                raise SessionStoreError(f"Could not save session: {exc}") from exc
        record_session_operation(operation=operation, result="hit")

    async def touch(self, session_id: str) -> None:
        """Restart the idle clock without rewriting the payload.

        Only needed under ``session_refresh_on_load=False``; the default load
        already refreshed in the same round trip as the read.

        A counter and no span: one command is not worth a span, but the count
        is the only way to confirm that ``refresh_on_load=False`` is doing
        anything at all.
        """
        try:
            await self._expire(session_id, self.idle_seconds)
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

        A new ID is a new key, and a new key starts its TTL - the absolute
        clock - at full length, so the age is that length minus what is left.
        Both numbers come from Redis, so a skewed container clock cannot make
        an old session look recent.

        ``None`` when the request has no stored session, or when the key has
        more time left than the configured length allows - a session created
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
        subject: str | None = None,
        descriptor: dict[str, Any] | None = None,
    ) -> str:
        """Issue a new identifier for *session*, deleting the old key first.

        This is the defence against session fixation, and the **middleware
        normally drives it** when the principal changes (Section 5.1).  It is
        public for the rare handler that must force one - a step-up that
        leaves no trace in session state.

        **The order is the security control, not an implementation detail.**
        The old key and its index entry go first, so an interrupted rotation
        signs the user out rather than leaving two identifiers that both work.
        Signed out is recoverable; two live identifiers after a privilege
        change is the fixation this design exists to prevent.

        The new session starts a fresh absolute clock, which is correct:
        rotation follows authentication or a privilege change, so the deadline
        should run from that moment rather than from whenever the anonymous
        session began.

        Returns:
            The new session ID.

        Raises:
            SessionStoreError: On any store failure.
        """
        with self._observe("rotate"):
            return await self._rotate(state, subject=subject, descriptor=descriptor)

    async def _rotate(
        self,
        state: SessionState,
        *,
        subject: str | None,
        descriptor: dict[str, Any] | None,
    ) -> str:
        """Body of :meth:`rotate`, so the span wraps the whole four trips."""
        subject = subject if subject is not None else state.subject
        old_id = state.session_id
        if old_id is not None:
            await self.delete(old_id)
            # The entry lives under the subject it was *written* with, which is
            # not always the one being written now - an account switch changes
            # it mid-request.
            if state.subject:
                await self._index_drop(state.subject, old_id)

        new_id = self.new_id()
        record = self.new_record(dict(state.data))
        await self.create(new_id, record)
        state.session_id = new_id
        state.created = record.metadata.created
        state.absolute_remaining = self.absolute_seconds
        # Cleared, not set. ``rotated`` means "the middleware still owes this
        # session a rotation"; we have just performed one. Leaving it set made
        # the middleware rotate a second time at response start, which threw
        # away the key written here and turned the ID returned to the caller
        # into a stale value. The middleware notices the new ID by comparing it
        # with the one it loaded, so the cookie still goes out.
        state.rotated = False
        state.revoked = False
        state.subject = subject
        if subject is not None:
            await self.index(
                subject,
                new_id,
                record,
                absolute_remaining=self.absolute_seconds,
                extra=descriptor,
            )
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

        *subject* drops the index entry alongside the key.  The middleware
        supplies it from the subject captured at load time, so a handler
        calling ``store.revoke(session)`` need not pass anything; pass it
        explicitly only when using the store outside a request.

        Leaving the entry behind is not cosmetic: ``list_for_subject`` prunes
        it on the next read but ``count_for_subject`` does not, so a
        concurrent-session cap counts sessions the user has already signed out
        of and eventually refuses a legitimate login.
        """
        old_id = state.session_id
        with self._observe("revoke"):
            if old_id is not None:
                await self.delete(old_id)
                resolved = subject if subject is not None else state.subject
                if resolved:
                    await self._index_drop(resolved, old_id)
            state.data.clear()
            state.session_id = None
            state.subject = None
            state.revoked = True
            state.rotated = False
        # ``miss`` is a session that was never written: there was no key to
        # delete and no cookie to replace, so nothing reached Redis.
        record_session_operation(
            operation="revoke", result="hit" if old_id is not None else "miss"
        )

    async def revoke_id(self, session_id: str, *, subject: str) -> bool:
        """End one session by ID, and refuse an ID not indexed under *subject*.

        The subject is **required** and checked, so a caller holding an
        arbitrary identifier cannot end a stranger's session.  Without it this
        method would be a cross-user revocation primitive reachable from any
        handler that takes an ID from a request.

        Returns:
            True if a session was ended, False if that ID is not this
            subject's - including when it has already expired.
        """
        if not self.is_valid_id(session_id):
            record_session_operation(operation="revoke_id", result="miss")
            return False
        with self._observe("revoke_id"):
            try:
                members = await self._index_members(subject)
            except STORE_ERRORS as exc:
                raise SessionStoreError(
                    f"Could not read the session index: {exc}"
                ) from exc
            if session_id not in members:
                # Worth its own label: a run of these is either a broken UI or
                # somebody trying identifiers that are not theirs.
                record_session_operation(operation="revoke_id", result="miss")
                return False
            await self.delete(session_id)
            await self._index_drop(subject, session_id)
            record_session_operation(operation="revoke_id", result="hit")
            return True

    async def revoke_all(self, subject: str) -> int:
        """End every session belonging to *subject*.

        Two round trips whatever the session count: one to read the index,
        one pipeline that deletes every key and then drops the index itself.

        Returns:
            How many session **keys** were removed, counted from the server's
            own ``DEL`` replies.  The index is an upper bound, so this is
            lower than the number of entries it held whenever some of those
            sessions had already died.
        """
        with self._observe("revoke_all"):
            try:
                members = await self._index_members(subject)
                if not members:
                    record_session_operation(operation="revoke_all", result="miss")
                    return 0
                removed = await self._delete_many(list(members))
                await self._index_clear(subject)
            except STORE_ERRORS as exc:
                raise SessionStoreError(f"Could not revoke sessions: {exc}") from exc
            record_session_operation(operation="revoke_all", result="hit")
            return removed

    # -- the reverse lookup ---------------------------------------------------

    async def index(
        self,
        subject: str,
        session_id: str,
        record: SessionRecord,
        *,
        absolute_remaining: int,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Record a session under its subject, expiring with it.

        Re-asserted on every write rather than only at login, which is what
        repairs an entry a partial failure lost.  The entry takes the
        **remaining** absolute time, never a fresh lifetime: a relative full
        lifetime restarts the entry's clock on every write and lets the index
        outlive the session it points at.

        *extra* is whatever the application wants a device listing to show -
        an IP, a user agent, a device name.  It is stored **beside** the
        session payload, never inside it, so it can never appear in the
        application's own ``request.session``.  The middleware fills it from
        the ``descriptor_of`` seam.
        """
        descriptor = self._coder.encode(
            {
                "c": record.metadata.created,
                "l": record.metadata.last_access,
                "d": dict(extra or {}),
            }
        )
        try:
            await self._index_add(subject, session_id, descriptor, absolute_remaining)
        except STORE_ERRORS as exc:
            raise SessionStoreError(f"Could not index the session: {exc}") from exc

    async def _index_drop(self, subject: str, session_id: str) -> None:
        """Remove one index entry, wrapping driver errors."""
        try:
            await self._index_remove(subject, session_id)
        except STORE_ERRORS as exc:
            raise SessionStoreError(
                f"Could not update the session index: {exc}"
            ) from exc

    async def list_for_subject(self, subject: str) -> list[SessionInfo]:
        """Live sessions for *subject*, verified before they are reported.

        **The index is an upper bound.**  Most sessions die of idleness long
        before their absolute deadline, and the entry's TTL is the absolute
        deadline, so an entry routinely outlives the session it names.
        Reporting one would show a user a device they are no longer signed in
        on, and offer them a "sign out" button that does nothing.

        So every candidate is checked against its session key, and dead
        entries are pruned on the way past.  Two round trips whatever the
        session count: one to read the index, one to verify the batch.
        """
        # Deliberately not ``_observe``: this read fails **open**, so the
        # error has to be counted where the exception is swallowed.  Under
        # ``fail_closed`` it escapes as well, and ``_observe`` would then
        # count the same failure a second time.
        with session_span("session.list"), timed_session("list"):
            try:
                members = await self._index_members(subject)
            except STORE_ERRORS as exc:
                record_session_operation(operation="list", result="error")
                self._read_failed(exc)
                return []
            if not members:
                record_session_operation(operation="list", result="miss")
                return []

            live = await self._verify(list(members))
            infos: list[SessionInfo] = []
            dead: list[str] = []
            for session_id, raw in members.items():
                if live is not None and session_id not in live:
                    dead.append(session_id)
                    continue
                infos.append(self._to_info(session_id, raw))
            for session_id in dead:
                # Best-effort tidy-up on a read path: failing a device listing
                # because we could not prune a stale row helps nobody.
                try:
                    await self._index_remove(subject, session_id)
                except STORE_ERRORS as exc:
                    logger.warning("Could not prune a dead index entry: %s", exc)
            infos.sort(key=lambda info: info.last_access, reverse=True)
            record_session_operation(
                operation="list", result="hit" if infos else "miss"
            )
            return infos

    async def count_for_subject(self, subject: str, *, limit: int | None = None) -> int:
        """How many sessions *subject* has, as an upper bound by default.

        The fast path is ``_index_size`` - one ``HLEN`` - and it transfers a
        single integer.  That is what makes a "cap concurrent sessions" check
        cheap on every login: reading the members instead would ship every
        identifier **and every descriptor** across the wire to compute their
        length, and a descriptor holds whatever ``descriptor_of`` returns.

        Pass *limit* to make the count exact **only when it matters**: below
        the limit the fast answer is returned, and both the members read and
        the verification round trip are paid solely by the request that is
        about to be refused.

        **This one does not fail open.**  Section 7's argument for an empty
        session on a failed read is that the application's own authorization
        still runs; here the store *is* the answer, and ``0`` is the
        permissive one - a concurrent-session cap would wave every login
        through exactly when Redis is unhealthy.

        Raises:
            SessionStoreError: If the count could not be established.
        """
        with self._observe("count"):
            try:
                upper = await self._index_size(subject)
                if limit is None or upper < limit:
                    record_session_operation(operation="count", result="hit")
                    return upper
                members = await self._index_members(subject)
            except STORE_ERRORS as exc:
                raise SessionStoreError(
                    f"Could not count sessions for the subject: {exc}"
                ) from exc
            live = await self._verify(list(members))
            if live is None:
                raise SessionStoreError(
                    "Could not verify session liveness while counting; refusing to "
                    "answer rather than under-count."
                )
            record_session_operation(operation="count", result="hit")
            return len(live)

    async def _verify(self, session_ids: list[str]) -> set[str] | None:
        """Which of *session_ids* are alive, or ``None`` if we could not ask.

        One batch, not one call per session, so a subject with fifty devices
        costs the same round trip as one with two.

        **``None`` and the empty set mean different things, and conflating
        them destroys the index.**  An earlier version returned an empty set
        on failure; ``list_for_subject`` then read "absent from the live set"
        as proof of death and ``HDEL``-ed every entry for the subject.  One
        transient pipeline error - the ordinary case on a cluster mid-failover
        - left every live session of that user invisible to ``revoke_all``
        until its absolute deadline.  Callers must treat ``None`` as "report
        everything, prune nothing".
        """
        try:
            return await self._alive(session_ids)
        except STORE_ERRORS as exc:
            logger.warning("Could not verify session liveness: %s", exc)
            return None

    def _to_info(self, session_id: str, raw: bytes | str) -> SessionInfo:
        """Build a listing row from an index descriptor alone.

        Never reads the session record: that is the point of storing the
        descriptor beside the identifier.
        """
        text = raw if isinstance(raw, str) else raw.decode()
        try:
            payload = self._coder.decode(text)
            return SessionInfo(
                session_id=session_id,
                created=float(payload.get("c", 0.0)),
                last_access=float(payload.get("l", 0.0)),
                descriptor=dict(payload.get("d", {})),
            )
        except Exception:
            # A descriptor is display data. A malformed one must not hide a
            # live session from the user who is trying to sign it out.
            logger.warning("Unreadable index descriptor for a live session")
            return SessionInfo(
                session_id=session_id, created=0.0, last_access=0.0, descriptor={}
            )

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
    # Eleven methods.  Underscore-prefixed because no application calls them,
    # *not* because a backend may not override them - overriding them is the
    # only way to write one, and Python refuses to instantiate a subclass that
    # leaves one out.  Each docstring below carries the rule that is easy to
    # get wrong, because these docstrings are the only statement of the
    # contract: it is deliberately not described in the published docs.

    @abstractmethod
    async def _read(
        self, session_id: str, *, refresh_idle: int | None
    ) -> tuple[bytes | str | None, int | Deadline]:
        """Return ``(payload or None, the absolute deadline)``.

        The deadline is the number of seconds left, or a :class:`Deadline`
        member when it is not a number.  Do not invent negative sentinels -
        the base class does not interpret them.

        *refresh_idle* is the idle window in seconds, or ``None`` to read
        without refreshing.  A backend that can do both in one round trip
        should; one that cannot may take two.
        """

    @abstractmethod
    async def _write(
        self, session_id: str, payload: str, *, idle: int, absolute: int | None
    ) -> None:
        """Write the payload with an idle TTL.

        *absolute* is ``None`` on an update, and the deadline must then be
        left completely alone - neither refreshed nor recreated.  When it is
        an int this is a create, and the session takes that deadline.
        """

    @abstractmethod
    async def _expire(self, session_id: str, idle: int) -> None:
        """Restart the idle clock, leaving the payload and the deadline alone."""

    @abstractmethod
    async def _delete(self, session_id: str) -> None:
        """Remove the session entirely."""

    @abstractmethod
    async def _index_add(
        self, subject: str, session_id: str, descriptor: str, absolute_remaining: int
    ) -> None:
        """Record *session_id* under *subject*, expiring with the session.

        *absolute_remaining* is what is left of the session's absolute clock,
        never the full lifetime: a full lifetime here would restart the entry's
        clock on every write and let the index outlive the session it points at.
        """

    @abstractmethod
    async def _index_remove(self, subject: str, session_id: str) -> None:
        """Drop one session from a subject's index."""

    @abstractmethod
    async def _delete_many(self, session_ids: list[str]) -> int:
        """Remove many sessions in one round trip; return how many existed."""

    @abstractmethod
    async def _index_clear(self, subject: str) -> None:
        """Drop a subject's whole index in one command."""

    @abstractmethod
    async def _alive(self, session_ids: list[str]) -> set[str]:
        """Return which of *session_ids* still have a live session."""

    @abstractmethod
    async def _index_members(self, subject: str) -> dict[str, bytes | str]:
        """Return ``{session_id: descriptor}`` for a subject.

        An **upper bound**: entries can name sessions that have since died, so
        a caller must verify before reporting them.
        """

    @abstractmethod
    async def _index_size(self, subject: str) -> int:
        """How many entries a subject's index holds.

        The same **upper bound** as ``_index_members``, and it exists so that
        ``count_for_subject`` need not read the descriptors to count them.
        Implement it with whatever counts without transferring the values -
        ``HLEN`` on a hash, ``SCARD`` on a set.  Falling back to
        ``len(await self._index_members(subject))`` is correct but gives up
        the only reason this method is separate.
        """


class RedisSessionStore(SessionStore):
    """The Redis implementation of the storage primitives.

    Everything here is one pipelined round trip per operation.  Both keys are
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
        self._session_prefix = f"{base}:session:"
        self._index_prefix = f"{base}:sessions-of:"

    # -- keys ----------------------------------------------------------------

    def session_key(self, session_id: str) -> str:
        """``<prefix>:session:<sid>``."""
        return f"{self._session_prefix}{session_id}"

    def index_key(self, subject: str) -> str:
        """``<prefix>:sessions-of:<subject>``.

        A different prefix from :meth:`session_key`, not a nested one.  The two
        strings first differ at the character after ``session``, before either
        key's variable part begins, so no session ID - however exotic - can
        produce the index key.  An earlier layout nested them and rested on
        ``token_urlsafe`` never emitting a ``:``, which stopped being true the
        moment ``id_factory`` became a supported seam.
        """
        return f"{self._index_prefix}{subject}"

    # -- capability ----------------------------------------------------------

    async def _has_hsetex(self) -> bool:
        """Whether to use the 8.0 commands.  Probed once, then cached."""
        if self._caps.supports_hsetex is None:
            self._caps.supports_hsetex = await probe_hsetex_support(self._redis)
        # A probe that could not reach the server answers None. Take the
        # 7.4 path: it works everywhere, so an unknown answer costs a command
        # rather than an error.
        return bool(self._caps.supports_hsetex)

    # -- the primitives ------------------------------------------------------

    async def _read(
        self, session_id: str, *, refresh_idle: int | None
    ) -> tuple[bytes | str | None, int | Deadline]:
        key = self.session_key(session_id)
        pipe = self._redis.pipeline(transaction=False)
        if refresh_idle is None:
            pipe.execute_command("HGET", key, FIELD_DATA)
            reads = 1
        elif await self._has_hsetex():
            # HGETEX reads the field and sets its expiration in one command,
            # so the load *is* the idle refresh: no second command at response
            # time, and nothing for a refresh threshold to optimise away.
            pipe.execute_command(
                "HGETEX", key, "EX", refresh_idle, "FIELDS", 1, FIELD_DATA
            )
            reads = 1
        else:
            pipe.execute_command("HGET", key, FIELD_DATA)
            pipe.execute_command("HEXPIRE", key, refresh_idle, "FIELDS", 1, FIELD_DATA)
            reads = 2
        pipe.execute_command("TTL", key)
        replies = await pipe.execute()
        return _first(replies[0]), _ttl(replies[reads])

    async def _write(
        self, session_id: str, payload: str, *, idle: int, absolute: int | None
    ) -> None:
        key = self.session_key(session_id)
        modern = await self._has_hsetex()
        pipe = self._redis.pipeline(transaction=False)
        if modern:
            pipe.execute_command(
                "HSETEX", key, "EX", idle, "FIELDS", 1, FIELD_DATA, payload
            )
        else:
            # HSET clears a field's TTL, so the idle clock is reapplied here.
            pipe.execute_command("HSET", key, FIELD_DATA, payload)
            pipe.execute_command("HEXPIRE", key, idle, "FIELDS", 1, FIELD_DATA)
        if absolute is not None:
            # A create. EXPIRE on a missing key does nothing, so the deadline
            # follows the write that creates the key. A pipeline cut between
            # the two leaves a key with no TTL, which the load treats as over;
            # the write raised, so no cookie names it anyway.
            pipe.execute_command("EXPIRE", key, absolute)
        await pipe.execute()

    async def _expire(self, session_id: str, idle: int) -> None:
        await self._redis.execute_command(
            "HEXPIRE", self.session_key(session_id), idle, "FIELDS", 1, FIELD_DATA
        )

    async def _delete(self, session_id: str) -> None:
        await self._redis.delete(self.session_key(session_id))

    async def _index_add(
        self, subject: str, session_id: str, descriptor: str, absolute_remaining: int
    ) -> None:
        key = self.index_key(subject)
        if absolute_remaining <= 0:
            # Nothing to record: the session it would point at is already gone.
            return
        if await self._has_hsetex():
            await self._redis.execute_command(
                "HSETEX",
                key,
                "EX",
                absolute_remaining,
                "FIELDS",
                1,
                session_id,
                descriptor,
            )
            return
        pipe = self._redis.pipeline(transaction=False)
        pipe.execute_command("HSET", key, session_id, descriptor)
        pipe.execute_command(
            "HEXPIRE", key, absolute_remaining, "FIELDS", 1, session_id
        )
        await pipe.execute()

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

    async def _index_clear(self, subject: str) -> None:
        await self._redis.delete(self.index_key(subject))

    async def _index_remove(self, subject: str, session_id: str) -> None:
        await self._redis.hdel(self.index_key(subject), session_id)

    async def _index_members(self, subject: str) -> dict[str, bytes | str]:
        raw = await self._redis.hgetall(self.index_key(subject))
        return {(k.decode() if isinstance(k, bytes) else k): v for k, v in raw.items()}

    async def _index_size(self, subject: str) -> int:
        return int(await self._redis.hlen(self.index_key(subject)))

    async def _alive(self, session_ids: list[str]) -> set[str]:
        """One pipelined ``HTTL`` and ``TTL`` per candidate, in a single batch.

        A session is alive only while field ``d`` and the key both have time
        left - the same rule as the load.  Redis deletes the key at the
        absolute deadline, so ``d`` alone is usually enough; the key's TTL
        also catches a key a late save recreated with no deadline, which the
        load treats as over.  One more command in the same round trip.
        """
        if not session_ids:
            return set()
        pipe = self._redis.pipeline(transaction=False)
        for session_id in session_ids:
            key = self.session_key(session_id)
            pipe.execute_command("HTTL", key, "FIELDS", 1, FIELD_DATA)
            pipe.execute_command("TTL", key)
        replies = await pipe.execute()
        return {
            session_id
            for session_id, idle, deadline in zip(
                session_ids, replies[0::2], replies[1::2], strict=True
            )
            if _has_time_left(idle) and _has_time_left(deadline)
        }


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


def _first(reply: Any) -> bytes | str | None:
    """Unwrap a one-element array reply.

    ``HGETEX`` answers with an array even for a single field, while ``HGET``
    answers with the value itself.  Both paths land here.
    """
    if isinstance(reply, (list, tuple)):
        first = reply[0] if reply else None
    else:
        first = reply
    if first is None or isinstance(first, (bytes, str)):
        return first
    return str(first)


def _has_time_left(reply: Any) -> bool:
    """Whether a ``TTL`` reply, or a one-field ``HTTL`` reply, is positive.

    An empty or malformed reply reads as "not alive": the safe answer for a
    liveness check is to omit the session rather than to report one that may
    already be gone.
    """
    value = reply[0] if isinstance(reply, (list, tuple)) and reply else reply
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return False


def _ttl(reply: Any) -> int | Deadline:
    """Map the session key's ``TTL`` reply onto the deadline contract.

    This is the only place that knows what ``-1`` and ``-2`` mean, which is
    the point: the encoding stays inside the Redis backend.
    """
    if reply is None:
        return Deadline.ABSENT
    seconds = int(reply)
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
        subject: str | None = None,
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
