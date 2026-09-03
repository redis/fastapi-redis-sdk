"""Redis-backed session store.

Two keys, both hashes with a time-to-live on each field::

    redis:fastapi:session:<sid>          field "a" = "1"        TTL = absolute
                                         field "d" = <envelope> TTL = idle
    redis:fastapi:sessions-of:<subject>  field <sid> = <descriptor>
                                                                TTL = absolute

Splitting the two clocks across two fields is what makes "a session outlives
its absolute deadline" unreachable rather than merely unlikely: Redis enforces
both deadlines and this module computes neither.  Writing the payload touches
field ``d`` alone, so no number of writes can extend field ``a``.

See ``docs/specs/session-design.md`` Sections 3 and 4.
"""

from __future__ import annotations

import logging
import secrets
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.exceptions import RedisError

from redis_fastapi.config import get_settings
from redis_fastapi.sessions import (
    Session,
    SessionConfigurationError,
    SessionStoreError,
)
from redis_fastapi.telemetry import (
    record_session_operation,
    session_span,
    timed_session,
)
from redis_fastapi.types import Coder, JsonCoder

logger = logging.getLogger(__name__)

# Hash field names.  Two characters, and identical in every session key.
# Section 13.2: a uniform schema is what a future compact-hash encoding would
# reward, and a short name is fewer bytes on the wire meanwhile.
FIELD_ABSOLUTE = "a"
FIELD_DATA = "d"

# The value of field ``a`` is never read - the field exists for its TTL alone.
_ABSOLUTE_MARKER = "1"

# ``HTTL`` answers -2 for "no such field, or no such key" and -1 for "the field
# exists with no expiry".  Naming them stops either being read as a duration.
TTL_NO_FIELD = -2
TTL_NO_EXPIRY = -1

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
    except (RedisError, OSError):
        return None
    if not reply:
        return False
    first = reply[0] if isinstance(reply, (list, tuple)) else reply
    return first is not None


@dataclass(frozen=True)
class LoadedSession:
    """A live session, plus the deadline Redis is counting down for it.

    ``absolute_remaining`` comes from ``HTTL`` on field ``a`` - the server's
    own number, never one this process computed.  Section 6 needs it to size
    the cookie's ``max-age`` as ``min(idle, absolute_remaining)`` so the cookie
    and the record cannot disagree.
    """

    record: SessionRecord
    absolute_remaining: int


class SessionStore(ABC):
    """Owns the session lifecycle; a subclass owns only the storage.

    The lifecycle is a security control, so it is written once here rather
    than once per backend.  A new backend implements the seven abstract
    primitives at the bottom of this class and inherits the identifier rules,
    the two-clock policy, the envelope format and the error handling.

    Section 7 of ``session-mgmt.md`` gives the reason this is an abstract base
    class and not a bare protocol.  ``SessionStoreProtocol`` describes the much
    smaller surface the middleware calls, for a caller who wants no
    inheritance.
    """

    def __init__(
        self,
        *,
        coder: type[Coder] = JsonCoder,
        idle_ttl: int | timedelta | None = None,
        absolute_ttl: int | timedelta | None = None,
        gc_ttl: int | timedelta | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        settings = get_settings()
        self._coder = coder
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
        falls back to ``gc_ttl`` rather than being left unexpiring.  Section
        3.2: a field with no TTL breaks the meaning of ``HTTL``'s ``-2``.
        """
        return self._idle_ttl or self._gc_ttl

    @property
    def absolute_seconds(self) -> int:
        """TTL for field ``a``, set once at creation and never refreshed.

        ``session_absolute_ttl=0`` disables the absolute clock, and the field
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
        return encoded

    def decode(self, raw: str | bytes) -> SessionRecord:
        """Parse the envelope; raise :class:`SessionStoreError` if it is junk.

        A corrupted record is treated as unreadable rather than as an empty
        session, because the two mean different things: the caller decides
        whether to sign the user out or to fail the request.
        """
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

    def new_record(self, data: dict[str, Any]) -> SessionRecord:
        """Build a record for a session that does not exist yet."""
        now = time.time()
        return SessionRecord(
            data=data,
            metadata=SessionMetadata(
                created=now, last_access=now, lifetime=self._absolute_ttl
            ),
        )

    # -- lifecycle -----------------------------------------------------------

    async def load(
        self, session_id: str, *, refresh: bool = True
    ) -> LoadedSession | None:
        """Read a session, and restart its idle clock in the same round trip.

        Returns ``None`` for every way a session can fail to exist: never
        created, idle-expired, past its absolute deadline, or revoked.  The
        caller cannot tell those apart, and must not: to an application they
        are all "no session".

        *refresh* false makes this a plain read, for
        ``session_refresh_on_load=False``.  The idle clock then advances only
        when the response writes.

        The two answers are read **as a pair**.  Neither one is a sentinel on
        its own: ``HTTL`` returns ``-2`` both for "this field expired" and for
        "there is no such key", so reading it alone marks every session in a
        deployment with no absolute limit as already dead.

        Raises:
            SessionStoreError: If the record exists but cannot be decoded, or
                if the read failed and ``session_fail_closed`` is set.
        """
        if not self.is_valid_id(session_id):
            return None
        with session_span("session.load"), timed_session("load"):
            try:
                raw, absolute_ttl = await self._read(
                    session_id, refresh_idle=self.idle_seconds if refresh else None
                )
            except (RedisError, OSError) as exc:
                record_session_operation(operation="load", result="error")
                self._read_failed(exc)
                return None

        if absolute_ttl == TTL_NO_EXPIRY:
            # Unreachable by construction: every write gives field "a" a TTL.
            # Reaching it means something wrote the key outside this store, so
            # say so loudly and treat the session as absent rather than guess.
            logger.error(
                "Session key has a field 'a' with no expiry, which this store "
                "never writes. Treating the session as absent. Key was "
                "written by something else, or by an older version."
            )
            await self._safe_delete(session_id)
            return None

        if raw is None or absolute_ttl <= 0:
            # Rows two and four of the state table: a half-dead key, alive on
            # one clock and dead on the other. Delete it so the index entry can
            # follow, rather than leaving a candidate that every later
            # verification has to reject. Row three - dead on both - is simply
            # absent, and there is nothing to remove.
            if raw is not None or absolute_ttl > 0:
                await self._safe_delete(session_id)
            record_session_operation(
                operation="load",
                result="expired" if raw is not None or absolute_ttl > 0 else "miss",
            )
            return None

        record_session_operation(operation="load", result="hit")
        return LoadedSession(record=self.decode(raw), absolute_remaining=absolute_ttl)

    async def save(self, session_id: str, record: SessionRecord) -> None:
        """Write the payload, leaving the absolute deadline untouched.

        Field ``a`` is written **if it does not already exist**, so one call
        covers both creating a session and updating one, and no number of
        updates can extend the absolute deadline.  That conditional write is
        the whole of N-6: outliving the deadline is not a bug to avoid here,
        it is unreachable.

        Raises:
            SessionStoreError: On any store failure.  A write never fails
                quietly, whatever ``session_fail_closed`` says - losing a login
                or a rotation is the worst outcome in this design.
        """
        with session_span("session.save"), timed_session("save"):
            try:
                await self._write(
                    session_id,
                    self.encode(record),
                    idle=self.idle_seconds,
                    absolute=self.absolute_seconds,
                )
            except (RedisError, OSError) as exc:
                record_session_operation(operation="save", result="error")
                raise SessionStoreError(f"Could not save session: {exc}") from exc
        record_session_operation(operation="save", result="hit")

    async def touch(self, session_id: str) -> None:
        """Restart the idle clock without rewriting the payload.

        Only needed under ``session_refresh_on_load=False``; the default load
        already refreshed in the same round trip as the read.
        """
        try:
            await self._expire(session_id, self.idle_seconds)
        except (RedisError, OSError) as exc:
            raise SessionStoreError(f"Could not refresh session: {exc}") from exc

    async def delete(self, session_id: str) -> None:
        """Remove a session outright.

        Raises:
            SessionStoreError: On any store failure.  Deletion is revocation,
                so a failure here must reach the caller.
        """
        try:
            await self._delete(session_id)
        except (RedisError, OSError) as exc:
            raise SessionStoreError(f"Could not delete session: {exc}") from exc

    # -- rotation and revocation ---------------------------------------------

    def session_id(self, session: Session) -> str | None:
        """The identifier this session is stored under, if it has one yet.

        ``None`` for a session that has never been written.  Useful for
        marking "this device" in a listing.
        """
        return session.sid

    async def rotate(
        self,
        session: Session,
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
        old_id = session.sid
        if old_id is not None:
            await self.delete(old_id)
            if subject is not None:
                await self._index_drop(subject, old_id)

        new_id = self.new_id()
        record = self.new_record(session.raw())
        await self.save(new_id, record)
        session.sid = new_id
        # Cleared, not set. ``rotated`` means "the middleware still owes this
        # session a rotation"; we have just performed one. Leaving it set made
        # the middleware rotate a second time at response start, which threw
        # away the key written here and turned the ID returned to the caller
        # into a stale value. The middleware notices the new ID by comparing it
        # with the one it loaded, so the cookie still goes out.
        session.rotated = False
        session.revoked = False
        if subject is not None:
            await self.index(
                subject, new_id, record, absolute_remaining=self.absolute_seconds
            )
        # The runtime backstop for a misconfigured principal resolver: sign-ins
        # with no rotations is a visible anomaly on a dashboard, and a silent
        # misconfiguration is the one genuine cost of detecting rather than
        # being told.
        record_session_operation(operation="rotate", result="hit")
        return new_id

    async def reauthenticate(self, session: Session) -> None:
        """Force a rotation on the way out of this request.

        For a privilege change the principal cannot see - an impersonation
        that leaves no trace in session state, or a step-up before a sensitive
        action.  The middleware performs the rotation at
        ``http.response.start`` so it lands in the same response as the new
        cookie.
        """
        session.rotated = True
        session.mark_modified()

    async def revoke(self, session: Session) -> None:
        """End the session in hand and clear its cookie.

        Safe to call on a session that was never written.
        """
        old_id = session.sid
        if old_id is not None:
            await self.delete(old_id)
        session.clear()
        session.sid = None
        session.revoked = True
        session.rotated = False

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
            return False
        try:
            members = await self._index_members(subject)
        except (RedisError, OSError) as exc:
            raise SessionStoreError(f"Could not read the session index: {exc}") from exc
        if session_id not in members:
            return False
        await self.delete(session_id)
        await self._index_drop(subject, session_id)
        return True

    async def revoke_all(self, subject: str) -> int:
        """End every session belonging to *subject*.

        Returns:
            How many session keys were removed.  The index is an upper bound,
            so this can be lower than the number of entries it held - the
            difference is sessions that had already died.
        """
        try:
            members = await self._index_members(subject)
            removed = 0
            for session_id in members:
                await self._delete(session_id)
                removed += 1
            for session_id in members:
                await self._index_remove(subject, session_id)
        except (RedisError, OSError) as exc:
            record_session_operation(operation="revoke_all", result="error")
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
    ) -> None:
        """Record a session under its subject, expiring with it.

        Re-asserted on every write rather than only at login, which is what
        repairs an entry a partial failure lost.  The entry takes the
        **remaining** absolute time, never a fresh lifetime: a relative full
        lifetime restarts the entry's clock on every write and lets the index
        outlive the session it points at.
        """
        descriptor = self._coder.encode(
            {
                "c": record.metadata.created,
                "l": record.metadata.last_access,
                "d": record.data.get("__descriptor__", {}),
            }
        )
        try:
            await self._index_add(subject, session_id, descriptor, absolute_remaining)
        except (RedisError, OSError) as exc:
            raise SessionStoreError(f"Could not index the session: {exc}") from exc

    async def _index_drop(self, subject: str, session_id: str) -> None:
        """Remove one index entry, wrapping driver errors."""
        try:
            await self._index_remove(subject, session_id)
        except (RedisError, OSError) as exc:
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
        try:
            members = await self._index_members(subject)
        except (RedisError, OSError) as exc:
            self._read_failed(exc)
            return []
        if not members:
            return []

        live = await self._verify(list(members))
        infos: list[SessionInfo] = []
        dead: list[str] = []
        for session_id, raw in members.items():
            if session_id not in live:
                dead.append(session_id)
                continue
            infos.append(self._to_info(session_id, raw))
        for session_id in dead:
            # Best-effort tidy-up on a read path: failing a device listing
            # because we could not prune a stale row helps nobody.
            try:
                await self._index_remove(subject, session_id)
            except (RedisError, OSError) as exc:
                logger.warning("Could not prune a dead index entry: %s", exc)
        infos.sort(key=lambda info: info.last_access, reverse=True)
        return infos

    async def count_for_subject(self, subject: str, *, limit: int | None = None) -> int:
        """How many sessions *subject* has, as an upper bound by default.

        Counting the index is ``O(1)`` and needs no verification, which is what
        makes a "cap concurrent sessions" check cheap on every login.  Pass
        *limit* to make the count exact **only when it matters**: below the
        limit the fast answer is returned, and the verification round trip is
        paid solely by the request that is about to be refused.
        """
        try:
            members = await self._index_members(subject)
        except (RedisError, OSError) as exc:
            self._read_failed(exc)
            return 0
        upper = len(members)
        if limit is None or upper < limit:
            return upper
        return len(await self._verify(list(members)))

    async def _verify(self, session_ids: list[str]) -> set[str]:
        """Return the subset of *session_ids* whose sessions are still alive.

        One batch, not one call per session, so a subject with fifty devices
        costs the same round trip as one with two.
        """
        try:
            return await self._alive(session_ids)
        except (RedisError, OSError) as exc:
            logger.warning("Could not verify session liveness: %s", exc)
            return set()

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

    @abstractmethod
    async def _alive(self, session_ids: list[str]) -> set[str]:
        """Return which of *session_ids* still have a live session."""

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
        except (RedisError, OSError) as exc:
            logger.warning("Could not remove a dead session key: %s", exc)

    # -- the whole surface a new backend implements --------------------------

    @abstractmethod
    async def _read(
        self, session_id: str, *, refresh_idle: int | None
    ) -> tuple[bytes | str | None, int]:
        """Return ``(payload or None, seconds left on the absolute clock)``.

        *refresh_idle* is the idle window in seconds, or ``None`` to read
        without refreshing.  A backend that can do both in one round trip
        should; one that cannot may take two.
        """

    @abstractmethod
    async def _write(
        self, session_id: str, payload: str, *, idle: int, absolute: int
    ) -> None:
        """Write the payload with an idle TTL.

        Must give the absolute deadline its TTL **only when it does not
        already exist**, so that repeated writes cannot extend it.
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
    async def _index_members(self, subject: str) -> dict[str, bytes | str]:
        """Return ``{session_id: descriptor}`` for a subject.

        An **upper bound**: entries can name sessions that have since died, so
        a caller must verify before reporting them.
        """


class RedisSessionStore(SessionStore):
    """The Redis implementation of the seven storage primitives.

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
    ) -> tuple[bytes | str | None, int]:
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
        pipe.execute_command("HTTL", key, "FIELDS", 1, FIELD_ABSOLUTE)
        replies = await pipe.execute()
        return _first(replies[0]), _ttl(replies[reads])

    async def _write(
        self, session_id: str, payload: str, *, idle: int, absolute: int
    ) -> None:
        key = self.session_key(session_id)
        pipe = self._redis.pipeline(transaction=False)
        if await self._has_hsetex():
            # FNX: set only if the field does not already exist. On an update
            # the whole command is a no-op, so field "a" keeps the deadline it
            # was created with.
            pipe.execute_command(
                "HSETEX",
                key,
                "FNX",
                "EX",
                absolute,
                "FIELDS",
                1,
                FIELD_ABSOLUTE,
                _ABSOLUTE_MARKER,
            )
            pipe.execute_command(
                "HSETEX", key, "EX", idle, "FIELDS", 1, FIELD_DATA, payload
            )
        else:
            # The 7.4 spelling of the same two writes. HSETNX will not touch an
            # existing field, and HEXPIRE's NX only sets an expiry on a field
            # that has none - which, given the line above, is only ever a field
            # this call just created.
            pipe.execute_command("HSETNX", key, FIELD_ABSOLUTE, _ABSOLUTE_MARKER)
            pipe.execute_command(
                "HEXPIRE", key, absolute, "NX", "FIELDS", 1, FIELD_ABSOLUTE
            )
            pipe.execute_command("HSET", key, FIELD_DATA, payload)
            pipe.execute_command("HEXPIRE", key, idle, "FIELDS", 1, FIELD_DATA)
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

    async def _index_remove(self, subject: str, session_id: str) -> None:
        await self._redis.hdel(self.index_key(subject), session_id)

    async def _index_members(self, subject: str) -> dict[str, bytes | str]:
        raw = await self._redis.hgetall(self.index_key(subject))
        return {(k.decode() if isinstance(k, bytes) else k): v for k, v in raw.items()}

    async def _alive(self, session_ids: list[str]) -> set[str]:
        """One pipelined ``HTTL`` per candidate, sent as a single batch.

        **Both clocks are checked, not one.**  A session is alive only while
        field ``d`` and field ``a`` both have time left, and the two die for
        different reasons: ``d`` when the user goes idle, ``a`` when the
        absolute deadline passes however active they were.

        Asking about ``d`` alone would report a session past its absolute
        deadline as live, because ``d`` may have been refreshed minutes ago and
        still hold most of the idle window.  It is tempting to argue that the
        case cannot arise - the index entry carries the same absolute deadline
        as field ``a``, so it should expire at the same moment and never become
        a candidate.  That is true today and it is not a guarantee: it holds
        only while every write re-asserts the entry with the *remaining*
        absolute time, which is one refactor away from being wrong.  Asking
        about both fields costs nothing and does not depend on the argument.
        """
        if not session_ids:
            return set()
        pipe = self._redis.pipeline(transaction=False)
        for session_id in session_ids:
            pipe.execute_command(
                "HTTL",
                self.session_key(session_id),
                "FIELDS",
                2,
                FIELD_DATA,
                FIELD_ABSOLUTE,
            )
        replies = await pipe.execute()
        return {
            session_id
            for session_id, reply in zip(session_ids, replies, strict=True)
            if _all_ttls_positive(reply)
        }


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


def _all_ttls_positive(reply: Any) -> bool:
    """Whether every TTL in a multi-field ``HTTL`` reply has time left.

    An empty or malformed reply reads as "not alive": the safe answer for a
    liveness check is to omit the session rather than to report one that may
    already be gone.
    """
    if not isinstance(reply, (list, tuple)) or not reply:
        return False
    return all(value is not None and int(value) > 0 for value in reply)


def _ttl(reply: Any) -> int:
    """Unwrap ``HTTL``'s one-element array reply into an int."""
    value = reply[0] if isinstance(reply, (list, tuple)) and reply else reply
    if value is None:
        return TTL_NO_FIELD
    return int(value)


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

    def session_id(self, session: Session) -> str | None:
        """The identifier this session is stored under, if any (no I/O)."""
        return self._store.session_id(session)

    def rotate(
        self,
        session: Session,
        *,
        subject: str | None = None,
        descriptor: dict[str, Any] | None = None,
    ) -> str:
        """Issue a new identifier, deleting the old key first (blocking)."""
        result: str = self._run(
            lambda: self._store.rotate(session, subject=subject, descriptor=descriptor)
        )
        return result

    def reauthenticate(self, session: Session) -> None:
        """Force a rotation on the way out of this request (blocking)."""
        self._run(lambda: self._store.reauthenticate(session))

    def revoke(self, session: Session) -> None:
        """End the session in hand and clear its cookie (blocking)."""
        self._run(lambda: self._store.revoke(session))

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
