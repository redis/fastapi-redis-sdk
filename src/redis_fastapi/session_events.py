"""Real-time session-end events, driven by Redis keyspace notifications.

A session store knows when a session dies.  Every other backend discovers it
on the next request, because a row that expired quietly tells nobody.  Redis
can say so as it happens, and this module turns that into an application
callback - closing a WebSocket the moment a user is signed out being the case
that pays for it.

**Two properties matter more than the feature itself.**

*Nothing depends on it.*  Pub/Sub is fire-and-forget, events sent while no
subscriber is connected are lost, and an expiry event fires when Redis removes
the field or the key rather than when the deadline passed.  So the index still
prunes itself through field expiry and the load is still the authority on
whether a session is alive.  Every guarantee in the store holds with this
module switched off, which is why it is off by default.

*It degrades to silence.*  On a server that cannot supply the events, handlers
are registered and never called.  That is a deliberate choice with a sharp
edge: a revocation handler that never fires looks exactly like one that works.
An application that closes sockets on this signal and nothing else will hold
them open after a sign-out on such a server.  The guide must say so; this
module logs one warning at startup and does nothing further about it.

See ``docs/specs/session-design.md`` Section 13.4.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

from redis_fastapi.session_backend import STORE_ERRORS
from redis_fastapi.telemetry import record_session_event

logger = logging.getLogger(__name__)

Tier = Literal["none", "key"]

# Two members, and there is no third.  ``Cause`` is a ``Literal`` in a public
# callback signature, so it is a promise about which values a handler can be
# called with: a caller writing the exhaustive ``match`` that a type checker
# rewards must not be left with an arm that can never run and that mypy will
# not let them delete.
#
# **Revocation is deliberately absent.**  ``revoke`` is a ``DEL`` of the whole
# key, which publishes a ``del`` event - the same event Redis publishes when an
# idle expiry empties the hash.  So a ``del`` cannot say whether the session
# was revoked or went idle, and this module does not subscribe to it.  If a
# revocation event is ever needed, widening this union is the ordinary cost of
# widening any union - smaller than shipping a member nothing can produce.
Cause = Literal["idle", "absolute"]

# Exported, so a caller under mypy --strict can name the type of the callable
# ``on_session_end`` requires them to pass.
Handler = Callable[[str, Cause], Awaitable[None]]

# Hash-field expiry, and with it the ``hexpired`` event, arrived in Redis 7.4,
# which is also this package's floor.
_MIN_VERSION = (7, 4)

# Two key-level channels, one for each clock.  The payload of both is the key
# name, and nothing else.
#
# * ``hexpired``: a hash field expired.  Field ``d`` is the only field of a
#   session key that has a TTL, so on a session key this is the idle clock.
# * ``expired``: a key expired.  The absolute deadline is the session key's
#   TTL, so on a session key this is the absolute clock.
#
# Neither fires for the other clock.  Checked against Redis 8.7: an idle
# expiry publishes ``hexpired`` and then ``del`` - the hash is empty - and no
# ``expired``; an absolute expiry publishes ``expired`` and no ``hexpired``.
_IDLE_CHANNEL = "__keyevent@{db}__:hexpired"
_ABSOLUTE_CHANNEL = "__keyevent@{db}__:expired"

# Flags that must be present in ``notify-keyspace-events``: ``E`` for the
# ``__keyevent@`` channels, ``h`` for hash events, which include ``hexpired``,
# and ``x`` for ``expired``.
#
# ``A`` is Redis's alias for every event class, ``h`` and ``x`` included, and
# ``CONFIG GET`` reports it in place of the classes it covers: a server set to
# ``KEA`` answers ``AKE``, with no literal ``h`` or ``x`` in it.
_KEYEVENT_FLAG = "E"
_EVENT_CLASSES = "hx"
_ALL_CLASSES_ALIAS = "A"

REQUIRED_CONFIG = "Ehx"


class SessionEvents:
    """Calls registered handlers when a session ends.

    Build one in the lifespan, register handlers on it, and let it run for the
    life of the process::

        events = SessionEvents(redis, key_prefix="redis:fastapi")

        @events.on_session_end
        async def _(session_id: str, cause: Cause) -> None:
            await close_sockets_for(session_id)

        await events.start()
        ...
        await events.stop()

    Attributes:
        tier: ``"key"`` when the server can deliver events, ``"none"`` when
            it cannot.  Read it to decide whether a handler will ever run.
    """

    def __init__(
        self,
        redis: AsyncRedis | AsyncRedisCluster,
        *,
        key_prefix: str,
        db: int = 0,
    ) -> None:
        self._redis = redis
        self._session_prefix = f"{key_prefix}:session:"
        self._db = db
        self._handlers: list[Handler] = []
        self._task: asyncio.Task[None] | None = None
        self.tier: Tier = "none"

    def on_session_end(self, handler: Handler) -> Handler:
        """Register *handler*, and return it so this works as a decorator.

        The handler receives the session ID and the cause: ``"idle"`` when the
        user went quiet, ``"absolute"`` when the deadline passed however
        active they were.  They are the only two values - a revocation is a
        ``DEL``, which this module does not observe.  See :data:`Cause`.

        Returns:
            *handler*, so this works as a decorator.
        """
        self._handlers.append(handler)
        return handler

    # -- capability ----------------------------------------------------------

    async def probe(self) -> Tier:
        """Decide which tier this server can supply.  Asked once, at startup.

        Two separate questions, and both can fail:

        1. *Can the server do it?*  ``INFO server`` for the version.
        2. *Is it switched on?*  ``CONFIG GET notify-keyspace-events``.

        **A failure to ask is an answer.**  Managed Redis commonly restricts,
        renames or forbids ``CONFIG``, and an ACL without ``@admin`` does the
        same.  Any error here yields ``"none"`` rather than propagating, so a
        locked-down instance degrades instead of breaking startup.
        """
        try:
            if not await self._version_ok():
                return "none"
            return "key" if await self._config_ok() else "none"
        except STORE_ERRORS as exc:
            logger.info("Could not probe session-event support: %s", exc)
            return "none"

    async def _version_ok(self) -> bool:
        info = await self._redis.info("server")
        raw = str(info.get("redis_version", "0.0.0"))
        parts: list[int] = []
        for chunk in raw.split(".")[:2]:
            digits = "".join(c for c in chunk if c.isdigit())
            parts.append(int(digits) if digits else 0)
        while len(parts) < 2:
            parts.append(0)
        if tuple(parts) < _MIN_VERSION:
            logger.warning(
                "Session events need Redis %d.%d or later for hash-field "
                "expiry events; this server reports %s. Handlers will not "
                "fire. Everything else works unchanged.",
                _MIN_VERSION[0],
                _MIN_VERSION[1],
                raw,
            )
            return False
        return True

    async def _config_ok(self) -> bool:
        config = await self._redis.config_get("notify-keyspace-events")
        flags = str(config.get("notify-keyspace-events", ""))
        classes_ok = _ALL_CLASSES_ALIAS in flags or all(
            flag in flags for flag in _EVENT_CLASSES
        )
        if _KEYEVENT_FLAG not in flags or not classes_ok:
            logger.warning(
                "Session events need notify-keyspace-events to include '%s' "
                "(the __keyevent@ channels, plus hash and expired events); "
                "this server has %r. Handlers will not fire. This library "
                "will not set the option for you: it is server-wide and "
                "affects every other application on the instance.",
                REQUIRED_CONFIG,
                flags,
            )
            return False
        return True

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        """Probe, and subscribe when the server can supply events.

        Never raises.  When the tier is ``"none"`` this returns having done
        nothing but log, and the handlers stay registered and silent.
        """
        if self._task is not None:
            return
        self.tier = await self.probe()
        if self.tier == "none":
            return
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Cancel the subscriber and wait for it to finish."""
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run(self) -> None:
        """Subscribe and dispatch until cancelled.

        On a cluster this covers one node only.  Keyspace events are
        node-local and are **not** broadcast, so a full deployment needs one
        subscriber per node - which the caller composes, because only the
        caller knows the topology.
        """
        channels: dict[str, Cause] = {
            _IDLE_CHANNEL.format(db=self._db): "idle",
            _ABSOLUTE_CHANNEL.format(db=self._db): "absolute",
        }
        try:
            pubsub = self._redis.pubsub()
            await pubsub.subscribe(*channels)
            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                cause = channels.get(_text(message.get("channel")))
                if cause is not None:
                    await self._dispatch(message.get("data"), cause)
        except asyncio.CancelledError:
            raise
        except STORE_ERRORS as exc:
            # Losing the subscription is not an application error. Say so once
            # and stop; nothing downstream depends on this stream.
            logger.warning("Session event subscription ended: %s", exc)

    async def _dispatch(self, data: Any, cause: Cause) -> None:
        session_id = self._session_id(data)
        if session_id is None:
            return
        for handler in self._handlers:
            try:
                await handler(session_id, cause)
            except Exception:
                # One bad handler must not take down the subscriber and with
                # it every other handler.
                logger.exception("A session-end handler raised")
                record_session_event(cause=cause, result="dropped")
            else:
                record_session_event(cause=cause, result="delivered")

    def _session_id(self, data: Any) -> str | None:
        """The session ID in one notification, or ``None`` if it names none.

        The payload is the key name.  Anything that does not start with this
        store's session prefix is another application's key - or this store's
        index, whose entries expire constantly and are not session deaths - and
        is ignored.
        """
        key = _text(data)
        if not key.startswith(self._session_prefix):
            return None
        return key[len(self._session_prefix) :] or None


def _text(value: Any) -> str:
    """A Pub/Sub frame field as text, whether the client decodes or not."""
    return value.decode() if isinstance(value, bytes) else str(value)
