"""Real-time session-end events, driven by Redis keyspace notifications.

A session store knows when a session dies.  Every other backend discovers it
on the next request, because a row that expired quietly tells nobody.  Redis
can say so as it happens, and this module turns that into an application
callback - closing a WebSocket the moment a user is signed out being the case
that pays for it.

**Two properties matter more than the feature itself.**

*Nothing depends on it.*  Pub/Sub is fire-and-forget, events sent while no
subscriber is connected are lost, and an expiry event fires when Redis removes
the field rather than when the deadline passed.  So the index still prunes
itself through field expiry and the load-time state table is still the
authority on whether a session is alive.  Every guarantee in the store holds
with this module switched off, which is why it is off by default and why it is
the last thing to build.

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
from redis.exceptions import RedisError

from redis_fastapi.session_backend import FIELD_ABSOLUTE, FIELD_DATA
from redis_fastapi.telemetry import record_session_event

logger = logging.getLogger(__name__)

Tier = Literal["none", "field"]
Cause = Literal["idle", "absolute", "revoked"]
Handler = Callable[[str, Cause], Awaitable[None]]

# Subkey notifications arrived in Redis 8.8.  There is no key-level tier here
# on purpose: our session key has no key-level TTL - it dies as a side effect
# of its last field expiring - so a key-level event carries no field name and
# cannot separate an idle death from an absolute one.  That is most of what a
# subscriber wants to know, so the ladder has two rungs, not three.
_MIN_VERSION = (8, 8)

# The channel that names both the key and the field.
_CHANNEL = "__subkeyevent@{db}__:hexpired"

# Flags that must be present in ``notify-keyspace-events``.  The four subkey
# channels are S, T, I and V, and they are **independent of K and E** -
# enabling standard keyspace notifications does not enable these, and the
# reverse holds too.  This is the commonest configuration mistake.
_SUBKEY_FLAGS = frozenset("STIV")
_HASH_FLAG = "h"

REQUIRED_CONFIG = "Th"


class SessionEvents:
    """Calls registered handlers when a session ends.

    Build one in the lifespan, register handlers on it, and let it run for the
    life of the process::

        events = SessionEvents(redis, key_prefix="redis:fastapi")

        @events.on_session_end
        async def _(session_id: str, cause: str) -> None:
            await close_sockets_for(session_id)

        await events.start()
        ...
        await events.stop()

    Attributes:
        tier: ``"field"`` when the server can deliver events, ``"none"`` when
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
        active they were.  Distinguishing the two is the whole reason this
        needs Redis 8.8.
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
            return "field" if await self._config_ok() else "none"
        except (RedisError, OSError) as exc:
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
                "Session events need Redis %d.%d or later for hash subkey "
                "notifications; this server reports %s. Handlers will not "
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
        if not (set(flags) & _SUBKEY_FLAGS) or _HASH_FLAG not in flags:
            logger.warning(
                "Session events need notify-keyspace-events to include '%s' "
                "(a subkey channel plus hash events); this server has %r. "
                "Handlers will not fire. Note that the subkey flags S/T/I/V "
                "are independent of K and E. This library will not set the "
                "option for you: it is server-wide and affects every other "
                "application on the instance.",
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
        channel = _CHANNEL.format(db=self._db)
        try:
            pubsub = self._redis.pubsub()
            await pubsub.subscribe(channel)
            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                await self._dispatch(message.get("data"))
        except asyncio.CancelledError:
            raise
        except (RedisError, OSError) as exc:
            # Losing the subscription is not an application error. Say so once
            # and stop; nothing downstream depends on this stream.
            logger.warning("Session event subscription ended: %s", exc)

    async def _dispatch(self, data: Any) -> None:
        parsed = self._parse(data)
        if parsed is None:
            return
        session_id, cause = parsed
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

    def _parse(self, data: Any) -> tuple[str, Cause] | None:
        """Pull the session ID and the cause out of one notification.

        The payload is ``<key_len>:<key>|<len>:<subkey>[,...]`` - length
        prefixed so a key or field containing the delimiters stays parseable.
        Anything that does not match this store's key prefix is another
        application's hash and is ignored.
        """
        text = data.decode() if isinstance(data, bytes) else str(data)
        key_part, _, field_part = text.partition("|")
        key = _strip_length(key_part)
        if key is None or not key.startswith(self._session_prefix):
            return None
        session_id = key[len(self._session_prefix) :]

        fields = {
            stripped
            for chunk in field_part.split(",")
            if (stripped := _strip_length(chunk)) is not None
        }
        # Both fields expiring at once is the absolute deadline arriving: the
        # idle clock is the shorter one, so it only ever expires alone.
        if FIELD_ABSOLUTE in fields:
            return session_id, "absolute"
        if FIELD_DATA in fields:
            return session_id, "idle"
        return None


def _strip_length(chunk: str) -> str | None:
    """Turn ``"7:field1"`` into ``"field1"``.

    Returns ``None`` for a chunk that carries no length prefix, which means
    the payload is not the shape this version of Redis documents.
    """
    length, sep, value = chunk.partition(":")
    if not sep or not length.isdigit():
        return None
    return value
