"""The driver failures every Redis-backed feature must catch.

``RedisClusterException`` is **not** a subclass of ``RedisError``: redis-py
raises it from the cluster client's own code paths - no node reachable, a
slot with no owner, a lost connection during resharding or a failover.
Catching only ``RedisError`` lets the commonest cluster failure escape every
policy a feature has, so each feature catches this tuple instead.  One
definition, so no feature can drift from the others.
"""

from __future__ import annotations

from redis.exceptions import RedisClusterException, RedisError

STORE_ERRORS: tuple[type[BaseException], ...] = (
    RedisError,
    RedisClusterException,
    OSError,
)
