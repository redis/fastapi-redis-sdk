"""Fixtures for the integration suite only."""

from collections.abc import Iterator

import pytest
import redis as sync_redis

from redis_fastapi.config import get_settings
from tests.conftest import REDIS_URL

_DEFAULT_URL = "redis://localhost:6379/0"


@pytest.fixture(autouse=True)
def _app_uses_the_server_under_test(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Point the app of a test at the server that the fixtures read.

    The root conftest removes each ``REDIS_*`` variable, ``REDIS_URL``
    included, so an app built in a test connects to the default server.  With
    ``REDIS_URL`` set to another server, the app would write to one Redis and
    the test would read the other.  This puts the variable back.  With the
    default server it changes nothing.
    """
    if REDIS_URL == _DEFAULT_URL:
        yield
        return
    monkeypatch.setenv("REDIS_URL", REDIS_URL)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _indexes_of(client: sync_redis.Redis, prefix: str) -> list[str]:
    """Names of the search indexes under *prefix*; none without the Query Engine."""
    try:
        names = client.execute_command("FT._LIST")
    except sync_redis.RedisError:
        return []
    return [name for name in names if name.startswith(f"{prefix}:")]


@pytest.fixture(autouse=True)
def _drop_session_indexes(test_prefix: str) -> Iterator[None]:
    """Drop each search index a test left under its own prefix.

    The session store creates an index when an app starts and when a store
    first searches.  ``FLUSHDB`` removes the keys and keeps the index
    definition, so without this each test would leave one or two indexes on
    the server.  The drop has no ``DD``: the fixtures of the root conftest
    remove the keys.
    """
    yield
    try:
        client = sync_redis.Redis.from_url(REDIS_URL, decode_responses=True)
    except sync_redis.RedisError:
        return
    try:
        for name in _indexes_of(client, test_prefix):
            client.ft(name).dropindex(delete_documents=False)
    except sync_redis.RedisError:
        # Redis is down. The tests that need it skipped or failed already.
        pass
    finally:
        client.close()
