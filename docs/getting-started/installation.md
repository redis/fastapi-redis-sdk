# Installation

=== "pip"

    ```bash
    pip install fastapi-redis-sdk
    ```

=== "uv"

    ```bash
    uv add fastapi-redis-sdk
    ```

=== "poetry"

    ```bash
    poetry add fastapi-redis-sdk
    ```

## Requirements

| Dependency          | Supported versions | Notes                   |
|---------------------|--------------------|-------------------------|
| Python              | >= 3.10            |                         |
| `redis` (redis-py)  | >= 6.0             | Installed automatically |
| `fastapi`           | >= 0.115           | Installed automatically |
| `pydantic`          | >= 2.0             | Installed automatically |
| Redis server        | >= 7.4             | Required at runtime     |

A running Redis server (or [Redis Cloud](https://redis.io/cloud/) instance) is
required at runtime.

Session search needs the
[Redis Query Engine](https://redis.io/docs/latest/develop/ai/search-and-query/),
which is part of Redis 8.0 and later. On Redis 7.4 without it, sessions work
and the search methods do not: `list_for_subject`, `count_for_subject` and
`revoke_all` raise `SessionSearchUnavailableError`. See
[Versions and limits](../guide/sessions.md#versions-and-limits).

