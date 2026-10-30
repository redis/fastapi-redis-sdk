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
| `redis` (redis-py)  | >= 6.2             | Installed automatically |
| `fastapi`           | >= 0.115           | Installed automatically |
| `pydantic`          | >= 2.0             | Installed automatically |
| Redis server        | >= 7.4             | Required at runtime     |

A running Redis server (or [Redis Cloud](https://redis.io/cloud/) instance) is
required at runtime.

