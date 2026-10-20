"""Disposable local servers: no shared Redis, no FLUSHDB, no paid resources."""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest
import redis
import redis.asyncio as redis_async
from redis.exceptions import RedisError
from store import Store


def pytest_addoption(parser):
    parser.addoption("--redis-server", required=True, help="Pinned Redis 8.10.2 binary")
    parser.addoption(
        "--topology", action="append", choices=["standalone", "cluster", "enterprise"]
    )


def pytest_generate_tests(metafunc):
    if "topology" in metafunc.fixturenames:
        metafunc.parametrize(
            "topology",
            metafunc.config.getoption("--topology") or ["standalone", "cluster"],
            scope="session",
        )


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def eventually_sync(check, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (RedisError, OSError):
            pass
        time.sleep(0.01)
    raise AssertionError("Server readiness timed out")


@contextmanager
def server(binary, directory, *, cluster=False):
    directory.mkdir()
    port = unused_port()
    config = directory / "redis.conf"
    config.write_text(
        f"bind 127.0.0.1\nport {port}\nprotected-mode yes\n"
        f'dir "{directory}"\nsave ""\nappendonly yes\nappendfsync always\n'
        + (
            f"cluster-enabled yes\ncluster-port {unused_port()}\n"
            "cluster-config-file nodes.conf\ncluster-node-timeout 1000\n"
            "cluster-require-full-coverage yes\n"
            if cluster
            else ""
        )
    )
    with (directory / "server.log").open("w") as log:
        process = subprocess.Popen([str(binary), str(config)], stdout=log, stderr=log)
        client = redis.Redis(
            host="127.0.0.1",
            port=port,
            decode_responses=True,
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        try:
            eventually_sync(client.ping)
            assert process.poll() is None
            yield port
        finally:
            client.close()
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@pytest.fixture(scope="session")
def binary(pytestconfig):
    value = Path(pytestconfig.getoption("--redis-server")).resolve()
    version = subprocess.check_output([str(value), "--version"], text=True, timeout=5)
    assert "v=8.10.2 " in version, version
    return value


@pytest.fixture(scope="session")
def standalone_server(binary, tmp_path_factory):
    with server(binary, tmp_path_factory.mktemp("standalone") / "node") as port:
        yield [port]


@pytest.fixture(scope="session")
def cluster_servers(binary, tmp_path_factory):
    from contextlib import ExitStack

    root = tmp_path_factory.mktemp("cluster")
    with ExitStack() as stack:
        ports = [
            stack.enter_context(server(binary, root / f"node-{n}", cluster=True))
            for n in range(3)
        ]
        result = subprocess.run(
            [
                str(binary.with_name("redis-cli")),
                "--cluster",
                "create",
                *(f"127.0.0.1:{port}" for port in ports),
                "--cluster-replicas",
                "0",
                "--cluster-yes",
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert result.returncode == 0, result.stderr + result.stdout
        for port in ports:
            with redis.Redis(
                port=port, decode_responses=True, socket_timeout=1
            ) as node:
                eventually_sync(
                    lambda: (
                        node.execute_command("CLUSTER INFO")["cluster_state"] == "ok"
                    )
                )
        yield ports


@pytest.fixture
async def deployment(topology, request):
    if topology == "enterprise":
        url = os.getenv("STREAM_SPIKE_ENTERPRISE_URL")
        if not url:
            pytest.skip("Redis Enterprise sharded endpoint unavailable; unverified")
        if os.getenv("STREAM_SPIKE_ALLOW_EXTERNAL") != "1":
            pytest.fail("External writes require STREAM_SPIKE_ALLOW_EXTERNAL=1")
        ports = []
    else:
        ports = request.getfixturevalue(
            "cluster_servers" if topology == "cluster" else "standalone_server"
        )
        url = f"redis://127.0.0.1:{ports[0]}"
    prefix = f"stream-spike:{uuid.uuid4().hex}"
    relay_name = prefix + ":relay"
    kwargs = {
        "decode_responses": True,
        "socket_connect_timeout": 2,
        "socket_timeout": 3,
    }

    def client(*, blocking=False):
        options = dict(kwargs, max_connections=4 if blocking else 16)
        if blocking:
            options["client_name"] = relay_name
        factory = (
            redis_async.RedisCluster if topology == "cluster" else redis_async.Redis
        )
        return factory.from_url(url, **options)

    commands = client()
    blocking = client(blocking=True)
    store = Store(commands, prefix)
    try:
        await store.initialize()
        yield store, blocking, ports, relay_name
    finally:
        await blocking.aclose()
        try:
            await asyncio.wait_for(store.cleanup(), timeout=5)
        finally:
            await commands.aclose()


async def eventually(check, timeout=5):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        value = await check()
        if value:
            return value
        await asyncio.sleep(0.01)
    raise AssertionError("Observed state did not converge before deadline")


async def blocked_connections(store, ports, name):
    if not ports:
        pytest.skip("Connection introspection needs topology-specific operator access")
    total = 0
    for port in ports:
        async with redis_async.Redis(
            port=port, decode_responses=True, socket_timeout=2
        ) as node:
            clients = await node.client_list()
            total += sum(
                row.get("name") == name
                and row.get("cmd") == "xread"
                and "b" in row["flags"]
                for row in clients
            )
    return total
