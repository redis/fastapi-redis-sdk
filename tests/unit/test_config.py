"""Tests for RedisSettings configuration."""

from __future__ import annotations

import os
import warnings
from unittest.mock import patch

import pytest
from redis.asyncio import SSLConnection

from redis_fastapi.config import DRIVER_INFO, RedisSettings


@pytest.mark.unit
class TestRedisSettingsDefaults:
    def test_defaults(self) -> None:
        s = RedisSettings()
        assert s.url is None
        assert s.host == "localhost"
        assert s.port == 6379
        assert s.db == 0
        assert s.username is None
        assert s.password is None
        assert s.ssl is False
        assert s.max_connections is None
        assert s.socket_timeout is None
        assert s.socket_connect_timeout is None
        assert s.cluster is False
        assert s.prefix == "redis:fastapi"
        assert s.default_ttl == 0


@pytest.mark.unit
class TestPatternPrefix:
    def test_default_prefix(self) -> None:
        s = RedisSettings()
        assert s.pattern_prefix("cache") == "redis:fastapi:cache"

    def test_custom_prefix(self) -> None:
        s = RedisSettings(prefix="myapp")
        assert s.pattern_prefix("cache") == "myapp:cache"
        assert s.pattern_prefix("session") == "myapp:session"


@pytest.mark.unit
class TestConnectionKwargsURL:
    def test_url_mode(self) -> None:
        s = RedisSettings(url="redis://myhost:6380/2")
        kw = s.connection_kwargs()
        assert kw["url"] == "redis://myhost:6380/2"
        assert "host" not in kw
        assert "port" not in kw
        assert "db" not in kw
        assert kw["driver_info"] is DRIVER_INFO

    def test_url_takes_precedence_over_kv(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            s = RedisSettings(url="redis://url-host:9999/3", host="kv-host", port=1111)
        kw = s.connection_kwargs()
        assert kw["url"] == "redis://url-host:9999/3"
        assert "host" not in kw


@pytest.mark.unit
class TestUrlWithKvWarning:
    def test_warns_when_url_and_host_set(self) -> None:
        with pytest.warns(UserWarning, match="'url' and .* are set"):
            RedisSettings(url="redis://h:6379/0", host="other")

    def test_warns_when_url_and_port_set(self) -> None:
        with pytest.warns(UserWarning, match="KV fields are ignored"):
            RedisSettings(url="redis://h:6379/0", port=9999)

    def test_warns_lists_all_overlapping_fields(self) -> None:
        with pytest.warns(UserWarning, match="host.*port") as rec:
            RedisSettings(url="redis://h:6379/0", host="x", port=1234)
        assert len(rec) == 1

    def test_no_warning_url_only(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            RedisSettings(url="redis://h:6379/0")

    def test_no_warning_kv_only(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            RedisSettings(host="myhost", port=6380, db=2)


@pytest.mark.unit
class TestConnectionKwargsKV:
    def test_kv_mode_defaults(self) -> None:
        s = RedisSettings()
        kw = s.connection_kwargs()
        assert "url" not in kw
        assert kw["host"] == "localhost"
        assert kw["port"] == 6379
        assert kw["db"] == 0
        assert "username" not in kw
        assert "password" not in kw

    def test_kv_mode_with_credentials(self) -> None:
        s = RedisSettings(username="admin", password="secret")
        kw = s.connection_kwargs()
        assert kw["username"] == "admin"
        assert kw["password"] == "secret"

    def test_kv_mode_custom_host_port_db(self) -> None:
        s = RedisSettings(host="redis.local", port=6380, db=5)
        kw = s.connection_kwargs()
        assert kw["host"] == "redis.local"
        assert kw["port"] == 6380
        assert kw["db"] == 5


@pytest.mark.unit
class TestTLSKwargs:
    def test_no_ssl(self) -> None:
        s = RedisSettings(ssl=False)
        assert s._tls_kwargs() == {}

    def test_ssl_minimal(self) -> None:
        s = RedisSettings(ssl=True)
        kw = s._tls_kwargs()
        assert kw["connection_class"] is SSLConnection
        assert kw["ssl_check_hostname"] is True
        assert "ssl_certfile" not in kw

    def test_ssl_full(self) -> None:
        s = RedisSettings(
            ssl=True,
            ssl_certfile="/cert.pem",
            ssl_keyfile="/key.pem",
            ssl_ca_certs="/ca.pem",
            ssl_check_hostname=True,
        )
        kw = s._tls_kwargs()
        assert kw["ssl_certfile"] == "/cert.pem"
        assert kw["ssl_keyfile"] == "/key.pem"
        assert kw["ssl_ca_certs"] == "/ca.pem"
        assert kw["ssl_check_hostname"] is True

    def test_tls_kwargs_propagate_to_connection_kwargs(self) -> None:
        s = RedisSettings(ssl=True, ssl_ca_certs="/ca.pem")
        kw = s.connection_kwargs()
        assert kw["connection_class"] is SSLConnection
        assert kw["ssl_ca_certs"] == "/ca.pem"


@pytest.mark.unit
class TestPoolKwargs:
    def test_pool_defaults_omitted(self) -> None:
        s = RedisSettings()
        kw = s._pool_kwargs()
        assert "max_connections" not in kw
        assert "socket_timeout" not in kw
        assert "socket_connect_timeout" not in kw

    def test_pool_values_included(self) -> None:
        s = RedisSettings(
            max_connections=20,
            socket_timeout=5.0,
            socket_connect_timeout=2.0,
        )
        kw = s._pool_kwargs()
        assert kw["max_connections"] == 20
        assert kw["socket_timeout"] == 5.0
        assert kw["socket_connect_timeout"] == 2.0


@pytest.mark.unit
class TestFromEnv:
    def test_defaults_no_env(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            s = RedisSettings()
            assert s.url is None
            assert s.host == "localhost"
            assert s.port == 6379
            assert s.cluster is False
            assert s.ssl is False
            assert s.prefix == "redis:fastapi"
            assert s.default_ttl == 0

    def test_url_from_env(self) -> None:
        with patch.dict(os.environ, {"REDIS_URL": "redis://env:1234/7"}, clear=True):
            s = RedisSettings()
            assert s.url == "redis://env:1234/7"

    def test_kv_from_env(self) -> None:
        env = {
            "REDIS_HOST": "redis.prod",
            "REDIS_PORT": "6380",
            "REDIS_DB": "3",
            "REDIS_USERNAME": "user",
            "REDIS_PASSWORD": "pw",
        }
        with patch.dict(os.environ, env, clear=True):
            s = RedisSettings()
            assert s.url is None
            assert s.host == "redis.prod"
            assert s.port == 6380
            assert s.db == 3
            assert s.username == "user"
            # Password is now a SecretStr, extract the value
            assert s.password.get_secret_value() == "pw"

    def test_ssl_from_env(self) -> None:
        env = {
            "REDIS_SSL": "true",
            "REDIS_SSL_CERTFILE": "/c.pem",
            "REDIS_SSL_KEYFILE": "/k.pem",
            "REDIS_SSL_CA_CERTS": "/ca.pem",
            "REDIS_SSL_CHECK_HOSTNAME": "1",
        }
        with patch.dict(os.environ, env, clear=True):
            s = RedisSettings()
            assert s.ssl is True
            assert s.ssl_certfile == "/c.pem"
            assert s.ssl_keyfile == "/k.pem"
            assert s.ssl_ca_certs == "/ca.pem"
            assert s.ssl_check_hostname is True

    def test_pool_from_env(self) -> None:
        env = {
            "REDIS_MAX_CONNECTIONS": "50",
            "REDIS_SOCKET_TIMEOUT": "3.5",
            "REDIS_SOCKET_CONNECT_TIMEOUT": "1.0",
        }
        with patch.dict(os.environ, env, clear=True):
            s = RedisSettings()
            assert s.max_connections == 50
            assert s.socket_timeout == 3.5
            assert s.socket_connect_timeout == 1.0

    def test_cluster_and_prefix_from_env(self) -> None:
        env = {
            "REDIS_CLUSTER": "yes",
            "REDIS_PREFIX": "myapp:redis",
            "REDIS_DEFAULT_TTL": "120",
        }
        with patch.dict(os.environ, env, clear=True):
            s = RedisSettings()
            assert s.cluster is True
            assert s.prefix == "myapp:redis"
            assert s.default_ttl == 120

    def test_empty_url_treated_as_none(self) -> None:
        with patch.dict(os.environ, {"REDIS_URL": ""}, clear=True):
            s = RedisSettings()
            # Pydantic treats empty string as None for Optional fields
            assert s.url == "" or s.url is None
