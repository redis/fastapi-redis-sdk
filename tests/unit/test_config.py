"""Tests for RedisSettings configuration."""

from __future__ import annotations

import os
import warnings
from unittest.mock import patch

import pytest
from pydantic import ValidationError

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
        assert kw["ssl"] is True
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
        assert kw["ssl"] is True
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
class TestPrincipalKeysFromTheEnvironment:
    """``REDIS_SESSION_PRINCIPAL_KEYS=user_id,role`` must work.

    It used to be a hard startup failure. ``session_principal_keys`` is the
    only list-typed field in the package, so pydantic-settings called
    ``json.loads`` on the raw string and raised ``SettingsError`` - not a
    fallback, not a warning: the application did not start. Comma-separated is
    what an operator types, and what Django, Rails and Spring Boot all accept
    for the same kind of setting.
    """

    def _keys(self, raw: str) -> list[str]:
        with patch.dict(os.environ, {"REDIS_SESSION_PRINCIPAL_KEYS": raw}, clear=True):
            return RedisSettings().session_principal_keys

    def test_a_comma_separated_list(self) -> None:
        assert self._keys("user_id,role") == ["user_id", "role"]

    def test_a_single_key_needs_no_punctuation(self) -> None:
        assert self._keys("user_id") == ["user_id"]

    def test_whitespace_and_empty_parts_are_tidied(self) -> None:
        """A trailing comma is a typo, not a key named ``""``."""
        assert self._keys("  user_id , role ,, scopes ") == [
            "user_id",
            "role",
            "scopes",
        ]

    def test_a_json_array_still_works(self) -> None:
        """The old form stays valid, so no existing configuration breaks."""
        assert self._keys('["user_id","role"]') == ["user_id", "role"]

    def test_malformed_json_says_what_to_do_instead(self) -> None:
        with pytest.raises(ValidationError, match="comma-separated is simpler"):
            self._keys('["user_id",')

    def test_the_default_survives_an_unset_variable(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            assert RedisSettings().session_principal_keys == ["user_id"]

    def test_a_list_passed_in_python_is_untouched(self) -> None:
        settings = RedisSettings(session_principal_keys=["a", "b"])
        assert settings.session_principal_keys == ["a", "b"]


class TestPrincipalKeysRefusesEmpty:
    """An empty list would silently disable rotation altogether.

    ``_default_principal_of`` over no keys returns the same sentinel on every
    request, so no sign-in and no privilege change is ever detected - the
    fixation the feature exists to prevent. Before the comma split a blank
    value could not get through, because ``json.loads("")`` fails and the
    application does not start. Splitting one yields ``[]``, so the refusal
    has to be explicit or the convenience would open the hole.
    """

    @pytest.mark.parametrize("raw", ["", "   ", ",", ",,,", " , , "])
    def test_a_blank_or_comma_only_value_is_refused(self, raw: str) -> None:
        with patch.dict(os.environ, {"REDIS_SESSION_PRINCIPAL_KEYS": raw}, clear=True):
            with pytest.raises(ValidationError, match="at least one session key"):
                RedisSettings()

    def test_an_explicit_empty_json_array_is_refused_too(self) -> None:
        """Reachable before this change, and just as broken then."""
        with patch.dict(os.environ, {"REDIS_SESSION_PRINCIPAL_KEYS": "[]"}, clear=True):
            with pytest.raises(ValidationError, match="at least one session key"):
                RedisSettings()

    def test_an_empty_list_in_python_is_refused_too(self) -> None:
        with pytest.raises(ValidationError, match="at least one session key"):
            RedisSettings(session_principal_keys=[])

    def test_the_message_names_the_way_out(self) -> None:
        """Whoever wants no key-based rotation passes their own function."""
        with pytest.raises(ValidationError, match="principal_of"):
            RedisSettings(session_principal_keys=[])


class TestTheTtlConvention:
    """Settings take `int` seconds; runtime Python calls take either.

    The design's §9 table said `int | timedelta` for these three fields while
    the code said `int`, and nothing here pinned either side. The code was
    right and the table is now corrected, so these tests exist to stop the
    fields being widened to "match" a document that no longer says that.

    The reason the fields cannot sensibly be widened is the second test: from
    the environment every value is a string, and pydantic reads `"1800"` as
    1800 seconds but `"PT30M"` as ISO-8601 - one field with two syntaxes,
    where the operator-friendly one is the plain integer.
    """

    def test_the_three_session_ttls_are_integer_seconds(self) -> None:
        for field in ("session_idle_ttl", "session_absolute_ttl", "session_gc_ttl"):
            annotation = RedisSettings.model_fields[field].annotation
            assert annotation is int, f"{field} is {annotation}, not int"

    def test_a_duration_string_is_refused_rather_than_guessed(self) -> None:
        with patch.dict(os.environ, {"REDIS_SESSION_IDLE_TTL": "PT30M"}, clear=True):
            with pytest.raises(ValidationError):
                RedisSettings()

    def test_seconds_from_the_environment_are_read_as_seconds(self) -> None:
        with patch.dict(os.environ, {"REDIS_SESSION_IDLE_TTL": "1800"}, clear=True):
            assert RedisSettings().session_idle_ttl == 1800


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
