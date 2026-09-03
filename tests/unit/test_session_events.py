"""Tests for :class:`SessionEvents`.

The two properties that matter are the ones asserted hardest: the probe
degrades instead of raising, and a handler at tier ``none`` is silent rather
than broken.
"""

from __future__ import annotations

import pytest
from redis.exceptions import RedisError

from redis_fastapi.session_events import REQUIRED_CONFIG, SessionEvents


class _FakeRedis:
    """Just enough Redis to answer a probe."""

    def __init__(self, version: str = "8.8.0", flags: str = REQUIRED_CONFIG) -> None:
        self._version = version
        self._flags = flags
        self.raise_on_config = False
        self.raise_on_info = False

    async def info(self, section: str) -> dict:
        if self.raise_on_info:
            raise RedisError("INFO is not available")
        return {"redis_version": self._version}

    async def config_get(self, name: str) -> dict:
        if self.raise_on_config:
            raise RedisError("unknown command 'CONFIG'")
        return {name: self._flags}


def _events(redis) -> SessionEvents:
    return SessionEvents(redis, key_prefix="redis:fastapi")


class TestProbe:
    async def test_a_capable_and_configured_server_gives_the_field_tier(self) -> None:
        assert await _events(_FakeRedis()).probe() == "field"

    @pytest.mark.parametrize("version", ["7.4.0", "8.0.3", "8.6.1"])
    async def test_a_server_below_8_8_gives_none(self, version: str) -> None:
        assert await _events(_FakeRedis(version=version)).probe() == "none"

    @pytest.mark.parametrize("flags", ["", "KEA", "AKE", "Kh", "ST"])
    async def test_missing_flags_give_none(self, flags: str) -> None:
        """The subkey flags are independent of K and E.

        ``KEA`` enables every standard keyspace event and still delivers no
        subkey notification, which is the commonest configuration mistake.
        ``ST`` picks subkey channels but omits the hash class.
        """
        assert await _events(_FakeRedis(flags=flags)).probe() == "none"

    async def test_config_being_forbidden_degrades_rather_than_raising(self) -> None:
        """Managed Redis routinely restricts or renames CONFIG.

        A probe that propagated the error would break startup on every such
        deployment, so failing to ask is itself an answer.
        """
        redis = _FakeRedis()
        redis.raise_on_config = True
        assert await _events(redis).probe() == "none"

    async def test_info_being_forbidden_degrades_too(self) -> None:
        redis = _FakeRedis()
        redis.raise_on_info = True
        assert await _events(redis).probe() == "none"

    async def test_an_unparseable_version_degrades(self) -> None:
        assert await _events(_FakeRedis(version="unknown")).probe() == "none"


class TestSilentFallback:
    async def test_start_succeeds_at_tier_none(self) -> None:
        events = _events(_FakeRedis(version="7.4.0"))
        await events.start()
        assert events.tier == "none"
        await events.stop()

    async def test_a_handler_at_tier_none_is_never_called(self) -> None:
        """The documented, deliberate failure mode.

        Asserted rather than left to chance, because a handler that silently
        never fires is indistinguishable from one that works - which is
        exactly why the guide has to warn about it.
        """
        called: list[str] = []
        events = _events(_FakeRedis(version="7.4.0"))

        @events.on_session_end
        async def _(session_id: str, cause: str) -> None:
            called.append(session_id)

        await events.start()
        await events.stop()
        assert called == []

    async def test_one_warning_is_logged_and_startup_continues(self, caplog) -> None:
        events = _events(_FakeRedis(version="7.4.0"))
        with caplog.at_level("WARNING"):
            await events.start()
        assert len(caplog.records) == 1
        assert "8.8" in caplog.text


class TestParsingNotifications:
    @pytest.fixture()
    def events(self) -> SessionEvents:
        return _events(_FakeRedis())

    def test_an_expired_idle_field_reads_as_idle(self, events: SessionEvents) -> None:
        payload = "26:redis:fastapi:session:abc|1:d"
        assert events._parse(payload) == ("abc", "idle")

    def test_an_expired_deadline_reads_as_absolute(self, events: SessionEvents) -> None:
        payload = "26:redis:fastapi:session:abc|1:a"
        assert events._parse(payload) == ("abc", "absolute")

    def test_both_fields_at_once_read_as_absolute(self, events: SessionEvents) -> None:
        """The idle clock is the shorter one, so it only expires alone."""
        payload = "26:redis:fastapi:session:abc|1:d,1:a"
        assert events._parse(payload) == ("abc", "absolute")

    def test_another_application_s_hash_is_ignored(self, events: SessionEvents) -> None:
        assert events._parse("9:other:key|1:d") is None

    def test_the_index_key_is_ignored(self, events: SessionEvents) -> None:
        """Index entries expire constantly and are not session deaths."""
        assert events._parse("29:redis:fastapi:sessions-of:42|3:abc") is None

    def test_bytes_are_accepted(self, events: SessionEvents) -> None:
        assert events._parse(b"26:redis:fastapi:session:abc|1:d") == ("abc", "idle")

    @pytest.mark.parametrize(
        "payload",
        ["garbage", "", "nolengthprefix|1:d", "26:redis:fastapi:session:abc|nope"],
    )
    def test_a_malformed_payload_is_dropped_not_raised(
        self, events: SessionEvents, payload: str
    ) -> None:
        assert events._parse(payload) is None


class TestDispatch:
    async def test_every_handler_receives_the_event(self) -> None:
        events = _events(_FakeRedis())
        seen: list[tuple[str, str]] = []

        @events.on_session_end
        async def first(session_id: str, cause: str) -> None:
            seen.append(("first", cause))

        @events.on_session_end
        async def second(session_id: str, cause: str) -> None:
            seen.append(("second", cause))

        await events._dispatch("26:redis:fastapi:session:abc|1:d")
        assert seen == [("first", "idle"), ("second", "idle")]

    async def test_one_raising_handler_does_not_stop_the_others(self) -> None:
        events = _events(_FakeRedis())
        seen: list[str] = []

        @events.on_session_end
        async def broken(session_id: str, cause: str) -> None:
            raise ValueError("boom")

        @events.on_session_end
        async def working(session_id: str, cause: str) -> None:
            seen.append(session_id)

        await events._dispatch("26:redis:fastapi:session:abc|1:d")
        assert seen == ["abc"], (
            "a bad handler took down the subscriber and with it every other handler"
        )
