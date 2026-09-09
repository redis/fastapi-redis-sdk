"""Tests for :class:`SessionEvents`.

The two properties that matter are the ones asserted hardest: the probe
degrades instead of raising, and a handler at tier ``none`` is silent rather
than broken.
"""

from __future__ import annotations

import asyncio
from typing import get_args

import pytest
from redis.exceptions import RedisError

from redis_fastapi.session_events import (
    _CHANNEL,
    _HASH_FLAG,
    _SUBKEY_FLAG,
    REQUIRED_CONFIG,
    Cause,
    Handler,
    SessionEvents,
)


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

    @pytest.mark.parametrize("flags", ["Sh", "Ih", "Vh", "SIVh", "Sah"])
    async def test_a_subkey_flag_that_is_not_t_gives_none(self, flags: str) -> None:
        """Redis 8.8 has four subkey channels; this module listens on one.

        ``S``, ``I`` and ``V`` enable ``__subkeyspace@``,
        ``__subkeyspaceitem@`` and ``__subkeyspaceevent@``. Only ``T`` enables
        ``__subkeyevent@``, which is where ``listen()`` subscribes. Redis
        accepts a subscription to any channel name, so on ``Sh`` the
        subscription succeeds and no event ever arrives - and the probe used
        to report ``"field"`` for it, which defeats the ``events.tier`` check
        the guide offers against precisely this.
        """
        assert await _events(_FakeRedis(flags=flags)).probe() == "none"

    @pytest.mark.parametrize("flags", ["Th", "ATh", "KEATh", "hT", "STIVh"])
    async def test_t_plus_the_hash_class_gives_the_field_tier(self, flags: str) -> None:
        """Order does not matter, and extra flags are somebody else's business."""
        assert await _events(_FakeRedis(flags=flags)).probe() == "field"

    async def test_the_warning_names_t_specifically(self, caplog) -> None:
        """An operator reading it must not conclude that any subkey flag does."""
        with caplog.at_level("WARNING"):
            assert await _events(_FakeRedis(flags="Sh")).probe() == "none"
        assert "'Th'" in caplog.text
        assert "__subkeyevent@" in caplog.text

    def test_the_documented_config_is_the_one_that_is_checked(self) -> None:
        """``REQUIRED_CONFIG`` is what the guide tells operators to set."""
        assert REQUIRED_CONFIG == "Th"
        assert set(REQUIRED_CONFIG) == {_SUBKEY_FLAG, _HASH_FLAG}

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


class TestTheCauseUnionIsInhabited:
    """``Cause`` is a ``Literal`` in a public callback signature.

    That makes it a promise about which values a handler can be called with.
    It used to include ``"revoked"``, which nothing could produce: ``revoke``
    is a ``DEL``, and ``DEL`` emits no subkey notification at any Redis
    version - it is not among the commands that do, and the mechanism forbids
    it, because a subkey event is published only when at least one subkey is
    present and a deleted key has none left to name. A caller writing the
    exhaustive ``match`` a type checker rewards was left with an arm that
    could never run and that mypy would not let them delete.
    """

    def test_cause_has_exactly_the_two_values_parse_can_return(self) -> None:
        assert set(get_args(Cause)) == {"idle", "absolute"}

    def test_every_member_is_reachable_from_a_real_payload(self) -> None:
        """The union and the parser are checked against each other.

        Adding a member without a payload that produces it fails here, which
        is the regression that let ``"revoked"`` survive.
        """
        events = _events(_FakeRedis())
        key = "redis:fastapi:session:" + "a" * 40
        payloads = {
            f"{len(key)}:{key}|1:d": "idle",
            f"{len(key)}:{key}|1:a": "absolute",
        }
        produced = set()
        for payload, expected in payloads.items():
            parsed = events._parse(payload)
            assert parsed is not None, payload
            assert parsed[1] == expected
            produced.add(parsed[1])
        assert produced == set(get_args(Cause)), (
            "a Cause member no payload can produce, or a payload the union "
            "does not cover"
        )

    def test_a_revocation_produces_no_event_to_parse(self) -> None:
        """A ``DEL`` is not an ``hexpired`` with no fields - it is no message.

        Asserted through the parser rather than the wire: a notification whose
        field list names neither of this store's two fields is not a session
        death, and must not be reported as one.
        """
        events = _events(_FakeRedis())
        key = "redis:fastapi:session:" + "a" * 40
        assert events._parse(f"{len(key)}:{key}|") is None
        assert events._parse(f"{len(key)}:{key}|5:other") is None

    def test_the_handler_type_is_exported(self) -> None:
        """A mypy --strict caller must be able to name what they must pass."""
        import redis_fastapi

        assert "Handler" in redis_fastapi.__all__
        assert redis_fastapi.Handler is Handler

    def test_the_handler_type_refers_to_cause_rather_than_respelling_it(
        self,
    ) -> None:
        """Otherwise the two could drift and only one would be corrected."""
        parameters, _ = get_args(Handler)
        assert parameters == [str, Cause]


class _FakePubSub:
    """Records what was subscribed to, then yields a scripted message stream."""

    def __init__(self, messages: list[dict]) -> None:
        self.messages = messages
        self.subscribed: list[str] = []
        self.closed = False

    async def subscribe(self, channel: str) -> None:
        self.subscribed.append(channel)

    async def listen(self):
        for message in self.messages:
            yield message
        # A real ``listen()`` blocks for the life of the subscription; end
        # here instead so the task completes and the test can assert.

    async def aclose(self) -> None:
        self.closed = True


class _ScriptedRedis(_FakeRedis):
    """A capable, configured server whose Pub/Sub replays fixed messages."""

    def __init__(self, messages: list[dict]) -> None:
        super().__init__()
        self.pubsub_obj = _FakePubSub(messages)

    def pubsub(self) -> _FakePubSub:
        return self.pubsub_obj


def _hexpired(session_id: str, field: str, prefix: str = "redis:fastapi") -> dict:
    """One notification in the documented `__subkeyevent@` payload format."""
    key = f"{prefix}:session:{session_id}"
    return {"type": "message", "data": f"{len(key)}:{key}|{len(field)}:{field}"}


class TestTheDeliveryPath:
    """N-14: the documented recipe has to be executable code under test.

    The probe was covered thoroughly and ``_parse`` and ``_dispatch`` were
    covered directly, but not the path between them - subscribe, receive a
    message, fire a handler. That is where the channel name, the ``pubsub()``
    lifecycle and the message-shape filtering live, so a typo in ``_CHANNEL``
    passed every test in this file.

    Driven through a scripted Pub/Sub rather than a live server: the real
    thing needs Redis 8.8 with ``notify-keyspace-events Th``, which is a
    timing-dependent integration test (see ``test_session_integration.py``).
    The logic is deterministic and belongs here.
    """

    def test_the_channel_matches_the_documented_format(self) -> None:
        """The cheap half, and it needs no server at all."""
        assert _CHANNEL.format(db=0) == "__subkeyevent@0__:hexpired"
        assert _CHANNEL.format(db=3) == "__subkeyevent@3__:hexpired"

    async def test_it_subscribes_to_the_channel_for_its_own_database(self) -> None:
        redis = _ScriptedRedis([])
        events = SessionEvents(redis, key_prefix="redis:fastapi", db=7)
        await events.start()
        await asyncio.wait_for(events._task, timeout=5)
        await events.stop()
        assert redis.pubsub_obj.subscribed == ["__subkeyevent@7__:hexpired"]

    async def test_a_field_expiry_reaches_a_handler(self) -> None:
        """The whole point of the feature, end to end through ``_run``."""
        sid = "a" * 40
        redis = _ScriptedRedis([_hexpired(sid, "d")])
        events = SessionEvents(redis, key_prefix="redis:fastapi")

        seen: list[tuple[str, str]] = []

        @events.on_session_end
        async def _(session_id: str, cause: Cause) -> None:
            seen.append((session_id, cause))

        await events.start()
        assert events.tier == "field"
        await asyncio.wait_for(events._task, timeout=5)
        await events.stop()

        assert seen == [(sid, "idle")]

    async def test_both_causes_arrive_on_the_one_channel(self) -> None:
        first, second = "a" * 40, "b" * 40
        redis = _ScriptedRedis([_hexpired(first, "d"), _hexpired(second, "a")])
        events = SessionEvents(redis, key_prefix="redis:fastapi")

        seen: list[tuple[str, str]] = []
        events.on_session_end(
            lambda session_id, cause: _record(seen, session_id, cause)
        )

        await events.start()
        await asyncio.wait_for(events._task, timeout=5)
        await events.stop()

        assert seen == [(first, "idle"), (second, "absolute")]

    async def test_non_message_frames_are_ignored(self) -> None:
        """Only ``type == "message"`` is an event, whatever the frame carries.

        Redis's own ``subscribe`` confirmation carries a subscription count,
        which would not parse as a payload anyway - so the frames below carry
        a **valid** payload deliberately. Otherwise the assertion holds even
        with the type filter deleted, and the filter is what makes the rule
        "one channel, one frame type" true rather than incidental.
        """
        sid = "a" * 40
        valid = _hexpired(sid, "d")["data"]
        redis = _ScriptedRedis(
            [
                {"type": "subscribe", "data": 1},
                {"type": "psubscribe", "data": valid},
                {"type": "pmessage", "data": valid},
                _hexpired(sid, "d"),
            ]
        )
        events = SessionEvents(redis, key_prefix="redis:fastapi")
        seen: list[tuple[str, str]] = []
        events.on_session_end(
            lambda session_id, cause: _record(seen, session_id, cause)
        )
        await events.start()
        await asyncio.wait_for(events._task, timeout=5)
        await events.stop()
        assert seen == [(sid, "idle")]

    async def test_another_applications_hash_is_ignored(self) -> None:
        """The channel is server-wide; most traffic on it is not ours."""
        redis = _ScriptedRedis(
            [
                _hexpired("x" * 40, "d", prefix="someone-else"),
                {"type": "message", "data": "9:other:key|1:d"},
            ]
        )
        events = SessionEvents(redis, key_prefix="redis:fastapi")
        seen: list[tuple[str, str]] = []
        events.on_session_end(
            lambda session_id, cause: _record(seen, session_id, cause)
        )
        await events.start()
        await asyncio.wait_for(events._task, timeout=5)
        await events.stop()
        assert seen == []

    async def test_a_lost_subscription_ends_quietly(self) -> None:
        """Nothing downstream depends on this stream, so it must not raise."""

        class _Dropping(_ScriptedRedis):
            def pubsub(self):
                raise RedisError("connection reset")

        events = SessionEvents(_Dropping([]), key_prefix="redis:fastapi")
        await events.start()
        await asyncio.wait_for(events._task, timeout=5)
        await events.stop()

    async def test_stop_cancels_a_live_subscription(self) -> None:
        """``listen()`` normally never returns, so cancellation is the exit."""

        class _Blocking(_ScriptedRedis):
            def pubsub(self):
                pubsub = _FakePubSub([])

                async def _forever():
                    while True:
                        await asyncio.sleep(0.01)
                        yield {"type": "subscribe", "data": 1}

                pubsub.listen = _forever  # type: ignore[method-assign]
                return pubsub

        events = SessionEvents(_Blocking([]), key_prefix="redis:fastapi")
        await events.start()
        task = events._task
        assert task is not None and not task.done()
        await events.stop()
        assert task.cancelled() or task.done()
        assert events._task is None


async def _record(sink: list, session_id: str, cause: str) -> None:
    sink.append((session_id, cause))
