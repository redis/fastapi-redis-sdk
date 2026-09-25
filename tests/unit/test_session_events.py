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
    _ABSOLUTE_CHANNEL,
    _ALL_CLASSES_ALIAS,
    _EVENT_CLASSES,
    _IDLE_CHANNEL,
    _KEYEVENT_FLAG,
    REQUIRED_CONFIG,
    Cause,
    Handler,
    SessionEvents,
)


class _FakeRedis:
    """Just enough Redis to answer a probe."""

    def __init__(self, version: str = "7.4.0", flags: str = REQUIRED_CONFIG) -> None:
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
    async def test_a_capable_and_configured_server_gives_the_key_tier(self) -> None:
        assert await _events(_FakeRedis()).probe() == "key"

    @pytest.mark.parametrize("version", ["6.2.14", "7.2.5"])
    async def test_a_server_below_7_4_gives_none(self, version: str) -> None:
        """Hash-field expiry, and the ``hexpired`` event, arrived in 7.4."""
        assert await _events(_FakeRedis(version=version)).probe() == "none"

    @pytest.mark.parametrize("version", ["7.4.0", "8.0.3", "8.8.0"])
    async def test_7_4_and_later_give_the_key_tier(self, version: str) -> None:
        assert await _events(_FakeRedis(version=version)).probe() == "key"

    @pytest.mark.parametrize("flags", ["", "K", "Kh", "Ex", "Eh", "hx", "KA"])
    async def test_missing_flags_give_none(self, flags: str) -> None:
        """``E`` for the channel, and both ``h`` and ``x`` for the two events.

        ``KA`` enables every event class but only on ``__keyspace@``, not on
        the ``__keyevent@`` channels this module subscribes to.
        """
        assert await _events(_FakeRedis(flags=flags)).probe() == "none"

    @pytest.mark.parametrize("flags", ["Ehx", "hxE", "xhE", "Eghxl", "KEhx"])
    async def test_e_plus_both_classes_gives_the_key_tier(self, flags: str) -> None:
        """Order does not matter, and extra flags are somebody else's business."""
        assert await _events(_FakeRedis(flags=flags)).probe() == "key"

    @pytest.mark.parametrize("flags", ["AKE", "EA", "AE"])
    async def test_the_all_classes_alias_counts_as_both(self, flags: str) -> None:
        """``CONFIG GET`` reports ``KEA`` as ``AKE``, with no ``h`` or ``x``.

        Checked against Redis 8.7.  A probe that looked for the literal
        letters refused the commonest configuration there is.
        """
        assert await _events(_FakeRedis(flags=flags)).probe() == "key"

    async def test_the_warning_names_the_required_config(self, caplog) -> None:
        with caplog.at_level("WARNING"):
            assert await _events(_FakeRedis(flags="Eh")).probe() == "none"
        assert "'Ehx'" in caplog.text
        assert "__keyevent@" in caplog.text

    def test_the_documented_config_is_the_one_that_is_checked(self) -> None:
        """``REQUIRED_CONFIG`` is what the guide tells operators to set."""
        assert REQUIRED_CONFIG == "Ehx"
        assert set(REQUIRED_CONFIG) == {_KEYEVENT_FLAG, *_EVENT_CLASSES}
        assert _ALL_CLASSES_ALIAS not in REQUIRED_CONFIG

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
        events = _events(_FakeRedis(version="7.2.0"))
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
        events = _events(_FakeRedis(flags=""))

        @events.on_session_end
        async def _(session_id: str, cause: str) -> None:
            called.append(session_id)

        await events.start()
        await events.stop()
        assert called == []

    async def test_one_warning_is_logged_and_startup_continues(self, caplog) -> None:
        events = _events(_FakeRedis(version="7.2.0"))
        with caplog.at_level("WARNING"):
            await events.start()
        assert len(caplog.records) == 1
        assert "7.4" in caplog.text


class TestSessionIdFromNotification:
    @pytest.fixture()
    def events(self) -> SessionEvents:
        return _events(_FakeRedis())

    def test_a_session_key_gives_its_id(self, events: SessionEvents) -> None:
        assert events._session_id("redis:fastapi:session:abc") == "abc"

    def test_bytes_are_accepted(self, events: SessionEvents) -> None:
        assert events._session_id(b"redis:fastapi:session:abc") == "abc"

    def test_another_application_s_key_is_ignored(self, events: SessionEvents) -> None:
        assert events._session_id("other:key") is None

    def test_the_index_key_is_ignored(self, events: SessionEvents) -> None:
        """Index entries expire constantly and are not session deaths."""
        assert events._session_id("redis:fastapi:sessions-of:42") is None

    @pytest.mark.parametrize("payload", ["", "redis:fastapi:session:", 1])
    def test_a_payload_naming_no_session_is_dropped_not_raised(
        self, events: SessionEvents, payload: object
    ) -> None:
        assert events._session_id(payload) is None


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

        await events._dispatch("redis:fastapi:session:abc", "idle")
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

        await events._dispatch("redis:fastapi:session:abc", "idle")
        assert seen == ["abc"], (
            "a bad handler took down the subscriber and with it every other handler"
        )


class _FakePubSub:
    """Records what was subscribed to, then yields a scripted message stream."""

    def __init__(self, messages: list[dict]) -> None:
        self.messages = messages
        self.subscribed: list[str] = []
        self.closed = False

    async def subscribe(self, *channels: str) -> None:
        self.subscribed.extend(channels)

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


def _event(
    event: str, session_id: str, *, prefix: str = "redis:fastapi", db: int = 0
) -> dict:
    """One notification in the documented ``__keyevent@`` format."""
    return {
        "type": "message",
        "channel": f"__keyevent@{db}__:{event}",
        "data": f"{prefix}:session:{session_id}",
    }


async def _run(redis: _ScriptedRedis, **kwargs) -> list[tuple[str, str]]:
    """Start a subscriber on *redis*, let it drain, and return what it saw."""
    events = SessionEvents(redis, key_prefix="redis:fastapi", **kwargs)
    seen: list[tuple[str, str]] = []
    events.on_session_end(lambda session_id, cause: _record(seen, session_id, cause))
    await events.start()
    assert events.tier == "key"
    await asyncio.wait_for(events._task, timeout=5)
    await events.stop()
    return seen


class TestTheCauseUnionIsInhabited:
    """``Cause`` is a ``Literal`` in a public callback signature.

    That makes it a promise about which values a handler can be called with.
    It used to include ``"revoked"``, which nothing could produce. A caller
    writing the exhaustive ``match`` a type checker rewards was left with an
    arm that could never run and that mypy would not let them delete.
    """

    def test_cause_has_exactly_the_two_values(self) -> None:
        assert set(get_args(Cause)) == {"idle", "absolute"}

    async def test_every_member_is_reachable_from_a_real_event(self) -> None:
        """The union and the channels are checked against each other.

        Adding a member without an event that produces it fails here, which
        is the regression that let ``"revoked"`` survive.
        """
        sid = "a" * 40
        seen = await _run(
            _ScriptedRedis([_event("hexpired", sid), _event("expired", sid)])
        )
        assert {cause for _, cause in seen} == set(get_args(Cause)), (
            "a Cause member no event can produce, or an event the union does not cover"
        )

    async def test_a_deletion_produces_no_event(self) -> None:
        """A ``del`` is either a revocation or an idle expiry emptying the hash.

        It cannot say which, so it is not subscribed to - and if one arrives
        anyway, it is not reported.
        """
        seen = await _run(_ScriptedRedis([_event("del", "a" * 40)]))
        assert seen == []

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


class TestTheDeliveryPath:
    """N-14: the documented recipe has to be executable code under test.

    Subscribe, receive a message, fire a handler. That is where the channel
    names, the ``pubsub()`` lifecycle and the message-shape filtering live,
    so a typo in a channel name must fail here.

    Driven through a scripted Pub/Sub rather than a live server, because the
    logic is deterministic. ``test_session_integration.py`` checks the same
    path against a real server.
    """

    def test_the_channels_match_the_documented_format(self) -> None:
        assert _IDLE_CHANNEL.format(db=0) == "__keyevent@0__:hexpired"
        assert _ABSOLUTE_CHANNEL.format(db=3) == "__keyevent@3__:expired"

    async def test_it_subscribes_to_both_channels_for_its_own_database(
        self,
    ) -> None:
        redis = _ScriptedRedis([])
        await _run(redis, db=7)
        assert sorted(redis.pubsub_obj.subscribed) == [
            "__keyevent@7__:expired",
            "__keyevent@7__:hexpired",
        ]

    async def test_a_field_expiry_reads_as_idle(self) -> None:
        sid = "a" * 40
        assert await _run(_ScriptedRedis([_event("hexpired", sid)])) == [(sid, "idle")]

    async def test_a_key_expiry_reads_as_absolute(self) -> None:
        sid = "a" * 40
        seen = await _run(_ScriptedRedis([_event("expired", sid)]))
        assert seen == [(sid, "absolute")]

    async def test_another_database_s_channel_is_ignored(self) -> None:
        """The cause comes from the channel, so only our own two count."""
        seen = await _run(_ScriptedRedis([_event("expired", "a" * 40, db=1)]))
        assert seen == []

    async def test_non_message_frames_are_ignored(self) -> None:
        """Only ``type == "message"`` is an event, whatever the frame carries.

        The frames below carry a **valid** channel and payload deliberately.
        Otherwise the assertion holds even with the type filter deleted.
        """
        sid = "a" * 40
        valid = _event("hexpired", sid)
        seen = await _run(
            _ScriptedRedis(
                [
                    {**valid, "type": "subscribe"},
                    {**valid, "type": "psubscribe"},
                    {**valid, "type": "pmessage"},
                    valid,
                ]
            )
        )
        assert seen == [(sid, "idle")]

    async def test_another_applications_key_is_ignored(self) -> None:
        """The channels are server-wide; most traffic on them is not ours."""
        seen = await _run(
            _ScriptedRedis(
                [
                    _event("expired", "x" * 40, prefix="someone-else"),
                    {
                        "type": "message",
                        "channel": "__keyevent@0__:expired",
                        "data": "other:key",
                    },
                ]
            )
        )
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
