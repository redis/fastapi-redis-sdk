"""Tests for the response decision, exercised as a pure function.

``decide_outcome`` takes no store, no request and no Redis, so its whole input
space fits in a loop.  That is the point of naming the outcomes: three defects
in this middleware were overlapping conditions in the wrong order, and an
ordered chain of ``if ... return`` statements cannot be enumerated.
"""

from __future__ import annotations

import itertools

import pytest

from redis_fastapi.sessions import Outcome, _Signals, decide_outcome

_FLAGS = (
    "revoked",
    "rotated",
    "changed",
    "accessed",
    "modified",
    "empty",
    "stored",
    "id_changed",
    "failed",
    "always_save",
    "refresh_on_load",
)


def _signals(**overrides: bool) -> _Signals:
    """A quiet request, with the named flags flipped on."""
    base = dict.fromkeys(_FLAGS, False)
    base["refresh_on_load"] = True
    base["accessed"] = overrides.pop("accessed", True)
    base.update(overrides)
    return _Signals(**base)  # type: ignore[arg-type]


def _every_combination():
    for values in itertools.product([False, True], repeat=len(_FLAGS)):
        yield _Signals(**dict(zip(_FLAGS, values, strict=True)))  # type: ignore[arg-type]


class TestItIsTotalAndSingleValued:
    def test_every_input_maps_to_exactly_one_outcome(self) -> None:
        """2048 combinations, no gaps and no exceptions.

        The property an ordered ``if`` chain cannot assert about itself.
        """
        for signals in _every_combination():
            outcome = decide_outcome(signals)
            assert isinstance(outcome, Outcome)

    def test_every_outcome_is_reachable(self) -> None:
        """A branch nothing can reach is a branch that is wrong."""
        reached = {decide_outcome(s) for s in _every_combination()}
        assert reached == set(Outcome), f"unreachable: {set(Outcome) - reached}"

    def test_the_decision_is_deterministic(self) -> None:
        for signals in _every_combination():
            assert decide_outcome(signals) is decide_outcome(signals)


class TestPrecedence:
    """The orderings that are load-bearing, each asserted against the case it
    would otherwise be shadowed by."""

    def test_revocation_beats_everything(self) -> None:
        assert (
            decide_outcome(
                _signals(revoked=True, changed=True, rotated=True, modified=True)
            )
            is Outcome.CLEAR_COOKIE
        )

    def test_a_failed_response_suppresses_a_rotation(self) -> None:
        assert (
            decide_outcome(_signals(changed=True, modified=True, failed=True))
            is Outcome.SUPPRESS
        )

    def test_a_failed_response_suppresses_an_explicit_reauthentication(
        self,
    ) -> None:
        """The half the guard used to miss.

        ``reauthenticate()`` leaves no trace in the principal, so a guard
        keyed on ``changed`` alone let the payload be written under the old,
        unrotated identifier.
        """
        assert (
            decide_outcome(_signals(rotated=True, modified=True, failed=True))
            is Outcome.SUPPRESS
        )

    def test_a_failed_response_still_persists_ordinary_data(self) -> None:
        """The other half: a failed-login counter has to increment."""
        assert decide_outcome(_signals(modified=True, failed=True)) is Outcome.WRITE

    def test_a_self_rotation_is_not_repeated(self) -> None:
        """Even when the principal changed too - the login-then-rotate idiom."""
        assert (
            decide_outcome(_signals(id_changed=True, changed=True, modified=True))
            is Outcome.COOKIE_ONLY
        )

    def test_emptying_signs_out_rather_than_rotating(self) -> None:
        """Emptying drops the principal to None, which *is* a change."""
        assert (
            decide_outcome(
                _signals(modified=True, empty=True, stored=True, changed=True)
            )
            is Outcome.SIGN_OUT
        )

    def test_emptying_a_session_that_was_never_stored_is_not_a_sign_out(
        self,
    ) -> None:
        assert (
            decide_outcome(_signals(modified=True, empty=True, stored=False))
            is Outcome.WRITE
        )


class TestTheOrdinaryCases:
    def test_an_untouched_request_owes_nothing(self) -> None:
        assert decide_outcome(_signals(accessed=False)) is Outcome.NOTHING

    def test_a_read_only_request_owes_nothing_by_default(self) -> None:
        assert decide_outcome(_signals()) is Outcome.NOTHING

    def test_a_read_only_request_touches_when_the_load_did_not(self) -> None:
        assert (
            decide_outcome(_signals(refresh_on_load=False, stored=True))
            is Outcome.TOUCH
        )

    def test_there_is_nothing_to_touch_without_a_stored_session(self) -> None:
        assert (
            decide_outcome(_signals(refresh_on_load=False, stored=False))
            is Outcome.NOTHING
        )

    def test_a_modification_writes(self) -> None:
        assert decide_outcome(_signals(modified=True)) is Outcome.WRITE

    def test_always_save_writes_without_a_modification(self) -> None:
        assert decide_outcome(_signals(always_save=True)) is Outcome.WRITE

    def test_a_sign_in_rotates(self) -> None:
        assert decide_outcome(_signals(changed=True, modified=True)) is Outcome.ROTATE


class TestInvariantsOverTheWholeSpace:
    """Properties that must hold for *every* input, not just the examples."""

    def test_a_failed_response_never_rotates(self) -> None:
        """§4.3: a response the client saw fail must not issue a new identity."""
        for signals in _every_combination():
            if signals.failed and not signals.revoked:
                assert decide_outcome(signals) is not Outcome.ROTATE

    def test_a_failed_response_never_signs_out_an_identified_session(self) -> None:
        """The narrower true statement, found by the exhaustive sweep.

        An explicit ``clear()`` on a failed response *is* honoured - but only
        for a session that had no principal to lose.  The moment emptying one
        changes the principal, ``SUPPRESS`` takes precedence and nothing is
        written at all.  Signing out grants nothing, so honouring the
        handler's explicit intent is the safe direction here.
        """
        for signals in _every_combination():
            if signals.failed and not signals.revoked:
                if decide_outcome(signals) is Outcome.SIGN_OUT:
                    assert not signals.changed and not signals.rotated

    def test_an_untouched_unflagged_request_never_does_i_o(self) -> None:
        """N-1: a request that never used the session costs nothing."""
        writes = {Outcome.WRITE, Outcome.ROTATE, Outcome.SIGN_OUT, Outcome.TOUCH}
        for signals in _every_combination():
            quiet = not (
                signals.accessed
                or signals.modified
                or signals.revoked
                or signals.rotated
                or signals.changed
                or signals.id_changed
            )
            if quiet:
                assert decide_outcome(signals) not in writes

    def test_revocation_always_clears_the_cookie(self) -> None:
        for signals in _every_combination():
            if signals.revoked:
                assert decide_outcome(signals) is Outcome.CLEAR_COOKIE

    def test_a_changed_identifier_is_never_rotated_again(self) -> None:
        for signals in _every_combination():
            if signals.id_changed and not signals.revoked and not signals.failed:
                assert decide_outcome(signals) is not Outcome.ROTATE


class TestTheDispatchHandlesEveryOutcome:
    def test_no_outcome_is_missing_from_the_middleware(self) -> None:
        """The dispatch ends in an ``AssertionError`` for an unhandled member.

        Adding an ``Outcome`` without a branch is then a loud failure rather
        than a silent fall-through, but this test catches it first.
        """
        import inspect

        from redis_fastapi.sessions import SessionMiddleware

        source = inspect.getsource(SessionMiddleware._on_response_start)
        for member in Outcome:
            assert f"Outcome.{member.name}" in source, f"{member.name} not handled"


@pytest.mark.parametrize("member", list(Outcome))
def test_every_outcome_is_documented(member: Outcome) -> None:
    """Each member carries its own docstring in the enum body."""
    assert Outcome.__doc__
    import inspect

    source = inspect.getsource(Outcome)
    marker = f"{member.name} = auto()"
    assert marker in source
    after = source.split(marker, 1)[1].lstrip()
    assert after.startswith('"""'), f"{member.name} has no docstring"
