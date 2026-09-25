"""Server-side sessions for FastAPI and Starlette.

The session record lives in Redis; the cookie carries an opaque identifier and
nothing else.  See ``docs/specs/session-design.md`` for the full design.

This module holds the request-facing half of the feature: the :class:`Session`
mapping the application sees as ``request.session``, the middleware that loads
and saves it, and the exception hierarchy.  The Redis half lives in
``session_backend.py``.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable, Iterable, Mapping, MutableMapping
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum, auto
from inspect import isawaitable
from typing import Any, cast, overload

from fastapi import FastAPI, HTTPException
from fastapi.security.base import SecurityBase
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.status import HTTP_401_UNAUTHORIZED
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from redis_fastapi.config import (
    CACHE_ROUTE_SCOPE_KEY,
    CACHE_SUPPRESS_VARY_SCOPE_KEY,
    SESSION_GATED_SCOPE_KEY,
    SESSION_NO_STORE_SCOPE_KEY,
    get_settings,
)
from redis_fastapi.exceptions import (
    SessionConfigurationError,
    SessionRejected,
)
from redis_fastapi.session_backend import (
    SessionState,
    SessionStore,
    _seconds,
    issued_ago,
)
from redis_fastapi.telemetry import record_session_operation
from redis_fastapi.types import (
    Challenge,
    Coder,
    Encryptor,
    OnReject,
    RecencyRejection,
    SessionRejection,
)

# Sentinel for ``pop``/``setdefault`` so that ``None`` stays a usable default.
_MISSING: Any = object()


class Session(dict):  # type: ignore[type-arg]
    """The mapping an application sees as ``request.session``.

    A ``dict`` subclass that tracks two flags so the middleware knows what to
    do on the way out:

    * ``accessed`` - the application read the session.  Drives ``Vary: Cookie``
      and, under ``session_refresh_on_load=False``, the idle-clock refresh.
    * ``modified`` - the application changed it.  Drives the write.

    ``mark_accessed`` carries exactly that name so Starlette's own
    ``hasattr(session, "mark_accessed")`` hook finds it and calls it.

    Beyond the methods Starlette's session overrides, this class also overrides
    ``popitem``, ``__ior__`` and ``pop`` - each of which loses a mutation
    upstream.  See the table in Section 8 of the design.

    **One fault is unfixable here.**  A change inside a nested value,
    ``session["a"]["b"] = 1``, reaches no method of this class and sets no
    flag.  No ``dict`` subclass in any language can see it.  Reassign the
    top-level key, call ``save()``, or set ``session_always_save=True``.
    """

    __slots__ = ("accessed", "modified")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Construction is the middleware populating us from Redis, not the
        # application touching anything, so both flags start clean.
        self.accessed = False
        self.modified = False
        # Set by the middleware after a load, and by the store after a
        # rotation.  ``None`` means this session has never been written, so
        # there is no key to delete and no cookie to replace.

    # -- flags ---------------------------------------------------------------

    def mark_accessed(self) -> None:
        """Record that the session was read."""
        self.accessed = True

    def mark_modified(self) -> None:
        """Record that the session was changed.

        Sets ``accessed`` too: changing a session is a way of touching it, and
        treating the two independently is the upstream ``pop()`` bug.
        """
        self.accessed = True
        self.modified = True

    # -- reads ---------------------------------------------------------------
    def raw(self) -> dict[Any, Any]:
        """Return a plain-``dict`` snapshot **without** marking the session.

        Every other read marks ``accessed``, which is what the application
        wants and what drives ``Vary: Cookie``.  Two callers must not:

        * the store, when it serializes the payload on the way out - a save
          must not be able to flip a flag it is reacting to;
        * ``principal_of``, which the middleware calls twice per request on
          sessions the application may never have touched.

        Note that neither ``dict(session)`` nor ``session.copy()`` is a
        substitute.  CPython routes both through the subclass's ``keys()``,
        which marks.  Calling the unbound ``dict.items`` is what skips the
        override.
        """
        return dict(dict.items(self))

    def __getitem__(self, key: Any) -> Any:
        self.accessed = True
        return super().__getitem__(key)

    def __contains__(self, key: Any) -> bool:
        self.accessed = True
        return super().__contains__(key)

    def __iter__(self) -> Any:
        self.accessed = True
        return super().__iter__()

    def get(self, key: Any, default: Any = None) -> Any:
        self.accessed = True
        return super().get(key, default)

    def keys(self) -> Any:
        self.accessed = True
        return super().keys()

    def values(self) -> Any:
        self.accessed = True
        return super().values()

    def items(self) -> Any:
        self.accessed = True
        return super().items()

    # -- writes --------------------------------------------------------------

    def __setitem__(self, key: Any, value: Any) -> None:
        self.mark_modified()
        super().__setitem__(key, value)

    def __delitem__(self, key: Any) -> None:
        self.mark_modified()
        super().__delitem__(key)

    def __or__(self, other: Mapping[Any, Any]) -> Session:
        """Return a new ``Session`` merging *other*; leave this one unchanged.

        Overridden only so its return type matches ``__ior__``.  The result is
        a fresh object, so its flags start clean.
        """
        self.accessed = True
        return Session({**dict(self), **dict(other)})

    def __ior__(self, other: Mapping[Any, Any]) -> Session:  # type: ignore[override]
        """Override ``|=``.

        ``dict.__ior__`` runs in C and never calls ``update()``, so without
        this the merge would set no flag and the write would be dropped.
        """
        self.mark_modified()
        super().update(other)
        return self

    def clear(self) -> None:
        self.mark_modified()
        super().clear()

    def update(  # type: ignore[override]
        self, *args: Mapping[Any, Any] | Iterable[tuple[Any, Any]], **kwargs: Any
    ) -> None:
        self.mark_modified()
        super().update(*args, **kwargs)

    def setdefault(self, key: Any, default: Any = None) -> Any:
        """Insert *key* with *default* when absent, and flag only then.

        A ``setdefault`` that finds the key is a read, so flagging it as a
        modification would write the record on every request that called it.
        """
        if key in dict.keys(self):
            self.accessed = True
            return super().__getitem__(key)
        self.mark_modified()
        return super().setdefault(key, default)

    def pop(self, key: Any, default: Any = _MISSING) -> Any:
        """Remove *key*, flagging both ``accessed`` and ``modified``.

        Upstream sets ``modified`` alone, which leaves ``Vary: Cookie`` off a
        response that did touch the session.
        """
        if default is _MISSING:
            value = super().pop(key)
            self.mark_modified()
            return value
        had_key = key in dict.keys(self)
        value = super().pop(key, default)
        if had_key:
            self.mark_modified()
        else:
            self.accessed = True
        return value

    def popitem(self) -> tuple[Any, Any]:
        """Remove and return the last pair.  Upstream sets no flag at all."""
        item = super().popitem()
        self.mark_modified()
        return item


@dataclass(frozen=True)
class CookieSpec:
    """Everything needed to render one ``Set-Cookie`` header.

    Passed to a ``cookie_builder`` seam so an application can add attributes
    this package does not know about.  Starlette's own middleware cannot emit
    ``Partitioned`` and cannot use a ``__Host-`` prefix, and Section 2a of
    ``session-mgmt.md`` records that as a common reason people abandon it.

    ``max_age`` of ``None`` means a session cookie: no ``Max-Age``, and the
    browser drops it when it closes.
    """

    name: str
    value: str
    max_age: int | None
    path: str
    domain: str | None
    secure: bool
    http_only: bool
    same_site: str

    def cleared(self) -> CookieSpec:
        """The same cookie, as a deletion.

        An empty value and ``Max-Age=0``, with every scoping attribute
        untouched - which is the part that matters, because a browser removes
        a cookie only when the clearing header repeats them exactly.

        Deleting is therefore not a second rendering path with its own
        function to keep in step; it is one field change to the spec the
        builder was going to receive anyway.  A ``cookie_builder`` added to
        emit ``Partitioned`` or a ``__Host-`` prefix applies to both without
        knowing this method exists.
        """
        return dataclasses.replace(self, value="", max_age=0)


def build_cookie(spec: CookieSpec) -> str:
    """Render a ``Set-Cookie`` value.  The default ``cookie_builder``.

    Renders a deletion too, when handed ``spec.cleared()``: an empty value and
    ``Max-Age=0`` are what a deletion *is*.  There is deliberately no second
    function for it, because two renderers that must stay in lockstep are two
    renderers that will not.
    """
    parts = [f"{spec.name}={spec.value}", f"Path={spec.path}"]
    if spec.max_age is not None:
        parts.append(f"Max-Age={spec.max_age}")
    if spec.domain:
        parts.append(f"Domain={spec.domain}")
    if spec.secure:
        parts.append("Secure")
    if spec.http_only:
        parts.append("HttpOnly")
    parts.append(f"SameSite={spec.same_site.capitalize()}")
    return "; ".join(parts)


# ---------------------------------------------------------------------------
# SessionMiddleware - the eager load, the response rule, the cookie
# ---------------------------------------------------------------------------

SCOPE_KEY = "session"
STATE_SCOPE_KEY = "redis_session_state"
_STATE_ATTR = "_redis_session"
# How the load went, and the store it used - both for valid_session(), which
# runs after the middleware and must judge the request by what it recorded.
_LOAD_SCOPE_KEY = "redis_session_load"
_STORE_SCOPE_KEY = "redis_session_store"


class _Load(Enum):
    """How the middleware's load went.  Private: ``valid_session()`` maps it
    to a reason."""

    LOADED = auto()
    MISSING = auto()
    """No cookie, or one that fails ``is_valid_id``."""
    NOT_FOUND = auto()
    """A well-formed cookie with no live record."""
    FAILED = auto()
    """The read raised, and ``session_fail_closed`` let the request continue."""
    SKIPPED = auto()
    """The ``skip`` predicate matched, so nothing was loaded."""


# What ``principal_of`` returns when there is no identity to speak of.
_NO_PRINCIPAL: Any = None


class Outcome(Enum):
    """What a response owes the session, decided once.

    Eight mutually exclusive answers.  Naming them is not decoration: three
    separate defects in this middleware were the same shape - overlapping
    boolean conditions evaluated in an order where an earlier branch silently
    shadowed a later one - and an ordered list of ``if ... return`` statements
    cannot show that overlap to a reader or to a test.  Computing the answer
    first, in one pure function, turns "these branches happen to be in the
    right order" into a property that :func:`decide_outcome` states and the
    suite enumerates.

    The three defects, for the record: ``session.clear()`` read as a privilege
    change and minted a new session instead of signing the user out; a handler
    that wrote the identity and then rotated was rotated a second time and got
    back a dead identifier; and a step-up on a failed response wrote the new
    state under the old identifier.
    """

    NOTHING = auto()
    """Not touched, or nothing left to do."""

    CLEAR_COOKIE = auto()
    """The store already ended it; the browser's copy is all that is left."""

    SUPPRESS = auto()
    """The principal changed on a failed response.  Persist nothing at all."""

    SIGN_OUT = auto()
    """The session was emptied.  Delete it, unindex it, clear the cookie."""

    COOKIE_ONLY = auto()
    """A handler rotated for itself.  Only the cookie is outstanding."""

    ROTATE = auto()
    """The principal changed, or a rotation was requested and not performed."""

    WRITE = auto()
    """An ordinary save."""

    TOUCH = auto()
    """Read-only, and the load did not advance the idle clock."""


@dataclass(frozen=True)
class _Signals:
    """The flags :func:`decide_outcome` reads.

    A record rather than ten positional arguments, so the decision can be
    exercised over its whole input space without a store, a request or Redis.
    """

    revoked: bool
    rotated: bool
    changed: bool
    accessed: bool
    modified: bool
    empty: bool
    stored: bool
    """The session has an identifier - it has been written at least once."""
    id_changed: bool
    """That identifier differs from the one the request arrived with."""
    failed: bool
    """The response status is 400 or more."""
    always_save: bool
    refresh_on_load: bool


def decide_outcome(signals: _Signals) -> Outcome:
    """Reduce the request's signals to exactly one :class:`Outcome`.

    Pure, and total: every combination of inputs maps to one answer.  The
    order of the tests below is the whole of the middleware's correctness, so
    each one that must precede another says why.
    """
    # The store has already done the work; nothing may undo or repeat it.
    if signals.revoked:
        return Outcome.CLEAR_COOKIE

    # A response the client saw fail must not hand out an authenticated
    # session.  Writing the payload while skipping the rotation would store
    # the new identity against the *old* identifier, which is the fixation
    # this design exists to prevent, arrived at by being helpful.
    if signals.failed and (signals.changed or signals.rotated):
        return Outcome.SUPPRESS

    # Before ROTATE: a handler that rotated for itself has already replaced
    # the key.  Rotating again would delete what it just wrote and hand the
    # caller back an identifier naming nothing.
    if signals.id_changed:
        return Outcome.COOKIE_ONLY

    # Before ROTATE: emptying a session drops the principal to None, which is
    # a change - so without this test a sign-out reads as a privilege change
    # and mints the user a brand-new valid session.
    if signals.modified and signals.empty and signals.stored:
        return Outcome.SIGN_OUT

    if signals.changed or signals.rotated:
        return Outcome.ROTATE

    # After every branch above, because each of them implies the session was
    # touched even when the application never read it.
    if not signals.accessed:
        return Outcome.NOTHING

    # ``always_save`` is qualified by ``not empty``, and the qualifier is the
    # whole difference between a setting and a footgun.  ``accessed`` is set
    # by *reading* - including Starlette's own ``mark_accessed()`` when a
    # handler touches ``request.session`` at all - so the unqualified test
    # wrote a key and set a cookie for every anonymous visitor to any route
    # that so much as asked ``session.get("user_id")``.  On a public page that
    # is one Redis key per crawler, per health check, per preflight, held for
    # ``gc_ttl``.
    #
    # Nothing is lost that the setting exists for.  Its purpose is nested
    # mutation - ``session["a"]["b"] = 1``, which no ``dict`` subclass can see
    # - and that implies a top-level key already holding the nested value, so
    # the session is not empty.  What it no longer does is create a session
    # out of an empty one, which no nested mutation could have produced.
    if signals.modified or (signals.always_save and not signals.empty):
        return Outcome.WRITE

    # The load was a plain read under this setting, so this is the only place
    # left that can advance the idle clock.  Without it the clock freezes and
    # every session is immortal until its absolute deadline.
    if not signals.refresh_on_load and signals.stored:
        return Outcome.TOUCH

    return Outcome.NOTHING


@dataclass
class _RequestState:
    """Per-request session bookkeeping, kept off the ``Session`` itself."""

    session: Session
    # Everything about the session that is not its data. The store reads and
    # writes this; ``store_state.data`` *is* the ``Session`` above, so a
    # ``revoke`` that clears the data clears what the application holds.
    store_state: SessionState
    loaded_id: str | None
    principal_before: Any
    # Only for the ``descriptor_of`` seam, which is given the live request.
    request: Request | None = None
    load: _Load = _Load.MISSING


class SessionMiddleware:
    """Loads the session before the application and writes it after.

    The load is **eager**, and it has to be: ``HTTPConnection.session`` is a
    synchronous property and cannot await, so by the time any dependency or
    endpoint touches ``request.session`` the Redis read has already happened.
    This middleware is the last place in the chain that can still ``await``.

    Rotation is **detected, not requested**.  The middleware evaluates the
    principal once before the application runs and once at
    ``http.response.start``; if the two differ on a successful response it
    rotates.  An application signs a user in by writing the identity and
    nothing else, so there is no rotation call to forget - which is the only
    mistake in this API that would be a vulnerability.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        store_factory: Callable[[Request], Awaitable[SessionStore]],
        principal_of: Callable[[Session], Any] | None = None,
        subject_of: Callable[[Session], str | None] | None = None,
        cookie_builder: Callable[[CookieSpec], str] | None = None,
        descriptor_of: Callable[[Request, Session], dict[str, Any]] | None = None,
        skip: Callable[[Request], bool] | None = None,
    ) -> None:
        self.app = app
        self._store_factory = store_factory
        self._principal_of = principal_of or _default_principal_of
        self._subject_of = subject_of or _default_subject_of
        self._cookie_builder = cookie_builder or build_cookie
        self._descriptor_of = descriptor_of
        self._skip = skip

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive=receive)
        if self._skip is not None and self._skip(request):
            empty = Session()
            scope[SCOPE_KEY] = empty
            scope[STATE_SCOPE_KEY] = SessionState(data=empty)
            scope[_LOAD_SCOPE_KEY] = _Load.SKIPPED
            await self.app(scope, receive, send)
            return

        settings = get_settings()
        store = await self._store_factory(request)
        scope[_STORE_SCOPE_KEY] = store
        state = await self._load(request, store, settings)
        scope[_LOAD_SCOPE_KEY] = state.load
        scope[SCOPE_KEY] = state.session
        setattr(request.state, _STATE_ATTR, state)
        scope[STATE_SCOPE_KEY] = state.store_state

        started = False

        async def send_with_session(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start" and not started:
                started = True
                headers = list(message.get("headers", []))
                message["headers"] = await self._on_response_start(
                    store, state, settings, message["status"], headers
                )
            await send(message)

        await self.app(scope, receive, send_with_session)

    # -- before the application ----------------------------------------------

    async def _load(
        self, request: Request, store: SessionStore, settings: Any
    ) -> _RequestState:
        """Read the cookie, load the record, take the first principal snapshot."""
        raw_cookie = request.cookies.get(settings.session_cookie_name)
        session = Session()
        store_state = SessionState(data=session)
        loaded_id: str | None = None
        load = _Load.MISSING

        # Validate before use. This is not cosmetic: the value is written back
        # into a Set-Cookie header, so an unvalidated one is a header-injection
        # vector. Anything unexpected is treated as no session at all.
        if raw_cookie and not store.is_valid_id(raw_cookie):
            # The one certain sign of injection: every identifier this store
            # issues passes is_valid_id. Counted on every request, gated or
            # not; never logged, so a scanner cannot flood the log.
            record_session_operation(operation="load", result="malformed")
        elif raw_cookie:
            loaded, failed = await _load_with_status(
                store, raw_cookie, refresh=settings.session_refresh_on_load
            )
            if loaded is not None:
                load = _Load.LOADED
                session = Session(loaded.record.data)
                loaded_id = raw_cookie
                store_state = SessionState(
                    data=session,
                    session_id=raw_cookie,
                    subject=self._subject_of_quietly(session),
                    created=loaded.record.metadata.created,
                    absolute_remaining=loaded.absolute_remaining,
                )
            else:
                load = _Load.FAILED if failed else _Load.NOT_FOUND

        return _RequestState(
            session=session,
            store_state=store_state,
            loaded_id=loaded_id,
            principal_before=self._snapshot(session),
            request=request,
            load=load,
        )

    def _snapshot(self, session: Session) -> Any:
        """Evaluate ``principal_of`` without letting it mark the session.

        The middleware calls this twice on every request, including requests
        the application never touched.  Letting those calls set ``accessed``
        would put ``Vary: Cookie`` on responses that do not vary by cookie and,
        under ``refresh_on_load=False``, refresh the idle clock of a user who
        did nothing.
        """
        accessed, modified = session.accessed, session.modified
        try:
            return self._principal_of(session)
        finally:
            session.accessed, session.modified = accessed, modified

    # -- after the application -----------------------------------------------

    async def _on_response_start(
        self,
        store: SessionStore,
        state: _RequestState,
        settings: Any,
        status: int,
        headers: list[tuple[bytes, bytes]],
    ) -> list[tuple[bytes, bytes]]:
        """Decide once what this response owes, then do exactly that.

        The decision is :func:`decide_outcome`, which is pure and lives apart
        from the I/O so its branch order can be read, tested and argued about
        on its own.  This method only carries it out.
        """
        session = state.session
        store_state = state.store_state

        if session.accessed:
            self._apply_cache_headers(scope_of(state), headers)

        # ``principal_of`` is user code and must not mark the session, so the
        # snapshot is shielded. It is the one signal that costs anything.
        changed = self._snapshot(session) != state.principal_before

        outcome = decide_outcome(
            _Signals(
                revoked=store_state.revoked,
                rotated=store_state.rotated,
                changed=changed,
                accessed=session.accessed,
                modified=session.modified,
                empty=not session,
                stored=state.loaded_id is not None,
                id_changed=(
                    store_state.session_id is not None
                    and store_state.session_id != state.loaded_id
                ),
                failed=status >= 400,
                always_save=bool(settings.session_always_save),
                refresh_on_load=bool(settings.session_refresh_on_load),
            )
        )

        if outcome is Outcome.NOTHING or outcome is Outcome.SUPPRESS:
            return headers

        if outcome is Outcome.CLEAR_COOKIE:
            return self._with_cookie(headers, settings, clear=True)

        if outcome is Outcome.SIGN_OUT:
            await store.revoke(store_state)
            return self._with_cookie(headers, settings, clear=True)

        if outcome is Outcome.COOKIE_ONLY:
            # Any payload change the handler made *after* its rotate call is
            # not in the record ``rotate`` wrote, so catch it here.
            if session.modified:
                await self._write(store, state, settings, create=False)
            return self._with_cookie(headers, settings, store_state)

        if outcome is Outcome.ROTATE:
            await store.rotate(
                store_state,
                subject=self._subject_of_quietly(session) or None,
                descriptor=self._descriptor(state),
            )
            return self._with_cookie(headers, settings, store_state)

        if outcome is Outcome.WRITE:
            await self._write(
                store, state, settings, create=store_state.session_id is None
            )
            return self._with_cookie(headers, settings, store_state)

        if outcome is Outcome.TOUCH:
            await store.touch(cast(str, state.loaded_id))
            return headers

        raise AssertionError(f"unhandled outcome {outcome!r}")  # pragma: no cover

    @staticmethod
    def _apply_cache_headers(
        scope: MutableMapping[str, Any], headers: list[tuple[bytes, bytes]]
    ) -> None:
        """Say how this response varies, and who may store it.

        Two rules, and both exist because the caching feature and this
        middleware must agree rather than each appending a header of its own.

        ``Vary: Cookie`` is **merged** into any existing value rather than
        appended, so a response that already varies on ``Accept-Encoding``
        ends up with one header listing both.  Two ``Vary`` lines are legal
        but proxies handle them inconsistently.

        It is **omitted** entirely when a ``cache(vary_on_session=False)``
        route has said the body does not depend on the session.  The session
        was read - by an auth dependency, typically - but the answer is the
        same for everyone, and claiming otherwise forces a shared cache to
        keep one copy per user of an identical payload.

        ``Cache-Control: private`` is emitted only when no ``cache()`` owns
        the route.  Where one does, it sets the directive itself from the
        route's declaration, and a second writer here is what produced
        ``max-age=300, private, no-store`` in one response.

        ``no-store`` wins over ``private``.  A route gated by
        ``valid_session(issued_within=...)`` gets ``no-store`` instead, and a
        response whose handler already said ``no-store`` gets nothing added:
        ``no-store, private`` is valid, but the ``private`` says nothing.
        """
        if not scope.get(CACHE_SUPPRESS_VARY_SCOPE_KEY):
            _merge_header(headers, b"vary", b"Cookie")
        if scope.get(CACHE_ROUTE_SCOPE_KEY):
            return
        if scope.get(SESSION_NO_STORE_SCOPE_KEY):
            _merge_header(headers, b"cache-control", b"no-store")
        elif not _has_directive(headers, b"cache-control", b"no-store"):
            _merge_header(headers, b"cache-control", b"private")

    def _with_cookie(
        self,
        headers: list[tuple[bytes, bytes]],
        settings: Any,
        store_state: SessionState | None = None,
        *,
        clear: bool = False,
    ) -> list[tuple[bytes, bytes]]:
        """Append one ``Set-Cookie``, always through the configured builder.

        The value and ``max-age`` both come from *store_state*: its identifier,
        and what the server says is left of its absolute clock.  A create, a
        rotation and a load each leave that number there, so one source serves
        every live cookie.

        Deletion goes through the same seam as creation.  A browser removes a
        cookie only when the clearing header repeats every scoping attribute,
        so a ``cookie_builder`` added to emit ``Partitioned`` or a ``__Host-``
        prefix - the seam's whole purpose - has to be consulted here too.  It
        was not, and the result was a sign-out that left the cookie in place.
        """
        if store_state is None:
            spec = self._spec(settings, "", None)
        else:
            spec = self._spec(
                settings, store_state.session_id or "", store_state.absolute_remaining
            )
        if clear:
            spec = spec.cleared()
        headers.append((b"set-cookie", self._cookie_builder(spec).encode()))
        return headers

    def _descriptor(self, state: _RequestState) -> dict[str, Any] | None:
        """Evaluate the ``descriptor_of`` seam, if one was supplied."""
        if self._descriptor_of is None or state.request is None:
            return None
        return self._descriptor_of(state.request, state.session)

    def _subject_of_quietly(self, session: Session) -> str | None:
        """Call ``subject_of`` without letting it mark the session.

        Same reason as :meth:`_snapshot`: the middleware calls it on requests
        the application never touched, and a read that sets ``accessed`` would
        put ``Vary: Cookie`` on responses that do not vary by cookie.
        """
        accessed, modified = session.accessed, session.modified
        try:
            return self._subject_of(session)
        finally:
            session.accessed, session.modified = accessed, modified

    async def _write(
        self, store: SessionStore, state: _RequestState, settings: Any, *, create: bool
    ) -> None:
        """Persist the payload, and re-assert the index entry beside it."""
        session = state.session
        store_state = state.store_state
        if store_state.session_id is None:
            store_state.session_id = store.new_id()
            store_state.absolute_remaining = store.absolute_seconds
            create = True
        record = store.new_record(session.raw(), created=store_state.created)
        if create:
            await store.create(store_state.session_id, record)
            store_state.created = record.metadata.created
        else:
            await store.save(store_state.session_id, record)

        subject = self._subject_of_quietly(session)
        store_state.subject = subject
        if subject:
            # Re-asserted on every write, not only at login: it costs nothing
            # extra here, and it repairs an entry that a partial failure lost.
            # The remaining absolute time, never a fresh lifetime.
            await store.index(
                subject,
                store_state.session_id,
                record,
                absolute_remaining=store_state.absolute_remaining
                or store.absolute_seconds,
                extra=self._descriptor(state),
            )

    def _spec(self, settings: Any, value: str, absolute: int | None) -> CookieSpec:
        """Build the cookie spec, sizing ``max-age`` from the absolute clock.

        The absolute remainder, from Redis rather than from this process.  Not
        ``min(idle, absolute remaining)``: the idle clock slides on every
        request, but a read-only response sends no cookie, so a cookie sized by
        the idle clock expires while the record is alive and signs a reading
        user out with no cause and no log line.  The absolute clock never
        slides, so a cookie sent on any write stays correct until the record's
        last possible moment.  Redis still enforces the idle clock.
        """
        cookie_only = (
            not settings.session_idle_ttl and not settings.session_absolute_ttl
        )
        return CookieSpec(
            name=settings.session_cookie_name,
            value=value,
            # Cookie-only mode sends no max-age: the browser decides.
            max_age=None if cookie_only else absolute,
            path=settings.session_cookie_path,
            domain=settings.session_cookie_domain,
            secure=settings.session_cookie_https_only,
            http_only=True,
            same_site=settings.session_cookie_same_site,
        )


def _default_principal_of(session: Session) -> Any:
    """Read the configured principal keys, in order.

    Returns a tuple so that adding ``role`` or ``scopes`` to the setting makes
    a privilege change rotate as well as a sign-in.
    """
    keys = get_settings().session_principal_keys
    values = tuple(dict.get(session, key) for key in keys)
    return values if any(v is not None for v in values) else _NO_PRINCIPAL


def _default_subject_of(session: Session) -> str | None:
    """The index key for this session, or ``None`` for an anonymous one.

    No subject means no index entry, which is the right answer: there is
    nothing for ``revoke_all`` to promise.
    """
    keys = get_settings().session_principal_keys
    for key in keys:
        value = dict.get(session, key)
        if value is not None:
            return str(value)
    return None


# ---------------------------------------------------------------------------
# add_redis_sessions() - one-time app setup
# ---------------------------------------------------------------------------


def add_redis_sessions(
    app: FastAPI,
    *,
    principal_of: Callable[[Session], Any] | None = None,
    principal_keys: list[str] | None = None,
    subject_of: Callable[[Session], str | None] | None = None,
    cookie_builder: Callable[[CookieSpec], str] | None = None,
    descriptor_of: Callable[[Request, Session], dict[str, Any]] | None = None,
    skip: Callable[[Request], bool] | None = None,
    challenge: Challenge | None = None,
    store: SessionStore | None = None,
    store_factory: Callable[[Request], Any] | None = None,
    coder: type[Coder] | None = None,
    encryptor: Encryptor | None = None,
    id_factory: Callable[[], str] | None = None,
    key_prefix: str | None = None,
    idle_ttl: int | timedelta | None = None,
    absolute_ttl: int | timedelta | None = None,
    gc_ttl: int | timedelta | None = None,
) -> None:
    """Register :class:`SessionMiddleware` on *app*.

    Prefer the builder API::

        FastAPIRedis(app).lifespan().sessions()

    Args:
        app: The FastAPI application.
        principal_of: Pure function returning the value whose change triggers
            a rotation.  Defaults to reading ``session_principal_keys``.
            **Must be pure, cheap and deterministic** - it runs twice per
            request and its two results are compared by value.  It must also
            return a *verified* identity, never something the client set.
        principal_keys: Convenience for the common case: the session keys to
            watch, instead of writing a function.  Overrides the setting.
        subject_of: Which subject a session is indexed under.  ``None``
            disables the index for that session, which is correct for an
            anonymous one.  The subject need not be a user - it can be a
            tenant, a device, or an API client.
        cookie_builder: Renders the ``Set-Cookie`` value, for attributes this
            package does not know about.  Used for clearing the cookie as
            well as setting it.
        descriptor_of: What a device listing should show for this session -
            an IP, a user agent, a device name.  Stored beside the session in
            the index, never inside the payload, so it cannot appear in the
            application's own ``request.session``.
        skip: Predicate for requests that need no session at all.  A request
            it returns true for costs zero Redis calls.
        challenge: What the default rejection of ``valid_session()`` sends as
            ``WWW-Authenticate``: a FastAPI security scheme, whose own
            challenge is used; a fixed string; or a callable receiving the
            request and the reason.  ``None``, the default, sends no header.
            ``Basic`` is refused, because it makes every browser open its
            password dialog on a gated page.
        store: A ready-made store, used for every request.  The escape hatch
            for a backend that is not Redis, and for a test double.
        store_factory: Called per request to build one, when a single instance
            will not do.  May be sync or async.
        coder: Serializer for the envelope.  Defaults to ``JsonCoder``.
        encryptor: Encrypts the serialized envelope at rest.  Encryption wraps
            serialization, so the coder never sees ciphertext.
        id_factory: Generates session IDs.  Its output is validated on every
            call, because a seam supplying a security-critical value has to be
            checked rather than trusted.
        key_prefix: Overrides the key namespace.
        idle_ttl: Idle clock, overriding ``session_idle_ttl``.  Accepts a
            ``timedelta``.
        absolute_ttl: Absolute clock, overriding ``session_absolute_ttl``.
        gc_ttl: Backstop TTL, overriding ``session_gc_ttl``.

    These last eight exist because the store constructor has always accepted
    them and nothing reachable from here passed them on: the only way to change
    a coder was to replace the whole dependency, and that did not reach the
    middleware at all.

    Raises:
        SessionConfigurationError: If the cookie settings contradict each
            other, or *challenge* names ``Basic``.
    """
    settings = get_settings()
    if settings.session_cookie_same_site == "none" and not (
        settings.session_cookie_https_only
    ):
        raise SessionConfigurationError(
            "session_cookie_same_site='none' requires "
            "session_cookie_https_only=True: browsers reject a SameSite=None "
            "cookie that is not Secure, so the session would never be stored."
        )

    resolver = principal_of
    if resolver is None and principal_keys is not None:
        watched = list(principal_keys)

        def resolver(session: Session) -> Any:  # noqa: F811
            values = tuple(dict.get(session, key) for key in watched)
            return values if any(v is not None for v in values) else None

    from redis_fastapi.deps import get_session_store

    # Tells the lifespan that a session-event subscriber may be worth starting;
    # cache-only apps never set it. Safe before startup: the builder runs at
    # app-construction time and the lifespan reads it later.
    app.state._redis_sessions = True

    from redis_fastapi.deps import _SessionStoreOptions

    kwargs: dict[str, Any] = {
        name: value
        for name, value in (
            ("coder", coder),
            ("encryptor", encryptor),
            ("id_factory", id_factory),
            ("key_prefix", key_prefix),
            ("idle_ttl", idle_ttl),
            ("absolute_ttl", absolute_ttl),
            ("gc_ttl", gc_ttl),
        )
        if value is not None
    }
    if store is not None and store_factory is not None:
        raise SessionConfigurationError("Pass either store or store_factory, not both.")
    if isinstance(challenge, (str, SecurityBase)):
        _refuse_basic(_scheme_challenge(challenge))
    app.state._redis_session_challenge = challenge
    app.add_exception_handler(SessionRejected, session_rejected_handler)
    app.state._redis_session_options = _SessionStoreOptions(
        store=store, store_factory=store_factory, kwargs=kwargs
    )

    app.add_middleware(
        SessionMiddleware,
        store_factory=get_session_store,
        principal_of=resolver,
        subject_of=subject_of,
        cookie_builder=cookie_builder,
        descriptor_of=descriptor_of,
        skip=skip,
    )


def session_state_of(request: Request) -> SessionState:
    """Return the :class:`SessionState` the middleware built for *request*.

    The handle the store operates on: the identifier, the subject, the
    timestamps and the pending rotation or revocation.  Handlers need it to
    call ``rotate``, ``revoke`` or ``session_id``; ``request.session`` remains
    the way to reach the data.

    Raises:
        SessionConfigurationError: If no middleware is registered.
    """
    state = request.scope.get(STATE_SCOPE_KEY)
    if not isinstance(state, SessionState):
        raise SessionConfigurationError(
            "No session was loaded for this request. Call "
            "FastAPIRedis(app).lifespan().sessions() during setup, or "
            "add_redis_sessions(app) directly."
        )
    return state


def session_of(request: Request) -> Session:
    """Return the :class:`Session` the middleware loaded for *request*.

    The backing dependency for ``SessionDep``.  Never performs I/O: the load
    already happened before the application ran.

    Raises:
        SessionConfigurationError: If no middleware is registered, which would
            otherwise surface as a confusing ``KeyError`` deep in a handler.
    """
    session = request.scope.get(SCOPE_KEY)
    if not isinstance(session, Session):
        raise SessionConfigurationError(
            "No session was loaded for this request. Call "
            "FastAPIRedis(app).lifespan().sessions() during setup, or "
            "add_redis_sessions(app) directly."
        )
    return session


def _has_directive(
    headers: list[tuple[bytes, bytes]], name: bytes, directive: bytes
) -> bool:
    """Whether a header of that name already lists *directive*."""
    for existing_name, existing_value in headers:
        if existing_name.lower() != name:
            continue
        parts = [p.strip().lower() for p in existing_value.split(b",")]
        if directive in parts:
            return True
    return False


def _merge_header(
    headers: list[tuple[bytes, bytes]], name: bytes, value: bytes
) -> None:
    """Add *value* to an existing header of that name, or append a new one.

    Idempotent: a value already present is not repeated.
    """
    for index, (existing_name, existing_value) in enumerate(headers):
        if existing_name.lower() != name:
            continue
        parts = [p.strip() for p in existing_value.split(b",") if p.strip()]
        if value in parts:
            return
        headers[index] = (existing_name, b", ".join([*parts, value]))
        return
    headers.append((name, value))


def scope_of(state: _RequestState) -> MutableMapping[str, Any]:
    """The ASGI scope behind a request state, for reading cross-feature flags."""
    return state.request.scope if state.request is not None else {}


# ---------------------------------------------------------------------------
# valid_session() - the session gate
# ---------------------------------------------------------------------------

_REASON: dict[_Load, SessionRejection] = {
    _Load.MISSING: "missing",
    _Load.NOT_FOUND: "expired",
    _Load.FAILED: "unavailable",
}


@overload
def valid_session(
    *, on_reject: OnReject[SessionRejection] | None = None
) -> Callable[[Request], Awaitable[None]]: ...


@overload
def valid_session(
    *,
    issued_within: int | timedelta,
    on_reject: OnReject[RecencyRejection] | None = None,
) -> Callable[[Request], Awaitable[None]]: ...


def valid_session(
    *,
    issued_within: int | timedelta | None = None,
    on_reject: OnReject[Any] | None = None,
) -> Callable[[Request], Awaitable[None]]:
    """Return a dependency that rejects a request without a valid session.

    Valid means the cookie named a record that was live in Redis when the
    request arrived, and nothing earlier in the request has ended it.  Says
    nothing about who the session belongs to: an anonymous session passes.

    Marks the session as read, so the response carries ``Vary: Cookie`` and
    is treated as depending on the session - which it does.  On a route
    cached with ``cache(vary_on_session=False)`` the response says
    ``private``, so a shared cache cannot serve a gated page to a caller the
    gate refuses.  List it **before** ``cache()``: in the other order it
    raises on the first request, because a cache hit would skip it.

    Args:
        issued_within: Also require the session's ID to have been issued at
            most this long ago - by a sign-in, a rotation or
            ``store.reauthenticate()``.  Adds the reason ``"stale"``, and makes
            the response ``Cache-Control: no-store``.  **Not an
            authentication check:** a new anonymous session is recent, so pair
            it with the application's own authentication.
        on_reject: Build the rejection.  Receives the request and the reason,
            and returns the response - sync or async.  Default: 401, with a
            ``WWW-Authenticate`` header only if the ``challenge`` setting is
            configured.

    Raises:
        SessionConfigurationError: At request time, if sessions are not set
            up, the route is excluded by ``skip``, or ``cache()`` runs before
            this gate.
        TypeError: At request time, if *on_reject* returns something that is
            not a ``Response``.
    """
    limit = None if issued_within is None else _seconds(issued_within)

    async def _dependency(request: Request) -> None:
        if request.scope.get(CACHE_SUPPRESS_VARY_SCOPE_KEY):
            # cache() already ran, so on a later request its hit would be
            # served before this gate. Refusing here keeps the first response
            # from ever being stored, so no hit can exist.
            raise SessionConfigurationError(
                f"{request.url.path}: valid_session() must come before "
                "cache(vary_on_session=False) in dependencies=[...]"
            )
        session = session_of(request)
        state = session_state_of(request)
        load = request.scope.get(_LOAD_SCOPE_KEY, _Load.MISSING)
        if load is _Load.SKIPPED:
            raise SessionConfigurationError(
                f"{request.url.path} is excluded by skip and gated by "
                "valid_session(); it can never pass."
            )
        session.mark_accessed()
        if limit is not None:
            request.scope[SESSION_NO_STORE_SCOPE_KEY] = True

        reason: RecencyRejection
        if load is _Load.LOADED:
            # Emptied earlier in this request: the middleware will sign it
            # out on the way out, so it is already ended. ``dict.__len__``
            # does not mark the session.
            cleared = session.modified and dict.__len__(session) == 0
            if state.session_id is not None and not state.revoked and not cleared:
                request.scope[SESSION_GATED_SCOPE_KEY] = True
                if limit is None:
                    return
                age = _age_of(request, state)
                if age is not None and age <= limit:
                    return
                reason = "stale"
            else:
                reason = "missing"
        else:
            reason = _REASON[load]

        if on_reject is None:
            raise _default_rejection(request, reason, limit)
        response = on_reject(request, reason)
        if isawaitable(response):
            response = await response
        if not isinstance(response, Response):
            # Fail closed, and name the bug: a callback that returned nothing
            # must not let the request through.
            raise TypeError(
                f"on_reject must return a Response, got {type(response).__name__}"
            )
        raise SessionRejected(response)

    return _dependency


async def session_rejected_handler(request: Request, exc: Exception) -> Response:
    """Return the response carried by :class:`SessionRejected`."""
    return cast(SessionRejected, exc).response


def _age_of(request: Request, state: SessionState) -> int | None:
    """Seconds since the session's ID was issued, measured with the store the
    middleware loaded it with - the one whose ``absolute_seconds`` sized the
    key's TTL."""
    store = request.scope.get(_STORE_SCOPE_KEY)
    if store is None:
        return None
    return issued_ago(store.absolute_seconds, state)


def _default_rejection(
    request: Request, reason: RecencyRejection, limit: int | None
) -> Exception:
    """The 401 ``valid_session()`` raises when no ``on_reject`` is given."""
    headers = challenge_headers(request, reason)
    if reason == "stale":
        return SessionRejected(
            JSONResponse(
                {
                    "detail": "A recently issued session is required",
                    "error": "stale_session",
                    "issued_within": limit,
                },
                status_code=HTTP_401_UNAUTHORIZED,
                headers=headers or None,
            )
        )
    return HTTPException(
        HTTP_401_UNAUTHORIZED, "No valid session", headers=headers or None
    )


def challenge_headers(request: Request, reason: str) -> dict[str, str]:
    """The ``WWW-Authenticate`` header for a default rejection, or none.

    Read from the ``challenge`` setting at request time, so it does not matter
    whether routes are declared before or after ``.sessions()``.
    """
    challenge = getattr(request.app.state, "_redis_session_challenge", None)
    if challenge is None:
        return {}
    if isinstance(challenge, (str, SecurityBase)):
        value = _scheme_challenge(challenge)
    else:
        value = challenge(request, reason)
        if value is not None:
            _refuse_basic(value)
    return {"WWW-Authenticate": value} if value else {}


def _scheme_challenge(challenge: str | SecurityBase) -> str | None:
    """The challenge a string or a FastAPI security scheme stands for."""
    if isinstance(challenge, str):
        return challenge
    # Every class in fastapi.security builds its own 401 with its challenge.
    # An older FastAPI without the method sends no header.
    make_error = getattr(challenge, "make_not_authenticated_error", None)
    if make_error is None:
        return None
    headers = getattr(make_error(), "headers", None) or {}
    return cast("str | None", headers.get("WWW-Authenticate"))


def _refuse_basic(value: str | None) -> None:
    """Refuse a ``Basic`` challenge: every browser answers it with a native
    password dialog, on every gated page."""
    if value and value.split(maxsplit=1)[0].lower() == "basic":
        raise SessionConfigurationError(
            "challenge must not be Basic: browsers answer it with a password "
            "dialog on every gated page. Use the application's own scheme, "
            "such as APIKeyCookie, or a string like 'Cookie realm=...'."
        )


async def _load_with_status(
    store: SessionStore, session_id: str, *, refresh: bool
) -> tuple[Any, bool]:
    """``store.load_with_status``, or ``load`` for a store that lacks it.

    A store implementing only ``SessionStoreProtocol`` cannot say that its
    read failed, so a failure reads as an unknown identifier: the gate then
    says "expired" during an outage - the wrong word, but still a rejection.
    """
    load_with_status = getattr(store, "load_with_status", None)
    if load_with_status is not None:
        return cast(
            "tuple[Any, bool]", await load_with_status(session_id, refresh=refresh)
        )
    return await store.load(session_id, refresh=refresh), False
