"""Server-side sessions for FastAPI and Starlette.

The session record lives in Redis; the cookie carries an opaque identifier and
nothing else.  See ``docs/specs/session-design.md`` for the full design.

This module holds the request-facing half of the feature: the :class:`Session`
mapping the application sees as ``request.session``, the middleware that loads
and saves it, and the exception hierarchy.  The Redis half lives in
``session_backend.py``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI
from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from redis_fastapi.config import get_settings

if TYPE_CHECKING:
    from redis_fastapi.session_backend import SessionStore

# Sentinel for ``pop``/``setdefault`` so that ``None`` stays a usable default.
_MISSING: Any = object()


class SessionError(Exception):
    """Base for every error this feature raises.

    Catching this catches the whole feature.  A ``redis.RedisError`` never
    reaches application code - the store wraps it in
    :class:`SessionStoreError`.
    """


class SessionConfigurationError(SessionError):
    """A session setting is missing, invalid, or contradicts another one."""


class SessionStoreError(SessionError):
    """The store could not complete an operation.

    Raised for every failed **write**, whatever ``session_fail_closed`` is set
    to, because losing a login or a rotation must never be silent.  Failed
    reads raise this only when ``session_fail_closed`` is true; otherwise they
    yield an empty session.  Section 7 of the design explains the asymmetry.
    """


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

    __slots__ = ("accessed", "modified", "sid", "revoked", "rotated")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Construction is the middleware populating us from Redis, not the
        # application touching anything, so both flags start clean.
        self.accessed = False
        self.modified = False
        # Set by the middleware after a load, and by the store after a
        # rotation.  ``None`` means this session has never been written, so
        # there is no key to delete and no cookie to replace.
        self.sid: str | None = None
        # The store sets these; the middleware acts on them at
        # ``http.response.start``.  They are the only channel between the two
        # halves of the feature, so they are attributes rather than a side
        # table keyed on the request.
        self.revoked = False
        self.rotated = False

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


def build_cookie(spec: CookieSpec) -> str:
    """Render a ``Set-Cookie`` value.  The default ``cookie_builder``."""
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


def clear_cookie(spec: CookieSpec) -> str:
    """Render a ``Set-Cookie`` that deletes the cookie.

    An empty value and ``Max-Age=0``.  Every attribute that scopes the cookie
    must match the one that set it, or the browser keeps the original.
    """
    parts = [f"{spec.name}=", f"Path={spec.path}", "Max-Age=0"]
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
_STATE_ATTR = "_redis_session"

# What ``principal_of`` returns when there is no identity to speak of.
_NO_PRINCIPAL: Any = None


@dataclass
class _RequestState:
    """Per-request session bookkeeping, kept off the ``Session`` itself."""

    session: Session
    loaded_id: str | None
    principal_before: Any
    absolute_remaining: int | None


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
        skip: Callable[[Request], bool] | None = None,
    ) -> None:
        self.app = app
        self._store_factory = store_factory
        self._principal_of = principal_of or _default_principal_of
        self._subject_of = subject_of or _default_subject_of
        self._cookie_builder = cookie_builder or build_cookie
        self._skip = skip

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive=receive)
        if self._skip is not None and self._skip(request):
            scope[SCOPE_KEY] = Session()
            await self.app(scope, receive, send)
            return

        settings = get_settings()
        store = await self._store_factory(request)
        state = await self._load(request, store, settings)
        scope[SCOPE_KEY] = state.session
        setattr(request.state, _STATE_ATTR, state)

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
        loaded_id: str | None = None
        absolute_remaining: int | None = None

        # Validate before use. This is not cosmetic: the value is written back
        # into a Set-Cookie header, so an unvalidated one is a header-injection
        # vector. Anything unexpected is treated as no session at all.
        if raw_cookie and store.is_valid_id(raw_cookie):
            loaded = await store.load(
                raw_cookie, refresh=settings.session_refresh_on_load
            )
            if loaded is not None:
                session = Session(loaded.record.data)
                session.sid = raw_cookie
                loaded_id = raw_cookie
                absolute_remaining = loaded.absolute_remaining

        return _RequestState(
            session=session,
            loaded_id=loaded_id,
            principal_before=self._snapshot(session),
            absolute_remaining=absolute_remaining,
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
        """Apply the write rule, then the cookie rule."""
        session = state.session

        if session.accessed:
            # Without this a shared cache can serve one user's page to another.
            headers.append((b"vary", b"Cookie"))

        if session.revoked:
            headers.append(
                (b"set-cookie", clear_cookie(self._spec(settings, "", None)).encode())
            )
            return headers

        principal_after = self._snapshot(session)
        changed = principal_after != state.principal_before
        rotating = (session.rotated or changed) and status < 400

        if changed and status >= 400:
            # A response the client saw fail must not hand out an
            # authenticated session. Persisting the data while skipping the
            # rotation would store the new identity against the *old*,
            # unrotated ID - precisely the fixation this design prevents,
            # arrived at by being helpful. So this request writes nothing.
            return headers

        if rotating:
            new_id = await store.rotate(
                session, subject=self._subject_of(session) or None
            )
            headers.append(
                (
                    b"set-cookie",
                    self._cookie_builder(
                        self._spec(settings, new_id, store.absolute_seconds)
                    ).encode(),
                )
            )
            return headers

        if session.sid is not None and session.sid != state.loaded_id:
            # A handler called ``store.rotate()`` itself, so the work is done
            # and only the cookie is outstanding. Without this branch the
            # browser would keep an identifier whose key the handler deleted,
            # and the user would be signed out by their own step-up.
            headers.append(
                (
                    b"set-cookie",
                    self._cookie_builder(
                        self._spec(settings, session.sid, store.absolute_seconds)
                    ).encode(),
                )
            )
            return headers

        if not session.accessed:
            return headers

        if session.modified or settings.session_always_save:
            await self._write(store, state, settings)
            headers.append(
                (
                    b"set-cookie",
                    self._cookie_builder(
                        self._spec(
                            settings, session.sid or "", state.absolute_remaining
                        )
                    ).encode(),
                )
            )
        elif not settings.session_refresh_on_load and state.loaded_id is not None:
            # The load was a plain read under this setting, so this is the only
            # place left that can advance the idle clock. Omitting this branch
            # freezes the clock and makes every session immortal until its
            # absolute deadline.
            await store.touch(state.loaded_id)

        return headers

    async def _write(
        self, store: SessionStore, state: _RequestState, settings: Any
    ) -> None:
        """Persist the payload, and re-assert the index entry beside it."""
        session = state.session
        if session.sid is None:
            session.sid = store.new_id()
            state.absolute_remaining = store.absolute_seconds
        record = store.new_record(session.raw())
        await store.save(session.sid, record)

        subject = self._subject_of(session)
        if subject:
            # Re-asserted on every write, not only at login: HSETEX is
            # idempotent, it costs nothing extra here, and it repairs an entry
            # that a partial failure lost. The remaining absolute time, never a
            # fresh lifetime.
            await store.index(
                subject,
                session.sid,
                record,
                absolute_remaining=state.absolute_remaining or store.absolute_seconds,
            )

    def _spec(self, settings: Any, value: str, absolute: int | None) -> CookieSpec:
        """Build the cookie spec, sizing ``max-age`` from the server's clocks.

        ``min(idle, absolute remaining)`` - whichever deadline fires first, and
        both numbers come from Redis rather than from this process.  A cookie
        that outlives its record gets the user signed out with no cause and no
        log line; deriving both from the same two server-side numbers is what
        prevents that.
        """
        idle = settings.session_idle_ttl
        max_age: int | None
        if not idle and not settings.session_absolute_ttl:
            max_age = None  # cookie-only mode: the browser decides
        elif absolute is None:
            max_age = idle or None
        elif not idle:
            max_age = absolute
        else:
            max_age = min(idle, absolute)
        return CookieSpec(
            name=settings.session_cookie_name,
            value=value,
            max_age=max_age,
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
    skip: Callable[[Request], bool] | None = None,
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
            package does not know about.
        skip: Predicate for requests that need no session at all.  A request
            it returns true for costs zero Redis calls.

    Raises:
        SessionConfigurationError: If the cookie settings contradict each
            other.
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

    app.add_middleware(
        SessionMiddleware,
        store_factory=get_session_store,
        principal_of=resolver,
        subject_of=subject_of,
        cookie_builder=cookie_builder,
        skip=skip,
    )


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
