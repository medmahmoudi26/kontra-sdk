"""Sessions and login (PRD §10.3, §12).

A :class:`Session` turns a long-lived credential -- the profile's refresh token, or a
program's API key (:meth:`Session.from_api_key`, ``KONTRA_API_KEY``) -- into what the rest
of the SDK needs:

* a gate access token (15 min), refreshed 60 s before it expires and on a 401, behind
  :attr:`Session.gate`;
* Temporal worker JWTs (:meth:`Session.temporal_token`), cached the same way;
* a ``temporalio`` client whose ``api_key`` is replaced in the background before the JWT
  expires (:meth:`Session.connect_temporal`), so a worker runs for days on 15-minute tokens;
  :meth:`Session.release` stops that when the client is done with, :meth:`Session.aclose`
  stops all of them.

The gate rotates the refresh token on every use and revokes the whole session if an old
one is presented again, so refreshing is serialised: within a process by a lock, between
processes sharing a profile by :func:`kontra.profile.profile_lock`, and each refresh first
adopts a token another process may have rotated in the meantime. An API key does not rotate:
its session refreshes by exchanging the key again, and writes nothing anywhere. Such a
session can hire, serve and read, never publish or manage keys (decisions I19).

Login is the RFC 8628 device flow (:func:`login_device`; with ``email``, approved through a
link the gate mails) or, against ``kontra dev``, :func:`dev_login`; :func:`login` runs
either and writes the profile and key.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import os
import sys
import time
import webbrowser
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

import httpx

from kontra.contract.names import validate_handle
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.profile import NotLoggedIn, Profile, ProfileError, kontra_home, profile_lock

if TYPE_CHECKING:
    from temporalio.client import Client
    from temporalio.converter import DataConverter

__all__ = [
    "API_KEY_ENV",
    "DEFAULT_GATE_URL",
    "DEVICE_GRANT",
    "GATE_ENV",
    "REFRESH_MARGIN",
    "LoginError",
    "Session",
    "api_key_from_env",
    "dev_login",
    "login",
    "login_device",
]

log = logging.getLogger("kontra.auth")

DEVICE_GRANT = m.DEVICE_GRANT
#: A program's credential. When set, :meth:`Session.from_env` -- ``Kontra()``, ``kontra
#: serve``, ``kontra mcp`` and every CLI command -- uses it instead of the profile.
API_KEY_ENV = "KONTRA_API_KEY"
#: The gate, when no ``--gate`` and (for a login) no profile says otherwise.
GATE_ENV = "KONTRA_GATE"
#: The gate when nothing is configured at all.
DEFAULT_GATE_URL = "https://api.kontra.run"
#: Tokens are replaced this long before they expire (or at half their lifetime if shorter).
REFRESH_MARGIN = timedelta(seconds=60)
#: The background refresher never sleeps less than this, so a skewed clock cannot spin it.
_MIN_REFRESH_DELAY_S = 1.0
_MAX_RETRY_DELAY_S = 30.0
_SLOW_DOWN_S = 5  # RFC 8628 §3.5
_MAX_POLL_INTERVAL_S = 60
_LOOPBACK = frozenset({"localhost", "127.0.0.1", "::1", "[::1]"})

Clock = Callable[[], datetime]
Sleep = Callable[[float], Awaitable[None]]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _refresh_point(issued: datetime, expires: datetime) -> datetime:
    """When to replace a token issued at ``issued`` that expires at ``expires``."""
    margin = min(REFRESH_MARGIN, (expires - issued) / 2)
    return expires - max(margin, timedelta(0))


_MAX_LIFETIME = timedelta(days=1)


def _jwt_lifetime(token: str) -> timedelta | None:
    """``exp - iat`` of a JWT, read without verifying it (the gate and Temporal verify it).

    Both claims are on the issuer's clock, so their difference is the token's lifetime
    whatever this machine's clock says; comparing the gate's absolute ``expires_at`` with our
    clock instead would keep an expired ``api_key`` for as long as our clock is behind, or
    refresh every second once it is ahead by more than the lifetime. None when the token is
    not a JWT with numeric ``iat < exp``. Capped at a day, so a nonsense claim cannot stop
    refreshes."""
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        raw = base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4))
        claims: object = json.loads(raw)
    except ValueError:  # binascii.Error, UnicodeDecodeError and JSONDecodeError are ValueErrors
        return None
    if not isinstance(claims, dict):
        return None
    found = cast("dict[str, object]", claims)
    iat, exp = found.get("iat"), found.get("exp")
    if (
        not isinstance(iat, int | float)
        or not isinstance(exp, int | float)
        or isinstance(iat, bool)
        or isinstance(exp, bool)
        or not 0 < exp - iat < float("inf")  # also refuses NaN
    ):
        return None
    return timedelta(seconds=min(exp - iat, _MAX_LIFETIME.total_seconds()))


def _check_api_key(value: str, where: str) -> None:
    """Refuse a value that cannot be an API key, without ever showing it: a key with a
    stray newline or quote is still the secret."""
    if not m.is_api_key(value):
        raise ProfileError(
            f"{where} is not a Kontra API key ({m.API_KEY_PREFIX} and 43 letters, digits, "
            "- or _); check it for stray whitespace or quotes"
        )


def api_key_from_env() -> str | None:
    """``$KONTRA_API_KEY`` when it is set and not empty (checked for shape: a malformed one
    raises :class:`~kontra.profile.ProfileError`, which never quotes it), else None."""
    value = os.environ.get(API_KEY_ENV)
    if not value:
        return None
    _check_api_key(value, API_KEY_ENV)
    return value


class LoginError(Exception):
    """A login that cannot complete. ``code`` is the RFC 8628 error (``access_denied``,
    ``expired_token``) or one of ours (``key_mismatch``, ``party_mismatch``, ``insecure_gate``)."""

    def __init__(self, code: str, message: str) -> None:
        self.code, self.message = code, message
        super().__init__(f"{code}: {message}")


class _ApiKeyHolder(Protocol):
    """What the background refresher needs of a client: ``temporalio.client.Client``
    re-sends its ``api_key`` on every call made after it is assigned."""

    @property
    def api_key(self) -> str | None: ...
    @api_key.setter
    def api_key(self, value: str | None) -> None: ...


class _AccessTokens:
    """The :data:`~kontra.gate_client.TokenProvider` a session hands its GateClient."""

    def __init__(self, session: Session) -> None:
        self._session = session

    async def __call__(self) -> str | None:
        return await self._session.access_token()

    def invalidate(self, token: str) -> None:
        self._session._invalidate(token)


class Session:
    """A logged-in party: ``Session(Profile.load())`` or :meth:`Session.load` for a person's
    login, :meth:`Session.from_api_key` for a program, :meth:`Session.from_env` for
    whichever this process was given.

    With a profile, ``home`` is where a rotated refresh token is written back; by default
    the directory the profile was loaded from (a profile built in memory is not persisted).
    With an ``api_key`` (``gate_url`` defaults to :data:`DEFAULT_GATE_URL`) nothing is ever
    written; ``home`` is only where :meth:`identity` reads the party's key, and the session
    learns who it is from the gate on first use (:meth:`resolve`). ``clock`` and ``sleep``
    exist for tests.
    """

    def __init__(
        self,
        profile: Profile | None = None,
        *,
        api_key: str | None = None,
        gate_url: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        home: Path | str | None = None,
        clock: Clock | None = None,
        sleep: Sleep | None = None,
    ) -> None:
        if (profile is None) == (api_key is None):
            raise TypeError("a Session takes a profile or an api_key")
        if profile is not None:
            if gate_url is not None:
                raise TypeError("a profile names its own gate; gate_url is for an api_key")
            gate_url = profile.gate_url
            self._home: Path | None = Path(home) if home is not None else profile.home
            self._identity_home: Path | None = self._home
        else:
            assert api_key is not None
            _check_api_key(api_key, "the API key")
            gate_url = gate_url or DEFAULT_GATE_URL
            _check_gate_url(gate_url, dev=False)  # a key is a bearer credential: TLS only
            self._home = None  # an API key's session persists nothing
            self._identity_home = Path(home) if home is not None else None
        self._profile = profile
        self._api_key = api_key
        self._gate_url = gate_url
        #: Who the API key belongs to and its ``created_at``, from its first exchange.
        self._key_party: str | None = None
        self._key_auth_time: datetime | None = None
        self._resolve_lock = asyncio.Lock()
        self._clock = clock or _utcnow
        self._sleep = sleep or asyncio.sleep
        self._access: str | None = None
        self._access_refresh_at = datetime.min.replace(tzinfo=UTC)
        self._access_lock = asyncio.Lock()
        #: The refresh token we last read from or wrote to the profile on disk. Only a disk
        #: copy that differs from *this* was written by someone else and is newer than ours;
        #: a disk copy that merely differs from memory may be one we failed to replace.
        self._disk_token = profile.refresh_token if profile is not None else None
        self._temporal: dict[str, tuple[m.TemporalToken, datetime]] = {}
        self._temporal_lock = asyncio.Lock()
        #: Background api_key refreshers and the client each one keeps fresh.
        self._tasks: dict[asyncio.Task[None], _ApiKeyHolder] = {}
        #: Set by aclose(): no refresher may start after it, as nothing would ever stop it.
        self._closed = False
        self._gate = GateClient(gate_url, token=_AccessTokens(self), transport=transport)

    @classmethod
    def load(
        cls, home: Path | str | None = None, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> Session:
        """The person's login in ``home`` (``$KONTRA_HOME``). :meth:`from_env` also honours
        ``KONTRA_API_KEY``."""
        return cls(Profile.load(home), transport=transport)

    @classmethod
    async def from_api_key(
        cls,
        gate_url: str,
        api_key: str,
        *,
        home: Path | str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Clock | None = None,
        sleep: Sleep | None = None,
    ) -> Session:
        """A program's session: ``api_key`` is exchanged now, so a revoked or mistyped key
        fails here rather than on the first real call, and again whenever the access token
        runs out. Nothing is written anywhere. ``home`` is where the party's key file
        (``key``) is read from, for decrypting deliveries."""
        session = cls(
            api_key=api_key,
            gate_url=gate_url,
            home=home,
            transport=transport,
            clock=clock,
            sleep=sleep,
        )
        try:
            await session.resolve()
        except BaseException:
            await session.aclose()
            raise
        return session

    @classmethod
    def from_env(
        cls, home: Path | str | None = None, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> Session:
        """``$KONTRA_API_KEY`` when it is set, to ``$KONTRA_GATE`` (else
        :data:`DEFAULT_GATE_URL`; a profile's gate is not consulted, so a key never goes to
        a gate it was not made for by accident); otherwise the profile in ``home``. Does no
        I/O: an API key's session asks the gate who it is on first use (:meth:`resolve`)."""
        key = api_key_from_env()
        if key is not None:
            gate_url = os.environ.get(GATE_ENV) or DEFAULT_GATE_URL
            return cls(api_key=key, gate_url=gate_url, home=home, transport=transport)
        return cls(Profile.load(home), home=home, transport=transport)

    @property
    def gate(self) -> GateClient:
        return self._gate

    @property
    def profile(self) -> Profile:
        """Who this session is. An API key's session knows only once :meth:`resolve` ran
        (it does on the first gate call of a :class:`~kontra.hire.Kontra` or a worker)."""
        if self._profile is None:
            raise RuntimeError(
                "this API key's session has not asked the gate who it is yet: "
                "`await session.resolve()` first"
            )
        return self._profile

    @property
    def resolved(self) -> bool:
        """Whether :attr:`profile` is known (always, for a person's login)."""
        return self._profile is not None

    @property
    def uses_api_key(self) -> bool:
        return self._api_key is not None

    async def resolve(self) -> Profile:
        """The profile; for an API key, learned from the gate (the exchange and
        ``GET /v1/me``) the first time and kept in memory only. ``tls`` to Temporal is on
        unless the gate is a dev gate, as a fresh login's would be."""
        if self._profile is not None:
            return self._profile
        async with self._resolve_lock:
            if self._profile is None:
                await self.access_token()
                me = await self._gate.me()
                if me.party != self._key_party or self._key_auth_time is None:
                    raise ProfileError(
                        f"the gate says this API key's session is {me.party!r}, but it "
                        f"exchanged the key for {self._key_party!r}"
                    )
                self._profile = Profile(
                    gate_url=self._gate_url,
                    temporal_address=me.temporal_address,
                    party=me.party,
                    namespace=me.namespace,
                    refresh_token=None,
                    auth_time=self._key_auth_time,
                    public_key=me.public_key,
                    tls=not me.dev,
                    dev=me.dev,
                )
            return self._profile

    def identity(self) -> str:
        """Our secret age identity, from the same directory as the profile (for an API
        key, the ``home`` it was given, else ``$KONTRA_HOME``)."""
        return Profile.identity(self._identity_home or kontra_home())

    async def aclose(self) -> None:
        self._closed = True
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._gate.aclose()

    async def __aenter__(self) -> Session:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # ------------------------------------------------------------------ gate access token

    async def access_token(self) -> str:
        """A gate access token valid for at least another minute (or half its lifetime)."""
        async with self._access_lock:
            if self._access is not None and self._clock() < self._access_refresh_at:
                return self._access
            return await self._refresh()

    def _invalidate(self, token: str) -> None:
        """The gate refused ``token`` (revoked, key rotated, clock skew): refresh next time."""
        if token == self._access:
            self._access = None

    async def _refresh(self) -> str:
        if self._api_key is not None:
            return await self._exchange_api_key(self._api_key)
        async with profile_lock(self._home):
            persist = self._adopt_rotated_token()
            if self.profile.refresh_token is None:
                raise NotLoggedIn(
                    f"the profile of {self.profile.party} holds no session. Run `kontra login`."
                )
            issued = self._clock()
            req = m.TokenRequest(
                grant_type="refresh_token", refresh_token=self.profile.refresh_token
            )
            try:
                tok = await self._gate.token(req)
            except GateError as e:
                if e.code == "invalid_grant":
                    raise NotLoggedIn(
                        f"the session of {self.profile.party} at {self.profile.gate_url} is no "
                        f"longer valid ({e.message}). Run `kontra login`."
                    ) from e
                raise
            if tok.party != self.profile.party:
                raise ProfileError(
                    f"the gate answered for party {tok.party!r} to the refresh token of "
                    f"{self.profile.party!r}; run `kontra login` again"
                )
            if tok.refresh_token is None:
                raise ProfileError(
                    f"the gate at {self.profile.gate_url} answered a refresh without the next "
                    "refresh token; run `kontra login` again"
                )
            changed = (tok.refresh_token, tok.auth_time) != (
                self.profile.refresh_token,
                self.profile.auth_time,
            )
            if changed:
                self.profile.refresh_token = tok.refresh_token
                self.profile.auth_time = tok.auth_time
            if persist and (changed or self.profile.refresh_token != self._disk_token):
                self._persist()
        self._access = tok.access_token
        self._access_refresh_at = _refresh_point(issued, issued + timedelta(seconds=tok.expires_in))
        return tok.access_token

    async def _exchange_api_key(self, api_key: str) -> str:
        """The ``api_key`` grant. Nothing rotates and nothing is written: the next refresh
        exchanges the same key again."""
        issued = self._clock()
        try:
            tok = await self._gate.token(m.TokenRequest(grant_type="api_key", api_key=api_key))
        except GateError as e:
            if e.code == "invalid_grant":
                raise NotLoggedIn(
                    f"the gate at {self._gate_url} does not accept this API key ({e.message}): "
                    "it was revoked, or was made on another gate. Make a new one with "
                    "`kontra keys create`."
                ) from e
            raise
        if self._key_party is None:
            self._key_party, self._key_auth_time = tok.party, tok.auth_time
        elif tok.party != self._key_party:
            raise ProfileError(
                f"the gate answered for party {tok.party!r} to the API key of {self._key_party!r}"
            )
        self._access = tok.access_token
        self._access_refresh_at = _refresh_point(issued, issued + timedelta(seconds=tok.expires_in))
        return tok.access_token

    def _adopt_rotated_token(self) -> bool:
        """If another process sharing our profile rotated the refresh token, use its token
        instead of the (now spent) one in memory. Returns whether the profile on disk is
        still ours to write back to (the user may have logged in as someone else since)."""
        if self._home is None:
            return False
        try:
            disk = Profile.load(self._home)
        except ProfileError:
            return True  # gone or corrupt: ours is the best copy left
        if (disk.party, disk.gate_url) != (self.profile.party, self.profile.gate_url):
            log.warning(
                "the profile in %s now belongs to %s at %s; not writing %s's token back",
                self._home,
                disk.party,
                disk.gate_url,
                self.profile.party,
            )
            return False
        if disk.refresh_token != self._disk_token:
            # Written by someone else since we last read or wrote it (another process
            # rotated it, or a new login): newer than ours. A disk copy that only differs
            # from memory is the one we failed to replace, and is already spent.
            self.profile.refresh_token = disk.refresh_token
            self.profile.auth_time = disk.auth_time
            self._disk_token = disk.refresh_token
        return True

    def _persist(self) -> None:
        """Write the rotated refresh token back to the profile.

        A failed write (disk full, read-only) is logged rather than raised: the new token is
        valid and kept in memory, the next refresh writes again, and raising would only fail
        a call that holds a good access token."""
        if self._home is None:
            return
        try:
            self.profile.save(self._home)
        except OSError as e:
            log.error(
                "could not write the rotated refresh token to %s (%s); this process keeps "
                "working, but another process using this profile will be logged out when "
                "it next refreshes",
                self._home,
                e,
            )
        else:
            self._disk_token = self.profile.refresh_token

    # ------------------------------------------------------------------ Temporal

    async def temporal_token(self, role: str = "worker") -> m.TemporalToken:
        """A Temporal JWT for ``role``, cached until 60 s before it expires. The lifetime is
        the JWT's own ``exp - iat`` measured from when we asked (see :func:`_jwt_lifetime`);
        ``expires_at`` is only the fallback for a token that is not a JWT."""
        async with self._temporal_lock:
            cached = self._temporal.get(role)
            if cached is not None and self._clock() < cached[1]:
                return cached[0]
            issued = self._clock()
            tok = await self._gate.temporal_token(role)
            lifetime = _jwt_lifetime(tok.token)
            expires = issued + lifetime if lifetime is not None else tok.expires_at
            self._temporal[role] = (tok, _refresh_point(issued, expires))
            return tok

    async def connect_temporal(
        self,
        role: str = "worker",
        namespace: str | None = None,
        data_converter: DataConverter | None = None,
        **kwargs: Any,
    ) -> Client:
        """``temporalio.client.Client.connect`` to the profile's frontend as ``role``.

        The JWT is the client's ``api_key`` and is replaced in the background before it
        expires (§10.3: "swapped into the Temporal client at runtime"), until
        :meth:`release` of this client or :meth:`aclose`. Release a client you are done
        with while the session lives on (a worker shutting down on a shared session):
        otherwise its refresher keeps fetching tokens for nobody. ``tls`` defaults to the
        profile's (temporalio would otherwise turn TLS on by itself because an api key is
        set); other kwargs go to ``Client.connect``.
        """
        from temporalio.client import Client

        if "api_key" in kwargs:
            raise TypeError("connect_temporal manages api_key itself")
        if self._closed:
            raise RuntimeError("this Session is closed: it keeps no client's api_key fresh")
        await self.resolve()
        tok = await self.temporal_token(role)
        kwargs.setdefault("tls", self.profile.tls)
        if data_converter is not None:
            kwargs["data_converter"] = data_converter
        client = await Client.connect(
            self.profile.temporal_address,
            namespace=namespace or self.profile.namespace,
            api_key=tok.token,
            **kwargs,
        )
        self.keep_fresh(client, role)  # refused if the session closed while connecting
        return client

    def keep_fresh(self, client: _ApiKeyHolder, role: str = "worker") -> asyncio.Task[None]:
        """Keep ``client.api_key`` a valid ``role`` JWT until :meth:`release` of ``client``
        or :meth:`aclose`. Returns the background task. Raises ``RuntimeError`` once the
        session is closed: that refresher would retry against a closed gate client forever,
        with nobody left to cancel it."""
        if self._closed:
            raise RuntimeError("this Session is closed: it keeps no client's api_key fresh")
        task = asyncio.get_running_loop().create_task(
            self._keep_fresh(client, role), name=f"kontra-temporal-token-{role}"
        )
        self._tasks[task] = client
        task.add_done_callback(self._forget)
        return task

    def _forget(self, task: asyncio.Task[None]) -> None:
        self._tasks.pop(task, None)

    async def release(self, client: object) -> None:
        """Stop keeping ``client``'s api_key fresh and wait until its refresher has stopped.
        Other clients of this session keep theirs. Idempotent; a client this session never
        connected is a no-op."""
        tasks = [task for task, held in self._tasks.items() if held is client]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _keep_fresh(self, client: _ApiKeyHolder, role: str) -> None:
        failures = 0
        while True:
            if failures:
                delay = min(2.0**failures, _MAX_RETRY_DELAY_S)
            else:
                cached = self._temporal.get(role)
                refresh_at = cached[1] if cached is not None else self._clock()
                delay = max((refresh_at - self._clock()).total_seconds(), _MIN_REFRESH_DELAY_S)
            await self._sleep(delay)
            try:
                tok = await self.temporal_token(role)
            except Exception:
                failures += 1
                log.warning(
                    "could not refresh the Temporal %s token (attempt %d); the current one "
                    "stays in use until it expires",
                    role,
                    failures,
                    exc_info=True,
                )
                continue
            failures = 0
            if client.api_key != tok.token:
                client.api_key = tok.token


# --------------------------------------------------------------------------- login


def _check_gate_url(gate_url: str, *, dev: bool) -> None:
    """Refresh tokens are bearer credentials: outside ``kontra dev`` they only travel over
    TLS or to this machine (§10.2)."""
    try:
        url = httpx.URL(gate_url)
    except httpx.InvalidURL as e:
        raise LoginError("insecure_gate", f"{gate_url!r} is not a URL") from e
    if url.scheme not in ("http", "https") or not url.host:
        raise LoginError("insecure_gate", f"{gate_url!r} is not an http(s) URL")
    if url.scheme == "http" and not dev and url.host not in _LOOPBACK:
        raise LoginError(
            "insecure_gate",
            f"refusing to log in to {gate_url} over plain HTTP; use https:// "
            "(plain HTTP is accepted only for localhost or with dev)",
        )


def _print_prompt(start: m.DeviceStart) -> None:
    print(
        f"To log in, open {start.verification_uri} and enter the code {start.user_code}\n"
        f"  (or open {start.verification_uri_complete})",
        file=sys.stderr,
        flush=True,
    )


def _email_prompt(address: str) -> Callable[[m.DeviceStart], None]:
    def show(start: m.DeviceStart) -> None:
        print(
            f"Check {address} for a sign-in link; when the page asks, type {start.user_code}\n"
            "  (the code is only on this screen: the mail never carries it)",
            file=sys.stderr,
            flush=True,
        )

    return show


def _plain_email(email: str) -> str:
    address = email.strip()
    if not m.is_email(address):
        raise LoginError(
            "invalid_email", f"{address} is not a plain email address (like name@example.com)"
        )
    return address


async def _open_browser(url: str) -> None:
    # Only ever hand the browser an http(s) URL from the gate: never file:, javascript: ...
    if httpx.URL(url).scheme not in ("http", "https"):
        log.warning("not opening %r in a browser: not an http(s) URL", url)
        return
    with contextlib.suppress(webbrowser.Error):
        await asyncio.to_thread(webbrowser.open, url)


async def _check_device_page(gate: GateClient, gate_url: str) -> None:
    """Fail at once, with the command that works, when the gate's device page can approve
    nothing: a production gate signs in by email only, and that sign-in starts here (the
    page does not mail links, decisions I19). A gate that predates ``GET /v1/info`` is
    tried as before."""
    try:
        offered = await gate.info()
    except GateError as e:
        if e.status in (404, 405):
            return
        raise
    if not (offered.github_sign_in or offered.dev):
        raise LoginError(
            "email_sign_in",
            f"{gate_url} signs in by email: run `kontra login --email you@example.com` "
            "(the mail brings a link whose page asks for the code this command prints)",
        )


async def login_device(
    gate_url: str,
    handle: str | None = None,
    open_browser: bool = True,
    *,
    email: str | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    prompt: Callable[[m.DeviceStart], None] | None = None,
    sleep: Sleep | None = None,
    clock: Callable[[], float] | None = None,
) -> m.TokenResponse:
    """RFC 8628 device authorization: show a code, wait for the browser, return tokens.

    ``handle`` is the party to claim on first login. ``email``: instead of a browser, the
    gate mails that address a sign-in link (``POST /v1/auth/magic``); its page asks for the
    code, which only this terminal shows, so a link someone else asked for approves nothing
    (decisions I19). Polling follows §3.5: ``authorization_pending`` keeps the interval,
    ``slow_down`` adds 5 s for the rest of the flow, a dropped connection doubles it,
    ``expired_token`` and ``access_denied`` (and the code's own ``expires_in`` running out)
    end it with :class:`LoginError`.
    """
    if handle is not None:
        validate_handle(handle)
    address = _plain_email(email) if email is not None else None
    _check_gate_url(gate_url, dev=False)
    sleep = sleep or asyncio.sleep
    clock = clock or time.monotonic
    expired = "the login code expired; run the login again"
    if address is not None:
        # The same for every address: the gate never says which of these applies.
        expired = (
            f"the login code expired before the link was used; run the login again (no mail? "
            f"check {address} and its spam folder: sign-ups are by invitation, and an address "
            "gets only so many links an hour, also when someone else asks for them)"
        )
    async with GateClient(gate_url, transport=transport) as gate:
        if address is None:
            await _check_device_page(gate, gate_url)
        start = await gate.device_start()
        if address is not None:
            await gate.magic_link(address, start.user_code)
            (prompt or _email_prompt(address))(start)
        else:
            (prompt or _print_prompt)(start)
            if open_browser:
                await _open_browser(start.verification_uri_complete)
        interval = max(start.interval, 1)
        deadline = clock() + start.expires_in
        req = m.TokenRequest(grant_type=DEVICE_GRANT, device_code=start.device_code, handle=handle)
        while True:
            await sleep(interval)
            if clock() >= deadline:
                raise LoginError("expired_token", expired)
            try:
                return await gate.token(req)
            except httpx.TransportError:
                interval = min(interval * 2, _MAX_POLL_INTERVAL_S)
            except GateError as e:
                if e.code == "authorization_pending":
                    continue
                if e.code == "slow_down":
                    asked = (e.detail or {}).get("interval")
                    interval = max(interval + _SLOW_DOWN_S, asked if isinstance(asked, int) else 0)
                elif e.code == "expired_token" and address is not None:
                    raise LoginError(e.code, expired) from e
                elif e.code in ("expired_token", "access_denied"):
                    raise LoginError(e.code, e.message) from e
                else:
                    raise


async def dev_login(
    gate_url: str, handle: str, *, transport: httpx.AsyncBaseTransport | None = None
) -> m.TokenResponse:
    """Log in to a ``kontra dev`` gate as ``handle`` at once (the party is created, with
    play money, if it does not exist). Any other gate answers 404."""
    validate_handle(handle)
    async with GateClient(gate_url, transport=transport) as gate:
        return await gate.dev_login(handle)


async def login(
    gate_url: str,
    *,
    handle: str | None = None,
    dev: bool = False,
    email: str | None = None,
    home: Path | str | None = None,
    open_browser: bool = True,
    replace_key: bool = False,
    tls: bool | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    prompt: Callable[[m.DeviceStart], None] | None = None,
) -> Profile:
    """``kontra login``: get tokens (device flow -- approved in the browser, or through
    the link the gate mails ``email`` -- or :func:`dev_login` with ``dev``), make the
    encryption key if this machine has none, register its public key with the gate, and
    write the profile.

    If the gate already holds a different public key for the party, the login stops with
    ``key_mismatch`` unless ``replace_key``: replacing it makes deliveries to the old key
    unreadable on whichever machine holds it, so copying ``key`` over is usually the fix.
    ``tls`` (to Temporal) defaults to on unless the gate says it is a dev gate.
    """
    _check_gate_url(gate_url, dev=dev)
    if dev:
        if handle is None:
            raise ValueError("a dev login needs a handle")
        if email is not None:
            raise ValueError("a dev login takes no email")
        tokens = await dev_login(gate_url, handle, transport=transport)
    else:
        tokens = await login_device(
            gate_url, handle, open_browser, email=email, transport=transport, prompt=prompt
        )
    if tokens.refresh_token is None:
        raise LoginError("invalid_response", "the gate answered the login without a refresh token")
    _, public = Profile.ensure_identity(home)

    async def bearer() -> str:
        return tokens.access_token

    async with GateClient(gate_url, token=bearer, transport=transport) as gate:
        me = await gate.me()
        if me.party != tokens.party:
            raise LoginError(
                "party_mismatch", f"logged in as {tokens.party!r} but the gate says {me.party!r}"
            )
        if me.public_key != public:
            if me.public_key is not None and not replace_key:
                raise LoginError(
                    "key_mismatch",
                    f"{me.party} already has the public key {me.public_key} and this machine's "
                    f"key is {public}. Copy the key file from the machine that registered it "
                    f"into {Path(home) if home is not None else kontra_home()}, or log in "
                    "again replacing the key.",
                )
            me = await gate.set_key(public)
    profile = Profile(
        gate_url=gate_url,
        temporal_address=me.temporal_address,
        party=me.party,
        namespace=me.namespace,
        refresh_token=tokens.refresh_token,
        auth_time=tokens.auth_time,
        public_key=public,
        tls=(not me.dev) if tls is None else tls,
        dev=me.dev,
    )
    profile.save(home)
    return profile
