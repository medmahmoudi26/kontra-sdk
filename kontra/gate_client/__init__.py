"""Hand-written async client for the gate HTTP API (seam ②, PRD §19.2).

One method per row of :data:`kontra.gate_client.routes.ROUTES`, same name. Twelve-ish
endpoints do not justify a generated client; the route table and the gate's OpenAPI
snapshot keep this honest. Every method looks its route up in the table, so the method,
path and auth of a call cannot drift from the table without ``tests/gate_client`` noticing.

Presigned bucket URLs (artifacts, claim-check blobs) are fetched with :attr:`GateClient.http`,
which shares the transport but never sends the gate credential.

Confirm it's you (decisions I23): a change (a ``fresh`` or ``resolver`` row) that the gate
answers 403 ``confirmation_required`` is handed to the client's ``confirm`` hook, when it has
one; if the hook confirms the session, the same request is sent once more. The gate decides
that 403 before the route does anything, so nothing happened the first time.

Errors: a non-2xx answer raises :class:`GateError` (decoded from
:class:`~kontra.gate_client.models.ErrorBody`, or synthesised with code ``http_error`` when
something other than the gate answered). A 2xx body that does not match the route's
response model raises ``GateError(code="invalid_response")``. Transport failures
(``httpx.TransportError``) propagate unchanged: the gate did not answer, and pretending it
did would hide that.
"""

from __future__ import annotations

import asyncio
import codecs
import json
import re
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar, runtime_checkable
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ValidationError

from kontra import __version__
from kontra.contract.names import OfferingRef
from kontra.gate_client import models as m
from kontra.gate_client.routes import BY_NAME, Route

__all__ = [
    "ConfirmHook",
    "GateClient",
    "GateError",
    "SSEDecoder",
    "ServerSentEvent",
    "TokenProvider",
]

#: Returns a bearer token for the gate (refreshing it if needed), or None for anonymous calls.
#: A provider that also has ``invalidate(token: str) -> None`` is told when the gate answers
#: 401 to a token, before it is asked for another one (so it can refresh early).
TokenProvider = Callable[[], Awaitable[str | None]]
#: Asked when a change answers 403 ``confirmation_required``: ``(route name, what the gate
#: needs)`` -> whether the session is confirmed now. True sends the same request once more;
#: False leaves the call failed as the gate answered; an exception propagates as it is.
ConfirmHook = Callable[[str, m.ConfirmationRequired], Awaitable[bool]]

_ModelT = TypeVar("_ModelT", bound=BaseModel)

#: Route auth kinds that carry ``Authorization: Bearer``. ``none`` and ``dev`` never do: the
#: token route itself is ``none``, so a refreshing provider can never recurse into itself.
_AUTHENTICATED = frozenset(
    {"party", "human", "fresh", "worker", "party|worker", "resolver", "resolver_human"}
)
#: The route auth kinds whose 403 ``confirmation_required`` a ``confirm`` hook may answer:
#: the changes. Never a read (``human``), never another code.
_CONFIRMABLE = frozenset({"fresh", "resolver"})
#: Statuses a progress stream retries (a gate restarting behind its proxy); others are final.
_STREAM_RETRY_STATUS = frozenset({502, 503, 504})
_ERROR_TEXT_MAX = 500

# Indirection so tests can observe reconnect delays without patching asyncio itself.
_sleep = asyncio.sleep


@runtime_checkable
class _Invalidates(Protocol):
    def invalidate(self, token: str) -> None: ...


class GateError(Exception):
    """A non-2xx answer from the gate, decoded from :class:`~kontra.gate_client.models.ErrorBody`."""

    def __init__(
        self, status: int, code: str, message: str, detail: dict[str, Any] | None = None
    ) -> None:
        self.status, self.code, self.message, self.detail = status, code, message, detail
        super().__init__(f"{status} {code}: {message}")


def _error_from(resp: httpx.Response) -> GateError:
    """Decode an error answer. The body must already be read."""
    body = resp.content
    try:
        err = m.ErrorBody.model_validate_json(body)
    except ValidationError:
        pass
    else:
        return GateError(resp.status_code, err.code, err.message, err.detail)
    # Not the gate's error funnel: a proxy, a load balancer, or a wrong base URL.
    message = resp.reason_phrase or "error"
    detail: dict[str, Any] | None = None
    try:
        parsed: object = json.loads(body)
    except ValueError:
        text = body.decode("utf-8", "replace").strip()
        if text:
            message = text[:_ERROR_TEXT_MAX]
    else:
        if isinstance(parsed, dict):
            detail = {str(k): v for k, v in parsed.items()}
            inner = detail.get("detail", detail.get("message"))
            if isinstance(inner, str):
                message = inner[:_ERROR_TEXT_MAX]
    return GateError(resp.status_code, "http_error", message, detail)


def _segment(value: str) -> str:
    """One URL path segment. ``/``, ``?`` and ``#`` are escaped by ``quote``; a bare ``.`` or
    ``..`` is escaped too, because httpx removes dot segments and ``accept("..")`` would
    otherwise reach a different route."""
    if not value:
        raise ValueError("path parameters must not be empty")
    if value in (".", ".."):
        return value.replace(".", "%2E")
    return quote(value, safe="")


def _ref_path(ref: str) -> dict[str, str]:
    """``acme/enrich[@1[.2[.0]]]`` -> the catalog path parameters. A missing version is
    ``latest``; partial versions are resolved by the gate."""
    r = OfferingRef.parse(ref)
    nums = [n for n in (r.major, r.minor, r.patch) if n is not None]
    version = ".".join(str(n) for n in nums) if nums else "latest"
    return {"party": r.party, "name": r.name, "version": version}


# --------------------------------------------------------------------------- server-sent events


@dataclass(frozen=True, slots=True)
class ServerSentEvent:
    event: str
    data: str
    id: str | None = None


_LINE_END = re.compile(r"\r\n|\r|\n")
#: Upper bound, in characters, on one unfinished line plus the data of one event. Progress
#: events are small; without a bound a server or proxy that never ends a line or an event
#: would grow the buffer until the buyer's process runs out of memory.
_SSE_MAX = 4 * 1024 * 1024


class SSEDecoder:
    """Incremental ``text/event-stream`` decoder (WHATWG HTML, "event stream interpretation").

    Written by hand rather than with ``httpx``'s line iterator, which also splits on
    U+2028, form feeds and other Unicode line breaks that may legally appear inside a JSON
    string in a ``data:`` line. Only CRLF, CR and LF end a line here, including a CRLF
    split across two chunks. An event still open when the stream ends is discarded, as
    the spec requires.
    """

    def __init__(self, *, max_size: int | None = None) -> None:
        self._utf8 = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._buf = ""
        self._first = True
        self._cr_pending = False
        self._data: list[str] = []
        self._data_size = 0
        self._event = ""
        self._last_id: str | None = None
        self._max = max_size if max_size is not None else _SSE_MAX

    def feed(self, chunk: bytes) -> list[ServerSentEvent]:
        text = self._utf8.decode(chunk)
        if not text:
            return []
        if self._first:
            self._first = False
            if text.startswith("﻿"):
                text = text[1:]
        if self._cr_pending:
            self._cr_pending = False
            if text.startswith("\n"):
                text = text[1:]
        buf = self._buf + text
        out: list[ServerSentEvent] = []
        start = 0
        for match in _LINE_END.finditer(buf):
            ev = self._line(buf[start : match.start()])
            if ev is not None:
                out.append(ev)
            start = match.end()
            if match.group() == "\r" and start == len(buf):
                self._cr_pending = True  # maybe the first half of a CRLF
        self._buf = buf[start:]
        if len(self._buf) + self._data_size > self._max:
            raise ValueError(f"a server-sent event is larger than {self._max} characters")
        return out

    def _line(self, line: str) -> ServerSentEvent | None:
        if not line:
            if not self._data:
                self._event = ""
                return None
            ev = ServerSentEvent(self._event or "message", "\n".join(self._data), self._last_id)
            self._data, self._data_size, self._event = [], 0, ""
            return ev
        if line.startswith(":"):
            return None  # comment / keepalive
        field, colon, value = line.partition(":")
        if colon and value.startswith(" "):
            value = value[1:]
        if field == "data":
            self._data.append(value)
            self._data_size += len(value) + 1
            if self._data_size > self._max:
                raise ValueError(f"a server-sent event is larger than {self._max} characters")
        elif field == "event":
            self._event = value
        elif field == "id" and "\0" not in value:
            self._last_id = value
        return None  # "retry" and unknown fields are ignored


async def _sse_events(chunks: AsyncIterable[bytes], status: int) -> AsyncIterator[ServerSentEvent]:
    decoder = SSEDecoder()
    async for chunk in chunks:
        try:
            events = decoder.feed(chunk)
        except ValueError as e:  # oversized: the server misbehaves; reconnecting won't help
            raise GateError(status, "invalid_response", str(e)) from None
        for ev in events:
            yield ev


# --------------------------------------------------------------------------- the client


class GateClient:
    """``GateClient(base_url, token=...)``. ``transport`` lets tests run the client against
    an in-process gate (``kontra.testing.fake_gate``).

    The bearer token is asked of ``token`` on every authenticated call (the provider owns
    caching and refresh). On a 401 the provider is asked once more -- after
    ``invalidate(rejected)`` if it has that method -- and the call is retried once if it
    returned a different token.

    ``confirm`` (:data:`ConfirmHook`) is asked when a change answers 403
    ``confirmation_required``; if it returns True the call is sent once more, and that
    answer stands, whatever it is.
    """

    #: Delays between consecutive failed reconnects of a progress stream; the stream raises
    #: once they are used up. A successful connect resets the count.
    reconnect_delays: tuple[float, ...] = (0.5, 1.0, 2.0, 4.0, 8.0, 15.0)

    def __init__(
        self,
        base_url: str,
        *,
        token: TokenProvider | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 30.0,
        user_agent: str | None = None,
        confirm: ConfirmHook | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._tokens = token
        self._confirm = confirm
        self._transport = transport
        self._timeout = timeout
        self._user_agent = user_agent or f"kontra-sdk/{__version__}"
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            transport=transport,
            timeout=timeout,
            headers={"User-Agent": self._user_agent, "Accept": "application/json"},
        )
        self._http: httpx.AsyncClient | None = None

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def http(self) -> httpx.AsyncClient:
        """Unauthenticated client on the same transport, for presigned URLs."""
        if self._http is None:
            self._http = httpx.AsyncClient(
                transport=self._transport,
                timeout=self._timeout,
                headers={"User-Agent": self._user_agent},
            )
        return self._http

    async def aclose(self) -> None:
        await self._client.aclose()
        if self._http is not None:
            await self._http.aclose()

    async def __aenter__(self) -> GateClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # ------------------------------------------------------------------ plumbing

    async def _bearer(self, route: Route) -> str | None:
        if route.auth not in _AUTHENTICATED or self._tokens is None:
            return None
        return await self._tokens()

    def _build(
        self,
        route: Route,
        path: Mapping[str, str] | None,
        params: Mapping[str, str | int | None] | None,
        body: BaseModel | None,
        token: str | None,
        accept: str | None,
    ) -> httpx.Request:
        url = route.path.format_map({k: _segment(v) for k, v in (path or {}).items()})
        query = {k: v for k, v in (params or {}).items() if v is not None}
        headers: dict[str, str] = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if accept is not None:
            headers["Accept"] = accept
        return self._client.build_request(
            route.method,
            url,
            params=query or None,
            json=body.model_dump(mode="json") if body is not None else None,
            headers=headers,
        )

    async def _send(
        self,
        name: str,
        *,
        path: Mapping[str, str] | None = None,
        params: Mapping[str, str | int | None] | None = None,
        body: BaseModel | None = None,
        stream: bool = False,
        accept: str | None = None,
    ) -> httpx.Response:
        """Send one call, retrying once on a 401 with a fresh token. The response is
        returned whatever its status; with ``stream`` its body is unread."""
        route = BY_NAME[name]
        token = await self._bearer(route)
        resp = await self._client.send(
            self._build(route, path, params, body, token, accept), stream=stream
        )
        tokens = self._tokens
        if resp.status_code == 401 and token is not None and tokens is not None:
            hook: object = tokens
            if isinstance(hook, _Invalidates):
                hook.invalidate(token)
            try:
                fresh = await tokens()
            except BaseException:
                await resp.aclose()  # a streamed 401 would otherwise hold its connection
                raise
            if fresh is not None and fresh != token:
                await resp.aclose()
                resp = await self._client.send(
                    self._build(route, path, params, body, fresh, accept), stream=stream
                )
        return resp

    async def _call(
        self,
        name: str,
        *,
        path: Mapping[str, str] | None = None,
        params: Mapping[str, str | int | None] | None = None,
        body: BaseModel | None = None,
    ) -> httpx.Response:
        resp = await self._send(name, path=path, params=params, body=body)
        if resp.is_success:
            return resp
        err = _error_from(resp)
        need = self._confirmable(BY_NAME[name], err)
        if need is None or self._confirm is None or not await self._confirm(name, need):
            raise err
        # The same request once more: the 403 came before the route did anything.
        resp = await self._send(name, path=path, params=params, body=body)
        if not resp.is_success:
            raise _error_from(resp)
        return resp

    def _confirmable(self, route: Route, err: GateError) -> m.ConfirmationRequired | None:
        """What the gate needs to let ``route`` through, when ``err`` is a change's 403
        ``confirmation_required`` with a detail that says it; None otherwise."""
        if (
            self._confirm is None
            or route.auth not in _CONFIRMABLE
            or (err.status, err.code) != (403, m.CONFIRMATION_REQUIRED)
        ):
            return None
        try:
            return m.ConfirmationRequired.model_validate(err.detail)
        except ValidationError:
            return None

    async def _model(
        self,
        name: str,
        model: type[_ModelT],
        *,
        path: Mapping[str, str] | None = None,
        params: Mapping[str, str | int | None] | None = None,
        body: BaseModel | None = None,
    ) -> _ModelT:
        if BY_NAME[name].response is not model:
            raise TypeError(f"route {name} does not answer {model.__name__}")  # a bug here
        resp = await self._call(name, path=path, params=params, body=body)
        try:
            return model.model_validate_json(resp.content)
        except ValidationError as e:
            raise GateError(
                resp.status_code,
                "invalid_response",
                f"the gate's answer to {name} is not a {model.__name__}: {e}",
            ) from e

    async def _object(self, name: str, *, path: Mapping[str, str] | None = None) -> dict[str, Any]:
        resp = await self._call(name, path=path)
        try:
            parsed: object = resp.json()
        except ValueError:
            parsed = None
        if not isinstance(parsed, dict):
            raise GateError(
                resp.status_code,
                "invalid_response",
                f"the gate's answer to {name} is not a JSON object",
            )
        return {str(k): v for k, v in parsed.items()}

    async def _none(
        self,
        name: str,
        *,
        path: Mapping[str, str] | None = None,
        body: BaseModel | None = None,
    ) -> None:
        await self._call(name, path=path, body=body)

    # auth & identity
    async def jwks(self) -> dict[str, Any]:
        return await self._object("jwks")

    async def info(self) -> m.GateInfo:
        """What this gate offers before sign-in: dev login, email sign-in, GitHub."""
        return await self._model("info", m.GateInfo)

    async def revocations(self) -> m.Revocations:
        """The sessions revoked in the gate's window, as sha256 digests of their ids."""
        return await self._model("revocations", m.Revocations)

    async def device_start(self) -> m.DeviceStart:
        return await self._model("device_start", m.DeviceStart)

    async def token(self, req: m.TokenRequest) -> m.TokenResponse:
        return await self._model("token", m.TokenResponse, body=req)

    async def magic_link(self, email: str, device_user_code: str) -> m.MagicLinkSent:
        """Ask the gate to email ``email`` a sign-in link for the device authorization
        ``device_user_code``. The answer is the same whether or not a mail goes out."""
        return await self._model(
            "magic_link",
            m.MagicLinkSent,
            body=m.MagicLinkRequest(email=email, device_user_code=device_user_code),
        )

    async def dev_login(self, handle: str) -> m.TokenResponse:
        return await self._model("dev_login", m.TokenResponse, body=m.DevLogin(handle=handle))

    async def revoke(self) -> None:
        await self._none("revoke")

    async def temporal_token(self, role: str = "worker") -> m.TemporalToken:
        req = m.TemporalTokenRequest.model_validate({"role": role})
        return await self._model("temporal_token", m.TemporalToken, body=req)

    async def me(self) -> m.Me:
        return await self._model("me", m.Me)

    async def add_recipient(self, label: str, public_key: str) -> m.Me:
        """Add one age recipient to this party (a change: needs a person's confirmed
        session). Adding never replaces: a second machine is one more recipient, not a new
        party key."""
        return await self._model(
            "add_recipient", m.Me, body=m.AddKey(label=label, public_key=public_key)
        )

    async def revoke_recipient(self, public_key: str) -> None:
        """Stop encrypting to ``public_key``. Deliveries already made to it stay readable
        where their ciphertext and that key are; everything after it is not."""
        await self._none("revoke_recipient", path={"public_key": public_key})

    async def party(self, party: str) -> m.PartyView:
        return await self._model("party", m.PartyView, path={"party": party})

    # API keys
    async def create_api_key(self, label: str) -> m.ApiKeyCreated:
        """A new API key for programs (a change: needs a person's confirmed session). The
        key is in the answer this once; the gate keeps only its hash."""
        return await self._model(
            "create_api_key", m.ApiKeyCreated, body=m.ApiKeyCreate(label=label)
        )

    async def api_keys(self) -> m.ApiKeyList:
        return await self._model("api_keys", m.ApiKeyList)

    async def revoke_api_key(self, key_id: str) -> None:
        """Revoke a key: its exchanges fail at once, and its access and worker tokens stop
        working on their next request."""
        await self._none("revoke_api_key", path={"key_id": key_id})

    async def sessions(self) -> m.SessionList:
        """The party's sign-ins (people's logins, not keys), newest first; ``current`` is
        this one. Needs a person's session (never a key's), not a confirmation."""
        return await self._model("sessions", m.SessionList)

    async def end_session(self, session_id: str) -> None:
        """End one of the party's sign-ins: its refresh tokens stop at once, its access and
        worker tokens on their next request. A change: needs a confirmed session."""
        await self._none("end_session", path={"session_id": session_id})

    async def confirm_start(self) -> m.ConfirmStart:
        """Ask to confirm it's you, for this session (decisions I23): a ``pending``
        confirmation whose ``user_code`` only this screen shows (the gate mails the account's
        sign-in address a link whose page asks for it), or, on a dev gate's development
        sign-in, one ``confirmed`` already. 409 ``confirm_unavailable``: only a new sign-in
        confirms this session."""
        return await self._model("confirm_start", m.ConfirmStart)

    async def confirm_status(self, confirm_id: str) -> m.ConfirmStatus:
        """One poll of a confirmation this session started; another session's is 404."""
        return await self._model("confirm_status", m.ConfirmStatus, path={"confirm_id": confirm_id})

    # catalog
    async def publish(self, document: dict[str, Any]) -> m.CatalogEntry:
        return await self._model(
            "publish", m.CatalogEntry, body=m.PublishRequest(document=document)
        )

    async def search(
        self, q: str = "", *, cursor: str | None = None, limit: int = 20
    ) -> m.CatalogList:
        return await self._model(
            "search", m.CatalogList, params={"q": q or None, "cursor": cursor, "limit": limit}
        )

    async def get_offering(self, ref: str) -> m.CatalogEntry:
        """``ref`` is ``acme/enrich``, ``acme/enrich@1``, ``@1.2`` or exact; the gate resolves
        partial versions to the latest match (path version ``latest``, ``1``, ``1.2``, ``1.2.0``)
        among the visible versions. An exact version is answered retired too
        (``entry.retired``): the jobs hired under it name it (decisions I22)."""
        return await self._model("get_offering", m.CatalogEntry, path=_ref_path(ref))

    async def card(self, ref: str) -> dict[str, Any]:
        return await self._object("card", path=_ref_path(ref))

    async def by_endpoint(self, endpoint: str) -> m.CatalogEntry:
        """The offering behind an endpoint name. Endpoint names are opaque: this lookup is
        the only way from one to the other."""
        return await self._model("by_endpoint", m.CatalogEntry, path={"endpoint": endpoint})

    async def workers(self, ref: str) -> m.WorkersView:
        """Whether the workers keep up with one of the caller's own offering versions:
        Temporal's task-queue stats and pollers, and a verdict. ``ref`` resolves as for
        :meth:`get_offering`, among the caller's versions."""
        return await self._model("workers", m.WorkersView, path=_ref_path(ref))

    # quotes & jobs
    async def quote(self, offering: str, payload: m.WirePayload) -> m.QuoteView:
        OfferingRef.parse(offering)  # refuse a malformed reference before sending the input
        return await self._model(
            "quote", m.QuoteView, body=m.QuoteRequest(offering=offering, input=payload)
        )

    async def hire(
        self, quote_id: str, *, buyer_key: str, max_price_minor: int | None = None
    ) -> m.HireView:
        """``buyer_key`` is the job's own age recipient, made by the caller at hire: the
        gate signs it into the hold token and never consults the party's own keys for the
        buyer side (decisions I7)."""
        return await self._model(
            "hire",
            m.HireView,
            body=m.HireRequest(
                quote_id=quote_id, buyer_key=buyer_key, max_price_minor=max_price_minor
            ),
        )

    async def jobs(
        self,
        *,
        as_: str = "buyer",
        state: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> m.JobList:
        return await self._model(
            "jobs",
            m.JobList,
            params={"as": as_, "state": state, "cursor": cursor, "limit": limit},
        )

    async def job(self, job_id: str) -> m.JobView:
        return await self._model("job", m.JobView, path={"job_id": job_id})

    async def seller_job(self, job_id: str) -> m.SellerJobView:
        return await self._model("seller_job", m.SellerJobView, path={"job_id": job_id})

    async def accept(self, job_id: str) -> m.JobView:
        return await self._model(
            "accept", m.JobView, path={"job_id": job_id}, body=m.AcceptRequest()
        )

    async def reject(self, job_id: str, reason: str) -> m.JobView:
        return await self._model(
            "reject", m.JobView, path={"job_id": job_id}, body=m.RejectRequest(reason=reason)
        )

    async def cancel(self, job_id: str) -> m.JobView:
        return await self._model("cancel", m.JobView, path={"job_id": job_id})

    async def start(self, job_id: str, payload: m.WirePayload) -> m.JobView:
        return await self._model(
            "start", m.JobView, path={"job_id": job_id}, body=m.StartRequest(input=payload)
        )

    async def result(self, job_id: str) -> m.ResultView:
        return await self._model("result", m.ResultView, path={"job_id": job_id})

    def progress(self, job_id: str, *, after: int = 0) -> AsyncIterator[m.ProgressEvent]:
        """Server-sent events until the job is closed; reconnects from the last ``seq``."""
        return self._progress(job_id, after)

    async def _progress(self, job_id: str, after: int) -> AsyncIterator[m.ProgressEvent]:
        """Follow the stream, resuming with ``?after=<last seq>`` after a dropped connection
        or a 502/503/504, with bounded backoff. A clean end of stream is the gate saying the
        job is closed and drained, so it ends the iteration. Events at or below the last
        seq (a replay overlapping what we already yielded) are skipped."""
        _segment(job_id)  # fail before connecting, not on the first iteration's request
        last = after
        failures = 0
        while True:
            try:
                resp = await self._send(
                    "progress",
                    path={"job_id": job_id},
                    params={"after": last},
                    stream=True,
                    accept="text/event-stream",
                )
                try:
                    if not resp.is_success:
                        await resp.aread()
                        err = _error_from(resp)
                        if resp.status_code not in _STREAM_RETRY_STATUS:
                            raise err
                        failures += 1
                        if failures > len(self.reconnect_delays):
                            raise err
                        await _sleep(self.reconnect_delays[failures - 1])
                        continue
                    failures = 0
                    async for sse in _sse_events(resp.aiter_bytes(), resp.status_code):
                        if sse.event not in ("message", "progress") or not sse.data:
                            continue
                        try:
                            event = m.ProgressEvent.model_validate_json(sse.data)
                        except ValidationError as e:
                            raise GateError(
                                resp.status_code,
                                "invalid_response",
                                f"a progress event is not a ProgressEvent: {e}",
                            ) from e
                        if event.seq <= last:
                            continue
                        last = event.seq
                        yield event
                finally:
                    await resp.aclose()
                return
            except httpx.TransportError:
                failures += 1
                if failures > len(self.reconnect_delays):
                    raise
                await _sleep(self.reconnect_delays[failures - 1])

    async def post_progress(self, job_id: str, event: m.ProgressPost) -> None:
        await self._none("post_progress", path={"job_id": job_id}, body=event)

    # artifacts & claim-check blobs
    async def create_artifact(self, req: m.ArtifactCreate) -> m.ArtifactUpload:
        return await self._model("create_artifact", m.ArtifactUpload, body=req)

    async def complete_artifact(self, key: str, req: m.ArtifactComplete) -> None:
        await self._none("complete_artifact", path={"key": key}, body=req)

    async def download_artifact(self, key: str) -> m.DownloadUrl:
        return await self._model("download_artifact", m.DownloadUrl, path={"key": key})

    async def create_blob(self, req: m.BlobCreate) -> m.BlobUrl:
        return await self._model("create_blob", m.BlobUrl, body=req)

    async def get_blob(self, sha256: str) -> m.BlobUrl:
        return await self._model("get_blob", m.BlobUrl, path={"sha256": sha256})

    # money
    async def balance(self, currency: str = "USD") -> m.Balance:
        return await self._model("balance", m.Balance, params={"currency": currency})

    async def topup(self, amount_minor: int, currency: str = "USD") -> m.TopupResult:
        return await self._model(
            "topup", m.TopupResult, body=m.Topup(amount_minor=amount_minor, currency=currency)
        )

    # disputes
    async def disputes(self, *, cursor: str | None = None, limit: int = 50) -> m.JobList:
        """The resolver's queue, newest first; pass ``next_cursor`` back to page through it."""
        return await self._model("disputes", m.JobList, params={"cursor": cursor, "limit": limit})

    async def resolve(self, job_id: str, req: m.ResolveRequest) -> m.JobView:
        return await self._model("resolve", m.JobView, path={"job_id": job_id}, body=req)

    async def health(self) -> dict[str, Any]:
        return await self._object("health")
