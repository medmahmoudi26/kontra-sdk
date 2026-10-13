"""An in-process gate (seam ②) that behaves like the real one, for tests with no containers.

:class:`FakeGate` serves every row of :data:`kontra.gate_client.routes.ROUTES` from memory:
identity (dev login, the device flow, rotating refresh tokens, ES256 tokens and a JWKS),
parties with Temporal namespaces, the catalog (contract validation, opaque endpoint names,
the endpoint created through the control plane), the quote relay, hire with a signed hold
token, the escrow lifecycle, progress over SSE, artifacts and claim-check blobs behind
signed URLs, balances and disputes. Its rules are the platform's: the job rules are
:mod:`kontra.escrow` (shared with the real gate), money is a
:func:`~kontra.testing.fake_ledger`, and Temporal is reached through
:class:`kontra._control.Control` -- usually on a dev server
(:mod:`kontra.testing.devserver`) -- exactly as the real gate reaches its internal frontend.
With no control plane (``control=None``) it is a gate whose sellers are all offline:
publishing records the endpoint name, quotes and starts answer 503, and open jobs refund
at their deadline.

Two ways to run it:

* ``FakeGate.app`` is the ASGI app, for ``httpx.ASGITransport``. Use ``async with gate:``
  to run the sweeper on the current loop. A progress stream only ends when its job does
  (``ASGITransport`` buffers whole responses).
* ``await gate.serve()`` runs uvicorn on ``127.0.0.1:0`` in a background thread and returns
  the base URL, so SSE streams for real; ``await gate.stop()`` ends it. The gate's state
  lives on that thread's loop: inspect it with ``await gate.run(fn)``.

Time is the gate's own: ``gate.advance(...)`` moves the clock every rule reads (review
windows, deadlines, token expiry, quote TTLs, signed URLs). Timestamps observed from
Temporal are shifted by the same offset, as if the whole world had moved. Tokens are
verified against this clock, not the machine's.

Differences from the platform gate, all deliberate: state is in memory; artifacts and blobs
are stored in memory behind the gate's own HMAC-signed ``/_blob/`` URLs (not S3); top-ups
are always instant play money (production would answer with a checkout URL); the device
flow can approve itself.
"""

# No ``from __future__ import annotations`` here: FastAPI reads the route functions'
# annotations (bodies, auth dependencies) at runtime.

import asyncio
import base64
import binascii
import contextlib
import hashlib
import hmac
import json
import logging
import math
import re
import secrets
import socket
import sys
import threading
import time
import types
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any, Literal, TypeVar

import jwt
import uvicorn
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from google.protobuf.message import DecodeError
from jwt.algorithms import ECAlgorithm
from pydantic import ValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from temporalio.api.common.v1 import Payload

from kontra import escrow
from kontra._control import (
    BuyerCall,
    BuyerLookupError,
    EndpointConflict,
    HistorySummary,
    NexusSyncKind,
    Occupation,
    TemporalControl,
    TemporalError,
    hold_digest,
    input_size,
)
from kontra.codec import InvalidPayload
from kontra.contract import ContractError, ContractInfo, KontraQuote, agent_card, validate_document
from kontra.contract.facts import check_public_key
from kontra.contract.names import (
    NameError_,
    OfferingId,
    OfferingRef,
    is_endpoint_name,
    is_handle,
    is_job_id,
    job_workflow_id,
    new_endpoint_name,
    new_job_id,
    validate_handle,
)
from kontra.contract.quote import MAX_ETA_SECONDS
from kontra.escrow import (
    Effect,
    EffectKind,
    EscrowError,
    JobRecord,
    Observation,
    ObservedStatus,
    Provenance,
)
from kontra.gate_client import models as m
from kontra.gate_client.routes import BY_NAME
from kontra.ledger import (
    CURRENCIES,
    CurrencyMismatch,
    HandleState,
    HoldState,
    InsufficientFunds,
    InvalidAmount,
    InvalidSplit,
    Ledger,
    LedgerError,
    Money,
    Op,
    Split,
    UnknownHold,
)
from kontra.testing.ledger import InMemoryLedger

__all__ = [
    "AUD_API",
    "AUD_HOLD",
    "AUD_TEMPORAL",
    "HOLD_HEADER",
    "FakeGate",
    "GateHTTPError",
    "Principal",
]

log = logging.getLogger("kontra.testing.fake_gate")

AUD_API = "kontra:api"
AUD_TEMPORAL = "kontra:temporal"
AUD_HOLD = "kontra:hold"
#: The Nexus header carrying the hold token (D10).
HOLD_HEADER = "kontra-hold"
QUOTE_OPERATION = "quote"
EXECUTE_OPERATION = "execute"
DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"
#: RFC 8628 §6.1: no vowels, no look-alikes.
USER_CODE_ALPHABET = "BCDFGHJKLMNPQRSTVWXZ"
#: A relayed Nexus payload (quote or start input): Temporal's own default blob limit.
MAX_PAYLOAD_BYTES = 2 * 1024 * 1024
MAX_PROGRESS_BYTES = 64 * 1024
#: Heartbeat ciphertext kept per job, in total (later heartbeats are dropped).
HEARTBEAT_BUDGET_BYTES = 64 * 1024 * 1024
#: Every age v1 file starts with this line: progress the gate keeps must be ciphertext.
AGE_MAGIC = b"age-encryption.org/v1\n"
MAX_BLOB_BYTES = 64 * 1024 * 1024
NOTE_MAX = 2000
WORKER_TTL = timedelta(minutes=15)
LIST_LIMIT_MAX = 200
SEARCH_LIMIT_MAX = 100
SEARCH_Q_MAX = 200

_T = TypeVar("_T")
_OPEN = (m.JobState.SUBMITTED, m.JobState.WORKING)
_STOPS = frozenset(
    {EffectKind.CANCEL_WORKFLOW, EffectKind.TERMINATE_WORKFLOW, EffectKind.TERMINATE_RUN}
)
_CLOSED_ESCROW = frozenset({m.Escrow.RELEASED, m.Escrow.REFUNDED, m.Escrow.RESOLVED})
_ESCROW_HOLD = {
    m.Escrow.HELD: HoldState.HELD,
    m.Escrow.RELEASED: HoldState.RELEASED,
    m.Escrow.REFUNDED: HoldState.REFUNDED,
    m.Escrow.DISPUTED: HoldState.DISPUTED,
    m.Escrow.RESOLVED: HoldState.RESOLVED,
}
_NO_HOLD_OK = frozenset({HoldState.HELD, HoldState.REFUNDED})
_SECRET_LEN = 43  # secrets.token_urlsafe(32)
_PATH_VERSION_MAX = 29
_JSON_PLAIN = b"json/plain"
_ACTIVITY = re.compile(r"[^\x00-\x1f\x7f]{1,255}")
_SHA256 = re.compile(r"[0-9a-f]{64}", re.ASCII)
#: Printable ASCII, no control characters (it becomes a header value downstream).
_CONTENT_TYPE = re.compile(r"[\x21-\x7e][\x20-\x7e]{0,254}", re.ASCII)


# --------------------------------------------------------------------------- errors


class GateHTTPError(Exception):
    """Raised anywhere in the fake gate to answer ``status`` with an
    :class:`~kontra.gate_client.models.ErrorBody` (``code`` is stable, as on the real gate)."""

    def __init__(
        self,
        status: int,
        code: str,
        message: str,
        detail: dict[str, Any] | None = None,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.message, self.detail = status, code, message, detail
        self.headers = dict(headers or {})


def _error(
    status: int,
    code: str,
    message: str,
    detail: dict[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    body = m.ErrorBody(code=code, message=message, detail=detail)
    return JSONResponse(
        body.model_dump(mode="json"), status_code=status, headers=dict(headers or {})
    )


def _unauthorized(code: str, message: str) -> GateHTTPError:
    return GateHTTPError(401, code, message, headers={"WWW-Authenticate": f'Bearer error="{code}"'})


def _amount_text(amount_minor: int, currency: str) -> str:
    """``(1240, "USD")`` -> ``"12.40"``."""
    return f"{Money(amount_minor, currency).to_decimal():.{CURRENCIES[currency]}f}"


def _b64url_json(value: Any) -> str:
    return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")


def _unb64url_json(cursor: str) -> Any:
    bad = GateHTTPError(422, "invalid_cursor", "cursor is not one this gate handed out")
    if len(cursor) > 200:
        raise bad
    try:
        return json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
    except (ValueError, binascii.Error):
        raise bad from None


# --------------------------------------------------------------------------- keys


class _KeyRing:
    """One ES256 key: signs every token the gate issues, published in the JWKS. Tokens are
    verified against the gate's clock (PyJWT's own expiry check reads the machine's)."""

    def __init__(self) -> None:
        self._key = ec.generate_private_key(ec.SECP256R1())
        self.kid = "fake-" + secrets.token_hex(8)
        jwk: dict[str, Any] = json.loads(ECAlgorithm.to_jwk(self._key.public_key()))
        jwk.update(kid=self.kid, use="sig", alg="ES256")
        self._jwk = jwk

    def jwks(self) -> dict[str, Any]:
        return {"keys": [dict(self._jwk)]}

    def sign(self, claims: Mapping[str, Any]) -> str:
        return jwt.encode(dict(claims), self._key, algorithm="ES256", headers={"kid": self.kid})

    def verify(self, token: str, *, audience: str, issuer: str, now: datetime) -> dict[str, Any]:
        try:
            header = jwt.get_unverified_header(token)
            if header.get("kid") != self.kid:
                raise _unauthorized("invalid_token", "unknown signing key")
            claims: dict[str, Any] = jwt.decode(
                token,
                self._key.public_key(),
                algorithms=["ES256"],
                audience=audience,
                issuer=issuer,
                options={
                    "verify_exp": False,
                    "verify_iat": False,
                    "verify_nbf": False,
                    "require": ["exp", "iat", "sub"],
                },
            )
        except jwt.PyJWTError as e:
            raise _unauthorized("invalid_token", f"token rejected: {e}") from None
        exp = claims.get("exp")
        if not isinstance(exp, int | float) or isinstance(exp, bool):
            raise _unauthorized("invalid_token", "token has no expiry")
        if now.timestamp() >= exp:
            raise _unauthorized("token_expired", "token has expired")
        return claims


def _unverified_audience(token: str) -> Any:
    try:
        claims: dict[str, Any] = jwt.decode(token, options={"verify_signature": False})
    except jwt.PyJWTError:
        return None
    return claims.get("aud")


# --------------------------------------------------------------------------- records


@dataclass(frozen=True, slots=True)
class Principal:
    """Who is calling. ``party`` is also their Temporal namespace."""

    party: str
    session_id: str
    auth_time: datetime
    kind: Literal["api", "worker"]


@dataclass
class _Party:
    handle: str
    namespace: str
    created_at: datetime
    public_key: str | None = None


@dataclass
class _Session:
    id: str
    party: str
    auth_time: datetime
    via: str
    revoked: bool = False


@dataclass
class _Refresh:
    session_id: str
    expires_at: datetime
    used: bool = False


@dataclass
class _Device:
    user_code: str
    expires_at: datetime
    interval: int
    status: Literal["pending", "approved", "denied", "consumed"] = "pending"
    requested_handle: str | None = None
    party: str | None = None
    approved_at: datetime | None = None
    last_polled: datetime | None = None


@dataclass
class _Offering:
    info: ContractInfo
    endpoint: str
    endpoint_ready: bool
    card: dict[str, Any]
    published_at: datetime
    retired: bool = False

    @property
    def id(self) -> OfferingId:
        return self.info.offering

    @property
    def key(self) -> str:
        return self.info.offering.key

    @property
    def search_text(self) -> str:
        f = self.info.facts
        return " ".join([self.id.full_name, f.description, *f.tags, f.pricing_hint or ""]).lower()

    def entry(self) -> m.CatalogEntry:
        oid, f = self.id, self.info.facts
        return m.CatalogEntry(
            offering=oid.key,
            party=oid.party,
            name=oid.full_name,
            version=str(oid.version),
            description=f.description,
            pricing_hint=f.pricing_hint,
            contract_hash=self.info.contract_hash,
            document=self.info.document,
            card=self.card,
            endpoint=self.endpoint,
            service=oid.service_name,
            task_queue=oid.task_queue,
            public_key=f.public_key,
            published_at=self.published_at,
            retired=self.retired,
        )


@dataclass
class _Quote:
    quote_id: str
    buyer: str
    offering: str
    contract_hash: str
    amount: Money
    max_duration: timedelta
    created_at: datetime
    expires_at: datetime
    job_id: str | None = None
    #: :func:`~kontra._control.input_size` of the input the seller priced: the hold's bound.
    input_size: int | None = None


@dataclass
class _Job:
    record: JobRecord
    buyer_ns: str
    buyer_key: str
    contract_hash: str
    endpoint: str
    service: str
    max_duration: timedelta
    quote_id: str
    hold_ref: str | None = None
    settled: bool = False
    pending_stop: EffectKind | None = None
    pending_stop_reason: str = ""
    delivery: bytes | None = None
    split: Split | None = None
    resolution_note: str = ""
    escrow_closed_at: datetime | None = None
    output_reserved: int = 0
    events: list[m.ProgressEvent] = field(default_factory=list[m.ProgressEvent])
    #: Set once no more progress can arrive (the job closed and its last history summary,
    #: if any, is recorded): a stream that has sent everything then ends.
    progress_final: bool = False
    #: The job state and history summary of the last history event recorded.
    last_progress: tuple[m.JobState, HistorySummary] | None = None
    heartbeats: deque[float] = field(default_factory=deque[float])
    heartbeat_bytes: int = 0
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    #: :func:`~kontra._control.hold_digest` of every hold token issued for the job: a run
    #: of ``job-<id>`` counts only if the buyer's call carried one of them (decisions I15).
    holds: set[str] = field(default_factory=set[str])
    #: The quoted input's size, carried in the hold (decisions I16).
    input_size: int | None = None

    @property
    def job_id(self) -> str:
        return self.record.job_id

    @property
    def workflow_id(self) -> str:
        return job_workflow_id(self.record.job_id)

    @property
    def operation_id(self) -> str:
        """The standalone Nexus operation id in the buyer's namespace."""
        return f"job-{self.record.job_id}"

    @property
    def call(self) -> BuyerCall:
        """The buyer's ``execute`` call as this gate recorded it: what an observed run must
        trace back to before it can deliver."""
        return BuyerCall(
            namespace=self.buyer_ns,
            endpoint=self.endpoint,
            service=self.service,
            holds=frozenset(self.holds),
        )

    @property
    def hold_expires_at(self) -> datetime:
        """The hold token's ``exp``: lock time + max_duration (decisions I10), in whole
        seconds as the token carries it, so the gate and a seller's handler agree on it."""
        exp = int((self.record.created_at + self.max_duration).timestamp())
        return datetime.fromtimestamp(exp, UTC)

    def notify(self) -> None:
        """Wake every progress stream of this job."""
        event, self.changed = self.changed, asyncio.Event()
        event.set()

    def view(self) -> m.JobView:
        r = self.record
        delivery = None
        if r.delivered_at is not None and r.delivery_sha256 is not None:
            delivery = m.DeliveryView(
                sha256=r.delivery_sha256, size=r.delivery_size or 0, delivered_at=r.delivered_at
            )
        return m.JobView(
            job_id=r.job_id,
            offering=r.offering,
            buyer=r.buyer,
            seller=r.seller,
            state=r.state,
            escrow=r.escrow,
            amount_minor=r.amount.amount_minor,
            currency=r.amount.currency,
            price=_amount_text(r.amount.amount_minor, r.amount.currency),
            created_at=r.created_at,
            started_at=r.started_at,
            deadline=r.deadline,
            delivery=delivery,
            review_deadline=r.review_deadline,
            reason=r.reason,
            operation_id=self.operation_id,
            workflow_id=self.workflow_id,
        )


@dataclass
class _Artifact:
    key: str
    owner: str
    size: int
    content_type: str
    job_id: str | None
    upload_id: str | None
    plan: list[int]
    created_at: datetime
    parts: dict[int, bytes] = field(default_factory=dict[int, bytes])
    etags: dict[int, str] = field(default_factory=dict[int, str])
    completed: bool = False
    ciphertext_sha256: str | None = None

    def data(self) -> bytes:
        return b"".join(self.parts.get(n, b"") for n in range(1, len(self.plan) + 1))


# --------------------------------------------------------------------------- the gate


class FakeGate:
    """An in-memory gate. Every parameter has a test-friendly default.

    ``control``: the Temporal control plane (:class:`kontra._control.Control`), or None for
    a gate with no Temporal. ``ledger``: defaults to ``fake_ledger(fee_bps)`` on this gate's
    clock. ``base_url``: what URLs and token issuers say in ASGI mode (``serve()`` replaces
    it with the real address). ``temporal_address``: handed to clients in ``Me`` and
    Temporal tokens (default: the control plane's frontend). ``signup_grant``: play money
    a new party gets (USD). ``resolvers``: parties allowed to resolve disputes.
    ``device_auto_approve``: the device flow approves on the first poll, as the handle
    requested (else :meth:`approve_device`); by default only a ``dev`` gate does, since
    approving any requested handle is logging in as anyone, which is what dev login is.

    The clock offset (:meth:`advance`) moves token ``iat``/``exp`` too: a seller's
    handler verifying a hold token issued *after* an advance on the machine's clock sees
    an ``iat`` in the future, which PyJWT refuses by default. Advance after the start.
    """

    def __init__(
        self,
        control: TemporalControl | None = None,
        *,
        ledger: Ledger | None = None,
        fee_bps: int = 500,
        base_url: str = "http://localhost",
        temporal_address: str | None = None,
        dev: bool = True,
        resolvers: Iterable[str] = (),
        signup_grant: Decimal | int | str = "100.00",
        sweep_interval: float = 0.2,
        grace: timedelta = timedelta(seconds=1),
        observe_give_up: timedelta = timedelta(minutes=15),
        access_ttl: timedelta = timedelta(minutes=15),
        refresh_ttl: timedelta = timedelta(days=30),
        freshness: timedelta = timedelta(minutes=10),
        quote_ttl: timedelta = timedelta(minutes=10),
        quote_rate_per_min: int = 60,
        quote_timeout: float = 10.0,
        device_auto_approve: bool | None = None,
        device_interval: int = 1,
        part_size: int = 16 * 1024 * 1024,
        max_input_bytes: int = 5 * 1000**3,
        url_ttl: timedelta = timedelta(hours=1),
        progress_rate_per_s: int = 5,
        input_retention: timedelta = timedelta(days=7),
        orphan_upload_ttl: timedelta = timedelta(hours=24),
        keepalive: float = 5.0,
    ) -> None:
        if part_size <= 0:
            raise ValueError("part_size must be positive")
        self.control = control
        self._offset = timedelta(0)
        self.ledger: Ledger = (
            ledger if ledger is not None else InMemoryLedger(fee_bps, clock=self.now)
        )
        self.base_url = base_url.rstrip("/")
        self.temporal_address = temporal_address or _frontend_of(control)
        self.dev = dev
        self.resolvers = frozenset(resolvers)
        grant = Money.from_decimal(signup_grant, "USD")
        self.signup_grant = grant if grant.amount_minor > 0 else None
        self.sweep_interval = sweep_interval
        self.grace = grace
        self.observe_give_up = observe_give_up
        self.access_ttl = access_ttl
        self.refresh_ttl = refresh_ttl
        self.freshness = freshness
        self.quote_ttl = quote_ttl
        self.quote_rate_per_min = quote_rate_per_min
        self.quote_timeout = quote_timeout
        self.device_auto_approve = dev if device_auto_approve is None else device_auto_approve
        self.device_interval = device_interval
        self.part_size = part_size
        self.max_input_bytes = max_input_bytes
        self.url_ttl = url_ttl
        self.progress_rate_per_s = progress_rate_per_s
        self.input_retention = input_retention
        self.orphan_upload_ttl = orphan_upload_ttl
        self.keepalive = keepalive

        self.keys = _KeyRing()
        self._url_secret = secrets.token_bytes(32)
        self.parties: dict[str, _Party] = {}
        self.sessions: dict[str, _Session] = {}
        self._refresh: dict[str, _Refresh] = {}
        self._devices: dict[str, _Device] = {}
        self.offerings: dict[str, _Offering] = {}
        self._by_endpoint: dict[str, str] = {}
        self.quotes: dict[str, _Quote] = {}
        self._quote_rate: dict[str, tuple[datetime, int]] = {}
        self.jobs: dict[str, _Job] = {}
        self.artifacts: dict[str, _Artifact] = {}
        self.blobs: dict[str, bytes] = {}
        self._party_locks: dict[str, asyncio.Lock] = {}

        self._loop: asyncio.AbstractEventLoop | None = None
        self._sweeper: asyncio.Task[None] | None = None
        self._stopping = False
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None
        self.app = _build_app(self)

    # ------------------------------------------------------------------ clock

    def now(self) -> datetime:
        """The gate's clock: real UTC time plus :attr:`clock_offset`."""
        return datetime.now(UTC) + self._offset

    @property
    def clock_offset(self) -> timedelta:
        return self._offset

    @clock_offset.setter
    def clock_offset(self, value: timedelta) -> None:
        self._offset = value

    def advance(self, by: timedelta | float) -> None:
        """Move the gate's clock forward (review windows, deadlines, token expiry...)."""
        self._offset += by if isinstance(by, timedelta) else timedelta(seconds=by)

    @property
    def issuer(self) -> str:
        return self.base_url

    # ------------------------------------------------------------------ running

    async def __aenter__(self) -> "FakeGate":
        await self._startup()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self._shutdown()

    async def _startup(self) -> None:
        """Bind to the running loop and start the sweeper (ASGI mode, or uvicorn's lifespan)."""
        self._loop = asyncio.get_running_loop()
        self._stopping = False
        if self.sweep_interval > 0 and self._sweeper is None:
            self._sweeper = self._loop.create_task(self._sweep_forever(), name="fake-gate-sweeper")

    async def _shutdown(self) -> None:
        self._stopping = True
        for job in self.jobs.values():
            job.notify()
        task, self._sweeper = self._sweeper, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def serve(self, host: str = "127.0.0.1", port: int = 0) -> str:
        """Serve on ``host:port`` (a free port by default) from a background thread, with
        the sweeper running there. Returns the base URL, which becomes :attr:`base_url`."""
        if self._thread is not None:
            raise RuntimeError("this gate is already serving")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        self.base_url = f"http://{host}:{sock.getsockname()[1]}"
        config = uvicorn.Config(
            self.app,
            log_level="warning",
            access_log=False,
            lifespan="on",
            timeout_graceful_shutdown=2,
        )
        server = uvicorn.Server(config)
        thread = threading.Thread(
            target=server.run, kwargs={"sockets": [sock]}, name="fake-gate", daemon=True
        )
        self._server, self._thread = server, thread
        thread.start()
        while not server.started:
            if not thread.is_alive():
                self._server = self._thread = None
                raise RuntimeError("the fake gate failed to start")
            await asyncio.sleep(0.01)
        return self.base_url

    async def stop(self) -> None:
        """Stop :meth:`serve` (open progress streams end)."""
        server, thread = self._server, self._thread
        if server is None or thread is None:
            return
        self._stopping = True
        loop = self._loop
        if loop is not None and not loop.is_closed():
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(self._wake_all)
        server.should_exit = True
        await asyncio.to_thread(thread.join, 15)
        self._server = self._thread = None

    def _wake_all(self) -> None:
        for job in self.jobs.values():
            job.notify()

    async def run(self, fn: Callable[[], Awaitable[_T]]) -> _T:
        """``await fn()`` on the gate's loop: the way to read or change gate state (the
        ledger above all, which is not thread-safe) while :meth:`serve` runs it."""
        loop = self._loop
        if loop is None or loop is asyncio.get_running_loop() or self._thread is None:
            return await fn()

        async def call() -> _T:
            return await fn()

        return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(call(), loop))

    async def sweep(self) -> None:
        """One sweeper pass, now (on the gate's loop)."""
        await self.run(self._sweep_once)

    # ------------------------------------------------------------------ auth

    def _issue_access(self, session: _Session) -> str:
        now = self.now()
        return self.keys.sign(
            {
                "iss": self.issuer,
                "aud": AUD_API,
                "sub": session.party,
                "sid": session.id,
                "auth_time": int(session.auth_time.timestamp()),
                "iat": int(now.timestamp()),
                "exp": int((now + self.access_ttl).timestamp()),
                "jti": secrets.token_hex(8),
            }
        )

    def _new_refresh(self, session_id: str) -> str:
        token = secrets.token_urlsafe(32)
        self._refresh[_digest(token)] = _Refresh(session_id, self.now() + self.refresh_ttl)
        return token

    def _open_session(self, party: str, auth_time: datetime, via: str) -> m.TokenResponse:
        session = _Session(secrets.token_hex(16), party, auth_time, via)
        self.sessions[session.id] = session
        return m.TokenResponse(
            access_token=self._issue_access(session),
            expires_in=int(self.access_ttl.total_seconds()),
            refresh_token=self._new_refresh(session.id),
            party=party,
            auth_time=auth_time,
        )

    def authenticate(self, request: Request, *, allow_worker: bool) -> Principal:
        header = request.headers.get("authorization", "")
        scheme, _, token = header.partition(" ")
        if scheme.lower() != "bearer" or not token.strip():
            raise _unauthorized("unauthenticated", "missing bearer token")
        token = token.strip()
        worker = allow_worker and _unverified_audience(token) == AUD_TEMPORAL
        claims = self.keys.verify(
            token,
            audience=AUD_TEMPORAL if worker else AUD_API,
            issuer=self.issuer,
            now=self.now(),
        )
        party, sid = claims.get("sub"), claims.get("sid")
        if (
            not isinstance(party, str)
            or not is_handle(party)
            or not isinstance(sid, str)
            or not sid
        ):
            raise _unauthorized("invalid_token", "token is missing its subject or session")
        if worker and claims.get("permissions") != [f"{party}:worker"]:
            raise _unauthorized(
                "invalid_token", f"worker token must carry exactly the permission {party}:worker"
            )
        session = self.sessions.get(sid)
        if session is None or session.party != party:
            raise _unauthorized("invalid_token", "unknown session")
        if session.revoked:
            raise _unauthorized("invalid_token", "the session was revoked; log in again")
        return Principal(party, sid, session.auth_time, "worker" if worker else "api")

    def require_fresh(self, who: Principal) -> Principal:
        if self.now() - who.auth_time >= self.freshness:
            raise GateHTTPError(
                403,
                "stale_session",
                "this needs a recent login; run `kontra login` again",
                {"max_age_seconds": int(self.freshness.total_seconds())},
            )
        return who

    def require_resolver(self, who: Principal) -> Principal:
        if who.party not in self.resolvers:
            raise GateHTTPError(403, "not_resolver", "only a dispute resolver may do this")
        return who

    def require_dev(self) -> None:
        if not self.dev:
            raise GateHTTPError(404, "not_found", "Not Found")

    def revoke_session(self, session_id: str) -> None:
        session = self.sessions.get(session_id)
        if session is not None:
            session.revoked = True

    # ------------------------------------------------------------------ identity routes

    async def dev_login(self, handle: str) -> m.TokenResponse:
        party = await self.ensure_party(handle)
        return self._open_session(party.handle, self.now(), "dev-login")

    def device_start(self) -> m.DeviceStart:
        now = self.now()
        device_code = secrets.token_urlsafe(32)
        while True:
            raw = "".join(secrets.choice(USER_CODE_ALPHABET) for _ in range(8))
            user_code = f"{raw[:4]}-{raw[4:]}"
            if all(d.user_code != user_code for d in self._devices.values()):
                break
        self._devices[_digest(device_code)] = _Device(
            user_code=user_code,
            expires_at=now + timedelta(minutes=10),
            interval=self.device_interval,
        )
        return m.DeviceStart(
            device_code=device_code,
            user_code=user_code,
            verification_uri=f"{self.base_url}/device",
            verification_uri_complete=f"{self.base_url}/device?user_code={user_code}",
            interval=self.device_interval,
            expires_in=600,
        )

    def _device_by_user_code(self, user_code: str) -> _Device | None:
        return next((d for d in self._devices.values() if d.user_code == user_code), None)

    async def approve_device(self, user_code: str, party: str) -> bool:
        """Approve a pending device login as ``party`` (created if missing), as a human
        would on the verification page. False when the code is not pending."""
        device = self._device_by_user_code(user_code)
        if device is None or device.status != "pending" or self.now() >= device.expires_at:
            return False
        await self.ensure_party(party)
        device.status, device.party, device.approved_at = "approved", party, self.now()
        return True

    def deny_device(self, user_code: str) -> bool:
        device = self._device_by_user_code(user_code)
        if device is None or device.status != "pending":
            return False
        device.status = "denied"
        return True

    async def token(self, req: m.TokenRequest) -> m.TokenResponse:
        if req.grant_type == DEVICE_GRANT:
            if req.device_code is None or req.refresh_token is not None:
                raise GateHTTPError(
                    400, "invalid_request", "the device_code grant takes device_code only"
                )
            return await self._exchange_device(req.device_code, req.handle)
        if req.refresh_token is None or req.device_code is not None or req.handle is not None:
            raise GateHTTPError(
                400, "invalid_request", "the refresh_token grant takes refresh_token only"
            )
        return self._rotate(req.refresh_token)

    async def _exchange_device(self, device_code: str, handle: str | None) -> m.TokenResponse:
        if handle is not None and not is_handle(handle):
            raise GateHTTPError(400, "invalid_request", "handle must be a valid party handle")
        device = (
            self._devices.get(_digest(device_code)) if len(device_code) == _SECRET_LEN else None
        )
        now = self.now()
        if device is None:
            raise GateHTTPError(400, "invalid_grant", "device code is not valid")
        if device.status == "consumed":
            raise GateHTTPError(400, "invalid_grant", "device code was already exchanged")
        if now >= device.expires_at:
            raise GateHTTPError(400, "expired_token", "device code expired; run the login again")
        if device.status == "denied":
            raise GateHTTPError(400, "access_denied", "the login was denied in the browser")
        if handle is not None and device.requested_handle is None:
            device.requested_handle = handle
        if device.status == "pending" and self.device_auto_approve:
            wanted = device.requested_handle
            if wanted is None:
                raise GateHTTPError(
                    400,
                    "invalid_request",
                    "this gate approves a device login only for a requested handle",
                )
            await self.approve_device(device.user_code, wanted)
        if device.status == "approved":
            assert device.party is not None
            assert device.approved_at is not None
            device.status = "consumed"
            return self._open_session(device.party, device.approved_at, "device")
        last, device.last_polled = device.last_polled, now
        if last is not None and now - last < timedelta(seconds=device.interval):
            device.interval += 5
            raise GateHTTPError(
                400,
                "slow_down",
                f"poll at most every {device.interval} seconds",
                {"interval": device.interval},
            )
        raise GateHTTPError(400, "authorization_pending", "waiting for approval in the browser")

    def _rotate(self, refresh_token: str) -> m.TokenResponse:
        """Rotate: the presented token is spent and a new one returned; a spent token
        presented again revokes the whole session (RFC 9700 §4.14.2)."""
        rec = (
            self._refresh.get(_digest(refresh_token)) if len(refresh_token) == _SECRET_LEN else None
        )
        if rec is None:
            raise GateHTTPError(400, "invalid_grant", "refresh token is not valid")
        session = self.sessions.get(rec.session_id)
        if session is None:
            raise GateHTTPError(400, "invalid_grant", "refresh token is not valid")
        if rec.used:
            session.revoked = True
            raise GateHTTPError(
                400,
                "invalid_grant",
                "refresh token was already used; the session is revoked, log in again",
            )
        if session.revoked:
            raise GateHTTPError(400, "invalid_grant", "the session is revoked; log in again")
        if self.now() >= rec.expires_at:
            raise GateHTTPError(400, "invalid_grant", "refresh token has expired; log in again")
        rec.used = True
        return m.TokenResponse(
            access_token=self._issue_access(session),
            expires_in=int(self.access_ttl.total_seconds()),
            refresh_token=self._new_refresh(session.id),
            party=session.party,
            auth_time=session.auth_time,
        )

    def temporal_token(self, who: Principal) -> m.TemporalToken:
        """The party's Temporal *worker* JWT: the only Temporal token there is (decisions
        I3). A dev server does not enforce it; it is well-formed all the same."""
        now = self.now()
        expires = datetime.fromtimestamp(int((now + WORKER_TTL).timestamp()), UTC)
        token = self.keys.sign(
            {
                "iss": self.issuer,
                "aud": AUD_TEMPORAL,
                "sub": who.party,
                "sid": who.session_id,
                "permissions": [f"{who.party}:worker"],
                "iat": int(now.timestamp()),
                "exp": int(expires.timestamp()),
            }
        )
        return m.TemporalToken(
            token=token,
            expires_at=expires,
            namespace=self.parties[who.party].namespace,
            temporal_address=self.temporal_address,
            role="worker",
        )

    # ------------------------------------------------------------------ parties

    async def ensure_party(self, handle: str) -> _Party:
        """The party ``handle``, created (namespace registered, signup grant paid) if
        missing."""
        try:
            validate_handle(handle)
        except NameError_ as e:
            raise GateHTTPError(422, "invalid_handle", str(e)) from None
        lock = self._party_locks.setdefault(handle, asyncio.Lock())
        async with lock:
            existing = self.parties.get(handle)
            if existing is not None:
                return existing
            if self.control is not None:
                try:
                    await self.control.ensure_namespace(handle)
                except TemporalError as e:
                    raise GateHTTPError(
                        503, "namespace_unavailable", f"could not register the namespace: {e}"
                    ) from None
            party = _Party(handle=handle, namespace=handle, created_at=self.now())
            if self.signup_grant is not None:
                await self.ledger.deposit(handle, self.signup_grant, ref=f"signup:{handle}")
            self.parties[handle] = party
            return party

    def _party(self, handle: str) -> _Party:
        p = self.parties.get(handle)
        if p is None:
            raise GateHTTPError(404, "party_not_found", "no such party")
        return p

    def me(self, who: Principal) -> m.Me:
        p = self._party(who.party)
        return m.Me(
            party=p.handle,
            namespace=p.namespace,
            public_key=p.public_key,
            temporal_address=self.temporal_address,
            created_at=p.created_at,
            dev=self.dev,
        )

    def set_key(self, who: Principal, public_key: str) -> m.Me:
        try:
            check_public_key(public_key)
        except ValueError as e:
            raise GateHTTPError(422, "invalid_key", str(e)) from None
        self._party(who.party).public_key = public_key
        return self.me(who)

    def reputation(self, party: str) -> tuple[int, int, int]:
        """(released, refunded, disputed) of the seller's jobs: §17's raw counts. Self-hires
        and jobs the buyer canceled -- through the gate, or by cancelling its own call
        (``Observation.buyer_canceled``) -- do not count; a hold never locked does not
        either. A canceled job counts only when the buyer's own run of it ended canceled
        without the buyer asking: the seller stopped it."""
        released = refunded = disputed = 0
        for job in self.jobs.values():
            r = job.record
            if r.seller != party or r.buyer == r.seller:
                continue
            if r.escrow is m.Escrow.RELEASED:
                released += 1
            elif r.escrow is m.Escrow.REFUNDED:
                if job.hold_ref is None:
                    continue
                if r.state is not m.JobState.CANCELED or (
                    r.run_id is not None and r.reason != escrow.BUYER_CANCEL_REASON
                ):
                    refunded += 1
            elif r.escrow in (m.Escrow.DISPUTED, m.Escrow.RESOLVED):
                disputed += 1
        return released, refunded, disputed

    def party_view(self, handle: str) -> m.PartyView:
        p = self._party(handle)
        released, refunded, disputed = self.reputation(handle)
        return m.PartyView(
            party=handle,
            public_key=p.public_key,
            released=released,
            refunded=refunded,
            disputed=disputed,
        )

    # ------------------------------------------------------------------ catalog

    def entry_url(self, oid: OfferingId) -> str:
        return f"{self.base_url}/v1/catalog/{oid.party}/{oid.name}/{oid.version}"

    def _visible(self) -> list[_Offering]:
        return [o for o in self.offerings.values() if o.endpoint_ready and not o.retired]

    def _open_jobs(self, offering: str) -> list[_Job]:
        return [j for j in self.jobs.values() if j.record.offering == offering and j.record.open]

    async def publish(self, who: Principal, document: dict[str, Any]) -> m.CatalogEntry:
        """Publishing (§11.4, decisions I1/I3): validate, check ownership, assign an opaque
        endpoint name once per version, and create the endpoint through the control plane
        targeting the caller's namespace and the version's task queue."""
        try:
            info = validate_document(document)
        except ContractError as e:
            raise GateHTTPError(
                422, "invalid_contract", f"{e.path}: {e.message}", {"path": e.path}
            ) from None
        oid = info.offering
        if oid.party != who.party:
            raise GateHTTPError(
                403,
                "not_owner",
                f"{oid.full_name} belongs to the party {oid.party!r}; you are {who.party!r}",
            )
        owner = self._party(who.party)
        card = agent_card(info.document, url=self.entry_url(oid))
        now = self.now()
        row = self.offerings.get(oid.key)
        if row is None:
            row = _Offering(
                info=info,
                endpoint=new_endpoint_name(),
                endpoint_ready=False,
                card=card,
                published_at=now,
            )
            self.offerings[oid.key] = row
            self._by_endpoint[row.endpoint] = oid.key
        elif row.info.contract_hash != info.contract_hash:
            open_jobs = len(self._open_jobs(oid.key))
            if open_jobs:
                raise GateHTTPError(
                    409,
                    "version_in_use",
                    f"{oid.key} has {open_jobs} open job(s) under contract {row.info.contract_hash}; "
                    "publish a new version instead",
                    {"open_jobs": open_jobs, "contract_hash": row.info.contract_hash},
                )
            row.info, row.card, row.published_at = info, card, now
        if self.control is not None:
            try:
                await self.control.create_endpoint(row.endpoint, owner.namespace, oid.task_queue)
            except EndpointConflict:
                raise GateHTTPError(
                    409,
                    "endpoint_conflict",
                    "the offering's Nexus endpoint exists with another target",
                ) from None
            except TemporalError:
                log.exception("creating endpoint %s of %s failed", row.endpoint, oid.key)
                raise GateHTTPError(
                    503,
                    "endpoint_unavailable",
                    "could not create the Nexus endpoint yet; publish again shortly",
                ) from None
        row.endpoint_ready = True
        return row.entry()

    def resolve_ref(self, ref: OfferingRef) -> _Offering | None:
        """The highest visible version ``ref`` accepts (§7.4)."""
        candidates = [
            o
            for o in self._visible()
            if o.id.party == ref.party and o.id.name == ref.name and ref.matches(o.id.version)
        ]
        return max(candidates, key=lambda o: o.id.version) if candidates else None

    def resolve_path(self, party: str, name: str, version: str) -> _Offering:
        """The catalog path ``/{party}/{name}/{version}``, each segment validated on its own
        and the reference built from the parts (never joined and re-parsed)."""
        nums: list[int | None] = []
        ok = is_handle(party) and is_handle(name) and len(version) <= _PATH_VERSION_MAX
        if ok and version != "latest":
            pieces = version.split(".")
            ok = 1 <= len(pieces) <= 3 and all(_is_number(p) for p in pieces)
            nums = [int(p) for p in pieces] if ok else []
        if not ok:
            raise GateHTTPError(404, "offering_not_found", "no such offering")
        nums += [None] * (3 - len(nums))
        found = self.resolve_ref(OfferingRef(party, name, nums[0], nums[1], nums[2]))
        if found is None:
            raise GateHTTPError(404, "offering_not_found", "no such offering")
        return found

    def by_endpoint(self, endpoint: str) -> m.CatalogEntry:
        key = self._by_endpoint.get(endpoint) if is_endpoint_name(endpoint) else None
        row = self.offerings.get(key) if key is not None else None
        if row is None or not row.endpoint_ready or row.retired:
            raise GateHTTPError(404, "endpoint_not_found", "no offering has this endpoint")
        return row.entry()

    def search(self, q: str | None, cursor: str | None, limit: int) -> m.CatalogList:
        latest: dict[tuple[str, str], _Offering] = {}
        for o in self._visible():
            k = (o.id.party, o.id.name)
            if k not in latest or o.id.version > latest[k].id.version:
                latest[k] = o
        rows = sorted(latest.values(), key=lambda o: (o.id.party, o.id.name))
        if q:
            needle = q.lower()
            rows = [o for o in rows if needle in o.search_text]
        if cursor:
            after = _unb64url_json(cursor)
            if (
                not isinstance(after, list)
                or len(after) != 2
                or not all(is_handle(x) for x in after)
            ):  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
                raise GateHTTPError(422, "invalid_cursor", "cursor is not one this gate handed out")
            start = (str(after[0]), str(after[1]))  # pyright: ignore[reportUnknownArgumentType]
            rows = [o for o in rows if (o.id.party, o.id.name) > start]
        page = rows[:limit]
        items = [
            m.CatalogSummary(
                offering=o.key,
                name=o.id.full_name,
                version=str(o.id.version),
                description=o.info.facts.description,
                pricing_hint=o.info.facts.pricing_hint,
                contract_hash=o.info.contract_hash,
                seller=self.party_view(o.id.party),
            )
            for o in page
        ]
        nxt = (
            _b64url_json([page[-1].id.party, page[-1].id.name])
            if len(rows) > limit and page
            else None
        )
        return m.CatalogList(items=items, next_cursor=nxt)

    # ------------------------------------------------------------------ quotes

    def _rate_limit(self, party: str) -> None:
        now = self.now()
        window = now.replace(second=0, microsecond=0)
        start, count = self._quote_rate.get(party, (window, 0))
        count = count + 1 if start == window else 1
        self._quote_rate[party] = (window, count)
        if count > self.quote_rate_per_min:
            retry = math.ceil((window + timedelta(minutes=1) - now).total_seconds())
            raise GateHTTPError(
                429,
                "rate_limited",
                f"at most {self.quote_rate_per_min} quote requests per minute",
                {"retry_after_seconds": retry},
                headers={"Retry-After": str(retry)},
            )

    @staticmethod
    def _eta(until: datetime | None, now: datetime) -> int:
        if until is None:
            return 0
        return min(MAX_ETA_SECONDS, max(0, math.ceil((until - now).total_seconds())))

    def _busy(self, row: _Offering, now: datetime) -> tuple[int, int] | None:
        """(open jobs, eta seconds) when the version is at its ``max_concurrent_jobs``."""
        limit = row.info.facts.max_concurrent_jobs
        if limit is None:
            return None
        open_jobs = self._open_jobs(row.key)
        if len(open_jobs) < limit:
            return None
        earliest = min(j.record.deadline for j in open_jobs)
        return len(open_jobs), self._eta(earliest, now)

    async def quote(self, who: Principal, req: m.QuoteRequest) -> m.QuoteView:
        """The quote relay (§8.2 step 1, decisions I4/I11): busy at the concurrency limit,
        else the buyer's payload relayed byte for byte to the seller's sync ``quote``."""
        self._rate_limit(who.party)
        try:
            ref = OfferingRef.parse(req.offering)
        except NameError_ as e:
            raise GateHTTPError(422, "invalid_offering", str(e)) from None
        payload = _decode_wire_payload(req.input)
        row = self.resolve_ref(ref)
        if row is None:
            raise GateHTTPError(404, "offering_not_found", f"no published offering matches {ref}")
        oid, facts = row.id, row.info.facts

        def view(status: m.QuoteStatus, **fields: Any) -> m.QuoteView:
            return m.QuoteView.model_validate(
                {
                    "quote_id": None,
                    "offering": oid.key,
                    "status": status,
                    "contract_hash": row.info.contract_hash,
                    "endpoint": row.endpoint,
                    "service": oid.service_name,
                    "seller_public_key": facts.public_key,
                    **fields,
                }
            )

        now = self.now()
        busy = self._busy(row, now)
        if busy is not None:
            return view(
                m.QuoteStatus.BUSY,
                eta_seconds=busy[1],
                reason=f"{oid.key} is running its maximum of {facts.max_concurrent_jobs} concurrent jobs",
            )
        if self.control is None:
            raise GateHTTPError(
                503,
                "seller_unavailable",
                "the seller did not answer; no worker is serving this offering right now",
            )
        try:
            answer = await self.control.nexus_sync(
                oid.namespace,
                oid.task_queue,
                oid.service_name,
                QUOTE_OPERATION,
                payload,
                {},
                self.quote_timeout,
            )
        except TemporalError:
            log.exception("quote relay to %s failed", oid.key)
            raise GateHTTPError(
                503, "seller_unavailable", "the seller could not be reached; try again later"
            ) from None
        if answer.kind is NexusSyncKind.HANDLER_ERROR:
            log.info("quote handler of %s failed: %s", oid.key, answer.message[:500])
            raise GateHTTPError(502, "seller_error", "the seller's quote handler failed")
        if answer.kind is not NexusSyncKind.OK:
            raise GateHTTPError(
                503,
                "seller_unavailable",
                "the seller did not answer; no worker is serving this offering right now",
                {"reason": answer.kind.value},
            )
        q = _parse_quote(answer.payload)
        if q.status == "rejected":
            return view(m.QuoteStatus.REJECTED, reason=q.reason)
        if q.status == "busy":
            return view(m.QuoteStatus.BUSY, eta_seconds=q.eta_seconds, reason=q.reason)
        amount, currency, seconds = q.amount_minor, q.currency, q.max_duration_seconds
        assert amount is not None
        assert currency is not None
        assert seconds is not None
        if seconds > facts.max_duration.total_seconds():
            raise GateHTTPError(
                502, "invalid_quote", "the seller quoted a max_duration above the offering's"
            )
        try:
            # Read once the seller's quote fetched what a reference names and checked it
            # against its size: the bound is the seller's verified figure (decisions I16).
            size = input_size(payload)
        except InvalidPayload:
            size = payload.ByteSize()  # the seller took it as it is: a plain payload
        stored = _Quote(
            quote_id=new_job_id(),
            buyer=who.party,
            offering=oid.key,
            contract_hash=row.info.contract_hash,
            amount=Money(amount, currency),
            max_duration=timedelta(seconds=seconds),
            created_at=now,
            expires_at=now + self.quote_ttl,
            input_size=size,
        )
        self.quotes[stored.quote_id] = stored
        return view(
            m.QuoteStatus.OK,
            quote_id=stored.quote_id,
            amount_minor=amount,
            currency=currency,
            price=_amount_text(amount, currency),
            max_duration_seconds=seconds,
            expires_at=stored.expires_at,
        )

    # ------------------------------------------------------------------ hire & jobs

    def hold_token(self, job: _Job) -> str:
        """The hold token (D10, decisions I10), from the job alone: re-issued for ``start``
        it carries the same claims as at hire. Every token issued is remembered by
        :func:`~kontra._control.hold_digest`, the digest of the bytes its signature covers,
        so the buyer's call is matched to the job by exact equality whichever of the
        spellings the seller's handler accepts it carried (decisions I15). Re-signing the
        same claims gives the same digest."""
        r = job.record
        claims = m.HoldClaims(
            iss=self.issuer,
            aud="kontra:hold",
            sub=r.job_id,
            iat=int(r.created_at.timestamp()),
            exp=int(job.hold_expires_at.timestamp()),
            job_id=r.job_id,
            offering=r.offering,
            seller=r.seller,
            seller_ns=r.seller_ns,
            buyer=r.buyer,
            buyer_ns=job.buyer_ns,
            buyer_key=job.buyer_key,
            amount_minor=r.amount.amount_minor,
            currency=r.amount.currency,
            contract_hash=job.contract_hash,
            max_duration_seconds=int(job.max_duration.total_seconds()),
            input_size=job.input_size,
            # The refund deadline with this gate's own grace (one second by default), so a
            # seller's handler ends a late-started run by it (decisions I16).
            deadline=int(r.deadline.timestamp()),
        )
        token = self.keys.sign(claims.model_dump())
        job.holds.add(hold_digest(token))
        return token

    async def hire(self, who: Principal, req: m.HireRequest) -> m.HireView:
        now = self.now()
        q = self.quotes.get(req.quote_id)
        if q is None or q.buyer != who.party:
            raise GateHTTPError(404, "quote_not_found", "no such quote")
        if q.job_id is not None:
            raise GateHTTPError(
                409, "quote_used", "this quote was already hired", {"job_id": q.job_id}
            )
        if now >= q.expires_at:
            raise GateHTTPError(409, "quote_expired", "this quote has expired; quote again")
        price = _amount_text(q.amount.amount_minor, q.amount.currency)
        if req.max_price_minor is not None and q.amount.amount_minor > req.max_price_minor:
            raise GateHTTPError(
                409,
                "price_above_max",
                f"the quote is {price} {q.amount.currency}, above your maximum",
                {"amount_minor": q.amount.amount_minor, "max_price_minor": req.max_price_minor},
            )
        me = self._party(who.party)
        if me.public_key is None:
            raise GateHTTPError(
                409,
                "no_public_key",
                "set your encryption key first (kontra login): the delivery is encrypted to it",
            )
        row = self.offerings.get(q.offering)
        if row is None or not row.endpoint_ready or row.retired:
            raise GateHTTPError(409, "offering_unavailable", f"{q.offering} is not available")
        if row.info.contract_hash != q.contract_hash:
            raise GateHTTPError(
                409, "quote_stale", f"{q.offering} was republished since the quote; quote again"
            )
        busy = self._busy(row, now)
        if busy is not None:
            raise GateHTTPError(
                409,
                "busy",
                f"{q.offering} is running its maximum of {row.info.facts.max_concurrent_jobs} jobs",
                {"eta_seconds": busy[1]},
            )
        oid = row.id
        job_id = new_job_id()
        q.job_id = job_id  # claimed before the first await: two hires of one quote make one job
        record = escrow.new_job(
            job_id=job_id,
            offering=oid.key,
            buyer=who.party,
            seller=oid.party,
            seller_ns=oid.namespace,
            amount=q.amount,
            max_duration=q.max_duration,
            review_window=row.info.facts.review_window,
            now=now,
            grace=self.grace,
        )
        job = _Job(
            record=record,
            buyer_ns=me.namespace,
            buyer_key=me.public_key,
            contract_hash=row.info.contract_hash,
            endpoint=row.endpoint,
            service=oid.service_name,
            max_duration=q.max_duration,
            quote_id=q.quote_id,
            input_size=q.input_size,
        )
        self.jobs[job_id] = job
        try:
            handle = await self.ledger.lock(
                job_id, who.party, oid.party, q.amount, expires_at=record.deadline
            )
            if handle.state is HandleState.FAILED:
                raise InsufficientFunds("the ledger could not lock the price")
        except InsufficientFunds:
            self._abandon(job)
            raise GateHTTPError(
                402,
                "insufficient_funds",
                f"your available balance does not cover {price} {q.amount.currency}",
                {"amount_minor": q.amount.amount_minor, "currency": q.amount.currency},
            ) from None
        except CurrencyMismatch:
            self._abandon(job)
            raise GateHTTPError(
                409, "unsupported_currency", f"this gate does not settle {q.amount.currency}"
            ) from None
        except LedgerError:
            self._abandon(job)
            log.exception("ledger refused the hold of job %s", job_id)
            raise GateHTTPError(409, "hold_refused", "the ledger refused the hold") from None
        job.hold_ref = handle.ref
        return m.HireView(
            job_id=job_id,
            hold_token=self.hold_token(job),
            offering=oid.key,
            endpoint=row.endpoint,
            service=oid.service_name,
            workflow_id=job.workflow_id,
            operation_id=job.operation_id,
            contract_hash=job.contract_hash,
            amount_minor=q.amount.amount_minor,
            currency=q.amount.currency,
            max_duration_seconds=int(q.max_duration.total_seconds()),
            seller_public_key=row.info.facts.public_key,
            expires_at=job.hold_expires_at,
        )

    def _abandon(self, job: _Job) -> None:
        """The ledger refused the lock: no job, and the quote may be hired again."""
        self.jobs.pop(job.job_id, None)
        q = self.quotes.get(job.quote_id)
        if q is not None and q.job_id == job.job_id:
            q.job_id = None

    def job_for(self, job_id: str, party: str) -> _Job:
        """The job, if ``party`` is its buyer or seller; 404 otherwise (a stranger learns
        nothing, not even that the id exists)."""
        job = self.jobs.get(job_id) if is_job_id(job_id) else None
        if job is None or party not in (job.record.buyer, job.record.seller):
            raise GateHTTPError(404, "job_not_found", "no such job")
        return job

    @staticmethod
    def _require_buyer(job: _Job, party: str, what: str) -> None:
        if party != job.record.buyer:
            raise GateHTTPError(403, "forbidden", f"only the buyer can {what}")

    def _page(self, rows: list[_Job], cursor: str | None, limit: int) -> m.JobList:
        rows = sorted(rows, key=lambda j: (j.record.created_at, j.job_id), reverse=True)
        if cursor:
            raw = _unb64url_json(cursor)
            try:
                at_text, after_id = raw
                at = datetime.fromisoformat(at_text)
                if at.tzinfo is None or not is_job_id(after_id):
                    raise ValueError
            except (TypeError, ValueError):
                raise GateHTTPError(
                    422, "invalid_cursor", "cursor is not one this gate handed out"
                ) from None
            rows = [j for j in rows if (j.record.created_at, j.job_id) < (at, after_id)]
        page = rows[:limit]
        nxt = None
        if len(rows) > limit and page:
            nxt = _b64url_json([page[-1].record.created_at.isoformat(), page[-1].job_id])
        return m.JobList(items=[j.view() for j in page], next_cursor=nxt)

    def list_jobs(
        self, party: str, as_: str, state: m.JobState | None, cursor: str | None, limit: int
    ) -> m.JobList:
        rows = [
            j
            for j in self.jobs.values()
            if (j.record.buyer if as_ == "buyer" else j.record.seller) == party
            and (state is None or j.record.state is state)
        ]
        return self._page(rows, cursor, limit)

    def seller_view(self, who: Principal, job_id: str) -> m.SellerJobView:
        job = self.job_for(job_id, who.party)
        if who.party != job.record.seller:
            raise GateHTTPError(403, "forbidden", "only the seller can read this")
        return m.SellerJobView(
            job_id=job_id,
            offering=job.record.offering,
            buyer_public_key=job.buyer_key,
            contract_hash=job.contract_hash,
            max_duration_seconds=int(job.max_duration.total_seconds()),
        )

    async def start(self, who: Principal, job_id: str, wire: m.WirePayload) -> m.JobView:
        """Start ``execute`` as a standalone Nexus operation in the BUYER's namespace on its
        behalf (decisions I4), with the hold token in its header. Idempotent."""
        job = self.job_for(job_id, who.party)
        self._require_buyer(job, who.party, "start the job")
        if not job.record.open:
            raise GateHTTPError(409, "not_open", f"job is {job.record.state.value}")
        now = self.now()
        if now >= job.hold_expires_at:
            raise GateHTTPError(
                409, "hold_expired", "the hold token has expired; the job will refund"
            )
        payload = _decode_wire_payload(wire)
        if self.control is None:
            raise GateHTTPError(
                503, "temporal_unavailable", "could not start the job yet; try again shortly"
            )
        try:
            await self.control.start_operation(
                job.buyer_ns,
                job.operation_id,
                job.endpoint,
                job.service,
                EXECUTE_OPERATION,
                payload,
                {HOLD_HEADER: self.hold_token(job)},
                max(timedelta(seconds=1), job.record.deadline - now),
            )
        except TemporalError:
            log.exception("starting job %s in %s failed", job_id, job.buyer_ns)
            raise GateHTTPError(
                503, "temporal_unavailable", "could not start the job yet; try again shortly"
            ) from None
        return job.view()

    def result(self, who: Principal, job_id: str) -> m.ResultView:
        job = self.job_for(job_id, who.party)
        self._require_buyer(job, who.party, "fetch the result")
        if job.delivery is None or job.record.delivered_at is None:
            raise GateHTTPError(404, "not_delivered", "the job has not delivered")
        return m.ResultView(
            job_id=job_id,
            payload=m.WirePayload(b64=base64.b64encode(job.delivery).decode("ascii")),
            sha256=hashlib.sha256(job.delivery).hexdigest(),
            size=len(job.delivery),
            delivered_at=job.record.delivered_at,
        )

    async def accept(self, who: Principal, job_id: str) -> m.JobView:
        self.job_for(job_id, who.party)
        now = self.now()
        return (await self._transition(job_id, lambda r: escrow.accept(r, who.party, now))).view()

    async def reject(self, who: Principal, job_id: str, reason: str) -> m.JobView:
        self.job_for(job_id, who.party)
        now = self.now()
        return (
            await self._transition(job_id, lambda r: escrow.reject(r, who.party, reason, now))
        ).view()

    async def cancel(self, who: Principal, job_id: str) -> m.JobView:
        """The buyer cancels before delivery (refund). The seller's workflow is looked at
        first: a buyer whose operation already returned the result could otherwise cancel
        in the gap before the sweeper sees the completion, and keep both the delivery and
        the money. When Temporal cannot say, the cancel is refused rather than guessed.

        A job with no run yet is closed to starts before it is refunded
        (:meth:`~kontra._control.Control.occupy_job`, decisions I16): the hold token stays
        valid until it expires and the seller's handler checks it offline, so the buyer
        could otherwise run ``execute`` with it after the refund and get a short job done
        for free. If a run appeared in the meantime, it is looked at again and stopped as a
        running job's is. If the id cannot be taken, the cancel is refused.

        A job with an observed run is canceled in the two steps of :func:`escrow.cancel`:
        its run is terminated, and the refund waits for what Temporal shows of the pinned
        chain afterwards (:meth:`_complete_cancel`)."""
        job = self.job_for(job_id, who.party)
        if who.party == job.record.buyer and job.record.open and self.control is not None:
            obs = await self._look_before_cancel(job)
            if obs.status is ObservedStatus.NOT_FOUND and job.record.open:
                try:
                    occupied = await self.control.occupy_job(
                        job.record.seller_ns, job.workflow_id, escrow.BUYER_CANCEL_REASON
                    )
                except Exception:
                    log.warning("cannot occupy %s for a cancel", job.workflow_id, exc_info=True)
                    raise GateHTTPError(
                        503,
                        "temporal_unavailable",
                        "could not close the job to late starts; try again shortly",
                    ) from None
                if occupied is Occupation.STARTED:
                    await self._look_before_cancel(job)
        now = self.now()
        job = await self._transition(job_id, lambda r: escrow.cancel(r, who.party, now))
        if job.record.open:
            await self._complete_cancel(job)
            # Closed now: canceled (refunded, an idempotent repeat), or delivered or failed
            # before the termination landed (refused, 409, as any cancel of a closed job).
            now = self.now()
            job = await self._transition(job_id, lambda r: escrow.cancel(r, who.party, now))
        return job.view()

    async def _complete_cancel(self, job: _Job) -> None:
        """Step two of a cancel of a job with an observed run. The cancel's
        ``TERMINATE_RUN`` has been carried out (or failed, and is still pending); the money
        now follows what Temporal shows of the pinned chain, never the request alone:
        terminated is the buyer's cancel, refunded; completed first is a delivery, which
        stands. A termination that did not land, or a chain that cannot be observed, is a
        503: the cancel stays requested, the sweeper keeps terminating and observing until
        the chain closes, and asking again is safe."""
        if job.pending_stop is EffectKind.TERMINATE_RUN or self.control is None:
            raise GateHTTPError(
                503,
                "temporal_unavailable",
                "could not stop the job's run yet; the cancel stands, try again shortly",
            )
        await self._look_before_cancel(job)
        if job.record.open:
            raise GateHTTPError(
                503,
                "temporal_unavailable",
                "the job's run is still being stopped; the cancel stands, try again shortly",
            )

    async def _look_before_cancel(self, job: _Job) -> Observation:
        """Observe ``job-<id>`` and apply what it shows, for a cancel: 503 when Temporal
        cannot say."""
        assert self.control is not None
        try:
            obs, result = await self.control.observe(
                job.record.seller_ns, job.workflow_id, job.call, job.record.run_id
            )
        except Exception:
            log.warning("cannot observe %s before a cancel", job.workflow_id, exc_info=True)
            raise GateHTTPError(
                503,
                "temporal_unavailable",
                "could not check whether the job has delivered; try again shortly",
            ) from None
        await self._apply_observation(job.job_id, obs, result)
        return obs

    # ------------------------------------------------------------------ disputes

    def disputes(self, cursor: str | None, limit: int) -> m.JobList:
        rows = [j for j in self.jobs.values() if j.record.escrow is m.Escrow.DISPUTED]
        return self._page(rows, cursor, limit)

    async def resolve(self, who: Principal, job_id: str, req: m.ResolveRequest) -> m.JobView:
        job = self.jobs.get(job_id) if is_job_id(job_id) else None
        if job is None:
            raise GateHTTPError(404, "job_not_found", "no such job")
        if who.party in (job.record.buyer, job.record.seller):
            raise GateHTTPError(
                403, "conflict_of_interest", "a resolver cannot decide a dispute they are party to"
            )
        if len(req.note) > NOTE_MAX:
            raise GateHTTPError(422, "invalid_note", f"note is at most {NOTE_MAX} characters")
        try:
            split = Split(req.buyer_minor, req.seller_minor)
        except InvalidSplit as e:
            raise GateHTTPError(422, "invalid_split", str(e)) from None
        if split.buyer_minor + split.seller_minor != job.record.amount.amount_minor:
            raise GateHTTPError(
                422,
                "invalid_split",
                f"the split must add up to the held amount ({job.record.amount.amount_minor})",
            )
        if job.split is not None and job.split != split:
            raise GateHTTPError(
                409, "already_resolved", "this dispute was resolved with another split"
            )
        if job.record.escrow is not m.Escrow.DISPUTED and job.split is None:
            raise GateHTTPError(
                409,
                "not_disputed",
                f"escrow is {job.record.escrow.value}; only a disputed job can be resolved",
            )
        job.split, job.resolution_note = split, req.note
        now = self.now()
        return (await self._transition(job_id, lambda r: escrow.resolve(r, now))).view()

    # ------------------------------------------------------------------ money

    async def balance(self, party: str, currency: str) -> m.Balance:
        if currency not in CURRENCIES:
            raise GateHTTPError(
                422,
                "unsupported_currency",
                f"currency must be one of {', '.join(sorted(CURRENCIES))}",
            )
        try:
            b = await self.ledger.balance(party, currency)
        except CurrencyMismatch:
            raise GateHTTPError(
                422, "unsupported_currency", f"this gate does not settle {currency}"
            ) from None
        return m.Balance(
            party=party,
            currency=currency,
            available_minor=b.available_minor,
            held_minor=b.held_minor,
            available=_amount_text(b.available_minor, currency),
            held=_amount_text(b.held_minor, currency),
        )

    async def topup(self, party: str, req: m.Topup) -> m.TopupResult:
        """Instant play money (the fake gate never takes a payment)."""
        if req.currency not in CURRENCIES:
            raise GateHTTPError(
                422,
                "unsupported_currency",
                f"currency must be one of {', '.join(sorted(CURRENCIES))}",
            )
        try:
            await self.ledger.deposit(
                party,
                Money(req.amount_minor, req.currency),
                ref=f"dev-topup:{secrets.token_hex(16)}",
            )
        except (InvalidAmount, CurrencyMismatch) as e:
            raise GateHTTPError(422, "invalid_amount", str(e)) from None
        return m.TopupResult(credited=True, balance=await self.balance(party, req.currency))

    # ------------------------------------------------------------------ progress

    def _record_event(self, job: _Job, **fields: Any) -> m.ProgressEvent:
        event = m.ProgressEvent.model_validate({"seq": len(job.events) + 1, **fields})
        job.events.append(event)
        job.notify()
        return event

    def post_progress(self, who: Principal, job_id: str, post: m.ProgressPost) -> None:
        """A seller's heartbeat details, encrypted to the buyer. At most
        ``progress_rate_per_s`` per second per job are kept (latest-wins is the sender's
        job); the excess is dropped without an error, as a throttle should."""
        job = self.job_for(job_id, who.party)
        if who.party != job.record.seller:
            raise GateHTTPError(403, "forbidden", "only the job's seller posts progress")
        if not job.record.open:
            raise GateHTTPError(409, "not_open", f"job is {job.record.state.value}")
        if _ACTIVITY.fullmatch(post.activity) is None:
            raise GateHTTPError(
                422,
                "invalid_progress",
                "activity must be 1 to 255 characters without control characters",
            )
        if len(post.ciphertext_b64) > 4 * math.ceil(MAX_PROGRESS_BYTES / 3):
            raise GateHTTPError(
                413, "too_large", f"a progress event is at most {MAX_PROGRESS_BYTES} bytes"
            )
        try:
            raw = base64.b64decode(post.ciphertext_b64, validate=True)
        except (binascii.Error, ValueError):
            raise GateHTTPError(
                422, "invalid_progress", "ciphertext_b64 is not standard base64"
            ) from None
        if len(raw) > MAX_PROGRESS_BYTES:
            raise GateHTTPError(
                413, "too_large", f"a progress event is at most {MAX_PROGRESS_BYTES} bytes"
            )
        if not raw.startswith(AGE_MAGIC):
            # As the platform gate does: a client bug must not leave plaintext here.
            raise GateHTTPError(
                422,
                "not_ciphertext",
                "progress details must be encrypted to the buyer (an age v1 file)",
            )
        mono = time.monotonic()
        beats = job.heartbeats
        while beats and mono - beats[0] >= 1.0:
            beats.popleft()
        if len(beats) >= self.progress_rate_per_s:
            return
        if job.heartbeat_bytes + len(raw) > HEARTBEAT_BUDGET_BYTES:
            return  # the job's budget is spent; history events still show liveness
        beats.append(mono)
        job.heartbeat_bytes += len(raw)
        self._record_event(
            job,
            at=post.at if post.at.tzinfo is not None else post.at.replace(tzinfo=UTC),
            source=m.ProgressSource.HEARTBEAT,
            activity=post.activity,
            ciphertext_b64=post.ciphertext_b64,
            state=job.record.state,
        )

    async def progress_stream(self, job: _Job, after: int) -> AsyncIterator[bytes]:
        """Replay events after ``after``, then follow; keepalive comments while idle; end
        once the job is closed and every event has been sent."""
        last = after
        while True:
            waiter = job.changed
            for event in job.events[last:]:
                last = event.seq
                data = event.model_dump_json()
                yield f"id: {event.seq}\nevent: progress\ndata: {data}\n\n".encode()
            if (job.progress_final and last >= len(job.events)) or self._stopping:
                return
            try:
                async with asyncio.timeout(self.keepalive):
                    await waiter.wait()
            except TimeoutError:
                yield b": keepalive\n\n"

    # ------------------------------------------------------------------ transitions & effects

    async def _transition(
        self,
        job_id: str,
        rule: Callable[[JobRecord], tuple[JobRecord, list[Effect]]],
        *,
        result: bytes | None = None,
        observed: bool = False,
    ) -> _Job:
        """Apply ``rule`` to the job's *current* record, save what it returns, then carry
        out its effects. No await between reading and saving, so a buyer's call and the
        sweeper serialize per job. ``observed``: the sweeper will record the closing
        history summary itself, so the progress stream is not finalized here."""
        job = self.jobs.get(job_id)
        if job is None:
            raise GateHTTPError(404, "job_not_found", "no such job")
        new, effects = rule(job.record)
        stop = next((e for e in effects if e.kind in _STOPS), None)
        if new != job.record or stop is not None:
            old = job.record
            job.record = new
            if new.escrow is not old.escrow:
                job.settled = False
                if new.escrow in _CLOSED_ESCROW:
                    job.escrow_closed_at = self.now()
            if stop is not None:
                job.pending_stop, job.pending_stop_reason = stop.kind, stop.reason
            if result is not None and new.state is m.JobState.COMPLETED and job.delivery is None:
                job.delivery = result  # a job delivers once: the first stored wins
            if old.open and not new.open and not observed:
                job.progress_final = True
            job.notify()
        await self._execute_effects(job, effects)
        return job

    async def _execute_effects(self, job: _Job, effects: Iterable[Effect]) -> None:
        """Carry out effects in order. A failure is logged and left to the sweeper (ledger
        calls are re-issued by :meth:`_settle`, workflow stops from ``pending_stop``)."""
        for effect in effects:
            try:
                await self._execute(job, effect)
            except Exception:
                log.exception("job %s: %s failed; the sweeper will retry", job.job_id, effect.kind)

    async def _execute(self, job: _Job, effect: Effect) -> None:
        match effect.kind:
            case EffectKind.RELEASE:
                await self.ledger.release(job.job_id)
            case EffectKind.REFUND:
                with contextlib.suppress(UnknownHold):  # never locked: nothing is owed
                    await self.ledger.refund(job.job_id)
            case EffectKind.DISPUTE:
                await self.ledger.dispute(job.job_id)
            case EffectKind.RESOLVE:
                if job.split is None:
                    raise RuntimeError(f"job {job.job_id} is resolved but no split is recorded")
                await self.ledger.resolve(job.job_id, job.split)
            case (
                EffectKind.CANCEL_WORKFLOW
                | EffectKind.TERMINATE_WORKFLOW
                | EffectKind.TERMINATE_RUN
            ):
                await self._stop_workflow(job, effect.kind, effect.reason)

    async def _stop_workflow(self, job: _Job, kind: EffectKind, reason: str) -> None:
        """Carry out a workflow stop. ``TERMINATE_RUN`` (a cancel's) terminates the open run
        of ``job-<id>``: that is the pinned chain's run whenever the chain is still open (a
        workflow id has one open run at most), and when it is not, whatever the termination
        hits is a run nobody may work on; what it achieved for the money is read back from
        the pinned chain by the caller, never assumed."""
        if self.control is not None:
            ns, wf = job.record.seller_ns, job.workflow_id
            if kind is EffectKind.CANCEL_WORKFLOW:
                await self.control.cancel_workflow(ns, wf, reason)
            elif kind is EffectKind.TERMINATE_RUN:
                # Only the pinned chain: a run outside it is never terminated by a cancel.
                await self.control.terminate_workflow(
                    ns, wf, reason, first_run_id=job.record.run_id or ""
                )
            else:
                await self.control.terminate_workflow(ns, wf, reason)
        if job.pending_stop is kind:
            job.pending_stop, job.pending_stop_reason = None, ""

    async def _settle(self, job: _Job) -> bool:
        """Reconcile the ledger with the job's intended escrow (decisions I6)."""
        hold = await self.ledger.get_hold(job.job_id)
        owed = escrow.reconcile(job.record, hold.state if hold is not None else None)
        if owed:
            await self._execute_effects(job, owed)
            hold = await self.ledger.get_hold(job.job_id)
            if escrow.reconcile(job.record, hold.state if hold is not None else None):
                return False
        want = _ESCROW_HOLD[job.record.escrow]
        actual = hold.state if hold is not None else None
        if actual is not want and not (actual is None and want in _NO_HOLD_OK):
            log.error(
                "job %s: escrow %s but the ledger hold is %s", job.job_id, job.record.escrow, actual
            )
        lock = hold.handles.get(Op.LOCK) if hold is not None else None
        if job.hold_ref is None and lock is not None:
            job.hold_ref = lock.ref
        job.settled = True
        return True

    # ------------------------------------------------------------------ the sweeper

    async def _sweep_forever(self) -> None:
        while True:
            await asyncio.sleep(self.sweep_interval)
            try:
                await self._sweep_once()
            except Exception:
                log.exception("sweep failed")

    async def _sweep_once(self) -> None:
        """Observe open jobs, close review windows, settle the ledger, retry workflow stops,
        expire quotes and stored objects. Job by job: one failure never stops the rest."""
        for job_id in [j.job_id for j in self.jobs.values() if j.record.open]:
            try:
                await self._observe(job_id)
            except Exception:
                log.exception("sweeping job %s failed", job_id)
        now = self.now()
        for job in list(self.jobs.values()):
            r = job.record
            if (
                r.state is m.JobState.COMPLETED
                and r.escrow is m.Escrow.HELD
                and r.review_deadline is not None
                and r.review_deadline <= now
            ):
                try:
                    await self._transition(job.job_id, lambda rec: escrow.tick(rec, now))
                except Exception:
                    log.exception("closing the review window of job %s failed", job.job_id)
        for job in [j for j in self.jobs.values() if not j.settled]:
            try:
                await self._settle(job)
            except Exception:
                log.exception("settling job %s failed", job.job_id)
        for job in [j for j in self.jobs.values() if self._watch_after_cancel(j, now)]:
            try:
                await self._stop_late_start(job)
            except Exception:
                log.warning("watching canceled job %s failed", job.job_id, exc_info=True)
        for job in [j for j in self.jobs.values() if j.pending_stop is not None]:
            try:
                assert job.pending_stop is not None
                await self._stop_workflow(job, job.pending_stop, job.pending_stop_reason)
            except Exception:
                log.warning("job %s: %s still pending", job.job_id, job.pending_stop, exc_info=True)
        self._expire(now)

    def _watch_after_cancel(self, job: _Job, now: datetime) -> bool:
        """A canceled job's hold token stays valid until it expires, and the seller's
        handler checks it offline (decisions I10): the buyer can still start ``execute``
        with it, and a seller whose worker was behind starts the job late. Until the
        deadline (token expiry + grace) the gate keeps stopping any run of ``job-<id>``,
        so nobody works on a refunded job and the buyer gets no result for free."""
        return (
            self.control is not None
            and job.record.state is m.JobState.CANCELED
            and job.pending_stop is None
            and now < job.record.deadline
        )

    async def _stop_late_start(self, job: _Job) -> None:
        """Any run of ``job-<id>`` is stopped, not only the pinned one: nobody works on a
        refunded job, whoever started the run."""
        assert self.control is not None
        obs, _ = await self.control.observe(job.record.seller_ns, job.workflow_id, job.call, None)
        if (
            obs.status is ObservedStatus.RUNNING
            and obs.provenance is not Provenance.OCCUPIED  # the cancel's own placeholder
            and job.pending_stop is None
        ):
            job.pending_stop = EffectKind.CANCEL_WORKFLOW
            job.pending_stop_reason = job.record.reason or "canceled by buyer"

    def _shift(self, obs: Observation) -> Observation:
        """Temporal's timestamps on the gate's clock."""
        off = self._offset
        if not off:
            return obs
        return replace(
            obs,
            started_at=obs.started_at + off if obs.started_at else None,
            closed_at=obs.closed_at + off if obs.closed_at else None,
        )

    async def _observe(self, job_id: str) -> None:
        job = self.jobs.get(job_id)
        if job is None or not job.record.open:
            return
        if self.control is None:
            now = self.now()
            await self._transition(job_id, lambda r: escrow.tick(r, now))
            return
        try:
            obs, result = await self.control.observe(
                job.record.seller_ns, job.workflow_id, job.call, job.record.run_id
            )
        except BuyerLookupError:
            # The buyer's own namespace could not be read. The buyer controls its load, so
            # this never gives up into a blind refund: the buyer could otherwise keep it
            # failing past the deadline and be refunded for a delivered job.
            log.warning("cannot read %s's records of %s; retrying", job.buyer_ns, job.job_id)
            return
        except Exception:
            now = self.now()
            if now < job.record.deadline + self.observe_give_up:
                log.warning("cannot observe %s; retrying", job.workflow_id, exc_info=True)
                return
            await self._transition(job_id, lambda r: escrow.tick(r, now))
            return
        await self._apply_observation(job_id, obs, result)

    async def _apply_observation(self, job_id: str, obs: Observation, result: bytes | None) -> None:
        """Feed one observation of ``job-<id>`` to the escrow rules: the delivery (if this
        one delivers), history progress, and the end of the progress stream."""
        if result is not None:
            obs = replace(
                obs, result_sha256=hashlib.sha256(result).hexdigest(), result_size=len(result)
            )
        obs = self._shift(obs)
        now = self.now()
        job = await self._transition(
            job_id, lambda r: escrow.observe(r, obs, now), result=result, observed=True
        )
        if obs.status is not ObservedStatus.NOT_FOUND:
            await self._history_progress(job)
        if not job.record.open and not job.progress_final:
            job.progress_final = True
            job.notify()

    async def _history_progress(self, job: _Job) -> None:
        """Progress from history metadata (§8.4), recorded when it changed -- of the pinned
        chain only, as the gate does (FIXUPS #34). Before a pin, whatever runs as
        ``job-<id>`` is no one's job: a run the seller started itself, or the placeholder a
        cancel left (decisions I16), is not the buyer's progress."""
        if self.control is None or job.record.run_id is None:
            return
        try:
            summary = await self.control.history_summary(
                job.record.seller_ns, job.workflow_id, job.record.run_id
            )
        except Exception:
            log.warning("history of %s unavailable", job.workflow_id, exc_info=True)
            return
        if summary is None:
            return
        # Keyed on the job's state too: a workflow that completes between the observation
        # and this read gives the same summary on the next pass, when the job is
        # ``completed``, and the stream must still end on an event saying so.
        progress = (job.record.state, summary)
        if progress == job.last_progress:
            return
        job.last_progress = progress
        self._record_event(
            job,
            at=self.now(),
            source=m.ProgressSource.HISTORY,
            state=job.record.state,
            last_event=summary.last_event,
            activities_completed=summary.activities_completed,
            activities_running=list(summary.activities_running),
            continue_as_new_count=summary.continue_as_new_count,
        )

    def _expire(self, now: datetime) -> None:
        for qid in [
            q.quote_id for q in self.quotes.values() if q.job_id is None and q.expires_at <= now
        ]:
            del self.quotes[qid]
        for a in list(self.artifacts.values()):
            if not a.completed:
                if now - a.created_at >= self.orphan_upload_ttl:
                    del self.artifacts[a.key]
                continue
            if a.job_id is None:
                if now - a.created_at >= self.input_retention:
                    del self.artifacts[a.key]
                continue
            job = self.jobs.get(a.job_id)
            row = self.offerings.get(job.record.offering) if job is not None else None
            if job is None or job.escrow_closed_at is None or row is None:
                continue
            if now - job.escrow_closed_at >= row.info.facts.artifact_retention:
                del self.artifacts[a.key]

    # ------------------------------------------------------------------ artifacts & blobs

    def _sign_url(self, method: str, path: str, length: int | None, exp: int) -> str:
        msg = f"{method}\n{path}\n{'-' if length is None else length}\n{exp}".encode()
        sig = hmac.new(self._url_secret, msg, hashlib.sha256).hexdigest()
        query = f"exp={exp}&sig={sig}" if length is None else f"len={length}&exp={exp}&sig={sig}"
        return f"{self.base_url}{path}?{query}"

    def check_url(self, request: Request, method: str, length: int | None) -> None:
        """Verify a URL this gate signed: path, method, signed length, expiry."""
        q = request.query_params
        exp_text, sig = q.get("exp", ""), q.get("sig", "")
        signed_len = q.get("len")
        if length is not None and signed_len != str(length):
            raise GateHTTPError(403, "signature_mismatch", "the URL was not signed for this object")
        msg = f"{method}\n{request.url.path}\n{'-' if length is None else length}\n{exp_text}".encode()
        want = hmac.new(self._url_secret, msg, hashlib.sha256).hexdigest()
        # compare_digest refuses non-ASCII text: check first, so a mangled URL is a 403.
        if not sig.isascii() or not hmac.compare_digest(want, sig):
            raise GateHTTPError(403, "signature_mismatch", "the URL signature does not match")
        if not exp_text.isdigit() or self.now().timestamp() >= int(exp_text):
            raise GateHTTPError(
                403, "url_expired", "the URL has expired; ask the gate for a new one"
            )

    def _exp(self) -> tuple[int, datetime]:
        expires = self.now() + self.url_ttl
        return int(expires.timestamp()), datetime.fromtimestamp(int(expires.timestamp()), UTC)

    def create_artifact(self, who: Principal, req: m.ArtifactCreate) -> m.ArtifactUpload:
        """Presigned part URLs for an upload of ``size`` ciphertext bytes. An output is
        refused above the offering's ``max_output_bytes`` (cumulative per job) before
        anything is stored (acceptance 15)."""
        if _CONTENT_TYPE.fullmatch(req.content_type) is None:
            raise GateHTTPError(
                422, "invalid_request", "content_type must be 1 to 255 printable ASCII characters"
            )
        if req.size < 1:
            raise GateHTTPError(422, "invalid_request", "an artifact has at least one byte")
        if req.job_id is not None:
            job = self.job_for(req.job_id, who.party)
            if who.party != job.record.seller:
                raise GateHTTPError(403, "forbidden", "only the job's seller uploads its outputs")
            if not job.record.open:
                raise GateHTTPError(409, "not_open", f"job is {job.record.state.value}")
            row = self.offerings.get(job.record.offering)
            limit = row.info.facts.max_output_bytes if row is not None else 0
            if job.output_reserved + req.size > limit:
                raise GateHTTPError(
                    413,
                    "too_large",
                    f"the job's outputs may total at most {limit} bytes (max_output_bytes)",
                    {"max_output_bytes": limit, "requested": req.size, "used": job.output_reserved},
                )
            job.output_reserved += req.size
        elif req.size > self.max_input_bytes:
            raise GateHTTPError(
                413,
                "too_large",
                f"an input artifact is at most {self.max_input_bytes} bytes",
                {"max_input_bytes": self.max_input_bytes, "requested": req.size},
            )
        key = secrets.token_hex(16)
        # Like the gate: parts grow (in whole MiB) when the usual size would need more than
        # MAX_PARTS of them, so any size an offering may allow gets a plan.
        part_size = self.part_size
        if -(-req.size // part_size) > m.MAX_PARTS:
            part_size = -(-req.size // m.MAX_PARTS)
            part_size = -(-part_size // 2**20) * 2**20
        if req.size <= part_size:
            plan, upload_id = [req.size], None
        else:
            full, rest = divmod(req.size, part_size)
            plan = [part_size] * full + ([rest] if rest else [])
            upload_id = secrets.token_hex(16)
        self.artifacts[key] = _Artifact(
            key=key,
            owner=who.party,
            size=req.size,
            content_type=req.content_type,
            job_id=req.job_id,
            upload_id=upload_id,
            plan=plan,
            created_at=self.now(),
        )
        exp, expires_at = self._exp()
        parts = [
            m.UploadPart(
                number=n, url=self._sign_url("PUT", f"/_blob/a/{key}/{n}", size, exp), size=size
            )
            for n, size in enumerate(plan, start=1)
        ]
        return m.ArtifactUpload(
            key=key,
            upload_id=upload_id,
            part_size=self.part_size,
            parts=parts,
            expires_at=expires_at,
        )

    def complete_artifact(self, who: Principal, key: str, req: m.ArtifactComplete) -> None:
        if _SHA256.fullmatch(req.ciphertext_sha256) is None:
            raise GateHTTPError(
                422, "invalid_request", "ciphertext_sha256 must be 64 lowercase hex characters"
            )
        a = self.artifacts.get(key)
        if a is None or a.owner != who.party:
            raise GateHTTPError(404, "artifact_not_found", "no such artifact")
        if a.completed:
            if req.ciphertext_sha256 == a.ciphertext_sha256:
                return
            raise GateHTTPError(
                409, "already_completed", "the artifact was completed with other bytes"
            )
        if req.upload_id != a.upload_id:
            raise GateHTTPError(400, "bad_upload", "upload id mismatch")
        if a.job_id is not None:
            # An output is attached to an open job only: once the job closed (canceled,
            # failed, delivered) the seller cannot add to what the buyer was owed.
            job = self.jobs.get(a.job_id)
            if job is None or not job.record.open:
                del self.artifacts[key]
                if job is not None:
                    job.output_reserved = max(0, job.output_reserved - a.size)
                state = job.record.state.value if job is not None else "gone"
                raise GateHTTPError(409, "not_open", f"job is {state}")
        if any(len(a.parts.get(n, b"")) != size for n, size in enumerate(a.plan, start=1)) or len(
            a.parts
        ) != len(a.plan):
            raise GateHTTPError(400, "bad_upload", "parts are missing")
        expected = [a.etags[n] for n in range(1, len(a.plan) + 1)]
        if a.upload_id is not None and req.etags != expected:
            raise GateHTTPError(400, "bad_upload", "etags do not match the stored parts")
        digest = hashlib.sha256(a.data()).hexdigest()
        if not hmac.compare_digest(digest, req.ciphertext_sha256):
            raise GateHTTPError(
                400, "bad_upload", "ciphertext sha256 does not match the stored bytes"
            )
        a.completed, a.ciphertext_sha256 = True, digest

    def download_artifact(self, who: Principal, key: str) -> m.DownloadUrl:
        """Outputs go to the job's seller at any time, and to its buyer once the job
        delivered (state ``completed``: in review or later) -- before that the buyer could
        still cancel for a refund, so an output whose key leaked early (in a progress
        message, say) would be the work for nothing: 409 ``not_delivered``. Inputs go to any
        party (the key is the capability and the bytes are ciphertext)."""
        a = self.artifacts.get(key)
        if a is None or not a.completed:
            raise GateHTTPError(404, "artifact_not_found", "no such artifact")
        if a.job_id is not None:
            job = self.jobs.get(a.job_id)
            if job is None or who.party not in (job.record.buyer, job.record.seller):
                raise GateHTTPError(404, "artifact_not_found", "no such artifact")
            if who.party != job.record.seller and job.record.state is not m.JobState.COMPLETED:
                raise GateHTTPError(
                    409,
                    "not_delivered",
                    f"job is {job.record.state.value}; "
                    "its buyer may fetch its outputs once it delivered",
                )
        exp, expires_at = self._exp()
        return m.DownloadUrl(
            url=self._sign_url("GET", f"/_blob/a/{key}", None, exp),
            size=a.size,
            expires_at=expires_at,
        )

    def create_blob(self, req: m.BlobCreate) -> m.BlobUrl:
        if req.size > MAX_BLOB_BYTES:
            raise GateHTTPError(
                413, "too_large", f"a claim-check blob is at most {MAX_BLOB_BYTES} bytes"
            )
        exp, expires_at = self._exp()
        if req.sha256 in self.blobs:
            url = self._sign_url("GET", f"/_blob/b/{req.sha256}", None, exp)
            return m.BlobUrl(sha256=req.sha256, url=url, exists=True, expires_at=expires_at)
        url = self._sign_url("PUT", f"/_blob/b/{req.sha256}", req.size, exp)
        return m.BlobUrl(sha256=req.sha256, url=url, exists=False, expires_at=expires_at)

    def get_blob(self, sha256: str) -> m.BlobUrl:
        if sha256 not in self.blobs:
            raise GateHTTPError(404, "blob_not_found", "no such blob")
        exp, expires_at = self._exp()
        return m.BlobUrl(
            sha256=sha256,
            url=self._sign_url("GET", f"/_blob/b/{sha256}", None, exp),
            exists=True,
            expires_at=expires_at,
        )


# --------------------------------------------------------------------------- helpers


def _frontend_of(control: TemporalControl | None) -> str:
    """The address clients are told to dial: the control plane's own frontend when it
    has one (a dev server is both), else a name that never resolves."""
    client: Any = getattr(control, "client", None)
    target = getattr(
        getattr(getattr(client, "service_client", None), "config", None), "target_host", None
    )
    return target if isinstance(target, str) and target else "temporal.invalid:7233"


def _digest(secret: str) -> str:
    return hashlib.sha256(secret.encode("ascii", "replace")).hexdigest()


def _is_number(text: str) -> bool:
    return text.isascii() and text.isdigit() and len(text) <= 9 and (text == "0" or text[0] != "0")


def _decode_wire_payload(wire: m.WirePayload) -> Payload:
    """The ``Payload`` a :class:`~kontra.gate_client.models.WirePayload` carries. Relayed,
    never interpreted: only what is not a payload at all, or too big, is refused."""
    if len(wire.b64) > 4 * math.ceil(MAX_PAYLOAD_BYTES / 3):
        raise GateHTTPError(
            413, "too_large", f"a payload is at most {MAX_PAYLOAD_BYTES} bytes; use an Artifact"
        )
    try:
        raw = base64.b64decode(wire.b64, validate=True)
    except (binascii.Error, ValueError):
        raise GateHTTPError(422, "invalid_payload", "payload is not standard base64") from None
    payload = Payload()
    try:
        payload.ParseFromString(raw)
    except DecodeError:
        raise GateHTTPError(
            422, "invalid_payload", "payload is not a serialized temporal Payload"
        ) from None
    return payload


def _parse_quote(payload: Payload | None) -> KontraQuote:
    """The seller's answer must be a plaintext ``json/plain`` ``KontraQuote`` (decisions
    I4); anything else is the seller's bug, reported as 502."""
    if payload is None or payload.metadata.get("encoding") != _JSON_PLAIN:
        raise GateHTTPError(
            502, "invalid_quote", "the seller's quote is not a plaintext JSON payload"
        )
    try:
        return KontraQuote.model_validate_json(payload.data)
    except ValidationError:
        raise GateHTTPError(
            502, "invalid_quote", "the seller's quote is not a KontraQuote"
        ) from None


def _parse_range(header: str | None, size: int) -> tuple[int, int] | None:
    """``bytes=a-`` / ``bytes=a-b`` -> (start, stop); None for no (or an unusable) range."""
    if not header or not header.startswith("bytes=") or "," in header:
        return None
    first, _, last = header[len("bytes=") :].partition("-")
    if not first.isdigit() or (last and not last.isdigit()):
        return None
    start = int(first)
    stop = min(size, int(last) + 1) if last else size
    if start >= size or start >= stop:
        return None
    return start, stop


# --------------------------------------------------------------------------- the app


def _gate_of(request: Request) -> FakeGate:
    gate: FakeGate = request.app.state.gate
    return gate


async def _auth_party(request: Request) -> Principal:
    return _gate_of(request).authenticate(request, allow_worker=False)


async def _auth_party_or_worker(request: Request) -> Principal:
    return _gate_of(request).authenticate(request, allow_worker=True)


async def _auth_fresh(request: Request) -> Principal:
    gate = _gate_of(request)
    return gate.require_fresh(gate.authenticate(request, allow_worker=False))


async def _auth_resolver(request: Request) -> Principal:
    gate = _gate_of(request)
    return gate.require_resolver(gate.authenticate(request, allow_worker=False))


async def _dev_only(request: Request) -> None:
    _gate_of(request).require_dev()


# One per ``auth`` value of the route table (``none`` takes no dependency).
Party = Annotated[Principal, Depends(_auth_party)]
PartyOrWorker = Annotated[Principal, Depends(_auth_party_or_worker)]
Fresh = Annotated[Principal, Depends(_auth_fresh)]
Resolver = Annotated[Principal, Depends(_auth_resolver)]


def _build_app(gate: FakeGate) -> FastAPI:
    """Every row of ``ROUTES``, registered from the table itself (method, path, response
    model, ``operation_id`` = the row's name), plus the gate's own signed blob URLs."""

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncGenerator[None]:
        await gate._startup()  # pyright: ignore[reportPrivateUsage]
        try:
            yield
        finally:
            await gate._shutdown()  # pyright: ignore[reportPrivateUsage]

    app = FastAPI(title="kontra fake gate", lifespan=lifespan)
    app.state.gate = gate

    # ---------------------------------------------------------------- error funnel

    async def gate_error(_: Request, exc: Exception) -> Response:
        assert isinstance(exc, GateHTTPError)
        return _error(exc.status, exc.code, exc.message, exc.detail, exc.headers)

    async def validation_error(_: Request, exc: Exception) -> Response:
        assert isinstance(exc, RequestValidationError)
        errors: list[dict[str, Any]] = []
        msg = "invalid request"
        for e in exc.errors():
            loc = [str(p) for p in e.get("loc", ())]
            text = str(e.get("msg", ""))
            if not errors:
                msg = f"{'.'.join(loc)}: {text}"
            errors.append({"loc": loc, "msg": text, "type": str(e.get("type", ""))})
        return _error(422, "invalid_request", msg, {"errors": errors})

    async def http_error(_: Request, exc: Exception) -> Response:
        assert isinstance(exc, StarletteHTTPException)
        codes = {404: "not_found", 405: "method_not_allowed"}
        code = codes.get(exc.status_code, f"http_{exc.status_code}")
        return _error(exc.status_code, code, str(exc.detail or code), headers=exc.headers)

    async def escrow_error(_: Request, exc: Exception) -> Response:
        assert isinstance(exc, EscrowError)
        return _error(403 if exc.code == "forbidden" else 409, exc.code, str(exc))

    async def crash(_: Request, exc: Exception) -> Response:
        log.error("unhandled error", exc_info=exc)
        return _error(500, "internal", "internal error")

    app.add_exception_handler(GateHTTPError, gate_error)
    app.add_exception_handler(RequestValidationError, validation_error)
    app.add_exception_handler(StarletteHTTPException, http_error)
    app.add_exception_handler(EscrowError, escrow_error)
    app.add_exception_handler(Exception, crash)

    def add(
        name: str, endpoint: Callable[..., Any], *, status_code: int = 200, **kwargs: Any
    ) -> None:
        route = BY_NAME[name]
        app.add_api_route(
            route.path,
            endpoint,
            methods=[route.method],
            name=name,
            operation_id=name,
            response_model=None if route.stream else route.response,
            status_code=status_code,
            **kwargs,
        )

    def no_store(response: Response) -> None:
        response.headers["Cache-Control"] = "no-store"

    # ---------------------------------------------------------------- identity

    async def jwks() -> JSONResponse:
        return JSONResponse(gate.keys.jwks())

    async def device_start(response: Response) -> m.DeviceStart:
        no_store(response)
        return gate.device_start()

    async def token(body: m.TokenRequest, response: Response) -> m.TokenResponse:
        no_store(response)
        return await gate.token(body)

    async def dev_login(body: m.DevLogin, response: Response) -> m.TokenResponse:
        no_store(response)
        return await gate.dev_login(body.handle)

    async def revoke(who: Party) -> Response:
        gate.revoke_session(who.session_id)
        return Response(status_code=204)

    async def temporal_token(
        who: Party, body: m.TemporalTokenRequest, response: Response
    ) -> m.TemporalToken:
        no_store(response)
        return gate.temporal_token(who)

    async def me(who: PartyOrWorker) -> m.Me:
        return gate.me(who)

    async def set_key(who: Fresh, body: m.SetKey) -> m.Me:
        return gate.set_key(who, body.public_key)

    async def party_view(party: str) -> m.PartyView:
        return gate.party_view(party)

    add("jwks", jwks)
    add("device_start", device_start)
    add("token", token)
    add("dev_login", dev_login, dependencies=[Depends(_dev_only)])
    add("revoke", revoke, status_code=204)
    add("temporal_token", temporal_token)
    add("me", me)
    add("set_key", set_key)
    add("party", party_view)

    # ---------------------------------------------------------------- catalog

    async def publish(who: Fresh, body: m.PublishRequest) -> m.CatalogEntry:
        return await gate.publish(who, body.document)

    async def search(
        q: Annotated[str | None, Query(max_length=SEARCH_Q_MAX)] = None,
        cursor: str | None = None,
        limit: Annotated[int, Query(ge=1, le=SEARCH_LIMIT_MAX)] = 20,
    ) -> m.CatalogList:
        return gate.search(q, cursor, limit)

    async def get_offering(party: str, name: str, version: str) -> m.CatalogEntry:
        return gate.resolve_path(party, name, version).entry()

    async def card(party: str, name: str, version: str) -> JSONResponse:
        return JSONResponse(gate.resolve_path(party, name, version).card)

    async def by_endpoint(endpoint: str) -> m.CatalogEntry:
        return gate.by_endpoint(endpoint)

    add("publish", publish)
    add("search", search)
    add("get_offering", get_offering)
    add("card", card)
    add("by_endpoint", by_endpoint)

    # ---------------------------------------------------------------- quotes & jobs

    async def quote(who: Party, body: m.QuoteRequest) -> m.QuoteView:
        return await gate.quote(who, body)

    async def hire(who: Party, body: m.HireRequest) -> m.HireView:
        return await gate.hire(who, body)

    async def jobs(
        who: PartyOrWorker,
        as_: Annotated[Literal["buyer", "seller"], Query(alias="as")] = "buyer",
        state: m.JobState | None = None,
        cursor: str | None = None,
        limit: Annotated[int, Query(ge=1, le=LIST_LIMIT_MAX)] = 50,
    ) -> m.JobList:
        return gate.list_jobs(who.party, as_, state, cursor, limit)

    async def job(who: PartyOrWorker, job_id: str) -> m.JobView:
        return gate.job_for(job_id, who.party).view()

    async def seller_job(who: PartyOrWorker, job_id: str) -> m.SellerJobView:
        return gate.seller_view(who, job_id)

    async def accept(who: Party, job_id: str, body: m.AcceptRequest) -> m.JobView:
        return await gate.accept(who, job_id)

    async def reject(who: Party, job_id: str, body: m.RejectRequest) -> m.JobView:
        return await gate.reject(who, job_id, body.reason)

    async def cancel(who: Party, job_id: str) -> m.JobView:
        return await gate.cancel(who, job_id)

    async def start(who: Party, job_id: str, body: m.StartRequest) -> m.JobView:
        return await gate.start(who, job_id, body.input)

    async def result(who: Party, job_id: str) -> m.ResultView:
        return gate.result(who, job_id)

    async def progress(
        who: Party, job_id: str, after: Annotated[int, Query(ge=0)] = 0
    ) -> StreamingResponse:
        target = gate.job_for(job_id, who.party)
        return StreamingResponse(
            gate.progress_stream(target, after),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )

    async def post_progress(who: PartyOrWorker, job_id: str, body: m.ProgressPost) -> Response:
        gate.post_progress(who, job_id, body)
        return Response(status_code=204)

    add("quote", quote)
    add("hire", hire)
    add("jobs", jobs)
    add("job", job)
    add("seller_job", seller_job)
    add("accept", accept)
    add("reject", reject)
    add("cancel", cancel)
    add("start", start)
    add("result", result)
    add("progress", progress, response_class=StreamingResponse)
    add("post_progress", post_progress, status_code=204)

    # ---------------------------------------------------------------- artifacts & blobs

    async def create_artifact(who: PartyOrWorker, body: m.ArtifactCreate) -> m.ArtifactUpload:
        return gate.create_artifact(who, body)

    async def complete_artifact(who: PartyOrWorker, key: str, body: m.ArtifactComplete) -> Response:
        gate.complete_artifact(who, key, body)
        return Response(status_code=204)

    async def download_artifact(who: PartyOrWorker, key: str) -> m.DownloadUrl:
        return gate.download_artifact(who, key)

    async def create_blob(who: PartyOrWorker, body: m.BlobCreate) -> m.BlobUrl:
        return gate.create_blob(body)

    async def get_blob(who: PartyOrWorker, sha256: str) -> m.BlobUrl:
        return gate.get_blob(sha256)

    add("create_artifact", create_artifact)
    add("complete_artifact", complete_artifact, status_code=204)
    add("download_artifact", download_artifact)
    add("create_blob", create_blob)
    add("get_blob", get_blob)

    # ---------------------------------------------------------------- money & disputes

    async def balance(who: Party, currency: str = "USD") -> m.Balance:
        return await gate.balance(who.party, currency)

    async def topup(who: Party, body: m.Topup) -> m.TopupResult:
        return await gate.topup(who.party, body)

    async def disputes(
        who: Resolver,
        cursor: str | None = None,
        limit: Annotated[int, Query(ge=1, le=LIST_LIMIT_MAX)] = 50,
    ) -> m.JobList:
        return gate.disputes(cursor, limit)

    async def resolve(who: Resolver, job_id: str, body: m.ResolveRequest) -> m.JobView:
        return await gate.resolve(who, job_id, body)

    async def health() -> JSONResponse:
        return JSONResponse({"status": "ok", "dev": gate.dev})

    add("balance", balance)
    add("topup", topup)
    add("disputes", disputes)
    add("resolve", resolve)
    add("health", health)

    # ---------------------------------------------------------------- the "bucket"

    async def read_exact(request: Request, length: int) -> bytes:
        declared = request.headers.get("content-length")
        if declared is None:
            raise GateHTTPError(411, "length_required", "Content-Length is required")
        if declared != str(length):
            raise GateHTTPError(
                403, "length_mismatch", f"this URL is signed for exactly {length} bytes"
            )
        data = bytearray()
        async for chunk in request.stream():
            data += chunk
            if len(data) > length:
                raise GateHTTPError(400, "length_mismatch", f"more than the signed {length} bytes")
        if len(data) != length:
            raise GateHTTPError(400, "length_mismatch", f"fewer than the signed {length} bytes")
        return bytes(data)

    async def put_part(request: Request, key: str, number: int) -> Response:
        a = gate.artifacts.get(key)
        if a is None or not 1 <= number <= len(a.plan) or a.completed:
            raise GateHTTPError(404, "no_such_upload", "no such upload")
        size = a.plan[number - 1]
        gate.check_url(request, "PUT", size)
        data = await read_exact(request, size)
        etag = '"' + hashlib.md5(data, usedforsecurity=False).hexdigest() + '"'
        a.parts[number], a.etags[number] = data, etag
        return Response(status_code=200, headers={"ETag": etag})

    async def get_object(request: Request, key: str) -> Response:
        gate.check_url(request, "GET", None)
        a = gate.artifacts.get(key)
        if a is None or not a.completed:
            raise GateHTTPError(404, "no_such_key", "no such object")
        data = a.data()
        span = _parse_range(request.headers.get("range"), len(data))
        if span is None:
            return Response(
                data, media_type="application/octet-stream", headers={"Accept-Ranges": "bytes"}
            )
        start, stop = span
        return Response(
            data[start:stop],
            status_code=206,
            media_type="application/octet-stream",
            headers={
                "Content-Range": f"bytes {start}-{stop - 1}/{len(data)}",
                "Accept-Ranges": "bytes",
            },
        )

    async def put_blob(request: Request, sha256: str) -> Response:
        # The length is the one this URL was signed for (as S3 signs Content-Length), not
        # whatever the latest POST /v1/blobs for this hash declared: another party asking
        # for the same hash cannot invalidate an upload already handed out.
        signed = request.query_params.get("len", "")
        if not (signed.isascii() and signed.isdigit() and len(signed) <= 12):
            raise GateHTTPError(403, "signature_mismatch", "the URL was not signed for an upload")
        size = int(signed)
        gate.check_url(request, "PUT", size)
        if sha256 in gate.blobs:
            return Response(status_code=200)  # content-addressed: already stored, unchanged
        data = await read_exact(request, size)
        if hashlib.sha256(data).hexdigest() != sha256:
            raise GateHTTPError(400, "bad_digest", "the bytes do not hash to the blob's sha256")
        gate.blobs[sha256] = data
        return Response(status_code=200)

    async def get_blob_bytes(request: Request, sha256: str) -> Response:
        gate.check_url(request, "GET", None)
        data = gate.blobs.get(sha256)
        if data is None:
            raise GateHTTPError(404, "no_such_key", "no such blob")
        return Response(data, media_type="application/octet-stream")

    app.add_api_route("/_blob/a/{key}/{number}", put_part, methods=["PUT"], include_in_schema=False)
    app.add_api_route("/_blob/a/{key}", get_object, methods=["GET"], include_in_schema=False)
    app.add_api_route("/_blob/b/{sha256}", put_blob, methods=["PUT"], include_in_schema=False)
    app.add_api_route("/_blob/b/{sha256}", get_blob_bytes, methods=["GET"], include_in_schema=False)
    return app


# --------------------------------------------------------------------------- the factory


class _CallableModule(types.ModuleType):
    """``kontra.testing.fake_gate`` is both this module and the factory
    ``kontra.testing.fake_gate(...)``: importing a submodule rebinds the package attribute
    of the same name to the module, so the module itself answers calls like the factory."""

    def __call__(self, control: TemporalControl | None = None, **options: Any) -> FakeGate:
        return FakeGate(control, **options)


sys.modules[__name__].__class__ = _CallableModule
