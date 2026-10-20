"""An in-process gate (seam ②) that behaves like the real one, for tests with no containers.

:class:`FakeGate` serves every row of :data:`kontra.gate_client.routes.ROUTES` from memory:
identity (dev login, the device flow, magic links to an allowlist, API keys, rotating
refresh tokens, ES256 tokens and a JWKS, "Confirm it's you" by mail or by a passkey -- whose
answers :mod:`kontra._passkeys` verifies, as on the platform), parties with Temporal
namespaces, the devices that read a party's jobs and the job-key wraps copied to them
(decisions I25, with :mod:`kontra._jobkeys`'s proofs), the catalog (contract validation,
opaque endpoint names,
the endpoint created through the control plane), the Workers view (Temporal's task-queue
stats of a seller's own version), the quote relay, hire with a signed hold
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
flow can approve itself; mail is not sent but appended to :attr:`FakeGate.outbox`, and the
magic link's page is :meth:`FakeGate.use_magic_link` (the human typing the code their screen
shows: a device's, to sign in, or a confirmation's, to confirm it's them -- decisions I23).
A passkey's answers come from :class:`kontra.testing.SoftAuthenticator` rather than a browser.
"""

# No ``from __future__ import annotations`` here: FastAPI reads the route functions'
# annotations (bodies, auth dependencies) at runtime.

import asyncio
import base64
import binascii
import contextlib
import hashlib
import hmac
import ipaddress
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
import warnings
from collections import deque
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any, Literal, TypeVar
from urllib.parse import parse_qs, urlsplit

import jwt
import uvicorn
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from google.protobuf.message import DecodeError
from jwt.algorithms import ECAlgorithm
from pydantic import JsonValue, ValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from temporalio.api.common.v1 import Payload

from kontra import _jobkeys as jk
from kontra import _passkeys as pk
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
from kontra.workers import offline, workers_view

__all__ = [
    "AUD_API",
    "AUD_HOLD",
    "AUD_TEMPORAL",
    "HOLD_HEADER",
    "SUBJECT_DEVICE_ADDED",
    "SUBJECT_DEVICE_REMOVED",
    "FakeGate",
    "GateHTTPError",
    "MagicOutcome",
    "Principal",
    "SentMail",
]

log = logging.getLogger("kontra.testing.fake_gate")

AUD_API = "kontra:api"
AUD_TEMPORAL = "kontra:temporal"
AUD_HOLD = "kontra:hold"
#: The Nexus header carrying the hold token (D10).
HOLD_HEADER = "kontra-hold"
QUOTE_OPERATION = "quote"
EXECUTE_OPERATION = "execute"
DEVICE_GRANT = m.DEVICE_GRANT
#: RFC 8628 §6.1: no vowels, no look-alikes.
USER_CODE_ALPHABET = m.USER_CODE_ALPHABET
#: A typed user code before it is normalised (``bcdf-ghjk``, ``BCDFGHJK``), as the page reads it.
_TYPED_USER_CODE = re.compile(r"[A-Za-z]{4}-?[A-Za-z]{4}", re.ASCII)
DEVICE_TTL = timedelta(minutes=10)
#: A magic link is good for this long, once, and for this many wrong user codes.
MAGIC_LINK_TTL = timedelta(minutes=10)
MAGIC_LINK_ATTEMPTS = 3
#: "Confirm it's you" (decisions I23): a confirmation's link lives as long as a sign-in
#: link; the asking screen polls this often; one sign-in may ask this many times an hour.
CONFIRM_LINK_TTL = MAGIC_LINK_TTL
CONFIRM_INTERVAL = 3
CONFIRM_RATE_PER_SESSION = 5
CONFIRM_RATE_WINDOW = timedelta(hours=1)
#: Settled confirmations are forgotten this long after they expire.
CONFIRM_PURGE_AFTER = timedelta(days=1)
_SESSION_PREFIX = re.compile(r"[0-9a-f]{8,32}", re.ASCII)
_CONFIRM_ID = re.compile(r"cfm_[0-9a-f]{32}", re.ASCII)
#: How a sign-in proved who it is (``via``), in words, as the console and the mail say it.
VIA_WORDS = {
    "device:email": "a link by email",
    "device:github": "GitHub",
    "device:dev": "a development handle",
    "dev-login": "a development handle",
}
#: The sign-ins a ``KONTRA_DEV=1`` gate confirms at once (development handles).
_DEV_VIA = frozenset({"dev-login", "device:dev"})
SUBJECT_SIGN_IN = "Your Kontra sign-in link"
SUBJECT_CONFIRM = "Confirm it's you on Kontra"
SUBJECT_PASSKEY_ADDED = pk.SUBJECT_ADDED
SUBJECT_PASSKEY_REVOKED = pk.SUBJECT_REVOKED
SUBJECT_PASSKEY_REMOVED = pk.SUBJECT_REMOVED
SUBJECT_DEVICE_ADDED = jk.SUBJECT_DEVICE_ADDED
SUBJECT_DEVICE_REMOVED = jk.SUBJECT_DEVICE_REMOVED
#: ``GET /v1/me/job-keys`` serves at most this many jobs a page.
JOB_KEYS_LIMIT_MAX = 100
#: Passkeys (decisions I24), the platform's limits: one sign-in asks for at most this many
#: ceremonies of one purpose an hour, and gets none once this many of its answers were
#: refused in the last hour.
PASSKEY_OPTIONS_PER_HOUR = 20
PASSKEY_FAILURES_PER_HOUR = 10
PASSKEY_RATE_WINDOW = timedelta(hours=1)
#: ``GET /v1/passkeys`` lists every live passkey, then the latest revoked ones, this many in all.
PASSKEYS_LISTED = 50
#: Settled ceremonies are forgotten this long after they expire.
CEREMONY_PURGE_AFTER = timedelta(days=1)
#: The operator's ``--passkey``: 8 to 36 characters of a passkey's id.
_PASSKEY_PREFIX = re.compile(r"pky_[0-9a-f]{4,32}", re.ASCII)
#: ``last_used_at`` of an API key moves at most this often.
KEY_USE_RESOLUTION = timedelta(minutes=1)
#: ``GET /v1/keys`` lists every live key, then the latest revoked ones, this many in all.
KEYS_LISTED = 100
#: ``GET /v1/sessions`` lists at most this many sign-ins.
SESSIONS_LISTED = 100
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
#: How long a revoked session stays on ``GET /v1/auth/revocations``: past any worker token's
#: life, with room for the clocks of the gate and Temporal to disagree.
REVOCATION_WINDOW = WORKER_TTL + timedelta(minutes=5)
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
_SECRET = re.compile(r"[A-Za-z0-9_-]{43}", re.ASCII)
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
    #: The session is an API key's: it never makes changes and never confirms (I19, I23).
    api_key: bool = False
    #: How the session's person proved who they are (``device:email``, ``dev-login``...).
    via: str = ""
    #: When the person last confirmed it's them for this session (None: never; the sign-in
    #: itself, ``auth_time``, counts as one).
    confirmed_at: datetime | None = None
    #: When this session was last asked to confirm again -- by the operator
    #: (:meth:`FakeGate.require_confirmation`) or the revocation of a passkey (None: never).
    confirm_required_at: datetime | None = None


@dataclass
class _PartyKey:
    """One recipient of a party. Revoking sets ``revoked_at`` and keeps the row: an
    in-flight job encrypted to a revoked seller key is still that seller's to decrypt, and
    a dispute may need the key on record (decisions I7)."""

    label: str
    public_key: str
    added_at: datetime
    revoked_at: datetime | None = None


@dataclass
class _Party:
    handle: str
    namespace: str
    created_at: datetime
    keys: list[_PartyKey] = field(default_factory=list)

    def recipients(self) -> list[str]:
        """The unrevoked keys, oldest first: what a counterparty encrypts to."""
        return [k.public_key for k in self.keys if k.revoked_at is None]


@dataclass
class _Session:
    id: str
    party: str
    auth_time: datetime
    via: str
    revoked_at: datetime | None = None
    created_at: datetime | None = None
    #: The last completed confirmation of this session (decisions I23); never a key's.
    confirmed_at: datetime | None = None
    #: When it was asked to confirm again -- by the operator (``gate.admin
    #: require-confirmation``) or the revocation of a passkey (decisions I24): no change until
    #: a later confirmation. Never a key's.
    confirm_required_at: datetime | None = None

    @property
    def revoked(self) -> bool:
        return self.revoked_at is not None


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
    #: How the human approved it: the session it opens records this.
    via: str = "device"


@dataclass
class _ApiKey:
    id: str
    party: str
    label: str
    session_id: str
    created_at: datetime
    last_used_at: datetime | None = None
    revoked_at: datetime | None = None

    def view(self) -> m.ApiKeyView:
        return m.ApiKeyView(
            id=self.id,
            label=self.label,
            created_at=self.created_at,
            last_used_at=self.last_used_at,
            revoked_at=self.revoked_at,
        )


@dataclass
class _MagicLink:
    email: str
    #: A sign-in link: the device authorization it approves. None for a confirmation link.
    device_user_code: str | None
    expires_at: datetime
    #: Who asked for it (the requester's network, :func:`_requester_network`) and when: the
    #: mail limits count by these, over both kinds of link.
    client: str = ""
    created_at: datetime | None = None
    consumed_at: datetime | None = None
    attempts: int = 0
    #: A confirmation link: the confirmation it completes. None for a sign-in link.
    confirmation_id: str | None = None


@dataclass
class _Confirmation:
    """One "Confirm it's you" a person's session asked for (``POST /v1/auth/confirm``).
    ``user_code`` is what that session's screen shows; the mail carries a link to it, never
    the code. Completing it confirms ``session_id`` and no other session."""

    id: str
    session_id: str
    user_code: str
    status: Literal["pending", "confirmed", "burned"]
    client: str
    created_at: datetime
    expires_at: datetime
    confirmed_at: datetime | None = None


@dataclass(slots=True)
class _PasskeyUser:
    """A party's WebAuthn user handle (``user.id`` of every registration): 32 random bytes,
    never derived from the handle or an address, made by its first ceremony. One per party,
    so an authenticator keeps one Kontra passkey per account."""

    party: str
    user_handle: bytes
    created_at: datetime


@dataclass
class _Passkey:
    """A passkey that confirms a party's sign-ins (decisions I24). ``id`` (``pky_`` + 32
    random hex) is the API's handle, never derived from ``credential_id``, which is unique
    across the gate, revoked ones included. ``sign_count`` is the last use count seen."""

    id: str
    party: str
    credential_id: bytes
    public_key: bytes
    algorithm: int
    sign_count: int
    transports: tuple[pk.Transport, ...]
    aaguid: str
    label: str
    backup_eligible: bool
    backed_up: bool
    created_at: datetime
    created_session_id: str | None
    last_used_at: datetime | None = None
    revoked_at: datetime | None = None
    revoked_reason: m.PasskeyRevokedBy | None = None

    def known(self) -> pk.Known:
        return pk.Known(
            self.credential_id,
            self.public_key,
            self.sign_count,
            self.backup_eligible,
            self.transports,
        )

    def view(self) -> m.PasskeyView:
        return m.PasskeyView(
            id=self.id,
            label=self.label,
            created_at=self.created_at,
            added_by=self.created_session_id[:12] if self.created_session_id else None,
            last_used_at=self.last_used_at,
            backed_up=self.backed_up,
            transports=list(self.transports),
            revoked_at=self.revoked_at,
            revoked_by=self.revoked_reason,
        )


@dataclass
class _Ceremony:
    """One WebAuthn ceremony: its challenge, bound to the session that asked and to its
    purpose, used once (``used_at``, with the outcome) whatever the outcome, before
    ``expires_at``."""

    id: str
    session_id: str
    purpose: pk.Purpose
    challenge: bytes
    created_at: datetime
    expires_at: datetime
    used_at: datetime | None = None
    outcome: Literal["ok", "rejected", "cloned"] | None = None


@dataclass(slots=True)
class _Handoff:
    """A sign-in the link's page just approved, waiting for that browser's console to trade
    it for its own session (the ``handoff`` grant): once, within ``HANDOFF_TTL_SECONDS``."""

    party: str
    auth_time: datetime
    via: str
    expires_at: datetime
    consumed: bool = False


@dataclass
class _DeviceKey:
    """One device of a party (decisions I25): a key that reads the jobs the party hires.
    Removing it sets ``revoked_at`` and keeps the row, so the key is never added again. A
    *browser* the gate retires -- idle for ``DEVICE_IDLE_SECONDS``, or the one used least
    recently when a new device needs room -- is not live either, but may come back."""

    party: str
    public_key: str
    label: str
    kind: m.DeviceKind
    added_at: datetime
    added_by: str | None
    last_used_at: datetime | None = None
    revoked_at: datetime | None = None
    retired_at: datetime | None = None

    @property
    def last_seen(self) -> datetime:
        """When it was last used, else added: what idleness and making room go by."""
        return self.last_used_at or self.added_at

    def live(self, now: datetime) -> bool:
        if self.revoked_at is not None or self.retired_at is not None:
            return False
        idle = timedelta(seconds=m.DEVICE_IDLE_SECONDS)
        return self.kind != "browser" or self.last_seen > now - idle

    def view(self, *, hidden: bool = False) -> m.DeviceView:
        """``hidden``: as an API key's session reads it -- the key, not the names."""
        return m.DeviceView(
            public_key=self.public_key,
            label=m.HIDDEN_DEVICE_LABEL if hidden else self.label,
            kind=self.kind,
            added_at=self.added_at,
            added_by=self.added_by[:12] if self.added_by and not hidden else None,
            last_used_at=None if hidden else self.last_used_at,
        )


@dataclass(frozen=True, slots=True)
class _KeyWrapRow:
    """A job key's copy for one device, as stored: ciphertext only, never updated."""

    job_id: str
    recipient: str
    wrap: bytes
    wrapped_at: datetime
    wrapped_by: str

    def view(self) -> m.KeyWrap:
        return m.KeyWrap(
            recipient=self.recipient,
            wrap=base64.b64encode(self.wrap).decode("ascii"),
            wrapped_at=self.wrapped_at,
        )


@dataclass(frozen=True, slots=True)
class SentMail:
    """A mail the gate would have sent (:attr:`FakeGate.outbox`). A sign-in mail carries the
    link and never the device's user code: the human must type that from their own screen."""

    to: str
    subject: str
    text: str
    link: str
    sent_at: datetime


#: What the magic link's page answers the human who typed a user code into it: a sign-in
#: link ``approved`` (or ``handle_taken``), a confirmation link ``confirmed``.
MagicOutcome = Literal["approved", "confirmed", "wrong_code", "gone", "handle_taken"]


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

    def entry(self, recipients: Sequence[str]) -> m.CatalogEntry:
        """``recipients``: the seller party's unrevoked keys NOW. They are catalog data,
        not contract data -- the document seals no key (spec v2, decisions I7) -- so the
        caller reads them from the party row rather than from the sealed facts."""
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
            recipients=list(recipients),
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
    #: A workflow stop is being carried out: the sweeper does not issue it a second time.
    stopping: bool = False
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

    def view(self, *, buyer_key: bool = False) -> m.JobView:
        """``buyer_key``: with the job's own key (``?include=buyer_key``); absent otherwise."""
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
            buyer_key=self.buyer_key if buyer_key else None,
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
    requested (else :meth:`approve_device`, or a magic link); by default only a ``dev`` gate
    does, since approving any requested handle is logging in as anyone, which is what dev
    login is. ``signup``: ``allowlist`` mails sign-in links only to the addresses of
    :meth:`allow_email`; ``open`` to any address (the default on a ``dev`` gate, as the
    platform's ``KONTRA_SIGNUP``). Mail goes to :attr:`outbox`. ``github``: the device page
    signs in with GitHub (``GateInfo.github_sign_in``; :meth:`approve_device` is the human
    doing it). Without it, and off a dev gate, the device page approves nothing, as on
    production: email is the way in.
    ``magic_link_rate_per_email``: sign-in link requests (429 past it) and mails one address
    gets from one client an hour; ``magic_link_rate_per_client``: requests one client makes
    an hour; ``magic_link_max_per_email``: mails one address gets an hour from every client
    together (past it the request is answered as usual and no mail goes out). Confirmation
    mail (decisions I23) counts against all three, like a sign-in link.
    ``confirm_ttl``: how long a sign-in or a confirmation lets a person make changes (the
    platform's ``KONTRA_CONFIRM_TTL``); ``freshness`` is its old name, deprecated.
    ``confirm_interval``: how often the asking screen is told to poll a confirmation.
    ``passkeys``: a passkey may confirm it's you (decisions I24); False is the platform's
    ``KONTRA_PASSKEYS=0`` (every ceremony 409 ``passkeys_unavailable``, stored passkeys
    kept). ``passkey_rp_id`` (``KONTRA_PASSKEY_RP_ID``): the WebAuthn RP ID; by default the
    host of :attr:`base_url`, as the platform derives it from the URL it serves its own
    console at, and none -- passkeys off -- when that host is an IP address (as after
    :meth:`serve`). ``passkey_rp_name`` (``KONTRA_PASSKEY_RP_NAME``): what authenticators
    show. ``passkey_origins``: the console's origins (``KONTRA_CONSOLE_ORIGINS``; default
    :attr:`base_url`), of which only those on the RP ID are taken (:attr:`passkey_origins`).

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
        confirm_ttl: timedelta = timedelta(hours=1),
        freshness: timedelta | None = None,
        confirm_interval: int = CONFIRM_INTERVAL,
        quote_ttl: timedelta = timedelta(minutes=10),
        quote_rate_per_min: int = 60,
        quote_timeout: float = 10.0,
        device_auto_approve: bool | None = None,
        device_interval: int = 1,
        github: bool = False,
        signup: Literal["allowlist", "open"] | None = None,
        magic_link_rate_per_email: int = 5,
        magic_link_rate_per_client: int = 30,
        magic_link_max_per_email: int = 20,
        magic_link_rate_window: timedelta = timedelta(hours=1),
        part_size: int = 16 * 1024 * 1024,
        max_input_bytes: int = 5 * 1000**3,
        url_ttl: timedelta = timedelta(hours=1),
        progress_rate_per_s: int = 5,
        input_retention: timedelta = timedelta(days=7),
        orphan_upload_ttl: timedelta = timedelta(hours=24),
        keepalive: float = 5.0,
        passkeys: bool = True,
        passkey_rp_id: str | None = None,
        passkey_rp_name: str = pk.DEFAULT_RP_NAME,
        passkey_origins: Sequence[str] | None = None,
    ) -> None:
        if part_size <= 0:
            raise ValueError("part_size must be positive")
        if passkey_rp_id is not None and not pk.is_rp_id(passkey_rp_id):
            raise ValueError(
                "passkey_rp_id (KONTRA_PASSKEY_RP_ID) must be a lower-case host name, not an "
                "IP address, that is localhost or has a dot"
            )
        if not pk.is_rp_name(passkey_rp_name):
            raise ValueError("passkey_rp_name is 1 to 64 printable ASCII characters")
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
        if freshness is not None:
            warnings.warn(
                "FakeGate(freshness=) is deprecated: it is confirm_ttl now (how long a sign-in "
                "or a confirmation lets a person make changes)",
                DeprecationWarning,
                stacklevel=2,
            )
            confirm_ttl = freshness
        self.confirm_ttl = confirm_ttl
        self.confirm_interval = confirm_interval
        self.quote_ttl = quote_ttl
        self.quote_rate_per_min = quote_rate_per_min
        self.quote_timeout = quote_timeout
        self.device_auto_approve = dev if device_auto_approve is None else device_auto_approve
        self.device_interval = device_interval
        self.github = github
        self.signup: Literal["allowlist", "open"] = signup or ("open" if dev else "allowlist")
        self.magic_link_rate_per_email = magic_link_rate_per_email
        self.magic_link_rate_per_client = magic_link_rate_per_client
        self.magic_link_max_per_email = magic_link_max_per_email
        self.magic_link_rate_window = magic_link_rate_window
        self.part_size = part_size
        self.max_input_bytes = max_input_bytes
        self.url_ttl = url_ttl
        self.progress_rate_per_s = progress_rate_per_s
        self.input_retention = input_retention
        self.orphan_upload_ttl = orphan_upload_ttl
        self.keepalive = keepalive
        self.passkeys = passkeys
        self.passkey_rp_id = passkey_rp_id
        self.passkey_rp_name = passkey_rp_name
        self._passkey_origins = None if passkey_origins is None else tuple(passkey_origins)

        self.keys = _KeyRing()
        self._url_secret = secrets.token_bytes(32)
        self.parties: dict[str, _Party] = {}
        self.sessions: dict[str, _Session] = {}
        self._refresh: dict[str, _Refresh] = {}
        self._devices: dict[str, _Device] = {}
        #: Lowercased addresses that may sign in by email (``signup="allowlist"``).
        self.allowed_emails: set[str] = set()
        #: ``(provider, subject)`` -> party: who an email address signs in as.
        self.identities: dict[tuple[str, str], str] = {}
        #: Every mail the gate sent, oldest first.
        self.outbox: list[SentMail] = []
        self._magic: dict[str, _MagicLink] = {}  # by the link token's sha256
        self._confirmations: dict[str, _Confirmation] = {}  # by id
        self._handoffs: dict[str, _Handoff] = {}  # by the handoff's sha256
        self._passkey_users: dict[str, _PasskeyUser] = {}  # by party
        self._passkey_rows: dict[str, _Passkey] = {}  # by id
        self._ceremonies: dict[str, _Ceremony] = {}  # by id
        #: Every handoff the link's page issued, oldest first (what the platform's page puts
        #: in the console URL's fragment).
        self.handoffs: list[str] = []
        self._magic_rate: dict[str, deque[datetime]] = {}
        self.api_keys: dict[str, _ApiKey] = {}  # by id
        self._api_key_ids: dict[str, str] = {}  # the key's sha256 -> id
        self.offerings: dict[str, _Offering] = {}
        self._by_endpoint: dict[str, str] = {}
        self.quotes: dict[str, _Quote] = {}
        self._quote_rate: dict[str, tuple[datetime, int]] = {}
        self.jobs: dict[str, _Job] = {}
        #: Devices by (party, public key), removed ones included (decisions I25).
        self.devices: dict[tuple[str, str], _DeviceKey] = {}
        #: Job-key wraps by (job id, device key): first write wins, never updated.
        self.job_key_wraps: dict[tuple[str, str], _KeyWrapRow] = {}
        #: The HMAC key of job-key challenges (the platform derives its own from
        #: ``KONTRA_SIGNING_KEYS``).
        self.job_key_proof_key = jk.proof_key(secrets.token_bytes(32))
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
        session = _Session(secrets.token_hex(16), party, auth_time, via, created_at=self.now())
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
        return Principal(
            party,
            sid,
            session.auth_time,
            "worker" if worker else "api",
            api_key=session.via == "api_key",
            via=session.via,
            confirmed_at=session.confirmed_at,
            confirm_required_at=session.confirm_required_at,
        )

    @property
    def freshness(self) -> timedelta:
        """Deprecated: :attr:`confirm_ttl`."""
        return self.confirm_ttl

    def confirmed_until(self, who: Principal | _Session) -> datetime | None:
        """Until when ``who``'s session may make changes: its sign-in or its last completed
        confirmation, whichever is later, plus :attr:`confirm_ttl` (decisions I23). None --
        no change at any time -- while the operator's request
        (:meth:`require_confirmation`) is not followed by a confirmation."""
        required, confirmed = who.confirm_required_at, who.confirmed_at
        if required is not None and (confirmed is None or confirmed <= required):
            return None
        since = max(who.auth_time, confirmed or who.auth_time)
        return since + self.confirm_ttl

    def require_human(self, who: Principal) -> Principal:
        """A person's session, of any age (the reads of keys and sign-ins). An API key's
        session never qualifies, however new the key."""
        if who.api_key:
            raise GateHTTPError(
                403,
                "fresh_login_required",
                "an API key cannot do this (publish, change recipients, see or manage API "
                "keys and sign-ins, resolve disputes); a person's sign-in can (`kontra login`)",
            )
        return who

    def require_confirmed(self, who: Principal) -> Principal:
        """A person's session confirmed within :attr:`confirm_ttl` (a sign-in counts): every
        change. The API-key test comes first; the 403 says how this session can confirm, and
        comes before the route does anything, so a client confirms and sends it again."""
        self.require_human(who)
        until = self.confirmed_until(who)
        if until is not None and self.now() < until:
            return who
        raise self._confirmation_required(who, _confirmation_required_message(self.confirm_ttl))

    #: Deprecated: :meth:`require_confirmed`.
    require_fresh = require_confirmed

    def require_recent(self, who: Principal, message: str = pk.RECENT_REQUIRED) -> Principal:
        """``recent`` (decisions I24): a person's session confirmed -- by its sign-in or a
        completed confirmation -- less than ``RECENT_CONFIRMATION_SECONDS`` (five minutes)
        ago, with no mark since: adding a passkey, which can confirm every change after it,
        and adding a device, which gets the keys of past jobs (I25). The API-key test comes
        first, as for ``fresh``; the 403 is ``fresh``'s, with the route's own ``message``."""
        self.require_human(who)
        until = self.confirmed_until(who)
        since = max(who.auth_time, who.confirmed_at or who.auth_time)
        now = self.now()
        recent = now - since < timedelta(seconds=m.RECENT_CONFIRMATION_SECONDS)
        if until is not None and now < until and recent:
            return who
        raise self._confirmation_required(who, message)

    def _confirmation_required(self, who: Principal, message: str) -> GateHTTPError:
        need = m.ConfirmationRequired(
            confirm_with=self.confirm_channel(who),
            confirm_ttl_seconds=int(self.confirm_ttl.total_seconds()),
        )
        return GateHTTPError(403, m.CONFIRMATION_REQUIRED, message, need.model_dump(mode="json"))

    def require_resolver(self, who: Principal) -> Principal:
        """A resolver's confirmed human session: splitting escrow is not for an API key or an
        unconfirmed login (a stolen refresh token). The list is checked first, so anyone else
        gets ``not_resolver`` and a resolver learns that a confirmation is what is missing."""
        if who.party not in self.resolvers:
            raise GateHTTPError(403, "not_resolver", "only a dispute resolver may do this")
        return self.require_confirmed(who)

    def require_resolver_human(self, who: Principal) -> Principal:
        """A resolver's own login, of any age: reading the queue asks no confirmation."""
        if who.party not in self.resolvers:
            raise GateHTTPError(403, "not_resolver", "only a dispute resolver may do this")
        return self.require_human(who)

    def require_dev(self) -> None:
        if not self.dev:
            raise GateHTTPError(404, "not_found", "Not Found")

    def revoke_session(self, session_id: str) -> None:
        """Revoke a session; an API key's session takes its key with it (a key whose session
        is gone could never be exchanged again)."""
        session = self.sessions.get(session_id)
        if session is not None and session.revoked_at is None:
            session.revoked_at = self.now()
        for key in self.api_keys.values():
            if key.session_id == session_id and key.revoked_at is None:
                key.revoked_at = self.now()

    # ------------------------------------------------------------------ identity routes

    def info(self) -> m.GateInfo:
        """``GET /v1/info``: this gate's dev switch and ways in. Its mail is the outbox, so
        email sign-in is always on."""
        return m.GateInfo(dev=self.dev, email_sign_in=True, github_sign_in=self.github)

    def revocations(self) -> m.Revocations:
        """``GET /v1/auth/revocations``: sessions revoked within :data:`REVOCATION_WINDOW`,
        newest first, as sha256 digests of their ids."""
        since = self.now() - REVOCATION_WINDOW
        recent = sorted(
            (
                s
                for s in self.sessions.values()
                if s.revoked_at is not None and s.revoked_at > since
            ),
            key=lambda s: s.revoked_at or since,
            reverse=True,
        )[: m.REVOCATION_WINDOW_MAX]
        return m.Revocations(
            window_seconds=int(REVOCATION_WINDOW.total_seconds()),
            sessions=[hashlib.sha256(s.id.encode("ascii")).hexdigest() for s in recent],
        )

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
            expires_at=now + DEVICE_TTL,
            interval=self.device_interval,
        )
        return m.DeviceStart(
            device_code=device_code,
            user_code=user_code,
            verification_uri=f"{self.base_url}/device",
            verification_uri_complete=f"{self.base_url}/device?user_code={user_code}",
            interval=self.device_interval,
            expires_in=int(DEVICE_TTL.total_seconds()),
        )

    def _device_by_user_code(self, user_code: str) -> _Device | None:
        return next((d for d in self._devices.values() if d.user_code == user_code), None)

    async def approve_device(self, user_code: str, party: str, *, via: str = "device") -> bool:
        """Approve a pending device login as ``party`` (created if missing), as a human
        would on the verification page. False when the code is not pending."""
        device = self._device_by_user_code(user_code)
        if device is None or not self._pending(device):
            return False
        await self.ensure_party(party)
        device.status, device.party, device.approved_at = "approved", party, self.now()
        device.via = via
        return True

    def _pending(self, device: _Device) -> bool:
        return device.status == "pending" and self.now() < device.expires_at

    def pending_user_codes(self) -> list[str]:
        """The user codes of the device logins waiting for a human, oldest first: what the
        person at the terminal reads off their screen, for a test to type in for them."""
        return [d.user_code for d in self._devices.values() if self._pending(d)]

    def deny_device(self, user_code: str) -> bool:
        device = self._device_by_user_code(user_code)
        if device is None or device.status != "pending":
            return False
        device.status = "denied"
        return True

    async def token(self, req: m.TokenRequest) -> m.TokenResponse:
        misfit = req.grant_error()
        if misfit is not None:
            raise GateHTTPError(400, "invalid_request", misfit)
        if req.device_code is not None:
            return await self._exchange_device(req.device_code, req.handle)
        if req.api_key is not None:
            return self._exchange_api_key(req.api_key)
        if req.handoff is not None:
            return self._exchange_handoff(req.handoff)
        assert req.refresh_token is not None
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
            # On a dev gate this is a development handle signing in, as the platform records
            # it (``device:dev``); such a sign-in confirms at once (decisions I23).
            await self.approve_device(
                device.user_code, wanted, via="device:dev" if self.dev else "device"
            )
        if device.status == "approved":
            assert device.party is not None
            assert device.approved_at is not None
            device.status = "consumed"
            return self._open_session(device.party, device.approved_at, device.via)
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
            if session.revoked_at is None:
                session.revoked_at = self.now()
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

    # ------------------------------------------------------------------ API keys

    def _exchange_api_key(self, api_key: str) -> m.TokenResponse:
        """The ``api_key`` grant: an access token for the key's own session, no refresh
        token. An unknown key and a revoked one get the same answer."""
        key_id = self._api_key_ids.get(_digest(api_key))
        key = self.api_keys.get(key_id) if key_id is not None else None
        session = self.sessions.get(key.session_id) if key is not None else None
        if key is None or session is None or key.revoked_at is not None or session.revoked:
            raise GateHTTPError(400, "invalid_grant", "the API key is not valid")
        now = self.now()
        if key.last_used_at is None or now - key.last_used_at >= KEY_USE_RESOLUTION:
            key.last_used_at = now
        return m.TokenResponse(
            access_token=self._issue_access(session),
            expires_in=int(self.access_ttl.total_seconds()),
            refresh_token=None,
            party=key.party,
            auth_time=key.created_at,
        )

    def create_api_key(self, who: Principal, label: str) -> m.ApiKeyCreated:
        """A new key for ``who`` (a person's confirmed session, enforced by the route) and the
        session it owns, whose ``auth_time`` is the key's creation."""
        live = [k for k in self.api_keys.values() if k.party == who.party and k.revoked_at is None]
        if len(live) >= m.MAX_API_KEYS:
            raise GateHTTPError(
                409,
                "too_many_keys",
                f"a party holds at most {m.MAX_API_KEYS} live API keys; revoke one first",
            )
        now = self.now()
        secret = m.API_KEY_PREFIX + secrets.token_urlsafe(32)
        key_id = "key_" + secrets.token_hex(8)
        while key_id in self.api_keys:  # pragma: no cover - 64 random bits
            key_id = "key_" + secrets.token_hex(8)
        session = _Session(secrets.token_hex(16), who.party, now, "api_key", created_at=now)
        self.sessions[session.id] = session
        self.api_keys[key_id] = _ApiKey(key_id, who.party, label, session.id, now)
        self._api_key_ids[_digest(secret)] = key_id
        return m.ApiKeyCreated(id=key_id, label=label, created_at=now, key=secret)

    def list_api_keys(self, who: Principal) -> m.ApiKeyList:
        """Every live key of the party, newest first, then its latest revoked ones."""
        mine = [k for k in self.api_keys.values() if k.party == who.party]
        live = sorted(
            (k for k in mine if k.revoked_at is None), key=lambda k: k.created_at, reverse=True
        )
        revoked = sorted(
            (k for k in mine if k.revoked_at is not None),
            key=lambda k: k.revoked_at or k.created_at,
            reverse=True,
        )
        return m.ApiKeyList(keys=[k.view() for k in [*live, *revoked][:KEYS_LISTED]])

    def revoke_api_key(self, who: Principal, key_id: str) -> None:
        """Revoke one of the party's keys and its session (its tokens die on their next
        request). Revoking a revoked key changes nothing; another party's is not found."""
        key = self.api_keys.get(key_id)
        if key is None or key.party != who.party:
            raise GateHTTPError(404, "not_found", "no such API key")
        if key.revoked_at is None:
            key.revoked_at = self.now()
        self.revoke_session(key.session_id)

    # ------------------------------------------------------------------ sign-ins

    def _signs_in(self, session: _Session) -> bool:
        """A person's login (not a key's) that can still get tokens: not revoked, with an
        unused refresh token that has not expired."""
        now = self.now()
        return (
            session.via != "api_key"
            and session.revoked_at is None
            and any(
                r.session_id == session.id and not r.used and now < r.expires_at
                for r in self._refresh.values()
            )
        )

    def list_sessions(self, who: Principal) -> m.SessionList:
        """``GET /v1/sessions``: the party's sign-ins, newest first (keys are /v1/keys)."""
        mine = sorted(
            (s for s in self.sessions.values() if s.party == who.party and self._signs_in(s)),
            key=lambda s: s.created_at or s.auth_time,
            reverse=True,
        )[:SESSIONS_LISTED]
        return m.SessionList(
            sessions=[
                m.SessionView(
                    id=s.id,
                    via=s.via,
                    auth_time=s.auth_time,
                    created_at=s.created_at or s.auth_time,
                    current=s.id == who.session_id,
                )
                for s in mine
            ]
        )

    def end_session(self, who: Principal, session_id: str) -> None:
        """``DELETE /v1/sessions/{id}``: end one of the party's sign-ins (this one too).
        Another party's, a key's (``/v1/keys``) or an unknown id is not found; one ended
        already changes nothing."""
        session = self.sessions.get(session_id)
        if session is None or session.party != who.party or session.via == "api_key":
            raise GateHTTPError(404, "not_found", "no such session")
        self.revoke_session(session_id)

    def require_confirmation(self, party: str, session: str | None = None) -> list[str]:
        """Ask ``party``'s live sign-ins -- or the one whose id starts with ``session`` (8 to
        32 lower-case hex characters) -- to confirm it's them before their next change, as
        ``python -m gate.admin require-confirmation`` does: until a later confirmation every
        change answers 403 ``confirmation_required``; reads go on. Signs nobody out, never
        moves ``auth_time`` and never touches an API key's session. Returns the 12-character
        id prefixes asked, by sign-in time; ``ValueError`` (nothing changed) for an unknown
        party, a malformed prefix, or one naming no session of the party, more than one, an
        API key's or one signed out."""
        if session is not None and _SESSION_PREFIX.fullmatch(session) is None:
            raise ValueError("--session takes 8 to 32 lower-case hex characters of a sign-in's id")
        if party not in self.parties:
            raise ValueError(f"no party {party!r}")
        mine = [s for s in self.sessions.values() if s.party == party]
        if session is not None:
            found = [s for s in mine if s.id.startswith(session)]
            if len(found) != 1:
                raise ValueError(
                    f"{len(found) or 'no'} session(s) of {party} have an id starting {session}"
                )
            if found[0].via == "api_key":
                raise ValueError("an API key's session can never confirm (revoke the key)")
            if found[0].revoked:
                raise ValueError("that sign-in is signed out already")
            mine = found
        asked = sorted(
            (s for s in mine if not s.revoked and s.via != "api_key"),
            key=lambda s: (s.auth_time, s.id),
        )
        now = self.now()
        for s in asked:
            s.confirm_required_at = now
        return [s.id[:12] for s in asked]

    # ------------------------------------------------------------------ magic links

    def allow_email(self, email: str) -> str:
        """Put ``email`` on the sign-up allowlist (``python -m gate.admin allow``); returns
        it as stored (lowercased)."""
        address = email.strip()
        if not m.is_email(address):
            raise ValueError("not a plain email address (local@domain)")
        address = address.lower()
        self.allowed_emails.add(address)
        return address

    def disallow_email(self, email: str) -> None:
        self.allowed_emails.discard(email.strip().lower())

    def _may_sign_in(self, email: str) -> bool:
        return self.signup == "open" or email in self.allowed_emails

    def _mails_allowed(self, email: str, network: str, now: datetime) -> bool:
        """The platform's database caps: one address gets at most ``rate_per_email`` mails
        an hour asked from one network, and ``max_per_email`` from all of them."""
        since = now - self.magic_link_rate_window
        mailed = [
            link
            for link in self._magic.values()
            if link.email == email and link.created_at is not None and link.created_at > since
        ]
        mine = sum(1 for link in mailed if link.client == network)
        return mine < self.magic_link_rate_per_email and len(mailed) < self.magic_link_max_per_email

    def _count_magic_request(self, key: str, limit: int, now: datetime) -> bool:
        """Record one request under ``key``; False when it is over ``limit`` this window."""
        seen = self._magic_rate.setdefault(key, deque())
        while seen and now - seen[0] >= self.magic_link_rate_window:
            seen.popleft()
        seen.append(now)
        return len(seen) <= limit

    def _count_link_request(self, email: str, network: str, client: str, now: datetime) -> None:
        """Count one request for a link to ``email`` (a sign-in or a confirmation: one
        budget) from ``client`` in ``network``; 429 past either limit."""
        by_email = self._count_magic_request(
            f"email:{email}:network:{network}", self.magic_link_rate_per_email, now
        )
        by_client = self._count_magic_request(
            "client:" + client, self.magic_link_rate_per_client, now
        )
        if not (by_email and by_client):
            retry = int(self.magic_link_rate_window.total_seconds())
            raise GateHTTPError(
                429,
                "rate_limited",
                "too many sign-in links asked for; try again later",
                {"retry_after_seconds": retry},
                headers={"Retry-After": str(retry)},
            )

    def request_magic_link(self, req: m.MagicLinkRequest, client: str) -> m.MagicLinkSent:
        """``POST /v1/auth/magic``. Mail goes out only for an address that may sign in and a
        user code that is pending; every other request gets the identical answer, and every
        request counts against the rate limits, so neither the answer nor a 429 says who is
        invited. An address's limits are counted per requesting network (an IPv4 /24, an
        IPv6 /48, as the platform counts them; ``client`` is the requester's address): a
        stranger who asks for links to an address spends his own network's budget, never
        its owner's. Past :attr:`magic_link_max_per_email` mails to one address an hour,
        from every network together, no more go out (the answer stays the same)."""
        now = self.now()
        email = req.email.lower()
        network = _requester_network(client)
        self._count_link_request(email, network, client, now)
        device = self._device_by_user_code(req.device_user_code)
        if (
            device is not None
            and self._pending(device)
            and self._may_sign_in(email)
            and self._mails_allowed(email, network, now)
        ):
            token = secrets.token_urlsafe(32)
            self._magic[_digest(token)] = _MagicLink(
                email,
                device.user_code,
                min(now + MAGIC_LINK_TTL, device.expires_at),
                client=network,
                created_at=now,
            )
            link = f"{self.base_url}/auth/magic?t={token}"
            self.outbox.append(
                SentMail(
                    to=email,
                    subject=SUBJECT_SIGN_IN,
                    text=(
                        f"Open this link to sign in to Kontra:\n\n{link}\n\n"
                        "The page asks for the code your terminal or browser shows. "
                        "If you did not ask to sign in, ignore this mail: nobody can use "
                        "the link without that code. It expires in 10 minutes."
                    ),
                    link=link,
                    sent_at=now,
                )
            )
        return m.MagicLinkSent()

    async def use_magic_link(
        self, link: str, user_code: str, *, handle: str | None = None
    ) -> MagicOutcome:
        """What the link's page does when the human submits the user code they typed
        (opening the page consumes nothing). A wrong code counts an attempt and the
        third burns the link. The right one consumes the link and approves the device: as
        the party of the email's identity, or, on the address's first sign-in, as a new
        party ``handle`` (default: the handle the device asked for). ``gone``: the link is
        unknown, used, expired or burned, the address is no longer allowed, or the device
        is no longer pending. ``handle_taken``: the handle belongs to another party; the
        link stays usable for another handle (the page's signed grant, on the platform).
        A confirmation link (decisions I23) answers as :meth:`_use_confirmation` says."""
        token = parse_qs(urlsplit(link).query).get("t", [link])
        rec = (
            self._magic.get(_digest(token[0]))
            if len(token) == 1 and _SECRET.fullmatch(token[0])
            else None
        )
        now = self.now()
        if rec is None or rec.consumed_at is not None or now >= rec.expires_at:
            return "gone"
        if not self._may_sign_in(rec.email):
            rec.consumed_at = now
            return "gone"
        if rec.confirmation_id is not None:
            return self._use_confirmation(rec, rec.confirmation_id, user_code, now)
        assert rec.device_user_code is not None  # a sign-in link names its device
        typed = _normalize_user_code(user_code)
        if typed is None:
            # Not a code at all (empty, a typo outside the alphabet, a scanner's blind
            # submit): it cannot be a guess, so it costs no attempt -- as on the platform.
            return "wrong_code"
        if not hmac.compare_digest(typed, rec.device_user_code):
            rec.attempts += 1
            if rec.attempts >= MAGIC_LINK_ATTEMPTS:
                rec.consumed_at = now
            return "wrong_code"
        device = self._device_by_user_code(rec.device_user_code)
        if device is None or not self._pending(device):
            rec.consumed_at = now
            return "gone"
        party = self.identities.get(("email", rec.email))
        if party is None:
            wanted = handle or device.requested_handle
            if wanted is None:
                raise ValueError(f"{rec.email} signs in for the first time: pass handle=")
            if not is_handle(wanted):
                raise GateHTTPError(422, "invalid_handle", "not a party handle")
            if wanted in self.parties:
                return "handle_taken"
            rec.consumed_at = now  # before any await: a racing submit finds it used
            try:
                await self.ensure_party(wanted, new=True)
            except GateHTTPError as e:
                if e.code != "handle_taken":
                    raise
                rec.consumed_at = None  # nothing was approved: another handle may follow
                return "handle_taken"
            self.identities[("email", rec.email)] = wanted
            party = wanted
        rec.consumed_at = now
        approved = await self.approve_device(device.user_code, party, via="device:email")
        if approved:
            self._new_handoff(party, now, "device:email")
        return "approved" if approved else "gone"

    # ------------------------------------------------------------------ confirm it's you

    def _email_of(self, party: str) -> str | None:
        """The party's sign-in address: its oldest email identity, if it has one."""
        return next(
            (
                subject
                for (provider, subject), p in self.identities.items()
                if provider == "email" and p == party
            ),
            None,
        )

    def confirm_channel(self, who: Principal) -> m.ConfirmWith:
        """How ``who``'s session can confirm it's the person (decisions I23): ``dev`` for a
        development sign-in on a dev gate, ``email`` when the party has an address that may
        still sign in (the gate mails it a link), else ``sign_in`` (only a new sign-in will
        do: a party without an email identity, or whose address was taken off the list)."""
        if self.dev and who.via in _DEV_VIA:
            return "dev"
        address = self._email_of(who.party)
        if address is not None and self._may_sign_in(address):
            return "email"
        return "sign_in"

    def _new_confirm_id(self) -> str:
        confirm_id = "cfm_" + secrets.token_hex(16)
        while confirm_id in self._confirmations:  # pragma: no cover - 128 random bits
            confirm_id = "cfm_" + secrets.token_hex(16)
        return confirm_id

    def _confirm_session(self, session: _Session, now: datetime) -> bool:
        """Mark ``session`` confirmed at ``now``: only a person's session that is still live
        (an API key's never is; the platform's CHECK makes it unrepresentable)."""
        if session.revoked or session.via == "api_key":
            return False
        session.confirmed_at = now
        return True

    def confirm_start(self, who: Principal, client: str) -> m.ConfirmStart:
        """``POST /v1/auth/confirm`` (a person's session; the route checks that). A dev
        sign-in on a dev gate is confirmed at once. Otherwise: the same request limits as a
        sign-in link (per address and requesting network, per client: one budget), at most
        :data:`CONFIRM_RATE_PER_SESSION` an hour per session, and a mail with a link -- never
        the code -- to the party's sign-in address, counted against the same caps as sign-in
        mail. Pending confirmations of a session never cancel each other."""
        session = self.sessions[who.session_id]
        now = self.now()
        channel = self.confirm_channel(who)
        if channel == "sign_in":
            raise GateHTTPError(
                409,
                "confirm_unavailable",
                "this account cannot confirm it's you by email on this gate; a new sign-in "
                "confirms it (`kontra login`)",
                {"confirm_with": "sign_in"},
            )
        network = _requester_network(client)
        if channel == "dev":
            done = _Confirmation(
                self._new_confirm_id(), session.id, "", "confirmed", network, now, now, now
            )
            self._confirmations[done.id] = done
            self._confirm_session(session, now)
            return m.ConfirmStart(
                confirm_id=done.id,
                channel="dev",
                status="confirmed",
                asked_at=now,
                expires_in=0,
                interval=self.confirm_interval,
                confirmed_until=self.confirmed_until(session),
            )
        address = self._email_of(who.party)
        assert address is not None  # the email channel has one
        self._count_link_request(address, network, client, now)
        self._purge_confirmations(now)
        recent = sorted(
            c.created_at
            for c in self._confirmations.values()
            if c.session_id == session.id and now - c.created_at < CONFIRM_RATE_WINDOW
        )
        if len(recent) >= CONFIRM_RATE_PER_SESSION:
            retry = max(1, math.ceil((recent[0] + CONFIRM_RATE_WINDOW - now).total_seconds()))
            raise GateHTTPError(
                429,
                "rate_limited",
                "this sign-in asked to confirm it's you too often; try again later",
                {"retry_after_seconds": retry},
                headers={"Retry-After": str(retry)},
            )
        live = {c.user_code for c in self._confirmations.values() if c.status == "pending"}
        while True:
            raw = "".join(secrets.choice(USER_CODE_ALPHABET) for _ in range(8))
            code = f"{raw[:4]}-{raw[4:]}"
            if code not in live:
                break
        pending = _Confirmation(
            self._new_confirm_id(),
            session.id,
            code,
            "pending",
            network,
            now,
            now + CONFIRM_LINK_TTL,
        )
        self._confirmations[pending.id] = pending
        self._mail_confirmation(pending, session, address, now)
        return m.ConfirmStart(
            confirm_id=pending.id,
            channel="email",
            status="pending",
            user_code=code,
            asked_at=now,
            expires_in=int(CONFIRM_LINK_TTL.total_seconds()),
            interval=self.confirm_interval,
        )

    def _purge_confirmations(self, now: datetime) -> None:
        for c in [
            c for c in self._confirmations.values() if c.expires_at < now - CONFIRM_PURGE_AFTER
        ]:
            del self._confirmations[c.id]

    def _mail_confirmation(
        self, confirmation: _Confirmation, session: _Session, address: str, now: datetime
    ) -> None:
        """What the platform's mail sender does for a confirmation: nothing unless it is
        still pending, its session live and the address allowed and under its caps (the
        caps count sign-in and confirmation links together); then a link and a mail that
        names the session, and never the code."""
        if (
            confirmation.status != "pending"
            or now >= confirmation.expires_at
            or session.revoked
            or not self._may_sign_in(address)
            or not self._mails_allowed(address, confirmation.client, now)
        ):
            return
        token = secrets.token_urlsafe(32)
        self._magic[_digest(token)] = _MagicLink(
            address,
            None,
            min(now + CONFIRM_LINK_TTL, confirmation.expires_at),
            client=confirmation.client,
            created_at=now,
            confirmation_id=confirmation.id,
        )
        link = f"{self.base_url}/auth/magic?t={token}"
        self.outbox.append(
            SentMail(
                to=address,
                subject=SUBJECT_CONFIRM,
                text=_confirm_mail_text(link, confirmation.created_at, session, self.confirm_ttl),
                link=link,
                sent_at=now,
            )
        )

    def confirm_status(self, who: Principal, confirm_id: str) -> m.ConfirmStatus:
        """``GET /v1/auth/confirm/{id}``: this session's confirmation (any other id, well
        formed or not, is 404), and until when the session may make changes -- by this
        confirmation, another one or its sign-in -- if it may now."""
        found = self._confirmations.get(confirm_id) if _CONFIRM_ID.fullmatch(confirm_id) else None
        if found is None or found.session_id != who.session_id:
            raise GateHTTPError(404, "not_found", "no such confirmation")
        now = self.now()
        status: Literal["pending", "confirmed", "expired", "burned"] = found.status
        if status == "pending" and now >= found.expires_at:
            status = "expired"
        until = self.confirmed_until(self.sessions[who.session_id])
        return m.ConfirmStatus(
            confirm_id=found.id,
            status=status,
            confirmed_until=until if until is not None and now < until else None,
        )

    def pending_confirm_codes(self) -> list[str]:
        """The codes of the confirmations waiting for a person, oldest first: what the
        asking screen shows, for a test to type in for them."""
        now = self.now()
        return [
            c.user_code
            for c in self._confirmations.values()
            if c.status == "pending" and now < c.expires_at
        ]

    def _use_confirmation(
        self, rec: _MagicLink, confirmation_id: str, user_code: str, now: datetime
    ) -> MagicOutcome:
        """The confirmation link's page, on the POST that carries a typed code (the link is
        live and its address allowed), checked in the platform's order: the confirmation
        must still be pending (else ``gone``); a sign-in that ended since consumes the link
        whatever was typed (``gone``: the platform's page says "nothing to confirm"); not a
        code at all is the form again, no attempt counted; a wrong code counts one, and the
        third consumes the link and burns the confirmation. The right one consumes the link
        and confirms the confirmation's own session and nothing else: no session, no
        handoff."""
        confirmation = self._confirmations.get(confirmation_id)
        if (
            confirmation is None
            or confirmation.status != "pending"
            or now >= confirmation.expires_at
        ):
            return "gone"
        session = self.sessions.get(confirmation.session_id)
        if session is None or session.revoked or session.via == "api_key":
            rec.consumed_at = now
            return "gone"  # the sign-in ended since: nothing to confirm
        typed = _normalize_user_code(user_code)
        if typed is None:
            return "wrong_code"
        if not hmac.compare_digest(_digest(typed), _digest(confirmation.user_code)):
            rec.attempts += 1
            if rec.attempts >= MAGIC_LINK_ATTEMPTS:
                rec.consumed_at = now
                confirmation.status = "burned"
            return "wrong_code"
        rec.consumed_at = now
        self._confirm_session(session, now)
        confirmation.status, confirmation.confirmed_at = "confirmed", now
        return "confirmed"

    # ------------------------------------------------------------------ passkeys

    def _rp_candidate(self) -> str | None:
        if not self.passkeys:
            return None
        return self.passkey_rp_id or pk.rp_id_of(self.base_url)

    @property
    def webauthn_rp_id(self) -> str | None:
        """The RP ID passkeys are made for and checked against (decisions I24); None:
        this gate takes no passkeys (:attr:`passkeys` off, no host it could derive one
        from, or no console origin on it -- as the platform)."""
        return self._rp_candidate() if self.passkey_origins else None

    @property
    def passkey_origins(self) -> tuple[str, ...]:
        """The origins a passkey's answer may come from: the console's on the RP ID."""
        rp_id = self._rp_candidate()
        if rp_id is None:
            return ()
        given = self._passkey_origins
        return pk.origins_on(rp_id, (self.base_url,) if given is None else given)

    def _passkeys_on(self) -> str:
        rp_id = self.webauthn_rp_id
        if rp_id is None:
            raise GateHTTPError(409, "passkeys_unavailable", pk.UNAVAILABLE)
        return rp_id

    def _live_passkeys(self, party: str) -> list[_Passkey]:
        """The party's passkeys that confirm, newest first."""
        return sorted(
            (p for p in self._passkey_rows.values() if p.party == party and p.revoked_at is None),
            key=lambda p: p.created_at,
            reverse=True,
        )

    def _ceremony_limits(self, session_id: str, purpose: pk.Purpose, now: datetime) -> None:
        """429 past :data:`PASSKEY_OPTIONS_PER_HOUR` ceremonies of ``purpose``, or
        :data:`PASSKEY_FAILURES_PER_HOUR` refused answers, of this sign-in in the last
        hour (settled ceremonies are purged a day after they expire, first)."""
        for c in [
            c for c in self._ceremonies.values() if c.expires_at < now - CEREMONY_PURGE_AFTER
        ]:
            del self._ceremonies[c.id]
        mine = [c for c in self._ceremonies.values() if c.session_id == session_id]
        asked = sorted(
            c.created_at
            for c in mine
            if c.purpose == purpose and now - c.created_at < PASSKEY_RATE_WINDOW
        )
        refused = sorted(
            c.used_at
            for c in mine
            if c.outcome == "rejected"
            and c.used_at is not None
            and now - c.used_at < PASSKEY_RATE_WINDOW
        )
        for times, limit, message in (
            (asked, PASSKEY_OPTIONS_PER_HOUR, pk.OPTIONS_LIMITED),
            (refused, PASSKEY_FAILURES_PER_HOUR, pk.FAILURES_LIMITED),
        ):
            if len(times) >= limit:
                ends = times[-limit] + PASSKEY_RATE_WINDOW
                retry = max(1, math.ceil((ends - now).total_seconds()))
                raise GateHTTPError(
                    429,
                    "rate_limited",
                    message,
                    {"retry_after_seconds": retry},
                    headers={"Retry-After": str(retry)},
                )

    def _new_ceremony(self, who: Principal, purpose: pk.Purpose, now: datetime) -> _Ceremony:
        ceremony_id = "wac_" + secrets.token_hex(16)
        while ceremony_id in self._ceremonies:  # pragma: no cover - 128 random bits
            ceremony_id = "wac_" + secrets.token_hex(16)
        ceremony = _Ceremony(
            ceremony_id,
            who.session_id,
            purpose,
            secrets.token_bytes(pk.CHALLENGE_BYTES),
            now,
            now + pk.CEREMONY_TTL,
        )
        self._ceremonies[ceremony.id] = ceremony
        return ceremony

    @staticmethod
    def _ceremony(
        rp_id: str, ceremony: _Ceremony, options: dict[str, JsonValue]
    ) -> m.PasskeyCeremony:
        return m.PasskeyCeremony(
            ceremony_id=ceremony.id,
            rp_id=rp_id,
            options=options,
            expires_in=int(pk.CEREMONY_TTL.total_seconds()),
        )

    def _spend(self, who: Principal, ceremony_id: str, purpose: pk.Purpose) -> _Ceremony:
        """The ceremony ``ceremony_id`` of this session and ``purpose``, used up now --
        before its answer is looked at, whatever comes of it (``rejected`` until it is known
        to be ``ok``) -- or 410 ``ceremony_gone``: unknown, another session's, the other
        purpose, used or expired are one answer."""
        now = self.now()
        found = self._ceremonies.get(ceremony_id) if m.is_ceremony_id(ceremony_id) else None
        if (
            found is None
            or found.session_id != who.session_id
            or found.purpose != purpose
            or found.used_at is not None
            or now >= found.expires_at
        ):
            raise GateHTTPError(410, "ceremony_gone", pk.GONE)
        found.used_at, found.outcome = now, "rejected"
        return found

    def _refused(self, who: Principal, op: str, reason: str) -> GateHTTPError:
        log.info(
            "passkey %s refused for %s (sign-in %s): %s", op, who.party, who.session_id[:12], reason
        )
        return GateHTTPError(400, "passkey_rejected", pk.REJECTED)

    def _mark_sessions(self, party: str, now: datetime, *, but: str | None = None) -> None:
        """Ask the party's live sign-ins (all, or all ``but`` one) to confirm again before
        their next change: whoever held a revoked passkey may have confirmed one of them."""
        for s in self.sessions.values():
            if s.party == party and not s.revoked and s.via != "api_key" and s.id != but:
                s.confirm_required_at = now

    def _notify(self, party: str, subject: str, text: str, now: datetime) -> None:
        """A notice to the party's sign-in address, if it has one: no link, no code."""
        address = self._email_of(party)
        if address is not None:
            self.outbox.append(
                SentMail(to=address, subject=subject, text=text, link="", sent_at=now)
            )

    def list_passkeys(self, who: Principal) -> m.PasskeyList:
        """``GET /v1/passkeys``: live passkeys newest first, then the latest revoked ones."""
        live = self._live_passkeys(who.party)
        revoked = sorted(
            (
                p
                for p in self._passkey_rows.values()
                if p.party == who.party and p.revoked_at is not None
            ),
            key=lambda p: p.revoked_at or p.created_at,
            reverse=True,
        )
        rows = [*live, *revoked][:PASSKEYS_LISTED]
        return m.PasskeyList(rp_id=self.webauthn_rp_id, passkeys=[p.view() for p in rows])

    def passkey_options(self, who: Principal) -> m.PasskeyCeremony:
        """``POST /v1/passkeys/options`` (``recent``): creation options for a new passkey,
        the party's live ones excluded; a party's first makes its user handle."""
        rp_id = self._passkeys_on()
        now = self.now()
        self._ceremony_limits(who.session_id, "register", now)
        live = self._live_passkeys(who.party)
        if len(live) >= m.MAX_PASSKEYS:
            raise _too_many_passkeys()
        user = self._passkey_users.get(who.party)
        if user is None:
            user = _PasskeyUser(who.party, secrets.token_bytes(pk.USER_HANDLE_BYTES), now)
            self._passkey_users[who.party] = user
        ceremony = self._new_ceremony(who, "register", now)
        options = pk.registration_options(
            rp_id=rp_id,
            rp_name=self.passkey_rp_name,
            user_handle=user.user_handle,
            user_name=who.party,
            challenge=ceremony.challenge,
            exclude=[p.known() for p in live],
        )
        return self._ceremony(rp_id, ceremony, options)

    def add_passkey(self, who: Principal, req: m.PasskeyAdd) -> m.PasskeyView:
        """``POST /v1/passkeys`` (``fresh``): verify the browser's answer to this session's
        registration ceremony and keep the passkey; then mail the party that it was added."""
        rp_id = self._passkeys_on()
        ceremony = self._spend(who, req.ceremony_id, "register")
        now = ceremony.used_at or self.now()
        try:
            new = pk.verify_registration(
                req.credential,
                challenge=ceremony.challenge,
                rp_id=rp_id,
                origins=self.passkey_origins,
            )
        except pk.PasskeyRejected as e:
            raise self._refused(who, "register", e.reason) from None
        if len(self._live_passkeys(who.party)) >= m.MAX_PASSKEYS:
            raise _too_many_passkeys()
        if any(p.credential_id == new.credential_id for p in self._passkey_rows.values()):
            raise GateHTTPError(409, "passkey_exists", pk.EXISTS)
        passkey_id = "pky_" + secrets.token_hex(16)
        while passkey_id in self._passkey_rows:  # pragma: no cover - 128 random bits
            passkey_id = "pky_" + secrets.token_hex(16)
        row = _Passkey(
            passkey_id,
            who.party,
            new.credential_id,
            new.public_key,
            new.algorithm,
            new.sign_count,
            new.transports,
            new.aaguid,
            req.label,
            new.backup_eligible,
            new.backed_up,
            now,
            who.session_id,
        )
        self._passkey_rows[row.id] = row
        ceremony.outcome = "ok"
        session = self.sessions[who.session_id]
        text = pk.added_mail(
            req.label,
            now,
            via=VIA_WORDS.get(session.via, session.via),
            signed_in=session.auth_time,
            session_prefix=session.id[:12],
        )
        self._notify(who.party, SUBJECT_PASSKEY_ADDED, text, now)
        return row.view()

    def revoke_passkey(self, who: Principal, passkey_id: str) -> None:
        """``DELETE /v1/passkeys/{id}`` (``fresh``): revoke one of the party's passkeys, ask
        its other sign-ins to confirm again, and mail the party which sign-in removed it. A
        revoked one changes nothing; another party's, or an id that is none, is not found."""
        row = self._passkey_rows.get(passkey_id) if m.is_passkey_id(passkey_id) else None
        if row is None or row.party != who.party:
            raise GateHTTPError(404, "not_found", "no such passkey")
        if row.revoked_at is not None:
            return
        now = self.now()
        row.revoked_at, row.revoked_reason = now, "revoked"
        self._mark_sessions(who.party, now, but=who.session_id)
        session = self.sessions[who.session_id]
        text = pk.removed_mail(
            row.label,
            now,
            via=VIA_WORDS.get(session.via, session.via),
            signed_in=session.auth_time,
            session_prefix=session.id[:12],
        )
        self._notify(who.party, SUBJECT_PASSKEY_REMOVED, text, now)

    def confirm_passkey_options(self, who: Principal) -> m.PasskeyCeremony:
        """``POST /v1/auth/confirm/passkey/options`` (``human``): request options listing
        the party's live passkeys. Mails nobody and confirms nothing."""
        rp_id = self._passkeys_on()
        now = self.now()
        self._ceremony_limits(who.session_id, "confirm", now)
        live = self._live_passkeys(who.party)
        if not live:
            raise GateHTTPError(409, "no_passkeys", pk.NO_PASSKEYS)
        ceremony = self._new_ceremony(who, "confirm", now)
        options = pk.assertion_options(
            rp_id=rp_id, challenge=ceremony.challenge, allow=[p.known() for p in live]
        )
        return self._ceremony(rp_id, ceremony, options)

    def confirm_passkey(self, who: Principal, req: m.PasskeyConfirm) -> m.PasskeyConfirmed:
        """``POST /v1/auth/confirm/passkey`` (``human``): verify the browser's answer to
        this session's confirm ceremony with the live passkey of the party it names, then
        judge its use count (:func:`kontra._passkeys.judge_counter`): forward (or 0 on both
        sides) confirms this session, as the mailed link does; behind, for a synced passkey
        (backup eligible), is refused and nothing else happens; behind otherwise is a copy of
        the authenticator answering -- the passkey is revoked, every sign-in of the party
        asked to confirm again, the party mailed, and nothing confirmed."""
        rp_id = self._passkeys_on()
        ceremony = self._spend(who, req.ceremony_id, "confirm")
        now = ceremony.used_at or self.now()
        raw = pk.answer_credential_id(req.credential)
        row = next(
            (
                p
                for p in self._live_passkeys(who.party)
                if raw is not None and hmac.compare_digest(p.credential_id, raw)
            ),
            None,
        )
        user = self._passkey_users.get(who.party)
        if row is None or user is None:
            raise self._refused(who, "confirm", "unknown")
        try:
            assertion = pk.verify_assertion(
                req.credential,
                challenge=ceremony.challenge,
                rp_id=rp_id,
                origins=self.passkey_origins,
                known=row.known(),
                user_handle=user.user_handle,
            )
        except pk.PasskeyRejected as e:
            raise self._refused(who, "confirm", e.reason) from None
        verdict = pk.judge_counter(
            row.sign_count, assertion.sign_count, backup_eligible=row.backup_eligible
        )
        if verdict == "behind":  # a synced passkey's other copy: refused, nothing revoked
            raise self._refused(who, "confirm", "counter")
        if verdict == "copy":
            row.revoked_at, row.revoked_reason = now, "counter"
            self._mark_sessions(who.party, now)
            ceremony.outcome = "cloned"
            self._notify(who.party, SUBJECT_PASSKEY_REVOKED, pk.revoked_mail(row.label, now), now)
            log.warning(
                "passkey %s of %s answered an old use count: revoked", row.id[:12], who.party
            )
            raise GateHTTPError(403, "passkey_cloned", pk.CLONED)
        if not self._confirm_session(self.sessions[who.session_id], now):  # pragma: no cover
            raise _unauthorized("invalid_token", "the session was revoked; log in again")
        row.sign_count, row.backed_up, row.last_used_at = (
            assertion.sign_count,
            assertion.backed_up,
            now,
        )
        ceremony.outcome = "ok"
        return m.PasskeyConfirmed(confirmed_until=now + self.confirm_ttl, passkey_id=row.id)

    def revoke_passkeys(self, party: str, passkey: str | None = None) -> list[str]:
        """Revoke ``party``'s live passkeys -- or the one whose id starts with ``passkey``
        (8 to 36 characters) -- ask every sign-in of the party to confirm again, and mail the
        party each passkey removed, as ``python -m gate.admin revoke-passkeys`` does (the
        reason is ``operator``). Returns the 12-character id prefixes revoked, oldest first;
        ``ValueError`` (nothing changed) for an unknown party, a malformed prefix, or one
        naming none of the party's passkeys, more than one, or a revoked one."""
        if passkey is not None and _PASSKEY_PREFIX.fullmatch(passkey) is None:
            raise ValueError("--passkey takes 8 to 36 characters of a passkey's id (pky_...)")
        if party not in self.parties:
            raise ValueError(f"no party {party!r}")
        mine = [p for p in self._passkey_rows.values() if p.party == party]
        if passkey is not None:
            found = [p for p in mine if p.id.startswith(passkey)]
            if len(found) != 1:
                raise ValueError(
                    f"{len(found) or 'no'} passkey(s) of {party} have an id starting {passkey}"
                )
            if found[0].revoked_at is not None:
                raise ValueError("that passkey is revoked already")
            mine = found
        revoked = sorted((p for p in mine if p.revoked_at is None), key=lambda p: p.created_at)
        now = self.now()
        for p in revoked:
            p.revoked_at, p.revoked_reason = now, "operator"
            self._notify(party, SUBJECT_PASSKEY_REMOVED, pk.removed_mail(p.label, now), now)
        if revoked:
            self._mark_sessions(party, now)
        return [p.id[:12] for p in revoked]

    def _new_handoff(self, party: str, auth_time: datetime, via: str) -> str:
        handoff = secrets.token_urlsafe(32)
        expires = self.now() + timedelta(seconds=m.HANDOFF_TTL_SECONDS)
        self._handoffs[_digest(handoff)] = _Handoff(party, auth_time, via, expires)
        self.handoffs.append(handoff)
        return handoff

    def _exchange_handoff(self, handoff: str) -> m.TokenResponse:
        rec = self._handoffs.get(_digest(handoff)) if _SECRET.fullmatch(handoff) else None
        if rec is None or rec.consumed or self.now() >= rec.expires_at:
            raise GateHTTPError(400, "invalid_grant", "the sign-in handoff is not valid")
        rec.consumed = True
        return self._open_session(rec.party, rec.auth_time, rec.via)

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

    async def ensure_party(self, handle: str, *, new: bool = False) -> _Party:
        """The party ``handle``, created (namespace registered, signup grant paid) if
        missing. ``new``: it must not exist yet (409 ``handle_taken``): a sign-up never
        joins someone else's party."""
        try:
            validate_handle(handle)
        except NameError_ as e:
            raise GateHTTPError(422, "invalid_handle", str(e)) from None
        lock = self._party_locks.setdefault(handle, asyncio.Lock())
        async with lock:
            existing = self.parties.get(handle)
            if existing is not None:
                if new:
                    raise GateHTTPError(409, "handle_taken", f"the handle {handle} is taken")
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
            keys=[
                m.PartyKey(label=k.label, public_key=k.public_key, added_at=k.added_at)
                for k in p.keys
                if k.revoked_at is None
            ],
            temporal_address=self.temporal_address,
            created_at=p.created_at,
            dev=self.dev,
        )

    def add_recipient(self, who: Principal, label: str, public_key: str) -> m.Me:
        """Add one recipient. Adding never replaces, so a second machine is one more
        recipient and ``key_mismatch`` no longer exists; adding the same key again under
        the same label is a no-op, so `kontra login` twice on one machine is idempotent."""
        try:
            check_public_key(public_key)
        except ValueError as e:
            raise GateHTTPError(422, "invalid_public_key", str(e)) from None
        if not 0 < len(label) <= 64 or not label.isprintable() or label != label.strip():
            raise GateHTTPError(
                422,
                "invalid_label",
                "a label is 1 to 64 printable characters, no surrounding spaces",
            )
        p = self._party(who.party)
        for k in p.keys:
            if k.public_key == public_key:
                if k.revoked_at is not None:
                    raise GateHTTPError(
                        409,
                        "recipient_revoked",
                        "that key was revoked for this party; make a new one",
                    )
                return self.me(who)
        if any(k.label == label for k in p.keys if k.revoked_at is None):
            raise GateHTTPError(409, "label_taken", f"this party already has a key {label!r}")
        if len(p.recipients()) >= m.RECIPIENTS_MAX:
            raise GateHTTPError(
                409,
                "too_many_recipients",
                f"a party holds at most {m.RECIPIENTS_MAX} recipients; revoke one first",
            )
        p.keys.append(_PartyKey(label=label, public_key=public_key, added_at=self.now()))
        return self.me(who)

    def revoke_recipient(self, who: Principal, public_key: str) -> None:
        """Stop encrypting to this key. The row stays (see :class:`_PartyKey`)."""
        p = self._party(who.party)
        for k in p.keys:
            if k.public_key == public_key and k.revoked_at is None:
                k.revoked_at = self.now()
                return
        raise GateHTTPError(
            404, "recipient_not_found", "this party has no such unrevoked recipient"
        )

    # ------------------------------------------------------------------ devices & job keys

    def _live_devices(self, party: str) -> list[_DeviceKey]:
        """The party's live devices, oldest first."""
        now = self.now()
        rows = [d for d in self.devices.values() if d.party == party and d.live(now)]
        return sorted(rows, key=lambda d: (d.added_at, d.public_key))

    def list_devices(self, who: Principal, for_: str | None = None) -> m.DeviceList:
        """``GET /v1/me/devices`` (``party``): the live devices, oldest first. ``for_``: the
        caller's own device, marked used. An API key's session reads keys, not names."""
        self._check_for(for_)
        self._touch_device(who.party, for_)
        live = self._live_devices(who.party)
        return m.DeviceList(devices=[d.view(hidden=who.api_key) for d in live])

    def _make_room(self, party: str, now: datetime, but: str) -> _DeviceKey:
        """At ``DEVICES_MAX`` live devices: retire the browser used least recently (never
        ``but``), else 409 ``too_many_devices``."""
        browsers = [
            d for d in self._live_devices(party) if d.kind == "browser" and d.public_key != but
        ]
        if not browsers:
            raise GateHTTPError(409, "too_many_devices", jk.TOO_MANY_DEVICES)
        oldest = min(browsers, key=lambda d: (d.last_seen, d.added_at, d.public_key))
        oldest.retired_at = now
        return oldest

    def add_device(self, who: Principal, req: m.AddDevice) -> m.DeviceView:
        """``POST /v1/me/devices`` (``recent``): one more device, mailed to the party's
        address. The same live key again is the same view, and mails nothing; a removed one
        is never added again; a retired one comes back, mailing nothing. Past
        ``DEVICES_MAX`` the browser used least recently makes room; past
        ``DEVICE_ADDS_PER_HOUR`` new ones in the hour, 429 ``rate_limited``."""
        try:
            check_public_key(req.public_key)
        except ValueError as e:
            raise GateHTTPError(422, "invalid_public_key", str(e)) from None
        self._party(who.party)
        now = self.now()
        known = self.devices.get((who.party, req.public_key))
        if known is not None:
            if known.revoked_at is not None:
                raise GateHTTPError(409, "device_revoked", jk.DEVICE_REVOKED)
            if known.live(now):
                return known.view()
            if len(self._live_devices(who.party)) >= m.DEVICES_MAX:
                self._make_room(who.party, now, known.public_key)
            known.retired_at = None
            known.last_used_at = now.replace(second=0, microsecond=0)
            return known.view()
        hour_ago = now - timedelta(hours=1)
        added = sorted(
            d.added_at
            for d in self.devices.values()
            if d.party == who.party and d.added_at > hour_ago
        )
        if len(added) >= m.DEVICE_ADDS_PER_HOUR:
            retry = max(1, math.ceil((added[0] + timedelta(hours=1) - now).total_seconds()))
            raise GateHTTPError(
                429,
                "rate_limited",
                jk.TOO_MANY_ADDED,
                {"retry_after_seconds": retry},
                headers={"Retry-After": str(retry)},
            )
        retired = None
        if len(self._live_devices(who.party)) >= m.DEVICES_MAX:
            retired = self._make_room(who.party, now, req.public_key)
        row = _DeviceKey(who.party, req.public_key, req.label, req.kind, now, who.session_id)
        self.devices[(who.party, req.public_key)] = row
        session = self.sessions[who.session_id]
        text = jk.device_added_mail(
            req.label,
            req.kind,
            now,
            via=VIA_WORDS.get(session.via, session.via),
            signed_in=session.auth_time,
            session_prefix=session.id[:12],
            retired=retired.label if retired is not None else None,
        )
        self._notify(who.party, SUBJECT_DEVICE_ADDED, text, now)
        return row.view()

    def remove_device(self, who: Principal, public_key: str) -> None:
        """``DELETE /v1/me/devices/{key}`` (``fresh``): the device gets no key of a new job,
        and its wraps of the party's jobs are deleted now; the party is mailed."""
        row = self.devices.get((who.party, public_key))
        if row is None or row.revoked_at is not None:
            raise GateHTTPError(404, "device_not_found", jk.DEVICE_NOT_FOUND)
        now = self.now()
        row.revoked_at = now
        self.drop_wraps(who.party, public_key)
        session = self.sessions[who.session_id]
        text = jk.device_removed_mail(
            row.label,
            row.kind,
            now,
            via=VIA_WORDS.get(session.via, session.via),
            signed_in=session.auth_time,
            session_prefix=session.id[:12],
        )
        self._notify(who.party, SUBJECT_DEVICE_REMOVED, text, now)

    def drop_wraps(self, party: str, public_key: str) -> int:
        """Delete every wrap to ``public_key`` of ``party``'s jobs; how many."""
        gone = [
            k
            for k, w in self.job_key_wraps.items()
            if w.recipient == public_key
            and (job := self.jobs.get(w.job_id)) is not None
            and job.record.buyer == party
        ]
        for k in gone:
            del self.job_key_wraps[k]
        return len(gone)

    def _shareable(self, job: _Job) -> bool:
        """A job whose ``buyer_key`` is or was one of its buyer's recipients (every job from
        before per-job keys: the party's login key) is never shared: that key also reads
        seller input and every older job."""
        party = self.parties.get(job.record.buyer)
        ever = {k.public_key for k in party.keys} if party is not None else set[str]()
        return job.buyer_key not in ever

    def _job_key_view(self, who: Principal, job: _Job, for_: str | None) -> m.JobKey:
        wraps: list[m.KeyWrap] = []
        if self._shareable(job):
            live = {d.public_key for d in self._live_devices(job.record.buyer)}
            for (job_id, recipient), w in sorted(self.job_key_wraps.items()):
                if job_id == job.job_id and recipient in live and for_ in (None, recipient):
                    wraps.append(w.view())
        challenge = jk.make_challenge(
            self.job_key_proof_key, job.job_id, who.session_id, job.buyer_key, self.now()
        )
        return m.JobKey(
            job_id=job.job_id, buyer_key=job.buyer_key, wraps=wraps, challenge=challenge
        )

    def _touch_device(self, party: str, public_key: str | None) -> None:
        """A read for ``public_key`` (``?for=``): its ``last_used_at``, to the minute, at
        most once a minute -- a live device's only (a retired one comes back by an add)."""
        row = self.devices.get((party, public_key)) if public_key is not None else None
        now = self.now()
        if row is None or not row.live(now):
            return
        if row.last_used_at is None or now - row.last_used_at >= KEY_USE_RESOLUTION:
            row.last_used_at = now.replace(second=0, microsecond=0)

    @staticmethod
    def _check_for(value: str | None) -> None:
        if value is not None:
            try:
                check_public_key(value)
            except ValueError as e:
                raise GateHTTPError(422, "invalid_public_key", str(e)) from None

    def job_key(self, who: Principal, job_id: str, for_: str | None) -> m.JobKey:
        """``GET /v1/jobs/{id}/key`` (``party``): the buyer's only, like ``result``."""
        self._check_for(for_)
        job = self.job_for(job_id, who.party)
        self._require_buyer(job, who.party, "read the job's key")
        self._touch_device(who.party, for_)
        return self._job_key_view(who, job, for_)

    def job_keys(
        self, who: Principal, lacking: str, for_: str | None, cursor: str | None, limit: int
    ) -> m.JobKeyList:
        """``GET /v1/me/job-keys`` (``party``): the caller's shareable buyer jobs with no wrap
        to ``lacking`` -- a live device of the caller's -- by job id."""
        self._check_for(for_)
        if cursor is not None and not is_job_id(cursor):
            raise GateHTTPError(422, "invalid_cursor", "cursor is not one this gate handed out")
        device = self.devices.get((who.party, lacking))
        if device is None or not device.live(self.now()):
            raise GateHTTPError(404, "device_not_found", jk.DEVICE_NOT_FOUND)
        rows = sorted(
            (
                j
                for j in self.jobs.values()
                if j.record.buyer == who.party
                and self._shareable(j)
                and (j.job_id, lacking) not in self.job_key_wraps
                and (cursor is None or j.job_id > cursor)
            ),
            key=lambda j: j.job_id,
        )
        page = rows[:limit]
        self._touch_device(who.party, for_)
        nxt = page[-1].job_id if len(rows) > limit and page else None
        return m.JobKeyList(items=[self._job_key_view(who, j, for_) for j in page], next_cursor=nxt)

    def share_job_keys(self, who: Principal, req: m.ShareJobKeys) -> m.SharedJobKeys:
        """``POST /v1/me/job-keys`` (``party``): per job, the caller is its buyer, it is
        shareable, the proof is this session's for it now, and every wrap has a wrap's shape
        -- else the job is refused whole. A wrap to no live device of the caller, or for a
        (job, device) that has one already, is skipped: the first write wins."""
        now = self.now()
        live = {d.public_key for d in self._live_devices(who.party)}
        stored = skipped = 0
        refused: list[m.ShareRefusal] = []
        for item in req.items:
            job = self.jobs.get(item.job_id) if is_job_id(item.job_id) else None
            code: str | None = None
            blobs: list[bytes] = []
            if job is None or job.record.buyer != who.party:
                code = "job_not_found"
            elif not self._shareable(job):
                code = "not_shareable"
            elif not jk.check_proof(
                self.job_key_proof_key, job.job_id, who.session_id, item.proof, now
            ):
                code = "bad_proof"
            else:
                try:
                    blobs = [jk.check_wrap_shape(w.wrap) for w in item.wraps]
                except jk.InvalidWrap:
                    code = "invalid_wrap"
            if code is not None:
                refused.append(m.ShareRefusal(job_id=item.job_id, code=code))
                continue
            for w, blob in zip(item.wraps, blobs, strict=True):
                key = (item.job_id, w.recipient)
                if w.recipient not in live or key in self.job_key_wraps:
                    skipped += 1
                    continue
                self.job_key_wraps[key] = _KeyWrapRow(
                    item.job_id, w.recipient, blob, now, who.session_id
                )
                stored += 1
        return m.SharedJobKeys(stored=stored, skipped=skipped, refused=refused)

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

    def catalog_entry(self, row: _Offering) -> m.CatalogEntry:
        """A catalog entry with the seller's recipients as they are now."""
        return row.entry(self._party(row.id.party).recipients())

    def party_view(self, handle: str) -> m.PartyView:
        p = self._party(handle)
        released, refunded, disputed = self.reputation(handle)
        return m.PartyView(
            party=handle,
            recipients=p.recipients(),
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
            if row.retired:
                # Its contract is what its jobs were hired under, and the only copy of it
                # (a job stores the hash): a retired version is never rewritten.
                raise GateHTTPError(
                    409,
                    "version_retired",
                    f"{oid.key} is retired; publish a new version instead",
                    {"contract_hash": row.info.contract_hash},
                )
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
        return self.catalog_entry(row)

    def resolve_ref(self, ref: OfferingRef, *, visible_only: bool = True) -> _Offering | None:
        """The highest visible version ``ref`` accepts (§7.4); with ``visible_only=False``,
        the highest of every version ever published, retired and unconfirmed ones too."""
        rows = self._visible() if visible_only else self.offerings.values()
        candidates = [
            o
            for o in rows
            if o.id.party == ref.party and o.id.name == ref.name and ref.matches(o.id.version)
        ]
        return max(candidates, key=lambda o: o.id.version) if candidates else None

    def lookup(self, ref: OfferingRef) -> _Offering | None:
        """What ``GET /v1/catalog/{party}/{name}/{version}`` (and its card) answers, as the
        gate's ``catalog.lookup``. An exact version is a historical fact -- every job hired
        under it names it, and its buyer and seller read its contract to open the job -- so
        it is answered retired too (``retired: true``), once its endpoint was confirmed. A
        partial version or ``latest`` chooses among the visible versions only, as quotes,
        hires and search do: a retired version is never what a buyer is pointed at."""
        if ref.exact is None:
            return self.resolve_ref(ref)
        found = self.resolve_ref(ref, visible_only=False)
        return found if found is not None and found.endpoint_ready else None

    def resolve_path(self, party: str, name: str, version: str) -> _Offering:
        """The catalog path ``/{party}/{name}/{version}``, each segment validated on its own
        and the reference built from the parts (never joined and re-parsed)."""
        found = self.lookup(_path_ref(party, name, version))
        if found is None:
            raise GateHTTPError(404, "offering_not_found", "no such offering")
        return found

    def by_endpoint(self, endpoint: str) -> m.CatalogEntry:
        key = self._by_endpoint.get(endpoint) if is_endpoint_name(endpoint) else None
        row = self.offerings.get(key) if key is not None else None
        if row is None or not row.endpoint_ready or row.retired:
            raise GateHTTPError(404, "endpoint_not_found", "no offering has this endpoint")
        return self.catalog_entry(row)

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

    # ------------------------------------------------------------------ workers

    async def workers(self, who: Principal, party: str, name: str, version: str) -> m.WorkersView:
        """The Workers view of one of the caller's own versions (kontra-cloud#6). The path
        resolves as the catalog's does, among every version the caller published -- a
        retired one too, whose workers may still be finishing its jobs -- and another
        party's version is 404 like a missing one. Temporal is asked in the caller's
        namespace through the control plane; with none, every seller is offline and Temporal
        reports nothing (:func:`kontra.workers.offline`)."""
        ref = _path_ref(party, name, version)
        mine = [
            o
            for o in self.offerings.values()
            if o.id.party == who.party == ref.party
            and o.id.name == ref.name
            and ref.matches(o.id.version)
        ]
        if not mine:
            raise GateHTTPError(404, "offering_not_found", "no such offering")
        row = max(mine, key=lambda o: o.id.version)
        task_queue = row.id.task_queue
        if self.control is None:
            return workers_view(row.key, task_queue, offline())
        try:
            described = await self.control.describe_task_queue(
                self._party(who.party).namespace, task_queue
            )
        except TemporalError:
            log.exception("describing task queue %s failed", task_queue)
            raise GateHTTPError(
                503, "temporal_unavailable", "could not ask Temporal yet; try again shortly"
            ) from None
        return workers_view(row.key, task_queue, described)

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
                    "seller_recipients": self._party(oid.party).recipients(),
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
        # The buyer's party keys are NOT consulted: the job's key comes with the request
        # and the gate signs THAT into the hold token (decisions I7). So hiring needs no
        # key setup at all -- a fresh machine, CI with an API key, `kontra mcp`.
        try:
            check_public_key(req.buyer_key)
        except ValueError as e:
            raise GateHTTPError(422, "invalid_public_key", str(e)) from None
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
            buyer_key=req.buyer_key,
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
            seller_recipients=self._party(oid.party).recipients(),
            expires_at=job.hold_expires_at,
        )

    def _abandon(self, job: _Job) -> None:
        """The ledger refused the lock: no job, and the quote may be hired again."""
        self.jobs.pop(job.job_id, None)
        for key in [k for k in self.job_key_wraps if k[0] == job.job_id]:
            del self.job_key_wraps[key]  # the platform's rows cascade with the job's
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

    def _page(
        self, rows: list[_Job], cursor: str | None, limit: int, *, buyer_key: bool = False
    ) -> m.JobList:
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
        return m.JobList(items=[j.view(buyer_key=buyer_key) for j in page], next_cursor=nxt)

    def list_jobs(
        self,
        party: str,
        as_: str,
        state: m.JobState | None,
        cursor: str | None,
        limit: int,
        *,
        buyer_key: bool = False,
    ) -> m.JobList:
        rows = [
            j
            for j in self.jobs.values()
            if (j.record.buyer if as_ == "buyer" else j.record.seller) == party
            and (state is None or j.record.state is state)
        ]
        return self._page(rows, cursor, limit, buyer_key=buyer_key)

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
        the pinned chain by the caller, never assumed. A stop already being carried out is not
        issued again: the sweeper's next pass retries it only if it failed."""
        if job.stopping:
            return
        job.stopping = True
        try:
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
        finally:
            job.stopping = False
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


def _requester_network(client: str) -> str:
    """The network a sign-in link request is charged to, as the platform counts it: the
    client's IPv4 /24 or IPv6 /48 (one machine easily holds a whole routed IPv6 /48, so a
    budget per address would let one stranger use up an address's mail by himself).
    Anything that is not an address (a test client's name) is its own network."""
    try:
        net = ipaddress.ip_network(client, strict=False)
    except ValueError:
        return client
    prefix = 24 if net.version == 4 else 48
    return str(net.supernet(new_prefix=prefix)) if net.prefixlen > prefix else str(net)


def _duration_words(ttl: timedelta) -> str:
    """``an hour``, ``2 hours``, ``30 minutes``, ``a minute``: the platform's words for how
    long a confirmation lasts (``gate.auth.magic.ttl_words``)."""
    seconds = int(ttl.total_seconds())
    if seconds % 3600 == 0:
        hours = seconds // 3600
        return "an hour" if hours == 1 else f"{hours} hours"
    minutes = max(1, round(seconds / 60))
    return "a minute" if minutes == 1 else f"{minutes} minutes"


def _too_many_passkeys() -> GateHTTPError:
    return GateHTTPError(
        409,
        "too_many_passkeys",
        f"a party holds at most {m.MAX_PASSKEYS} live passkeys; revoke one first",
        {"max_passkeys": m.MAX_PASSKEYS},
    )


def _confirmation_required_message(ttl: timedelta) -> str:
    """The platform's message of 403 ``confirmation_required`` (``once an hour`` at the
    default ``confirm_ttl``)."""
    words = _duration_words(ttl)
    often = f"once {words}" if words in ("an hour", "a minute") else f"once every {words}"
    return (
        f"this change needs you to confirm it's you ({often} per sign-in): the console asks in "
        "place, and so does kontra-sdk 0.2.2 or later (`kontra confirm`); a new `kontra login` "
        "also counts"
    )


def _confirm_mail_text(link: str, asked_at: datetime, session: _Session, ttl: timedelta) -> str:
    """The confirmation mail: the link, never the code; when it was asked (the asking screen
    says the same), and which sign-in it confirms, so a person who did not ask knows which
    one to sign out (decisions I23)."""
    via = VIA_WORDS.get(session.via, session.via)
    signed_in = session.auth_time.astimezone(UTC).strftime("%Y-%m-%d %H:%M")
    asked = asked_at.astimezone(UTC).strftime("%H:%M")
    return (
        "Open this link to confirm it's you on Kontra:\n\n"
        f"{link}\n\n"
        "The page asks for the code shown on the screen that asked you to confirm: the\n"
        '"Confirm it\'s you" panel of the Kontra console, or the terminal where you ran a kontra\n'
        f"command. It was asked at {asked} UTC. Type the code only if you asked for this "
        "yourself, just\nnow. Kontra never sends that code by mail, chat or phone.\n\n"
        f"It confirms one sign-in of yours ({via}, signed in {signed_in} UTC, id\n"
        f"{session.id[:12]}) for {_duration_words(ttl)}. The link expires in 10 minutes and "
        "works once.\n\n"
        "If you did not ask for this, do not use the link. Only a sign-in to your account can "
        "ask for\nthis mail, so one of your sign-ins may be in someone else's hands: in the "
        "Kontra console, open\nAPI keys > Where you are signed in and sign out the one above.\n"
    )


def _digest(secret: str) -> str:
    return hashlib.sha256(secret.encode("ascii", "replace")).hexdigest()


def _normalize_user_code(typed: str) -> str | None:
    """``bcdf-ghjk``, ``BCDFGHJK`` -> ``BCDF-GHJK``; anything else -> None. Checked as
    typed (anchored, bounded) before it is case-folded, as the gate's page does."""
    if len(typed) > 9 or _TYPED_USER_CODE.fullmatch(typed) is None:
        return None
    code = typed.replace("-", "").upper()
    if any(c not in USER_CODE_ALPHABET for c in code):
        return None
    return f"{code[:4]}-{code[4:]}"


def _is_number(text: str) -> bool:
    return text.isascii() and text.isdigit() and len(text) <= 9 and (text == "0" or text[0] != "0")


def _path_ref(party: str, name: str, version: str) -> OfferingRef:
    """The reference a catalog-shaped path names (``{party}/{name}/{version}``, the version
    ``latest`` or one to three numbers), each segment validated on its own and the reference
    built from the parts, never joined and re-parsed; 404 for a path no offering can have."""
    nums: list[int | None] = []
    ok = is_handle(party) and is_handle(name) and len(version) <= _PATH_VERSION_MAX
    if ok and version != "latest":
        pieces = version.split(".")
        ok = 1 <= len(pieces) <= 3 and all(_is_number(p) for p in pieces)
        nums = [int(p) for p in pieces] if ok else []
    if not ok:
        raise GateHTTPError(404, "offering_not_found", "no such offering")
    nums += [None] * (3 - len(nums))
    return OfferingRef(party, name, nums[0], nums[1], nums[2])


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


async def _auth_human(request: Request) -> Principal:
    gate = _gate_of(request)
    return gate.require_human(gate.authenticate(request, allow_worker=False))


async def _auth_fresh(request: Request) -> Principal:
    gate = _gate_of(request)
    return gate.require_confirmed(gate.authenticate(request, allow_worker=False))


async def _auth_recent(request: Request) -> Principal:
    gate = _gate_of(request)
    return gate.require_recent(gate.authenticate(request, allow_worker=False))


async def _auth_recent_device(request: Request) -> Principal:
    gate = _gate_of(request)
    return gate.require_recent(gate.authenticate(request, allow_worker=False), jk.RECENT_REQUIRED)


async def _auth_resolver(request: Request) -> Principal:
    gate = _gate_of(request)
    return gate.require_resolver(gate.authenticate(request, allow_worker=False))


async def _auth_resolver_human(request: Request) -> Principal:
    gate = _gate_of(request)
    return gate.require_resolver_human(gate.authenticate(request, allow_worker=False))


async def _dev_only(request: Request) -> None:
    _gate_of(request).require_dev()


# One per ``auth`` value of the route table (``none`` takes no dependency).
Party = Annotated[Principal, Depends(_auth_party)]
PartyOrWorker = Annotated[Principal, Depends(_auth_party_or_worker)]
Human = Annotated[Principal, Depends(_auth_human)]
Fresh = Annotated[Principal, Depends(_auth_fresh)]
Recent = Annotated[Principal, Depends(_auth_recent)]
#: ``recent``, with the device route's own words.
RecentDevice = Annotated[Principal, Depends(_auth_recent_device)]
Resolver = Annotated[Principal, Depends(_auth_resolver)]
ResolverHuman = Annotated[Principal, Depends(_auth_resolver_human)]


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

    def add(name: str, endpoint: Callable[..., Any], **kwargs: Any) -> None:
        route = BY_NAME[name]
        app.add_api_route(
            route.path,
            endpoint,
            methods=[route.method],
            name=name,
            operation_id=name,
            response_model=None if route.stream else route.response,
            status_code=route.status,
            **kwargs,
        )

    def no_store(response: Response) -> None:
        response.headers["Cache-Control"] = "no-store"

    # ---------------------------------------------------------------- identity

    async def jwks() -> JSONResponse:
        return JSONResponse(gate.keys.jwks())

    async def info() -> m.GateInfo:
        return gate.info()

    async def revocations(response: Response) -> m.Revocations:
        no_store(response)
        return gate.revocations()

    async def device_start(response: Response) -> m.DeviceStart:
        no_store(response)
        return gate.device_start()

    async def token(body: m.TokenRequest, response: Response) -> m.TokenResponse:
        no_store(response)
        return await gate.token(body)

    async def magic_link(request: Request, body: m.MagicLinkRequest) -> m.MagicLinkSent:
        client = request.client.host if request.client is not None else ""
        return gate.request_magic_link(body, client)

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

    async def add_recipient(who: Fresh, body: m.AddKey, response: Response) -> m.Me:
        response.status_code = 201
        return gate.add_recipient(who, body.label, body.public_key)

    async def revoke_recipient(who: Fresh, public_key: str) -> Response:
        gate.revoke_recipient(who, public_key)
        return Response(status_code=204)

    async def party_view(party: str) -> m.PartyView:
        return gate.party_view(party)

    async def devices(
        who: Party, for_: Annotated[str | None, Query(alias="for")] = None
    ) -> m.DeviceList:
        return gate.list_devices(who, for_)

    async def add_device(who: RecentDevice, body: m.AddDevice, response: Response) -> m.DeviceView:
        response.status_code = 201
        return gate.add_device(who, body)

    async def remove_device(who: Fresh, public_key: str) -> Response:
        gate.remove_device(who, public_key)
        return Response(status_code=204)

    async def create_api_key(
        who: Fresh, body: m.ApiKeyCreate, response: Response
    ) -> m.ApiKeyCreated:
        no_store(response)
        return gate.create_api_key(who, body.label)

    async def api_keys(who: Human) -> m.ApiKeyList:
        return gate.list_api_keys(who)

    async def revoke_api_key(who: Fresh, key_id: str) -> Response:
        gate.revoke_api_key(who, key_id)
        return Response(status_code=204)

    add("jwks", jwks)
    add("info", info)
    add("revocations", revocations)
    add("device_start", device_start)
    add("token", token)
    add("magic_link", magic_link)
    add("dev_login", dev_login, dependencies=[Depends(_dev_only)])
    add("revoke", revoke)
    add("temporal_token", temporal_token)
    add("me", me)
    add("add_recipient", add_recipient)
    add("revoke_recipient", revoke_recipient)
    add("party", party_view)
    add("devices", devices)
    add("add_device", add_device)
    add("remove_device", remove_device)
    add("create_api_key", create_api_key)
    add("api_keys", api_keys)
    add("revoke_api_key", revoke_api_key)

    async def sessions(who: Human, response: Response) -> m.SessionList:
        no_store(response)
        return gate.list_sessions(who)

    async def end_session(who: Fresh, session_id: str) -> Response:
        gate.end_session(who, session_id)
        return Response(status_code=204)

    async def confirm_start(who: Human, request: Request, response: Response) -> m.ConfirmStart:
        no_store(response)
        client = request.client.host if request.client is not None else ""
        return gate.confirm_start(who, client)

    async def confirm_status(who: Human, confirm_id: str, response: Response) -> m.ConfirmStatus:
        no_store(response)
        return gate.confirm_status(who, confirm_id)

    add("sessions", sessions)
    add("end_session", end_session)
    add("confirm_start", confirm_start)
    add("confirm_status", confirm_status)

    # ---------------------------------------------------------------- passkeys

    async def passkeys(who: Human, response: Response) -> m.PasskeyList:
        no_store(response)
        return gate.list_passkeys(who)

    async def passkey_options(who: Recent, response: Response) -> m.PasskeyCeremony:
        no_store(response)
        return gate.passkey_options(who)

    async def add_passkey(who: Fresh, body: m.PasskeyAdd, response: Response) -> m.PasskeyView:
        no_store(response)
        return gate.add_passkey(who, body)

    async def revoke_passkey(who: Fresh, passkey_id: str) -> Response:
        gate.revoke_passkey(who, passkey_id)
        return Response(status_code=204, headers={"Cache-Control": "no-store"})

    async def confirm_passkey_options(who: Human, response: Response) -> m.PasskeyCeremony:
        no_store(response)
        return gate.confirm_passkey_options(who)

    async def confirm_passkey(
        who: Human, body: m.PasskeyConfirm, response: Response
    ) -> m.PasskeyConfirmed:
        no_store(response)
        return gate.confirm_passkey(who, body)

    add("passkeys", passkeys)
    add("passkey_options", passkey_options)
    add("add_passkey", add_passkey)
    add("revoke_passkey", revoke_passkey)
    add("confirm_passkey_options", confirm_passkey_options)
    add("confirm_passkey", confirm_passkey)

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
        return gate.catalog_entry(gate.resolve_path(party, name, version))

    async def card(party: str, name: str, version: str) -> JSONResponse:
        return JSONResponse(gate.resolve_path(party, name, version).card)

    async def by_endpoint(endpoint: str) -> m.CatalogEntry:
        return gate.by_endpoint(endpoint)

    async def workers(who: PartyOrWorker, party: str, name: str, version: str) -> m.WorkersView:
        return await gate.workers(who, party, name, version)

    add("publish", publish)
    add("search", search)
    add("get_offering", get_offering)
    add("card", card)
    add("by_endpoint", by_endpoint)
    add("workers", workers)

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
        include: m.JobInclude | None = None,
    ) -> m.JobList:
        return gate.list_jobs(
            who.party, as_, state, cursor, limit, buyer_key=include == "buyer_key"
        )

    async def job(
        who: PartyOrWorker, job_id: str, include: m.JobInclude | None = None
    ) -> m.JobView:
        return gate.job_for(job_id, who.party).view(buyer_key=include == "buyer_key")

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

    async def job_key(
        who: Party, job_id: str, for_: Annotated[str | None, Query(alias="for")] = None
    ) -> m.JobKey:
        return gate.job_key(who, job_id, for_)

    async def job_keys(
        who: Party,
        lacking: str,
        for_: Annotated[str | None, Query(alias="for")] = None,
        cursor: str | None = None,
        limit: Annotated[int, Query(ge=1, le=JOB_KEYS_LIMIT_MAX)] = JOB_KEYS_LIMIT_MAX,
    ) -> m.JobKeyList:
        return gate.job_keys(who, lacking, for_, cursor, limit)

    async def share_job_keys(who: Party, body: m.ShareJobKeys) -> m.SharedJobKeys:
        return gate.share_job_keys(who, body)

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
    add("job_key", job_key)
    add("job_keys", job_keys)
    add("share_job_keys", share_job_keys)
    add("progress", progress, response_class=StreamingResponse)
    add("post_progress", post_progress)

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
    add("complete_artifact", complete_artifact)
    add("download_artifact", download_artifact)
    add("create_blob", create_blob)
    add("get_blob", get_blob)

    # ---------------------------------------------------------------- money & disputes

    async def balance(who: Party, currency: str = "USD") -> m.Balance:
        return await gate.balance(who.party, currency)

    async def topup(who: Party, body: m.Topup) -> m.TopupResult:
        return await gate.topup(who.party, body)

    async def disputes(
        who: ResolverHuman,
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
