"""Wire models of the gate HTTP API (seam ②).

One set of pydantic models serves the SDK, the gate and the MCP server (PRD §19.2). The
gate imports these; it never defines a twin. A change here is a change to the gate's
OpenAPI document, whose snapshot test must be updated in the same PR.

Conventions:
* Amounts are ``amount_minor: int`` + ``currency``; ``price`` is a display string.
* Times are timezone-aware UTC datetimes.
* Opaque Temporal payloads travel as :class:`WirePayload` -- the serialized
  ``temporal.api.common.v1.Payload`` proto, base64. The gate relays them byte for byte
  and never decodes them; they are ciphertext.
* Every model forbids unknown fields, so a client and gate that drift fail loudly.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "API_VERSION",
    "AcceptRequest",
    "AccessToken",
    "ArtifactComplete",
    "ArtifactCreate",
    "ArtifactUpload",
    "Balance",
    "BlobCreate",
    "BlobUrl",
    "CatalogEntry",
    "CatalogList",
    "CatalogSummary",
    "DeliveryView",
    "DevLogin",
    "DeviceStart",
    "DownloadUrl",
    "ErrorBody",
    "Escrow",
    "HireRequest",
    "HireView",
    "HoldClaims",
    "JobList",
    "JobState",
    "JobView",
    "Me",
    "PartyView",
    "ProgressEvent",
    "ProgressPost",
    "ProgressSource",
    "PublishRequest",
    "QuoteRequest",
    "QuoteStatus",
    "QuoteView",
    "RejectRequest",
    "ResolveRequest",
    "ResultView",
    "SellerJobView",
    "SetKey",
    "StartRequest",
    "TemporalToken",
    "TemporalTokenRequest",
    "TokenRequest",
    "TokenResponse",
    "Topup",
    "TopupResult",
    "UploadPart",
    "WirePayload",
]

API_VERSION = "v1"


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ErrorBody(_M):
    """Every non-2xx response. ``code`` is stable and machine-readable."""

    code: str
    message: str
    detail: dict[str, Any] | None = None


# --------------------------------------------------------------------------- auth


class DeviceStart(_M):
    """RFC 8628 device authorization response (``POST /v1/auth/device``)."""

    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str
    interval: int
    expires_in: int


class TokenRequest(_M):
    """``POST /v1/auth/token``."""

    grant_type: Literal["urn:ietf:params:oauth:grant-type:device_code", "refresh_token"]
    device_code: str | None = None
    refresh_token: str | None = None
    #: First login only: the handle to claim (the party id and Temporal namespace).
    handle: str | None = None


class DevLogin(_M):
    """``POST /v1/auth/dev-login`` -- only when the gate runs with ``KONTRA_DEV=1``. Creates
    the party if missing and returns tokens at once, with play money (§7.4)."""

    handle: str


class TokenResponse(_M):
    access_token: str  # gate API JWT, aud "kontra:api"
    token_type: Literal["Bearer"] = "Bearer"  # noqa: S105
    expires_in: int
    refresh_token: str
    party: str
    #: The ``auth_time`` of the human session this refresh token descends from.
    auth_time: datetime


class AccessToken(_M):
    access_token: str
    expires_in: int


class TemporalTokenRequest(_M):
    """``POST /v1/auth/temporal-token``. There is one Temporal role, ``worker`` (a buyer is a
    worker of its own namespace too). No token can reach Temporal's Operator API: endpoints
    are created by the platform when a fresh human session publishes (decisions I3)."""

    role: Literal["worker"] = "worker"


class TemporalToken(_M):
    token: str  # JWT for the Temporal frontend; default claim mapper format
    expires_at: datetime
    namespace: str
    temporal_address: str
    role: Literal["worker"] = "worker"


class Me(_M):
    party: str
    namespace: str
    public_key: str | None
    temporal_address: str
    created_at: datetime
    dev: bool = False


class SetKey(_M):
    """``PUT /v1/me/key``: the party's age X25519 recipient (``age1...``). Needs a fresh
    human session: the key decides who can read future deliveries."""

    public_key: str


class PartyView(_M):
    party: str
    public_key: str | None
    released: int = 0
    refunded: int = 0
    disputed: int = 0


# --------------------------------------------------------------------------- catalog


class PublishRequest(_M):
    """``POST /v1/catalog``: the full contract document (with ``x-kontra``), as JSON. Needs a
    human session younger than the gate's freshness window (§10.4); the gate then creates
    the Nexus endpoint itself, targeting the caller's namespace and the offering's task
    queue, under a fresh random name (decisions I1)."""

    document: dict[str, Any]


class CatalogEntry(_M):
    offering: str  # acme/enrich@1.2.0
    party: str
    name: str  # acme/enrich
    version: str
    description: str
    pricing_hint: str | None = None
    contract_hash: str
    document: dict[str, Any]
    card: dict[str, Any]
    endpoint: str  # opaque, assigned by the catalog; never derived from the offering
    service: str
    task_queue: str
    public_key: str
    published_at: datetime
    retired: bool = False


class CatalogSummary(_M):
    offering: str
    name: str
    version: str
    description: str
    pricing_hint: str | None = None
    contract_hash: str
    seller: PartyView


class CatalogList(_M):
    items: list[CatalogSummary]
    next_cursor: str | None = None


# --------------------------------------------------------------------------- quotes & jobs


class WirePayload(_M):
    """A ``temporal.api.common.v1.Payload``, serialized and base64-encoded (standard alphabet)."""

    b64: str


class QuoteRequest(_M):
    """``POST /v1/quotes``. ``offering`` is a reference (``acme/enrich``, ``@1``, exact)."""

    offering: str
    input: WirePayload


class QuoteStatus(StrEnum):
    OK = "ok"
    REJECTED = "rejected"
    BUSY = "busy"


class QuoteView(_M):
    quote_id: str | None  # None unless status == ok
    offering: str  # resolved, exact
    status: QuoteStatus
    amount_minor: int | None = None
    currency: str | None = None
    price: str | None = None  # "12.40"
    max_duration_seconds: int | None = None
    reason: str | None = None
    eta_seconds: int | None = None
    expires_at: datetime | None = None
    contract_hash: str
    endpoint: str
    service: str
    seller_public_key: str


class HireRequest(_M):
    """``POST /v1/jobs``. Refused with ``price_above_max`` when the quote exceeds
    ``max_price_minor``; refused with ``insufficient_funds`` when the lock fails."""

    quote_id: str
    max_price_minor: int | None = None


class JobState(StrEnum):
    """A2A task states (D12). ``input-required`` is the quote step."""

    INPUT_REQUIRED = "input-required"
    SUBMITTED = "submitted"
    WORKING = "working"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class Escrow(StrEnum):
    HELD = "held"
    RELEASED = "released"
    REFUNDED = "refunded"
    DISPUTED = "disputed"
    RESOLVED = "resolved"


class HireView(_M):
    job_id: str
    hold_token: str
    offering: str
    endpoint: str
    service: str
    workflow_id: str  # job-<job_id>, in the seller's namespace
    operation_id: str  # the standalone Nexus operation id to use in the buyer's namespace
    contract_hash: str
    amount_minor: int
    currency: str
    max_duration_seconds: int
    seller_public_key: str
    expires_at: datetime  # the hold token's expiry: start before this


class DeliveryView(_M):
    sha256: str  # of the result payload as stored in history (ciphertext)
    size: int
    delivered_at: datetime


class JobView(_M):
    job_id: str
    offering: str
    buyer: str
    seller: str
    state: JobState
    escrow: Escrow
    amount_minor: int
    currency: str
    price: str
    created_at: datetime
    started_at: datetime | None = None
    deadline: datetime  # refund if not delivered by then (max_duration + grace)
    delivery: DeliveryView | None = None
    review_deadline: datetime | None = None
    reason: str | None = None  # failure / cancel / dispute reason, platform-observable only
    operation_id: str
    workflow_id: str


class StartRequest(_M):
    """``POST /v1/jobs/{id}/start``: the buyer's input, already encrypted to the seller.
    The gate starts the standalone Nexus operation in the BUYER's namespace on its behalf,
    with the hold token -- for clients without a Temporal connection (the console)."""

    input: WirePayload


class ResultView(_M):
    """``GET /v1/jobs/{id}/result``: the delivery as stored (ciphertext to the buyer)."""

    job_id: str
    payload: WirePayload
    sha256: str
    size: int
    delivered_at: datetime


class SellerJobView(_M):
    """``GET /v1/jobs/{id}/seller`` -- what a seller's worker needs on a cache miss."""

    job_id: str
    offering: str
    buyer_public_key: str
    contract_hash: str
    max_duration_seconds: int


class JobList(_M):
    items: list[JobView]
    next_cursor: str | None = None


class AcceptRequest(_M):
    pass


class RejectRequest(_M):
    reason: str = Field(min_length=1, max_length=2000)


class ResolveRequest(_M):
    """Resolver only (``POST /v1/admin/jobs/{id}/resolve``)."""

    buyer_minor: int
    seller_minor: int
    note: str = ""


# --------------------------------------------------------------------------- progress


class ProgressSource(StrEnum):
    HEARTBEAT = "heartbeat"
    HISTORY = "history"


class ProgressEvent(_M):
    """One event of ``GET /v1/jobs/{id}/progress`` (SSE, ``data:`` is this JSON).

    ``heartbeat`` events carry the seller's heartbeat details encrypted to the buyer
    (``ciphertext_b64``, age). ``history`` events carry platform-observable metadata."""

    seq: int
    at: datetime
    source: ProgressSource
    activity: str | None = None
    ciphertext_b64: str | None = None
    state: JobState | None = None
    last_event: str | None = None
    activities_completed: int | None = None
    activities_running: list[str] | None = None
    continue_as_new_count: int | None = None


class ProgressPost(_M):
    """``POST /v1/jobs/{id}/progress`` from the seller's worker."""

    at: datetime
    activity: str
    ciphertext_b64: str


# --------------------------------------------------------------------------- artifacts & blobs


class ArtifactCreate(_M):
    """``POST /v1/artifacts``. ``job_id`` is set for a seller's output (the gate refuses
    ``size`` above the offering's ``max_output_bytes`` before anything is stored) and
    unset for a buyer's input. ``size`` is the ciphertext size."""

    size: int = Field(ge=0)
    content_type: str = "application/octet-stream"
    job_id: str | None = None


class UploadPart(_M):
    number: int
    url: str
    size: int


class ArtifactUpload(_M):
    key: str = Field(pattern=r"^[0-9a-f]{32}$")  # opaque, gate-assigned (see kontra.types.Artifact)
    upload_id: str | None  # None: single PUT to parts[0]
    part_size: int
    parts: list[UploadPart]
    expires_at: datetime


class ArtifactComplete(_M):
    """``POST /v1/artifacts/{key}/complete``."""

    upload_id: str | None
    etags: list[str]
    ciphertext_sha256: str


class DownloadUrl(_M):
    url: str
    size: int
    expires_at: datetime


class BlobCreate(_M):
    """Claim-check (``POST /v1/blobs``): content-addressed by the ciphertext's sha256."""

    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)


class BlobUrl(_M):
    sha256: str
    url: str  # presigned PUT (create) or GET (read)
    exists: bool = False
    expires_at: datetime


# --------------------------------------------------------------------------- money


class Balance(_M):
    party: str
    currency: str
    available_minor: int
    held_minor: int
    available: str
    held: str


class Topup(_M):
    amount_minor: int = Field(gt=0)
    currency: str = "USD"


class TopupResult(_M):
    """Dev: credited at once. Production: a checkout URL to complete (Phase 7)."""

    credited: bool
    balance: Balance | None = None
    checkout_url: str | None = None


# --------------------------------------------------------------------------- hold token


class HoldClaims(_M):
    """Claims of the hold token (D10, §10.5): a JWT signed by the gate (ES256, ``kid`` in
    the JWKS at ``/.well-known/jwks.json``), carried in the Nexus header ``kontra-hold``
    and verified offline by the seller's handler."""

    iss: str
    aud: Literal["kontra:hold"]
    sub: str  # job_id
    iat: int
    exp: int
    job_id: str
    offering: str  # acme/enrich@1.2.0
    seller: str
    seller_ns: str
    buyer: str
    buyer_ns: str
    buyer_key: str  # age1... -- the job's output recipient (§7.4: travels with the job)
    amount_minor: int
    currency: str
    contract_hash: str
    max_duration_seconds: int
