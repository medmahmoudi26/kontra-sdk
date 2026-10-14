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
* Every list, dict and free string has a ceiling (see "Bounds" below).
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    StringConstraints,
    field_validator,
    model_serializer,
    model_validator,
)

__all__ = [
    "API_KEY_ID_PATTERN",
    "API_KEY_LABEL_PATTERN",
    "API_KEY_PATTERN",
    "API_KEY_PREFIX",
    "API_VERSION",
    "DEVICE_GRANT",
    "EMAIL_MAX",
    "HANDOFF_PATTERN",
    "HANDOFF_TTL_SECONDS",
    "MAX_API_KEYS",
    "MAX_PARTS",
    "REVOCATION_WINDOW_MAX",
    "USER_CODE_ALPHABET",
    "USER_CODE_PATTERN",
    "AcceptRequest",
    "AccessToken",
    "AddKey",
    "ApiKeyCreate",
    "ApiKeyCreated",
    "ApiKeyList",
    "ApiKeyView",
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
    "GateInfo",
    "HireRequest",
    "HireView",
    "HoldClaims",
    "JobList",
    "JobState",
    "JobView",
    "MagicLinkRequest",
    "MagicLinkSent",
    "Me",
    "PartyKey",
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
    "Revocations",
    "SellerJobView",
    "SessionList",
    "SessionView",
    "StartRequest",
    "TemporalToken",
    "TemporalTokenRequest",
    "TokenRequest",
    "TokenResponse",
    "Topup",
    "TopupResult",
    "UploadPart",
    "WirePayload",
    "is_api_key",
    "is_api_key_id",
    "is_email",
    "is_user_code",
]

API_VERSION = "v1"

# --------------------------------------------------------------------------- bounds
#
# A gate parses a request body into these models before anything else looks at it, and a
# validation error costs far more memory than the bytes that caused it (a 1 MiB body of
# bad ETags was ~540 MiB of errors). So every list, dict and free string has a ceiling,
# and nothing can make one request produce more than a handful of errors: a list stops at
# its first item past ``max_length`` with a single error, the one list a request carries
# (``ArtifactComplete.etags``) is refused whole at its first bad item, and unknown keys
# are a few errors at most (:class:`_M`). The ceilings are not the rules: each is at or above
# the largest legitimate value, with room, so a gate's own checks (a handle's shape, a
# note's 2000 characters, the 2 MiB payload) still answer with their own error codes.
# Response models carry the same ceilings, above anything a gate produces.

#: Identifiers and short strings: handles and namespaces (32 characters; Temporal's own
#: ID limit is 255 in Kontra's server), offering references (95), endpoint names, job and
#: quote ids, hashes, currencies, prices, age public keys (62), content types (255).
_NAME_MAX = 255
#: Opaque third-party tokens and URLs: JWTs (ES256, under 2 KiB), refresh and device
#: secrets (43), presigned bucket URLs (with a session token, past 2 KiB), S3 upload ids.
_LONG_MAX = 16 * 1024
#: Human text a gate stores on a job: a dispute reason (2000, :class:`RejectRequest`), a
#: seller's quote reason (1000, ``kontra.contract.quote``), escrow's own reasons.
_TEXT_MAX = 2000
#: A resolver's note: room above the gate's own 2000-character rule (``invalid_note``).
_NOTE_MAX = 8 * 1024
#: A contract's description and pricing hint (``kontra.contract.offering``).
_DESCRIPTION_MAX = 2000
_PRICING_HINT_MAX = 200
#: An error message. A contract error names the JSON path of the offence, which can be as
#: long as the 256 KiB canonical document allows (``kontra.contract.validate``).
_ERROR_MESSAGE_MAX = 512 * 1024
#: Top-level members of a JSON object carried whole: a contract document has five, an
#: A2A card about a dozen, an error's detail one or two.
_MEMBERS_MAX = 64
#: Parts of a multipart upload: the S3 limit, which the gate plans within
#: (``gate.artifacts.service.MAX_PARTS``).
MAX_PARTS = 10_000
#: An ETag as a bucket returns it (S3's: a quoted MD5, 34 characters), printable ASCII.
_ETAG_MAX = 256
#: A page of a list route (the gates serve at most 200 items).
_PAGE_MAX = 1000
#: A serialized Temporal payload: twice the 2 MiB blob limit of Temporal's default and of
#: Kontra's server (``limit.blobSize.error``), base64. A gate's own limit is lower.
_PAYLOAD_B64_MAX = 4 * -(-(4 * 1024 * 1024) // 3)
#: A progress event's ciphertext: twice the gates' 64 KiB heartbeat limit, base64.
_PROGRESS_B64_MAX = 4 * -(-(128 * 1024) // 3)
#: Pending activities of a run and their type names, at Temporal's default limits (2000
#: and 1000 characters; Kontra's server sets 1000 and 255).
_PENDING_ACTIVITIES_MAX = 2000
_ACTIVITY_MAX = 1000

_Name = Annotated[str, StringConstraints(max_length=_NAME_MAX)]
_Long = Annotated[str, StringConstraints(max_length=_LONG_MAX)]
_Text = Annotated[str, StringConstraints(max_length=_TEXT_MAX)]
_Activity = Annotated[str, StringConstraints(max_length=_ACTIVITY_MAX)]
_ETag = Annotated[str, StringConstraints(max_length=_ETAG_MAX, pattern=r"^[\x20-\x7e]*$")]
#: What ``_ETag`` accepts, for the whole-list check (:meth:`ArtifactComplete._one_error`).
_ETAG_TEXT = re.compile(rf"[\x20-\x7e]{{0,{_ETAG_MAX}}}", re.ASCII)

#: Unknown keys pydantic reports one by one (``extra_forbidden``, each at its own location):
#: a few, each short. More, or a longer one, are one error naming the first few.
_UNKNOWN_REPORTED = 8
_UNKNOWN_KEY_REPORTED = 64
_UNKNOWN_NAMED = 3

# --------------------------------------------------------------------------- credentials
#
# What people type (an email address) and what the gate mints (keys, ids, user codes) is
# checked with anchored, ASCII-only patterns: pydantic's ``pattern`` (Rust regex, where ``$``
# is the end of the text, never before a trailing newline) or ``re.fullmatch``.

#: RFC 8628 §6.1 user codes: no vowels (no words), no look-alikes; ``BCDF-GHJK``.
USER_CODE_ALPHABET = "BCDFGHJKLMNPQRSTVWXZ"
USER_CODE_PATTERN = rf"^[{USER_CODE_ALPHABET}]{{4}}-[{USER_CODE_ALPHABET}]{{4}}$"
#: ``kontra_sk_`` + base64url(32 random bytes), unpadded. The gate stores its sha256 only.
API_KEY_PREFIX = "kontra_sk_"
API_KEY_PATTERN = rf"^{API_KEY_PREFIX}[A-Za-z0-9_-]{{43}}$"
#: ``key_`` + 16 random hex: never derived from the key.
API_KEY_ID_PATTERN = r"^key_[0-9a-f]{16}$"
#: A key's label: 1 to 64 printable ASCII characters.
API_KEY_LABEL_PATTERN = r"^[\x20-\x7e]{1,64}$"
#: The live keys a party may hold at once (the 26th is ``409 too_many_keys``).
MAX_API_KEYS = 25
#: A sign-in handoff: base64url(32 random bytes), unpadded. A gate whose link page has just
#: approved a sign-in sends the browser on to its console with one, in the URL fragment; the
#: console trades it once, within minutes, for that browser's own session (``handoff`` grant).
HANDOFF_PATTERN = r"^[A-Za-z0-9_-]{43}$"
#: How long a handoff may wait to be traded.
HANDOFF_TTL_SECONDS = 120
#: RFC 5321's path limit, and its local part's.
EMAIL_MAX = 254
_EMAIL_LOCAL_MAX = 64
#: A plain ``local@domain``: dot-separated atoms of letters, digits and ``_%+-`` (no quoted
#: strings, comments or display names), and a DNS name of at least two labels whose last
#: is alphabetic or an IDNA ``xn--`` label (so no IP literal, bracketed or not). Narrower
#: than RFC 5322 on purpose: the address reaches an SMTP envelope, a mail header and HTML.
_EMAIL = re.compile(
    r"[A-Za-z0-9_%+-]+(?:\.[A-Za-z0-9_%+-]+)*"
    r"@(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
    r"(?:[A-Za-z]{2,63}|xn--[A-Za-z0-9-]{1,59})",
    re.ASCII,
)
_API_KEY = re.compile(API_KEY_PATTERN, re.ASCII)
_API_KEY_ID = re.compile(API_KEY_ID_PATTERN, re.ASCII)
_USER_CODE = re.compile(USER_CODE_PATTERN, re.ASCII)

# Each with its ceiling too, so a gate refuses an oversized value before any pattern runs.
_Email = Annotated[str, StringConstraints(strip_whitespace=True, max_length=EMAIL_MAX)]
_UserCode = Annotated[str, StringConstraints(max_length=9, pattern=USER_CODE_PATTERN)]
_ApiKey = Annotated[
    str, StringConstraints(max_length=len(API_KEY_PREFIX) + 43, pattern=API_KEY_PATTERN)
]
_ApiKeyId = Annotated[str, StringConstraints(max_length=20, pattern=API_KEY_ID_PATTERN)]
_KeyLabel = Annotated[str, StringConstraints(max_length=64, pattern=API_KEY_LABEL_PATTERN)]
_Handoff = Annotated[str, StringConstraints(max_length=43, pattern=HANDOFF_PATTERN)]


def is_email(text: str) -> bool:
    """``text`` is a plain ``local@domain`` address :class:`MagicLinkRequest` accepts (after
    its whitespace is stripped; this takes it as given)."""
    if len(text) > EMAIL_MAX or _EMAIL.fullmatch(text) is None:
        return False
    return len(text.rpartition("@")[0]) <= _EMAIL_LOCAL_MAX


def is_api_key(text: str) -> bool:
    """``text`` has the shape of a Kontra API key (whether the gate knows it is another matter)."""
    return _API_KEY.fullmatch(text) is not None


def is_api_key_id(text: str) -> bool:
    return _API_KEY_ID.fullmatch(text) is not None


def is_user_code(text: str) -> bool:
    """``text`` is a device user code exactly as the gate issues it (``BCDF-GHJK``)."""
    return _USER_CODE.fullmatch(text) is not None


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="before")
    @classmethod
    def _unknown_keys_are_few_errors(cls, data: Any) -> Any:
        """``extra="forbid"`` reports every unknown key as an error of its own, located by
        the key: 4 MiB of stray keys cost a gate ~300 MiB, and a megabyte-long key ends up
        in the error's location and in the gate's message. A few short ones are left to
        pydantic's own report; anything more is refused here, before any field is
        validated, as one error that names the first few by their first characters."""
        if isinstance(data, dict):
            unknown = [key for key in data if key not in cls.model_fields]
            if len(unknown) > _UNKNOWN_REPORTED or any(
                not isinstance(key, str) or len(key) > _UNKNOWN_KEY_REPORTED for key in unknown
            ):
                named = ", ".join(repr(str(key)[:40]) for key in unknown[:_UNKNOWN_NAMED])
                more = len(unknown) - _UNKNOWN_NAMED
                rest = f" and {more} more" if more > 0 else ""
                raise ValueError(f"unknown field(s): {named}{rest}")
        return data


class ErrorBody(_M):
    """Every non-2xx response. ``code`` is stable and machine-readable."""

    code: _Name
    message: Annotated[str, StringConstraints(max_length=_ERROR_MESSAGE_MAX)]
    detail: dict[str, Any] | None = Field(default=None, max_length=_MEMBERS_MAX)


# --------------------------------------------------------------------------- auth


class DeviceStart(_M):
    """RFC 8628 device authorization response (``POST /v1/auth/device``)."""

    device_code: _Long
    user_code: _Name
    verification_uri: _Long
    verification_uri_complete: _Long
    interval: int
    expires_in: int


#: The RFC 8628 grant type, polled with the ``device_code`` of :class:`DeviceStart`.
DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"


class TokenRequest(_M):
    """``POST /v1/auth/token``. Each grant takes exactly its own fields
    (:meth:`grant_error`): ``device_code`` (and, on a first login, ``handle``) for the device
    grant, ``refresh_token`` for ``refresh_token``, ``api_key`` for ``api_key``, ``handoff``
    for ``handoff``."""

    grant_type: Literal[
        "urn:ietf:params:oauth:grant-type:device_code", "refresh_token", "api_key", "handoff"
    ]
    # The credentials stay out of ``repr``: a request in a log line or a traceback with
    # locals must not carry them.
    device_code: _Long | None = Field(default=None, repr=False)
    refresh_token: _Long | None = Field(default=None, repr=False)
    #: First login only: the handle to claim (the party id and Temporal namespace).
    handle: _Name | None = None
    #: The ``api_key`` grant's key (``kontra_sk_`` + 43 characters, :func:`is_api_key`).
    api_key: _ApiKey | None = Field(default=None, repr=False)
    #: The ``handoff`` grant's one-time code (:data:`HANDOFF_PATTERN`).
    handoff: _Handoff | None = Field(default=None, repr=False)

    def grant_error(self) -> str | None:
        """Why the fields present do not fit ``grant_type``, or None when they do. A gate
        answers a misfit as RFC 6749 does, 400 ``invalid_request`` with this message; a body
        that is not a TokenRequest at all is the usual 422."""
        present = {
            name
            for name in ("device_code", "refresh_token", "handle", "api_key", "handoff")
            if getattr(self, name) is not None
        }
        if self.grant_type == DEVICE_GRANT:
            if "device_code" not in present or present - {"device_code", "handle"}:
                return "the device_code grant takes device_code only"
        elif self.grant_type == "refresh_token":
            if present != {"refresh_token"}:
                return "the refresh_token grant takes refresh_token only"
        elif self.grant_type == "handoff":
            if present != {"handoff"}:
                return "the handoff grant takes handoff only"
        elif present != {"api_key"}:
            return "the api_key grant takes api_key only"
        return None


class DevLogin(_M):
    """``POST /v1/auth/dev-login`` -- only when the gate runs with ``KONTRA_DEV=1``. Creates
    the party if missing and returns tokens at once, with play money (§7.4)."""

    handle: _Name


class TokenResponse(_M):
    access_token: _Long  # gate API JWT, aud "kontra:api"
    token_type: Literal["Bearer"] = "Bearer"  # noqa: S105
    expires_in: int
    #: The next refresh token (the gate rotates it on every use). None for the ``api_key``
    #: grant: refreshing that session is exchanging the key again.
    refresh_token: _Long | None = Field(default=None, repr=False)
    party: _Name
    #: The ``auth_time`` of the human session this refresh token descends from; for an API
    #: key, the key's ``created_at`` (an API-key session is never fresh, whatever its age).
    auth_time: datetime


class AccessToken(_M):
    access_token: _Long
    expires_in: int


class MagicLinkRequest(_M):
    """``POST /v1/auth/magic``: email a sign-in link that approves the device authorization
    ``device_user_code`` (decisions I19). The gate lowercases ``email``. It answers 202
    :class:`MagicLinkSent` whatever happens next -- an address that is not invited, has no
    account, or a code that is not pending all look the same -- so the route says nothing
    about who is on the allowlist; 429 when rate-limited (per address and per client)."""

    email: _Email
    #: The ``user_code`` of the :class:`DeviceStart` to approve, as the gate issued it.
    device_user_code: _UserCode

    @field_validator("email")
    @classmethod
    def _plain_address(cls, value: str) -> str:
        if not is_email(value):  # the value stays out of the message
            raise ValueError("not a plain email address (local@domain)")
        return value


class MagicLinkSent(_M):
    """``POST /v1/auth/magic`` (202). The same answer whether or not a mail went out."""

    status: Literal["sent"] = "sent"


class ApiKeyCreate(_M):
    """``POST /v1/keys`` (fresh human session): an API key for a program."""

    label: _KeyLabel


class ApiKeyCreated(_M):
    """``POST /v1/keys`` (201). ``key`` is shown this once; the gate keeps only its sha256."""

    id: _ApiKeyId
    label: _KeyLabel
    created_at: datetime
    key: _ApiKey = Field(repr=False)


class ApiKeyView(_M):
    """One key of ``GET /v1/keys``; never the key itself."""

    id: _ApiKeyId
    label: _KeyLabel
    created_at: datetime
    last_used_at: datetime | None = None  # the last exchange, to the minute
    revoked_at: datetime | None = None


class ApiKeyList(_M):
    keys: list[ApiKeyView] = Field(max_length=_PAGE_MAX)


class TemporalTokenRequest(_M):
    """``POST /v1/auth/temporal-token``. There is one Temporal role, ``worker`` (a buyer is a
    worker of its own namespace too). No token can reach Temporal's Operator API: endpoints
    are created by the platform when a fresh human session publishes (decisions I3)."""

    role: Literal["worker"] = "worker"


class TemporalToken(_M):
    token: _Long  # JWT for the Temporal frontend; default claim mapper format
    expires_at: datetime
    namespace: _Name
    temporal_address: _Long
    role: Literal["worker"] = "worker"


#: The most sessions :class:`Revocations` lists. Far above what a gate revokes in a window;
#: a list that would be longer is a flood, and a gate answers it with the newest this many.
REVOCATION_WINDOW_MAX = 200_000
_SESSION_DIGEST = r"^[0-9a-f]{64}$"


class Revocations(_M):
    """``GET /v1/auth/revocations`` (public): the sessions revoked in the last
    ``window_seconds``, each as the sha256 (hex) of its id -- the ``sid`` claim of every token
    the gate mints. The window outlasts the longest Temporal worker token (15 minutes), so a
    worker token whose session is revoked is on this list until it expires. Kontra's Temporal
    server polls it and refuses those tokens (decisions I19): revoking an API key or a login
    reaches Temporal within seconds, not when the token expires. A digest says nothing to
    whoever reads the list without the token."""

    window_seconds: int
    sessions: list[Annotated[str, StringConstraints(pattern=_SESSION_DIGEST)]] = Field(
        max_length=REVOCATION_WINDOW_MAX
    )


_SESSION_ID = r"^[0-9a-f]{32}$"
_SessionId = Annotated[str, StringConstraints(max_length=32, pattern=_SESSION_ID)]


class SessionView(_M):
    """One of the party's sign-ins (``GET /v1/sessions``): a person's login that can still get
    tokens, never an API key's (those are ``/v1/keys``). ``via`` is how the person proved who
    they are (``device:email``, ``device:github``, ``device:dev``, ``dev-login``);
    ``current`` marks the session asking. One the person does not recognise -- a device
    login they were tricked into approving -- is ended with ``DELETE /v1/sessions/{id}``."""

    id: _SessionId
    via: _Name
    auth_time: datetime
    created_at: datetime
    current: bool = False


class SessionList(_M):
    sessions: list[SessionView] = Field(max_length=_PAGE_MAX)


class GateInfo(_M):
    """``GET /v1/info`` (public): what this gate offers before anyone signs in, so a client
    shows only what works here. ``dev``: ``KONTRA_DEV=1`` -- dev login
    (``POST /v1/auth/dev-login``) exists and new parties get play money; never on
    production. ``email_sign_in``: ``POST /v1/auth/magic`` mails sign-in links (the only way
    in on production). ``github_sign_in``: the device page signs in with GitHub."""

    dev: bool
    email_sign_in: bool
    github_sign_in: bool


#: The most recipients one party may hold (decisions I7). age writes one ~100-byte stanza
#: per recipient into every header, so the set is capped to keep that overhead bounded;
#: one key per fleet, never one per pod, keeps a real deployment far below it.
RECIPIENTS_MAX = 16


class PartyKey(_M):
    """One age X25519 recipient of a party: a machine, or a fleet's mounted key. Revoked
    keys are not listed; the gate keeps the row (a dispute may need the key on record)."""

    label: _Name
    public_key: _Name
    added_at: datetime


class Me(_M):
    party: _Name
    namespace: _Name
    #: Every unrevoked recipient of this party, newest last. Empty for a party that has
    #: never added one -- a buyer never needs to, since a job's key is made at hire.
    keys: list[PartyKey] = Field(default_factory=list, max_length=RECIPIENTS_MAX)
    temporal_address: _Long
    created_at: datetime
    dev: bool = False


class AddKey(_M):
    """``POST /v1/me/recipients``: one more age X25519 recipient (``age1...``) for this
    party, under a label that says which machine or fleet holds it. Needs a fresh human
    session: a recipient can read every delivery made after it is added.

    Adding never replaces: ``kontra login`` on a second machine adds that machine. The
    party is capped at :data:`RECIPIENTS_MAX`."""

    label: _Name
    public_key: _Name


class PartyView(_M):
    party: _Name
    #: The party's unrevoked recipients: what a counterparty encrypts to. Public by
    #: construction -- they are public keys.
    recipients: list[_Name] = Field(default_factory=list, max_length=RECIPIENTS_MAX)
    released: int = 0
    refunded: int = 0
    disputed: int = 0


# --------------------------------------------------------------------------- catalog


class PublishRequest(_M):
    """``POST /v1/catalog``: the full contract document (with ``x-kontra``), as JSON. Needs a
    human session younger than the gate's freshness window (§10.4); the gate then creates
    the Nexus endpoint itself, targeting the caller's namespace and the offering's task
    queue, under a fresh random name (decisions I1)."""

    document: dict[str, Any] = Field(max_length=_MEMBERS_MAX)


class CatalogEntry(_M):
    offering: _Name  # acme/enrich@1.2.0
    party: _Name
    name: _Name  # acme/enrich
    version: _Name
    description: Annotated[str, StringConstraints(max_length=_DESCRIPTION_MAX)]
    pricing_hint: Annotated[str, StringConstraints(max_length=_PRICING_HINT_MAX)] | None = None
    contract_hash: _Name
    document: dict[str, Any] = Field(max_length=_MEMBERS_MAX)
    card: dict[str, Any] = Field(max_length=_MEMBERS_MAX)
    endpoint: _Name  # opaque, assigned by the catalog; never derived from the offering
    service: _Name
    task_queue: _Name
    #: The seller party's unrevoked recipients, as the catalog holds them NOW -- catalog
    #: data, not contract data: adding a fleet key must never force a republish (I7).
    recipients: list[_Name] = Field(default_factory=list, max_length=RECIPIENTS_MAX)
    published_at: datetime
    retired: bool = False


class CatalogSummary(_M):
    offering: _Name
    name: _Name
    version: _Name
    description: Annotated[str, StringConstraints(max_length=_DESCRIPTION_MAX)]
    pricing_hint: Annotated[str, StringConstraints(max_length=_PRICING_HINT_MAX)] | None = None
    contract_hash: _Name
    seller: PartyView


class CatalogList(_M):
    items: list[CatalogSummary] = Field(max_length=_PAGE_MAX)
    next_cursor: _Long | None = None


# --------------------------------------------------------------------------- quotes & jobs


class WirePayload(_M):
    """A ``temporal.api.common.v1.Payload``, serialized and base64-encoded (standard alphabet)."""

    b64: Annotated[str, StringConstraints(max_length=_PAYLOAD_B64_MAX)]


class QuoteRequest(_M):
    """``POST /v1/quotes``. ``offering`` is a reference (``acme/enrich``, ``@1``, exact)."""

    offering: _Name
    input: WirePayload


class QuoteStatus(StrEnum):
    OK = "ok"
    REJECTED = "rejected"
    BUSY = "busy"


class QuoteView(_M):
    quote_id: _Name | None  # None unless status == ok
    offering: _Name  # resolved, exact
    status: QuoteStatus
    amount_minor: int | None = None
    currency: _Name | None = None
    price: _Name | None = None  # "12.40"
    max_duration_seconds: int | None = None
    reason: _Text | None = None
    eta_seconds: int | None = None
    expires_at: datetime | None = None
    contract_hash: _Name
    endpoint: _Name
    service: _Name
    #: Every recipient the seller can read with: the buyer encrypts the Nexus input to all
    #: of them, so whichever worker picks the task up can decrypt it.
    seller_recipients: list[_Name] = Field(default_factory=list, max_length=RECIPIENTS_MAX)


class HireRequest(_M):
    """``POST /v1/jobs``. Refused with ``price_above_max`` when the quote exceeds
    ``max_price_minor``; refused with ``insufficient_funds`` when the lock fails."""

    quote_id: _Name
    max_price_minor: int | None = None
    #: The age recipient this job's result is encrypted to, made by the buyer at hire and
    #: kept next to the job record (``~/.kontra/jobs/<job_id>.key``). The gate signs THIS
    #: into the hold token's ``buyer_key``; it never consults the party's own keys for the
    #: buyer side, so hiring needs no key setup and a compromise is scoped to open jobs.
    buyer_key: _Name


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
    job_id: _Name
    hold_token: _Long
    offering: _Name
    endpoint: _Name
    service: _Name
    workflow_id: _Name  # job-<job_id>, in the seller's namespace
    operation_id: _Name  # the standalone Nexus operation id to use in the buyer's namespace
    contract_hash: _Name
    amount_minor: int
    currency: _Name
    max_duration_seconds: int
    #: The seller's recipients at hire: who the buyer encrypts the Nexus input to.
    seller_recipients: list[_Name] = Field(default_factory=list, max_length=RECIPIENTS_MAX)
    expires_at: datetime  # the hold token's expiry: start before this


class DeliveryView(_M):
    sha256: _Name  # of the result payload as stored in history (ciphertext)
    size: int
    delivered_at: datetime


class JobView(_M):
    job_id: _Name
    offering: _Name
    buyer: _Name
    seller: _Name
    state: JobState
    escrow: Escrow
    amount_minor: int
    currency: _Name
    price: _Name
    created_at: datetime
    started_at: datetime | None = None
    deadline: datetime  # refund if not delivered by then (max_duration + grace)
    delivery: DeliveryView | None = None
    review_deadline: datetime | None = None
    reason: _Text | None = None  # failure / cancel / dispute reason, platform-observable only
    operation_id: _Name
    workflow_id: _Name


class StartRequest(_M):
    """``POST /v1/jobs/{id}/start``: the buyer's input, already encrypted to the seller.
    The gate starts the standalone Nexus operation in the BUYER's namespace on its behalf,
    with the hold token -- for clients without a Temporal connection (the console)."""

    input: WirePayload


class ResultView(_M):
    """``GET /v1/jobs/{id}/result``: the delivery as stored (ciphertext to the buyer)."""

    job_id: _Name
    payload: WirePayload
    sha256: _Name
    size: int
    delivered_at: datetime


class SellerJobView(_M):
    """``GET /v1/jobs/{id}/seller`` -- what a seller's worker needs on a cache miss."""

    job_id: _Name
    offering: _Name
    #: The job's own buyer key, the one the hold token seals: made by the buyer at hire,
    #: not the buyer party's. The seller's side is unchanged -- it encrypts the result to
    #: whatever key the signed token names, and caches it per job.
    buyer_public_key: _Name
    contract_hash: _Name
    max_duration_seconds: int


class JobList(_M):
    items: list[JobView] = Field(max_length=_PAGE_MAX)
    next_cursor: _Long | None = None


class AcceptRequest(_M):
    pass


class RejectRequest(_M):
    reason: str = Field(min_length=1, max_length=2000)


class ResolveRequest(_M):
    """Resolver only (``POST /v1/admin/jobs/{id}/resolve``)."""

    buyer_minor: int
    seller_minor: int
    note: Annotated[str, StringConstraints(max_length=_NOTE_MAX)] = ""


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
    activity: _Activity | None = None
    ciphertext_b64: Annotated[str, StringConstraints(max_length=_PROGRESS_B64_MAX)] | None = None
    state: JobState | None = None
    last_event: _Name | None = None
    activities_completed: int | None = None
    activities_running: list[_Activity] | None = Field(
        default=None, max_length=_PENDING_ACTIVITIES_MAX
    )
    continue_as_new_count: int | None = None


class ProgressPost(_M):
    """``POST /v1/jobs/{id}/progress`` from the seller's worker."""

    at: datetime
    activity: _Activity
    ciphertext_b64: Annotated[str, StringConstraints(max_length=_PROGRESS_B64_MAX)]


# --------------------------------------------------------------------------- artifacts & blobs


class ArtifactCreate(_M):
    """``POST /v1/artifacts``. ``job_id`` is set for a seller's output (the gate refuses
    ``size`` above the offering's ``max_output_bytes`` before anything is stored) and
    unset for a buyer's input. ``size`` is the ciphertext size."""

    size: int = Field(ge=0)
    content_type: _Name = "application/octet-stream"
    job_id: _Name | None = None


class UploadPart(_M):
    number: int
    url: _Long
    size: int


class ArtifactUpload(_M):
    key: str = Field(pattern=r"^[0-9a-f]{32}$")  # opaque, gate-assigned (see kontra.types.Artifact)
    upload_id: _Long | None  # None: single PUT to parts[0]
    part_size: int
    parts: list[UploadPart] = Field(max_length=MAX_PARTS)
    expires_at: datetime


class ArtifactComplete(_M):
    """``POST /v1/artifacts/{key}/complete``. One ETag per planned part, in order (a
    single PUT: one, possibly empty)."""

    upload_id: _Long | None
    etags: list[_ETag] = Field(max_length=MAX_PARTS)
    ciphertext_sha256: _Name

    @field_validator("etags", mode="before")
    @classmethod
    def _one_error(cls, value: Any) -> Any:
        """Refuse a list with a bad item as one error naming the first, before pydantic
        reports every item on its own: under the cap, 10 000 bad items were 10 000 errors
        (~25 MiB at a gate for a 1 MiB body). A list past the cap is left to pydantic's own
        single ``too_long``; the item type still states the rule (and the schema)."""
        items: Sequence[object] = (
            cast("Sequence[object]", value) if isinstance(value, list | tuple) else ()
        )
        if len(items) <= MAX_PARTS:
            for i, item in enumerate(items):
                if not isinstance(item, str) or _ETAG_TEXT.fullmatch(item) is None:
                    raise ValueError(
                        f"etags[{i}]: an ETag is at most {_ETAG_MAX} printable ASCII characters"
                    )
        return value


class DownloadUrl(_M):
    url: _Long
    size: int
    expires_at: datetime


class BlobCreate(_M):
    """Claim-check (``POST /v1/blobs``): content-addressed by the ciphertext's sha256."""

    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)


class BlobUrl(_M):
    sha256: _Name
    url: _Long  # presigned PUT (create) or GET (read)
    exists: bool = False
    expires_at: datetime


# --------------------------------------------------------------------------- money


class Balance(_M):
    party: _Name
    currency: _Name
    available_minor: int
    held_minor: int
    available: _Name
    held: _Name


class Topup(_M):
    amount_minor: int = Field(gt=0)
    currency: _Name = "USD"


class TopupResult(_M):
    """Dev: credited at once. Production: a checkout URL to complete (Phase 7)."""

    credited: bool
    balance: Balance | None = None
    checkout_url: _Long | None = None


# --------------------------------------------------------------------------- hold token


class HoldClaims(_M):
    """Claims of the hold token (D10, §10.5): a JWT signed by the gate (ES256, ``kid`` in
    the JWKS at ``/.well-known/jwks.json``), carried in the Nexus header ``kontra-hold``
    and verified offline by the seller's handler."""

    iss: _Long
    aud: Literal["kontra:hold"]
    sub: _Name  # job_id
    iat: int
    exp: int
    job_id: _Name
    offering: _Name  # acme/enrich@1.2.0
    seller: _Name
    seller_ns: _Name
    buyer: _Name
    buyer_ns: _Name
    buyer_key: _Name  # age1... -- the job's output recipient (§7.4: travels with the job)
    amount_minor: int
    currency: _Name
    contract_hash: _Name
    max_duration_seconds: int
    #: The size, in bytes, of the input the quote priced, as the seller's handler receives
    #: it (``kontra._control.input_size``: a claim-check's stored ciphertext, else the
    #: payload). The handler refuses an ``execute`` input above it before fetching anything,
    #: so one Nexus attempt is always enough to answer (decisions I16). Absent from tokens
    #: of a gate that does not set it (then the handler's only bound is its codec's), and
    #: then left out of the claims entirely, so re-signing such a token gives the same bytes.
    input_size: int | None = Field(default=None, ge=0)
    #: The gate's refund deadline for the job, in Unix seconds rounded down: the hold's
    #: lock time plus ``max_duration`` plus the gate's own grace (``kontra.escrow``). The
    #: handler ends the job's run by it, so a late start never delivers after the gate
    #: refunded the job, whatever grace this gate is configured with (decisions I16).
    #: Absent from tokens of a gate that does not set it (the handler then assumes
    #: ``exp`` plus ``kontra.escrow.DEFAULT_GRACE``), and then left out of the claims, as
    #: ``input_size`` is.
    deadline: int | None = Field(default=None, ge=0)

    @model_serializer(mode="wrap")
    def _omit_absent_bound(
        self, handler: SerializerFunctionWrapHandler, info: SerializationInfo
    ) -> dict[str, Any]:
        data: dict[str, Any] = handler(self)
        for optional in ("input_size", "deadline"):
            if data.get(optional) is None:
                data.pop(optional, None)
        return data
