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

import json
import re
from collections.abc import Sequence
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Final, Literal, cast

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    JsonValue,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    StringConstraints,
    ValidationInfo,
    field_validator,
    model_serializer,
    model_validator,
)

from kontra.contract.offering import STREAMS_MAX, TOPIC_PATTERN, check_streams

__all__ = [
    "ANSWER_CONTEXT",
    "API_KEY_ID_PATTERN",
    "API_KEY_LABEL_PATTERN",
    "API_KEY_PATTERN",
    "API_KEY_PREFIX",
    "API_VERSION",
    "CEREMONY_ID_PATTERN",
    "CHALLENGE_MAX",
    "CONFIRMATION_REQUIRED",
    "CONFIRM_ID_PATTERN",
    "DEFAULT_ENGINE",
    "DEVICES_MAX",
    "DEVICE_ADDS_PER_HOUR",
    "DEVICE_GRANT",
    "DEVICE_IDLE_SECONDS",
    "DEVICE_LABEL_PATTERN",
    "EMAIL_MAX",
    "ENGINE_NAMES",
    "HANDOFF_PATTERN",
    "HANDOFF_TTL_SECONDS",
    "HIDDEN_DEVICE_LABEL",
    "MAX_API_KEYS",
    "MAX_PARTS",
    "MAX_PASSKEYS",
    "PASSKEY_ID_PATTERN",
    "PASSKEY_LABEL_PATTERN",
    "POLLERS_MAX",
    "RECENT_CONFIRMATION_SECONDS",
    "REVOCATION_WINDOW_MAX",
    "SHARE_ITEMS_MAX",
    "STREAM_BLOBS_MAX",
    "STREAM_ITEM_MAX",
    "STREAM_JOB_BUDGET",
    "STREAM_LISTENERS_MAX",
    "STREAM_TOPIC_BUDGET",
    "USER_CODE_ALPHABET",
    "USER_CODE_PATTERN",
    "WEBAUTHN_JSON_MAX",
    "WRAP_MAX",
    "AcceptRequest",
    "AccessToken",
    "AddDevice",
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
    "ConfirmStart",
    "ConfirmStatus",
    "ConfirmWith",
    "ConfirmationRequired",
    "DeliveryView",
    "DevLogin",
    "DeviceKind",
    "DeviceList",
    "DeviceStart",
    "DeviceView",
    "DownloadUrl",
    "EngineName",
    "ErrorBody",
    "Escrow",
    "GateInfo",
    "HireRequest",
    "HireView",
    "HoldClaims",
    "JobInclude",
    "JobKey",
    "JobKeyList",
    "JobKeyShare",
    "JobList",
    "JobState",
    "JobView",
    "KeyWrap",
    "MagicLinkRequest",
    "MagicLinkSent",
    "Me",
    "PartyKey",
    "PartyView",
    "PasskeyAdd",
    "PasskeyCeremony",
    "PasskeyConfirm",
    "PasskeyConfirmed",
    "PasskeyList",
    "PasskeyRevokedBy",
    "PasskeyTransport",
    "PasskeyView",
    "PollerInfo",
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
    "ShareJobKeys",
    "ShareRefusal",
    "ShareRefusalCode",
    "SharedJobKeys",
    "StartRequest",
    "StreamBlobUrls",
    "StreamBlobsCreate",
    "StreamEvent",
    "StreamEventKind",
    "StreamGapReason",
    "TaskQueueStats",
    "TaskQueueType",
    "TaskQueueTypeInfo",
    "TemporalToken",
    "TemporalTokenRequest",
    "TokenRequest",
    "TokenResponse",
    "Topup",
    "TopupResult",
    "UploadPart",
    "WirePayload",
    "WorkersView",
    "WrapIn",
    "is_api_key",
    "is_api_key_id",
    "is_ceremony_id",
    "is_confirm_id",
    "is_email",
    "is_passkey_id",
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
#: The largest item ciphertext a gate relays from a job's stream (decisions I31): the gate
#: fetches no item above it from the bucket, and records a ``too_large`` gap instead. Base64
#: in an event.
STREAM_ITEM_MAX = 256 * 1024
#: The most blobs one ``POST /v1/jobs/{id}/stream/blobs`` creates: the items of one poll's
#: answer (``kontra.codec.stream.STREAM_ITEMS_MAX``, decisions I39).
STREAM_BLOBS_MAX = 64
_STREAM_B64_MAX = 4 * -(-STREAM_ITEM_MAX // 3)
#: Relayed ciphertext a gate stores per topic of a job, and per job; past either, one
#: ``topic_budget`` or ``job_budget`` gap (the first item that did not fit), and nothing
#: more of that topic, or of the job, is relayed.
STREAM_TOPIC_BUDGET = 16 * 1024 * 1024
STREAM_JOB_BUDGET = 64 * 1024 * 1024
#: Concurrent stream listeners of one job (429 ``too_many_listeners`` past it).
STREAM_LISTENERS_MAX = 8
#: Pending activities of a run and their type names, at Temporal's default limits (2000
#: and 1000 characters; Kontra's server sets 1000 and 255).
_PENDING_ACTIVITIES_MAX = 2000
_ACTIVITY_MAX = 1000
#: A poller's identity and a worker deployment's name, at Temporal's default ID limit
#: (``limit.maxIDLength``, 1000 characters, which the frontend applies to a poll's identity
#: too; Kontra's server sets 255). A build id is at most 255 (``limit.workerBuildIdSize``).
_TEMPORAL_ID_MAX = 1000
#: The pollers one task-queue type lists: a gate keeps the most recently seen this many.
POLLERS_MAX = 1000

_Name = Annotated[str, StringConstraints(max_length=_NAME_MAX)]
#: A workflow-stream topic (``offering.yaml`` ``streams:``, ``kontra.contract.offering``).
_Topic = Annotated[str, StringConstraints(max_length=64, pattern=f"^{TOPIC_PATTERN}$")]
_Long = Annotated[str, StringConstraints(max_length=_LONG_MAX)]
_Text = Annotated[str, StringConstraints(max_length=_TEXT_MAX)]
_Activity = Annotated[str, StringConstraints(max_length=_ACTIVITY_MAX)]
_TemporalId = Annotated[str, StringConstraints(max_length=_TEMPORAL_ID_MAX)]
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


#: The validation context a client parses the gate's answers with, ``extra="ignore"``: a
#: field a newer gate adds to an answer is skipped instead of failing the call (0.2.0-0.2.3
#: refuse it, which is why a gate adds none to what they read). The gate's own requests are
#: validated strictly, unknown fields refused, as always.
ANSWER_CONTEXT: dict[str, bool] = {"kontra.answer": True}


def _is_answer(info: ValidationInfo) -> bool:
    context: object = info.context
    return isinstance(context, dict) and context.get("kontra.answer") is True  # pyright: ignore[reportUnknownMemberType]


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="before")
    @classmethod
    def _unknown_keys_are_few_errors(cls, data: Any, info: ValidationInfo) -> Any:
        """``extra="forbid"`` reports every unknown key as an error of its own, located by
        the key: 4 MiB of stray keys cost a gate ~300 MiB, and a megabyte-long key ends up
        in the error's location and in the gate's message. A few short ones are left to
        pydantic's own report; anything more is refused here, before any field is
        validated, as one error that names the first few by their first characters.

        Not for an answer a client reads (:data:`ANSWER_CONTEXT`): it ignores the fields it
        does not know, so a newer gate's answer parses (from 0.2.4)."""
        if isinstance(data, dict) and not _is_answer(info):
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
    #: key, the key's ``created_at`` (an API-key session never makes changes, whatever its
    #: age, and can never be confirmed: decisions I19, I23).
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
    """``POST /v1/keys`` (a person's confirmed session): an API key for a program."""

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
    are created by the platform when a person's confirmed session publishes (decisions I3)."""

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


# --------------------------------------------------------------------------- confirm it's you

#: ``ErrorBody.code`` of a change whose session is not confirmed (403, decisions I23): the
#: client confirms in place (``POST /v1/auth/confirm``), then sends the same call again.
CONFIRMATION_REQUIRED = "confirmation_required"
#: A confirmation's id: ``cfm_`` + 32 random hex. Opaque, never derived from anything; not a
#: secret (only the session that asked can read it: any other gets 404).
CONFIRM_ID_PATTERN = r"^cfm_[0-9a-f]{32}$"
_CONFIRM_ID = re.compile(CONFIRM_ID_PATTERN, re.ASCII)
_ConfirmId = Annotated[str, StringConstraints(max_length=36, pattern=CONFIRM_ID_PATTERN)]

#: How a session can confirm it's the person: ``email`` (a link mailed to the account's
#: sign-in address; the page asks for the code the asking screen shows), ``dev`` (a
#: ``KONTRA_DEV=1`` gate confirms a development sign-in at once), ``sign_in`` (nothing here
#: can confirm it: a new sign-in, which counts as a confirmation, is the only way).
ConfirmWith = Literal["email", "dev", "sign_in"]


def is_confirm_id(text: str) -> bool:
    """``text`` is a confirmation id exactly as the gate issues it (``cfm_`` + 32 hex)."""
    return _CONFIRM_ID.fullmatch(text) is not None


class ConfirmationRequired(_M):
    """``ErrorBody.detail`` of 403 ``confirmation_required``: what a client needs to confirm
    in place (``POST /v1/auth/confirm``, then the same call again)."""

    confirm_with: ConfirmWith
    #: How long a confirmation lasts on this gate (``KONTRA_CONFIRM_TTL``), for the copy.
    confirm_ttl_seconds: int = Field(ge=1)


class ConfirmStart(_M):
    """``POST /v1/auth/confirm`` (201): a confirmation for the session that asked.

    ``channel="email"``, ``status="pending"``: show ``user_code`` on this screen only (the
    mail never carries it) and poll ``GET /v1/auth/confirm/{confirm_id}`` every ``interval``
    seconds for at most ``expires_in``. ``channel="dev"``, ``status="confirmed"``: done
    already (``confirmed_until``), no code, no mail."""

    confirm_id: _ConfirmId
    channel: Literal["email", "dev"]
    status: Literal["pending", "confirmed"]
    #: Shown on the asking screen only; never in the mail, never in a poll's answer.
    user_code: _UserCode | None = Field(default=None, repr=False)
    #: The gate's time of the request; the mail says the same, so a person can match them.
    asked_at: datetime
    expires_in: int = Field(ge=0)
    interval: int = Field(ge=1)
    confirmed_until: datetime | None = None

    @model_validator(mode="after")
    def _consistent(self) -> ConfirmStart:
        pending = self.status == "pending"
        if (
            pending != (self.user_code is not None)
            or pending == (self.confirmed_until is not None)
            or (self.channel == "dev") == pending
        ):
            raise ValueError(
                "a pending confirmation (email) carries a user_code; a confirmed one (dev "
                "only) carries confirmed_until"
            )
        return self


class ConfirmStatus(_M):
    """``GET /v1/auth/confirm/{confirm_id}``. ``status`` is this confirmation's:
    ``pending``, ``confirmed``, ``expired`` (not confirmed in time) or ``burned`` (its link
    took three wrong codes). ``confirmed_until`` is the asking session's: set while that
    session may make changes (by this confirmation, another one, or a recent sign-in), None
    otherwise. A client resumes as soon as ``confirmed_until`` is set."""

    confirm_id: _ConfirmId
    status: Literal["pending", "confirmed", "expired", "burned"]
    confirmed_until: datetime | None = None


# --------------------------------------------------------------------------- passkeys
#
# A passkey (a WebAuthn credential) confirms a person's sign-in in one step; it never signs
# anyone in (decisions I24). The console runs the ceremony in the browser and relays its
# JSON; the gate verifies it. The browser's JSON travels as an opaque object, bounded in
# size: typing its fields here would refuse what a new browser adds, and the gate's WebAuthn
# library is its parser.

#: ``recent`` (decisions I24): a sign-in or a completed confirmation at most this many
#: seconds old. Adding a passkey needs it, not just the hour: a passkey can confirm every
#: change after it, so a session confirmed 50 minutes ago (perhaps stolen since) must
#: confirm again, now, to add one.
RECENT_CONFIRMATION_SECONDS = 300
#: The live passkeys a party may hold at once (the 11th is ``409 too_many_passkeys``).
MAX_PASSKEYS = 10
#: A passkey's label: the API key label rule, 1 to 64 printable ASCII characters.
PASSKEY_LABEL_PATTERN = API_KEY_LABEL_PATTERN
#: ``pky_`` + 32 random hex: the API's handle of a passkey, never derived from the
#: authenticator's credential id (which the API never shows).
PASSKEY_ID_PATTERN = r"^pky_[0-9a-f]{32}$"
#: ``wac_`` + 32 random hex: one WebAuthn ceremony (its challenge, bound to the session that
#: asked and to its purpose). Opaque; not a secret.
CEREMONY_ID_PATTERN = r"^wac_[0-9a-f]{32}$"
#: The browser's WebAuthn JSON (``PublicKeyCredential.toJSON()``) and the options sent for
#: it: at most this many bytes as compact ASCII JSON. A registration with a 1023-byte
#: credential id and an RSA key is under 4 KiB; ten excluded credentials of that size are
#: under 15 KiB.
WEBAUTHN_JSON_MAX = 32 * 1024
_PASSKEY_ID = re.compile(PASSKEY_ID_PATTERN, re.ASCII)
_CEREMONY_ID = re.compile(CEREMONY_ID_PATTERN, re.ASCII)
_PasskeyId = Annotated[str, StringConstraints(max_length=36, pattern=PASSKEY_ID_PATTERN)]
_CeremonyId = Annotated[str, StringConstraints(max_length=36, pattern=CEREMONY_ID_PATTERN)]
_PasskeyLabel = _KeyLabel
#: An RP ID is a DNS name: at most 253 characters.
_RpId = Annotated[str, StringConstraints(min_length=1, max_length=253)]

#: How an authenticator reaches the browser (WebAuthn ``AuthenticatorTransport``), as the
#: gate keeps it: ``cable`` is stored as ``hybrid``, unknown values are dropped.
PasskeyTransport = Literal["usb", "nfc", "ble", "smart-card", "hybrid", "internal"]
#: Who revoked a passkey: the person (``revoked``), the gate on a use count that went
#: backwards (``counter``: a copy of the authenticator answered), or an operator.
PasskeyRevokedBy = Literal["revoked", "counter", "operator"]


def is_passkey_id(text: str) -> bool:
    """``text`` is a passkey id exactly as the gate issues it (``pky_`` + 32 hex)."""
    return _PASSKEY_ID.fullmatch(text) is not None


def is_ceremony_id(text: str) -> bool:
    """``text`` is a ceremony id exactly as the gate issues it (``wac_`` + 32 hex)."""
    return _CEREMONY_ID.fullmatch(text) is not None


def _webauthn_json(value: Any) -> Any:
    """Refuse a WebAuthn JSON object larger than :data:`WEBAUTHN_JSON_MAX` before pydantic
    walks it (its members are then checked to be JSON). Anything but an object is left to
    pydantic's own single error."""
    if isinstance(value, dict):
        try:
            size = len(json.dumps(value, separators=(",", ":"), allow_nan=False))
        except (TypeError, ValueError, RecursionError):
            raise ValueError("not a JSON object") from None
        if size > WEBAUTHN_JSON_MAX:
            raise ValueError(f"at most {WEBAUTHN_JSON_MAX} bytes of JSON")
    return value


#: A WebAuthn JSON object: ``RegistrationResponseJSON`` / ``AuthenticationResponseJSON`` from
#: the browser, ``PublicKeyCredential{Creation,Request}OptionsJSON`` from the gate.
#: (The member ceiling comes first: after the validator it would not reach the schema.)
_WebAuthnJson = Annotated[
    dict[str, JsonValue], Field(max_length=_MEMBERS_MAX), BeforeValidator(_webauthn_json)
]


class PasskeyCeremony(_M):
    """``POST /v1/passkeys/options`` and ``POST /v1/auth/confirm/passkey/options`` (201): one
    WebAuthn ceremony of the session that asked. ``options`` is
    ``PublicKeyCredentialCreationOptionsJSON`` (adding a passkey) or
    ``PublicKeyCredentialRequestOptionsJSON`` (confirming), for
    ``PublicKeyCredential.parse{Creation,Request}OptionsFromJSON``; send the browser's answer
    back with ``ceremony_id``, once, within ``expires_in`` seconds. ``rp_id``: a page whose
    host is neither it nor under it cannot use these options."""

    ceremony_id: _CeremonyId
    rp_id: _RpId
    options: _WebAuthnJson
    expires_in: int = Field(ge=1)


class PasskeyAdd(_M):
    """``POST /v1/passkeys``: the browser's answer to ``passkey_options``
    (``RegistrationResponseJSON``), and a name for the passkey."""

    ceremony_id: _CeremonyId
    label: _PasskeyLabel
    credential: _WebAuthnJson = Field(repr=False)


class PasskeyConfirm(_M):
    """``POST /v1/auth/confirm/passkey``: the browser's answer to ``confirm_passkey_options``
    (``AuthenticationResponseJSON``)."""

    ceremony_id: _CeremonyId
    credential: _WebAuthnJson = Field(repr=False)


class PasskeyView(_M):
    """One passkey of ``GET /v1/passkeys``; never its key or its credential id."""

    id: _PasskeyId
    label: _PasskeyLabel
    created_at: datetime
    #: The first 12 hex of the sign-in that added it (as the sign-ins list shows it); None
    #: once that sign-in's record is gone.
    added_by: Annotated[str, StringConstraints(max_length=12, pattern=r"^[0-9a-f]{12}$")] | None = (
        None
    )
    last_used_at: datetime | None = None
    #: The authenticator's backup state when last seen: a password manager or a cloud
    #: keychain holds a copy (synced), else it lives on this device only.
    backed_up: bool
    transports: list[PasskeyTransport] = Field(max_length=6)
    revoked_at: datetime | None = None
    revoked_by: PasskeyRevokedBy | None = None

    @model_validator(mode="after")
    def _consistent(self) -> PasskeyView:
        if (self.revoked_at is None) != (self.revoked_by is None):
            raise ValueError(
                "a revoked passkey carries revoked_at and revoked_by, a live one neither"
            )
        return self


class PasskeyList(_M):
    """``GET /v1/passkeys``: the party's live passkeys newest first, then its latest revoked
    ones. ``rp_id``: where they work (the console's host); None: this gate takes no passkeys
    (the list still shows what it holds)."""

    rp_id: _RpId | None
    passkeys: list[PasskeyView] = Field(max_length=_PAGE_MAX)


class PasskeyConfirmed(_M):
    """``POST /v1/auth/confirm/passkey`` (200): this session is confirmed until
    ``confirmed_until`` (now plus the gate's ``KONTRA_CONFIRM_TTL``), by ``passkey_id``."""

    confirmed_until: datetime
    passkey_id: _PasskeyId


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
    party, under a label that says which machine or fleet holds it. A change: it needs a
    person's session confirmed within the hour (decisions I23), because a recipient can read
    every delivery made after it is added.

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


# --------------------------------------------------------------------------- devices & job keys
#
# Share to my account (decisions I25). A *device* is a key that reads the jobs the party
# hires: a browser profile's, or a ``kontra login`` machine's ``~/.kontra/key``. Devices are a
# set of their own, never the recipients: recipients include fleet keys mounted on every
# seller worker, are public, and receive seller input. Each job's key is copied to each live
# device as a *wrap* (age ciphertext to that device alone), which the gate stores and cannot
# read. Every model here is new: no existing answer gained a field, so 0.2.0-0.2.3 keep
# parsing every answer of a gate that serves these routes.
#
# A device's life: added by a recent sign-in, removed by a person (``revoked``: never again),
# or -- a *browser* only -- **retired** by the gate: one nobody has used for
# :data:`DEVICE_IDLE_SECONDS` (a private window, a profile deleted), or the browser used least
# recently when a new device needs room past :data:`DEVICES_MAX`. A retired device is listed
# nowhere and gets nothing, but keeps its copies and comes back when its browser adds it again
# (the same key: no mail, nothing to confirm beyond the add's own ``recent``).

#: The live devices one party may hold at once. A new one past it takes the place of the
#: browser used least recently (retired); with no browser to retire, ``409 too_many_devices``.
DEVICES_MAX = 32
#: A browser device nobody used for this long is retired: no browser profile that is still
#: around goes a month without a catch-up (``GET /v1/me/devices?for=<it>`` marks it used), and
#: a sign-in that idle has expired anyway. A ``kontra login`` machine is never retired.
DEVICE_IDLE_SECONDS = 30 * 24 * 3600
#: New devices one party may add in an hour, removed and retired ones included (``429
#: rate_limited``): each mails the party's address. A device coming back is not counted.
DEVICE_ADDS_PER_HOUR = 48
#: The largest wrap, in bytes of age ciphertext (one is about 340).
WRAP_MAX = 1024
#: The largest challenge, in bytes of age ciphertext (one is 243).
CHALLENGE_MAX = 512
#: The jobs one ``POST /v1/me/job-keys`` carries at most.
SHARE_ITEMS_MAX = 100
#: A device's label: the API key label rule, 1 to 64 printable ASCII characters. Shown, never
#: parsed: nothing decides access by it.
DEVICE_LABEL_PATTERN = API_KEY_LABEL_PATTERN
#: A browser profile, or a ``kontra login`` machine.
DeviceKind = Literal["browser", "cli"]
#: A device's label as an API key's session reads it: the names of a person's browsers and
#: machines are for the person's sign-ins, not for a program.
HIDDEN_DEVICE_LABEL = "(hidden from API keys)"
_B64_STD = r"^[A-Za-z0-9+/]*={0,2}$"
_Wrap = Annotated[str, StringConstraints(max_length=4 * -(-WRAP_MAX // 3), pattern=_B64_STD)]
_Challenge = Annotated[
    str, StringConstraints(max_length=4 * -(-CHALLENGE_MAX // 3), pattern=_B64_STD)
]
#: base64url (unpadded) of an HMAC-SHA256: what a job's challenge holds.
_Proof = Annotated[str, StringConstraints(max_length=43, pattern=r"^[A-Za-z0-9_-]{43}$")]
_SessionPrefix = Annotated[str, StringConstraints(max_length=12, pattern=r"^[0-9a-f]{12}$")]


class AddDevice(_M):
    """``POST /v1/me/devices``: one more device that reads the jobs this party hires, under
    a label that says which browser or machine holds it. Needs a confirmation of the last
    five minutes (``recent``): a device also gets the keys of past jobs (decisions I25).
    Adding a live device again answers the same view; a removed one is never added again."""

    label: _KeyLabel
    public_key: _Name
    kind: DeviceKind


class DeviceView(_M):
    """One of the party's live devices (``GET /v1/me/devices``)."""

    public_key: _Name
    label: _KeyLabel
    kind: DeviceKind
    added_at: datetime
    #: The first 12 hex of the sign-in that added it (as the sign-ins list shows it); None
    #: once that sign-in's record is gone, and to an API key's session (as the label, which
    #: is then :data:`HIDDEN_DEVICE_LABEL`: a program needs the keys, not the names).
    added_by: _SessionPrefix | None = None
    #: The last read for it (``?for=<it>`` on ``GET /v1/me/devices``, ``/v1/jobs/{id}/key``
    #: or ``/v1/me/job-keys``), to the minute; None to an API key's session.
    last_used_at: datetime | None = None


class DeviceList(_M):
    """``GET /v1/me/devices``: the party's live devices, oldest first."""

    devices: list[DeviceView] = Field(max_length=_PAGE_MAX)


class KeyWrap(_M):
    """A job key's copy for one device: age ciphertext to ``recipient`` alone (base64)."""

    recipient: _Name
    wrap: _Wrap
    wrapped_at: datetime


class JobKey(_M):
    """``GET /v1/jobs/{id}/key`` (the buyer's): the job's exact ``buyer_key`` (the one its
    hold token seals), its wraps to the party's live devices (or to the ``for`` device
    only), and a challenge -- this session's proof, encrypted to ``buyer_key`` -- for
    sharing the key (``POST /v1/me/job-keys``). A job whose ``buyer_key`` is or was one of the
    party's recipients (every job from before per-job keys) is never shared: no wraps."""

    job_id: _Name
    buyer_key: _Name
    wraps: list[KeyWrap] = Field(max_length=DEVICES_MAX)
    challenge: _Challenge


class JobKeyList(_M):
    """``GET /v1/me/job-keys?lacking=<device>``: the caller's shareable buyer jobs with no
    wrap to that device, by job id; pass ``next_cursor`` back for more."""

    items: list[JobKey] = Field(max_length=_PAGE_MAX)
    next_cursor: _Long | None = None


class WrapIn(_M):
    """One wrap to store: the job key, encrypted to the device ``recipient`` alone."""

    recipient: _Name
    wrap: _Wrap


class JobKeyShare(_M):
    """The wraps of one job, with the proof that the sender holds its key (the job's
    challenge, opened)."""

    job_id: _Name
    proof: _Proof = Field(repr=False)
    wraps: list[WrapIn] = Field(max_length=DEVICES_MAX)


class ShareJobKeys(_M):
    """``POST /v1/me/job-keys``."""

    items: list[JobKeyShare] = Field(max_length=SHARE_ITEMS_MAX)


#: Why one job of a share was refused: not the caller's as buyer (or no such job), never
#: shared (its key is or was a recipient's), a proof that is not this session's for it now,
#: a wrap that is not one X25519 age file.
ShareRefusalCode = Literal["job_not_found", "not_shareable", "bad_proof", "invalid_wrap"]


class ShareRefusal(_M):
    job_id: _Name
    code: _Name


class SharedJobKeys(_M):
    """``POST /v1/me/job-keys`` (200): wraps ``stored``; wraps ``skipped`` (one for that job
    and device exists already -- the first write wins -- or the recipient is not a live
    device); the jobs ``refused`` whole, each with its code (:data:`ShareRefusalCode`)."""

    stored: int = Field(ge=0)
    skipped: int = Field(ge=0)
    refused: list[ShareRefusal] = Field(default_factory=list, max_length=SHARE_ITEMS_MAX)


# --------------------------------------------------------------------------- catalog


def _streams(value: list[str]) -> list[str]:
    try:
        return list(check_streams(value))
    except ValueError as e:
        raise ValueError(str(e)) from None


_Streams = Annotated[list[_Topic], Field(max_length=STREAMS_MAX), AfterValidator(_streams)]

#: The engines an offering version can run on (seam ⑥, ``kontra.engine``, decisions I32).
#: Additive: a gate learns a new name before any client sends it, and no model a gate answers
#: with carries it, so adding a value never breaks an old client.
EngineName = Literal["temporal"]
ENGINE_NAMES: Final[tuple[EngineName, ...]] = ("temporal",)
#: What a version runs on when its publish names no engine (every client before #55, and
#: every client since that publishes for this one: it omits the default).
DEFAULT_ENGINE: Final[EngineName] = "temporal"


def _is_default_engine(value: object) -> bool:
    return value == DEFAULT_ENGINE


class PublishRequest(_M):
    """``POST /v1/catalog``: the full contract document (with ``x-kontra``), as JSON. A
    change: it needs a person's session confirmed within the gate's ``KONTRA_CONFIRM_TTL``
    (a sign-in counts; §10.4, decisions I23); the gate then creates the Nexus endpoint
    itself, targeting the caller's namespace and the offering's task queue, under a fresh
    random name (decisions I1).

    ``streams``: ``offering.yaml``'s ``streams:``, the topics of a job's workflow stream a
    buyer may read (decisions I31) -- beside the document, never in it: catalog data, not in
    the contract hash. Canonical (sorted, distinct; ``kontra.contract.offering.check_streams``)
    and fixed per version: a republish of the same version with other streams is 409.

    ``engine``: the engine the version runs on (seam ⑥, decisions I32), beside the document
    like ``streams``: never hashed, fixed at the version's first publish (a republish with
    another is 409 ``engine_fixed``), and never in anything a buyer reads. Absent means
    :data:`DEFAULT_ENGINE`, and a client sends it only when it is not the default, so a
    client of this version publishes to a gate that predates the field (owner decision
    OD11 of #55). A name that is not an engine (:data:`ENGINE_NAMES`) and that the gate runs
    no adapter for is 422 ``unsupported_engine``; a known engine a gate has no adapter for (a
    fake gate with no Temporal) is recorded, its sellers offline."""

    document: dict[str, Any] = Field(max_length=_MEMBERS_MAX)
    streams: _Streams = Field(default_factory=list[str])
    engine: EngineName = Field(default=DEFAULT_ENGINE, exclude_if=_is_default_engine)


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
    #: The topics of a job's stream a buyer may read (``offering.yaml`` ``streams:``),
    #: fixed when the version was published: catalog data, as ``recipients`` is (I31).
    streams: _Streams = Field(default_factory=list[str])


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


# --------------------------------------------------------------------------- workers
#
# Whether a seller's workers keep up with one of its offering versions: what Temporal's
# ``DescribeTaskQueue`` (``report_stats``) says of the version's task queue in the seller's
# namespace, per task-queue type, with Temporal's own names (kontra-cloud#6). The verdict
# and the ``stale`` flag are :mod:`kontra.workers`'s, read from these numbers alone.


class TaskQueueType(StrEnum):
    """Temporal's ``TaskQueueType``: one task queue name is a queue per kind of task."""

    WORKFLOW = "workflow"
    ACTIVITY = "activity"
    NEXUS = "nexus"


class TaskQueueStats(_M):
    """Temporal's ``TaskQueueStats`` for one task-queue type. Rates are tasks per second
    averaged over the last 30 seconds; the backlog is approximate (Temporal's word) and
    leaves out sticky queues. A number Temporal does not report is zero."""

    approximate_backlog_count: int = Field(default=0, ge=0)
    #: The age of the oldest task in the backlog, in seconds (a ``Duration`` in Temporal).
    approximate_backlog_age: float = Field(default=0.0, ge=0, allow_inf_nan=False)
    tasks_add_rate: float = Field(default=0.0, ge=0, allow_inf_nan=False)
    tasks_dispatch_rate: float = Field(default=0.0, ge=0, allow_inf_nan=False)
    #: ``tasks_add_rate - tasks_dispatch_rate``, as Temporal's SDKs report it
    #: (``BacklogIncreaseRate``): positive while the backlog grows, negative while it drains.
    backlog_increase_rate: float = Field(default=0.0, allow_inf_nan=False)


class PollerInfo(_M):
    """A worker polling the task queue, as Temporal's ``PollerInfo`` reports it. Temporal
    keeps a poller listed for about five minutes after its last poll, and a worker whose
    slots are all busy does not poll, so ``last_access_time`` says how recent a listing is.

    ``build_id`` and ``deployment_name`` are what the worker said of its version when it
    polled (``WorkerDeploymentOptions``, else ``WorkerVersionCapabilities``); the Python SDK
    sends a build id even for an unversioned worker, a hash of its loaded code.

    ``sdk_name`` and ``sdk_version`` (``temporal-python``, ``1.34.0``) are not in
    ``DescribeTaskQueue``'s answer: they are what the worker of this identity said of itself
    in its last heartbeat on this task queue, as Temporal's ``ListWorkers`` reports it
    (``WorkerHeartbeat``). A worker sends one every 60 seconds by default, so both are None
    for a worker that has sent none yet, one that sends none, or a server that keeps none
    (``frontend.WorkerHeartbeatsEnabled``)."""

    identity: _TemporalId
    last_access_time: datetime
    rate_per_second: float = Field(allow_inf_nan=False)
    build_id: _Name | None = None
    deployment_name: _TemporalId | None = None
    sdk_name: _Name | None = None
    sdk_version: _Name | None = None


class TaskQueueTypeInfo(_M):
    """Temporal's ``TaskQueueTypeInfo``: one task-queue type's pollers, most recently seen
    first (at most :data:`POLLERS_MAX`), and its stats."""

    pollers: list[PollerInfo] = Field(default_factory=list, max_length=POLLERS_MAX)
    stats: TaskQueueStats = Field(default_factory=TaskQueueStats)


class WorkersView(_M):
    """``GET /v1/offerings/{party}/{name}/versions/{version}/workers``: the Workers view of
    one of the caller's own offering versions (another party's is 404, like a missing one).

    ``types_info`` holds every task-queue type the gate described (all three). ``verdict``
    is one line for people (:func:`kontra.workers.verdict`). ``stale``: Temporal reported
    nothing -- every number zero and no poller -- which is also what it reports for a task
    queue it unloaded after about five idle minutes, so the zeros may not be a measurement
    (:func:`kontra.workers.stale`)."""

    offering: _Name  # acme/enrich@1.2.0, exact
    task_queue: _Name
    types_info: dict[TaskQueueType, TaskQueueTypeInfo] = Field(max_length=len(TaskQueueType))
    verdict: _Name
    stale: bool


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


#: ``GET /v1/jobs/{id}?include=buyer_key`` (and ``GET /v1/jobs``): what a client asks to be
#: added to :class:`JobView`.
JobInclude = Literal["buyer_key"]


def _absent(value: object) -> bool:
    return value is None


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
    #: The job's own key, the one its hold token seals (decisions I7, I25): sent only when
    #: asked (``?include=buyer_key``) and absent from the JSON otherwise -- never ``null`` --
    #: since 0.2.0-0.2.3 parse every answer strictly and would refuse the field. With it a
    #: client looks the job's key up by name instead of trying each one.
    buyer_key: _Name | None = Field(default=None, exclude_if=_absent)


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


# --------------------------------------------------------------------------- streams & progress


class ProgressSource(StrEnum):
    """Where a buyer's ``Progress`` came from (``kontra.hire.Progress``): the seller's
    heartbeat (the job stream's ``kontra.progress`` topic), or history liveness."""

    HEARTBEAT = "heartbeat"
    HISTORY = "history"


class StreamEventKind(StrEnum):
    """What one event of ``GET /v1/jobs/{id}/stream`` is: a relayed ``item`` of the job's
    stream, a ``gap`` (an item not relayed, or the relay stopped), or platform liveness
    read from history (``history``)."""

    ITEM = "item"
    GAP = "gap"
    HISTORY = "history"


class StreamGapReason(StrEnum):
    """Why a gap: an item above :data:`STREAM_ITEM_MAX` (``too_large``), past the topic's or
    the job's stored budget (``topic_budget``, ``job_budget``), an item the seller's worker
    did not store in time or whose blob is missing or not what its reference names
    (``unavailable``, decisions I39) -- these carry the item's topic and offset --; or the
    relay stopped: the job's poll budget (``history_budget``), an answer that was not a
    stream envelope (``relay_failed``), an item the gate refused (``invalid``)."""

    TOO_LARGE = "too_large"
    TOPIC_BUDGET = "topic_budget"
    JOB_BUDGET = "job_budget"
    UNAVAILABLE = "unavailable"
    HISTORY_BUDGET = "history_budget"
    RELAY_FAILED = "relay_failed"
    INVALID = "invalid"


_HISTORY_FIELDS = (
    "last_event",
    "activities_completed",
    "activities_running",
    "continue_as_new_count",
)


class StreamEvent(_M):
    """One event of ``GET /v1/jobs/{id}/stream`` (SSE: ``id:`` is ``cursor``, ``event:`` is
    ``kind``, ``data:`` is this JSON), decisions I31.

    ``cursor`` is the gate's per-job, monotonic position: what ``after`` / ``Last-Event-ID``
    resume from. An ``item`` carries the topic and offset in the clear and the item's age
    ciphertext (the item's own ``Payload``, with its topic and offset sealed inside; only
    the job's parties read it). A ``gap`` carries its ``reason``, and the item's ``topic``,
    ``offset`` and ``size`` when it stands for one. A ``history`` event is the platform's
    liveness summary of the job's chain."""

    cursor: int = Field(ge=1)
    kind: StreamEventKind
    at: datetime
    topic: _Topic | None = None
    offset: int | None = Field(default=None, ge=0)
    ciphertext_b64: Annotated[str, StringConstraints(max_length=_STREAM_B64_MAX)] | None = None
    size: int | None = Field(default=None, ge=0)
    reason: StreamGapReason | None = None
    state: JobState | None = None
    last_event: _Name | None = None
    activities_completed: int | None = None
    activities_running: list[_Activity] | None = Field(
        default=None, max_length=_PENDING_ACTIVITIES_MAX
    )
    continue_as_new_count: int | None = None

    @model_validator(mode="after")
    def _per_kind(self) -> StreamEvent:
        history = [f for f in _HISTORY_FIELDS if getattr(self, f) is not None]
        if self.kind is StreamEventKind.ITEM:
            if self.topic is None or self.offset is None or self.ciphertext_b64 is None:
                raise ValueError("an item carries topic, offset and ciphertext_b64")
            if self.reason is not None or self.size is not None or history:
                raise ValueError("an item carries no reason, size or history fields")
        elif self.kind is StreamEventKind.GAP:
            if self.reason is None:
                raise ValueError("a gap carries its reason")
            if self.ciphertext_b64 is not None or history:
                raise ValueError("a gap carries no ciphertext or history fields")
            if (self.topic is None) != (self.offset is None):
                raise ValueError("a gap names both its item's topic and offset, or neither")
            if self.offset is None and self.size is not None:
                raise ValueError("a gap's size is its item's")
        else:
            if self.state is None:
                raise ValueError("a history event carries the job's state")
            if any(
                v is not None
                for v in (self.topic, self.offset, self.ciphertext_b64, self.size, self.reason)
            ):
                raise ValueError("a history event carries no topic, offset, ciphertext or gap")
        return self


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


class StreamBlobsCreate(_M):
    """``POST /v1/jobs/{job_id}/stream/blobs``, by the job's seller: the claim-check blobs
    of one poll's answer (decisions I39), content-addressed as :class:`BlobCreate` and scoped
    to the job -- readable by its seller (``GET /v1/blobs/{sha256}``) and the gate's own
    relay, nobody else."""

    blobs: list[BlobCreate] = Field(min_length=1, max_length=STREAM_BLOBS_MAX)


class StreamBlobUrls(_M):
    """One :class:`BlobUrl` per blob asked for, in no promised order: a PUT URL (signed for
    its length and its ``x-amz-checksum-sha256``) or, when stored already, ``exists``."""

    blobs: list[BlobUrl] = Field(max_length=STREAM_BLOBS_MAX)


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
