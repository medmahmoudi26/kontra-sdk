"""The gate's route table (seam ②), as data.

The hand-written client (:mod:`kontra.gate_client`), the fake gate
(:mod:`kontra.testing.fake_gate`) and the platform gate all implement exactly this table,
and ``tests/test_seam_gate_api.py`` checks each of them against it. Adding a route means
adding a row here, in the same PR.

``auth``:
* ``none``      -- public.
* ``party``     -- ``Authorization: Bearer <gate access token>`` (aud ``kontra:api``), from a
                   human login or from an API key.
* ``human``     -- a party token whose session is a person's: an API key's session gets 403
                   ``fresh_login_required`` (decisions I19), a worker token 401. No age check:
                   the reads of keys and sign-ins (decisions I23).
* ``fresh``     -- ``human``, and the session is confirmed: a sign-in or a completed
                   confirmation (``POST /v1/auth/confirm``) within the gate's
                   ``KONTRA_CONFIRM_TTL`` (an hour), else 403 ``confirmation_required`` with
                   :class:`~kontra.gate_client.models.ConfirmationRequired` as its detail. The
                   API-key test comes first, so a key always gets ``fresh_login_required``.
                   The changes: making or revoking an API key or a recipient, ending a
                   sign-in, publishing. The 403 is decided before the route does anything, so a
                   client confirms in place and sends the same call once more (decisions I23).
* ``worker``    -- the party's Temporal *worker* JWT, also accepted on seller routes so a
                   worker needs one credential (``kontra serve``).
* ``resolver``  -- a party listed in the gate's ``KONTRA_RESOLVERS`` (anyone else: 403
                   ``not_resolver``, checked first), then ``fresh``: a resolver splits escrow,
                   so an API key gets 403 ``fresh_login_required`` and an unconfirmed login 403
                   ``confirmation_required``.
* ``resolver_human`` -- the resolver list, then ``human``: reading the queue needs a
                   resolver's own login, never a confirmation.
* ``dev``       -- only when the gate runs with ``KONTRA_DEV=1``; 404 otherwise.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel

from kontra.gate_client import models as m

Auth = Literal[
    "none", "party", "human", "fresh", "worker", "party|worker", "resolver", "resolver_human", "dev"
]


@dataclass(frozen=True, slots=True)
class Route:
    name: str
    method: Literal["GET", "POST", "PUT", "DELETE"]
    path: str
    auth: Auth
    request: type[BaseModel] | None
    response: type[BaseModel] | None  # None: no body (204), or a stream (see ``stream``)
    stream: bool = False
    #: The success status. A client accepts any 2xx; the gates answer exactly this.
    status: int = 200


P = "/v1"

ROUTES: tuple[Route, ...] = (
    # auth & identity
    Route("jwks", "GET", "/.well-known/jwks.json", "none", None, None),
    # What this gate offers before sign-in (dev login, email, GitHub), for clients to show.
    Route("info", "GET", f"{P}/info", "none", None, m.GateInfo),
    # Recently revoked sessions, for Kontra's Temporal server to refuse their worker tokens.
    Route("revocations", "GET", f"{P}/auth/revocations", "none", None, m.Revocations),
    Route("device_start", "POST", f"{P}/auth/device", "none", None, m.DeviceStart),
    Route("token", "POST", f"{P}/auth/token", "none", m.TokenRequest, m.TokenResponse),
    Route(
        "magic_link",
        "POST",
        f"{P}/auth/magic",
        "none",
        m.MagicLinkRequest,
        m.MagicLinkSent,
        status=202,
    ),
    Route("dev_login", "POST", f"{P}/auth/dev-login", "dev", m.DevLogin, m.TokenResponse),
    Route("revoke", "POST", f"{P}/auth/revoke", "party", None, None, status=204),
    Route(
        "temporal_token",
        "POST",
        f"{P}/auth/temporal-token",
        "party",
        m.TemporalTokenRequest,
        m.TemporalToken,
    ),
    Route("me", "GET", f"{P}/me", "party|worker", None, m.Me),
    # Encryption recipients, NOT /v1/keys (API keys, below): an API key authenticates, a
    # recipient receives ciphertext. Adding never replaces, so a second machine is one
    # more recipient; both are changes a person confirms, because a recipient can read every
    # delivery made after it is added (decisions I7, I23).
    Route("add_recipient", "POST", f"{P}/me/recipients", "fresh", m.AddKey, m.Me, status=201),
    Route(
        "revoke_recipient",
        "DELETE",
        f"{P}/me/recipients/{{public_key}}",
        "fresh",
        None,
        None,
        status=204,
    ),
    Route("party", "GET", f"{P}/parties/{{party}}", "none", None, m.PartyView),
    # API keys for programs: listed by a person's session, minted and revoked by a confirmed
    # one; never by a key (decisions I19, I23)
    Route(
        "create_api_key", "POST", f"{P}/keys", "fresh", m.ApiKeyCreate, m.ApiKeyCreated, status=201
    ),
    Route("api_keys", "GET", f"{P}/keys", "human", None, m.ApiKeyList),
    Route("revoke_api_key", "DELETE", f"{P}/keys/{{key_id}}", "fresh", None, None, status=204),
    # The party's sign-ins (people's logins), to find and end one: a person's session reads
    # them, a confirmed one ends one
    Route("sessions", "GET", f"{P}/sessions", "human", None, m.SessionList),
    Route("end_session", "DELETE", f"{P}/sessions/{{session_id}}", "fresh", None, None, status=204),
    # Step-up for changes (decisions I23): a person's session asks; the gate mails the
    # account's sign-in address a link whose page asks for the code this session shows.
    Route("confirm_start", "POST", f"{P}/auth/confirm", "human", None, m.ConfirmStart, status=201),
    Route(
        "confirm_status", "GET", f"{P}/auth/confirm/{{confirm_id}}", "human", None, m.ConfirmStatus
    ),
    # catalog
    Route("publish", "POST", f"{P}/catalog", "fresh", m.PublishRequest, m.CatalogEntry),
    Route("search", "GET", f"{P}/catalog", "none", None, m.CatalogList),
    Route(
        "get_offering",
        "GET",
        f"{P}/catalog/{{party}}/{{name}}/{{version}}",
        "none",
        None,
        m.CatalogEntry,
    ),
    Route("card", "GET", f"{P}/catalog/{{party}}/{{name}}/{{version}}/card", "none", None, None),
    Route("by_endpoint", "GET", f"{P}/endpoints/{{endpoint}}", "none", None, m.CatalogEntry),
    # The seller's own versions only (another party's is 404): Temporal's DescribeTaskQueue
    # on the version's task queue, and what it means (kontra.workers).
    Route(
        "workers",
        "GET",
        f"{P}/offerings/{{party}}/{{name}}/versions/{{version}}/workers",
        "party|worker",
        None,
        m.WorkersView,
    ),
    # quotes & jobs
    Route("quote", "POST", f"{P}/quotes", "party", m.QuoteRequest, m.QuoteView),
    Route("hire", "POST", f"{P}/jobs", "party", m.HireRequest, m.HireView),
    Route("jobs", "GET", f"{P}/jobs", "party|worker", None, m.JobList),
    Route("job", "GET", f"{P}/jobs/{{job_id}}", "party|worker", None, m.JobView),
    Route(
        "seller_job", "GET", f"{P}/jobs/{{job_id}}/seller", "party|worker", None, m.SellerJobView
    ),
    Route("accept", "POST", f"{P}/jobs/{{job_id}}/accept", "party", m.AcceptRequest, m.JobView),
    Route("reject", "POST", f"{P}/jobs/{{job_id}}/reject", "party", m.RejectRequest, m.JobView),
    Route("cancel", "POST", f"{P}/jobs/{{job_id}}/cancel", "party", None, m.JobView),
    Route("start", "POST", f"{P}/jobs/{{job_id}}/start", "party", m.StartRequest, m.JobView),
    Route("result", "GET", f"{P}/jobs/{{job_id}}/result", "party", None, m.ResultView),
    Route(
        "progress",
        "GET",
        f"{P}/jobs/{{job_id}}/progress",
        "party",
        None,
        m.ProgressEvent,
        stream=True,
    ),
    Route(
        "post_progress",
        "POST",
        f"{P}/jobs/{{job_id}}/progress",
        "party|worker",
        m.ProgressPost,
        None,
        status=204,
    ),
    # artifacts & claim-check blobs
    Route(
        "create_artifact",
        "POST",
        f"{P}/artifacts",
        "party|worker",
        m.ArtifactCreate,
        m.ArtifactUpload,
    ),
    Route(
        "complete_artifact",
        "POST",
        f"{P}/artifacts/{{key}}/complete",
        "party|worker",
        m.ArtifactComplete,
        None,
        status=204,
    ),
    Route(
        "download_artifact", "GET", f"{P}/artifacts/{{key}}", "party|worker", None, m.DownloadUrl
    ),
    Route("create_blob", "POST", f"{P}/blobs", "party|worker", m.BlobCreate, m.BlobUrl),
    Route("get_blob", "GET", f"{P}/blobs/{{sha256}}", "party|worker", None, m.BlobUrl),
    # money
    Route("balance", "GET", f"{P}/balance", "party", None, m.Balance),
    Route("topup", "POST", f"{P}/topup", "party", m.Topup, m.TopupResult),
    # disputes (resolver of last resort)
    Route("disputes", "GET", f"{P}/admin/disputes", "resolver_human", None, m.JobList),
    Route(
        "resolve",
        "POST",
        f"{P}/admin/jobs/{{job_id}}/resolve",
        "resolver",
        m.ResolveRequest,
        m.JobView,
    ),
    # health
    Route("health", "GET", "/healthz", "none", None, None),
)

BY_NAME = {r.name: r for r in ROUTES}
