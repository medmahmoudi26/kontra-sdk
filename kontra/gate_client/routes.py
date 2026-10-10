"""The gate's route table (seam ②), as data.

The hand-written client (:mod:`kontra.gate_client`), the fake gate
(:mod:`kontra.testing.fake_gate`) and the platform gate all implement exactly this table,
and ``tests/test_seam_gate_api.py`` checks each of them against it. Adding a route means
adding a row here, in the same PR.

``auth``:
* ``none``      -- public.
* ``party``     -- ``Authorization: Bearer <gate access token>`` (aud ``kontra:api``).
* ``fresh``     -- a party token whose human session (``auth_time``) is younger than the
                   gate's freshness window; worker tokens never qualify (§10.4).
* ``worker``    -- the party's Temporal *worker* JWT, also accepted on seller routes so a
                   worker needs one credential (``kontra serve``).
* ``resolver``  -- a party listed in the gate's ``KONTRA_RESOLVERS``.
* ``dev``       -- only when the gate runs with ``KONTRA_DEV=1``; 404 otherwise.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel

from kontra.gate_client import models as m

Auth = Literal["none", "party", "fresh", "worker", "party|worker", "resolver", "dev"]


@dataclass(frozen=True, slots=True)
class Route:
    name: str
    method: Literal["GET", "POST", "PUT", "DELETE"]
    path: str
    auth: Auth
    request: type[BaseModel] | None
    response: type[BaseModel] | None  # None: 204, or a stream (see ``stream``)
    stream: bool = False


P = "/v1"

ROUTES: tuple[Route, ...] = (
    # auth & identity
    Route("jwks", "GET", "/.well-known/jwks.json", "none", None, None),
    Route("device_start", "POST", f"{P}/auth/device", "none", None, m.DeviceStart),
    Route("token", "POST", f"{P}/auth/token", "none", m.TokenRequest, m.TokenResponse),
    Route("dev_login", "POST", f"{P}/auth/dev-login", "dev", m.DevLogin, m.TokenResponse),
    Route("revoke", "POST", f"{P}/auth/revoke", "party", None, None),
    Route(
        "temporal_token",
        "POST",
        f"{P}/auth/temporal-token",
        "party",
        m.TemporalTokenRequest,
        m.TemporalToken,
    ),
    Route("me", "GET", f"{P}/me", "party|worker", None, m.Me),
    Route("set_key", "PUT", f"{P}/me/key", "party", m.SetKey, m.Me),
    Route("party", "GET", f"{P}/parties/{{party}}", "none", None, m.PartyView),
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
    Route("disputes", "GET", f"{P}/admin/disputes", "resolver", None, m.JobList),
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
