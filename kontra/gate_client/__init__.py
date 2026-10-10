"""Hand-written async client for the gate HTTP API (seam ②, PRD §19.2).

One method per row of :data:`kontra.gate_client.routes.ROUTES`, same name. Twelve-ish
endpoints do not justify a generated client; the route table and the gate's OpenAPI
snapshot keep this honest.

Presigned bucket URLs (artifacts, claim-check blobs) are fetched with :attr:`GateClient.http`,
which shares the transport but never sends the gate credential.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import httpx

from kontra.gate_client import models as m

__all__ = ["GateClient", "GateError", "TokenProvider"]

#: Returns a bearer token for the gate (refreshing it if needed), or None for anonymous calls.
TokenProvider = Callable[[], Awaitable[str | None]]


class GateError(Exception):
    """A non-2xx answer from the gate, decoded from :class:`~kontra.gate_client.models.ErrorBody`."""

    def __init__(
        self, status: int, code: str, message: str, detail: dict[str, Any] | None = None
    ) -> None:
        self.status, self.code, self.message, self.detail = status, code, message, detail
        super().__init__(f"{status} {code}: {message}")


class GateClient:
    """``GateClient(base_url, token=...)``. ``transport`` lets tests run the client against
    an in-process gate (``kontra.testing.fake_gate``)."""

    def __init__(
        self,
        base_url: str,
        *,
        token: TokenProvider | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 30.0,
        user_agent: str | None = None,
    ) -> None:
        raise NotImplementedError

    @property
    def base_url(self) -> str:
        raise NotImplementedError

    @property
    def http(self) -> httpx.AsyncClient:
        """Unauthenticated client on the same transport, for presigned URLs."""
        raise NotImplementedError

    async def aclose(self) -> None:
        raise NotImplementedError

    async def __aenter__(self) -> GateClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # auth & identity
    async def jwks(self) -> dict[str, Any]:
        raise NotImplementedError

    async def device_start(self) -> m.DeviceStart:
        raise NotImplementedError

    async def token(self, req: m.TokenRequest) -> m.TokenResponse:
        raise NotImplementedError

    async def dev_login(self, handle: str) -> m.TokenResponse:
        raise NotImplementedError

    async def revoke(self) -> None:
        raise NotImplementedError

    async def temporal_token(self, role: str = "worker") -> m.TemporalToken:
        raise NotImplementedError

    async def me(self) -> m.Me:
        raise NotImplementedError

    async def set_key(self, public_key: str) -> m.Me:
        raise NotImplementedError

    async def party(self, party: str) -> m.PartyView:
        raise NotImplementedError

    # catalog
    async def publish(self, document: dict[str, Any]) -> m.CatalogEntry:
        raise NotImplementedError

    async def search(
        self, q: str = "", *, cursor: str | None = None, limit: int = 20
    ) -> m.CatalogList:
        raise NotImplementedError

    async def get_offering(self, ref: str) -> m.CatalogEntry:
        """``ref`` is ``acme/enrich``, ``acme/enrich@1``, ``@1.2`` or exact; the gate resolves
        partial versions to the latest match (path version ``latest``, ``1``, ``1.2``, ``1.2.0``)."""
        raise NotImplementedError

    async def card(self, ref: str) -> dict[str, Any]:
        raise NotImplementedError

    async def by_endpoint(self, endpoint: str) -> m.CatalogEntry:
        """The offering behind an endpoint name. Endpoint names are opaque: this lookup is
        the only way from one to the other."""
        raise NotImplementedError

    # quotes & jobs
    async def quote(self, offering: str, payload: m.WirePayload) -> m.QuoteView:
        raise NotImplementedError

    async def hire(self, quote_id: str, *, max_price_minor: int | None = None) -> m.HireView:
        raise NotImplementedError

    async def jobs(
        self,
        *,
        as_: str = "buyer",
        state: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> m.JobList:
        raise NotImplementedError

    async def job(self, job_id: str) -> m.JobView:
        raise NotImplementedError

    async def seller_job(self, job_id: str) -> m.SellerJobView:
        raise NotImplementedError

    async def accept(self, job_id: str) -> m.JobView:
        raise NotImplementedError

    async def reject(self, job_id: str, reason: str) -> m.JobView:
        raise NotImplementedError

    async def cancel(self, job_id: str) -> m.JobView:
        raise NotImplementedError

    async def start(self, job_id: str, payload: m.WirePayload) -> m.JobView:
        raise NotImplementedError

    async def result(self, job_id: str) -> m.ResultView:
        raise NotImplementedError

    def progress(self, job_id: str, *, after: int = 0) -> AsyncIterator[m.ProgressEvent]:
        """Server-sent events until the job is closed; reconnects from the last ``seq``."""
        raise NotImplementedError

    async def post_progress(self, job_id: str, event: m.ProgressPost) -> None:
        raise NotImplementedError

    # artifacts & claim-check blobs
    async def create_artifact(self, req: m.ArtifactCreate) -> m.ArtifactUpload:
        raise NotImplementedError

    async def complete_artifact(self, key: str, req: m.ArtifactComplete) -> None:
        raise NotImplementedError

    async def download_artifact(self, key: str) -> m.DownloadUrl:
        raise NotImplementedError

    async def create_blob(self, req: m.BlobCreate) -> m.BlobUrl:
        raise NotImplementedError

    async def get_blob(self, sha256: str) -> m.BlobUrl:
        raise NotImplementedError

    # money
    async def balance(self, currency: str = "USD") -> m.Balance:
        raise NotImplementedError

    async def topup(self, amount_minor: int, currency: str = "USD") -> m.TopupResult:
        raise NotImplementedError

    # disputes
    async def disputes(self) -> m.JobList:
        raise NotImplementedError

    async def resolve(self, job_id: str, req: m.ResolveRequest) -> m.JobView:
        raise NotImplementedError

    async def health(self) -> dict[str, Any]:
        raise NotImplementedError
