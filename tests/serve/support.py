"""Helpers for the serve tests: per-test offering versions, a request-counting gate
transport, and a buyer that hires by hand with plain temporalio (until ``kontra.hire``)."""

from __future__ import annotations

import base64
import itertools
import json
import os
import uuid
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
from temporalio.client import Client, NexusOperationHandle
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import NexusSerializationContext

from kontra.codec import GateBlobStore, KontraCodec
from kontra.gate_client import models as m
from kontra.serve import HOLD_HEADER, seller_data_converter
from kontra.testing import Marketplace, TestParty

#: A buyer key for a direct `GateClient.hire` call. Real hires make one per job
#: (`Profile.new_pending_job_identity`); a test that drives the route itself only needs a
#: well-formed recipient (decisions I7).
_A_BUYER_KEY = "age1e0q4lyexsefmh6hurumvgl3cmmsfnz4zyfhkd7lfrjcpqc2e250qs6j4el"


HERE = Path(__file__).parent
MARKER = "kontra-serve-marker-5f1d"
_versions = itertools.count(1)


def unique_version() -> str:
    """A version no other test in this process uses: its task queue is its own, so a
    worker of another test (or a leftover workflow) never shares it."""
    return f"1.{os.getpid() % 100_000}.{next(_versions)}"


def write_offering(directory: Path, version: str | None = None, **facts: Any) -> Path:
    """``offering.yaml`` for ``acme/upper@<version>`` in ``directory``."""
    spec: dict[str, Any] = {
        "name": "acme/upper",
        "version": version or unique_version(),
        "description": "Upper-cases text, through an activity, a local activity and a child",
        "pricing_hint": "$0.01 per character",
        "max_duration": "10m",
        "review_window": "1h",
        "max_output_bytes": "1MB",
        "artifact_retention": "1d",
        **facts,
    }
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"offering-{uuid.uuid4().hex[:8]}.yaml"
    path.write_text("".join(f"{k}: {json.dumps(v)}\n" for k, v in spec.items()))
    return path


class CountingTransport(httpx.AsyncBaseTransport):
    """An HTTP transport that records every request (the worker's gate calls)."""

    def __init__(self) -> None:
        self.inner = httpx.AsyncHTTPTransport()
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return await self.inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self.inner.aclose()

    def paths(self, since: int = 0) -> list[str]:
        return [f"{r.method} {r.url.path}" for r in self.requests[since:]]


class Keys:
    """The buyer codec's ``SellerKeys``: endpoint -> the seller's recipients, as the hire
    answer says."""

    def __init__(self) -> None:
        self.keys: dict[str, str] = {}

    async def seller_recipients(self, endpoint: str) -> Sequence[str]:
        return [self.keys[endpoint]]


class Buyer:
    """A buyer with nothing but temporalio and the gate client: quote and hire through the
    gate, then start ``execute`` as a standalone Nexus operation in its own namespace."""

    def __init__(
        self, mp: Marketplace, party: TestParty, *, claim_check_threshold: int | None = None
    ) -> None:
        """``claim_check_threshold``: claim-check payloads above that many bytes of
        ciphertext through the gate's blob store (default: never)."""
        self.mp = mp
        self.party = party
        self.keys = Keys()
        self.codec = KontraCodec(
            party.identity,
            [party.public_key],
            role="buyer",
            seller_keys=self.keys,
            blobs=GateBlobStore(party.gate) if claim_check_threshold is not None else None,
            claim_check_threshold=claim_check_threshold or 0,
        )
        config = mp.client(party.namespace).config()
        config["data_converter"] = seller_data_converter(self.codec)
        self.client = Client(**config)

    async def wire(self, entry: m.CatalogEntry, value: Any) -> m.WirePayload:
        """``value`` as the gate relays it to ``quote``: encrypted to the seller and us."""
        self.keys.keys[entry.endpoint] = entry.public_key
        payload = PydanticPayloadConverter().to_payloads([value])[0]
        codec = self.codec.with_context(
            NexusSerializationContext(
                endpoint=entry.endpoint, service=entry.service, operation="quote"
            )
        )
        [encoded] = await codec.encode([payload])
        return m.WirePayload(b64=base64.b64encode(encoded.SerializeToString()).decode())

    async def quote(self, entry: m.CatalogEntry, value: Any) -> m.QuoteView:
        return await self.party.gate.quote(entry.offering, await self.wire(entry, value))

    async def hire(self, entry: m.CatalogEntry, value: Any) -> m.HireView:
        q = await self.quote(entry, value)
        assert q.status == m.QuoteStatus.OK, q
        assert q.quote_id is not None
        hired = await self.party.gate.hire(q.quote_id, buyer_key=_A_BUYER_KEY)
        self.keys.keys[hired.endpoint] = hired.seller_public_key
        return hired

    async def start(
        self,
        hired: m.HireView,
        value: Any,
        *,
        token: str | None = None,
        headers: dict[str, str] | None = None,
        result_type: type | None = None,
    ) -> NexusOperationHandle[Any]:
        """``execute`` with ``token`` (default: the hire's) in the hold header."""
        hdrs = {HOLD_HEADER: token if token is not None else hired.hold_token}
        if headers is not None:
            hdrs = headers
        return await self.client.create_nexus_client(
            service=hired.service, endpoint=hired.endpoint
        ).start_operation(
            "execute",
            value,
            id=f"{hired.operation_id}-{uuid.uuid4().hex[:6]}",
            headers=hdrs,
            schedule_to_close_timeout=timedelta(seconds=hired.max_duration_seconds),
            result_type=result_type,
        )


def failure_text(exc: BaseException) -> str:
    """Every message along an exception's cause chain."""
    out: list[str] = []
    cur: BaseException | None = exc
    while cur is not None:
        out.append(str(cur))
        cur = cur.__cause__
    return " | ".join(out)


def failure_traces(exc: BaseException) -> list[str]:
    """The stack traces the caller received along an exception's cause chain (what a
    Nexus handler error put in the failure, in the caller's history)."""
    out: list[str] = []
    cur: BaseException | None = exc
    while cur is not None:
        trace = getattr(cur, "stack_trace", None)
        if isinstance(trace, str) and trace:
            out.append(trace)
        cur = cur.__cause__
    return out
