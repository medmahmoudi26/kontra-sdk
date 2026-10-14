"""The claim-check of an ``execute`` input is fetched only after the hold token verified
(FIXUPS #20), end to end: a real dev server, the fake gate, a seller's ``kontra.serve``
Worker, and a buyer whose codec claim-checks every input.

temporalio decodes a Nexus operation's input before the handler runs, so before the hold
check. A forged ``execute`` naming a blob must not make the seller download it; the
generated handler takes the payload as it is and opens the claim-check once the token
checks out (:meth:`kontra.codec.KontraCodec.open_deferred`).
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import uuid
from collections.abc import AsyncIterator, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
from temporalio.api.common.v1 import Payload, Payloads
from temporalio.client import Client, NexusOperationFailureError
from temporalio.converter import NexusSerializationContext

from kontra.auth import Session
from kontra.codec import CLAIM_CHECK_ENCODING, GateBlobStore, KontraCodec
from kontra.codec.age import generate_identity
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.profile import Profile
from kontra.serve import HOLD_HEADER, Quote, Worker, offering, seller_data_converter
from kontra.testing import Marketplace, TestParty
from kontra.testing.devserver import (
    CLI_ENV,
    DevServer,
    acquire_shared_dev_server,
    release_shared_dev_server,
)

from .serve_flows import Doc, Measure, Measured

#: A buyer key for a direct `GateClient.hire` call. Real hires make one per job
#: (`Profile.new_pending_job_identity`); a test that drives the route itself only needs a
#: well-formed recipient (decisions I7).
_A_BUYER_KEY = "age1e0q4lyexsefmh6hurumvgl3cmmsfnz4zyfhkd7lfrjcpqc2e250qs6j4el"


pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]

TEXT = "claim-checked " * 50


@pytest.fixture(scope="module")
def dev_server() -> Iterator[DevServer]:
    try:
        server = acquire_shared_dev_server()
    except Exception as e:
        if os.environ.get(CLI_ENV):
            raise  # an explicit binary must work
        pytest.skip(f"no Temporal CLI 1.9.1 dev server: {e}")
    try:
        yield server
    finally:
        release_shared_dev_server()


async def quote(doc: Doc) -> Quote:
    return Quote(price=Decimal(len(doc.text)) / 100)


class Counting(httpx.AsyncBaseTransport):
    """The seller's gate transport: every request it makes, blob downloads included."""

    def __init__(self) -> None:
        self.inner = httpx.AsyncHTTPTransport()
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return await self.inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self.inner.aclose()

    def blob_reads(self, since: int) -> list[str]:
        return [
            f"{r.method} {r.url.path}"
            for r in self.requests[since:]
            if r.method == "GET" and ("/v1/blobs/" in r.url.path or "/_blob/" in r.url.path)
        ]


class Keys:
    def __init__(self) -> None:
        self.keys: dict[str, str] = {}

    async def seller_recipients(self, endpoint: str) -> Sequence[str]:
        return [self.keys[endpoint]]


@dataclass
class Deal:
    buyer: TestParty
    entry: m.CatalogEntry
    codec: KontraCodec
    keys: Keys
    client: Client
    seller_gate: Counting
    #: The secret half of every per-job buyer key this deal made. A delivery is encrypted
    #: to the job's key, never to a key of the buyer party (decisions I7), so the codec
    #: decrypts with these.
    job_identities: list[str] = field(default_factory=list[str])

    async def wire(self, value: Any, op: str) -> m.WirePayload:
        payload = self.client.data_converter.payload_converter.to_payloads([value])[0]
        ctx = NexusSerializationContext(
            endpoint=self.entry.endpoint, service=self.entry.service, operation=op
        )
        [encoded] = await self.codec.with_context(ctx).encode([payload])
        assert encoded.metadata["encoding"] == CLAIM_CHECK_ENCODING
        return m.WirePayload(b64=base64.b64encode(encoded.SerializeToString()).decode())

    async def hire(self, value: Any) -> m.HireView:
        self.keys.keys[self.entry.endpoint] = self.entry.recipients[0]
        q = await self.buyer.gate.quote(self.entry.offering, await self.wire(value, "quote"))
        assert q.status == m.QuoteStatus.OK, q
        assert q.quote_id is not None
        secret, job_key = generate_identity()  # what `kontra hire` makes per job
        self.job_identities.append(secret)
        hired = await self.buyer.gate.hire(q.quote_id, buyer_key=job_key)
        self.keys.keys[hired.endpoint] = hired.seller_recipients[0]
        return hired

    async def execute(
        self, hired: m.HireView, value: Any, token: str, *, within: float | None = None
    ) -> Any:
        handle = await self.client.create_nexus_client(
            service=hired.service, endpoint=hired.endpoint
        ).start_operation(
            "execute",
            value,
            id=f"{hired.operation_id}-{uuid.uuid4().hex[:6]}",
            headers={HOLD_HEADER: token},
            schedule_to_close_timeout=timedelta(seconds=hired.max_duration_seconds),
            result_type=Measured,
        )
        return await asyncio.wait_for(handle.result(), within)


def _offering(directory: Path) -> Path:
    spec = {
        "name": "acme/measure",
        "version": f"1.{os.getpid() % 100_000}.{uuid.uuid4().int % 10_000}",
        "description": "Measures a document",
        "max_duration": "10m",
        "review_window": "1h",
        "max_output_bytes": "1MB",
        "artifact_retention": "1d",
    }
    path = directory / "offering.yaml"
    path.write_text("".join(f"{k}: {json.dumps(v)}\n" for k, v in spec.items()))
    return path


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def deal(
    dev_server: DevServer, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[Deal]:
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="1000.00")
        bound = offering(
            _offering(tmp_path_factory.mktemp("measure")), workflow=Measure, quote=quote
        )
        transport = Counting()
        session = Session(Profile.load(acme.home), transport=transport, home=acme.home)
        worker = Worker(offerings=[bound])
        keys = Keys()
        job_identities: list[str] = []
        codec = KontraCodec(
            bob.identity,
            [bob.public_key],
            role="buyer",
            seller_keys=keys,
            blobs=GateBlobStore(bob.gate),
            claim_check_threshold=0,  # every input goes through the bucket
            extra_identities=lambda: tuple(job_identities),
        )
        config = mp.client(bob.namespace).config()
        config["data_converter"] = seller_data_converter(codec)
        try:
            async with mp.serve(worker, acme, session=session):
                entry = await bob.gate.get_offering(bound.key)
                yield Deal(bob, entry, codec, keys, Client(**config), transport, job_identities)
        finally:
            await session.aclose()


def _tampered(token: str) -> str:
    header, payload, signature = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    claims["amount_minor"] *= 100
    body = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"{header}.{body}.{signature}"


async def test_a_forged_execute_downloads_nothing(deal: Deal) -> None:
    value = Doc(text=TEXT)
    hired = await deal.hire(value)
    for token in (_tampered(hired.hold_token), "not-a-token", ""):
        before = len(deal.seller_gate.requests)
        with pytest.raises(NexusOperationFailureError) as failed:
            await deal.execute(hired, value, token)
        text = " | ".join(str(e) for e in _chain(failed.value))
        assert "hold token" in text, text
        assert deal.seller_gate.blob_reads(before) == []

    # The genuine token: the claim-check is opened after the hold check, once.
    before = len(deal.seller_gate.requests)
    out = await deal.execute(hired, value, hired.hold_token)
    assert out == Measured(length=len(TEXT))
    reads = deal.seller_gate.blob_reads(before)
    assert len([r for r in reads if "/v1/blobs/" in r]) == 1, reads


def _chain(exc: BaseException) -> Iterator[BaseException]:
    cur: BaseException | None = exc
    while cur is not None:
        yield cur
        cur = cur.__cause__


def _blob_downloads(reads: list[str]) -> int:
    return len([r for r in reads if "/v1/blobs/" in r])


async def test_a_quote_fetches_at_most_one_claim_check(deal: Deal) -> None:
    """``quote`` has no hold token to check before its input is read, and temporalio opens a
    Temporal "system" envelope inside a Nexus input and decodes every payload nested in it.
    One relayed quote naming the same blob a thousand times downloads nothing; an ordinary
    claim-checked quote downloads its blob once."""
    deal.keys.keys[deal.entry.endpoint] = deal.entry.recipients[0]
    wire = await deal.wire(Doc(text=TEXT), "quote")
    ref = Payload.FromString(base64.b64decode(wire.b64))
    envelope = Payload(
        metadata={
            "encoding": b"binary/protobuf",
            "messageType": b"temporal.api.common.v1.Payloads",
            "__temporal_system_payload": b"true",
        },
        data=Payloads(payloads=[ref] * 1000).SerializeToString(),
    )
    forged = m.WirePayload(b64=base64.b64encode(envelope.SerializeToString()).decode())
    before = len(deal.seller_gate.requests)
    with pytest.raises(GateError):
        await deal.buyer.gate.quote(deal.entry.offering, forged)
    assert deal.seller_gate.blob_reads(before) == []

    # The budget is per operation: every ordinary quote still gets its one download.
    for _ in range(2):
        before = len(deal.seller_gate.requests)
        q = await deal.buyer.gate.quote(deal.entry.offering, wire)
        assert q.status == m.QuoteStatus.OK, q
        assert _blob_downloads(deal.seller_gate.blob_reads(before)) == 1


async def test_an_input_the_seller_cannot_open_is_refused_at_once(deal: Deal) -> None:
    """A genuine hold, but an input claim-checked to someone else's key: retrying cannot
    help, so the handler refuses it (non-retryable) after one download, instead of a
    retryable error that downloads it again on every attempt until the hold runs out."""
    value = Doc(text=TEXT)
    hired = await deal.hire(value)
    deal.keys.keys[hired.endpoint] = generate_identity()[1]  # the buyer encrypts to a stranger
    before = len(deal.seller_gate.requests)
    try:
        with pytest.raises(NexusOperationFailureError) as failed:
            await deal.execute(hired, value, hired.hold_token, within=60)
    finally:
        deal.keys.keys[hired.endpoint] = hired.seller_recipients[0]
    text = " | ".join(str(e) for e in _chain(failed.value))
    assert "cannot be opened" in text, text
    assert _blob_downloads(deal.seller_gate.blob_reads(before)) == 1
