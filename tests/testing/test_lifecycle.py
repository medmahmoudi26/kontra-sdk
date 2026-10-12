"""Whole job lifecycles on a real Temporal dev server, with no containers (PRD §19.3): the
fake gate relays quotes to a real Nexus sync handler, starts ``execute`` as a standalone
operation in the buyer's namespace, watches ``job-<id>`` in the seller's namespace, and
moves money by kontra.escrow -- delivery and accept, silence and release, failure,
timeout and cancel refunds, with progress streamed from history."""

from __future__ import annotations

import asyncio
import base64
import json
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import timedelta

import pytest
from temporalio.api.workflowservice.v1 import StartNexusOperationExecutionRequest
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.converter import NexusSerializationContext

from kontra.codec import ENCODING, KontraCodec, data_converter
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.testing import Marketplace, TestParty
from kontra.testing.devserver import DevServer
from kontra.testing.ledger_contract import assert_invariants

from .seller import PRICE_PER_CHAR, serving
from .support import handle, json_payload, offering_doc, unwire, wire

pytestmark = [pytest.mark.temporal, pytest.mark.timeout(120)]

MARKER = "kontra-marker-7f3e"
OPEN = (m.JobState.SUBMITTED, m.JobState.WORKING)


@pytest.fixture
async def mp(dev_server: DevServer) -> AsyncIterator[Marketplace]:
    async with Marketplace(
        dev_server=dev_server, quote_timeout=4.0, resolvers=("judge",)
    ) as market:
        yield market


@dataclass
class Deal:
    mp: Marketplace
    seller: TestParty
    buyer: TestParty
    entry: m.CatalogEntry
    jwks: dict[str, object]

    def seller_client(self) -> Client:
        return self.mp.client(self.seller.namespace)

    async def hire(self, text: str, mode: str = "ok") -> m.HireView:
        q = await self.buyer.gate.quote(
            self.entry.name, wire(json_payload({"text": text, "mode": mode}))
        )
        assert q.status is m.QuoteStatus.OK, q
        assert q.quote_id is not None
        return await self.buyer.gate.hire(q.quote_id)

    async def start(self, job: m.HireView, text: str, mode: str = "ok") -> None:
        await self.buyer.gate.start(job.job_id, wire(json_payload({"text": text, "mode": mode})))

    async def until(
        self, job_id: str, ok: Callable[[m.JobView], bool], within: float = 30
    ) -> m.JobView:
        loop = asyncio.get_running_loop()
        end = loop.time() + within
        while True:
            view = await self.buyer.gate.job(job_id)
            if ok(view):
                return view
            if loop.time() > end:
                raise AssertionError(f"job never got there: {view}")
            await asyncio.sleep(0.1)


@pytest.fixture
async def deal(mp: Marketplace) -> Deal:
    seller = await mp.party(handle("seller"))
    buyer = await mp.party(handle("buyer"), balance="50.00")
    entry = await seller.gate.publish(
        offering_doc(seller.handle, seller.public_key, review_window="1h")
    )
    jwks = await buyer.gate.jwks()
    return Deal(mp, seller, buyer, entry, jwks)


async def _workflow_status(client: Client, workflow_id: str) -> WorkflowExecutionStatus | None:
    return (await client.get_workflow_handle(workflow_id).describe()).status


async def test_quote_hire_deliver_accept(deal: Deal) -> None:
    async with serving(deal.seller_client(), deal.entry, deal.jwks):
        q = await deal.buyer.gate.quote(deal.entry.name, wire(json_payload({"text": "hello"})))
        assert (q.status, q.amount_minor, q.price) == (m.QuoteStatus.OK, PRICE_PER_CHAR * 5, "5.00")
        job = await deal.buyer.gate.hire(q.quote_id or "")
        events: list[m.ProgressEvent] = []

        async def follow() -> None:
            async for e in deal.buyer.gate.progress(job.job_id):
                events.append(e)

        follower = asyncio.create_task(follow())
        await deal.start(job, "hello")
        view = await deal.until(job.job_id, lambda v: v.state is m.JobState.COMPLETED)
        await asyncio.wait_for(follower, 30)

    assert view.delivery is not None
    assert view.escrow is m.Escrow.HELD
    assert view.started_at is not None
    result = await deal.buyer.gate.result(job.job_id)
    assert (result.sha256, result.size) == (view.delivery.sha256, view.delivery.size)
    assert json.loads(unwire(result.payload).data) == {"text": "HELLO"}

    assert events, "no progress from history"
    assert [e.seq for e in events] == list(range(1, len(events) + 1))
    assert all(e.source is m.ProgressSource.HISTORY for e in events)
    assert events[-1].state is m.JobState.COMPLETED
    assert events[-1].last_event == "WorkflowExecutionCompleted"
    assert events[-1].activities_completed == 2

    accepted = await deal.buyer.gate.accept(job.job_id)
    assert accepted.escrow is m.Escrow.RELEASED
    assert (await deal.buyer.balance()).available == "45.00"
    assert (await deal.seller.balance()).available == "104.75"  # 5.00 minus the 5% fee
    await deal.mp.gate.run(lambda: assert_invariants(deal.mp.gate.ledger))


async def test_silence_releases_after_the_review_window(deal: Deal) -> None:
    async with serving(deal.seller_client(), deal.entry, deal.jwks):
        job = await deal.hire("quiet")
        await deal.start(job, "quiet")
        await deal.until(job.job_id, lambda v: v.state is m.JobState.COMPLETED)
    deal.mp.gate.advance(timedelta(hours=1, seconds=1))
    view = await deal.until(job.job_id, lambda v: v.escrow is not m.Escrow.HELD)
    assert view.escrow is m.Escrow.RELEASED


async def test_reject_and_resolve(deal: Deal) -> None:
    judge = await deal.mp.party("judge")  # a resolver of this marketplace
    async with serving(deal.seller_client(), deal.entry, deal.jwks):
        job = await deal.hire("dispute")
        await deal.start(job, "dispute")
        await deal.until(job.job_id, lambda v: v.state is m.JobState.COMPLETED)
    await deal.buyer.gate.reject(job.job_id, "not loud enough")
    assert [j.job_id for j in (await judge.gate.disputes()).items] == [job.job_id]
    view = await judge.gate.resolve(job.job_id, m.ResolveRequest(buyer_minor=700, seller_minor=0))
    assert view.escrow is m.Escrow.RESOLVED
    assert (await deal.buyer.balance()).available == "50.00"


async def test_a_failed_seller_workflow_refunds(deal: Deal) -> None:
    async with serving(deal.seller_client(), deal.entry, deal.jwks):
        job = await deal.hire("doomed")
        await deal.start(job, "doomed", mode="fail")
        view = await deal.until(job.job_id, lambda v: v.state not in OPEN)
    assert (view.state, view.escrow, view.reason) == (
        m.JobState.FAILED,
        m.Escrow.REFUNDED,
        "seller workflow failed",
    )
    assert (await deal.buyer.balance()).available == "50.00"
    with pytest.raises(GateError) as e:
        await deal.buyer.gate.result(job.job_id)
    assert e.value.code == "not_delivered"


async def test_running_past_max_duration_is_terminated_and_refunded(deal: Deal) -> None:
    async with serving(deal.seller_client(), deal.entry, deal.jwks):
        job = await deal.hire("forever")
        await deal.start(job, "forever", mode="slow")
        await deal.until(job.job_id, lambda v: v.state is m.JobState.WORKING)
        deal.mp.gate.advance(timedelta(hours=1, seconds=2))  # past max_duration + grace
        view = await deal.until(job.job_id, lambda v: v.escrow is m.Escrow.REFUNDED)
        assert (view.state, view.reason) == (m.JobState.FAILED, "max_duration exceeded")
        client = deal.seller_client()
        for _ in range(100):
            if (
                await _workflow_status(client, job.workflow_id)
                is WorkflowExecutionStatus.TERMINATED
            ):
                break
            await asyncio.sleep(0.1)
        assert await _workflow_status(client, job.workflow_id) is WorkflowExecutionStatus.TERMINATED


async def test_cancel_refunds_and_cancels_the_sellers_workflow(deal: Deal) -> None:
    async with serving(deal.seller_client(), deal.entry, deal.jwks):
        job = await deal.hire("stop me")
        await deal.start(job, "stop me", mode="slow")
        await deal.until(job.job_id, lambda v: v.state is m.JobState.WORKING)
        view = await deal.buyer.gate.cancel(job.job_id)
        assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
        client = deal.seller_client()
        for _ in range(100):
            if await _workflow_status(client, job.workflow_id) is WorkflowExecutionStatus.CANCELED:
                break
            await asyncio.sleep(0.1)
        assert await _workflow_status(client, job.workflow_id) is WorkflowExecutionStatus.CANCELED
    assert (await deal.buyer.balance()).available == "50.00"


async def test_an_offline_seller_is_refunded_at_the_deadline(deal: Deal) -> None:
    with pytest.raises(GateError) as e:  # nobody answers the quote
        await deal.buyer.gate.quote(deal.entry.name, wire(json_payload({"text": "x"})))
    assert (e.value.status, e.value.code) == (503, "seller_unavailable")
    assert e.value.detail == {"reason": "unavailable"}
    async with serving(deal.seller_client(), deal.entry, deal.jwks):
        job = await deal.hire("abandoned")
    await deal.start(job, "abandoned")  # the operation waits in the buyer's namespace
    await asyncio.sleep(0.5)
    assert (await deal.buyer.gate.job(job.job_id)).state is m.JobState.SUBMITTED
    deal.mp.gate.advance(timedelta(hours=1, seconds=2))
    view = await deal.until(job.job_id, lambda v: v.escrow is m.Escrow.REFUNDED)
    assert view.reason == "seller never started the job"


#: The order of the P-256 group: (r, s) and (r, N - s) are both valid ECDSA signatures.
_P256_N = 0xFFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551


def _respelled(token: str) -> str:
    """The same hold, spelled so that no byte of it but the signed part is the gate's: the
    other valid ES256 signature ``(r, N - s)``, base64url padding the verifier's decoder
    ignores, and spaces and a tab around it, which HTTP drops before the seller's handler
    sees the header (server 1.32, measured)."""
    signed, _, sig = token.rpartition(".")
    raw = base64.urlsafe_b64decode(sig + "=" * (-len(sig) % 4))
    s = int.from_bytes(raw[32:], "big")
    other = raw[:32] + (_P256_N - s).to_bytes(32, "big")
    return f" \t{signed}.{base64.urlsafe_b64encode(other).decode()} "


async def test_a_respelled_hold_the_handler_accepted_still_pays_the_seller(deal: Deal) -> None:
    """The buyer calls ``execute`` itself with its hold spelled differently from what the
    gate issued. The seller's handler verifies it (it is the gate's signature over the
    gate's claims) and does the job, so escrow must count the run as the buyer's: matching
    the header against the issued token byte for byte let a buyer take the delivery and
    have the job refund at the deadline."""
    async with serving(deal.seller_client(), deal.entry, deal.jwks):
        job = await deal.hire("respelled")
        token = _respelled(job.hold_token)
        assert token.strip() != job.hold_token
        req = StartNexusOperationExecutionRequest(
            namespace=deal.buyer.namespace,
            identity="the buyer itself",
            request_id=str(uuid.uuid4()),
            operation_id=f"mine-{job.job_id}",
            endpoint=deal.entry.endpoint,
            service=deal.entry.service,
            operation="execute",
            input=json_payload({"text": "respelled"}),
        )
        req.schedule_to_close_timeout.FromTimedelta(timedelta(minutes=5))
        req.nexus_header["kontra-hold"] = token
        await deal.mp.client(deal.buyer.namespace).workflow_service.start_nexus_operation_execution(
            req
        )
        client = deal.seller_client()
        for _ in range(200):  # the handler accepted the token and the job ran
            try:
                status = await _workflow_status(client, job.workflow_id)
            except Exception:
                status = None
            if status is WorkflowExecutionStatus.COMPLETED:
                break
            await asyncio.sleep(0.1)
        assert status is WorkflowExecutionStatus.COMPLETED
        view = await deal.until(job.job_id, lambda v: v.state is m.JobState.COMPLETED)
    assert (view.escrow, view.delivery is not None) == (m.Escrow.HELD, True)
    result = await deal.buyer.gate.result(job.job_id)
    assert json.loads(unwire(result.payload).data) == {"text": "RESPELLED"}


async def test_quote_refusals_from_a_real_handler(deal: Deal) -> None:
    async with serving(deal.seller_client(), deal.entry, deal.jwks):
        q = await deal.buyer.gate.quote(
            deal.entry.name, wire(json_payload({"text": "x", "mode": "reject"}))
        )
        assert (q.status, q.reason, q.quote_id) == (m.QuoteStatus.REJECTED, "no thanks", None)
        with pytest.raises(GateError) as e:
            await deal.buyer.gate.quote(
                deal.entry.name, wire(json_payload({"text": "x", "mode": "boom"}))
            )
        assert (e.value.status, e.value.code) == (502, "seller_error")


async def test_payloads_stay_ciphertext_end_to_end(deal: Deal) -> None:
    """Seller and buyer codecs (seam ⑤) around the fake gate: the gate relays and stores
    only ciphertext, and the buyer decrypts the delivery it fetched from the gate."""
    buyer_keys: dict[str, str] = {}
    seller_codec = KontraCodec(
        deal.seller.identity,
        deal.seller.public_key,
        role="seller",
        buyer_keys=_Keys(buyer_keys),
        services=[deal.entry.service],  # the replies it may send in plaintext (quote's price)
    )
    buyer_codec = KontraCodec(
        deal.buyer.identity, deal.buyer.public_key, role="buyer", seller_keys=_Seller(deal.entry)
    )
    seller_client = await deal.seller.temporal_client(data_converter=data_converter(seller_codec))

    async def sealed(op: str, value: object) -> m.WirePayload:
        codec = buyer_codec.with_context(
            NexusSerializationContext(deal.entry.endpoint, deal.entry.service, op)
        )
        [p] = await codec.encode([json_payload(value)])
        assert p.metadata["encoding"] == ENCODING
        return wire(p)

    text = f"secret {MARKER}"
    async with serving(seller_client, deal.entry, deal.jwks, buyer_keys=buyer_keys):
        q = await deal.buyer.gate.quote(deal.entry.name, await sealed("quote", {"text": text}))
        assert q.amount_minor == PRICE_PER_CHAR * len(
            text
        )  # the seller read it; the gate could not
        job = await deal.buyer.gate.hire(q.quote_id or "")
        await deal.buyer.gate.start(job.job_id, await sealed("execute", {"text": text}))
        await deal.until(job.job_id, lambda v: v.state is m.JobState.COMPLETED)

    result = await deal.buyer.gate.result(job.job_id)
    stored = base64.b64decode(result.payload.b64)
    assert MARKER.encode() not in stored
    assert MARKER.upper().encode() not in stored
    [plain] = await buyer_codec.decode([unwire(result.payload)])
    assert json.loads(plain.data) == {"text": text.upper()}
    relayed = deal.mp.gate.jobs[job.job_id].delivery or b""
    assert relayed == stored
    history = await deal.seller_client().get_workflow_handle(job.workflow_id).fetch_history()
    raw = b"".join(e.SerializeToString() for e in history.events)
    assert MARKER.encode() not in raw
    assert MARKER.upper().encode() not in raw


class _Keys:
    def __init__(self, keys: dict[str, str]) -> None:
        self.keys = keys

    async def buyer_key(self, job_id: str) -> str:
        return self.keys[job_id]


class _Seller:
    def __init__(self, entry: m.CatalogEntry) -> None:
        self.entry = entry

    async def seller_key(self, endpoint: str) -> str:
        if endpoint != self.entry.endpoint:
            raise LookupError(endpoint)
        return self.entry.public_key
