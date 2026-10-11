"""Quotes, hire and escrow on the fake gate, driven by a scripted control plane: every rule
of PRD §8.2/§8.3, §10.5 and decisions I4/I6/I10/I11, as the platform gate applies them
(the rules themselves are kontra.escrow, shared by both gates)."""

from __future__ import annotations

import asyncio
import base64
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from temporalio.api.common.v1 import Payload

from kontra._control import HistorySummary, NexusSyncKind, NexusSyncResult
from kontra.auth import Session
from kontra.escrow import ObservedStatus
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.ledger import PLATFORM_PARTY
from kontra.testing.fake_gate import FakeGate
from kontra.testing.ledger_contract import assert_invariants

from .conftest import World
from .support import ScriptedControl, json_payload, ok_quote, quote_payload, wire


@dataclass
class Shop:
    world: World
    seller: Session
    buyer: Session
    entry: m.CatalogEntry

    @property
    def gate(self) -> FakeGate:
        return self.world.gate

    @property
    def control(self) -> ScriptedControl:
        return self.world.control

    async def quote(self, amount_minor: int = 1234, **kw: object) -> m.QuoteView:
        self.control.responder = lambda p, h: ok_quote(amount_minor)
        return await self.buyer.gate.quote(self.entry.name, wire(json_payload({"text": "hi"})))

    async def hire(self, amount_minor: int = 1234) -> m.HireView:
        q = await self.quote(amount_minor)
        assert q.quote_id is not None
        return await self.buyer.gate.hire(q.quote_id)

    def observe(
        self, job_id: str, status: ObservedStatus, result: bytes | None = None, **kw: object
    ) -> None:
        self.control.observe_as("acme", f"job-{job_id}", status, result, **kw)

    async def balances(self) -> tuple[int, int, int]:
        ledger = self.gate.ledger
        b = await ledger.balance(self.buyer.profile.party, "USD")
        s = await ledger.balance("acme", "USD")
        k = await ledger.balance(PLATFORM_PARTY, "USD")
        return b.available_minor, s.available_minor, k.available_minor


@pytest.fixture
async def shop(world: World) -> Shop:
    seller = await world.party("acme")
    buyer = await world.party("bob")
    entry = await world.publish(seller, max_concurrent_jobs=2, review_window="1h")
    return Shop(world, seller, buyer, entry)


# --------------------------------------------------------------------------- quotes


async def test_quote_relays_the_buyers_bytes_and_prices(shop: Shop) -> None:
    payload = Payload(metadata={"encoding": b"binary/kontra-age"}, data=b"\x00ciphertext\xff")
    shop.control.responder = lambda p, h: ok_quote(1234, 600)
    q = await shop.buyer.gate.quote("acme/enrich@1", wire(payload))
    assert q.status is m.QuoteStatus.OK
    assert (q.amount_minor, q.currency, q.price, q.max_duration_seconds) == (
        1234,
        "USD",
        "12.34",
        600,
    )
    assert (q.offering, q.endpoint, q.service) == (
        shop.entry.offering,
        shop.entry.endpoint,
        "acme.enrich",
    )
    assert (q.contract_hash, q.seller_public_key) == (
        shop.entry.contract_hash,
        shop.entry.public_key,
    )
    assert q.expires_at is not None
    assert timedelta(minutes=9) < q.expires_at - shop.gate.now() <= timedelta(minutes=10)
    ns, tq, svc, op, relayed, headers, timeout = shop.control.sync_calls[-1]
    assert (ns, tq, svc, op, headers, timeout) == (
        "acme",
        "acme/enrich@1.0.0",
        "acme.enrich",
        "quote",
        {},
        10.0,
    )
    assert relayed.SerializeToString() == payload.SerializeToString()  # byte for byte


@pytest.mark.parametrize(
    ("answer", "status", "fields"),
    [
        (
            quote_payload(status="rejected", reason="too long"),
            m.QuoteStatus.REJECTED,
            {"reason": "too long"},
        ),
        (quote_payload(status="busy", eta_seconds=30), m.QuoteStatus.BUSY, {"eta_seconds": 30}),
    ],
)
async def test_quote_rejections_and_seller_busy(
    shop: Shop, answer: Payload, status: m.QuoteStatus, fields: dict[str, object]
) -> None:
    shop.control.responder = lambda p, h: answer
    q = await shop.buyer.gate.quote("acme/enrich", wire(json_payload({})))
    assert q.status is status
    assert q.quote_id is None
    for k, v in fields.items():
        assert getattr(q, k) == v


@pytest.mark.parametrize(
    ("answer", "status", "code"),
    [
        (NexusSyncResult(NexusSyncKind.HANDLER_ERROR, message="boom"), 502, "seller_error"),
        (
            NexusSyncResult(NexusSyncKind.UNAVAILABLE, message="no worker"),
            503,
            "seller_unavailable",
        ),
        (NexusSyncResult(NexusSyncKind.TIMEOUT, message="slow"), 503, "seller_unavailable"),
        (Payload(metadata={"encoding": b"binary/kontra-age"}, data=b"x"), 502, "invalid_quote"),
        (quote_payload(status="ok"), 502, "invalid_quote"),
        (
            quote_payload(status="ok", amount_minor=1, currency="EUR", max_duration_seconds=5),
            502,
            "invalid_quote",
        ),
        (
            quote_payload(status="ok", amount_minor=1, currency="USD", max_duration_seconds=7200),
            502,
            "invalid_quote",
        ),
        (json_payload("not a quote"), 502, "invalid_quote"),
    ],
)
async def test_quote_failures(shop: Shop, answer: object, status: int, code: str) -> None:
    shop.control.responder = lambda p, h: answer  # type: ignore[assignment,return-value]
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.quote("acme/enrich", wire(json_payload({})))
    assert (e.value.status, e.value.code) == (status, code)


@pytest.mark.parametrize(
    ("b64", "status", "code"),
    [
        ("not base64!", 422, "invalid_payload"),
        (base64.b64encode(b"\xff\xff\xff").decode(), 422, "invalid_payload"),
        ("A" * (4 * math.ceil(2 * 1024 * 1024 / 3) + 4), 413, "too_large"),
    ],
)
async def test_quote_inputs_must_be_payloads(shop: Shop, b64: str, status: int, code: str) -> None:
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.quote("acme/enrich", m.WirePayload(b64=b64))
    assert (e.value.status, e.value.code) == (status, code)


async def test_quote_unknown_offering(shop: Shop) -> None:
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.quote("acme/enrich@2", wire(json_payload({})))
    assert (e.value.status, e.value.code) == (404, "offering_not_found")


async def test_quote_is_busy_at_the_concurrency_limit_without_asking_the_seller(shop: Shop) -> None:
    spare = await shop.quote()  # quoted while there is room
    first, second = await shop.hire(), await shop.hire()
    calls = len(shop.control.sync_calls)
    q = await shop.buyer.gate.quote("acme/enrich", wire(json_payload({})))
    assert q.status is m.QuoteStatus.BUSY
    assert len(shop.control.sync_calls) == calls  # answered by the gate itself (I11)
    deadline = min([(await shop.buyer.gate.job(j.job_id)).deadline for j in (first, second)])
    assert q.eta_seconds == math.ceil((deadline - shop.gate.now()).total_seconds())
    assert spare.quote_id is not None
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.hire(spare.quote_id)
    assert (e.value.status, e.value.code) == (409, "busy")


async def test_quotes_are_rate_limited_per_party(tmp_path: object) -> None:
    import httpx

    from .support import logged_in, offering_doc

    control = ScriptedControl(responder=lambda p, h: ok_quote())
    gate = FakeGate(control, sweep_interval=0, quote_rate_per_min=2)
    async with gate:
        t = httpx.ASGITransport(app=gate.app)
        seller = await logged_in(gate.base_url, tmp_path / "acme", "acme", t)  # type: ignore[operator]
        buyer = await logged_in(gate.base_url, tmp_path / "bob", "bob", t)  # type: ignore[operator]
        await seller.gate.publish(offering_doc("acme", seller.profile.public_key or ""))
        for _ in range(2):
            await buyer.gate.quote("acme/enrich", wire(json_payload({})))
        with pytest.raises(GateError) as e:
            await buyer.gate.quote("acme/enrich", wire(json_payload({})))
        assert (e.value.status, e.value.code) == (429, "rate_limited")
        assert e.value.detail is not None
        assert 0 < e.value.detail["retry_after_seconds"] <= 60
        await seller.gate.quote("acme/enrich", wire(json_payload({})))  # per party
        gate.advance(timedelta(minutes=1))
        await buyer.gate.quote("acme/enrich", wire(json_payload({})))  # a new window
        await seller.aclose()
        await buyer.aclose()


# --------------------------------------------------------------------------- hire


async def test_hire_locks_the_price_and_signs_a_hold_token(shop: Shop) -> None:
    before = await shop.balances()
    q = await shop.quote(1234)
    assert q.quote_id is not None
    view = await shop.buyer.gate.hire(q.quote_id, max_price_minor=1234)
    assert (view.amount_minor, view.currency, view.offering) == (1234, "USD", shop.entry.offering)
    assert view.workflow_id == f"job-{view.job_id}"
    assert view.operation_id == f"job-{view.job_id}"
    assert (view.endpoint, view.service) == (shop.entry.endpoint, "acme.enrich")
    assert view.seller_public_key == shop.entry.public_key
    b = await shop.gate.ledger.balance("bob", "USD")
    assert (b.available_minor, b.held_minor) == (before[0] - 1234, 1234)

    jwks = jwt.PyJWKSet.from_dict(shop.gate.keys.jwks())
    claims = m.HoldClaims.model_validate(
        jwt.decode(view.hold_token, jwks.keys[0].key, algorithms=["ES256"], audience="kontra:hold")
    )
    assert claims.job_id == claims.sub == view.job_id
    assert (claims.seller, claims.seller_ns, claims.buyer, claims.buyer_ns) == (
        "acme",
        "acme",
        "bob",
        "bob",
    )
    assert claims.buyer_key == shop.buyer.profile.public_key
    assert (claims.amount_minor, claims.currency) == (1234, "USD")
    assert claims.contract_hash == shop.entry.contract_hash
    assert claims.max_duration_seconds == 3600
    assert claims.exp - claims.iat == 3600  # lock time + max_duration (I10)
    assert view.expires_at == datetime.fromtimestamp(claims.exp, UTC)
    assert claims.iss == shop.gate.issuer


async def test_hire_refusals(shop: Shop) -> None:
    gate = shop.buyer.gate
    q = await shop.quote(1234)
    assert q.quote_id is not None
    with pytest.raises(GateError) as e:
        await gate.hire(q.quote_id, max_price_minor=1000)
    assert (e.value.status, e.value.code) == (409, "price_above_max")
    with pytest.raises(GateError) as e:
        await shop.seller.gate.hire(q.quote_id)  # someone else's quote is not found
    assert (e.value.status, e.value.code) == (404, "quote_not_found")
    job = await gate.hire(q.quote_id)
    with pytest.raises(GateError) as e:
        await gate.hire(q.quote_id)
    assert (e.value.status, e.value.code) == (409, "quote_used")
    assert e.value.detail == {"job_id": job.job_id}
    late = await shop.quote()
    assert late.quote_id is not None
    shop.gate.advance(timedelta(minutes=11))
    with pytest.raises(GateError) as e:
        await gate.hire(late.quote_id)
    assert (e.value.status, e.value.code) == (409, "quote_expired")


async def test_hire_without_funds_frees_the_quote(shop: Shop) -> None:
    q = await shop.quote(50_000)  # the buyer has 100.00
    assert q.quote_id is not None
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.hire(q.quote_id)
    assert (e.value.status, e.value.code) == (402, "insufficient_funds")
    assert (await shop.buyer.gate.jobs()).items == []
    await shop.buyer.gate.topup(50_000)
    job = await shop.buyer.gate.hire(q.quote_id)  # the same quote, after a top-up
    assert job.amount_minor == 50_000


async def test_a_republished_contract_makes_old_quotes_stale(shop: Shop) -> None:
    q = await shop.quote()
    assert q.quote_id is not None
    await shop.world.publish(shop.seller, max_concurrent_jobs=2, description="new contract")
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.hire(q.quote_id)
    assert (e.value.status, e.value.code) == (409, "quote_stale")


async def test_a_buyer_without_a_key_cannot_hire(shop: Shop) -> None:
    tok = await shop.world.anon().dev_login("keyless")
    keyless = shop.world.bearer(tok.access_token)
    shop.control.responder = lambda p, h: ok_quote()
    q = await keyless.quote("acme/enrich", wire(json_payload({})))
    assert q.quote_id is not None
    with pytest.raises(GateError) as e:
        await keyless.hire(q.quote_id)
    assert (e.value.status, e.value.code) == (409, "no_public_key")


# --------------------------------------------------------------------------- start & result


async def test_start_runs_execute_in_the_buyers_namespace(shop: Shop) -> None:
    job = await shop.hire()
    payload = Payload(metadata={"encoding": b"binary/kontra-age"}, data=b"sealed input")
    view = await shop.buyer.gate.start(job.job_id, wire(payload))
    assert view.state is m.JobState.SUBMITTED
    started = shop.control.starts[-1]
    assert (started.namespace, started.operation_id) == ("bob", job.operation_id)
    assert (started.endpoint, started.service, started.operation) == (
        job.endpoint,
        "acme.enrich",
        "execute",
    )
    assert started.payload.SerializeToString() == payload.SerializeToString()
    assert set(started.headers) == {"kontra-hold"}
    reissued = jwt.decode(started.headers["kontra-hold"], options={"verify_signature": False})
    assert reissued == jwt.decode(job.hold_token, options={"verify_signature": False})
    remaining = view.deadline - shop.gate.now()
    assert abs(started.schedule_to_close - remaining) < timedelta(seconds=2)  # until the deadline
    await shop.buyer.gate.start(job.job_id, wire(payload))  # idempotent
    assert len(shop.control.starts) == 1
    with pytest.raises(GateError) as e:
        await shop.seller.gate.start(job.job_id, wire(payload))
    assert (e.value.status, e.value.code) == (403, "forbidden")


async def test_start_after_the_hold_expired_or_closed(shop: Shop) -> None:
    job = await shop.hire()
    shop.gate.advance(timedelta(hours=1))
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.start(job.job_id, wire(json_payload({})))
    assert (e.value.status, e.value.code) == (409, "hold_expired")
    shop.gate.advance(timedelta(seconds=2))
    await shop.gate.sweep()  # past the deadline: refunded
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.start(job.job_id, wire(json_payload({})))
    assert (e.value.status, e.value.code) == (409, "not_open")


async def test_result_is_the_stored_ciphertext_for_the_buyer_only(shop: Shop) -> None:
    job = await shop.hire()
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.result(job.job_id)
    assert (e.value.status, e.value.code) == (404, "not_delivered")
    stored = Payload(
        metadata={"encoding": b"binary/kontra-age"}, data=b"sealed result"
    ).SerializeToString()
    shop.observe(job.job_id, ObservedStatus.COMPLETED, stored)
    await shop.gate.sweep()
    res = await shop.buyer.gate.result(job.job_id)
    assert base64.b64decode(res.payload.b64) == stored
    import hashlib

    assert (res.sha256, res.size) == (hashlib.sha256(stored).hexdigest(), len(stored))
    view = await shop.buyer.gate.job(job.job_id)
    assert view.delivery is not None
    assert (view.delivery.sha256, view.delivery.size) == (res.sha256, res.size)
    with pytest.raises(GateError) as e:
        await shop.seller.gate.result(job.job_id)
    assert (e.value.status, e.value.code) == (403, "forbidden")


# --------------------------------------------------------------------------- escrow


async def test_accept_releases_with_the_platform_fee(shop: Shop) -> None:
    b0, s0, k0 = await shop.balances()
    job = await shop.hire(1000)
    shop.observe(job.job_id, ObservedStatus.RUNNING)
    await shop.gate.sweep()
    assert (await shop.buyer.gate.job(job.job_id)).state is m.JobState.WORKING
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.accept(job.job_id)
    assert (e.value.status, e.value.code) == (409, "not_delivered")
    shop.observe(job.job_id, ObservedStatus.COMPLETED, b"r")
    await shop.gate.sweep()
    with pytest.raises(GateError) as e:
        await shop.seller.gate.accept(job.job_id)
    assert (e.value.status, e.value.code) == (403, "forbidden")
    view = await shop.buyer.gate.accept(job.job_id)
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.RELEASED)
    assert await shop.balances() == (b0 - 1000, s0 + 950, k0 + 50)
    await shop.gate.sweep()
    assert shop.gate.jobs[job.job_id].settled
    await assert_invariants(shop.gate.ledger)


async def test_silence_releases_when_the_review_window_closes(shop: Shop) -> None:
    job = await shop.hire(1000)
    shop.observe(job.job_id, ObservedStatus.COMPLETED, b"r")
    await shop.gate.sweep()
    view = await shop.buyer.gate.job(job.job_id)
    assert view.review_deadline is not None
    assert view.escrow is m.Escrow.HELD
    shop.gate.advance(timedelta(minutes=59))
    await shop.gate.sweep()
    assert (await shop.buyer.gate.job(job.job_id)).escrow is m.Escrow.HELD
    shop.gate.advance(timedelta(minutes=2))
    await shop.gate.sweep()
    assert (await shop.buyer.gate.job(job.job_id)).escrow is m.Escrow.RELEASED
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.reject(job.job_id, "too late")
    assert (e.value.status, e.value.code) == (409, "escrow_closed")


async def test_reject_disputes_and_the_resolver_splits(shop: Shop) -> None:
    judge = await shop.world.party("judge")
    b0, s0, k0 = await shop.balances()
    job = await shop.hire(1000)
    shop.observe(job.job_id, ObservedStatus.COMPLETED, b"r")
    await shop.gate.sweep()
    view = await shop.buyer.gate.reject(job.job_id, "empty records")
    assert (view.escrow, view.reason) == (m.Escrow.DISPUTED, "empty records")
    shop.gate.advance(timedelta(hours=2))
    await shop.gate.sweep()  # a dispute is not released by the window
    assert (await shop.buyer.gate.job(job.job_id)).escrow is m.Escrow.DISPUTED
    with pytest.raises(GateError) as e:
        await judge.gate.resolve(job.job_id, m.ResolveRequest(buyer_minor=1, seller_minor=1))
    assert (e.value.status, e.value.code) == (422, "invalid_split")
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.resolve(
            job.job_id, m.ResolveRequest(buyer_minor=600, seller_minor=400)
        )
    assert (e.value.status, e.value.code) == (403, "not_resolver")
    resolved = await judge.gate.resolve(
        job.job_id, m.ResolveRequest(buyer_minor=600, seller_minor=400)
    )
    assert resolved.escrow is m.Escrow.RESOLVED
    assert await shop.balances() == (b0 - 400, s0 + 380, k0 + 20)
    again = await judge.gate.resolve(
        job.job_id, m.ResolveRequest(buyer_minor=600, seller_minor=400)
    )
    assert again.escrow is m.Escrow.RESOLVED
    with pytest.raises(GateError) as e:
        await judge.gate.resolve(job.job_id, m.ResolveRequest(buyer_minor=0, seller_minor=1000))
    assert (e.value.status, e.value.code) == (409, "already_resolved")
    assert (await judge.gate.disputes()).items == []


async def test_resolving_a_job_that_is_not_disputed(shop: Shop) -> None:
    judge = await shop.world.party("judge")
    job = await shop.hire(1000)
    with pytest.raises(GateError) as e:
        await judge.gate.resolve(job.job_id, m.ResolveRequest(buyer_minor=1000, seller_minor=0))
    assert (e.value.status, e.value.code) == (409, "not_disputed")
    with pytest.raises(GateError) as e:
        await judge.gate.resolve("0" * 32, m.ResolveRequest(buyer_minor=1000, seller_minor=0))
    assert (e.value.status, e.value.code) == (404, "job_not_found")


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (ObservedStatus.FAILED, "seller workflow failed"),
        (ObservedStatus.TIMED_OUT, "seller workflow timed out"),
        (ObservedStatus.TERMINATED, "seller workflow terminated"),
    ],
)
async def test_unsuccessful_closes_refund(shop: Shop, status: ObservedStatus, reason: str) -> None:
    b0 = (await shop.balances())[0]
    job = await shop.hire(1000)
    shop.observe(job.job_id, status)
    await shop.gate.sweep()
    view = await shop.buyer.gate.job(job.job_id)
    assert (view.state, view.escrow, view.reason) == (m.JobState.FAILED, m.Escrow.REFUNDED, reason)
    assert (await shop.balances())[0] == b0
    assert (await shop.world.anon().party("acme")).refunded == 1


async def test_a_seller_who_never_starts_is_refunded_at_the_deadline(shop: Shop) -> None:
    job = await shop.hire(1000)
    view = await shop.buyer.gate.job(job.job_id)
    assert view.deadline - view.created_at == timedelta(hours=1, seconds=1)  # max_duration + grace
    shop.gate.advance(timedelta(hours=1))
    await shop.gate.sweep()
    assert (await shop.buyer.gate.job(job.job_id)).escrow is m.Escrow.HELD
    shop.gate.advance(timedelta(seconds=2))
    await shop.gate.sweep()
    view = await shop.buyer.gate.job(job.job_id)
    assert (view.state, view.escrow, view.reason) == (
        m.JobState.FAILED,
        m.Escrow.REFUNDED,
        "seller never started the job",
    )
    assert shop.control.terminates == []


async def test_running_past_the_deadline_is_terminated_and_refunded(shop: Shop) -> None:
    job = await shop.hire(1000)
    shop.observe(job.job_id, ObservedStatus.RUNNING)
    shop.control.fail_stops = 2  # the terminate and its retry in the same pass both fail
    shop.gate.advance(timedelta(hours=1, seconds=2))
    await shop.gate.sweep()
    view = await shop.buyer.gate.job(job.job_id)
    assert (view.state, view.escrow, view.reason) == (
        m.JobState.FAILED,
        m.Escrow.REFUNDED,
        "max_duration exceeded",
    )
    assert shop.control.terminates == []
    assert shop.gate.jobs[job.job_id].pending_stop is not None
    await shop.gate.sweep()
    assert shop.control.terminates == [("acme", f"job-{job.job_id}", "max_duration exceeded")]
    assert shop.gate.jobs[job.job_id].pending_stop is None


async def test_a_delivery_after_the_deadline_refunds(shop: Shop) -> None:
    job = await shop.hire(1000)
    late = shop.gate.now() + timedelta(hours=2)
    shop.observe(job.job_id, ObservedStatus.COMPLETED, b"r", closed_at=late)
    await shop.gate.sweep()
    view = await shop.buyer.gate.job(job.job_id)
    assert (view.escrow, view.reason) == (m.Escrow.REFUNDED, "delivered after max_duration")


async def test_cancel_refunds_now_and_stops_the_workflow(shop: Shop) -> None:
    b0 = (await shop.balances())[0]
    job = await shop.hire(1000)
    with pytest.raises(GateError) as e:
        await shop.seller.gate.cancel(job.job_id)
    assert (e.value.status, e.value.code) == (403, "forbidden")
    view = await shop.buyer.gate.cancel(job.job_id)
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    assert shop.control.cancels == [("acme", f"job-{job.job_id}", "canceled by buyer")]
    assert (await shop.balances())[0] == b0
    shop.observe(job.job_id, ObservedStatus.COMPLETED, b"too late")
    await shop.gate.sweep()  # a delivery after the cancel is ignored
    assert (await shop.buyer.gate.job(job.job_id)).state is m.JobState.CANCELED
    assert (await shop.buyer.gate.cancel(job.job_id)).state is m.JobState.CANCELED  # idempotent
    assert (await shop.world.anon().party("acme")).refunded == 0  # sellers price for cancels


async def test_an_unobservable_job_waits_for_the_platform(shop: Shop) -> None:
    job = await shop.hire(1000)
    shop.control.fail_observe = RuntimeError("frontend down")
    shop.gate.advance(timedelta(hours=1, minutes=5))  # past the deadline, within the give-up
    await shop.gate.sweep()
    assert (await shop.buyer.gate.job(job.job_id)).escrow is m.Escrow.HELD
    shop.gate.advance(timedelta(minutes=15))
    await shop.gate.sweep()
    assert (await shop.buyer.gate.job(job.job_id)).escrow is m.Escrow.REFUNDED


async def test_the_sweeper_repairs_a_ledger_call_that_never_ran(shop: Shop) -> None:
    job = await shop.hire(1000)
    shop.observe(job.job_id, ObservedStatus.COMPLETED, b"r")
    await shop.gate.sweep()
    record = shop.gate.jobs[job.job_id]
    # the gate saved "released" and crashed before calling the ledger
    from dataclasses import replace

    record.record = replace(record.record, escrow=m.Escrow.RELEASED)
    record.settled = False
    hold = await shop.gate.ledger.get_hold(job.job_id)
    assert hold is not None
    assert hold.state.value == "held"
    await shop.gate.sweep()
    hold = await shop.gate.ledger.get_hold(job.job_id)
    assert hold is not None
    assert hold.state.value == "released"
    assert record.settled


# --------------------------------------------------------------------------- views


async def test_job_views_and_lists(shop: Shop) -> None:
    jobs = [await shop.hire(100) for _ in range(2)]
    shop.observe(jobs[0].job_id, ObservedStatus.RUNNING)
    await shop.gate.sweep()
    stranger = await shop.world.party()
    with pytest.raises(GateError) as e:
        await stranger.gate.job(jobs[0].job_id)
    assert (e.value.status, e.value.code) == (404, "job_not_found")
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.seller_job(jobs[0].job_id)
    assert (e.value.status, e.value.code) == (403, "forbidden")
    seller_view = await shop.seller.gate.seller_job(jobs[0].job_id)
    assert seller_view.buyer_public_key == shop.buyer.profile.public_key
    assert seller_view.contract_hash == shop.entry.contract_hash

    mine = await shop.buyer.gate.jobs()
    assert [j.job_id for j in mine.items] == [jobs[1].job_id, jobs[0].job_id]  # newest first
    assert [j.job_id for j in (await shop.buyer.gate.jobs(state="working")).items] == [
        jobs[0].job_id
    ]
    assert [j.job_id for j in (await shop.seller.gate.jobs(as_="seller")).items] == [
        jobs[1].job_id,
        jobs[0].job_id,
    ]
    assert (await shop.seller.gate.jobs()).items == []  # acme bought nothing
    page = await shop.buyer.gate.jobs(limit=1)
    assert page.next_cursor is not None
    rest = await shop.buyer.gate.jobs(limit=1, cursor=page.next_cursor)
    assert [j.job_id for j in rest.items] == [jobs[0].job_id]
    assert rest.next_cursor is None
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.jobs(cursor="nope")
    assert e.value.code == "invalid_cursor"
    view = await shop.buyer.gate.job(jobs[0].job_id)
    assert (view.price, view.amount_minor, view.state) == ("1.00", 100, m.JobState.WORKING)
    assert view.started_at is not None


async def test_balance_and_topup(shop: Shop) -> None:
    bal = await shop.buyer.gate.balance()
    assert (bal.party, bal.currency, bal.available) == ("bob", "USD", "100.00")
    res = await shop.buyer.gate.topup(250)
    assert res.balance is not None
    assert res.balance.available == "102.50"
    usdc = await shop.buyer.gate.balance("USDC")
    assert (usdc.available_minor, usdc.available) == (0, "0.000000")
    for currency in ("EUR", "usd"):
        with pytest.raises(GateError) as e:
            await shop.buyer.gate.balance(currency)
        assert (e.value.status, e.value.code) == (422, "unsupported_currency")
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.topup(1, "EUR")
    assert e.value.code == "unsupported_currency"


async def test_the_sweeper_runs_by_itself_in_asgi_mode(tmp_path: object) -> None:
    import httpx

    from .support import logged_in, offering_doc

    gate = FakeGate(ScriptedControl(responder=lambda p, h: ok_quote(100)), sweep_interval=0.02)
    async with gate:  # starts the sweeper on this loop
        t = httpx.ASGITransport(app=gate.app)
        seller = await logged_in(gate.base_url, tmp_path / "acme", "acme", t)  # type: ignore[operator]
        buyer = await logged_in(gate.base_url, tmp_path / "bob", "bob", t)  # type: ignore[operator]
        await seller.gate.publish(offering_doc("acme", seller.profile.public_key or ""))
        q = await buyer.gate.quote("acme/enrich", wire(json_payload({})))
        job = await buyer.gate.hire(q.quote_id or "")
        gate.advance(timedelta(hours=2))
        for _ in range(200):
            if (await buyer.gate.job(job.job_id)).escrow is m.Escrow.REFUNDED:
                break
            await asyncio.sleep(0.02)
        assert (await buyer.gate.job(job.job_id)).escrow is m.Escrow.REFUNDED
        await seller.aclose()
        await buyer.aclose()
    assert gate._sweeper is None  # pyright: ignore[reportPrivateUsage]


# --------------------------------------------------------------------------- review regressions


async def test_a_cancel_after_an_unseen_delivery_is_refused(shop: Shop) -> None:
    """The buyer's operation hands it the result as soon as the seller's workflow closes;
    a cancel landing before the sweeper's next look must not refund a delivered job."""
    b0, s0, k0 = await shop.balances()
    job = await shop.hire(1000)
    shop.observe(job.job_id, ObservedStatus.COMPLETED, b"the delivery")  # no sweep yet
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.cancel(job.job_id)
    assert (e.value.status, e.value.code) == (409, "not_open")
    view = await shop.buyer.gate.job(job.job_id)
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.HELD)
    assert view.delivery is not None
    assert base64.b64decode((await shop.buyer.gate.result(job.job_id)).payload.b64) == (
        b"the delivery"
    )
    assert shop.control.cancels == []
    await shop.buyer.gate.accept(job.job_id)
    assert await shop.balances() == (b0 - 1000, s0 + 950, k0 + 50)


async def test_a_cancel_is_refused_while_temporal_cannot_tell(shop: Shop) -> None:
    job = await shop.hire(1000)
    shop.control.fail_observe = RuntimeError("frontend down")
    with pytest.raises(GateError) as e:
        await shop.buyer.gate.cancel(job.job_id)
    assert (e.value.status, e.value.code) == (503, "temporal_unavailable")
    view = await shop.buyer.gate.job(job.job_id)
    assert (view.state, view.escrow) == (m.JobState.SUBMITTED, m.Escrow.HELD)
    with pytest.raises(GateError) as e:  # the seller is refused before anything is looked at
        await shop.seller.gate.cancel(job.job_id)
    assert (e.value.status, e.value.code) == (403, "forbidden")
    shop.control.fail_observe = None
    assert (await shop.buyer.gate.cancel(job.job_id)).escrow is m.Escrow.REFUNDED


async def test_a_run_started_after_the_cancel_is_stopped_until_the_deadline(shop: Shop) -> None:
    """The hold token outlives a cancel (it is checked offline): a start after the cancel,
    by the buyer or by a seller worker that was behind, is canceled by the sweeper until
    the token can no longer start anything."""
    job = await shop.hire(1000)
    wf = f"job-{job.job_id}"
    await shop.buyer.gate.cancel(job.job_id)  # nothing running yet: the cancel finds nothing
    assert shop.control.cancels == [("acme", wf, "canceled by buyer")]
    await shop.gate.sweep()
    assert len(shop.control.cancels) == 1
    shop.observe(job.job_id, ObservedStatus.RUNNING)  # the late start
    await shop.gate.sweep()
    assert shop.control.cancels[1:] == [("acme", wf, "canceled by buyer")]
    shop.observe(job.job_id, ObservedStatus.CANCELED)
    await shop.gate.sweep()
    assert len(shop.control.cancels) == 2
    view = await shop.buyer.gate.job(job.job_id)
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    shop.observe(job.job_id, ObservedStatus.RUNNING)
    shop.gate.advance(timedelta(hours=1, seconds=2))  # past the deadline: no token can start
    await shop.gate.sweep()
    assert len(shop.control.cancels) == 2


async def test_a_resolver_cannot_decide_their_own_dispute(shop: Shop) -> None:
    judge = await shop.world.party("judge")
    shop.gate.resolvers = frozenset({"judge", "bob", "acme"})
    job = await shop.hire(1000)
    shop.observe(job.job_id, ObservedStatus.COMPLETED, b"r")
    await shop.gate.sweep()
    await shop.buyer.gate.reject(job.job_id, "not what I asked for")
    for party in (shop.buyer, shop.seller):
        with pytest.raises(GateError) as e:
            await party.gate.resolve(job.job_id, m.ResolveRequest(buyer_minor=1000, seller_minor=0))
        assert (e.value.status, e.value.code) == (403, "conflict_of_interest")
    with pytest.raises(GateError) as e:
        await judge.gate.resolve(
            job.job_id, m.ResolveRequest(buyer_minor=1000, seller_minor=0, note="x" * 2001)
        )
    assert (e.value.status, e.value.code) == (422, "invalid_note")
    assert (await shop.buyer.gate.job(job.job_id)).escrow is m.Escrow.DISPUTED
    view = await judge.gate.resolve(job.job_id, m.ResolveRequest(buyer_minor=1000, seller_minor=0))
    assert view.escrow is m.Escrow.RESOLVED


async def test_the_last_history_event_carries_the_final_state(shop: Shop) -> None:
    """The workflow completes between the sweeper's observation and its history read: the
    summary is already final while the job is still ``working``. The next pass delivers the
    job with the same summary, and the stream must still end on a ``completed`` event."""
    job = await shop.hire(1000)
    wf = f"job-{job.job_id}"
    done = HistorySummary(
        state=ObservedStatus.COMPLETED,
        last_event="WorkflowExecutionCompleted",
        activities_completed=2,
    )
    shop.observe(job.job_id, ObservedStatus.RUNNING)
    shop.control.summaries[("acme", wf)] = done  # read after the workflow closed
    await shop.gate.sweep()
    shop.observe(job.job_id, ObservedStatus.COMPLETED, b"r")
    await shop.gate.sweep()
    events = shop.gate.jobs[job.job_id].events
    assert [(e.state, e.last_event) for e in events] == [
        (m.JobState.WORKING, "WorkflowExecutionCompleted"),
        (m.JobState.COMPLETED, "WorkflowExecutionCompleted"),
    ]
    assert shop.gate.jobs[job.job_id].progress_final
    await shop.gate.sweep()
    assert len(shop.gate.jobs[job.job_id].events) == 2
