"""The script door end to end (PRD §7.1, §8): quote, hire, result, accept, and the escrow
outcomes of §8.3 with money moving per §9 (5% fee at release).

Acceptance 9: this module runs twice, on two ledger adapters behind the fake gate (the
in-memory reference and a chain-like one whose every call is ``pending``); every test must
pass on both, unchanged."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from decimal import Decimal

import pytest
import pytest_asyncio
from temporalio.client import WorkflowExecutionStatus

from kontra.contract.names import job_workflow_id
from kontra.gate_client import models as m
from kontra.hire import JobFailed, Record
from kontra.testing import fake_ledger
from kontra.testing.devserver import DevServer
from kontra.testing.ledger_contract import assert_invariants, run_ledger_contract

from .flows import FAIL_MESSAGE
from .support import LEDGERS, PendingLedger, World, open_world, until

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]

START = 10_000  # every buyer starts with 100.00 USD


@pytest_asyncio.fixture(loop_scope="module", scope="module", params=sorted(LEDGERS))
async def world(
    request: pytest.FixtureRequest, dev_server: DevServer, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[World]:
    async with open_world(dev_server, tmp_path_factory.mktemp("acme"), ledger=request.param) as w:
        yield w
        gate = w.mp.gate

        async def audit() -> None:
            await assert_invariants(gate.ledger)

        await gate.run(audit)  # sum(postings) == balances after everything this module did


async def test_quote_hire_result_accept_moves_money_per_s9(world: World) -> None:
    buyer = await world.buyer()
    seller0, platform0 = await world.ledger_balance("acme"), await world.platform()
    text = "x" * 200  # one cent per character: 2.00 USD, a 0.10 fee

    q = await buyer.k.quote(world.key(), text=text)
    assert (q.ok, q.status, q.price, q.currency) == (True, m.QuoteStatus.OK, Decimal("2.00"), "USD")
    assert q.max_duration == timedelta(minutes=2)
    assert q.quote_id is not None
    assert q.expires_at is not None
    assert (q.reason, q.eta) == (None, None)
    assert (await buyer.balance()).held_minor == 0  # a quote locks nothing

    job = await q.hire(max_price="2.00")
    assert (job.offering, job.price, job.currency) == (world.key(), Decimal("2.00"), "USD")
    held = await buyer.balance()
    assert (held.available_minor, held.held_minor) == (START - 200, 200)

    result = await job.result()
    assert isinstance(result, Record)
    assert result.text == text.upper()
    assert result["length"] == 200
    assert result.summary.chars == 200
    assert [p.index for p in result.parts] == [0, 1]
    assert result.file is None
    assert result.to_dict() == {
        "text": text.upper(),
        "length": 200,
        "summary": {"chars": 200, "beats": 0},
        "parts": [{"index": 0, "file": None}, {"index": 1, "file": None}],
        "file": None,
        "files": {},
    }

    view = await job.accept()  # right after result(): waits for the gate to see the delivery
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.RELEASED)
    assert view.delivery is not None
    after = await buyer.balance()
    assert (after.available_minor, after.held_minor) == (START - 200, 0)
    assert await world.ledger_balance("acme") - seller0 == 190  # price minus the 5% fee
    assert await world.platform() - platform0 == 10


async def test_a_failed_seller_workflow_raises_job_failed_and_refunds(world: World) -> None:
    buyer = await world.buyer()
    job = await buyer.k.hire(world.key(), text="boom", mode="fail")
    with pytest.raises(JobFailed) as failed:
        await job.result()
    assert failed.value.reason == FAIL_MESSAGE  # the seller's words, decrypted for the buyer
    assert (failed.value.state, failed.value.job_id) == ("failed", job.job_id)
    view = await world.job(buyer, job.job_id, lambda v: v.escrow is m.Escrow.REFUNDED)
    assert view.state is m.JobState.FAILED
    after = await buyer.balance()
    assert (after.available_minor, after.held_minor) == (START, 0)


async def test_cancel_refunds_and_stops_the_seller(world: World) -> None:
    buyer = await world.buyer()
    seller0 = await world.ledger_balance("acme")
    job = await buyer.k.hire(world.key(), text="take your time", mode="slow")
    await world.job(buyer, job.job_id, lambda v: v.state is m.JobState.WORKING)

    view = await job.cancel()
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    with pytest.raises(JobFailed) as canceled:
        await job.result(timeout=30)
    assert canceled.value.state == "canceled"
    after = await buyer.balance()
    assert (after.available_minor, after.held_minor) == (START, 0)
    assert await world.ledger_balance("acme") == seller0

    handle = world.mp.client("acme").get_workflow_handle(job_workflow_id(job.job_id))

    async def status() -> WorkflowExecutionStatus | None:
        return (await handle.describe()).status

    await until(status, lambda s: s is WorkflowExecutionStatus.TERMINATED)


async def test_a_silent_buyer_pays_when_the_review_window_closes(world: World) -> None:
    """Acceptance 5 (a 2 s review window)."""
    buyer = await world.buyer()
    seller0, platform0 = await world.ledger_balance("acme"), await world.platform()
    job = await buyer.k.hire(world.key("review"), text="y" * 100)  # 1.00 USD, a 0.05 fee
    await job.result()
    view = await world.job(buyer, job.job_id, lambda v: v.escrow is m.Escrow.RELEASED)
    assert view.review_deadline is not None
    assert view.state is m.JobState.COMPLETED
    assert await world.ledger_balance("acme") - seller0 == 95
    assert await world.platform() - platform0 == 5
    after = await buyer.balance()
    assert (after.available_minor, after.held_minor) == (START - 100, 0)


async def test_reject_opens_a_dispute(world: World) -> None:
    buyer = await world.buyer()
    job = await buyer.k.hire(world.key(), text="meh")
    await job.result()
    view = await job.reject("every letter is a capital")
    assert (view.escrow, view.reason) == (m.Escrow.DISPUTED, "every letter is a capital")
    after = await buyer.balance()
    assert (after.available_minor, after.held_minor) == (START - 3, 3)  # held for the resolver


async def test_the_second_ledger_is_a_valid_adapter() -> None:
    """The chain-like configuration passes the seam ④ contract (a short run)."""
    await run_ledger_contract(_pending_usd, interleavings=200, audit_every=50)


async def _pending_usd() -> PendingLedger:
    return PendingLedger(fake_ledger(currencies=["USD"]), latency=0)
