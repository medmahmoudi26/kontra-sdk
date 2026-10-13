"""The workflow door (PRD §7.2): ``service(...).execute()`` inside a buyer's workflow, run on
``kontra.hire.worker``, with the same semantics as the script door -- including the recon
example, which composes two offerings and refunds the second when the first fails
(acceptance 11)."""

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from recon import Recon
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError, WorkflowHandle
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Replayer

from kontra.contract.names import job_workflow_id
from kontra.gate_client import models as m
from kontra.hire import HireActivities, HireWorker, worker
from kontra.hire._service import PrepareRequest
from kontra.testing.devserver import DevServer

from . import sellers
from .buyer_flows import HireCatching, HireOne, Order
from .flows import FAIL_MESSAGE
from .sandboxed_flows import HireSandboxed
from .support import Buyer, World, open_world, until

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]

START = 10_000


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def world(
    dev_server: DevServer, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[World]:
    async with open_world(dev_server, tmp_path_factory.mktemp("acme")) as w:
        recon = sellers.recon_worker(tmp_path_factory.mktemp("recon"))
        w.bound["probe"], w.bound["enrich"] = recon.offerings
        async with w.mp.serve(recon, w.seller):
            yield w


@dataclass
class Door:
    world: World
    buyer: Buyer
    worker: HireWorker
    task_queue: str

    async def start(self, wf: Any, arg: Any) -> WorkflowHandle[Any, Any]:
        return await self.worker.client.start_workflow(
            wf, arg, id=f"buyer-wf-{uuid.uuid4().hex[:8]}", task_queue=self.task_queue
        )

    async def run(self, wf: Any, arg: Any) -> Any:
        return await (await self.start(wf, arg)).result()

    async def jobs(self, count: int, ok: Any = lambda v: True) -> list[m.JobView]:
        return await until(
            self.buyer.k.jobs, lambda views: len(views) == count and all(ok(v) for v in views)
        )

    async def refunded(self) -> None:
        bal = await self.buyer.balance()
        assert (bal.available_minor, bal.held_minor) == (START, 0)


@pytest_asyncio.fixture(loop_scope="module")
async def door(world: World) -> AsyncIterator[Door]:
    buyer = await world.buyer()
    task_queue = f"buyer-{uuid.uuid4().hex[:8]}"
    w = await worker(
        world.mp.client(buyer.party.namespace),
        task_queue=task_queue,
        workflows=[HireOne, HireCatching, HireSandboxed, Recon],
        kontra=buyer.k,
    )
    async with w:
        yield Door(world, buyer, w, task_queue)


async def _seller_status(door: Door, job_id: str) -> WorkflowExecutionStatus | None:
    """The seller's handler workflow: Nexus cancellation reached it (not the gate's sweep)."""
    handle = door.world.mp.client("acme").get_workflow_handle(job_workflow_id(job_id))
    return (await handle.describe()).status


async def _replays(door: Door, handle: WorkflowHandle[Any, Any]) -> None:
    """The buyer workflow's recorded history replays without a nondeterminism error: the
    command sequence ``service().execute`` produced is the one the code produces again."""
    history = await handle.fetch_history()
    replayer = Replayer(
        workflows=[HireOne, HireCatching, HireSandboxed, Recon],
        data_converter=door.buyer.k.data_converter,
    )
    await replayer.replay_workflow(history)


def _cause(err: WorkflowFailureError) -> ApplicationError:
    assert isinstance(err.cause, ApplicationError), err.cause
    return err.cause


async def test_a_workflow_hires_and_reads_a_record(door: Door) -> None:
    out = await door.run(HireOne.run, Order(offering=door.world.key(), text="from a workflow"))
    assert (out["text"], out["chars"], out["second_part"]) == ("FROM A WORKFLOW", 15, 1)
    assert out["record"]["summary"] == {"chars": 15, "beats": 0}
    [view] = await door.jobs(1, lambda v: v.state is m.JobState.COMPLETED)
    assert (view.escrow, view.amount_minor) == (m.Escrow.HELD, 15)


async def test_service_works_without_a_sandbox_pass_through(door: Door) -> None:
    out = await door.run(HireSandboxed.run, door.world.key())
    assert out["text"] == "SANDBOXED"


async def test_hire_errors_reach_workflow_code_as_their_classes(door: Door) -> None:
    key = door.world.key()
    rejected = await door.run(HireCatching.run, Order(offering=key, text="reject me"))
    assert rejected["error"] == "QuoteRejected"
    assert rejected["fields"]["reason"] == "this text is not for sale"
    above = await door.run(
        HireCatching.run, Order(offering=key, text="far too long", max_price="0.05")
    )
    assert above["error"] == "PriceAboveMax"
    assert above["fields"]["max_price"] == "0.05"
    busy = await door.run(HireCatching.run, Order(offering=key, text="busy"))
    assert (busy["error"], busy["fields"]["eta_seconds"]) == ("SellerBusy", 30)
    invalid = await door.run(HireCatching.run, Order(offering=key, text="x", mode="sideways"))
    assert invalid["error"] == "InvalidInput"
    failed = await door.run(HireCatching.run, Order(offering=key, text="x", mode="fail"))
    assert (failed["error"], failed["message"]) == ("JobFailed", FAIL_MESSAGE)
    assert failed["fields"]["state"] == "failed"

    with pytest.raises(WorkflowFailureError) as uncaught:
        await door.run(HireOne.run, Order(offering=key, text="reject me"))
    assert _cause(uncaught.value).type == "kontra.QuoteRejected"
    await door.jobs(1, lambda v: v.escrow is m.Escrow.REFUNDED)  # only the failed one locked
    await door.refunded()


async def test_a_refused_start_refunds_at_once(door: Door) -> None:
    text = f"dearer-{uuid.uuid4().hex[:6]}"  # the handler's re-quote is above the hold
    handle = await door.start(HireCatching.run, Order(offering=door.world.key(), text=text))
    out = await handle.result()
    assert out["error"] == "JobFailed"
    assert "the seller refused to start the job" in out["message"]
    assert out["fields"]["state"] == "canceled"
    [view] = await door.jobs(1)
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    await door.refunded()
    await _replays(door, handle)


async def test_a_run_the_seller_started_itself_refunds_the_workflow(door: Door) -> None:
    """The seller completes ``job-<id>`` itself, so the workflow's own start is refused
    ("already ran"). That run is no delivery: only the run the buyer's own call started is
    (decisions I15). The hold refunds at once, the seller is paid nothing, and the workflow
    gets JobFailed, as a script would."""
    world = door.world
    seller_before = await world.ledger_balance(world.seller.handle)
    world.forge_on_hire(door.buyer.k, "forged by the seller")
    handle = await door.start(HireCatching.run, Order(offering=world.key(), text="genuine"))
    out = await handle.result()
    assert out["error"] == "JobFailed"
    assert "already ran" in out["message"]
    assert out["fields"]["state"] == "canceled"
    [view] = await door.jobs(1)
    assert (view.state, view.escrow, view.delivery) == (
        m.JobState.CANCELED,
        m.Escrow.REFUNDED,
        None,
    )
    await door.refunded()
    assert await world.ledger_balance(world.seller.handle) == seller_before
    await _replays(door, handle)


async def test_a_delivery_after_the_operation_timed_out_reaches_the_workflow(
    door: Door, tmp_path: Path
) -> None:
    """What the escrow-decides rule guards, in a workflow: the run the workflow's own call
    started delivers after the operation timed out (``max_duration`` from the call) but
    before the gate's deadline (lock + max_duration + grace). The gate counts that delivery
    and pays it at the review window, so the workflow gets it, not JobFailed."""
    world = door.world
    bound = sellers.text_offering(tmp_path, max_duration="8s")
    grace = await world.gate(lambda g: g.grace)
    await world.gate(lambda g: setattr(g, "grace", timedelta(seconds=4)))
    try:
        async with world.mp.serve(sellers.text_worker(bound), world.seller):
            # The handler takes 3 s to start the job (the operation's 8 s run from before
            # that); the job then works 6 s: done ~9 s after the call, before 8 + 4.
            text = f"late-{uuid.uuid4().hex[:6]}"
            handle = await door.start(
                HireOne.run, Order(offering=bound.key, text=text, mode="quiet", beats=6)
            )
            out = await handle.result()
    finally:
        await world.gate(lambda g: setattr(g, "grace", grace))
    assert out["text"] == text.upper()
    [view] = await door.jobs(1)
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.HELD)
    history = await handle.fetch_history()
    timed_out = [
        e for e in history.events if e.HasField("nexus_operation_timed_out_event_attributes")
    ]
    assert len(timed_out) == 1  # the operation itself returned nothing
    await _replays(door, handle)


async def test_a_retried_prepare_cancels_the_hold_an_earlier_attempt_locked(door: Door) -> None:
    """An attempt of ``kontra.hire.prepare`` that locked a hold and then died (a worker
    crash, a timeout) leaves a hold whose token is lost; the retry finds it through the
    quote id the attempt heartbeated, and cancels it rather than leave two holds."""
    k, key = door.buyer.k, door.world.key()
    order = PrepareRequest(offering=key, input={"text": "twice"})
    env = ActivityEnvironment()
    beats: list[Any] = []
    env.on_heartbeat = lambda *details: beats.append(details[0])
    first = await env.run(HireActivities(k).prepare, order)  # attempt 1: the hold it locks
    [quoted] = beats
    assert first.job_id not in str(quoted)  # the heartbeat names the quote, not the job

    env.info = dataclasses.replace(env.info, attempt=2, heartbeat_details=[quoted])
    second = await env.run(HireActivities(k).prepare, order)
    orphan = await k.gate.job(first.job_id)
    assert (orphan.state, orphan.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    bal = await door.buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START - 5, 5)  # one hold: "twice"
    await k.gate.cancel(second.job_id)

    # A heartbeat whose quote was never hired cancels nothing (the probe cannot lock).
    q = await k.quote(key, text="unhired")
    env.info = dataclasses.replace(env.info, heartbeat_details=[{"quote_id": q.quote_id}])
    third = await env.run(HireActivities(k).prepare, order)
    assert (await k.gate.job(third.job_id)).escrow is m.Escrow.HELD
    assert (await q.hire()).job_id != third.job_id  # the probed quote is still usable
    await door.jobs(4)


async def test_cancelling_the_workflow_cancels_a_running_job(door: Door) -> None:
    handle = await door.start(
        HireOne.run, Order(offering=door.world.key(), text="zzz", mode="slow")
    )
    await door.jobs(1, lambda v: v.state is m.JobState.WORKING)
    await handle.cancel()
    with pytest.raises(WorkflowFailureError) as stopped:
        await handle.result()
    assert isinstance(stopped.value.cause, CancelledError)
    assert (await handle.describe()).status is WorkflowExecutionStatus.CANCELED
    [view] = await door.jobs(1, lambda v: v.escrow is m.Escrow.REFUNDED)
    assert view.state is m.JobState.CANCELED
    assert await _seller_status(door, view.job_id) is WorkflowExecutionStatus.TERMINATED
    await door.refunded()


async def test_cancelling_the_workflow_refunds_even_if_the_seller_answers_with_a_result(
    door: Door,
) -> None:
    """A seller workflow may meet cancellation by completing with what it has. Nexus
    cancellation alone would then end in a delivery the gate counts and releases: the
    workflow door cancels at the gate first, as ``Job.cancel()`` does, so the hold refunds."""
    handle = await door.start(
        HireOne.run, Order(offering=door.world.key(), text="zzz", mode="graceful")
    )
    await door.jobs(1, lambda v: v.state is m.JobState.WORKING)
    await handle.cancel()
    with pytest.raises(WorkflowFailureError) as stopped:
        await handle.result()
    assert isinstance(stopped.value.cause, CancelledError)
    [view] = await door.jobs(1, lambda v: not v.state.value.startswith(("submitted", "working")))
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    await door.refunded()
    await _replays(door, handle)


async def test_cancelling_before_the_seller_starts_refunds_at_once(door: Door) -> None:
    text = f"stall-{uuid.uuid4().hex[:6]}"  # the seller takes 6 s to start it
    handle = await door.start(HireOne.run, Order(offering=door.world.key(), text=text))
    await door.jobs(1)
    loop = asyncio.get_running_loop()
    await asyncio.sleep(0.5)  # the Nexus start is in flight
    asked = loop.time()
    await handle.cancel()
    with pytest.raises(WorkflowFailureError) as stopped:
        await handle.result()
    assert isinstance(stopped.value.cause, CancelledError)
    [view] = await door.jobs(1, lambda v: v.escrow is m.Escrow.REFUNDED)
    assert view.state is m.JobState.CANCELED
    assert loop.time() - asked < sellers.STALL_SECONDS  # not left to the deadline
    await door.refunded()


async def test_recon_refunds_the_second_job_when_the_first_fails(door: Door) -> None:
    """Acceptance 11: the recon example's workflow, unchanged."""
    handle = await door.start(Recon.run, "fail.example")
    with pytest.raises(WorkflowFailureError) as failed:
        await handle.result()
    cause = _cause(failed.value)
    assert (cause.type, cause.message) == ("kontra.JobFailed", "every probe failed")
    views = await door.jobs(2, lambda v: v.escrow is m.Escrow.REFUNDED)
    states = {v.offering: v.state for v in views}
    [enrich] = [v for v in views if v.offering == door.world.key("enrich")]
    assert await _seller_status(door, enrich.job_id) is WorkflowExecutionStatus.TERMINATED
    world = door.world
    assert states == {
        world.key("probe"): m.JobState.FAILED,
        world.key("enrich"): m.JobState.CANCELED,  # cancelled by Recon, refunded
    }
    await door.refunded()
    await _replays(door, handle)


async def test_recon_composes_two_offerings(door: Door) -> None:
    out = await door.run(Recon.run, "example.com")
    assert out["domain"] == "example.com"
    assert [h["status"] for h in out["heads"]["heads"]] == [200, 200]
    assert out["enrich"] == {"domains": ["example.com", "www.example.com"]}
    bal = await door.buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START - 160, 160)  # 0.10 + 1.50, held


async def test_the_worker_from_the_profile_alone(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``examples/recon/main.py``'s path: ``worker(task_queue=..., workflows=...)`` with
    nothing else reads the profile, connects, and closes its client with the worker."""
    buyer = await world.buyer()
    monkeypatch.setenv("KONTRA_HOME", str(buyer.party.home))
    w = await worker(task_queue=f"buyer-{uuid.uuid4().hex[:8]}", workflows=[HireOne])
    owned = w._owned
    assert owned is not None
    assert owned.party == buyer.party.handle
    async with w:
        out = await w.client.execute_workflow(
            HireOne.run,
            Order(offering=world.key(), text="profile"),
            id=f"buyer-wf-{uuid.uuid4().hex[:8]}",
            task_queue=w.task_queue,
        )
    assert out["text"] == "PROFILE"
    assert w._owned is None  # closed on shutdown
