"""Delivery provenance (decisions I15) against a real dev server (CLI 1.9.1, server 1.32).

A run of ``job-<id>`` counts for escrow only if the buyer's own ``execute`` call started
it, which :meth:`Control.observe` establishes by looking the call up in the buyer's
namespace. Here: the buyer's two doors (a standalone operation, a buyer workflow) verify;
runs the seller starts itself -- before any call, or dressed in another call's callbacks,
links and request id -- do not; and once escrow has pinned the buyer's run, terminating it
and completing a new run of the same id does not move escrow."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

import pytest
from temporalio.api.common.v1 import (
    Callback,
    Link,
    Payload,
    Payloads,
    WorkflowExecution,
    WorkflowType,
)
from temporalio.api.enums.v1 import EventType, WorkflowIdReusePolicy
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    DescribeNexusOperationExecutionRequest,
    DescribeWorkflowExecutionRequest,
    GetWorkflowExecutionHistoryRequest,
    StartWorkflowExecutionRequest,
)
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.worker import Worker

from kontra import _control as control_module
from kontra import escrow
from kontra._control import (
    EXECUTE_OPERATION,
    HOLD_HEADER,
    BuyerCall,
    Control,
    ObservationIncomplete,
    hold_digest,
)
from kontra.contract.names import new_endpoint_name
from kontra.escrow import NOT_FROM_BUYER_REASON, EscrowError, JobRecord, ObservedStatus, Provenance
from kontra.gate_client.models import Escrow, JobState
from kontra.ledger import Money
from kontra.testing.devserver import DevServer

from .flows import BuyerFlow, Hire, Job, hang, step
from .handlers import make_handler

pytestmark = pytest.mark.temporal

_STARTED = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def job_id() -> str:
    return f"job-{uuid.uuid4().hex}"


def plan_payload(text: str) -> Payload:
    return Payload(metadata={"encoding": b"json/plain"}, data=json.dumps({"text": text}).encode())


@dataclass
class Market:
    """A seller (namespace, worker, endpoint) and a buyer (namespace, worker) on one
    server, and the control plane the gate would hold."""

    control: Control
    seller_ns: str
    task_queue: str
    service: str
    endpoint: str
    seller: Client
    buyer_ns: str
    buyer: Client
    buyer_queue: str

    def call(self, *tokens: str) -> BuyerCall:
        """The gate's record of a job whose hold tokens are ``tokens``."""
        return BuyerCall(
            namespace=self.buyer_ns,
            endpoint=self.endpoint,
            service=self.service,
            holds=frozenset(hold_digest(t) for t in tokens),
        )

    async def script_hire(self, token: str, text: str = "ok") -> str:
        """The script door: a standalone ``execute`` operation in the buyer's namespace (as
        the gate's ``start`` route makes it). Returns the job's workflow id."""
        wf = job_id()
        await self.control.start_operation(
            self.buyer_ns,
            unique("op"),
            self.endpoint,
            self.service,
            EXECUTE_OPERATION,
            plan_payload(text),
            {"x-workflow-id": wf, HOLD_HEADER: token},
            timedelta(minutes=5),
        )
        return wf

    async def workflow_hire(
        self, token: str, text: str = "ok", hold_header: str = HOLD_HEADER
    ) -> str:
        """The workflow door: a buyer workflow calls ``execute``; returns once it has the
        result. Returns the job's workflow id."""
        wf = job_id()
        out = await self.buyer.execute_workflow(
            BuyerFlow.run,
            Hire(self.endpoint, self.service, wf, token, text, hold_header),
            id=unique("buyer-flow"),
            task_queue=self.buyer_queue,
        )
        assert out == text.upper()
        return wf

    async def wait(self, workflow_id: str, *want: WorkflowExecutionStatus) -> str:
        """The run id of ``workflow_id``'s latest run once it has one of ``want``."""
        for _ in range(300):
            try:
                desc = await self.seller.get_workflow_handle(workflow_id).describe()
            except Exception:
                desc = None
            if desc is not None and desc.status in want:
                return desc.run_id
            await asyncio.sleep(0.05)
        raise AssertionError(f"{workflow_id} never reached {want}")

    async def started(self, workflow_id: str, run_id: str) -> HistoryEvent:
        resp = await self.seller.workflow_service.get_workflow_execution_history(
            GetWorkflowExecutionHistoryRequest(
                namespace=self.seller_ns,
                execution=WorkflowExecution(workflow_id=workflow_id, run_id=run_id),
                maximum_page_size=1,
            )
        )
        return resp.history.events[0]

    async def start_request_ids(self, workflow_id: str, run_id: str) -> list[str]:
        d = await self.seller.workflow_service.describe_workflow_execution(
            DescribeWorkflowExecutionRequest(
                namespace=self.seller_ns,
                execution=WorkflowExecution(workflow_id=workflow_id, run_id=run_id),
            )
        )
        infos = d.workflow_extended_info.request_id_infos
        return [rid for rid, info in infos.items() if info.event_type == _STARTED]

    async def seller_start(
        self,
        workflow_id: str,
        text: str,
        *,
        request_id: str,
        callbacks: Sequence[Callback] = (),
        links: Sequence[Link] = (),
    ) -> str:
        """What a seller can do with its own worker token: StartWorkflowExecution with any
        request id, completion callbacks and links. Returns the new run id."""
        req = StartWorkflowExecutionRequest(
            namespace=self.seller_ns,
            workflow_id=workflow_id,
            workflow_type=WorkflowType(name="Job"),
            task_queue=TaskQueue(name=self.task_queue),
            input=Payloads(payloads=[plan_payload(text)]),
            identity="the seller itself",
            request_id=request_id,
            workflow_id_reuse_policy=WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
        )
        req.completion_callbacks.extend(callbacks)
        req.links.extend(links)
        resp = await self.seller.workflow_service.start_workflow_execution(req)
        return resp.run_id


@pytest.fixture
async def market(dev_server: DevServer, control: Control) -> AsyncIterator[Market]:
    seller_ns, buyer_ns = unique("seller"), unique("buyer")
    await control.ensure_namespace(seller_ns)
    await control.ensure_namespace(buyer_ns)
    endpoint = new_endpoint_name()
    task_queue = f"{seller_ns}/svc@1.0.0"
    await control.create_endpoint(endpoint, seller_ns, task_queue)
    m = Market(
        control=control,
        seller_ns=seller_ns,
        task_queue=task_queue,
        service=f"{seller_ns}.svc",
        endpoint=endpoint,
        seller=dev_server.client_for(seller_ns),
        buyer_ns=buyer_ns,
        buyer=dev_server.client_for(buyer_ns),
        buyer_queue="buyer-flows",
    )
    async with (
        Worker(
            m.seller,
            task_queue=task_queue,
            workflows=[Job],
            activities=[step, hang],
            nexus_service_handlers=[make_handler(m.service)],
        ),
        Worker(m.buyer, task_queue=m.buyer_queue, workflows=[BuyerFlow]),
    ):
        yield m


def a_job(now: datetime) -> JobRecord:
    """An escrow record as the gate makes it at hire (max_duration 1h)."""
    return escrow.new_job(
        job_id=uuid.uuid4().hex,
        offering="acme/svc@1.0.0",
        buyer="bob",
        seller="acme",
        seller_ns="acme",
        amount=Money(1000, "USD"),
        max_duration=timedelta(hours=1),
        review_window=timedelta(hours=1),
        now=now,
    )


def stored_text(result: bytes | None) -> object:
    assert result is not None
    p = Payload()
    p.ParseFromString(result)
    return json.loads(p.data)


# --------------------------------------------------------------------------- the buyer's calls


async def test_the_buyers_script_call_is_verified_and_delivers(market: Market) -> None:
    token = unique("hold")
    wf = await market.script_hire(token)
    run = await market.wait(wf, WorkflowExecutionStatus.COMPLETED)
    call = market.call(token)

    obs, result = await market.control.observe(market.seller_ns, wf, call, None)
    assert (obs.status, obs.provenance, obs.run_id) == (
        ObservedStatus.COMPLETED,
        Provenance.VERIFIED,
        run,
    )
    assert stored_text(result) == "OK"
    assert result is not None
    assert obs.result_size == len(result)
    # Pinned, the same chain is followed and the same bytes come back.
    assert await market.control.observe(market.seller_ns, wf, call, run) == (obs, result)

    now = datetime.now(UTC)
    job, effects = escrow.observe(a_job(now), obs, now)
    assert (job.state, job.escrow, job.run_id) == (JobState.COMPLETED, Escrow.HELD, run)
    assert job.delivery_sha256 == obs.result_sha256
    assert effects == []


async def test_a_call_for_another_job_endpoint_service_or_buyer_does_not_count(
    market: Market,
) -> None:
    token = unique("hold")
    wf = await market.script_hire(token)
    await market.wait(wf, WorkflowExecutionStatus.COMPLETED)
    genuine = market.call(token)
    for call in (
        market.call(unique("another-jobs-hold")),  # the hold binds the call to the job
        replace(genuine, endpoint=new_endpoint_name()),
        replace(genuine, service="other.svc"),
        replace(genuine, namespace=unique("another-buyer")),
    ):
        obs, result = await market.control.observe(market.seller_ns, wf, call, None)
        assert obs.status is ObservedStatus.COMPLETED
        assert obs.provenance is Provenance.NOT_FROM_BUYER, call
        assert result is None

    obs, _ = await market.control.observe(market.seller_ns, wf, market.call("nope"), None)
    now = datetime.now(UTC)
    job, effects = escrow.observe(a_job(now), obs, now)
    assert (job.state, job.run_id, effects) == (JobState.SUBMITTED, None, [])
    late = job.deadline
    job, effects = escrow.observe(job, obs, late)
    assert (job.state, job.escrow, job.reason) == (
        JobState.FAILED,
        Escrow.REFUNDED,
        NOT_FROM_BUYER_REASON,
    )
    assert [e.kind for e in effects] == [escrow.EffectKind.REFUND]


async def test_the_buyers_workflow_call_is_verified(market: Market) -> None:
    token = unique("hold")
    wf = await market.workflow_hire(token)
    run = await market.wait(wf, WorkflowExecutionStatus.COMPLETED)
    obs, result = await market.control.observe(market.seller_ns, wf, market.call(token), None)
    assert (obs.status, obs.provenance, obs.run_id) == (
        ObservedStatus.COMPLETED,
        Provenance.VERIFIED,
        run,
    )
    assert stored_text(result) == "OK"
    obs, result = await market.control.observe(
        market.seller_ns, wf, market.call(unique("another-jobs-hold")), None
    )
    assert (obs.provenance, result) == (Provenance.NOT_FROM_BUYER, None)


async def test_the_hold_header_counts_however_the_buyer_spells_it(market: Market) -> None:
    """Header names reach the seller's handler lowercased: a buyer that writes
    ``Kontra-Hold`` has its job run, so escrow must not refuse the delivery for it. The
    Python SDK records the name lowercased; a record that kept the buyer's spelling is
    ``test_hold_header_names_are_matched_in_any_case``."""
    token = unique("hold")
    wf = await market.workflow_hire(token, hold_header="Kontra-Hold")
    await market.wait(wf, WorkflowExecutionStatus.COMPLETED)
    obs, _ = await market.control.observe(market.seller_ns, wf, market.call(token), None)
    assert obs.provenance is Provenance.VERIFIED


async def test_a_continue_as_new_chain_is_followed_from_its_first_run(market: Market) -> None:
    token = unique("hold")
    wf = await market.script_hire(token, "can2")
    last = await market.wait(wf, WorkflowExecutionStatus.COMPLETED)
    first = (await market.seller.get_workflow_handle(wf).describe()).raw_description
    root = first.workflow_execution_info.first_run_id
    assert root != last
    call = market.call(token)
    obs, result = await market.control.observe(market.seller_ns, wf, call, None)
    assert (obs.status, obs.provenance, obs.run_id) == (
        ObservedStatus.COMPLETED,
        Provenance.VERIFIED,
        root,
    )
    assert stored_text(result) == "CAN0"
    # Pinned to the first run, the chain is walked forward to the same last run.
    assert await market.control.observe(market.seller_ns, wf, call, root) == (obs, result)


async def test_a_long_chains_delivery_is_seen_by_a_gate_that_never_walked_it(
    market: Market, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A seller's workflow may continue-as-new hundreds of times. A gate replica that has
    not followed the chain (a restart, another replica, an evicted cache) must still see
    the delivery at once: had it reported the job as running, or the run as unverified, a
    buyer that already had the result could cancel and be refunded."""
    monkeypatch.setattr(control_module, "CHAIN_READS", 2)  # stands in for a chain of hundreds
    token = unique("hold")
    wf = await market.script_hire(token, "can4")
    await market.wait(wf, WorkflowExecutionStatus.COMPLETED)
    desc = (await market.seller.get_workflow_handle(wf).describe()).raw_description
    root = desc.workflow_execution_info.first_run_id
    call = market.call(token)
    for pinned in (None, root):
        fresh = Control(market.control.client, market.control.http_base)
        obs, result = await fresh.observe(market.seller_ns, wf, call, pinned)
        assert (obs.status, obs.provenance, obs.run_id) == (
            ObservedStatus.COMPLETED,
            Provenance.VERIFIED,
            root,
        ), pinned
        assert stored_text(result) == "CAN0"

    now = datetime.now(UTC)
    working = replace(a_job(now), state=JobState.WORKING, run_id=root, started_at=now)
    job, _ = escrow.observe(working, obs, now)
    assert job.state is JobState.COMPLETED
    with pytest.raises(EscrowError):  # the buyer's cancel comes too late
        escrow.cancel(job, job.buyer, now)


async def test_a_pinned_chain_left_behind_is_followed_over_several_observations(
    market: Market, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the latest run of the id is not the pinned chain's (the seller started another
    after the chain closed), the chain is walked forward a budget at a time; until its end
    is reached the observation says it cannot tell, never "still running"."""
    monkeypatch.setattr(control_module, "CHAIN_READS", 2)
    token = unique("hold")
    wf = await market.script_hire(token, "can4")
    await market.wait(wf, WorkflowExecutionStatus.COMPLETED)
    desc = (await market.seller.get_workflow_handle(wf).describe()).raw_description
    root = desc.workflow_execution_info.first_run_id
    await market.seller_start(wf, "slow", request_id=str(uuid.uuid4()))
    fresh = Control(market.control.client, market.control.http_base)
    incomplete = 0
    for _ in range(10):
        try:
            obs, result = await fresh.observe(market.seller_ns, wf, market.call(token), root)
        except ObservationIncomplete:
            incomplete += 1
            continue
        break
    else:
        raise AssertionError("the chain's end was never reached")
    assert incomplete >= 1
    assert (obs.status, obs.provenance, obs.run_id) == (
        ObservedStatus.COMPLETED,
        Provenance.VERIFIED,
        root,
    )
    assert stored_text(result) == "CAN0"


# --------------------------------------------------------------------------- the seller's own runs


async def test_a_run_the_seller_started_before_any_call_is_not_the_buyers(
    market: Market,
) -> None:
    """The seller learns ``job-<id>`` and starts it first, with callbacks and links that
    point into the buyer's namespace at what the buyer's call will look like."""
    token = unique("hold")
    wf, op = job_id(), unique("op")
    forged = [
        Link(
            nexus_operation=Link.NexusOperation(
                namespace=market.buyer_ns, operation_id=op, run_id=str(uuid.uuid4())
            )
        ),
        Link(
            workflow_event=Link.WorkflowEvent(
                namespace=market.buyer_ns,
                workflow_id=unique("buyer-flow"),
                run_id=str(uuid.uuid4()),
                event_ref=Link.WorkflowEvent.EventReference(
                    event_id=5, event_type=EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
                ),
            )
        ),
    ]
    callback = Callback(
        nexus=Callback.Nexus(url="temporal://system", header={"temporal-callback-token": "x"}),
        links=forged,
    )
    await market.seller_start(
        wf, "ok", request_id=str(uuid.uuid4()), callbacks=[callback], links=forged
    )
    await market.wait(wf, WorkflowExecutionStatus.COMPLETED)
    call = market.call(token)
    obs, result = await market.control.observe(market.seller_ns, wf, call, None)
    assert obs.status is ObservedStatus.COMPLETED
    assert obs.provenance is Provenance.UNVERIFIED  # nothing in the buyer's namespace
    assert result is None

    # The buyer's real call comes now; the seller's handler refuses a second run of the id.
    await market.control.start_operation(
        market.buyer_ns,
        op,
        market.endpoint,
        market.service,
        EXECUTE_OPERATION,
        plan_payload("ok"),
        {"x-workflow-id": wf, HOLD_HEADER: token},
        timedelta(seconds=10),  # refused by the handler; no need to keep retrying long
    )
    await asyncio.sleep(1)
    obs, result = await market.control.observe(market.seller_ns, wf, call, None)
    assert obs.provenance is not Provenance.VERIFIED
    now = datetime.now(UTC)
    job, _effects = escrow.observe(a_job(now), obs, now + timedelta(hours=2))
    assert (job.state, job.escrow, job.reason) == (
        JobState.FAILED,
        Escrow.REFUNDED,
        NOT_FROM_BUYER_REASON,
    )


@pytest.mark.parametrize("door", ["script", "workflow"])
async def test_a_run_dressed_in_another_jobs_call_is_not_the_buyers(
    market: Market, door: str
) -> None:
    """The seller copies a genuine call's callbacks, links and request id (all of which
    its handler saw) onto a run of another job of the same buyer."""
    token_a, token_b = unique("hold-a"), unique("hold-b")
    hire = market.script_hire if door == "script" else market.workflow_hire
    wf_a = await hire(token_a)
    run_a = await market.wait(wf_a, WorkflowExecutionStatus.COMPLETED)
    started_a = await market.started(wf_a, run_a)
    callbacks = list(started_a.workflow_execution_started_event_attributes.completion_callbacks)
    assert callbacks
    assert callbacks[0].links
    (request_a,) = await market.start_request_ids(wf_a, run_a)

    wf_b = job_id()
    run_b = await market.seller_start(
        wf_b, "ok", request_id=request_a, callbacks=callbacks, links=list(started_a.links)
    )
    await market.wait(wf_b, WorkflowExecutionStatus.COMPLETED)
    assert await market.start_request_ids(wf_b, run_b) == [request_a]

    obs, result = await market.control.observe(market.seller_ns, wf_b, market.call(token_b), None)
    assert (obs.status, obs.provenance, result) == (
        ObservedStatus.COMPLETED,
        Provenance.NOT_FROM_BUYER,
        None,
    )
    # What made the difference is the hold: were A's token one the gate had issued for B,
    # the call would be B's.
    obs, _ = await market.control.observe(market.seller_ns, wf_b, market.call(token_a), None)
    assert obs.provenance is Provenance.VERIFIED


async def test_a_link_with_malformed_ids_names_nothing_and_never_blocks_a_cancel(
    market: Market,
) -> None:
    """Links the seller writes may carry ids the frontend refuses as malformed. Had the
    observation failed on them, every observation of the job would fail, and a gate that
    cannot observe refuses the buyer's cancel: the seller would hold the buyer's money
    until the deadline. They name nothing; the job stays cancelable."""
    scheduled = Link.WorkflowEvent.EventReference(
        event_id=5, event_type=EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
    )
    malformed = [
        Link(
            workflow_event=Link.WorkflowEvent(
                namespace=market.buyer_ns,
                workflow_id=unique("buyer-flow"),
                run_id="not-a-run-id",
                event_ref=scheduled,
            )
        ),
        Link(
            nexus_operation=Link.NexusOperation(
                namespace=market.buyer_ns, operation_id=unique("op"), run_id="not-a-run-id"
            )
        ),
    ]
    for link in malformed:
        wf = job_id()
        callback = Callback(
            nexus=Callback.Nexus(url="temporal://system", header={"temporal-callback-token": "x"}),
            links=[link],
        )
        await market.seller_start(wf, "slow", request_id=str(uuid.uuid4()), callbacks=[callback])
        obs, result = await market.control.observe(
            market.seller_ns, wf, market.call(unique("hold")), None
        )
        assert (obs.status, obs.provenance, result) == (
            ObservedStatus.RUNNING,
            Provenance.NOT_FROM_BUYER,
            None,
        )
        now = datetime.now(UTC)
        job, _ = escrow.observe(a_job(now), obs, now)
        job, effects = escrow.cancel(job, job.buyer, now)
        assert (job.state, job.escrow) == (JobState.CANCELED, Escrow.REFUNDED)
        assert escrow.EffectKind.REFUND in [e.kind for e in effects]
        await market.seller.get_workflow_handle(wf).terminate("done")


async def test_a_link_by_request_id_is_not_followed_into_the_buyers_history(
    market: Market, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A caller's link names its scheduled event by id. A seller-written link "by request
    id" would have the gate scan a buyer's whole history for a string the seller chose,
    once per run the seller starts: it is refused without reading anything."""
    token = unique("hold")
    wf_a = await market.workflow_hire(token)
    run_a = await market.wait(wf_a, WorkflowExecutionStatus.COMPLETED)
    started_a = await market.started(wf_a, run_a)
    (request_a,) = await market.start_request_ids(wf_a, run_a)
    [callback] = started_a.workflow_execution_started_event_attributes.completion_callbacks
    [genuine] = callback.links
    by_request = Link(
        workflow_event=Link.WorkflowEvent(
            namespace=genuine.workflow_event.namespace,
            workflow_id=genuine.workflow_event.workflow_id,
            run_id=genuine.workflow_event.run_id,
            request_id_ref=Link.WorkflowEvent.RequestIdReference(
                request_id=request_a, event_type=EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
            ),
        )
    )
    forged = Callback()
    forged.CopyFrom(callback)
    del forged.links[:]
    forged.links.append(by_request)
    wf_b = job_id()
    await market.seller_start(wf_b, "ok", request_id=request_a, callbacks=[forged])
    await market.wait(wf_b, WorkflowExecutionStatus.COMPLETED)

    reads: list[str] = []
    pages = Control._pages  # pyright: ignore[reportPrivateUsage]

    def counting(
        self: Control, namespace: str, workflow_id: str, run_id: str, **kw: int
    ) -> AsyncIterator[Sequence[HistoryEvent]]:
        reads.append(namespace)
        return pages(self, namespace, workflow_id, run_id, **kw)

    monkeypatch.setattr(Control, "_pages", counting)
    obs, result = await market.control.observe(market.seller_ns, wf_b, market.call(token), None)
    assert (obs.provenance, result) == (Provenance.NOT_FROM_BUYER, None)
    assert market.buyer_ns not in reads


async def test_terminate_and_restart_does_not_move_escrow(market: Market) -> None:
    token = unique("hold")
    wf = await market.script_hire(token, "slow")
    first = await market.wait(wf, WorkflowExecutionStatus.RUNNING)
    call = market.call(token)
    obs, _ = await market.control.observe(market.seller_ns, wf, call, None)
    assert (obs.status, obs.provenance, obs.run_id) == (
        ObservedStatus.RUNNING,
        Provenance.VERIFIED,
        first,
    )
    now = datetime.now(UTC)
    job, _ = escrow.observe(a_job(now), obs, now)
    assert (job.state, job.run_id) == (JobState.WORKING, first)

    # The seller terminates the buyer's run and completes a new one of the same id with
    # everything the buyer's call gave it. The same request id is deduplicated while the
    # buyer's run is the latest, so a run with another id goes in between.
    started = await market.started(wf, first)
    callbacks = list(started.workflow_execution_started_event_attributes.completion_callbacks)
    (request_id,) = await market.start_request_ids(wf, first)
    await market.seller.get_workflow_handle(wf).terminate("the seller")
    await market.seller_start(wf, "slow", request_id=str(uuid.uuid4()))
    await market.seller.get_workflow_handle(wf).terminate("the seller")
    second = await market.seller_start(wf, "ok", request_id=request_id, callbacks=callbacks)
    assert second != first
    await market.wait(wf, WorkflowExecutionStatus.COMPLETED)
    assert await market.start_request_ids(wf, second) == [request_id]

    # Escrow follows the run it pinned: terminated, so the job fails and refunds.
    obs, result = await market.control.observe(market.seller_ns, wf, call, job.run_id)
    assert (obs.status, obs.run_id, result) == (ObservedStatus.TERMINATED, first, None)
    job, effects = escrow.observe(job, obs, datetime.now(UTC))
    assert (job.state, job.escrow, job.reason) == (
        JobState.FAILED,
        Escrow.REFUNDED,
        "seller workflow terminated",
    )
    assert [e.kind for e in effects] == [escrow.EffectKind.REFUND]

    # Unpinned (the gate never saw the first run), the new run is still not the buyer's:
    # the buyer's operation recorded the first run as the one its call started.
    obs, result = await market.control.observe(market.seller_ns, wf, call, None)
    assert (obs.status, obs.run_id, obs.provenance, result) == (
        ObservedStatus.COMPLETED,
        second,
        Provenance.NOT_FROM_BUYER,
        None,
    )
    fresh = a_job(now)
    assert escrow.observe(fresh, obs, now) == (fresh, [])
    job, _ = escrow.observe(fresh, obs, fresh.deadline)
    assert (job.escrow, job.reason) == (Escrow.REFUNDED, NOT_FROM_BUYER_REASON)


async def test_a_pinned_chain_ignores_every_other_run_of_the_id(market: Market) -> None:
    """Pinned, the observation is of the pinned chain only: a later run the seller starts
    and completes is never read, let alone delivered."""
    token = unique("hold")
    wf = await market.script_hire(token)
    first = await market.wait(wf, WorkflowExecutionStatus.COMPLETED)
    later = await market.seller_start(wf, "later", request_id=str(uuid.uuid4()))
    assert later != first
    await market.wait(wf, WorkflowExecutionStatus.COMPLETED)
    obs, result = await market.control.observe(market.seller_ns, wf, market.call(token), first)
    assert (obs.status, obs.run_id, obs.provenance) == (
        ObservedStatus.COMPLETED,
        first,
        Provenance.VERIFIED,
    )
    assert stored_text(result) == "OK"


async def test_the_gates_starts_carry_request_ids_no_one_can_predict(market: Market) -> None:
    """The request id is what a run proves its call by. The handler is sent the one the
    server generated for the operation, not the starter's: a seller cannot predict it and
    pre-start a run that would verify."""
    ids: list[str] = []
    for _ in range(2):
        op = unique("op")
        await market.control.start_operation(
            market.buyer_ns,
            op,
            market.endpoint,
            market.service,
            EXECUTE_OPERATION,
            plan_payload("ok"),
            {"x-workflow-id": job_id(), HOLD_HEADER: "t"},
            timedelta(minutes=5),
        )
        d = await market.buyer.workflow_service.describe_nexus_operation_execution(
            DescribeNexusOperationExecutionRequest(namespace=market.buyer_ns, operation_id=op)
        )
        ids.append(d.info.request_id)
    assert len(set(ids)) == 2
    assert all(uuid.UUID(i).version == 4 for i in ids)


def test_hold_header_names_are_matched_in_any_case() -> None:
    """Another SDK may record the buyer's own spelling; the handler sees it lowercased,
    so every spelling counts, and only the value (by digest) decides."""
    from kontra._control import _carries_hold  # pyright: ignore[reportPrivateUsage]

    holds = frozenset({hold_digest("tok")})
    assert _carries_hold({"Kontra-Hold": "tok"}, holds)
    assert _carries_hold({"KONTRA-HOLD": "other", "kontra-hold": "tok"}, holds)
    assert not _carries_hold({"kontra-hold": "other"}, holds)
    assert not _carries_hold({"kontra-hold-2": "tok", "x-kontra-hold": "tok"}, holds)
    assert not _carries_hold({}, holds)
    # Only ASCII folds: str.lower() maps the Kelvin sign to "k", but no handler is ever
    # handed a header under a name HTTP refuses.
    assert "\u212aontra-hold".lower() == HOLD_HEADER
    assert not _carries_hold({"\u212aontra-hold": "tok"}, holds)
