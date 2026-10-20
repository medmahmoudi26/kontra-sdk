"""Delivery provenance (decisions I15, I16) against a real dev server (CLI 1.9.1, server
1.32).

A run of ``job-<id>`` counts for escrow only if the buyer's own ``execute`` call started
it, which :meth:`Control.observe` establishes by looking the call up in the buyer's
namespace. Here: the buyer's two doors (a standalone operation, a buyer workflow) verify;
runs the seller starts itself -- before any call, or dressed in another call's callbacks,
links and request id -- do not; and once escrow has pinned the buyer's run, terminating it
and completing a new run of the same id does not move escrow. Then decisions I16: a cancel
occupies ``job-<id>`` so a still-valid hold cannot start it, a buyer cancelling its own call
is read from the buyer's records, and a failed read of the buyer's namespace is its own
error."""

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
from temporalio.api.enums.v1 import (
    EventType,
    NexusOperationExecutionStatus,
    WorkflowIdConflictPolicy,
    WorkflowIdReusePolicy,
)
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    DescribeNexusOperationExecutionRequest,
    DescribeWorkflowExecutionRequest,
    GetWorkflowExecutionHistoryRequest,
    RequestCancelNexusOperationExecutionRequest,
    StartWorkflowExecutionRequest,
    TerminateNexusOperationExecutionRequest,
)
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker

from kontra import _control as control_module
from kontra import escrow
from kontra._control import (
    EXECUTE_OPERATION,
    HOLD_HEADER,
    OCCUPY_TASK_QUEUE,
    OCCUPY_WORKFLOW_TYPE,
    BuyerCall,
    BuyerLookupError,
    Control,
    ObservationIncomplete,
    Occupation,
    TemporalError,
    hold_digest,
)
from kontra.contract.names import new_endpoint_name
from kontra.escrow import (
    BUYER_CANCEL_REASON,
    NOT_FROM_BUYER_REASON,
    OCCUPIED_REASON,
    EscrowError,
    JobRecord,
    ObservedStatus,
    Provenance,
)
from kontra.gate_client.models import Escrow, JobState
from kontra.ledger import Money
from kontra.testing.devserver import DevServer

from .flows import BuyerFlow, CancellingBuyerFlow, Hire, Job, StartingBuyerFlow, hang, step
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

    async def script_hire(
        self, token: str, text: str = "ok", *, operation_id: str | None = None
    ) -> str:
        """The script door: a standalone ``execute`` operation in the buyer's namespace (as
        the gate's ``start`` route makes it). Returns the job's workflow id."""
        wf = job_id()
        await self.control.start_operation(
            self.buyer_ns,
            operation_id or unique("op"),
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
        Worker(
            m.buyer,
            task_queue=m.buyer_queue,
            workflows=[BuyerFlow, CancellingBuyerFlow, StartingBuyerFlow],
        ),
    ):
        yield m


def a_job(now: datetime) -> JobRecord:
    """An escrow record as the gate makes it at hire (max_duration 1h)."""
    return escrow.new_job(
        job_id=uuid.uuid4().hex,
        offering="acme/svc@1.0.0",
        buyer="bob",
        seller="acme",
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
    assert (obs.status, obs.provenance, obs.pin) == (
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
    assert (job.state, job.escrow, job.pin) == (JobState.COMPLETED, Escrow.HELD, run)
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
    assert (job.state, job.pin, effects) == (JobState.SUBMITTED, None, [])
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
    assert (obs.status, obs.provenance, obs.pin) == (
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
    assert (obs.status, obs.provenance, obs.pin) == (
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
        assert (obs.status, obs.provenance, obs.pin) == (
            ObservedStatus.COMPLETED,
            Provenance.VERIFIED,
            root,
        ), pinned
        assert stored_text(result) == "CAN0"

    now = datetime.now(UTC)
    working = replace(a_job(now), state=JobState.WORKING, pin=root, started_at=now)
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
    assert (obs.status, obs.provenance, obs.pin) == (
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
    assert (obs.status, obs.provenance, obs.pin) == (
        ObservedStatus.RUNNING,
        Provenance.VERIFIED,
        first,
    )
    now = datetime.now(UTC)
    job, _ = escrow.observe(a_job(now), obs, now)
    assert (job.state, job.pin) == (JobState.WORKING, first)

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
    obs, result = await market.control.observe(market.seller_ns, wf, call, job.pin)
    assert (obs.status, obs.pin, result) == (ObservedStatus.TERMINATED, first, None)
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
    assert (obs.status, obs.pin, obs.provenance, result) == (
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
    assert (obs.status, obs.pin, obs.provenance) == (
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


# --------------------------------------------------------------------------- decisions I16


async def history_of(client: Client, namespace: str, workflow_id: str) -> list[HistoryEvent]:
    resp = await client.workflow_service.get_workflow_execution_history(
        GetWorkflowExecutionHistoryRequest(
            namespace=namespace, execution=WorkflowExecution(workflow_id=workflow_id)
        )
    )
    return list(resp.history.events)


async def handler_start(market: Market, workflow_id: str) -> RPCError:
    """``job-<id>`` started exactly as ``kontra.serve``'s handler starts it (a fresh request
    id, ``REJECT_DUPLICATE`` and ``FAIL``); the refusal it must get."""
    with pytest.raises(RPCError) as refused:
        await market.seller.workflow_service.start_workflow_execution(
            StartWorkflowExecutionRequest(
                namespace=market.seller_ns,
                workflow_id=workflow_id,
                workflow_type=WorkflowType(name="Job"),
                task_queue=TaskQueue(name=market.task_queue),
                input=Payloads(payloads=[plan_payload("ok")]),
                request_id=str(uuid.uuid4()),
                workflow_id_reuse_policy=WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
                workflow_id_conflict_policy=WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
            )
        )
    return refused.value


async def test_occupy_job_closes_the_id_to_the_handlers_start(market: Market) -> None:
    """A cancel before any run: ``job-<id>`` is taken by a placeholder that no worker ever
    runs and that is closed at once, so the handler's own start -- what a buyer's direct
    ``execute`` with its still-valid hold would trigger -- is refused, and the placeholder
    is never a delivery nor a run of the seller's."""
    wf = job_id()
    assert await market.control.occupy_job(market.seller_ns, wf, BUYER_CANCEL_REASON) is (
        Occupation.OCCUPIED
    )
    desc = await market.seller.get_workflow_handle(wf).describe()
    assert desc.status is WorkflowExecutionStatus.TERMINATED
    assert (desc.workflow_type, desc.task_queue) == (OCCUPY_WORKFLOW_TYPE, OCCUPY_TASK_QUEUE)
    assert await desc.memo() == {"kontra-occupied": BUYER_CANCEL_REASON}
    events = await history_of(market.seller, market.seller_ns, wf)
    assert [e.event_type for e in events] == [
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
    ]  # no workflow task was ever scheduled: no worker ran any of it
    assert events[1].workflow_execution_terminated_event_attributes.reason == BUYER_CANCEL_REASON

    refused = await handler_start(market, wf)
    assert refused.status == RPCStatusCode.ALREADY_EXISTS

    call = market.call(unique("hold"))
    obs, result = await market.control.observe(market.seller_ns, wf, call, None)
    assert (obs.status, obs.provenance, result) == (
        ObservedStatus.TERMINATED,
        Provenance.OCCUPIED,
        None,
    )
    now = datetime.now(UTC)
    job = a_job(now)
    assert escrow.observe(job, obs, now) == (job, [])  # not the seller's terminated run
    late, _ = escrow.observe(job, obs, job.deadline)
    assert (late.escrow, late.reason, late.pin) == (Escrow.REFUNDED, OCCUPIED_REASON, None)

    # Idempotent: a retried cancel finds its own placeholder; still one run.
    assert await market.control.occupy_job(market.seller_ns, wf, BUYER_CANCEL_REASON) is (
        Occupation.OCCUPIED
    )
    assert len(await history_of(market.seller, market.seller_ns, wf)) == 2


async def test_occupy_job_reports_a_run_that_got_there_first(market: Market) -> None:
    """A call (or the seller) that started ``job-<id>`` before the cancel is reported, and
    left alone: the cancel must stop it as a running job."""
    token = unique("hold")
    wf = await market.script_hire(token, "slow")
    run = await market.wait(wf, WorkflowExecutionStatus.RUNNING)
    assert await market.control.occupy_job(market.seller_ns, wf, BUYER_CANCEL_REASON) is (
        Occupation.STARTED
    )
    desc = await market.seller.get_workflow_handle(wf).describe()
    assert (desc.run_id, desc.status) == (run, WorkflowExecutionStatus.RUNNING)
    obs, _ = await market.control.observe(market.seller_ns, wf, market.call(token), None)
    assert (obs.status, obs.provenance) == (ObservedStatus.RUNNING, Provenance.VERIFIED)

    closed = await market.script_hire(unique("hold"))
    await market.wait(closed, WorkflowExecutionStatus.COMPLETED)
    assert await market.control.occupy_job(market.seller_ns, closed, BUYER_CANCEL_REASON) is (
        Occupation.STARTED
    )


async def test_a_placeholder_whose_termination_never_landed(
    market: Market, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The gate dies between the placeholder's start and its termination: the id is
    occupied all the same (the handler's ``FAIL`` refuses an open run too), no worker ever
    gets it (its first task is delayed past its own timeout), a later cancel finishes the
    job, and left alone it closes by itself."""
    landed: list[str] = []

    async def lost(self: Control, namespace: str, workflow_id: str, run_id: str, r: str) -> None:
        landed.append(run_id)

    close = Control._close_placeholder  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(Control, "_close_placeholder", lost)
    monkeypatch.setattr(control_module, "_OCCUPY_TIMEOUT", timedelta(seconds=2))
    wf, alone = job_id(), job_id()
    for w in (wf, alone):
        assert await market.control.occupy_job(market.seller_ns, w, "x") is Occupation.OCCUPIED
    assert len(landed) == 2
    assert (await market.seller.get_workflow_handle(wf).describe()).status is (
        WorkflowExecutionStatus.RUNNING
    )
    assert (await handler_start(market, wf)).status == RPCStatusCode.ALREADY_EXISTS
    obs, _ = await market.control.observe(market.seller_ns, wf, market.call("t"), None)
    assert (obs.status, obs.provenance) == (ObservedStatus.RUNNING, Provenance.OCCUPIED)

    monkeypatch.setattr(Control, "_close_placeholder", close)
    assert await market.control.occupy_job(market.seller_ns, wf, "x") is Occupation.OCCUPIED
    assert (await market.seller.get_workflow_handle(wf).describe()).status is (
        WorkflowExecutionStatus.TERMINATED
    )

    await market.wait(alone, WorkflowExecutionStatus.TIMED_OUT)
    assert [e.event_type for e in await history_of(market.seller, market.seller_ns, alone)] == [
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
    ]
    assert (await handler_start(market, alone)).status == RPCStatusCode.ALREADY_EXISTS


async def test_a_look_alike_the_seller_starts_is_not_the_placeholder(market: Market) -> None:
    """The placeholder is recognised by every field it sets: a seller's run of the same type
    and queue but its own identity is just a run the buyer's call did not start."""
    wf = job_id()
    req = StartWorkflowExecutionRequest(
        namespace=market.seller_ns,
        workflow_id=wf,
        workflow_type=WorkflowType(name=OCCUPY_WORKFLOW_TYPE),
        task_queue=TaskQueue(name=OCCUPY_TASK_QUEUE),
        identity="the seller itself",
        request_id=str(uuid.uuid4()),
    )
    await market.seller.workflow_service.start_workflow_execution(req)
    obs, _ = await market.control.observe(market.seller_ns, wf, market.call("t"), None)
    assert obs.provenance is Provenance.NOT_FROM_BUYER
    assert await market.control.occupy_job(market.seller_ns, wf, "x") is Occupation.STARTED
    await market.seller.get_workflow_handle(wf).terminate("done")


async def test_a_buyer_cancelling_its_own_script_call_is_read_from_its_records(
    market: Market,
) -> None:
    """The buyer cancels its standalone operation: the seller's run ends canceled, and the
    operation's own record (its cancellation info) says the buyer asked, so the refund is
    the buyer's cancel. A run the seller cancels itself is not."""
    token, op = unique("hold"), unique("op")
    wf = await market.script_hire(token, "slow", operation_id=op)
    run = await market.wait(wf, WorkflowExecutionStatus.RUNNING)
    call = market.call(token)
    obs, _ = await market.control.observe(market.seller_ns, wf, call, None)
    now = datetime.now(UTC)
    working, _ = escrow.observe(a_job(now), obs, now)
    assert working.pin == run
    await market.buyer.workflow_service.request_cancel_nexus_operation_execution(
        RequestCancelNexusOperationExecutionRequest(
            namespace=market.buyer_ns,
            operation_id=op,
            identity="the buyer",
            request_id=str(uuid.uuid4()),
            reason="changed my mind",
        )
    )
    await market.wait(wf, WorkflowExecutionStatus.CANCELED)
    for pinned in (run, None):
        fresh = Control(market.control.client, market.control.http_base)
        obs, result = await fresh.observe(market.seller_ns, wf, call, pinned)
        assert (obs.status, obs.provenance, obs.buyer_canceled, result) == (
            ObservedStatus.CANCELED,
            Provenance.VERIFIED,
            True,
            None,
        ), pinned
    job, effects = escrow.observe(working, obs, datetime.now(UTC))
    assert (job.state, job.escrow, job.reason) == (
        JobState.CANCELED,
        Escrow.REFUNDED,
        BUYER_CANCEL_REASON,
    )
    assert [e.reason for e in effects] == [BUYER_CANCEL_REASON]

    # Another job's hold does not make it this job's call, cancel or no cancel.
    obs, _ = await market.control.observe(market.seller_ns, wf, market.call("other"), None)
    assert (obs.provenance, obs.buyer_canceled) == (Provenance.NOT_FROM_BUYER, False)

    # The seller cancels its own run: nothing in the buyer's records asks for it.
    token = unique("hold")
    wf = await market.script_hire(token, "slow")
    run = await market.wait(wf, WorkflowExecutionStatus.RUNNING)
    await market.seller.get_workflow_handle(wf).cancel()
    await market.wait(wf, WorkflowExecutionStatus.CANCELED)
    obs, _ = await market.control.observe(market.seller_ns, wf, market.call(token), run)
    assert (obs.status, obs.buyer_canceled) == (ObservedStatus.CANCELED, False)
    job, _ = escrow.observe(working, replace(obs, pin=working.pin), datetime.now(UTC))
    assert job.reason == "canceled"


class _Forgetful:
    """A workflow service whose first ``misses`` descriptions of a standalone operation
    answer NOT_FOUND, as for a record that cannot be found (everything else works)."""

    def __init__(self, inner: object, misses: int) -> None:
        self._inner, self.misses = inner, misses

    def __getattr__(self, name: str) -> object:
        call = getattr(self._inner, name)
        if name != "describe_nexus_operation_execution":
            return call

        async def forgetful(req: object, **kw: object) -> object:
            if self.misses > 0:
                self.misses -= 1
                raise RPCError("operation not found", RPCStatusCode.NOT_FOUND, b"")
            return await call(req, **kw)

        return forgetful


class _ForgetfulControl(Control):
    def __init__(self, control: Control, misses: int) -> None:
        super().__init__(control.client, control.http_base)
        self.service = _Forgetful(control.client.workflow_service, misses)

    @property
    def _wf(self) -> object:  # pyright: ignore[reportIncompatibleVariableOverride]
        return self.service


async def test_a_cancel_lookup_that_cannot_find_the_call_cannot_say(market: Market) -> None:
    """Who canceled is said only once the buyer's call itself was read. A look that found
    nothing (the operation's record read as not found) is "cannot say", not "the buyer did
    not ask": escrow closes the job on the first answer, so that answer would count, for
    good, against the seller a cancel the buyer asked for. The next look asks again, and a
    known answer is remembered."""
    token, op = unique("hold"), unique("op")
    wf = await market.script_hire(token, "slow", operation_id=op)
    run = await market.wait(wf, WorkflowExecutionStatus.RUNNING)
    await market.buyer.workflow_service.request_cancel_nexus_operation_execution(
        RequestCancelNexusOperationExecutionRequest(
            namespace=market.buyer_ns, operation_id=op, request_id=str(uuid.uuid4())
        )
    )
    await market.wait(wf, WorkflowExecutionStatus.CANCELED)
    call = market.call(token)
    gate = _ForgetfulControl(market.control, misses=1)
    with pytest.raises(TemporalError) as err:  # pinned: verified already
        await gate.observe(market.seller_ns, wf, call, run)
    assert not isinstance(err.value, BuyerLookupError)  # read, and not found: no load
    obs, _ = await gate.observe(market.seller_ns, wf, call, run)
    assert (obs.status, obs.provenance, obs.buyer_canceled) == (
        ObservedStatus.CANCELED,
        Provenance.VERIFIED,
        True,
    )
    gate.service.misses = 1  # known now: the answer no longer depends on another read
    obs, _ = await gate.observe(market.seller_ns, wf, call, run)
    assert (obs.buyer_canceled, gate.service.misses) == (True, 1)


async def test_nothing_else_the_buyer_does_to_its_call_cancels_the_sellers_run(
    market: Market,
) -> None:
    """``buyer_canceled`` reads only an explicit cancel request. That is enough only while
    nothing else a buyer does to its own call cancels the seller's run: terminating the
    operation or the calling workflow, the workflow ending (or being cancelled) without
    cancelling the call, the operation timing out. Server 1.32 cancels the run on none of
    them (measured here); a server that starts to would let a buyer have the seller's run
    canceled with no request on record, the refund counted against the seller."""
    terminated_op, timed_out_op = unique("op"), unique("op")
    runs: dict[str, str] = {}
    timed_out = job_id()
    await market.control.start_operation(
        market.buyer_ns,
        timed_out_op,
        market.endpoint,
        market.service,
        EXECUTE_OPERATION,
        plan_payload("slow"),
        {"x-workflow-id": timed_out, HOLD_HEADER: unique("hold")},
        timedelta(seconds=8),  # long enough to start under load, then it times out
    )
    await market.wait(timed_out, WorkflowExecutionStatus.RUNNING)
    runs[timed_out] = "timed-out operation"
    wf = await market.script_hire(unique("hold"), "slow", operation_id=terminated_op)
    runs[wf] = "terminated operation"
    flows = []
    for flow, what in (
        (CancellingBuyerFlow.run, "terminated workflow"),
        (CancellingBuyerFlow.run, "cancelled workflow"),
        (StartingBuyerFlow.run, "workflow that ended"),
    ):
        wf = job_id()
        handle = await market.buyer.start_workflow(
            flow,
            Hire(market.endpoint, market.service, wf, unique("hold"), "slow"),
            id=unique("buyer-flow"),
            task_queue=market.buyer_queue,
        )
        flows.append((handle, what))
        runs[wf] = what
    for wf in runs:
        await market.wait(wf, WorkflowExecutionStatus.RUNNING)
    await market.buyer.workflow_service.terminate_nexus_operation_execution(
        TerminateNexusOperationExecutionRequest(
            namespace=market.buyer_ns, operation_id=terminated_op, request_id=str(uuid.uuid4())
        )
    )
    for handle, what in flows:
        if what == "terminated workflow":
            await handle.terminate("the buyer ends its own workflow")
        elif what == "cancelled workflow":
            await handle.cancel()
        else:
            assert await handle.result() == "started"
    timed = NexusOperationExecutionStatus.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT
    for _ in range(300):
        op = await market.buyer.workflow_service.describe_nexus_operation_execution(
            DescribeNexusOperationExecutionRequest(
                namespace=market.buyer_ns, operation_id=timed_out_op
            )
        )
        if op.info.status == timed:
            break
        await asyncio.sleep(0.1)
    assert op.info.status == timed
    await asyncio.sleep(3)  # any cancel these caused would have reached the seller by now
    for wf, what in runs.items():
        desc = await market.seller.get_workflow_handle(wf).describe()
        assert desc.status is WorkflowExecutionStatus.RUNNING, what
        await market.seller.get_workflow_handle(wf).terminate("done")


async def test_a_buyer_cancelling_its_own_workflow_call_is_read_from_its_history(
    market: Market,
) -> None:
    """The workflow door: the buyer's workflow cancels the call, which its history records
    as ``NexusOperationCancelRequested`` for the scheduled event the run's link names."""
    token = unique("hold")
    wf = job_id()
    buyer = await market.buyer.start_workflow(
        CancellingBuyerFlow.run,
        Hire(market.endpoint, market.service, wf, token, "slow"),
        id=unique("buyer-flow"),
        task_queue=market.buyer_queue,
    )
    run = await market.wait(wf, WorkflowExecutionStatus.RUNNING)
    call = market.call(token)
    obs, _ = await market.control.observe(market.seller_ns, wf, call, None)
    assert (obs.provenance, obs.buyer_canceled) == (Provenance.VERIFIED, False)
    await buyer.signal(CancellingBuyerFlow.cancel_call)
    assert await buyer.result() == "canceled"
    await market.wait(wf, WorkflowExecutionStatus.CANCELED)
    fresh = Control(market.control.client, market.control.http_base)
    obs, _ = await fresh.observe(market.seller_ns, wf, call, run)
    assert (obs.status, obs.provenance, obs.buyer_canceled) == (
        ObservedStatus.CANCELED,
        Provenance.VERIFIED,
        True,
    )

    # A workflow call the buyer never cancelled, ended by the seller: not the buyer's doing.
    token = unique("hold")
    wf = job_id()
    other = await market.buyer.start_workflow(
        CancellingBuyerFlow.run,
        Hire(market.endpoint, market.service, wf, token, "slow"),
        id=unique("buyer-flow"),
        task_queue=market.buyer_queue,
    )
    run = await market.wait(wf, WorkflowExecutionStatus.RUNNING)
    await market.seller.get_workflow_handle(wf).cancel()
    await market.wait(wf, WorkflowExecutionStatus.CANCELED)
    obs, _ = await fresh.observe(market.seller_ns, wf, market.call(token), run)
    assert (obs.status, obs.buyer_canceled) == (ObservedStatus.CANCELED, False)
    await other.signal(CancellingBuyerFlow.cancel_call)  # lets the buyer's flow end
    assert await other.result() == "canceled"


class _Busy:
    """A workflow service whose calls in one namespace fail as an overloaded namespace's do."""

    def __init__(self, inner: object, namespace: str) -> None:
        self._inner, self._namespace = inner, namespace

    def __getattr__(self, name: str) -> object:
        call = getattr(self._inner, name)

        async def busy(req: object, **kw: object) -> object:
            if getattr(req, "namespace", None) == self._namespace:
                raise RPCError(
                    "namespace rate limit exceeded", RPCStatusCode.RESOURCE_EXHAUSTED, b""
                )
            return await call(req, **kw)

        return busy


class _BusyControl(Control):
    """A control plane that cannot read ``namespace`` (everything else works)."""

    def __init__(self, control: Control, namespace: str) -> None:
        super().__init__(control.client, control.http_base)
        self._busy = _Busy(control.client.workflow_service, namespace)

    @property
    def _wf(self) -> object:  # pyright: ignore[reportIncompatibleVariableOverride]
        return self._busy


async def test_a_buyers_unreadable_namespace_is_its_own_error(market: Market) -> None:
    """A buyer can keep its own namespace too busy to read. That must surface as
    :class:`BuyerLookupError` -- a gate never gives up into a blind refund on it -- for
    both doors and for the cancellation lookup; the seller's side failing is a plain
    :class:`TemporalError`."""
    token = unique("hold")
    script = await market.script_hire(token)
    await market.wait(script, WorkflowExecutionStatus.COMPLETED)
    door_token = unique("hold")
    door = await market.workflow_hire(door_token)
    busy_buyer = _BusyControl(market.control, market.buyer_ns)
    with pytest.raises(BuyerLookupError):
        await busy_buyer.observe(market.seller_ns, script, market.call(token), None)
    with pytest.raises(BuyerLookupError):
        await busy_buyer.observe(market.seller_ns, door, market.call(door_token), None)

    op, token = unique("op"), unique("hold")
    wf = await market.script_hire(token, "slow", operation_id=op)
    run = await market.wait(wf, WorkflowExecutionStatus.RUNNING)
    await market.buyer.workflow_service.request_cancel_nexus_operation_execution(
        RequestCancelNexusOperationExecutionRequest(
            namespace=market.buyer_ns, operation_id=op, request_id=str(uuid.uuid4())
        )
    )
    await market.wait(wf, WorkflowExecutionStatus.CANCELED)
    with pytest.raises(BuyerLookupError):  # pinned: only who canceled is asked of the buyer
        await busy_buyer.observe(market.seller_ns, wf, market.call(token), run)

    busy_seller = _BusyControl(market.control, market.seller_ns)
    with pytest.raises(TemporalError) as err:
        await busy_seller.observe(market.seller_ns, script, market.call(token), None)
    assert not isinstance(err.value, BuyerLookupError)
