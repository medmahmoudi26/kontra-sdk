"""The stream relay, the worker controller, seller self-access and the denials escrow
depends on, over the wire (seam ①, decisions I30).

The Go deny matrix (``temporal/authorizer/authorizer_test.go``) decides every cell against
typed requests; this file proves the same cells against the real server binary with real
``temporalio`` clients and tokens minted the way the gate mints them:

* the gate's **stream relay** for job J (``names.relay_subject``, no permissions) polls its
  job's stream and reads its offset with only ``UpdateWorkflowExecution`` and
  ``QueryWorkflow`` -- the strict wire shape (I30, Q1) -- and is refused everything else,
  the stock ``WorkflowStreamClient.subscribe()`` included;
* the **worker controller** (KON-15) rolls out a versioned worker of Kontra's own in the
  platform namespace, and is refused everywhere else;
* a **party token** steers its own jobs (updates, queries, signals, cancel, terminate), and
  is refused every call that would change what escrow observes.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import timedelta
from typing import Any

import pytest
import temporal_isolation_app as app
from conftest import Cluster, connect_public
from google.protobuf.duration_pb2 import Duration
from google.protobuf.field_mask_pb2 import FieldMask
from temporalio.api.batch.v1 import BatchOperationReset
from temporalio.api.common.v1 import Payload, Payloads, WorkflowExecution, WorkflowType
from temporalio.api.deployment.v1 import WorkerDeploymentVersion as DeploymentVersion
from temporalio.api.enums.v1 import (
    ResetReapplyType,
    TaskQueueType,
    UpdateWorkflowExecutionLifecycleStage,
    WorkflowExecutionStatus,
)
from temporalio.api.errordetails.v1 import PermissionDeniedFailure
from temporalio.api.namespace.v1 import NamespaceConfig
from temporalio.api.query.v1 import WorkflowQuery
from temporalio.api.schedule.v1 import Schedule, ScheduleAction, ScheduleSpec
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.update.v1 import Input as UpdateInput
from temporalio.api.update.v1 import Meta as UpdateMeta
from temporalio.api.update.v1 import Request as UpdateRequest
from temporalio.api.update.v1 import UpdateRef, WaitPolicy
from temporalio.api.workflow.v1 import NewWorkflowExecutionInfo, WorkflowExecutionOptions
from temporalio.api.workflowservice.v1 import (
    CountWorkflowExecutionsRequest,
    CreateScheduleRequest,
    CreateWorkerDeploymentRequest,
    CreateWorkerDeploymentVersionRequest,
    DeleteNexusOperationExecutionRequest,
    DeleteWorkerDeploymentRequest,
    DeleteWorkerDeploymentVersionRequest,
    DeleteWorkflowExecutionRequest,
    DescribeNamespaceRequest,
    DescribeTaskQueueRequest,
    DescribeWorkerDeploymentRequest,
    DescribeWorkerDeploymentVersionRequest,
    DescribeWorkflowExecutionRequest,
    ExecuteMultiOperationRequest,
    GetWorkflowExecutionHistoryRequest,
    ListWorkerDeploymentsRequest,
    ListWorkflowExecutionsRequest,
    PauseWorkflowExecutionRequest,
    PollWorkflowExecutionUpdateRequest,
    PollWorkflowTaskQueueRequest,
    QueryWorkflowRequest,
    RequestCancelWorkflowExecutionRequest,
    ResetWorkflowExecutionRequest,
    SetWorkerDeploymentCurrentVersionRequest,
    SetWorkerDeploymentManagerRequest,
    SetWorkerDeploymentRampingVersionRequest,
    SignalWorkflowExecutionRequest,
    StartBatchOperationRequest,
    StartWorkflowExecutionRequest,
    TerminateWorkflowExecutionRequest,
    UpdateNamespaceRequest,
    UpdateWorkerDeploymentVersionComputeConfigRequest,
    UpdateWorkerDeploymentVersionMetadataRequest,
    UpdateWorkerVersioningRulesRequest,
    UpdateWorkflowExecutionOptionsRequest,
    UpdateWorkflowExecutionRequest,
    ValidateWorkerDeploymentVersionComputeConfigRequest,
)
from temporalio.client import (
    Client,
    WithStartWorkflowOperation,
    WorkflowHandle,
    WorkflowUpdateStage,
)
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion, WorkflowIDConflictPolicy
from temporalio.contrib.workflow_streams import PollInput, PollResult, WorkflowStreamClient
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker, WorkerDeploymentConfig
from wire_ledger import assert_allowed, assert_authorized, assert_denied, assert_refused, record

from kontra.contract import names

pytestmark = pytest.mark.temporal

SELLER = app.SELLER_NAMESPACE
BUYER = app.BUYER_NAMESPACE
OTHER = app.OTHER_NAMESPACE
PLATFORM = names.PLATFORM_NAMESPACE
#: The workflow stream's publish signal (temporalio.contrib.workflow_streams): a relay may
#: never send it, so it can never inject events into a stream.
STREAM_PUBLISH = "__temporal_workflow_stream_publish"
#: Each relay RPC's deadline. A poll still waiting when it passes is re-sent with the same
#: update id.
RELAY_RPC_TIMEOUT = timedelta(seconds=10)
COMPLETED = (
    UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
)


def wf(workflow_id: str) -> WorkflowExecution:
    return WorkflowExecution(workflow_id=workflow_id)


async def seller_client(cluster: Cluster) -> Client:
    return await connect_public(cluster, SELLER, api_key=cluster.signer.api_key(SELLER))


@contextlib.asynccontextmanager
async def stream_job(
    cluster: Cluster, steps: int = 2
) -> AsyncIterator[tuple[Client, str, WorkflowHandle[Any, str]]]:
    """A seller's worker serving StreamJob, and one job of it running: (the seller's client,
    the job id, its handle). The job is terminated, by the seller, on the way out."""
    seller = await seller_client(cluster)
    job_id = names.new_job_id()
    async with Worker(seller, task_queue=app.SELLER_TASK_QUEUE, workflows=[app.StreamJob]):
        handle = await seller.start_workflow(
            app.StreamJob.run,
            app.StreamJobInput(steps),
            id=names.job_workflow_id(job_id),
            task_queue=app.SELLER_TASK_QUEUE,
        )
        try:
            yield seller, job_id, handle
        finally:
            with contextlib.suppress(RPCError):
                await handle.terminate("the test is over")


async def eventually(check: Callable[[], Awaitable[bool]], within: float = 30.0) -> None:
    deadline = asyncio.get_running_loop().time() + within
    while not await check():
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError("condition not reached")
        await asyncio.sleep(0.2)


# --- the strict relay: the only two calls the gate's relay makes -----------------------


async def relay_poll(
    relay: Client,
    namespace: str,
    workflow_id: str,
    *,
    topics: list[str],
    from_offset: int,
    first_execution_run_id: str = "",
    job_closed: Callable[[], Awaitable[bool]] | None = None,
    within: float = 60.0,
    cooldown: float = 0.1,
) -> PollResult | None:
    """One subscribe poll made the way the gate's relay makes it (decisions I30, Q1).

    Only ``UpdateWorkflowExecution`` with the stream's poll update, ``run_id`` empty (the
    current run, so it follows a continue-as-new) and ``wait_policy`` COMPLETED. While the
    answer carries no outcome, the same request -- the same update id -- is sent again: the
    server finds an update by its id and returns the in-flight or completed update's outcome.
    So the relay never needs ``PollWorkflowExecutionUpdate`` (which would read any update by
    id) or ``DescribeWorkflowExecution``. A draining stream (continue-as-new) is polled
    again with a new id after a cooldown; a truncated offset restarts from 0; an update
    accepted by a run that then closed is retried with a new id, unless the gate's own
    observer (``job_closed``, the internal frontend) says the job is closed: then None.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    offset = from_offset
    while True:
        args = await relay.data_converter.encode([PollInput(topics=topics, from_offset=offset)])
        request = UpdateWorkflowExecutionRequest(
            namespace=namespace,
            workflow_execution=wf(workflow_id),
            first_execution_run_id=first_execution_run_id,
            wait_policy=WaitPolicy(lifecycle_stage=COMPLETED),
            request=UpdateRequest(
                meta=UpdateMeta(update_id=str(uuid.uuid4()), identity="kontra-relay"),
                input=UpdateInput(name=names.STREAM_POLL_UPDATE, args=Payloads(payloads=args)),
            ),
        )
        outcome = None
        while outcome is None:
            if loop.time() > deadline:
                raise TimeoutError(f"no poll outcome from {workflow_id} within {within}s")
            try:
                response = await relay.workflow_service.update_workflow_execution(
                    request, timeout=RELAY_RPC_TIMEOUT
                )
            except RPCError as e:
                if e.status in (RPCStatusCode.DEADLINE_EXCEEDED, RPCStatusCode.CANCELLED):
                    continue  # still waiting: the same update id again
                if job_closed is not None and await job_closed():
                    return None
                raise
            if response.HasField("outcome"):
                outcome = response.outcome
        if outcome.HasField("failure"):
            kind = outcome.failure.application_failure_info.type
            if kind == "StreamDraining":
                await asyncio.sleep(cooldown)
                continue
            if kind == "TruncatedOffset":
                offset = 0
                continue
            if kind == "AcceptedUpdateCompletedWorkflow":
                if job_closed is not None and await job_closed():
                    return None
                continue
            raise AssertionError(f"the poll failed: {kind}: {outcome.failure.message}")
        [result] = await relay.data_converter.decode(outcome.success.payloads, [PollResult])
        return result


async def relay_offset(relay: Client, namespace: str, workflow_id: str) -> int:
    """The stream's length, by the offset query: the relay's other call."""
    response = await relay.workflow_service.query_workflow(
        QueryWorkflowRequest(
            namespace=namespace,
            execution=wf(workflow_id),
            query=WorkflowQuery(query_type=names.STREAM_OFFSET_QUERY),
        ),
        timeout=RELAY_RPC_TIMEOUT,
    )
    [offset] = await relay.data_converter.decode(response.query_result.payloads, [int])
    return offset


def stream_items(client: Client, result: PollResult | None) -> list[tuple[str, str]]:
    """(topic, value) of every item of a poll result."""
    assert result is not None
    items: list[tuple[str, str]] = []
    for item in result.items:
        payload = Payload()
        payload.ParseFromString(base64.b64decode(item.data))
        items.append(
            (item.topic, client.data_converter.payload_converter.from_payload(payload, str))
        )
    return items


def denial_reason(e: RPCError) -> str:
    """The authorizer's reason for a PERMISSION_DENIED ("kontra: ..."), which Temporal
    carries in the status details, not in the message."""
    for detail in e.grpc_status.details:
        failure = PermissionDeniedFailure()
        if detail.Unpack(failure):
            return failure.reason
    return ""


def observer(control: Client, namespace: str, workflow_id: str) -> Callable[[], Awaitable[bool]]:
    """The gate's own view of a job, through the internal frontend (decisions I6)."""

    async def closed() -> bool:
        response = await control.workflow_service.describe_workflow_execution(
            DescribeWorkflowExecutionRequest(namespace=namespace, execution=wf(workflow_id))
        )
        status = response.workflow_execution_info.status
        return status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING

    return closed


async def test_relay_polls_its_jobs_stream(cluster: Cluster, control: Client) -> None:
    """The relay token of job J reads J's stream: from the start, by topic, from the tail
    while the seller publishes, and stops once the gate's observer sees J closed."""
    async with stream_job(cluster, steps=2) as (seller, job_id, handle):
        workflow_id = handle.id
        # The seller publishes too, through its own stock client: a signal to its own job.
        publisher = WorkflowStreamClient.create(seller, workflow_id)
        async with publisher:
            publisher.topic("progress", type=str).publish("from-the-seller", force_flush=True)
        record("seller-self/acme:worker/Signal(stream publish)", "allow", "OK")

        relay = await connect_public(cluster, SELLER, api_key=cluster.signer.relay(SELLER, job_id))
        offset = await assert_allowed(
            "relay-J/Query(offset)", relay_offset(relay, SELLER, workflow_id)
        )
        assert offset <= 5

        async def all_published() -> bool:
            return await relay_offset(relay, SELLER, workflow_id) == 5

        await eventually(all_published)

        # Once escrow has pinned the job's chain, the gate sets the pinned run as the
        # chain's first run: the server then refuses to relay a look-alike chain.
        pinned = handle.first_execution_run_id
        assert pinned

        everything = await assert_allowed(
            "relay-J/Update(poll) from 0, pinned to the first run",
            relay_poll(
                relay,
                SELLER,
                workflow_id,
                topics=[],
                from_offset=0,
                first_execution_run_id=pinned,
            ),
        )
        assert everything is not None
        assert everything.next_offset == 5
        assert sorted(stream_items(relay, everything)) == sorted(
            [
                ("progress", "step-0"),
                ("progress", "step-1"),
                ("progress", "from-the-seller"),
                ("log", "line-0"),
                ("log", "line-1"),
            ]
        )
        # Another first run: Temporal itself (not the authorizer) finds no such chain.
        await assert_refused(
            "relay-J/Update(poll) on a look-alike chain",
            RPCStatusCode.NOT_FOUND,
            relay_poll(
                relay,
                SELLER,
                workflow_id,
                topics=[],
                from_offset=0,
                first_execution_run_id=str(uuid.uuid4()),
                within=15,
            ),
        )
        progress = await relay_poll(relay, SELLER, workflow_id, topics=["progress"], from_offset=0)
        assert sorted(value for _, value in stream_items(relay, progress)) == [
            "from-the-seller",
            "step-0",
            "step-1",
        ]

        # From the tail: the poll waits until the seller publishes.
        waiting = asyncio.ensure_future(
            relay_poll(relay, SELLER, workflow_id, topics=["log"], from_offset=5)
        )
        await asyncio.sleep(1)
        assert not waiting.done()
        async with WorkflowStreamClient.create(seller, workflow_id) as late:
            late.topic("log", type=str).publish("late", force_flush=True)
        assert stream_items(relay, await waiting) == [("log", "late")]
        record("relay-J/Update(poll) from the tail", "allow", "OK")

        # The seller approves its own job, which completes; the relay's next poll ends.
        assert await handle.execute_update(app.StreamJob.approve, "the seller") == (
            "approved by the seller"
        )
        assert await handle.result() == "approved"
        after = await relay_poll(
            relay,
            SELLER,
            workflow_id,
            topics=[],
            from_offset=6,
            job_closed=observer(control, SELLER, workflow_id),
            within=30,
        )
        assert after is None
        record("relay-J/Update(poll) after the job closed", "stop", "observer: closed")


# --- the relay is refused everything else -----------------------------------------------

RelayCall = Callable[[Cluster, Client, str], Awaitable[Any]]


def _update(namespace: str, workflow_id: str, name: str) -> UpdateWorkflowExecutionRequest:
    return UpdateWorkflowExecutionRequest(
        namespace=namespace,
        workflow_execution=wf(workflow_id),
        wait_policy=WaitPolicy(lifecycle_stage=COMPLETED),
        request=UpdateRequest(
            meta=UpdateMeta(update_id=str(uuid.uuid4())), input=UpdateInput(name=name)
        ),
    )


def _query(namespace: str, workflow_id: str, query_type: str) -> QueryWorkflowRequest:
    return QueryWorkflowRequest(
        namespace=namespace, execution=wf(workflow_id), query=WorkflowQuery(query_type=query_type)
    )


def _start(namespace: str, workflow_id: str) -> StartWorkflowExecutionRequest:
    return StartWorkflowExecutionRequest(
        namespace=namespace,
        workflow_id=workflow_id,
        workflow_type=WorkflowType(name="StreamJob"),
        task_queue=TaskQueue(name=app.SELLER_TASK_QUEUE),
        request_id=str(uuid.uuid4()),
    )


async def _stock_subscribe(cluster: Cluster, relay: Client, _job_id: str) -> None:
    """The stock WorkflowStreamClient on a relay token: its poll Update is admitted, then it
    asks PollWorkflowExecutionUpdate for the outcome, which the strict relay never gets. The
    job has no new item from the tail, so the first poll waits and needs that call."""
    async with stream_job(cluster, steps=1) as (_seller, job_id, handle):
        relay = await connect_public(cluster, SELLER, api_key=cluster.signer.relay(SELLER, job_id))
        tail = await relay_offset(relay, SELLER, handle.id)
        stream = WorkflowStreamClient.create(relay, handle.id)

        async def first_item() -> None:
            async for _ in stream.subscribe(["progress"], from_offset=tail):
                pytest.fail("the stock subscriber got an item")

        try:
            await asyncio.wait_for(first_item(), timeout=30)
        except RPCError as e:
            # Admitted at the Update; refused at the outcome, the call a strict relay
            # never makes.
            reason = denial_reason(e)
            assert reason.endswith("/PollWorkflowExecutionUpdate"), reason
            raise


def _relay_update(name: str, *, job: str = "J", namespace: str = SELLER) -> RelayCall:
    async def call(_cluster: Cluster, relay: Client, job_id: str) -> Any:
        workflow_id = names.job_workflow_id(job_id if job == "J" else names.new_job_id())
        return await relay.workflow_service.update_workflow_execution(
            _update(namespace, workflow_id, name)
        )

    return call


def _relay_query(query_type: str, *, job: str = "J") -> RelayCall:
    async def call(_cluster: Cluster, relay: Client, job_id: str) -> Any:
        workflow_id = names.job_workflow_id(job_id if job == "J" else names.new_job_id())
        return await relay.workflow_service.query_workflow(_query(SELLER, workflow_id, query_type))

    return call


def _on_job(make: Callable[[str], Any], rpc: str) -> RelayCall:
    async def call(_cluster: Cluster, relay: Client, job_id: str) -> Any:
        return await getattr(relay.workflow_service, rpc)(make(names.job_workflow_id(job_id)))

    return call


RELAY_DENIED: dict[str, RelayCall] = {
    "Update(approve)@J": _relay_update("approve"),
    "Update(stream publish name)@J": _relay_update(STREAM_PUBLISH),
    "Query(__stack_trace)@J": _relay_query("__stack_trace"),
    "Query(__temporal_workflow_metadata)@J": _relay_query("__temporal_workflow_metadata"),
    "Signal(stream publish)@J": _on_job(
        lambda w: SignalWorkflowExecutionRequest(
            namespace=SELLER,
            workflow_execution=wf(w),
            signal_name=STREAM_PUBLISH,
            request_id=str(uuid.uuid4()),
        ),
        "signal_workflow_execution",
    ),
    "PollWorkflowExecutionUpdate@J": _on_job(
        lambda w: PollWorkflowExecutionUpdateRequest(
            namespace=SELLER,
            update_ref=UpdateRef(workflow_execution=wf(w), update_id="any-update"),
            wait_policy=WaitPolicy(lifecycle_stage=COMPLETED),
        ),
        "poll_workflow_execution_update",
    ),
    "DescribeWorkflow@J": _on_job(
        lambda w: DescribeWorkflowExecutionRequest(namespace=SELLER, execution=wf(w)),
        "describe_workflow_execution",
    ),
    "GetHistory@J": _on_job(
        lambda w: GetWorkflowExecutionHistoryRequest(namespace=SELLER, execution=wf(w)),
        "get_workflow_execution_history",
    ),
    "Terminate@J": _on_job(
        lambda w: TerminateWorkflowExecutionRequest(namespace=SELLER, workflow_execution=wf(w)),
        "terminate_workflow_execution",
    ),
    "RequestCancel@J": _on_job(
        lambda w: RequestCancelWorkflowExecutionRequest(
            namespace=SELLER, workflow_execution=wf(w), request_id=str(uuid.uuid4())
        ),
        "request_cancel_workflow_execution",
    ),
    "StartWorkflow(J's id)": _on_job(lambda w: _start(SELLER, w), "start_workflow_execution"),
    "ExecuteMultiOperation(update-with-start poll)@J": _on_job(
        lambda w: ExecuteMultiOperationRequest(
            namespace=SELLER,
            operations=[
                ExecuteMultiOperationRequest.Operation(start_workflow=_start(SELLER, w)),
                ExecuteMultiOperationRequest.Operation(
                    update_workflow=_update(SELLER, w, names.STREAM_POLL_UPDATE)
                ),
            ],
        ),
        "execute_multi_operation",
    ),
    "Update(poll)@K": _relay_update(names.STREAM_POLL_UPDATE, job="K"),
    "Query(offset)@K": _relay_query(names.STREAM_OFFSET_QUERY, job="K"),
    "Update(poll)@J in other": _relay_update(names.STREAM_POLL_UPDATE, namespace=OTHER),
    "Update(poll)@J in kontra-system": _relay_update(names.STREAM_POLL_UPDATE, namespace=PLATFORM),
    "ListWorkflows(acme)": _on_job(
        lambda _w: ListWorkflowExecutionsRequest(namespace=SELLER), "list_workflow_executions"
    ),
    "DescribeTaskQueue(acme)": _on_job(
        lambda _w: DescribeTaskQueueRequest(
            namespace=SELLER, task_queue=TaskQueue(name=app.SELLER_TASK_QUEUE)
        ),
        "describe_task_queue",
    ),
    "PollWorkflowTaskQueue(acme)": _on_job(
        lambda _w: PollWorkflowTaskQueueRequest(
            namespace=SELLER, task_queue=TaskQueue(name=app.SELLER_TASK_QUEUE)
        ),
        "poll_workflow_task_queue",
    ),
    "DescribeNamespace(acme)": _on_job(
        lambda _w: DescribeNamespaceRequest(namespace=SELLER), "describe_namespace"
    ),
    "stock WorkflowStreamClient.subscribe()": _stock_subscribe,
}


@pytest.mark.parametrize("cell", list(RELAY_DENIED))
async def test_relay_is_denied_everything_else(cluster: Cluster, cell: str) -> None:
    """Even a request aimed exactly at job J, with the stream's handler names, is refused on
    every other API; so are both handlers on another job and on J's id elsewhere."""
    job_id = names.new_job_id()
    relay = await connect_public(cluster, SELLER, api_key=cluster.signer.relay(SELLER, job_id))
    await assert_denied(f"relay-J/{cell}", RELAY_DENIED[cell](cluster, relay, job_id))


@pytest.mark.parametrize(
    ("permissions", "is_permission"),
    [
        ([f"{SELLER}:read"], True),
        ([f"{SELLER}:relay"], True),  # an unknown role still sets the namespace's key
        (["temporal-system:read"], True),  # a system role is a permission too
        ([], False),
        (["temporal-system:relay"], False),  # a system role the mapper does not know: dropped
        (["relay"], False),  # no ":": dropped
    ],
)
async def test_relay_subject_with_a_role_is_only_a_party_token(
    cluster: Cluster, permissions: list[str], is_permission: bool
) -> None:
    """A token that carries a permission is a party token, whatever its subject: the
    relay's subject with a role gets that role's calls (none) and never the relay's. A
    permission the claim mapper drops is no permission: an empty ``permissions`` claim, a
    ``temporal-system:`` role it does not know, an entry with no ``:``. That token is judged
    by its subject, here the relay's (decisions I30)."""
    job_id = names.new_job_id()
    workflow_id = names.job_workflow_id(job_id)
    relay = await connect_public(
        cluster, SELLER, api_key=cluster.signer.relay(SELLER, job_id, permissions=permissions)
    )
    poll = relay.workflow_service.update_workflow_execution(
        _update(SELLER, workflow_id, names.STREAM_POLL_UPDATE)
    )
    offset = relay.workflow_service.query_workflow(
        _query(SELLER, workflow_id, names.STREAM_OFFSET_QUERY)
    )
    shape = f"permissions={permissions}"
    if is_permission:
        await assert_denied(f"relay-J/{shape}/Update(poll)@J", poll)
        await assert_denied(f"relay-J/{shape}/Query(offset)@J", offset)
    else:
        # Authorized; the job does not exist, so Temporal answers NOT_FOUND.
        await assert_authorized(f"relay-J/{shape}/Update(poll)@J", poll)
        await assert_authorized(f"relay-J/{shape}/Query(offset)@J", offset)


async def test_a_revoked_relay_session_is_refused(cluster: Cluster) -> None:
    """The gate revokes a relay's session when the job closes or the last listener leaves;
    the relay token is then refused within a revocation poll, like a worker token."""
    job_id = names.new_job_id()
    workflow_id = names.job_workflow_id(job_id)
    session = "relay-" + uuid.uuid4().hex
    relay = await connect_public(
        cluster, SELLER, api_key=cluster.signer.relay(SELLER, job_id, sid=session)
    )

    def offset() -> Any:
        return relay.workflow_service.query_workflow(
            _query(SELLER, workflow_id, names.STREAM_OFFSET_QUERY)
        )

    await assert_authorized("revocation/relay/live-session", offset())
    cluster.jwks.revoke(session)
    deadline = asyncio.get_running_loop().time() + 20
    while True:  # the server reads the list every second
        try:
            await offset()
        except RPCError as e:
            if e.status == RPCStatusCode.PERMISSION_DENIED:
                break
            if e.status != RPCStatusCode.NOT_FOUND:
                raise
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail("a revoked relay session's token is still accepted")
        await asyncio.sleep(0.5)
    record("revocation/relay/revoked-session", "deny", "PERMISSION_DENIED")
    await assert_denied(
        "revocation/relay/revoked-session-poll",
        relay.workflow_service.update_workflow_execution(
            _update(SELLER, workflow_id, names.STREAM_POLL_UPDATE)
        ),
    )


# --- the worker controller (KON-15) ------------------------------------------------------


def _version(build_id: str) -> DeploymentVersion:
    return DeploymentVersion(deployment_name=app.PLATFORM_DEPLOYMENT, build_id=build_id)


async def test_controller_rolls_out_platform_workers(cluster: Cluster) -> None:
    """A versioned worker of Kontra's own polls the platform namespace with a platform worker
    token; the controller's token then makes each of its 10 calls there, and its promotion
    is what lets the worker run the platform's workflow."""
    build_id = "b-" + uuid.uuid4().hex[:8]
    platform = await connect_public(cluster, PLATFORM, api_key=cluster.signer.platform_worker())
    ctl = await connect_public(cluster, PLATFORM, api_key=cluster.signer.controller())
    svc = ctl.workflow_service
    identity = names.WORKER_CONTROLLER_SUBJECT
    async with Worker(
        platform,
        task_queue=app.PLATFORM_TASK_QUEUE,
        workflows=[app.PlatformPing],
        deployment_config=WorkerDeploymentConfig(
            version=WorkerDeploymentVersion(app.PLATFORM_DEPLOYMENT, build_id),
            use_worker_versioning=True,
            default_versioning_behavior=VersioningBehavior.PINNED,
        ),
    ):
        record("controller/platform worker/versioned poll(kontra-system)", "allow", "OK")

        async def registered() -> bool:
            try:
                await svc.describe_worker_deployment_version(
                    DescribeWorkerDeploymentVersionRequest(
                        namespace=PLATFORM, deployment_version=_version(build_id)
                    )
                )
            except RPCError as e:
                if e.status == RPCStatusCode.PERMISSION_DENIED:
                    raise
                return False
            return True

        await eventually(registered)
        await assert_authorized(
            "controller/DescribeWorkerDeployment(kontra-system)",
            svc.describe_worker_deployment(
                DescribeWorkerDeploymentRequest(
                    namespace=PLATFORM, deployment_name=app.PLATFORM_DEPLOYMENT
                )
            ),
        )
        await assert_authorized(
            "controller/DescribeWorkerDeploymentVersion(kontra-system)",
            svc.describe_worker_deployment_version(
                DescribeWorkerDeploymentVersionRequest(
                    namespace=PLATFORM, deployment_version=_version(build_id)
                )
            ),
        )
        await assert_authorized(
            "controller/DescribeTaskQueue(kontra-system)",
            svc.describe_task_queue(
                DescribeTaskQueueRequest(
                    namespace=PLATFORM,
                    task_queue=TaskQueue(name=app.PLATFORM_TASK_QUEUE),
                    task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
                )
            ),
        )
        await assert_authorized(
            "controller/CountWorkflowExecutions(kontra-system)",
            svc.count_workflow_executions(
                CountWorkflowExecutionsRequest(
                    namespace=PLATFORM,
                    query=(
                        f"TemporalWorkerDeploymentVersion = "
                        f"'{app.PLATFORM_DEPLOYMENT}:{build_id}' AND ExecutionStatus = 'Running'"
                    ),
                )
            ),
        )
        await assert_authorized(
            "controller/SetWorkerDeploymentManager(kontra-system)",
            svc.set_worker_deployment_manager(
                SetWorkerDeploymentManagerRequest(
                    namespace=PLATFORM,
                    deployment_name=app.PLATFORM_DEPLOYMENT,
                    self=True,
                    identity=identity,
                )
            ),
        )
        await assert_authorized(
            "controller/UpdateWorkerDeploymentVersionMetadata(kontra-system)",
            svc.update_worker_deployment_version_metadata(
                UpdateWorkerDeploymentVersionMetadataRequest(
                    namespace=PLATFORM,
                    deployment_version=_version(build_id),
                    upsert_entries={
                        "kontra-rollout": (await ctl.data_converter.encode(["test"]))[0]
                    },
                    identity=identity,
                )
            ),
        )
        await assert_authorized(
            "controller/SetWorkerDeploymentRampingVersion(kontra-system)",
            svc.set_worker_deployment_ramping_version(
                SetWorkerDeploymentRampingVersionRequest(
                    namespace=PLATFORM,
                    deployment_name=app.PLATFORM_DEPLOYMENT,
                    build_id=build_id,
                    percentage=10,
                    identity=identity,
                )
            ),
        )
        await assert_authorized(
            "controller/SetWorkerDeploymentCurrentVersion(kontra-system)",
            svc.set_worker_deployment_current_version(
                SetWorkerDeploymentCurrentVersionRequest(
                    namespace=PLATFORM,
                    deployment_name=app.PLATFORM_DEPLOYMENT,
                    build_id=build_id,
                    identity=identity,
                )
            ),
        )
        # Promoted: the platform's own workflow now runs on the versioned worker.
        pong = await assert_allowed(
            "platform-ns/kontra-system:worker/PlatformPing after the promotion",
            platform.execute_workflow(
                app.PlatformPing.run,
                id=f"ping-{uuid.uuid4().hex}",
                task_queue=app.PLATFORM_TASK_QUEUE,
                execution_timeout=timedelta(seconds=60),
            ),
        )
        assert pong == "pong"
        # Retiring the current version, then the deployment, is refused by Temporal for
        # their state, after the authorizer let them through.
        await assert_authorized(
            "controller/DeleteWorkerDeploymentVersion(kontra-system)",
            svc.delete_worker_deployment_version(
                DeleteWorkerDeploymentVersionRequest(
                    namespace=PLATFORM,
                    deployment_version=_version(build_id),
                    skip_drainage=True,
                    identity=identity,
                )
            ),
        )
        await assert_authorized(
            "controller/DeleteWorkerDeployment(kontra-system)",
            svc.delete_worker_deployment(
                DeleteWorkerDeploymentRequest(
                    namespace=PLATFORM, deployment_name=app.PLATFORM_DEPLOYMENT, identity=identity
                )
            ),
        )


def _controller_calls(namespace: str) -> dict[str, tuple[str, Any]]:
    """The controller's 10 calls, aimed at ``namespace``."""
    version = _version("b1")
    deployment = app.PLATFORM_DEPLOYMENT
    return {
        "DescribeWorkerDeployment": (
            "describe_worker_deployment",
            DescribeWorkerDeploymentRequest(namespace=namespace, deployment_name=deployment),
        ),
        "DescribeWorkerDeploymentVersion": (
            "describe_worker_deployment_version",
            DescribeWorkerDeploymentVersionRequest(namespace=namespace, deployment_version=version),
        ),
        "SetWorkerDeploymentCurrentVersion": (
            "set_worker_deployment_current_version",
            SetWorkerDeploymentCurrentVersionRequest(
                namespace=namespace, deployment_name=deployment, build_id="b1"
            ),
        ),
        "SetWorkerDeploymentRampingVersion": (
            "set_worker_deployment_ramping_version",
            SetWorkerDeploymentRampingVersionRequest(
                namespace=namespace, deployment_name=deployment, build_id="b1", percentage=5
            ),
        ),
        "SetWorkerDeploymentManager": (
            "set_worker_deployment_manager",
            SetWorkerDeploymentManagerRequest(
                namespace=namespace, deployment_name=deployment, self=True
            ),
        ),
        "UpdateWorkerDeploymentVersionMetadata": (
            "update_worker_deployment_version_metadata",
            UpdateWorkerDeploymentVersionMetadataRequest(
                namespace=namespace, deployment_version=version
            ),
        ),
        "DeleteWorkerDeploymentVersion": (
            "delete_worker_deployment_version",
            DeleteWorkerDeploymentVersionRequest(namespace=namespace, deployment_version=version),
        ),
        "DeleteWorkerDeployment": (
            "delete_worker_deployment",
            DeleteWorkerDeploymentRequest(namespace=namespace, deployment_name=deployment),
        ),
        "DescribeTaskQueue": (
            "describe_task_queue",
            DescribeTaskQueueRequest(namespace=namespace, task_queue=TaskQueue(name="any")),
        ),
        "CountWorkflowExecutions": (
            "count_workflow_executions",
            CountWorkflowExecutionsRequest(namespace=namespace),
        ),
    }


async def test_controller_is_denied_on_seller_namespaces(cluster: Cluster) -> None:
    """Its 10 calls in a seller's or a buyer's namespace; in the platform namespace, every
    call that is not a rollout -- workflows, tasks, other deployment and versioning calls,
    and compute config, which makes the server act on a cloud provider. A party token is
    denied the deployment calls everywhere, a platform worker's included."""
    ctl = (
        await connect_public(cluster, PLATFORM, api_key=cluster.signer.controller())
    ).workflow_service
    for namespace in (SELLER, BUYER):
        for name, (rpc, request) in _controller_calls(namespace).items():
            await assert_denied(f"controller/{name}({namespace})", getattr(ctl, rpc)(request))
    version = _version("b1")
    platform_denied: dict[str, tuple[str, Any]] = {
        "StartWorkflow": ("start_workflow_execution", _start(PLATFORM, "rollout-gate")),
        "DescribeWorkflow": (
            "describe_workflow_execution",
            DescribeWorkflowExecutionRequest(namespace=PLATFORM, execution=wf("rollout-gate")),
        ),
        "SignalWorkflow": (
            "signal_workflow_execution",
            SignalWorkflowExecutionRequest(
                namespace=PLATFORM, workflow_execution=wf("rollout-gate"), signal_name="s"
            ),
        ),
        "PollWorkflowTaskQueue": (
            "poll_workflow_task_queue",
            PollWorkflowTaskQueueRequest(
                namespace=PLATFORM, task_queue=TaskQueue(name=app.PLATFORM_TASK_QUEUE)
            ),
        ),
        "ListWorkerDeployments": (
            "list_worker_deployments",
            ListWorkerDeploymentsRequest(namespace=PLATFORM),
        ),
        "CreateWorkerDeployment": (
            "create_worker_deployment",
            CreateWorkerDeploymentRequest(namespace=PLATFORM, deployment_name="another"),
        ),
        "UpdateWorkerVersioningRules": (
            "update_worker_versioning_rules",
            UpdateWorkerVersioningRulesRequest(
                namespace=PLATFORM, task_queue=app.PLATFORM_TASK_QUEUE
            ),
        ),
        "CreateWorkerDeploymentVersion(compute config)": (
            "create_worker_deployment_version",
            CreateWorkerDeploymentVersionRequest(namespace=PLATFORM, deployment_version=version),
        ),
        "UpdateWorkerDeploymentVersionComputeConfig": (
            "update_worker_deployment_version_compute_config",
            UpdateWorkerDeploymentVersionComputeConfigRequest(
                namespace=PLATFORM, deployment_version=version
            ),
        ),
        "ValidateWorkerDeploymentVersionComputeConfig": (
            "validate_worker_deployment_version_compute_config",
            ValidateWorkerDeploymentVersionComputeConfigRequest(
                namespace=PLATFORM, deployment_version=version
            ),
        ),
    }
    for name, (rpc, request) in platform_denied.items():
        await assert_denied(f"controller/{name}(kontra-system)", getattr(ctl, rpc)(request))
    # The controller's subject with a role is a party token: a platform worker at most. A
    # system role is a permission too.
    with_role = await connect_public(
        cluster,
        PLATFORM,
        api_key=cluster.signer.controller(permissions=[f"{PLATFORM}:worker"]),
    )
    with_system_role = await connect_public(
        cluster,
        PLATFORM,
        api_key=cluster.signer.controller(permissions=["temporal-system:admin"]),
    )
    platform_worker = await connect_public(
        cluster, PLATFORM, api_key=cluster.signer.platform_worker()
    )
    seller = await seller_client(cluster)
    rpc, request = _controller_calls(PLATFORM)["SetWorkerDeploymentCurrentVersion"]
    for who, client in (
        ("controller+kontra-system:worker", with_role),
        ("controller+temporal-system:admin", with_system_role),
        ("kontra-system:worker", platform_worker),
        ("acme:worker", seller),
    ):
        await assert_denied(
            f"controller/{who}/SetWorkerDeploymentCurrentVersion(kontra-system)",
            getattr(client.workflow_service, rpc)(request),
        )
    rpc, request = _controller_calls(SELLER)["SetWorkerDeploymentCurrentVersion"]
    await assert_denied(
        "controller/acme:worker/SetWorkerDeploymentCurrentVersion(acme)",
        getattr(seller.workflow_service, rpc)(request),
    )
    # A permission the claim mapper drops (a system role it does not know, an entry with no
    # ":") is no permission: the token is judged by its subject, the controller's, and gets
    # the controller's calls and nothing more (decisions I30).
    rpc, request = _controller_calls(PLATFORM)["DescribeWorkerDeployment"]
    for dropped in ("temporal-system:bogus", PLATFORM):
        client = await connect_public(
            cluster, PLATFORM, api_key=cluster.signer.controller(permissions=[dropped])
        )
        who = f"controller+{dropped} (dropped)"
        await assert_authorized(
            f"controller/{who}/DescribeWorkerDeployment(kontra-system)",
            getattr(client.workflow_service, rpc)(request),
        )
        await assert_denied(
            f"controller/{who}/StartWorkflow(kontra-system)",
            client.workflow_service.start_workflow_execution(_start(PLATFORM, "rollout-gate")),
        )


# --- a party steers its own jobs ---------------------------------------------------------


async def test_seller_steers_its_own_jobs(cluster: Cluster) -> None:
    """Seller self-access (decisions I30): updates and their outcomes, queries, signals,
    signal-with-start, update-with-start, cancel and terminate, on its own jobs. Escrow
    reads none of what these write; stopping its own job is a refund against the seller."""
    async with stream_job(cluster, steps=1) as (seller, _job_id, handle):
        await assert_allowed("seller-self/Signal(nudge)", handle.signal(app.StreamJob.nudge))
        await assert_allowed(
            "seller-self/Query(stream offset)",
            handle.query(names.STREAM_OFFSET_QUERY, result_type=int),
        )
        update_id = "approve-" + uuid.uuid4().hex
        await assert_allowed(
            "seller-self/Update(approve)",
            handle.start_update(
                app.StreamJob.approve,
                "the seller",
                id=update_id,
                wait_for_stage=WorkflowUpdateStage.ACCEPTED,
            ),
        )
        # A fresh handle knows no outcome: result() asks PollWorkflowExecutionUpdate.
        outcome = await assert_allowed(
            "seller-self/PollWorkflowExecutionUpdate",
            handle.get_update_handle(update_id, result_type=str).result(),
        )
        assert outcome == "approved by the seller"
        assert await handle.result() == "approved"

        cancelled = await assert_allowed(
            "seller-self/SignalWithStart",
            seller.start_workflow(
                app.StreamJob.run,
                app.StreamJobInput(0),
                id=names.job_workflow_id(names.new_job_id()),
                task_queue=app.SELLER_TASK_QUEUE,
                start_signal="nudge",
            ),
        )
        await assert_allowed("seller-self/RequestCancel", cancelled.cancel())

        started = WithStartWorkflowOperation(
            app.StreamJob.run,
            app.StreamJobInput(0),
            id=names.job_workflow_id(names.new_job_id()),
            task_queue=app.SELLER_TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        )
        approved = await assert_allowed(
            "seller-self/ExecuteMultiOperation(update-with-start)",
            seller.execute_update_with_start_workflow(
                app.StreamJob.approve, "the seller", start_workflow_operation=started
            ),
        )
        assert approved == "approved by the seller"

        doomed = await seller.start_workflow(
            app.StreamJob.run,
            app.StreamJobInput(0),
            id=names.job_workflow_id(names.new_job_id()),
            task_queue=app.SELLER_TASK_QUEUE,
        )
        await assert_allowed("seller-self/Terminate", doomed.terminate("the seller stops it"))


# --- the denials escrow depends on ---------------------------------------------------------


async def test_escrow_guards_hold_over_the_wire(cluster: Cluster) -> None:
    """Every call that would change what escrow observes is refused to the party that
    would gain by it: a seller's reset (reviving a terminated pinned run), delete, batch
    (as Temporal's internal principal), schedule, pause, versioning override and Worker
    Versioning in its own namespace; a buyer's delete of its caller workflow or operation,
    reset, and retention change in its own (provenance is read from the buyer's records)."""
    seller = (await seller_client(cluster)).workflow_service
    job = wf(names.job_workflow_id(names.new_job_id()))
    seller_cells: dict[str, Any] = {
        "ResetWorkflowExecution": seller.reset_workflow_execution(
            ResetWorkflowExecutionRequest(
                namespace=SELLER,
                workflow_execution=job,
                reason="revive",
                workflow_task_finish_event_id=3,
                request_id=str(uuid.uuid4()),
                reset_reapply_type=ResetReapplyType.RESET_REAPPLY_TYPE_SIGNAL,
            )
        ),
        "DeleteWorkflowExecution": seller.delete_workflow_execution(
            DeleteWorkflowExecutionRequest(namespace=SELLER, workflow_execution=job)
        ),
        "StartBatchOperation(reset)": seller.start_batch_operation(
            StartBatchOperationRequest(
                namespace=SELLER,
                visibility_query="WorkflowType = 'StreamJob'",
                job_id=uuid.uuid4().hex,
                reason="revive every job",
                reset_operation=BatchOperationReset(),
            )
        ),
        "CreateSchedule": seller.create_schedule(
            CreateScheduleRequest(
                namespace=SELLER,
                schedule_id="every-minute",
                request_id=str(uuid.uuid4()),
                schedule=Schedule(
                    spec=ScheduleSpec(),
                    action=ScheduleAction(
                        start_workflow=NewWorkflowExecutionInfo(
                            workflow_id="scheduled",
                            workflow_type=WorkflowType(name="StreamJob"),
                            task_queue=TaskQueue(name=app.SELLER_TASK_QUEUE),
                        )
                    ),
                ),
            )
        ),
        "PauseWorkflowExecution": seller.pause_workflow_execution(
            PauseWorkflowExecutionRequest(
                namespace=SELLER,
                workflow_id=job.workflow_id,
                reason="hold",
                request_id=str(uuid.uuid4()),
            )
        ),
        "UpdateWorkflowExecutionOptions": seller.update_workflow_execution_options(
            UpdateWorkflowExecutionOptionsRequest(
                namespace=SELLER,
                workflow_execution=job,
                workflow_execution_options=WorkflowExecutionOptions(),
                update_mask=FieldMask(paths=["versioning_override"]),
            )
        ),
        "SetWorkerDeploymentCurrentVersion": seller.set_worker_deployment_current_version(
            SetWorkerDeploymentCurrentVersionRequest(
                namespace=SELLER, deployment_name=SELLER, build_id="b1"
            )
        ),
    }
    for name, call in seller_cells.items():
        await assert_denied(f"escrow-guards/acme:worker/{name}(acme)", call)

    buyer = (
        await connect_public(cluster, BUYER, api_key=cluster.signer.api_key(BUYER))
    ).workflow_service
    caller = wf(f"buyerflow-{uuid.uuid4().hex}")
    buyer_cells: dict[str, Any] = {
        "DeleteNexusOperationExecution": buyer.delete_nexus_operation_execution(
            DeleteNexusOperationExecutionRequest(namespace=BUYER, operation_id="job-op-1")
        ),
        "DeleteWorkflowExecution": buyer.delete_workflow_execution(
            DeleteWorkflowExecutionRequest(namespace=BUYER, workflow_execution=caller)
        ),
        "ResetWorkflowExecution": buyer.reset_workflow_execution(
            ResetWorkflowExecutionRequest(
                namespace=BUYER,
                workflow_execution=caller,
                reason="forget the call",
                workflow_task_finish_event_id=3,
                request_id=str(uuid.uuid4()),
            )
        ),
        "UpdateNamespace(retention)": buyer.update_namespace(
            UpdateNamespaceRequest(
                namespace=BUYER,
                config=NamespaceConfig(workflow_execution_retention_ttl=Duration(seconds=3600)),
            )
        ),
    }
    for name, call in buyer_cells.items():
        await assert_denied(f"escrow-guards/buyer:worker/{name}(buyer)", call)
