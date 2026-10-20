"""The §10.4 deny matrix and the positive hiring path, over the wire (seam ①).

Every denied cell must come back ``PERMISSION_DENIED`` from the PUBLIC frontend; the
control plane (the INTERNAL frontend) must be able to do the operator calls a token
cannot; and a real seller/buyer Nexus round trip must succeed. The outcomes are collected
into one table (``wire_ledger``) and printed at the end of the session. The stream relay,
the worker controller, seller self-access and the denials escrow depends on are in
``test_streams_and_platform_wire.py``.

The deny cells here mirror the Go deny matrix (``temporal/authorizer/authorizer_test.go``),
which proves the same cells fail-by-default against Temporal's own authorizers. This file
proves the policy as the server actually enforces it.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, timedelta
from typing import Any

import pytest
import temporal_isolation_app as app
from conftest import Cluster, connect_public
from temporalio.api.common.v1 import Callback, Payloads, WorkflowExecution, WorkflowType
from temporalio.api.enums.v1 import DescribeTaskQueueMode, TaskQueueType
from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import (
    CreateNexusEndpointRequest,
    DeleteNexusEndpointRequest,
    GetNexusEndpointRequest,
    ListNexusEndpointsRequest,
    UpdateNexusEndpointRequest,
)
from temporalio.api.query.v1 import WorkflowQuery
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.update.v1 import Input as UpdateInput
from temporalio.api.update.v1 import Meta as UpdateMeta
from temporalio.api.update.v1 import Request as UpdateRequest
from temporalio.api.workflowservice.v1 import (
    DescribeNamespaceRequest,
    DescribeTaskQueueRequest,
    DescribeWorkflowExecutionRequest,
    GetWorkflowExecutionHistoryRequest,
    ListNamespacesRequest,
    ListWorkflowExecutionsRequest,
    PollActivityTaskQueueRequest,
    PollWorkflowTaskQueueRequest,
    QueryWorkflowRequest,
    StartWorkflowExecutionRequest,
    UpdateWorkflowExecutionRequest,
)
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker
from wire_ledger import assert_allowed, assert_denied, assert_refused, record

pytestmark = pytest.mark.temporal


# --- no token / broken tokens ----------------------------------------------------------


async def test_no_token_is_denied_everything_but_health(cluster: Cluster) -> None:
    client = await connect_public(cluster, app.SELLER_NAMESPACE)  # no api_key
    # GetSystemInfo is how the client connects at all, so reaching here proves it is allowed.
    record("no-token/GetSystemInfo", "allow", "OK")
    await assert_denied(
        "no-token/DescribeNamespace",
        client.workflow_service.describe_namespace(
            DescribeNamespaceRequest(namespace=app.SELLER_NAMESPACE)
        ),
    )
    await assert_denied(
        "no-token/ListNexusEndpoints",
        client.operator_service.list_nexus_endpoints(ListNexusEndpointsRequest()),
    )
    await assert_denied(
        "no-token/StartWorkflow(poll)",
        client.workflow_service.poll_workflow_task_queue(
            PollWorkflowTaskQueueRequest(
                namespace=app.SELLER_NAMESPACE, task_queue=TaskQueue(name="x")
            )
        ),
    )


@pytest.mark.parametrize(
    ("cell", "make_token"),
    [
        ("expired", lambda s: s.api_key(app.SELLER_NAMESPACE, exp=_past())),
        ("unknown-signing-key", lambda s: s.api_key(app.SELLER_NAMESPACE, kid="not-in-jwks")),
        ("alg-none", lambda s: _alg_none(s)),
        ("hs256-with-public-key", lambda s: _hs256(s)),
        ("wrong-audience", lambda s: s.api_key(app.SELLER_NAMESPACE, audience="someone-else")),
        ("gate-api-token", lambda s: s.api_key(app.SELLER_NAMESPACE, audience="kontra:api")),
        ("missing-audience", lambda s: s.api_key(app.SELLER_NAMESPACE, audience=None)),
    ],
)
async def test_broken_tokens_are_denied(cluster: Cluster, cell: str, make_token: Any) -> None:
    # Connect without a token (GetSystemInfo is public) and attach the broken token as the
    # authorization header on one call, so the denial arrives as a per-request
    # PERMISSION_DENIED. A malformed token is rejected at the claim-mapping stage, before the
    # authorizer runs at all.
    token = make_token(cluster.signer)
    client = await connect_public(cluster, app.SELLER_NAMESPACE)
    await assert_denied(
        f"broken-token/{cell}",
        client.workflow_service.describe_namespace(
            DescribeNamespaceRequest(namespace=app.SELLER_NAMESPACE),
            metadata={"authorization": f"Bearer {token}"},
        ),
    )


async def test_a_revoked_sessions_worker_token_is_refused(cluster: Cluster) -> None:
    """The review's F3: the gate revoked a key (or a login), and its worker token -- valid by
    signature, expiry and audience, all Temporal's claim mapper checks -- used to keep
    polling the party's task queues and starting work for up to 15 minutes. Kontra's server
    polls the gate's revocation list and refuses it within a refresh."""
    token = cluster.signer.api_key(app.SELLER_NAMESPACE, sid="stolen-key-session")
    other = cluster.signer.api_key(app.SELLER_NAMESPACE, sid="the-owners-login")
    client = await connect_public(cluster, app.SELLER_NAMESPACE)

    def describe(t: str) -> Any:
        return client.workflow_service.describe_namespace(
            DescribeNamespaceRequest(namespace=app.SELLER_NAMESPACE),
            metadata={"authorization": f"Bearer {t}"},
        )

    await assert_allowed("revocation/live-session", describe(token))
    cluster.jwks.revoke("stolen-key-session")
    deadline = asyncio.get_running_loop().time() + 20
    while True:  # the server reads the list every second
        try:
            await describe(token)
        except RPCError as e:
            if e.status == RPCStatusCode.PERMISSION_DENIED:
                break
            raise
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail("a revoked session's worker token is still accepted")
        await asyncio.sleep(0.5)
    record("revocation/revoked-session", "deny", "PERMISSION_DENIED")
    await assert_denied(
        "revocation/revoked-session-poll",
        client.workflow_service.poll_activity_task_queue(
            PollActivityTaskQueueRequest(
                namespace=app.SELLER_NAMESPACE, task_queue=TaskQueue(name=app.SELLER_TASK_QUEUE)
            ),
            metadata={"authorization": f"Bearer {token}"},
        ),
    )
    await assert_allowed("revocation/other-session", describe(other))


async def test_a_token_naming_no_session_is_refused(cluster: Cluster) -> None:
    """Every token the gate mints names its session; one that does not cannot be checked
    against the revocation list, so it is refused."""
    token = cluster.signer.api_key(app.SELLER_NAMESPACE, sid=None)
    client = await connect_public(cluster, app.SELLER_NAMESPACE)
    await assert_denied(
        "revocation/no-session",
        client.workflow_service.describe_namespace(
            DescribeNamespaceRequest(namespace=app.SELLER_NAMESPACE),
            metadata={"authorization": f"Bearer {token}"},
        ),
    )


def _past() -> Any:
    from datetime import datetime, timedelta

    return datetime.now(UTC) - timedelta(minutes=1)


def _alg_none(signer: Any) -> str:
    import jwt

    return jwt.encode(
        {"sub": "x", "permissions": [f"{app.SELLER_NAMESPACE}:worker"], "aud": "kontra:temporal"},
        key=None,
        algorithm="none",
    )


def _hs256(signer: Any) -> str:
    """The classic algorithm-confusion attack: an HS256 token whose HMAC secret is the
    server's PUBLIC key (PEM). The server only accepts RS256/ES256, so it must reject it.
    pyjwt refuses to sign HS256 with an asymmetric key, so the token is built by hand."""
    import base64
    import hashlib
    import hmac
    import json

    from cryptography.hazmat.primitives import serialization

    public_pem = signer.key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )

    def b64(raw: bytes) -> bytes:
        return base64.urlsafe_b64encode(raw).rstrip(b"=")

    header = b64(json.dumps({"alg": "HS256", "typ": "JWT", "kid": signer.kid}).encode())
    payload = b64(
        json.dumps(
            {
                "sub": "x",
                "permissions": [f"{app.SELLER_NAMESPACE}:worker"],
                "aud": "kontra:temporal",
            }
        ).encode()
    )
    signing_input = header + b"." + payload
    sig = b64(hmac.new(public_pem, signing_input, hashlib.sha256).digest())
    return (signing_input + b"." + sig).decode()


# --- operator / admin APIs are denied every token --------------------------------------


@pytest.mark.parametrize(
    ("cell", "permissions"),
    [
        ("temporal-system-admin", ["temporal-system:admin"]),
        ("acme-admin", [f"{app.SELLER_NAMESPACE}:admin"]),
        ("acme-worker", [f"{app.SELLER_NAMESPACE}:worker"]),
    ],
)
async def test_operator_endpoint_apis_denied_every_token(
    cluster: Cluster, cell: str, permissions: list[str]
) -> None:
    token = cluster.signer.jwt(permissions, subject=cell)
    client = await connect_public(cluster, app.SELLER_NAMESPACE, api_key=token)
    ops = client.operator_service
    await assert_denied(
        f"operator/{cell}/CreateNexusEndpoint",
        ops.create_nexus_endpoint(
            CreateNexusEndpointRequest(
                spec=EndpointSpec(
                    name="k" + uuid.uuid4().hex,
                    target=EndpointTarget(
                        worker=EndpointTarget.Worker(
                            namespace=app.SELLER_NAMESPACE, task_queue=app.SELLER_TASK_QUEUE
                        )
                    ),
                )
            )
        ),
    )
    await assert_denied(
        f"operator/{cell}/UpdateNexusEndpoint",
        ops.update_nexus_endpoint(UpdateNexusEndpointRequest(id=cluster.endpoint_id, version=1)),
    )
    await assert_denied(
        f"operator/{cell}/DeleteNexusEndpoint",
        ops.delete_nexus_endpoint(DeleteNexusEndpointRequest(id=cluster.endpoint_id, version=1)),
    )
    await assert_denied(
        f"operator/{cell}/GetNexusEndpoint",
        ops.get_nexus_endpoint(GetNexusEndpointRequest(id=cluster.endpoint_id)),
    )
    await assert_denied(
        f"operator/{cell}/ListNexusEndpoints",
        ops.list_nexus_endpoints(ListNexusEndpointsRequest()),
    )


async def test_system_admin_token_cannot_administer_namespaces(cluster: Cluster) -> None:
    token = cluster.signer.jwt(["temporal-system:admin"], subject="operator")
    client = await connect_public(cluster, app.SELLER_NAMESPACE, api_key=token)
    await assert_denied(
        "operator/temporal-system-admin/ListNamespaces",
        client.workflow_service.list_namespaces(ListNamespacesRequest()),
    )


# --- cross-namespace reads: a buyer cannot see the seller's handler workflow ------------


async def test_buyer_cannot_read_sellers_namespace(cluster: Cluster, control: Client) -> None:
    buyer_token = cluster.signer.api_key(app.BUYER_NAMESPACE)
    buyer = await connect_public(cluster, app.SELLER_NAMESPACE, api_key=buyer_token)
    ws = buyer.workflow_service
    wf = WorkflowExecution(workflow_id="job-deadbeef")
    await assert_denied(
        "cross-ns/buyer/DescribeWorkflow(acme)",
        ws.describe_workflow_execution(
            DescribeWorkflowExecutionRequest(namespace=app.SELLER_NAMESPACE, execution=wf)
        ),
    )
    await assert_denied(
        "cross-ns/buyer/GetHistory(acme)",
        ws.get_workflow_execution_history(
            GetWorkflowExecutionHistoryRequest(namespace=app.SELLER_NAMESPACE, execution=wf)
        ),
    )
    await assert_denied(
        "cross-ns/buyer/Query(acme)",
        ws.query_workflow(
            QueryWorkflowRequest(
                namespace=app.SELLER_NAMESPACE,
                execution=wf,
                query=WorkflowQuery(query_type="progress"),
            )
        ),
    )
    await assert_denied(
        "cross-ns/buyer/ListWorkflows(acme)",
        ws.list_workflow_executions(
            ListWorkflowExecutionsRequest(namespace=app.SELLER_NAMESPACE, query="")
        ),
    )
    await assert_denied(
        "cross-ns/buyer/DescribeNamespace(acme)",
        ws.describe_namespace(DescribeNamespaceRequest(namespace=app.SELLER_NAMESPACE)),
    )


async def test_worker_of_c_cannot_poll_a(cluster: Cluster) -> None:
    other_token = cluster.signer.api_key(app.OTHER_NAMESPACE)
    other = await connect_public(cluster, app.SELLER_NAMESPACE, api_key=other_token)
    ws = other.workflow_service
    await assert_denied(
        "cross-ns/other/PollWorkflowTaskQueue(acme)",
        ws.poll_workflow_task_queue(
            PollWorkflowTaskQueueRequest(
                namespace=app.SELLER_NAMESPACE, task_queue=TaskQueue(name=app.SELLER_TASK_QUEUE)
            )
        ),
    )
    await assert_denied(
        "cross-ns/other/PollActivityTaskQueue(acme)",
        ws.poll_activity_task_queue(
            PollActivityTaskQueueRequest(
                namespace=app.SELLER_NAMESPACE, task_queue=TaskQueue(name=app.SELLER_TASK_QUEUE)
            )
        ),
    )


# --- the control plane can do what a token cannot --------------------------------------


async def test_internal_frontend_allows_operator_calls_without_token(control: Client) -> None:
    await assert_allowed(
        "control-plane/ListNexusEndpoints",
        control.operator_service.list_nexus_endpoints(ListNexusEndpointsRequest()),
    )
    await assert_allowed(
        "control-plane/ListNamespaces",
        control.workflow_service.list_namespaces(ListNamespacesRequest()),
    )


# --- the positive hiring path ----------------------------------------------------------


async def test_seller_worker_token_runs_in_its_own_namespace(cluster: Cluster) -> None:
    """A seller worker JWT (acme:worker) can poll, start and describe in acme."""
    token = cluster.signer.api_key(app.SELLER_NAMESPACE)
    seller = await connect_public(cluster, app.SELLER_NAMESPACE, api_key=token)
    async with Worker(
        seller,
        task_queue=app.SELLER_TASK_QUEUE,
        workflows=[app.EnrichJob],
        nexus_service_handlers=[app.EnrichHandler()],
    ):
        handle = await seller.start_workflow(
            app.EnrichJob.run,
            app.JobInput(5, "direct", False),
            id=f"job-{uuid.uuid4().hex}",
            task_queue=app.SELLER_TASK_QUEUE,
        )
        record("positive/seller/StartWorkflow", "allow", "OK")
        out = await handle.result()
        assert out.n == 10
        record("positive/seller/result", "allow", f"n={out.n}")


async def test_seller_describes_its_own_task_queue_backlog(cluster: Cluster) -> None:
    """A seller worker JWT reads its own backlog, and only its own (issue #9).

    This is what ``temporal task-queue describe`` and KEDA's Temporal scaler call. The
    response is metadata -- counts, ages and rates -- and the poller identities of the
    seller's own workers; no payload crosses it.
    """
    token = cluster.signer.api_key(app.SELLER_NAMESPACE)
    seller = await connect_public(cluster, app.SELLER_NAMESPACE, api_key=token)

    def request(namespace: str) -> DescribeTaskQueueRequest:
        return DescribeTaskQueueRequest(
            namespace=namespace,
            task_queue=TaskQueue(name=app.SELLER_TASK_QUEUE),
            api_mode=DescribeTaskQueueMode.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
            task_queue_types=[
                TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
                TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY,
            ],
            report_stats=True,
            report_pollers=True,
        )

    async with Worker(
        seller,
        task_queue=app.SELLER_TASK_QUEUE,
        workflows=[app.EnrichJob],
        nexus_service_handlers=[app.EnrichHandler()],
    ):
        response = await assert_allowed(
            "worker-A/acme:worker/DescribeTaskQueue(acme)",
            seller.workflow_service.describe_task_queue(request(app.SELLER_NAMESPACE)),
        )
        # report_stats is the half KEDA scales on; it must come back populated, not empty.
        versions = list(response.versions_info.values())
        assert versions, "enhanced describe returned no version info"
        by_type = versions[0].types_info
        assert TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW in by_type
        stats = by_type[TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW].stats
        assert stats.approximate_backlog_count >= 0
        record(
            "positive/seller/DescribeTaskQueue(stats)",
            "allow",
            f"backlog={stats.approximate_backlog_count}",
        )

    # The same token, the same queue name, another namespace: the backlog is the token's own.
    for namespace in (app.BUYER_NAMESPACE, app.OTHER_NAMESPACE):
        await assert_denied(
            f"cross-ns/acme:worker/DescribeTaskQueue({namespace})",
            seller.workflow_service.describe_task_queue(request(namespace)),
        )


async def test_standalone_nexus_hire_through_public_frontend(cluster: Cluster) -> None:
    """A buyer with only its own worker JWT runs a STANDALONE Nexus operation against the
    seller's endpoint on the public frontend: sync quote and a workflow-run execute, with
    the kontra-hold header carried through to the handler (decisions I4)."""
    seller_token = cluster.signer.api_key(app.SELLER_NAMESPACE)
    buyer_token = cluster.signer.api_key(app.BUYER_NAMESPACE)
    seller = await connect_public(cluster, app.SELLER_NAMESPACE, api_key=seller_token)
    buyer = await connect_public(cluster, app.BUYER_NAMESPACE, api_key=buyer_token)
    async with Worker(
        seller,
        task_queue=app.SELLER_TASK_QUEUE,
        workflows=[app.EnrichJob],
        nexus_service_handlers=[app.EnrichHandler()],
    ):
        nexus = buyer.create_nexus_client(service=app.EnrichService, endpoint=cluster.endpoint_name)
        quote = await assert_allowed(
            "positive/buyer/StartNexusOperation(quote)",
            nexus.start_operation(
                app.EnrichService.quote,
                app.EnrichInput(3),
                id=f"q-{uuid.uuid4().hex}",
                headers={"kontra-hold": "hold-q"},
                schedule_to_close_timeout=timedelta(minutes=2),
            ),
        )
        qres = await quote.result()
        assert qres.n == 30
        assert qres.hold == "hold-q"
        record("positive/buyer/quote.result", "allow", f"n={qres.n} hold={qres.hold}")

        op = await assert_allowed(
            "positive/buyer/StartNexusOperation(execute)",
            nexus.start_operation(
                app.EnrichService.execute,
                app.EnrichInput(6),
                id=f"job-op-{uuid.uuid4().hex}",
                headers={"kontra-hold": "hold-x"},
                schedule_to_close_timeout=timedelta(minutes=2),
            ),
        )
        res = await op.result()
        assert res.n == 12
        assert res.hold == "hold-x"
        record("positive/buyer/execute.result", "allow", f"n={res.n} hold={res.hold}")
        # The buyer can describe its OWN standalone operation...
        await assert_allowed("positive/buyer/DescribeNexusOperation", op.describe())
        # ...but still cannot reach the seller's handler workflow it just started.
        handler_wf = WorkflowExecution(workflow_id=res.handler_workflow_id)
        await assert_denied(
            "cross-ns/buyer/DescribeHandlerWorkflow(acme)",
            buyer.workflow_service.describe_workflow_execution(
                DescribeWorkflowExecutionRequest(
                    namespace=app.SELLER_NAMESPACE, execution=handler_wf
                )
            ),
        )


async def test_buyer_workflow_calls_endpoint(cluster: Cluster) -> None:
    """A buyer workflow hires via workflow.create_nexus_client (PRD §7.2)."""
    seller_token = cluster.signer.api_key(app.SELLER_NAMESPACE)
    buyer_token = cluster.signer.api_key(app.BUYER_NAMESPACE)
    seller = await connect_public(cluster, app.SELLER_NAMESPACE, api_key=seller_token)
    buyer = await connect_public(cluster, app.BUYER_NAMESPACE, api_key=buyer_token)
    async with (
        Worker(
            seller,
            task_queue=app.SELLER_TASK_QUEUE,
            workflows=[app.EnrichJob],
            nexus_service_handlers=[app.EnrichHandler()],
        ),
        Worker(buyer, task_queue=app.BUYER_TASK_QUEUE, workflows=[app.BuyerFlow]),
    ):
        out = await assert_allowed(
            "positive/buyer-workflow/execute",
            buyer.execute_workflow(
                app.BuyerFlow.run,
                args=[cluster.endpoint_name, 4],
                id=f"buyerflow-{uuid.uuid4().hex}",
                task_queue=app.BUYER_TASK_QUEUE,
            ),
        )
        quote, result = out
        assert quote.n == 40
        assert result.n == 8
        record("positive/buyer-workflow/result", "allow", f"quote={quote.n} result={result.n}")


# --- forged Nexus completions: one seller completing another seller's operation ---------
#
# Every Nexus handler is given the callback token of the operation it serves. The token is
# unsigned and names the caller's namespace, workflow, run and operation; history accepts
# a completion whose token has no request id without comparing it to the operation's.
# So a seller hired by the same buyer workflow as acme could rewrite its own token to
# point at acme's operation, drop the request id, attach it to a workflow it starts in
# its own namespace with a temporal://system callback, and deliver any result as acme's.
# The frontend must refuse such a token before it is ever stored.


async def _start_with_callback(client: Client, token: str, marker: str) -> None:
    """Start ForgedCompletion in the forger's own namespace, carrying ``token`` as a
    temporal://system completion callback -- what a handler does with its genuine token."""
    payloads = await client.data_converter.encode([marker])
    await client.workflow_service.start_workflow_execution(
        StartWorkflowExecutionRequest(
            namespace=app.OTHER_NAMESPACE,
            workflow_id=f"forge-{uuid.uuid4().hex}",
            workflow_type=WorkflowType(name="ForgedCompletion"),
            task_queue=TaskQueue(name=app.OTHER_TASK_QUEUE),
            input=Payloads(payloads=payloads),
            request_id=str(uuid.uuid4()),
            completion_callbacks=[
                Callback(
                    nexus=Callback.Nexus(
                        url="temporal://system", header={"Temporal-Callback-Token": token}
                    )
                )
            ],
        )
    )


async def _until(predicate: Any) -> None:
    """Wait up to 30 s for something a handler records (the workers run in this process)."""
    for _ in range(300):
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise TimeoutError("condition not reached")


async def _try_forge(cell: str, client: Client, token: str, marker: str) -> bool:
    """True if the frontend accepted the forged callback (the hole), recording the cell."""
    try:
        await _start_with_callback(client, token, marker)
    except RPCError as e:
        record(cell, "deny", e.status.name)
        return False
    record(cell, "deny", "ACCEPTED")
    return True


async def test_only_system_callbacks_with_a_request_id(cluster: Cluster) -> None:
    """A completion callback is an HTTP request Temporal itself makes. Any other shape than
    a handler's (temporal://system + one genuine token) is refused, on every API that can
    carry one -- including UpdateWorkflowExecution, whose callbacks Temporal v1.32.1 does
    not validate at all."""
    other = await connect_public(
        cluster, app.OTHER_NAMESPACE, api_key=cluster.signer.api_key(app.OTHER_NAMESPACE)
    )
    internal_http = "http://127.0.0.1:1/api/v1/namespaces/acme/workflows/x/terminate"
    for name, cb in {
        "internal-frontend-http-api": Callback(nexus=Callback.Nexus(url=internal_http)),
        "temporal-internal": Callback(nexus=Callback.Nexus(url="temporal://internal")),
        "system-without-token": Callback(nexus=Callback.Nexus(url="temporal://system")),
    }.items():
        await assert_denied(
            f"callback-shape/StartWorkflowExecution({name})",
            other.workflow_service.start_workflow_execution(
                StartWorkflowExecutionRequest(
                    namespace=app.OTHER_NAMESPACE,
                    workflow_id=f"shape-{uuid.uuid4().hex}",
                    workflow_type=WorkflowType(name="ForgedCompletion"),
                    task_queue=TaskQueue(name=app.OTHER_TASK_QUEUE),
                    request_id=str(uuid.uuid4()),
                    completion_callbacks=[cb],
                )
            ),
        )
    await assert_denied(
        "callback-shape/UpdateWorkflowExecution(internal-frontend-http-api)",
        other.workflow_service.update_workflow_execution(
            UpdateWorkflowExecutionRequest(
                namespace=app.OTHER_NAMESPACE,
                workflow_execution=WorkflowExecution(workflow_id="any"),
                request=UpdateRequest(
                    meta=UpdateMeta(update_id="u"),
                    input=UpdateInput(name="u"),
                    completion_callbacks=[Callback(nexus=Callback.Nexus(url=internal_http))],
                ),
            )
        ),
    )


@pytest.mark.parametrize(
    "header", ["Nexus-Callback-Temporal-Callback-Token", "Nexus-Request-Id", "Nexus-Link"]
)
async def test_callers_cannot_set_nexus_transport_headers(cluster: Cluster, header: str) -> None:
    """Temporal sets a caller's Nexus headers after its own on the request to the handler,
    so without the dynamic config's disallowed list a buyer could hand a seller's handler
    a callback token, request id or links of its choosing."""
    buyer = await connect_public(
        cluster, app.BUYER_NAMESPACE, api_key=cluster.signer.api_key(app.BUYER_NAMESPACE)
    )
    await assert_refused(
        f"nexus-header/buyer/StartNexusOperation({header})",
        RPCStatusCode.INVALID_ARGUMENT,
        buyer.create_nexus_client(
            service=app.ProbeService, endpoint=cluster.other_endpoint_name
        ).start_operation(
            app.ProbeService.probe,
            app.ProbeInput("p"),
            id=f"inject-{uuid.uuid4().hex}",
            headers={header: "chosen-by-the-buyer"},
            schedule_to_close_timeout=timedelta(minutes=1),
        ),
    )
