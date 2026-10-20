"""Seam ⑥ over the wire (decisions I32): the engine contract's isolation check (E18) and its
stream relay (E14) against :class:`~kontra.engine.temporal.TemporalEngine` on Kontra's own
Temporal server, the authorizer judging every credential the adapter mints.

The adapter reaches the INTERNAL frontend through :class:`kontra._control.Control`, as a gate
does; the seller's worker serves on the PUBLIC frontend with the worker token
:meth:`~kontra.engine.temporal.TemporalEngine.isolate` minted for it, and the relay reads the
job's stream there with the relay token the adapter mints inside ``stream`` (subject
``relay/<namespace>/job-<id>``, no permission, I30). So this proves, on the binary #59 built:

* the seller's credential reaches its own job's run; the buyer's and a stranger's neither
  reach it nor can terminate, request-cancel or fence it; no party's can provision a home or
  expose a route (E18);
* the relay credential reads exactly its job's stream, from offset 0 and from the tail (E14),
  and a job's relay outlives any one relay token (the adapter dials again with a fresh one);
* the handle, the bind and the member set hold here too (E1-E3).

Run with the rest of the wire suite (``make wire``; ``KONTRA_TEMPORAL_BIN`` names the server).
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import jwt
import pytest
import temporal_isolation_app as app
from conftest import Cluster, connect_public
from temporalio.api.common.v1 import Payload, WorkflowExecution, WorkflowType
from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import CreateNexusEndpointRequest
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    DescribeWorkflowExecutionRequest,
    RegisterNamespaceRequest,
    RequestCancelWorkflowExecutionRequest,
    StartWorkflowExecutionRequest,
    TerminateWorkflowExecutionRequest,
)
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker
from wire_ledger import record

from kontra._control import OCCUPY_TASK_QUEUE, OCCUPY_WORKFLOW_TYPE, Control, TemporalError
from kontra.codec import age
from kontra.contract.names import (
    OfferingId,
    job_workflow_id,
    new_endpoint_name,
    new_job_id,
    relay_subject,
)
from kontra.engine import (
    Engine,
    JobHandle,
    JobPlan,
    Pin,
    Sealed,
    SealedPayload,
    Stop,
    hold_digest,
)
from kontra.engine.temporal import (
    WORKER_TTL,
    TemporalEngine,
    _parse,  # pyright: ignore[reportPrivateUsage]
)
from kontra.escrow import ObservedStatus, Provenance
from kontra.testing.engine_contract import (
    LISTED_TOPIC,
    Behaviour,
    QuoteBehaviour,
    run_engine_contract,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "engine"))
import contract_flows
from engine_harness import open_text

pytestmark = pytest.mark.temporal


class WireSigner:
    """The gate's key ring as the cluster's JWKS publishes it."""

    issuer = "https://gate.wire"

    def __init__(self, cluster: Cluster) -> None:
        self._signer = cluster.signer

    def sign(self, claims: Mapping[str, Any]) -> str:
        return jwt.encode(
            dict(claims), self._signer.key, algorithm="ES256", headers={"kid": self._signer.kid}
        )


class DeadControl:
    def __getattr__(self, name: str) -> Any:
        if name == "client":
            raise AttributeError(name)

        async def unreachable(*args: Any, **kwargs: Any) -> Any:
            raise TemporalError(f"{name}: the frontend is unreachable")

        return unreachable


class WireHarness:
    enforces_isolation = True
    buyer_presence = True
    streams = True

    def __init__(self, cluster: Cluster, internal: Client) -> None:
        self.cluster = cluster
        self.internal = internal
        self.signer = WireSigner(cluster)
        self.identity, self.recipient = age.generate_identity()
        self.seller, self.buyer = app.SELLER_NAMESPACE, app.BUYER_NAMESPACE
        self.stranger = app.OTHER_NAMESPACE
        self.offering = OfferingId.parse(f"{self.seller}/contract@1.0.0")
        self.other_offering = OfferingId.parse(f"{self.seller}/contract-other@1.0.0")
        self.route = new_endpoint_name()
        #: Every relay token the adapter dialled the public frontend with.
        self.relay_tokens: list[str] = []
        self._engine = self._new(Control(internal, "http://127.0.0.1:9"))

    def _new(self, control: Any, *, worker_ttl: timedelta = WORKER_TTL) -> TemporalEngine:
        async def relay_service(token: str) -> Any:
            self.relay_tokens.append(token)
            client = await connect_public(self.cluster, self.seller, api_key=token)
            return client.workflow_service

        return TemporalEngine(
            control,
            signer=self.signer,
            address=self.cluster.public_address,
            relay_service=relay_service,
            relay_interval=0.2,
            worker_ttl=worker_ttl,
        )

    async def worker(self) -> Worker:
        """The seller's worker, on the public frontend with the token isolate minted."""
        iso = await self._engine.isolate(self.seller, session="seller-worker")
        assert iso.credential is not None
        client = await connect_public(self.cluster, self.seller, api_key=iso.credential)
        return Worker(
            client,
            task_queue=self.offering.task_queue,
            workflows=[contract_flows.ContractJob],
            activities=[contract_flows.seal, contract_flows.seal_items],
            nexus_service_handlers=[contract_flows.make_handler(self.offering.service_name)],
        )

    @property
    def engine(self) -> Engine:
        return self._engine

    def _payload(self, value: object) -> bytes:
        return Payload(
            metadata={"encoding": b"json/plain"}, data=json.dumps(value).encode()
        ).SerializeToString()

    def input(self, behaviour: Behaviour, marker: str) -> bytes:
        return self._payload({"b": behaviour, "m": marker, "to": self.recipient})

    def quote_input(self, behaviour: QuoteBehaviour) -> bytes:
        return self._payload({"q": behaviour})

    def quote_document(self) -> bytes:
        return contract_flows.QUOTE_DOCUMENT

    def hold(self, job_id: str) -> str:
        return f"testhold.{job_id}.sig"

    def open(self, result: SealedPayload) -> str:
        return open_text(result, self.identity)

    def open_item(self, item: Sealed) -> str:
        return age.decrypt(item.data, [self.identity]).decode()

    async def seller_start(
        self, handle: JobHandle, job_id: str, marker: str, behaviour: Behaviour = "ok"
    ) -> None:
        raise AssertionError("not run over the wire")

    async def seller_revive(self, handle: JobHandle, job_id: str) -> None:
        raise AssertionError("not run over the wire")

    async def buyer_withdraw(self, handle: JobHandle, job_id: str) -> None:
        raise AssertionError("not run over the wire")

    async def reach(self, credential: str, handle: JobHandle) -> bool:
        """Can ``credential`` read the job's run? (The adapter's own rig reads its handle.)"""
        job = _parse(handle).job
        who = await connect_public(self.cluster, self.seller, api_key=credential)
        cell = f"engine/reach/{_party_of(credential)}/DescribeWorkflowExecution(job)"
        try:
            await who.workflow_service.describe_workflow_execution(
                DescribeWorkflowExecutionRequest(
                    namespace=self.seller,
                    execution=WorkflowExecution(workflow_id=job_workflow_id(job)),
                )
            )
        except RPCError as e:
            if e.status == RPCStatusCode.PERMISSION_DENIED:
                record(cell, "probe", "PERMISSION_DENIED")
                return False
            raise
        record(cell, "probe", "OK")
        return True

    async def stops(self, credential: str, handle: JobHandle) -> bool:
        """Can ``credential`` stop the job's run -- terminate it, ask it to stop -- or start
        under its id, as the gate's fence does (``Control.occupy_job``)? A start refused only
        because the id is taken was authorized: that is power too."""
        job = _parse(handle).job
        who = await connect_public(self.cluster, self.seller, api_key=credential)
        party = _party_of(credential)
        execution = WorkflowExecution(workflow_id=job_workflow_id(job))
        attempts: dict[str, Callable[[], Awaitable[object]]] = {
            "TerminateWorkflowExecution": lambda: who.workflow_service.terminate_workflow_execution(
                TerminateWorkflowExecutionRequest(
                    namespace=self.seller, workflow_execution=execution, reason="a probe"
                )
            ),
            "RequestCancelWorkflowExecution": lambda: (
                who.workflow_service.request_cancel_workflow_execution(
                    RequestCancelWorkflowExecutionRequest(
                        namespace=self.seller,
                        workflow_execution=execution,
                        reason="a probe",
                        request_id=str(uuid.uuid4()),
                    )
                )
            ),
            "StartWorkflowExecution": lambda: who.workflow_service.start_workflow_execution(
                StartWorkflowExecutionRequest(
                    namespace=self.seller,
                    workflow_id=job_workflow_id(job),
                    workflow_type=WorkflowType(name=OCCUPY_WORKFLOW_TYPE),
                    task_queue=TaskQueue(name=OCCUPY_TASK_QUEUE),
                    request_id=str(uuid.uuid4()),
                )
            ),
        }
        for name, call in attempts.items():
            cell = f"engine/stop/{party}/{name}(job)"
            try:
                await call()
            except RPCError as e:
                if e.status == RPCStatusCode.ALREADY_EXISTS:
                    record(cell, "deny", "ALLOWED (the id was taken)")
                    return True
                if e.status != RPCStatusCode.PERMISSION_DENIED:
                    raise
                record(cell, "deny", "PERMISSION_DENIED")
                continue
            record(cell, "deny", "ALLOWED")
            return True
        return False

    async def controls(self, credential: str) -> bool:
        """Can ``credential`` do what only the gate may: provision a home (register a
        namespace) or expose a route (create a Nexus endpoint)?"""
        who = await connect_public(self.cluster, self.seller, api_key=credential)
        party = _party_of(credential)
        attempts = {
            "RegisterNamespace": who.workflow_service.register_namespace(
                RegisterNamespaceRequest(namespace=f"{party}-annex")
            ),
            "CreateNexusEndpoint": who.operator_service.create_nexus_endpoint(
                CreateNexusEndpointRequest(
                    spec=EndpointSpec(
                        name=new_endpoint_name(),
                        target=EndpointTarget(
                            worker=EndpointTarget.Worker(
                                namespace=self.seller, task_queue=self.offering.task_queue
                            )
                        ),
                    )
                )
            ),
        }
        for name, call in attempts.items():
            try:
                await call
            except RPCError as e:
                if e.status != RPCStatusCode.PERMISSION_DENIED:
                    raise
                record(f"engine/control/{party}/{name}", "deny", "PERMISSION_DENIED")
                continue
            record(f"engine/control/{party}/{name}", "deny", "ALLOWED")
            return True
        return False

    async def fresh_engine(self) -> Engine:
        return self._new(Control(self.internal, "http://127.0.0.1:9"))

    async def unreachable_engine(self) -> Engine:
        return self._new(DeadControl())


def _party_of(credential: str) -> str:
    return str(jwt.decode(credential, options={"verify_signature": False})["sub"])


@pytest.mark.timeout(300)
async def test_the_adapters_credentials_hold_over_the_wire(cluster: Cluster) -> None:
    internal = await Client.connect(cluster.internal_address, namespace="temporal-system")
    harness = WireHarness(cluster, internal)
    await harness.engine.isolate(harness.seller)
    await harness.engine.isolate(harness.buyer)
    await harness.engine.expose(harness.offering, harness.route)
    async with await harness.worker():

        async def make() -> WireHarness:
            return harness

        report = await run_engine_contract(make, only={"E1", "E2", "E3", "E14", "E18"})
    assert [line.split()[0] for line in report] == ["E1", "E2", "E3", "E14", "E18"]
    assert all(" ok: " in line for line in report), report
    for line in report:
        record(f"engine/contract/{line.split()[0]}", "pass", "OK")


@pytest.mark.timeout(300)
async def test_a_jobs_relay_outlives_its_token(cluster: Cluster) -> None:
    """A relay token lives ``worker_ttl`` (decisions I30) and the frontend refuses it after;
    a job's stream lasts as long as the job. With a 5-second token, a stream whose items
    come after the first token expired is relayed to its end: the adapter dialled again with
    a fresh token (the same job's subject and session), never a refused one."""
    internal = await Client.connect(cluster.internal_address, namespace="temporal-system")
    harness = WireHarness(cluster, internal)
    ttl = timedelta(seconds=5)
    short = harness._new(Control(internal, "http://127.0.0.1:9"), worker_ttl=ttl)  # pyright: ignore[reportPrivateUsage]
    await short.isolate(harness.seller)
    await short.isolate(harness.buyer)
    await short.expose(harness.offering, harness.route)
    async with await harness.worker():
        job_id = new_job_id()
        handle = short.bind(
            JobPlan(job_id, harness.offering, harness.route, harness.buyer, harness.seller)
        )
        holds = frozenset({hold_digest(harness.hold(job_id))})
        await short.start(
            handle,
            harness.input("publish", "MARKER-relay"),
            hold=harness.hold(job_id),
            deadline=datetime.now(UTC) + timedelta(minutes=10),
        )
        pin: Pin | None = None
        async with asyncio.timeout(30):
            while pin is None:
                o = (await short.observe(handle, pin=None, holds=holds)).observation
                if o.status is ObservedStatus.RUNNING and o.provenance is Provenance.VERIFIED:
                    pin = Pin(str(o.pin))
                await asyncio.sleep(0.1)

        async def closed() -> bool:
            seen = await short.observe(handle, pin=pin, holds=holds)
            return seen.observation.status is not ObservedStatus.RUNNING

        async def ask_later() -> None:
            # Past the first token's life: the rest is published, then the job completes,
            # and the relay's next calls are made after that token expired.
            await asyncio.sleep(ttl.total_seconds() + 2)
            await short.cancel(handle, stop=Stop.ASK, pin=pin, reason="the rest")

        offsets: list[int] = []
        asker = asyncio.create_task(ask_later())
        loop = asyncio.get_running_loop()
        began = loop.time()
        try:
            async with asyncio.timeout(120):
                async for batch in short.stream(
                    handle,
                    pin=pin,
                    topics=[LISTED_TOPIC],
                    after=0,
                    closed=closed,
                    session="relay-s",
                ):
                    offsets.extend(i.offset for i in batch.items)
        finally:
            asker.cancel()
        took = loop.time() - began
    assert offsets == [0, 2, 3, 5]
    assert took > ttl.total_seconds()
    claims = [jwt.decode(t, options={"verify_signature": False}) for t in harness.relay_tokens]
    assert len(claims) >= 2, claims
    assert {c["sub"] for c in claims} == {relay_subject(harness.seller, job_id)}
    assert {c["sid"] for c in claims} == {"relay-s"}
    record("engine/relay/token-renewed", "pass", f"{len(claims)} tokens")
