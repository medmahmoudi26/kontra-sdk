"""The gate's stream relay loop over the wire (seam ①, decisions I30/I31): ``kontra._relay``,
on a client minted the way the gate mints a relay's token (``names.relay_subject``, no
permission), reads a job's stream to its end with only ``UpdateWorkflowExecution`` and
``QueryWorkflow`` -- the two calls the Authorizer allows it -- against the real server binary.

The seller's worker runs Kontra's interceptor and a seller ``KontraCodec`` (a fixed buyer
key), so every poll's answer is the stream envelope: the relay reads the listed topic's items
in the clear, each sealed to the job's buyer and the seller, and drops the unlisted topic's,
whose names it never sees. No authorizer cell is added: these are #59's."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import pytest
import temporal_isolation_app as app
from conftest import Cluster, connect_public
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import WorkflowExecutionStatus
from temporalio.api.workflowservice.v1 import DescribeWorkflowExecutionRequest
from temporalio.client import Client
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.service import RPCError
from temporalio.worker import Worker

from kontra import _relay
from kontra.codec import KontraCodec
from kontra.codec import stream as s
from kontra.codec.age import AgeError, decrypt, generate_identity
from kontra.contract import names
from kontra.serve import seller_data_converter
from kontra.serve.interceptors import KontraInterceptor

pytestmark = pytest.mark.temporal

SELLER = app.SELLER_NAMESPACE
SELLER_KEY = generate_identity()
BUYER_KEY = generate_identity()
LISTED, UNLISTED = "progress", "log"  # StreamJob's two topics; the offering lists one


class _Keys:
    async def buyer_key(self, job_id: str) -> str:
        return BUYER_KEY[1]


class Recording:
    """The relay token's service, recording each call by name: anything but the two the
    relay may make would be an AttributeError here, before any request is sent."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.calls: list[str] = []

    async def update_workflow_execution(self, req: Any, retry: bool = False, metadata: Any = None, timeout: Any = None) -> Any:  # noqa: ASYNC109  # fmt: skip
        self.calls.append("UpdateWorkflowExecution")
        return await self._inner.update_workflow_execution(req, retry=retry, timeout=timeout)

    async def query_workflow(self, req: Any, retry: bool = False, metadata: Any = None, timeout: Any = None) -> Any:  # noqa: ASYNC109  # fmt: skip
        self.calls.append("QueryWorkflow")
        return await self._inner.query_workflow(req, retry=retry, timeout=timeout)


async def _seller(cluster: Cluster) -> Client:
    codec = KontraCodec(SELLER_KEY[0], [SELLER_KEY[1]], role="seller", buyer_keys=_Keys())
    return await connect_public(
        cluster,
        SELLER,
        api_key=cluster.signer.api_key(SELLER),
        data_converter=seller_data_converter(codec),
    )


def _closed(control: Client, workflow_id: str) -> Any:
    """The gate's observer: the internal frontend says whether the job is closed."""

    async def closed() -> bool:
        d = await control.workflow_service.describe_workflow_execution(
            DescribeWorkflowExecutionRequest(
                namespace=SELLER, execution=WorkflowExecution(workflow_id=workflow_id)
            )
        )
        return d.workflow_execution_info.status not in (
            WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
            WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
        )

    return closed


async def test_the_relay_reads_a_jobs_stream_with_its_two_calls_only(
    cluster: Cluster, control: Client
) -> None:
    seller = await _seller(cluster)
    steps = 4
    job_id = names.new_job_id()
    wf = names.job_workflow_id(job_id)
    interceptor = KontraInterceptor(None, streams={app.SELLER_TASK_QUEUE: [LISTED]})
    async with Worker(
        seller,
        task_queue=app.SELLER_TASK_QUEUE,
        workflows=[app.StreamJob],
        interceptors=[interceptor],
    ):
        handle = await seller.start_workflow(
            app.StreamJob.run, app.StreamJobInput(steps), id=wf, task_queue=app.SELLER_TASK_QUEUE
        )
        relay_client = await connect_public(
            cluster, SELLER, api_key=cluster.signer.relay(SELLER, job_id)
        )
        service = Recording(relay_client.workflow_service)
        closed = _closed(control, wf)
        pinned = handle.first_execution_run_id or ""
        try:
            # From offset 0 (the gate's start, owner OD2): every listed item, sealed.
            items: list[_relay.RelayItem] = []
            dropped = 0
            async with asyncio.timeout(60):
                async for batch in _relay.relay(
                    service,
                    namespace=SELLER,
                    job_id=job_id,
                    topics=[LISTED, names.PROGRESS_TOPIC],
                    start=0,
                    closed=closed,
                    first_execution_run_id=lambda: pinned,
                    min_interval=0.0,
                    rpc_timeout=10.0,
                ):
                    items += batch.items
                    dropped += batch.dropped
                    if len(items) >= steps:
                        break
            assert dropped == 0  # it asked for its topics only: none unlisted came back
            assert [(i.topic, i.offset) for i in items] == [(LISTED, 2 * n) for n in range(steps)]
            values = []
            for item in items:
                assert item.ciphertext is not None
                assert item.ciphertext.startswith(s.AGE_MAGIC)
                payload = s.open_item(item.ciphertext, [BUYER_KEY[0]], topic=LISTED, offset=item.offset)  # fmt: skip
                values.append(PydanticPayloadConverter().from_payload(payload, str))
                decrypt(item.ciphertext, SELLER_KEY[0])  # the seller's set reads it too
            assert values == [f"step-{n}" for n in range(steps)]

            # Asking for the unlisted topic by name: its items come back unnamed and are
            # dropped; their ciphertext does not open with the buyer's key.
            async with asyncio.timeout(60):
                async for batch in _relay.relay(
                    service,
                    namespace=SELLER,
                    job_id=job_id,
                    topics=[UNLISTED],
                    start=0,
                    closed=closed,
                    min_interval=0.0,
                ):
                    assert batch.items == ()
                    assert batch.dropped == steps
                    break

            # From the tail (the offset Query), until the job closes: nothing new, then the
            # end of the job ends the loop cleanly.
            async def follow() -> list[_relay.RelayBatch]:
                return [
                    b
                    async for b in _relay.relay(
                        service,
                        namespace=SELLER,
                        job_id=job_id,
                        topics=[LISTED],
                        closed=closed,
                        first_execution_run_id=lambda: pinned,
                        min_interval=0.0,
                        rpc_timeout=5.0,
                    )
                ]

            tail = asyncio.create_task(follow())
            await asyncio.sleep(1.0)
            assert await handle.execute_update(app.StreamJob.approve, "the test") == (
                "approved by the test"
            )
            assert await handle.result() == "approved"
            batches = await asyncio.wait_for(tail, 60)
            assert all(b.items == () for b in batches)
        finally:
            with contextlib.suppress(RPCError):
                await handle.terminate("the test is over")
    assert set(service.calls) == {"UpdateWorkflowExecution", "QueryWorkflow"}


async def test_the_buyer_key_opens_no_unlisted_item(cluster: Cluster) -> None:
    """A raw poll for every topic, as a misbehaving relay would send it: the unlisted
    items are unnamed and sealed to the seller alone."""
    seller = await _seller(cluster)
    job_id = names.new_job_id()
    wf = names.job_workflow_id(job_id)
    async with Worker(
        seller,
        task_queue=app.SELLER_TASK_QUEUE,
        workflows=[app.StreamJob],
        interceptors=[KontraInterceptor(None, streams={app.SELLER_TASK_QUEUE: [LISTED]})],
    ):
        handle = await seller.start_workflow(
            app.StreamJob.run, app.StreamJobInput(2), id=wf, task_queue=app.SELLER_TASK_QUEUE
        )
        relay_client = await connect_public(
            cluster, SELLER, api_key=cluster.signer.relay(SELLER, job_id)
        )
        try:
            req = _relay._request(SELLER, wf, "", [], 0, "kontra-relay")  # pyright: ignore[reportPrivateUsage]
            async with asyncio.timeout(60):
                while True:
                    resp = await relay_client.workflow_service.update_workflow_execution(req)
                    if resp.HasField("outcome"):
                        break
            env = s.parse_envelope(resp.outcome.success.payloads[0])
            unnamed = [i for i in env.items if i.topic is None]
            assert len(unnamed) == 2
            assert {i.topic for i in env.items} == {None, LISTED}
            for item in unnamed:
                assert item.ciphertext is not None
                with pytest.raises(AgeError):
                    decrypt(item.ciphertext, BUYER_KEY[0])
                inner = s.open_item(item.ciphertext, [SELLER_KEY[0]], topic=UNLISTED, offset=item.offset)  # fmt: skip
                assert PydanticPayloadConverter().from_payload(inner, str).startswith("line-")
        finally:
            with contextlib.suppress(RPCError):
                await handle.terminate("the test is over")
