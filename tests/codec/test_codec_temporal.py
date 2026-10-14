"""The codec under a real Temporal server: temporalio applies the serialization contexts,
and the raw history shows who can read each payload (decisions I7) and holds no plaintext."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import pytest
from temporalio.api.common.v1 import Payload
from temporalio.api.history.v1 import HistoryEvent
from temporalio.client import Client, WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from kontra.codec import ENCODING, KontraCodec, data_converter
from kontra.codec.age import AgeError, decrypt, generate_identity
from kontra.contract.names import job_workflow_id, new_job_id

from .temporal_flows import Child, FailFlow, JobFlow, shout

pytestmark = pytest.mark.temporal

SELLER = generate_identity()
BUYER = generate_identity()
MARKER = "kontra-history-marker-5b1e"
TASK_QUEUE = "acme/enrich@1.2.0"


class BuyerKeys:
    async def buyer_key(self, job_id: str) -> str:
        return BUYER[1]


def readers(p: Payload) -> set[str]:
    assert p.metadata["encoding"] == ENCODING
    out: set[str] = set()
    for name, (secret, _) in {"seller": SELLER, "buyer": BUYER}.items():
        try:
            decrypt(p.data, secret)
        except AgeError:
            continue
        out.add(name)
    return out


@pytest.fixture
async def client() -> AsyncIterator[Client]:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        codec = KontraCodec(SELLER[0], [SELLER[1]], role="seller", buyer_keys=BuyerKeys())
        config = env.client.config()
        config["data_converter"] = data_converter(codec)
        yield Client(**config)


async def _history(client: Client, workflow_id: str) -> list[HistoryEvent]:
    history = await client.get_workflow_handle(workflow_id).fetch_history()
    raw = b"".join(e.SerializeToString() for e in history.events)
    assert MARKER.encode() not in raw
    assert MARKER.upper().encode() not in raw
    return list(history.events)


async def test_job_workflow_payloads_by_context(client: Client) -> None:
    wf_id = job_workflow_id(new_job_id())
    async with Worker(
        client, task_queue=TASK_QUEUE, workflows=[JobFlow, Child], activities=[shout]
    ):
        result = await client.execute_workflow(
            JobFlow.run, "hello " + MARKER, id=wf_id, task_queue=TASK_QUEUE
        )
    assert result == ("HELLO " + MARKER).upper() + "!"

    seen: dict[str, set[str]] = {}
    for e in await _history(client, wf_id):
        if e.HasField("workflow_execution_started_event_attributes"):
            [p] = e.workflow_execution_started_event_attributes.input.payloads
            seen["workflow input"] = readers(p)
        elif e.HasField("activity_task_scheduled_event_attributes"):
            [p] = e.activity_task_scheduled_event_attributes.input.payloads
            seen["activity input"] = readers(p)
        elif e.HasField("activity_task_completed_event_attributes"):
            [p] = e.activity_task_completed_event_attributes.result.payloads
            seen["activity result"] = readers(p)
        elif e.HasField("start_child_workflow_execution_initiated_event_attributes"):
            [p] = e.start_child_workflow_execution_initiated_event_attributes.input.payloads
            seen["child input"] = readers(p)
        elif e.HasField("child_workflow_execution_completed_event_attributes"):
            [p] = e.child_workflow_execution_completed_event_attributes.result.payloads
            seen["child result"] = readers(p)
        elif e.HasField("workflow_execution_completed_event_attributes"):
            [p] = e.workflow_execution_completed_event_attributes.result.payloads
            seen["workflow result"] = readers(p)
    assert seen == {
        "workflow input": {"seller", "buyer"},
        "activity input": {"seller"},
        "activity result": {"seller"},
        "child input": {"seller"},
        "child result": {"seller"},
        "workflow result": {"seller", "buyer"},
    }
    child = await _history(client, wf_id + "-child")
    started = child[0].workflow_execution_started_event_attributes
    assert readers(started.input.payloads[0]) == {"seller"}


async def test_job_failure_is_encrypted_to_both(client: Client) -> None:
    wf_id = job_workflow_id(new_job_id())
    async with Worker(client, task_queue=TASK_QUEUE, workflows=[FailFlow]):
        with pytest.raises(WorkflowFailureError) as err:
            await client.execute_workflow(FailFlow.run, MARKER, id=wf_id, task_queue=TASK_QUEUE)
    # The seller's own client decodes the failure through the codec.
    assert MARKER in str(err.value.cause)
    [failed] = [
        e
        for e in await _history(client, wf_id)
        if e.HasField("workflow_execution_failed_event_attributes")
    ]
    failure = failed.workflow_execution_failed_event_attributes.failure
    assert failure.message == "Encoded failure"
    assert readers(failure.encoded_attributes) == {"seller", "buyer"}
    [details] = failure.application_failure_info.details.payloads
    assert readers(details) == {"seller", "buyer"}


async def test_non_job_workflow_is_seller_only(client: Client) -> None:
    wf_id = "enrich-" + uuid.uuid4().hex
    async with Worker(
        client, task_queue=TASK_QUEUE, workflows=[JobFlow, Child], activities=[shout]
    ):
        await client.execute_workflow(JobFlow.run, MARKER, id=wf_id, task_queue=TASK_QUEUE)
    for e in await _history(client, wf_id):
        if e.HasField("workflow_execution_started_event_attributes"):
            [p] = e.workflow_execution_started_event_attributes.input.payloads
            assert readers(p) == {"seller"}
