"""The codec across a real Nexus call (Temporal CLI 1.9.1 dev server): the recipients a
serialization context implies are the ones temporalio actually applies on each end.

A ``NexusSerializationContext`` is used for the caller's input *and* the handler's reply, so
this is where a seller-role codec could confuse the two: a seller's workflow that hires another
offering must encrypt its input like a buyer does, while the handler's quote output stays
plaintext for the gate (decisions I4, I7) -- for the service the worker generated, by its
exact name. On the calling side, a result that arrives in plaintext is a leak and is refused
(FIXUPS #1/#21); a refusal of ``execute`` is plaintext by design and marked as such.

The server binary is ``KONTRA_TEMPORAL_CLI`` if set, else CLI 1.9.1, downloaded once and
checksum-verified (kontra.devgate.temporal_cli); with neither available the test is skipped.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Iterable, Sequence

import pytest
from temporalio.api.common.v1 import Payload
from temporalio.api.failure.v1 import Failure
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import CreateNexusEndpointRequest
from temporalio.client import Client, WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from kontra.codec import ENCODING, PLAINTEXT_KEY, KontraCodec, data_converter
from kontra.codec.age import AgeError, decrypt, generate_identity
from kontra.contract.names import new_endpoint_name, new_job_id

from .temporal_flows import EnrichHandler, EnrichJob, Hire, Order

pytestmark = pytest.mark.temporal

SELLER = generate_identity()  # acme, serving acme/enrich
BUYER = generate_identity()  # a buyer (role buyer)
RESELLER = generate_identity()  # another seller whose workflow hires acme/enrich
STRANGER = generate_identity()
PARTIES = {"seller": SELLER, "buyer": BUYER, "reseller": RESELLER, "stranger": STRANGER}
MARKER = "kontra-nexus-marker-91c4"
TASK_QUEUE = "acme/enrich@1.2.0"
LEAKY_QUEUE = "acme/leaky@1.0.0"
SERVICE = "acme.enrich"  # what kontra.serve generates for acme/enrich (OfferingId.service_name)


class Keys:
    """BuyerKeys for acme (job -> hirer's key, as the hold token would say) and SellerKeys
    for the hirers (the endpoint's seller, as the catalog would say)."""

    def __init__(self, *endpoints: str) -> None:
        self.endpoints = endpoints
        self.buyers: dict[str, str] = {}

    async def buyer_key(self, job_id: str) -> str:
        return self.buyers[job_id]

    async def seller_recipients(self, endpoint: str) -> Sequence[str]:
        if endpoint not in self.endpoints:
            raise LookupError(endpoint)
        return [SELLER[1]]


class LeakyCodec(KontraCodec):
    """A seller that reads Kontra payloads but encrypts nothing it sends: its deliveries
    land in Temporal's history in plaintext."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return list(payloads)


def _failures(failure: Failure) -> Iterable[Failure]:
    while True:
        yield failure
        if not failure.HasField("cause"):
            return
        failure = failure.cause


def readers(p: Payload) -> set[str]:
    assert p.metadata["encoding"] == ENCODING
    out: set[str] = set()
    for name, (secret, _) in PARTIES.items():
        try:
            decrypt(p.data, secret)
        except AgeError:
            continue
        out.add(name)
    return out


async def _history(client: Client, workflow_id: str) -> list[HistoryEvent]:
    history = await client.get_workflow_handle(workflow_id).fetch_history()
    raw = b"".join(e.SerializeToString() for e in history.events)
    assert MARKER.encode() not in raw
    assert MARKER.upper().encode() not in raw
    return list(history.events)


def _nexus_payloads(events: Iterable[HistoryEvent]) -> dict[str, Payload]:
    out: dict[str, Payload] = {}
    for e in events:
        if e.HasField("nexus_operation_scheduled_event_attributes"):
            out["input"] = e.nexus_operation_scheduled_event_attributes.input
        elif e.HasField("nexus_operation_completed_event_attributes"):
            out["result"] = e.nexus_operation_completed_event_attributes.result
    return out


async def _start_env() -> WorkflowEnvironment:
    args = ["--namespace", "acme", "--namespace", "reseller", "--log-level", "error"]
    cli = os.environ.get("KONTRA_TEMPORAL_CLI")
    if cli:  # an explicit binary must work: no skip
        return await WorkflowEnvironment.start_local(
            dev_server_existing_path=cli, dev_server_extra_args=args
        )
    try:
        return await WorkflowEnvironment.start_local(
            dev_server_download_version="v1.9.1", dev_server_extra_args=args
        )
    except Exception as e:  # no cached binary and no network
        pytest.skip(f"no Temporal CLI 1.9.1 dev server: {e}")


async def _task_failure(client: Client, workflow_id: str, within: float = 30) -> str:
    """The message of the first failed workflow task of ``workflow_id`` (the hirer's own
    codec encrypted it, as every failure)."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while loop.time() < deadline:
        history = await client.get_workflow_handle(workflow_id).fetch_history()
        for e in history.events:
            if e.HasField("workflow_task_failed_event_attributes"):
                failure = e.workflow_task_failed_event_attributes.failure
                return str(await client.data_converter.decode_failure(failure))
        await asyncio.sleep(0.2)
    raise AssertionError(f"{workflow_id}: no workflow task failed within {within}s")


def _client(env: WorkflowEnvironment, namespace: str, codec: KontraCodec) -> Client:
    config = env.client.config()
    config["namespace"] = namespace
    config["data_converter"] = data_converter(codec)
    return Client(**config)


async def _endpoint(env: WorkflowEnvironment, task_queue: str) -> str:
    endpoint = new_endpoint_name()
    await env.client.operator_service.create_nexus_endpoint(
        CreateNexusEndpointRequest(
            spec=EndpointSpec(
                name=endpoint,
                target=EndpointTarget(
                    worker=EndpointTarget.Worker(namespace="acme", task_queue=task_queue)
                ),
            )
        )
    )
    return endpoint


async def test_nexus_call_recipients_on_both_ends() -> None:
    env = await _start_env()
    async with env:
        endpoint = await _endpoint(env, TASK_QUEUE)
        leaky_endpoint = await _endpoint(env, LEAKY_QUEUE)
        keys = Keys(endpoint, leaky_endpoint)
        acme = _client(
            env,
            "acme",
            KontraCodec(SELLER[0], [SELLER[1]], role="seller", buyer_keys=keys, services=[SERVICE]),
        )
        leaky = _client(
            env, "acme", LeakyCodec(SELLER[0], [SELLER[1]], role="seller", services=[SERVICE])
        )
        buyer = _client(
            env, "default", KontraCodec(BUYER[0], [BUYER[1]], role="buyer", seller_keys=keys)
        )
        # A seller-role process: its workflows may hire other offerings.
        reseller = _client(
            env,
            "reseller",
            KontraCodec(
                RESELLER[0], [RESELLER[1]], role="seller", buyer_keys=keys, seller_keys=keys
            ),
        )
        hirers = {"buyer": buyer, "reseller": reseller}
        workers = [
            Worker(
                acme,
                task_queue=TASK_QUEUE,
                workflows=[EnrichJob],
                nexus_service_handlers=[EnrichHandler()],
            ),
            Worker(buyer, task_queue="buyer-flows", workflows=[Hire]),
            Worker(reseller, task_queue="reseller-flows", workflows=[Hire]),
            Worker(
                leaky,
                task_queue=LEAKY_QUEUE,
                workflows=[EnrichJob],
                nexus_service_handlers=[EnrichHandler()],
            ),
        ]
        async with workers[0], workers[1], workers[2], workers[3]:

            async def hire(
                who: str, operation: str, order: Order, *, at: str = endpoint
            ) -> tuple[str, str]:
                wf_id = f"hire-{uuid.uuid4().hex}"
                result = await hirers[who].execute_workflow(
                    Hire.run,
                    args=[at, operation, order],
                    id=wf_id,
                    task_queue=f"{who}-flows",
                )
                return wf_id, result

            # execute, hired by a buyer and by a seller's workflow
            for who, party in (("buyer", BUYER), ("reseller", RESELLER)):
                job = new_job_id()
                keys.buyers[job] = party[1]
                wf_id, result = await hire(who, "execute", Order(job, f"text {MARKER}"))
                assert result == f"TEXT {MARKER}".upper()
                seen = _nexus_payloads(await _history(hirers[who], wf_id))
                assert readers(seen["input"]) == {"seller", who}
                assert readers(seen["result"]) == {"seller", who}
                for e in await _history(acme, f"job-{job}"):
                    if e.HasField("workflow_execution_started_event_attributes"):
                        [p] = e.workflow_execution_started_event_attributes.input.payloads
                        assert readers(p) == {"seller", who}
                    elif e.HasField("workflow_execution_completed_event_attributes"):
                        [p] = e.workflow_execution_completed_event_attributes.result.payloads
                        assert readers(p) == {"seller", who}

            # quote: the input is encrypted, the handler's output is a plaintext price (I4)
            for who in hirers:
                wf_id, price = await hire(who, "quote", Order(new_job_id(), MARKER))
                assert price == f"price:{len(MARKER)}"
                seen = _nexus_payloads(await _history(hirers[who], wf_id))
                assert readers(seen["input"]) == {"seller", who}
                assert seen["result"].metadata["encoding"] == b"json/plain"
                assert seen["result"].data == f'"price:{len(MARKER)}"'.encode()

            # a handler refusal reaches the hirer readably (it is plaintext by design, and
            # the seller's codec marks it so)
            refused_id = f"hire-{uuid.uuid4().hex}"
            with pytest.raises(WorkflowFailureError) as err:
                await buyer.execute_workflow(
                    Hire.run,
                    args=[endpoint, "execute", Order(new_job_id(), "refuse")],
                    id=refused_id,
                    task_queue="buyer-flows",
                )
            messages: list[str] = []
            cause: BaseException | None = err.value
            while cause is not None:
                messages.append(str(cause))
                cause = cause.__cause__
            assert any("refused by the handler" in m for m in messages)
            refusal = next(
                f
                for e in await _history(buyer, refused_id)
                if e.HasField("nexus_operation_failed_event_attributes")
                for f in _failures(e.nexus_operation_failed_event_attributes.failure)
                if f.HasField("encoded_attributes")
            )
            assert refusal.encoded_attributes.metadata[PLAINTEXT_KEY] == b"1"

            # A seller that delivers in plaintext: the buyer's workflow does not take the
            # leaked result as a delivery -- its workflow task fails with the codec's error.
            for who in hirers:
                job = new_job_id()
                keys.buyers[job] = PARTIES[who][1]
                wf_id = f"hire-{uuid.uuid4().hex}"
                handle = await hirers[who].start_workflow(
                    Hire.run,
                    args=[leaky_endpoint, "execute", Order(job, f"text {MARKER}")],
                    id=wf_id,
                    task_queue=f"{who}-flows",
                )
                try:
                    text = await _task_failure(hirers[who], wf_id)
                finally:
                    await handle.terminate()
                assert "arrived in plaintext" in text, text
                assert repr(leaky_endpoint) in text
                assert "'execute'" in text
