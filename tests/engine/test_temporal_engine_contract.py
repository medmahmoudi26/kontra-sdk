"""Seam ⑥: the engine contract against :class:`~kontra.engine.temporal.TemporalEngine` on a
Temporal dev server (CLI 1.9.1, server 1.32) -- the same suite the FakeEngine passes, unchanged.

The seller is vanilla temporalio (``contract_flows``); the adapter is the one the gates use,
over :class:`kontra._control.Control`. A dev server runs no authorizer, so isolation over the
wire (E18) is reported SKIPPED here and proven on Kontra's own server instead
(``tests/temporal/test_engine_isolation_wire.py``)."""

from __future__ import annotations

import contextlib
import json
import uuid
from collections.abc import Mapping
from typing import Any

import jwt
import pytest
from contract_flows import (
    QUOTE_DOCUMENT,
    ContractJob,
    make_handler,
    seal,
    seal_items,
)
from cryptography.hazmat.primitives.asymmetric import ec
from engine_harness import open_text, party
from temporalio.api.common.v1 import Payload
from temporalio.api.workflowservice.v1 import RequestCancelNexusOperationExecutionRequest
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.worker import Worker

from kontra._control import TemporalError
from kontra.codec import age
from kontra.contract.names import OfferingId, job_workflow_id, new_endpoint_name
from kontra.engine import Engine, JobHandle, Sealed, SealedPayload
from kontra.engine.temporal import TemporalEngine
from kontra.testing.devserver import DevServer
from kontra.testing.engine_contract import CHECKS, Behaviour, QuoteBehaviour, run_engine_contract

pytestmark = pytest.mark.temporal


class Signer:
    def __init__(self) -> None:
        self._key = ec.generate_private_key(ec.SECP256R1())

    @property
    def issuer(self) -> str:
        return "https://gate.contract"

    def sign(self, claims: Mapping[str, Any]) -> str:
        return jwt.encode(dict(claims), self._key, algorithm="ES256", headers={"kid": "k1"})


class DeadControl:
    """A control plane that cannot reach its frontend: every call fails."""

    def __getattr__(self, name: str) -> Any:
        if name == "client":
            raise AttributeError(name)  # no frontend to relay from either

        async def unreachable(*args: Any, **kwargs: Any) -> Any:
            raise TemporalError(f"{name}: the frontend is unreachable")

        return unreachable


class TemporalHarness:
    enforces_isolation = False  # a dev server runs no authorizer (E18: tests/temporal)
    buyer_presence = True
    streams = True

    def __init__(self, dev: DevServer) -> None:
        self.dev = dev
        self.signer = Signer()
        self.identity, self.recipient = age.generate_identity()
        self.seller, self.buyer, self.stranger = party("s"), party("b"), party("x")
        self.offering = OfferingId.parse(f"{self.seller}/contract@1.0.0")
        self.other_offering = OfferingId.parse(f"{self.seller}/contract-other@1.0.0")
        self.route = new_endpoint_name()
        self._engine = self._new()

    def _new(self, control: Any = None) -> TemporalEngine:
        return TemporalEngine(
            control if control is not None else self.dev.control(),
            signer=self.signer,
            address=self.dev.grpc_address,
            relay_interval=0.2,
        )

    async def setup(self) -> TemporalHarness:
        await self._engine.isolate(self.seller)
        await self._engine.isolate(self.buyer)
        await self._engine.expose(self.offering, self.route)
        return self

    def worker(self) -> Worker:
        return Worker(
            self.dev.client_for(self.seller),
            task_queue=self.offering.task_queue,
            workflows=[ContractJob],
            activities=[seal, seal_items],
            nexus_service_handlers=[make_handler(self.offering.service_name)],
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
        return QUOTE_DOCUMENT

    def hold(self, job_id: str) -> str:
        return f"testhold.{job_id}.sig"

    def open(self, result: SealedPayload) -> str:
        return open_text(result, self.identity)

    def open_item(self, item: Sealed) -> str:
        return age.decrypt(item.data, [self.identity]).decode()

    async def seller_start(self, handle: JobHandle, job_id: str, marker: str) -> None:
        """The seller runs ``job-<id>`` itself, as its own token may (I15's attack)."""
        # A run of the job may be open: the engine holds one at most.
        with contextlib.suppress(WorkflowAlreadyStartedError):
            await self.dev.client_for(self.seller).start_workflow(
                ContractJob.run,
                {"b": "ok", "m": marker, "to": self.recipient},
                id=job_workflow_id(job_id),
                task_queue=self.offering.task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
            )

    async def seller_revive(self, handle: JobHandle, job_id: str) -> None:
        """What a seller's token may do to an ended run: start the job again (Reset, which
        would revive the terminated run itself, is denied to every token by the authorizer,
        I30 -- proven over the wire in tests/temporal)."""
        await self.seller_start(handle, job_id, "revival")

    async def buyer_withdraw(self, handle: JobHandle, job_id: str) -> None:
        await self.dev.client_for(
            self.buyer
        ).workflow_service.request_cancel_nexus_operation_execution(
            RequestCancelNexusOperationExecutionRequest(
                namespace=self.buyer,
                operation_id=job_workflow_id(job_id),
                identity="the buyer",
                request_id=str(uuid.uuid4()),
                reason="changed my mind",
            )
        )

    async def reach(self, credential: str, handle: JobHandle) -> bool:
        raise AssertionError("a dev server enforces no isolation")

    async def controls(self, credential: str) -> bool:
        raise AssertionError("a dev server enforces no isolation")

    async def fresh_engine(self) -> Engine:
        return self._new()  # a fresh Control: no cache of the first one's

    async def unreachable_engine(self) -> Engine:
        return self._new(DeadControl())


@pytest.mark.timeout(600)
async def test_the_temporal_engine_passes_the_contract_on_a_dev_server(
    dev_server: DevServer,
) -> None:
    harness = await TemporalHarness(dev_server).setup()
    async with harness.worker():
        report = await run_engine_contract(lambda: _ready(harness))
    assert [line.split()[0] for line in report] == list(CHECKS)
    skipped = [line for line in report if " ok: " not in line]
    assert skipped == ["E18 SKIPPED (this harness enforces no isolation)"], report
    print("\n".join(report))


async def _ready(harness: TemporalHarness) -> TemporalHarness:
    return harness
