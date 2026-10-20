"""The engine contract's seller on Temporal (seam ⑥): vanilla temporalio, as every seller's
code is (decisions I29), speaking the contract's behaviour vocabulary
(:data:`kontra.testing.engine_contract.Behaviour`). An importable module, as the workflow
sandbox requires.

The seller side seals, as a seller's ``KontraCodec`` would: the job's result is the codec's
delivery envelope (``binary/kontra-age``, ``kontra-v`` 1, an age file of the inner payload),
sealed in an activity (a workflow may not draw randomness); a ``plain`` job returns its
result in the clear, as a seller that bypassed the codec would. The job hosts its own stream
the way the contrib's poll handler does, by its wire names, answering with the stream envelope
(:mod:`kontra.codec.stream`) whose items are references to age files stored in :data:`BLOBS`
(the job's stream blobs, which the harness's adapter reads): what the gate's relay reads."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler
from nexusrpc.handler import StartOperationContext
from temporalio import activity, nexus, workflow
from temporalio.common import RawValue, WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.nexus import WorkflowRunOperationContext
from temporalio.workflow import HandlerUnfinishedPolicy

with workflow.unsafe.imports_passed_through():
    from temporalio.api.common.v1 import Payload

    from kontra.codec import age
    from kontra.codec.stream import STREAM_ENCODING, offset_payload
    from kontra.contract.names import STREAM_OFFSET_QUERY, STREAM_POLL_UPDATE, job_workflow_id

LISTED, UNLISTED = "events", "secret"
FIRST = ((LISTED, 0), (UNLISTED, 1), (LISTED, 2))
LATER = ((LISTED, 3), (UNLISTED, 4), (LISTED, 5))
QUOTE_DOCUMENT = b'{"amount_minor":1234,"currency":"USD","max_duration_seconds":600,"status":"ok"}'
#: The stream blobs the seller's activities stored, by sha256 (the bucket, in this process).
BLOBS: dict[str, bytes] = {}
_SEALED = {"encoding": b"binary/kontra-age", "kontra-v": b"1"}
_PLAIN = {"encoding": b"json/plain"}


@activity.defn
async def seal(text: str, to: str) -> bytes:
    """The age file of the result's inner payload, to the job's key."""
    inner = Payload(metadata=_PLAIN, data=json.dumps(text).encode())
    return age.encrypt(inner.SerializeToString(), [to])


@activity.defn
async def seal_items(texts: list[str], to: str) -> list[tuple[str, int]]:
    """Stream items sealed one by one, each stored in :data:`BLOBS`: (sha256, size)."""
    out: list[tuple[str, int]] = []
    for t in texts:
        ct = age.encrypt(t.encode(), [to])
        digest = hashlib.sha256(ct).hexdigest()
        BLOBS[digest] = ct
        out.append((digest, len(ct)))
    return out


@workflow.defn(name="EngineContractJob")
class ContractJob:
    def __init__(self) -> None:
        self._log: list[tuple[str, str, int]] = []
        self._done = False

    async def _publish(self, items: tuple[tuple[str, int], ...], marker: str, to: str) -> None:
        texts = [f"ITEM {marker} {offset}" for _, offset in items]
        sealed = await workflow.execute_activity(
            seal_items, args=[texts, to], start_to_close_timeout=timedelta(seconds=30)
        )
        self._log.extend(
            (topic, digest, size) for (topic, _), (digest, size) in zip(items, sealed, strict=True)
        )

    @staticmethod
    async def _until_asked() -> None:
        with contextlib.suppress(asyncio.CancelledError):  # an ASK: this seller goes on
            await workflow.wait_condition(lambda: False)

    @workflow.run
    async def run(self, plan: dict[str, Any]) -> RawValue:
        behaviour, marker, to = plan["b"], plan["m"], plan["to"]
        try:
            if behaviour == "hang":
                await workflow.wait_condition(lambda: False)  # an ASK ends it CANCELED
            if behaviour == "ignore_ask":
                await self._until_asked()
            if behaviour.startswith("can") and int(behaviour[3:]) > 0:
                workflow.continue_as_new({**plan, "b": f"can{int(behaviour[3:]) - 1}"})
            if behaviour == "plain":
                text = json.dumps(f"PLAIN {marker}").encode()
                return RawValue(Payload(metadata=_PLAIN, data=text))
            if behaviour == "publish":
                await self._publish(FIRST, marker, to)
                await self._until_asked()
                await self._publish(LATER, marker, to)
            ct = await workflow.execute_activity(
                seal, args=[f"DONE {marker}", to], start_to_close_timeout=timedelta(seconds=30)
            )
            return RawValue(Payload(metadata=_SEALED, data=ct))
        finally:
            self._done = True

    @workflow.update(name=STREAM_POLL_UPDATE, unfinished_policy=HandlerUnfinishedPolicy.ABANDON)
    async def poll(self, req: RawValue) -> RawValue:
        args = json.loads(req.payload.data)
        offset, topics = args["from_offset"], set(args["topics"])
        await workflow.wait_condition(lambda: len(self._log) > offset or self._done)
        items = [
            {"topic": topic, "offset": i, "size": size, "sha256": digest}
            for i, (topic, digest, size) in enumerate(self._log)
            if i >= offset and topic in topics
        ]
        envelope = {"v": 2, "items": items, "next_offset": len(self._log), "more_ready": False}
        data = json.dumps(envelope, separators=(",", ":")).encode()
        return RawValue(
            Payload(metadata={"encoding": STREAM_ENCODING, "kontra-v": b"1"}, data=data)
        )

    @workflow.query(name=STREAM_OFFSET_QUERY)
    def offset(self) -> RawValue:
        return RawValue(offset_payload(len(self._log)))


def job_of(hold: str) -> str:
    """The job a test hold names (``testhold.<job id>.sig``): the stand-in for the real
    handler's offline verification of the gate's signed hold (decisions I10)."""
    return hold.strip().split(".")[1]


def make_handler(service: str) -> Any:
    class Handler:
        @nexusrpc.handler.sync_operation
        async def quote(self, ctx: StartOperationContext, req: dict[str, Any]) -> RawValue:
            if req["q"] == "fail":
                raise RuntimeError("the seller's quote refused")
            if req["q"] == "slow":
                await asyncio.sleep(8)
            return RawValue(Payload(metadata=_PLAIN, data=QUOTE_DOCUMENT))

        @nexus.workflow_run_operation
        async def execute(
            self, ctx: WorkflowRunOperationContext, plan: dict[str, Any]
        ) -> nexus.WorkflowHandle[RawValue]:
            try:
                return await ctx.start_workflow(
                    ContractJob.run,
                    plan,
                    id=job_workflow_id(job_of(ctx.headers["kontra-hold"])),
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                )
            except WorkflowAlreadyStartedError:  # one execution per hold, as kontra.serve
                raise nexusrpc.HandlerError(
                    "the job ran already",
                    type=nexusrpc.HandlerErrorType.BAD_REQUEST,
                    retryable_override=False,
                ) from None

    return nexusrpc.handler.service_handler(name=service)(Handler)()
