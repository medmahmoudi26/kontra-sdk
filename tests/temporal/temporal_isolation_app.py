"""Workflows and Nexus handlers used by the isolation tests.

They live in their own importable module because the workflow sandbox re-imports the
module that defines a workflow. Plain Temporal only: nothing here knows about Kontra.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import timedelta

import nexusrpc
import nexusrpc.handler
from temporalio import activity, nexus, workflow
from temporalio.contrib.workflow_streams import WorkflowStream

SELLER_NAMESPACE = "acme"
SELLER_TASK_QUEUE = "acme/enrich@1.2.0"  # OfferingId(acme, enrich, 1.2.0).task_queue
SELLER_SERVICE = "acme.enrich"  # OfferingId(acme, enrich, 1.2.0).service_name
BUYER_NAMESPACE = "buyer"
BUYER_TASK_QUEUE = "buyer/flows"
OTHER_NAMESPACE = "other"
OTHER_TASK_QUEUE = "other/probe@1.0.0"
OTHER_SERVICE = "other.probe"
PLATFORM_TASK_QUEUE = "kontra-system/platform"  # Kontra's own workers (KON-15)
PLATFORM_DEPLOYMENT = "kontra-platform"

# What the handlers saw, for the tests to read back (the workers run in the test process).
STARTED_JOBS: list[str] = []  # handler workflow ids started by EnrichHandler.execute
PROBE_CALLBACKS: list[tuple[str, dict[str, str]]] = []  # (callback_url, callback_headers)


@dataclass
class EnrichInput:
    n: int
    hold_for_signal: bool = False


@dataclass
class EnrichOutput:
    n: int
    hold: str  # the kontra-hold header the handler saw
    handler_workflow_id: str = ""  # set by the workflow-run operation


@nexusrpc.service(name=SELLER_SERVICE)
class EnrichService:
    quote: nexusrpc.Operation[EnrichInput, EnrichOutput]
    execute: nexusrpc.Operation[EnrichInput, EnrichOutput]


@dataclass
class JobInput:
    n: int
    hold: str
    hold_for_signal: bool


@workflow.defn
class EnrichJob:
    """The seller's handler workflow (`job-<id>` in the seller's namespace)."""

    def __init__(self) -> None:
        self._released = False

    @workflow.run
    async def run(self, job: JobInput) -> EnrichOutput:
        if job.hold_for_signal:
            await workflow.wait_condition(lambda: self._released, timeout=timedelta(minutes=2))
        return EnrichOutput(job.n * 2, job.hold, workflow.info().workflow_id)

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.query
    def progress(self) -> str:
        return "released" if self._released else "running"


@nexusrpc.handler.service_handler(service=EnrichService)
class EnrichHandler:
    @nexusrpc.handler.sync_operation
    async def quote(
        self, ctx: nexusrpc.handler.StartOperationContext, input: EnrichInput
    ) -> EnrichOutput:
        return EnrichOutput(input.n * 10, ctx.headers.get("kontra-hold", ""))

    @nexus.workflow_run_operation
    async def execute(
        self, ctx: nexus.WorkflowRunOperationContext, input: EnrichInput
    ) -> nexus.WorkflowHandle[EnrichOutput]:
        job = JobInput(input.n, ctx.headers.get("kontra-hold", ""), input.hold_for_signal)
        workflow_id = f"job-{uuid.uuid4().hex}"
        STARTED_JOBS.append(workflow_id)
        return await ctx.start_workflow(EnrichJob.run, job, id=workflow_id)


@workflow.defn
class BuyerFlow:
    """A buyer's own workflow that hires through Nexus (PRD §7.2)."""

    @workflow.run
    async def run(self, endpoint: str, n: int) -> list[EnrichOutput]:
        client = workflow.create_nexus_client(service=EnrichService, endpoint=endpoint)
        quote = await client.execute_operation(
            EnrichService.quote, EnrichInput(n), headers={"kontra-hold": "wf-quote"}
        )
        result = await client.execute_operation(
            EnrichService.execute,
            EnrichInput(n),
            headers={"kontra-hold": "wf-execute"},
            schedule_to_close_timeout=timedelta(minutes=2),
        )
        return [quote, result]


# --- the forged-completion probe ---------------------------------------------------------


@dataclass
class ProbeInput:
    tag: str


@dataclass
class ForgedResult:
    n: int
    hold: str
    handler_workflow_id: str = ""


@nexusrpc.service(name=OTHER_SERVICE)
class ProbeService:
    probe: nexusrpc.Operation[ProbeInput, str]


@nexusrpc.handler.service_handler(service=ProbeService)
class ProbeHandler:
    """Another seller's handler. It keeps the callback URL and token Temporal hands every
    handler, which is all a forger starts from."""

    @nexusrpc.handler.sync_operation
    async def probe(self, ctx: nexusrpc.handler.StartOperationContext, input: ProbeInput) -> str:
        PROBE_CALLBACKS.append((ctx.callback_url or "", dict(ctx.callback_headers)))
        return input.tag


@workflow.defn
class ForgedCompletion:
    """Completes immediately; its result is delivered to whatever callbacks it carries."""

    @workflow.run
    async def run(self, marker: str) -> ForgedResult:
        return ForgedResult(-1, marker, "forged")


@workflow.defn
class BuyerComposes:
    """A buyer workflow calling two sellers at once (the `recon` shape, PRD §16 item 11):
    acme's long-running `execute` and other's `probe`."""

    @workflow.run
    async def run(self, acme_endpoint: str, other_endpoint: str) -> EnrichOutput:
        acme = workflow.create_nexus_client(service=EnrichService, endpoint=acme_endpoint)
        other = workflow.create_nexus_client(service=ProbeService, endpoint=other_endpoint)
        first = asyncio.ensure_future(
            acme.execute_operation(
                EnrichService.execute,
                EnrichInput(7, hold_for_signal=True),
                headers={"kontra-hold": "compose"},
                schedule_to_close_timeout=timedelta(minutes=2),
            )
        )
        second = asyncio.ensure_future(
            other.execute_operation(ProbeService.probe, ProbeInput("probe"))
        )
        await second
        return await first


# --- workflow streams and the platform's own workers (decisions I30) -------------------


@dataclass
class StreamJobInput:
    steps: int  # items the workflow itself publishes on each of its two topics


@workflow.defn
class StreamJob:
    """A seller's handler workflow (`job-<id>`) hosting a workflow stream: it publishes on
    two topics, `progress` and `log`, and waits for an `approve` update -- the harness's
    approvals in the dev console's session pane (#54) -- before it completes."""

    @workflow.init
    def __init__(self, job: StreamJobInput) -> None:
        self.stream = WorkflowStream()
        self._approved = False

    @workflow.run
    async def run(self, job: StreamJobInput) -> str:
        progress = self.stream.topic("progress", type=str)
        log = self.stream.topic("log", type=str)
        for i in range(job.steps):
            progress.publish(f"step-{i}")
            log.publish(f"line-{i}")
        await workflow.wait_condition(lambda: self._approved, timeout=timedelta(minutes=2))
        return "approved"

    @workflow.update
    def approve(self, by: str) -> str:
        self._approved = True
        return f"approved by {by}"

    @workflow.signal
    def nudge(self) -> None:
        pass


@workflow.defn
class PlatformPing:
    """A workflow of Kontra's own, run by a versioned platform worker."""

    @workflow.run
    async def run(self) -> str:
        return "pong"


# --- the relay's bound (decisions I39) -------------------------------------------------


@dataclass
class BoundJobInput:
    items: int  # published as the run starts, on the listed topic
    pad: int = 64  # bytes of filler in each


@activity.defn
async def bound_step(n: int) -> int:
    """A step of the seller's own work: one activity per cycle."""
    return n


@workflow.defn
class BoundJob:
    """A seller's handler workflow (`job-<id>`) doing its own work while someone reads its
    stream: it publishes ``items`` items as it starts; each ``work`` signal runs one activity
    and publishes one more item (the seller's own workflow tasks); ``finish`` completes it."""

    @workflow.init
    def __init__(self, job: BoundJobInput) -> None:
        self.stream = WorkflowStream()
        self._queue: list[int] = []
        self._done = False

    @workflow.run
    async def run(self, job: BoundJobInput) -> str:
        events = self.stream.topic("events", type=str)
        for i in range(job.items):
            events.publish(f"item-{i}-" + "p" * job.pad)
        while True:
            await workflow.wait_condition(lambda: bool(self._queue) or self._done)
            if not self._queue:
                return "done"
            n = self._queue.pop(0)
            await workflow.execute_activity(
                bound_step, n, start_to_close_timeout=timedelta(seconds=30)
            )
            events.publish(f"work-{n}")

    @workflow.signal
    def work(self, n: int) -> None:
        self._queue.append(n)

    @workflow.signal
    def finish(self) -> None:
        self._done = True
