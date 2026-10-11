"""Workflows for the codec's real-Temporal round trip. Plain Temporal, no Kontra import (an
importable module, as the workflow sandbox requires)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import nexusrpc
import nexusrpc.handler
from temporalio import activity, nexus, workflow
from temporalio.exceptions import ApplicationError


@activity.defn
async def shout(text: str) -> str:
    return text.upper()


@workflow.defn
class Child:
    @workflow.run
    async def run(self, text: str) -> str:
        return text + "!"


@workflow.defn
class JobFlow:
    @workflow.run
    async def run(self, text: str) -> str:
        loud = await workflow.execute_activity(
            shout, text, start_to_close_timeout=timedelta(seconds=30)
        )
        return await workflow.execute_child_workflow(
            Child.run, loud, id=workflow.info().workflow_id + "-child"
        )


@workflow.defn
class FailFlow:
    @workflow.run
    async def run(self, text: str) -> str:
        raise ApplicationError("refused: " + text, {"input": text}, non_retryable=True)


# --------------------------------------------------------------------------- Nexus round trip


@dataclass
class Order:
    """The ``execute`` input. The job id travels in the input only because this test has no
    hold token; the handler never parses it out of anything."""

    job_id: str
    text: str


@nexusrpc.service(name="acme.enrich")
class Enrich:
    quote: nexusrpc.Operation[Order, str]
    execute: nexusrpc.Operation[Order, str]


@workflow.defn
class EnrichJob:
    """The root handler workflow ``job-<id>``."""

    @workflow.run
    async def run(self, text: str) -> str:
        return text.upper()


@nexusrpc.handler.service_handler(service=Enrich)
class EnrichHandler:
    @nexusrpc.handler.sync_operation
    async def quote(self, ctx: nexusrpc.handler.StartOperationContext, order: Order) -> str:
        return f"price:{len(order.text)}"

    @nexus.workflow_run_operation
    async def execute(
        self, ctx: nexus.WorkflowRunOperationContext, order: Order
    ) -> nexus.WorkflowHandle[str]:
        if order.text == "refuse":
            raise nexusrpc.HandlerError(
                "refused by the handler",
                type=nexusrpc.HandlerErrorType.BAD_REQUEST,
                retryable_override=False,
            )
        return await ctx.start_workflow(EnrichJob.run, order.text, id="job-" + order.job_id)


@workflow.defn
class Hire:
    """A workflow that calls an offering through Nexus (§7.2): a buyer's, or a seller's
    workflow subcontracting another offering."""

    @workflow.run
    async def run(self, endpoint: str, operation: str, order: Order) -> str:
        client = workflow.create_nexus_client(service=Enrich, endpoint=endpoint)
        timeout = timedelta(seconds=60)
        if operation == "quote":
            return await client.execute_operation(
                Enrich.quote, order, schedule_to_close_timeout=timeout
            )
        return await client.execute_operation(
            Enrich.execute, order, schedule_to_close_timeout=timeout
        )
