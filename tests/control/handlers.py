"""A Nexus service for the control-plane tests. ``echo`` answers with what it received
(input text and the ``kontra-hold`` header); the other sync operations fail in each of the
ways a seller's handler can; ``run`` starts :class:`~.flows.Job` as a workflow-run
operation under the id given in the ``x-workflow-id`` header, and ``execute`` does the same
the way ``kontra.serve``'s handler starts a job (an id that may run once)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import nexusrpc
import nexusrpc.handler
from nexusrpc.handler import StartOperationContext
from temporalio import nexus
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.nexus import WorkflowRunOperationContext

from .flows import Job, Plan


@dataclass
class Calls:
    run: list[str] = field(default_factory=list[str])


def make_handler(service: str, calls: Calls | None = None) -> Any:
    """A handler instance for the service named ``service`` (names vary per test)."""
    seen = calls if calls is not None else Calls()

    class Handler:
        @nexusrpc.handler.sync_operation
        async def echo(self, ctx: StartOperationContext, plan: Plan) -> dict[str, Any]:
            return {"text": plan.text, "hold": ctx.headers.get("kontra-hold")}

        @nexusrpc.handler.sync_operation
        async def boom(self, ctx: StartOperationContext, plan: Plan) -> dict[str, Any]:
            raise RuntimeError("quote exploded")

        @nexusrpc.handler.sync_operation
        async def bad(self, ctx: StartOperationContext, plan: Plan) -> dict[str, Any]:
            raise nexusrpc.HandlerError("bad input", type=nexusrpc.HandlerErrorType.BAD_REQUEST)

        @nexusrpc.handler.sync_operation
        async def refuse(self, ctx: StartOperationContext, plan: Plan) -> dict[str, Any]:
            raise nexusrpc.OperationError(
                "refused on purpose", state=nexusrpc.OperationErrorState.FAILED
            )

        @nexusrpc.handler.sync_operation
        async def slow(self, ctx: StartOperationContext, plan: Plan) -> dict[str, Any]:
            await asyncio.sleep(8)
            return {"text": plan.text}

        @nexus.workflow_run_operation
        async def run(
            self, ctx: WorkflowRunOperationContext, plan: Plan
        ) -> nexus.WorkflowHandle[str]:
            workflow_id = ctx.headers["x-workflow-id"]
            seen.run.append(workflow_id)
            return await ctx.start_workflow(Job.run, plan, id=workflow_id)

        @nexus.workflow_run_operation
        async def execute(
            self, ctx: WorkflowRunOperationContext, plan: Plan
        ) -> nexus.WorkflowHandle[str]:
            workflow_id = ctx.headers["x-workflow-id"]
            seen.run.append(workflow_id)
            return await ctx.start_workflow(
                Job.run,
                plan,
                id=workflow_id,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            )

    return nexusrpc.handler.service_handler(name=service)(Handler)()
