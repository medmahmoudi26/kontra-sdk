"""Workflows and activities for the control-plane tests: plain Temporal, no Kontra (an
importable module, as the workflow sandbox requires)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError, NexusOperationError


@dataclass
class Plan:
    """What :class:`Job` does: ``ok`` (two activities, then the text upper-cased), ``fail``,
    ``slow`` (sleeps an hour), ``hang`` (an activity that runs until cancelled), or
    ``can<n>`` (two activities, then continue-as-new ``n`` more times)."""

    text: str


@dataclass
class Hire:
    """What :class:`BuyerFlow` calls: ``execute`` on ``endpoint`` / ``service`` with the
    hold token in ``hold_header`` (the name a buyer's workflow happens to spell it with)."""

    endpoint: str
    service: str
    workflow_id: str
    hold: str
    text: str = "ok"
    hold_header: str = "kontra-hold"


@activity.defn
async def step(n: int) -> int:
    activity.heartbeat({"percent": 50 * n})
    return n


@activity.defn
async def hang() -> None:
    while True:
        activity.heartbeat()
        await asyncio.sleep(0.2)


@workflow.defn
class Job:
    @workflow.run
    async def run(self, plan: Plan) -> str:
        text = plan.text
        if text == "fail":
            raise ApplicationError("the job failed on purpose", non_retryable=True)
        if text == "slow":
            await workflow.sleep(timedelta(hours=1))
        if text == "hang":
            await workflow.execute_activity(
                hang,
                start_to_close_timeout=timedelta(hours=1),
                heartbeat_timeout=timedelta(seconds=10),
            )
        for i in range(2):
            await workflow.execute_activity(step, i, start_to_close_timeout=timedelta(seconds=30))
        if text.startswith("can") and int(text[3:]) > 0:
            workflow.continue_as_new(Plan(f"can{int(text[3:]) - 1}"))
        return text.upper()


@workflow.defn
class BuyerFlow:
    """A buyer's workflow hiring through Nexus (the workflow door): the caller is this
    workflow, so its own history records the call."""

    @workflow.run
    async def run(self, hire: Hire) -> str:
        nexus = workflow.create_nexus_client(service=hire.service, endpoint=hire.endpoint)
        return await nexus.execute_operation(
            "execute",
            Plan(hire.text),
            output_type=str,
            headers={"x-workflow-id": hire.workflow_id, hire.hold_header: hire.hold},
            schedule_to_close_timeout=timedelta(minutes=5),
        )


@workflow.defn
class CancellingBuyerFlow:
    """A buyer's workflow that cancels its own ``execute`` call when signalled (the workflow
    door): its history then records ``NexusOperationCancelRequested`` for the call."""

    def __init__(self) -> None:
        self._cancel = False

    @workflow.signal
    def cancel_call(self) -> None:
        self._cancel = True

    @workflow.run
    async def run(self, hire: Hire) -> str:
        nexus = workflow.create_nexus_client(service=hire.service, endpoint=hire.endpoint)
        handle = await nexus.start_operation(
            "execute",
            Plan(hire.text),
            output_type=str,
            headers={"x-workflow-id": hire.workflow_id, hire.hold_header: hire.hold},
            schedule_to_close_timeout=timedelta(minutes=5),
        )
        await workflow.wait_condition(lambda: self._cancel)
        handle.cancel()
        try:
            return await handle
        except NexusOperationError:
            return "canceled"


@workflow.defn
class StartingBuyerFlow:
    """A buyer's workflow that starts its ``execute`` call and then ends without waiting
    for it (the workflow door): its history never records a cancel request for the call."""

    @workflow.run
    async def run(self, hire: Hire) -> str:
        nexus = workflow.create_nexus_client(service=hire.service, endpoint=hire.endpoint)
        await nexus.start_operation(
            "execute",
            Plan(hire.text),
            output_type=str,
            headers={"x-workflow-id": hire.workflow_id, hire.hold_header: hire.hold},
            schedule_to_close_timeout=timedelta(minutes=5),
        )
        return "started"
