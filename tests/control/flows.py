"""Workflows and activities for the control-plane tests: plain Temporal, no Kontra (an
importable module, as the workflow sandbox requires)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError


@dataclass
class Plan:
    """What :class:`Job` does: ``ok`` (two activities, then the text upper-cased), ``fail``,
    ``slow`` (sleeps an hour), ``hang`` (an activity that runs until cancelled), or
    ``can<n>`` (two activities, then continue-as-new ``n`` more times)."""

    text: str


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
