"""Plain Temporal: no kontra import anywhere in here (D1), so this runs on any Temporal.
Kontra starts it with the id `job-<job id>` when a buyer hires the offering."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import greet
    from models import HelloInput, HelloOutput


@workflow.defn
class HelloFlow:
    @workflow.run
    async def run(self, inp: HelloInput) -> HelloOutput:
        return await workflow.execute_activity(
            greet,
            inp.name,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
