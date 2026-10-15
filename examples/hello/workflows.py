"""Temporal through the kontra façade (D1): `workflow` and `RetryPolicy` are Temporal's own
objects, so with `from temporalio import workflow` this runs on any Temporal. Kontra starts
it with the id `job-<job id>` when a buyer hires the offering."""

from datetime import timedelta

from kontra import RetryPolicy, workflow

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
