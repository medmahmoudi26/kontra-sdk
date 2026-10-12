"""Plain Temporal: nothing from kontra (D1)."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import produce
    from models import BulkInput, BulkOutput


@workflow.defn
class BulkFlow:
    @workflow.run
    async def run(self, inp: BulkInput) -> BulkOutput:
        return await workflow.execute_activity(
            produce,
            inp,
            start_to_close_timeout=timedelta(minutes=50),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
