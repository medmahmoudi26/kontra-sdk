"""One activity drives an agent whose loop is a process (Claude Code, in activities.py), so
the workflow is Temporal's plain shape for it: run the activity with a heartbeat timeout --
a worker that crashed is noticed within it -- and a retry policy; the retry resumes the
binary's own session. Vanilla Temporal, no `kontra` import (decisions I29); nothing here
knows agents."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import cook
    from models import CookInput, Dish


@workflow.defn
class ChefFlow:
    @workflow.run
    async def run(self, inp: CookInput) -> Dish:
        return await workflow.execute_activity(
            cook,
            inp,
            start_to_close_timeout=timedelta(minutes=15),
            heartbeat_timeout=timedelta(seconds=20),
            retry_policy=RetryPolicy(maximum_attempts=5, non_retryable_error_types=["MaxTurns"]),
        )
