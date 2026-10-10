"""The old Batch loop, as a workflow. Retries, heartbeats and resumption are Temporal's; the
old Session/checkpoint machinery has no counterpart because it is not needed."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import head_batch
    from models import Head, ProbeInput, ProbeOutput

BATCH = 100


@workflow.defn
class ProbeFlow:
    @workflow.run
    async def run(self, inp: ProbeInput) -> ProbeOutput:
        heads: list[Head] = []
        for start in range(0, len(inp.targets), BATCH):
            heads += await workflow.execute_activity(
                head_batch,
                inp.targets[start : start + BATCH],
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
        return ProbeOutput(heads=heads)
