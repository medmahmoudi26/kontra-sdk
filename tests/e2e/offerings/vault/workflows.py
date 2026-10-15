"""Temporal through the façade: kontra.workflow and kontra.activity only (D1)."""

from datetime import timedelta

from kontra import RetryPolicy, workflow

with workflow.unsafe.imports_passed_through():
    from activities import reseal
    from models import VaultInput, VaultOutput


@workflow.defn
class VaultFlow:
    @workflow.run
    async def run(self, inp: VaultInput) -> VaultOutput:
        return await workflow.execute_activity(
            reseal,
            inp,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )
