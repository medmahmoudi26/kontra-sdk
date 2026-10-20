"""Temporal through the kontra façade (D1): `workflow` and `RetryPolicy` are Temporal's own
objects, so with `from temporalio import ...` this runs on any Temporal. The one Kontra type
the job touches, `Artifact`, comes from models.py as the activity's return value."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import URL_DEADLINE_SECONDS, fetch_batch, write_records
    from models import Batch, EnrichInput, EnrichOutput, Record

BATCH = 50


@workflow.defn
class EnrichFlow:
    @workflow.run
    async def run(self, inp: EnrichInput) -> EnrichOutput:
        records: list[Record] = []
        for start in range(0, len(inp.urls), BATCH):
            records += await workflow.execute_activity(
                fetch_batch,
                Batch(urls=inp.urls[start : start + BATCH], done=start, total=len(inp.urls)),
                # A batch of URLs that all take their full deadline still fits.
                start_to_close_timeout=timedelta(seconds=BATCH * URL_DEADLINE_SECONDS + 60),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
        artifact = await workflow.execute_activity(
            write_records, records, start_to_close_timeout=timedelta(minutes=10)
        )
        ok = sum(1 for r in records if r.status is not None and r.status < 400)
        return EnrichOutput(
            records=artifact,
            summary={"urls": len(inp.urls), "ok": ok, "failed": len(records) - ok},
        )
