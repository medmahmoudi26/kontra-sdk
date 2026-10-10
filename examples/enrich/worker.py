"""Binds code to the offering; all worker tuning lives here (D11). Every keyword below
`activities` is a `temporalio.worker.Worker` argument, passed through verbatim."""

from datetime import timedelta

from activities import fetch_batch, write_records
from quote import quote
from workflows import EnrichFlow

from kontra.serve import Worker, offering

worker = Worker(
    offerings=[offering("offering.yaml", workflow=EnrichFlow, quote=quote)],
    activities=[fetch_batch, write_records],
    max_concurrent_activities=40,
    max_concurrent_workflow_tasks=20,
    max_activities_per_second=25,  # this worker
    max_task_queue_activities_per_second=50,  # server-enforced across all your workers
    graceful_shutdown_timeout=timedelta(seconds=60),
)

if __name__ == "__main__":
    worker.run()
