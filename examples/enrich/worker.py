"""Binds code to the offering; all worker tuning lives here (D11). `serve()` binds
offering.yaml and quote.py beside this file to `EnrichFlow` and registers every activity the
workflow imports. Every keyword is a `temporalio.worker.Worker` argument, passed through
verbatim."""

from datetime import timedelta

from workflows import EnrichFlow

from kontra import serve

serve(
    EnrichFlow,
    max_concurrent_activities=40,
    max_concurrent_workflow_tasks=20,
    max_activities_per_second=25,  # this worker
    max_task_queue_activities_per_second=50,  # server-enforced across all your workers
    graceful_shutdown_timeout=timedelta(seconds=60),
)
