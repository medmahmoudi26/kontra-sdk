"""Binds the code to the offering; all worker tuning lives here (D11). `serve()` binds
offering.yaml and quote.py beside this file to `SweepFlow` and registers every activity the
workflow imports, and `kontra.batch.v1`, the child `execute_activities_in_parallel` runs as.
Every keyword is a `temporalio.worker.Worker` argument, passed through verbatim."""

from datetime import timedelta

from workflows import SweepFlow

from kontra import serve

serve(
    SweepFlow,
    max_concurrent_activities=50,  # a batch keeps 50 URLs in flight
    graceful_shutdown_timeout=timedelta(seconds=60),
)
