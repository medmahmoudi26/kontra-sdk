"""Binds the code to the offering; all worker tuning lives here (D11), and this is the one file
that imports `kontra` (decisions I29). `serve()` binds offering.yaml and quote.py beside this
file to `SweepFlow`, and registers every `@activity.defn` function of this project (the
modules imported from this directory, its tests left out). Every keyword is a
`temporalio.worker.Worker` argument, passed through verbatim."""

from datetime import timedelta

from workflows import SweepFlow

from kontra import serve

serve(
    SweepFlow,
    max_concurrent_activities=50,  # a job keeps 50 URLs in flight (CHECKS_AT_ONCE)
    graceful_shutdown_timeout=timedelta(seconds=60),
)
