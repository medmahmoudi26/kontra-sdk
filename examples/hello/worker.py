"""Binds the code to the offering; all worker tuning lives here (D11). `kontra serve` runs
the module-level `worker` with your profile, and `kontra publish` imports it to find the
offerings. Every keyword after `activities` is a `temporalio.worker.Worker` argument,
passed through verbatim."""

from datetime import timedelta

from activities import greet
from quote import quote
from workflows import HelloFlow

from kontra.serve import Worker, offering

worker = Worker(
    offerings=[offering("offering.yaml", workflow=HelloFlow, quote=quote)],
    activities=[greet],
    max_concurrent_activities=10,
    graceful_shutdown_timeout=timedelta(seconds=30),
)

if __name__ == "__main__":
    worker.run()
