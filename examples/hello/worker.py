"""Binds the code to the offering; all worker tuning lives here (D11), and this is the one file
that imports `kontra` (decisions I29). `serve()` binds offering.yaml and quote.py beside this
file to `HelloFlow`, and registers every `@activity.defn` function of this project: the
modules imported from this directory, its tests left out. `kontra serve` (or
`python worker.py`) runs it with your profile, and `kontra publish` imports it to find the
offering. Every keyword is a `temporalio.worker.Worker` argument, passed through verbatim."""

from datetime import timedelta

from workflows import HelloFlow

from kontra import serve

serve(HelloFlow, max_concurrent_activities=10, graceful_shutdown_timeout=timedelta(seconds=30))
