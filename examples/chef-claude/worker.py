"""Binds the code to the offering; all worker tuning lives here (D11). `serve()` binds
offering.yaml and quote.py beside this file to `ChefFlow` and registers every activity the
workflow imports. Every keyword is a `temporalio.worker.Worker` argument, passed through
verbatim.

The `claude` binary and its credentials are this machine's too -- `claude` on PATH, logged
in or given `ANTHROPIC_API_KEY` -- worker config, like the number of activity slots."""

from datetime import timedelta

from workflows import ChefFlow

from kontra import serve

# A job holds one `claude` process while it cooks: cap them at what this machine, and your
# Anthropic rate limit, carry.
serve(ChefFlow, max_concurrent_activities=4, graceful_shutdown_timeout=timedelta(seconds=30))
