"""A BUYER's workflow that composes two offerings (PRD §7.2, acceptance criterion 11).

This is buyer code, so it imports `kontra.hire`: `service()` is a Nexus client with endpoint
resolution. Each `execute` quotes and holds in an activity (side effects cannot live in
workflow code), then calls the seller's Nexus operation with the hold token.

Both jobs run at once. If the probe fails, the enrichment is cancelled: cancelling the Nexus
operation cancels the seller's workflow, and its hold refunds.
"""

import asyncio
import contextlib
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from kontra.hire import JobFailed, service


@workflow.defn
class Recon:
    @workflow.run
    async def run(self, domain: str) -> dict[str, Any]:
        urls = [f"https://{domain}/", f"https://www.{domain}/"]
        probe = asyncio.ensure_future(
            service("acme/probe", max_price=1).execute(targets=[{"url": u} for u in urls])
        )
        enrich = asyncio.ensure_future(service("acme/enrich", max_price=5).execute(urls=urls))
        try:
            heads = await probe
        except JobFailed:
            enrich.cancel()  # the second job's hold refunds; nothing is paid for a half-recon
            with contextlib.suppress(JobFailed, asyncio.CancelledError):
                await enrich
            raise
        records = await enrich
        return {"domain": domain, "heads": heads.to_dict(), "enrich": records.to_dict()}
