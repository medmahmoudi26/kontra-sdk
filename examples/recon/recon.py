"""A BUYER's workflow that composes two offerings (PRD §7.2, acceptance criterion 11).

This is buyer code, so it imports `kontra.hire`: `service()` is a Nexus client with endpoint
resolution. Each `execute` quotes and holds in an activity (side effects cannot live in
workflow code), then calls the seller's Nexus operation with the hold token.

Both jobs run at once. If the probe does not deliver -- it fails, its quote is refused, or
this workflow is cancelled meanwhile -- the enrichment is cancelled while it still runs:
its hold refunds at once and the seller's workflow is cancelled through Nexus, so nothing
is paid for half a recon. (An enrichment that already delivered is the buyer's to accept
or reject like any delivery.)
"""

import asyncio
import contextlib
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from kontra.hire import service


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
        except (Exception, asyncio.CancelledError):
            enrich.cancel()  # its hold refunds; nothing is paid for half a recon
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await enrich
            raise
        records = await enrich
        return {"domain": domain, "heads": heads.to_dict(), "enrich": records.to_dict()}
