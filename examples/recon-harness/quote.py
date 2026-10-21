"""The price function: a cent per domain swept. Cheap and deterministic: the handler runs it
again on the input it actually receives and refuses a hold below this price.

A hire is one turn of the agent, with nobody on the other side to approve a tool call or
answer a callback (multi-turn sessions are a later Kontra feature, kontra-sdk#60), so a sweep
that asks for either is refused here, before any money is locked. The dev console drives
both for an agent run on its own (README.md)."""

from decimal import Decimal

from models import SweepRequest

from kontra.serve import Quote

PER_DOMAIN = Decimal("0.01")


async def quote(req: SweepRequest) -> Quote:
    if req.want_approval or req.want_callback:
        return Quote.reject("one hire is one turn: approvals and callbacks need a session")
    if any(not d.strip() for d in req.domains):
        return Quote.reject("a domain is blank")
    return Quote(price=PER_DOMAIN * len(req.domains), currency="USD")
