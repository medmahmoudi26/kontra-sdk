"""The price function: per URL, from the input. Cheap and deterministic: the handler runs it
again on the input it actually receives and refuses a hold below this price."""

from decimal import Decimal

from models import SweepInput

from kontra.serve import Quote

PER_URL = Decimal("0.002")
MINIMUM = Decimal("0.01")


async def quote(inp: SweepInput) -> Quote:
    # The contract caps a job at 200 URLs before anything is sent; the price rounds half up
    # to the cent.
    return Quote(price=max(PER_URL * len(inp.urls), MINIMUM), currency="USD")
