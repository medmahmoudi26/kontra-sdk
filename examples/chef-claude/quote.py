"""The price function: a flat 25 cents a dish, whatever the craving. Cheap and deterministic:
the handler runs it again on the input it actually receives and refuses a hold below this
price."""

from decimal import Decimal

from models import CookInput

from kontra.serve import Quote

PRICE = Decimal("0.25")


async def quote(inp: CookInput) -> Quote:
    # The contract refuses an empty craving before anything is sent; one of only spaces
    # passes it, and is refused here, before any money is locked.
    if not inp.craving.strip():
        return Quote.reject("the craving is blank")
    return Quote(price=PRICE, currency="USD")
