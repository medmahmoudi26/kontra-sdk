"""The price function: a flat cent per greeting. Cheap and deterministic: the handler runs it
again on the input it actually receives and refuses a hold below this price."""

from decimal import Decimal

from models import HelloInput

from kontra.serve import Quote

PRICE = Decimal("0.01")


async def quote(inp: HelloInput) -> Quote:
    # The contract refuses an empty name (`minLength: 1`) before anything is sent; a name of
    # only spaces passes it, and is refused here, before any money is locked.
    if not inp.name.strip():
        return Quote.reject("the name is blank")
    return Quote(price=PRICE, currency="USD")
