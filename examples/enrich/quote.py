"""The price function. Cheap and deterministic: the handler runs it again on the input it
actually receives and refuses a hold below this price."""

from models import EnrichInput

from kontra.serve import Quote


async def quote(inp: EnrichInput) -> Quote:
    if len(inp.urls) > 10_000:
        return Quote.reject("max 10,000 urls per job")
    return Quote(price=max(len(inp.urls) * 0.001, 0.01), currency="USD")
