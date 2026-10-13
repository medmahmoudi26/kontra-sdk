from decimal import Decimal

from models import PulseInput

from kontra.serve import Quote


async def quote(inp: PulseInput) -> Quote:
    return Quote(price=Decimal("0.02"), currency="USD")
