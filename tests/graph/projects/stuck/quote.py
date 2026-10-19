from decimal import Decimal

from models import StuckInput

from kontra.serve import Quote


async def quote(inp: StuckInput) -> Quote:
    return Quote(price=Decimal("0.01"), currency="USD")
