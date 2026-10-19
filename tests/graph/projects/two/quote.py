from decimal import Decimal

from models import TwoInput

from kontra.serve import Quote


async def quote(inp: TwoInput) -> Quote:
    return Quote(price=Decimal("0.01"), currency="USD")
