from decimal import Decimal

from models import NestedInput

from kontra.serve import Quote


async def quote(inp: NestedInput) -> Quote:
    return Quote(price=Decimal("0.01"), currency="USD")
