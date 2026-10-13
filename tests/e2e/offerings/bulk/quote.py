from decimal import Decimal

from models import BulkInput

from kontra.serve import Quote


async def quote(inp: BulkInput) -> Quote:
    return Quote(price=Decimal("0.05"), currency="USD")
