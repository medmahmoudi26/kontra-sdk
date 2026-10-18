from decimal import Decimal

from models import BranchesInput

from kontra.serve import Quote


async def quote(inp: BranchesInput) -> Quote:
    return Quote(price=Decimal("0.01"), currency="USD")
