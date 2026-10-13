from decimal import Decimal

from models import VaultInput

from kontra.serve import Quote


async def quote(inp: VaultInput) -> Quote:
    return Quote(price=Decimal("0.10"), currency="USD")
