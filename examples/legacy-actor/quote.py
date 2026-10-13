from models import ProbeInput

from kontra.serve import Quote


async def quote(inp: ProbeInput) -> Quote:
    return Quote(price=max(len(inp.targets) * 0.0005, 0.01), currency="USD")
