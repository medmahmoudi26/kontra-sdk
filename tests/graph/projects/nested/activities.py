"""The nested project's activities: their bodies never run under `kontra graph`."""

from models import NestedInput

from kontra import Page, activity


@activity.defn
async def note(inp: NestedInput) -> int:
    return len(inp.names)


@activity.defn
async def probe(name: str) -> bool:
    return bool(name)


@activity.defn
async def list_pages(cursor: str | None) -> Page[str]:
    return Page(items=[])


@activity.defn
async def visit(name: str) -> int:
    return 1
