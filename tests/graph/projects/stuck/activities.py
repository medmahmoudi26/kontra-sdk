"""The stuck project's activities: their bodies never run under `kontra graph`."""

from models import Odd

from kontra import activity


@activity.defn
async def step() -> int:
    return 1


@activity.defn
async def odd() -> Odd:
    return Odd(code="y")


@activity.defn
async def after() -> None:
    return None
