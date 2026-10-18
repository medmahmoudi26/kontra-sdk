"""The two project's activities: one per offering, and one no workflow schedules."""

from kontra import activity


@activity.defn
async def act_a(name: str) -> str:
    return name


@activity.defn
async def act_b(name: str) -> str:
    return name


@activity.defn
async def unused(name: str) -> str:
    return name
