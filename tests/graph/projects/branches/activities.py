"""The branches project's activities: their bodies never run under `kontra graph`."""

from models import BranchesInput, Plan, Verdict
from temporalio import activity


@activity.defn
async def plan(inp: BranchesInput) -> Plan:
    return Plan(items=sorted(set(inp.items)))


@activity.defn
async def fetch_a(p: Plan) -> int:
    return len(p.items)


@activity.defn
async def fetch_b(p: Plan) -> int:
    return len(p.items)


@activity.defn
async def check(item: str) -> bool:
    return bool(item)


@activity.defn
async def classify(checked: int) -> Verdict:
    return Verdict(flag=checked > 0)


@activity.defn
async def accept(checked: int) -> None:
    return None


@activity.defn
async def reject(checked: int) -> None:
    return None


@activity.defn
async def ship(checked: int) -> None:
    return None
