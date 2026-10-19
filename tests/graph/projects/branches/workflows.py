"""Plan; fan out to two different activities; check every item in a batch; branch on an
activity's result; wait for an approval that only a signal brings (a timer ends the wait)."""

import asyncio
from datetime import timedelta

from kontra import workflow

with workflow.unsafe.imports_passed_through():
    from activities import accept, check, classify, fetch_a, fetch_b, plan, reject, ship
    from models import BranchesInput, BranchesOutput

T = timedelta(seconds=30)


def count(acc: int, ok: bool) -> int:
    return acc + 1 if ok else acc


@workflow.defn
class BranchesFlow:
    def __init__(self) -> None:
        self.approved = False

    @workflow.signal
    def approve(self) -> None:
        self.approved = True

    @workflow.run
    async def run(self, inp: BranchesInput) -> BranchesOutput:
        p = await workflow.execute_activity(plan, inp, start_to_close_timeout=T)
        await asyncio.gather(
            workflow.execute_activity(fetch_a, p, start_to_close_timeout=T),
            workflow.execute_activity(fetch_b, p, start_to_close_timeout=T),
        )
        checked = await workflow.execute_activities_in_parallel(
            check, items=p.items, reduce=count, init=0, start_to_close_timeout=T
        )
        verdict = await workflow.execute_activity(classify, checked, start_to_close_timeout=T)
        if verdict.flag:
            await workflow.execute_activity(accept, checked, start_to_close_timeout=T)
        else:
            await workflow.execute_activity(reject, checked, start_to_close_timeout=T)
        await workflow.wait_condition(lambda: self.approved, timeout=timedelta(days=1))
        if self.approved:
            await workflow.execute_activity(ship, checked, start_to_close_timeout=T)
        return BranchesOutput(checked=checked, shipped=self.approved)
