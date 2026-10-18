"""A local activity (history has only its marker); a child workflow whose own child, a
batch, is the only one to schedule `probe`; a batch over pages (a Page's items have no
minItems, so run 0's first page is empty and only `list_pages` is scheduled)."""

from datetime import timedelta

from kontra import workflow

with workflow.unsafe.imports_passed_through():
    from activities import list_pages, note, probe, visit
    from models import NestedInput, NestedOutput

T = timedelta(seconds=30)


def add(acc: int, n: int) -> int:
    return acc + n


@workflow.defn
class Inner:
    @workflow.run
    async def run(self, names: list[str]) -> list[bool]:
        return await workflow.execute_activities_in_parallel(
            probe, items=names, start_to_close_timeout=T
        )


@workflow.defn
class NestedFlow:
    @workflow.run
    async def run(self, inp: NestedInput) -> NestedOutput:
        n = await workflow.execute_local_activity(note, inp, start_to_close_timeout=T)
        await workflow.execute_child_workflow(
            Inner.run, inp.names, id=f"{workflow.info().workflow_id}-inner"
        )
        seen = await workflow.execute_activities_in_parallel(
            visit, pages=list_pages, reduce=add, init=0, start_to_close_timeout=T
        )
        return NestedOutput(seen=n + seen)
