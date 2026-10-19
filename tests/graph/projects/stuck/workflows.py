"""One step, an activity whose result cannot be sampled, then: done, a loop that never
waits, a wait nothing ends, or a crash on the input."""

from datetime import timedelta

from kontra import workflow

with workflow.unsafe.imports_passed_through():
    from activities import after, odd, step
    from models import StuckInput, StuckOutput

T = timedelta(seconds=30)


@workflow.defn
class StuckFlow:
    def __init__(self) -> None:
        self.go = False

    @workflow.signal
    def release(self) -> None:
        self.go = True

    @workflow.run
    async def run(self, inp: StuckInput) -> StuckOutput:
        await workflow.execute_activity(step, start_to_close_timeout=T)
        await workflow.execute_activity(odd, start_to_close_timeout=T)
        while inp.then == "loop":
            await workflow.execute_activity(step, start_to_close_timeout=T)
        if inp.then == "wait":
            await workflow.wait_condition(lambda: self.go)  # no timeout: never ends
        if inp.then == "crash":
            raise RuntimeError("cannot handle a crash")  # a workflow task failure, retried
        await workflow.execute_activity(after, start_to_close_timeout=T)
        return StuckOutput(done=True)
