"""Two workflows, each bound to an offering of its own, each running its own activity."""

from datetime import timedelta

from kontra import workflow

with workflow.unsafe.imports_passed_through():
    from activities import act_a, act_b
    from models import TwoInput, TwoOutput

T = timedelta(seconds=30)


@workflow.defn
class AFlow:
    @workflow.run
    async def run(self, inp: TwoInput) -> TwoOutput:
        text = await workflow.execute_activity(act_a, inp.name, start_to_close_timeout=T)
        return TwoOutput(text=text)


@workflow.defn
class BFlow:
    @workflow.run
    async def run(self, inp: TwoInput) -> TwoOutput:
        text = await workflow.execute_activity(act_b, inp.name, start_to_close_timeout=T)
        return TwoOutput(text=text)
