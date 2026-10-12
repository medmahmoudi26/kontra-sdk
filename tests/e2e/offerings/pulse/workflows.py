"""Plain Temporal: nothing from kontra (D1)."""

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import pulse, quiet
    from models import PulseInput, PulseOutput


@workflow.defn
class PulseFlow:
    @workflow.run
    async def run(self, inp: PulseInput) -> PulseOutput:
        beats = quiet_seconds = 0
        if inp.beats:
            beats = await workflow.execute_activity(
                pulse, inp, start_to_close_timeout=timedelta(minutes=5)
            )
        if inp.quiet_seconds:
            quiet_seconds = await workflow.execute_activity(
                quiet, inp.quiet_seconds, start_to_close_timeout=timedelta(minutes=15)
            )
        return PulseOutput(beats=beats, quiet_seconds=quiet_seconds)
