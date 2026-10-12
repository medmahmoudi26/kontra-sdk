"""The test seller's workflow and activity: plain Temporal, no Kontra (an importable
module, as the workflow sandbox requires)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError


@dataclass
class Order:
    """The ``execute`` input, shaped like ``support.EnrichInput``. ``mode``: ``ok``,
    ``fail`` (the workflow fails), ``slow`` (sleeps an hour, until stopped), ``stubborn``
    (works until signalled ``finish``, and completes then even if it was asked to cancel:
    an honest seller whose last step finishes regardless)."""

    text: str
    mode: str = "ok"


@activity.defn
async def shout(text: str, part: int) -> str:
    activity.heartbeat({"percent": 50 * (part + 1), "message": f"part {part}"})
    await asyncio.sleep(0.3)
    return text.upper()


@workflow.defn
class EnrichJob:
    def __init__(self) -> None:
        self._finish = False

    @workflow.signal
    def finish(self) -> None:
        self._finish = True

    @workflow.run
    async def run(self, order: Order) -> dict[str, str]:
        if order.mode == "stubborn":
            while not self._finish:
                try:
                    await workflow.wait_condition(lambda: self._finish)
                except asyncio.CancelledError:
                    continue  # a cancel request changes nothing: it finishes when told
            return {"text": order.text.upper()}
        if order.mode == "fail":
            raise ApplicationError("the seller's job failed", non_retryable=True)
        if order.mode == "slow":
            await workflow.sleep(timedelta(hours=1))
        loud = ""
        for part in range(2):
            loud = await workflow.execute_activity(
                shout, args=[order.text, part], start_to_close_timeout=timedelta(seconds=30)
            )
        return {"text": loud}
