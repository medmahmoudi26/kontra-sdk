"""The test seller's workflows and activities: plain Temporal, nothing from kontra (D1).
An importable module, as Temporal's workflow sandbox requires."""

from __future__ import annotations

import time
from datetime import timedelta

from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from .models import Beats, Echo, Echoed, Order, OrderV2, Shout

_T = timedelta(seconds=30)


@activity.defn
async def shout(text: str) -> str:
    return text.upper()


@activity.defn
def count(text: str) -> int:
    return len(text)


@activity.defn
async def beat(beats: Beats) -> int:
    for i in range(1, beats.count + 1):
        activity.heartbeat({"percent": i, "message": f"{i} {beats.note}"})
    return beats.count


@activity.defn
def sync_beat(beats: Beats) -> int:
    for i in range(1, beats.count + 1):
        activity.heartbeat({"percent": 1000 + i, "message": f"sync {i} {beats.note}"})
        time.sleep(0.001)
    return beats.count


@workflow.defn
class EchoChild:
    @workflow.run
    async def run(self, inp: Echo) -> Echoed:
        job = await workflow.execute_activity("where", start_to_close_timeout=_T, result_type=str)
        return Echoed(text=inp.text, job=job or None)


@workflow.defn
class UpperFlow:
    def __init__(self) -> None:
        self._released = False

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self, order: Order) -> Shout:
        loud = await workflow.execute_activity(shout, order.text, start_to_close_timeout=_T)
        length = await workflow.execute_local_activity(count, order.text, start_to_close_timeout=_T)
        job = await workflow.execute_activity("where", start_to_close_timeout=_T, result_type=str)
        local_job = await workflow.execute_local_activity(
            "where", start_to_close_timeout=_T, result_type=str
        )
        child = await workflow.execute_child_workflow(
            EchoChild.run, Echo(text=loud), id=f"{workflow.info().workflow_id}-echo"
        )
        if order.beats:
            await workflow.execute_activity(
                beat, Beats(count=order.beats, note=order.text), start_to_close_timeout=_T
            )
        if order.sync_beats:
            await workflow.execute_activity(
                sync_beat, Beats(count=order.sync_beats, note=order.text), start_to_close_timeout=_T
            )
        if order.hold:
            await workflow.wait_condition(lambda: self._released)
        return Shout(
            text=child.text * order.copies,
            length=length,
            job=job or None,
            local_job=local_job or None,
            child_job=child.job,
        )


@workflow.defn(name="UpperFlow")
class ImpostorFlow(UpperFlow):
    """``UpperFlow``'s Temporal name on another class: what a plugin that swaps the entry
    class registers (``tests/serve/test_plugin.py``)."""

    @workflow.run
    async def run(self, order: Order) -> Shout:
        return Shout(text=order.text, length=0)


@workflow.defn
class SentinelFlow:
    """Only registered, never run (the kwargs passthrough test)."""

    @workflow.run
    async def run(self, order: Order) -> Shout:
        return Shout(text=order.text, length=0)


@workflow.defn
class UpperFlowV2:
    """The same offering after an edited input type (the hash-lock test)."""

    @workflow.run
    async def run(self, order: OrderV2) -> Shout:
        return Shout(text=order.text.upper(), length=len(order.text))


@workflow.defn
class Chain:
    """``steps`` activities, then ``hops`` continue-as-new more of the same, then waits for
    ever (``wait``) or returns: one workflow id with a chain of runs, as a seller's job may
    be (the history-summary tests)."""

    @workflow.run
    async def run(self, hops: int, steps: int, wait: bool) -> int:
        for _ in range(steps):
            await workflow.execute_activity(shout, "x", start_to_close_timeout=_T)
        if hops:
            workflow.continue_as_new(args=[hops - 1, steps, wait])
        if wait:
            await workflow.wait_condition(lambda: False)
        return steps
