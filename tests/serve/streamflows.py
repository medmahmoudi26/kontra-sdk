"""Vanilla Temporal workflows and activities for the stream tests (decisions I29, I31):
nothing here imports kontra. A seller publishes with temporalio's own API."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.workflow_streams import (
        WorkflowStream,
        WorkflowStreamClient,
        WorkflowStreamState,
    )

LISTED = "events"
UNLISTED = "private"


@dataclass
class Burst:
    """``n`` items on each of ``topics``, from an activity; ``marker`` goes in every item."""

    n: int
    marker: str = ""
    topics: tuple[str, ...] = (LISTED, UNLISTED)
    pause: float = 0.02
    heartbeat: bool = False


@activity.defn
async def burst(spec: Burst) -> None:
    client = WorkflowStreamClient.from_within_activity(batch_interval=timedelta(milliseconds=20))
    async with client:
        for i in range(spec.n):
            for topic in spec.topics:
                client.topic(topic).publish(
                    {"i": i, "secret": f"{spec.marker}-{topic}-activity-{i}"}
                )
            if spec.heartbeat:
                activity.heartbeat({"percent": i, "message": f"{spec.marker}-heartbeat-{i}"})
            await asyncio.sleep(spec.pause)


@workflow.defn
class HostedJob:
    """Hosts no stream: Kontra hosts one. Publishes from an activity, then waits for
    ``finish`` (or returns at once with ``wait=False``)."""

    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self, spec: Burst, wait: bool = True) -> str:
        await workflow.execute_activity(burst, spec, start_to_close_timeout=timedelta(minutes=1))
        if wait:
            await workflow.wait_condition(lambda: self.done)
        return "ok"

    @workflow.signal
    def finish(self) -> None:
        self.done = True


@dataclass
class OwnInput:
    n: int
    marker: str = ""
    rounds: int = 1
    state: WorkflowStreamState | None = None


@workflow.defn
class OwnStreamJob:
    """Hosts its own stream in ``@workflow.init`` and continues-as-new ``rounds - 1`` times
    with the contrib's recipe (``prior_state``). Publishes from the workflow and from an
    activity."""

    @workflow.init
    def __init__(self, inp: OwnInput) -> None:
        self.stream = WorkflowStream(inp.state)
        self.done = False

    @workflow.run
    async def run(self, inp: OwnInput) -> str:
        for i in range(inp.n):
            self.stream.topic(LISTED).publish(
                {"i": i, "secret": f"{inp.marker}-events-workflow-{i}"}
            )
            self.stream.topic(UNLISTED).publish({"secret": f"{inp.marker}-private-workflow-{i}"})
        await workflow.execute_activity(
            burst, Burst(inp.n, inp.marker), start_to_close_timeout=timedelta(minutes=1)
        )
        if inp.rounds > 1:
            await self.stream.continue_as_new(
                lambda state: [OwnInput(inp.n, inp.marker, inp.rounds - 1, state)]
            )
        await workflow.wait_condition(lambda: self.done)
        return "ok"

    @workflow.signal
    def finish(self) -> None:
        self.done = True


@workflow.defn
class ChainJob:
    """Hosts no stream and continues-as-new: Kontra carries the offsets across the chain."""

    @workflow.run
    async def run(self, rounds: int, per_round: int = 2) -> str:
        await workflow.execute_activity(
            burst,
            Burst(per_round, topics=(LISTED,)),
            start_to_close_timeout=timedelta(minutes=1),
        )
        if rounds > 1:
            workflow.continue_as_new(args=[rounds - 1, per_round])
        return "ok"
