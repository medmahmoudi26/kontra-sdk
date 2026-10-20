"""Vanilla Temporal workflows and activities for the stream tests (decisions I29, I31):
nothing here imports kontra. A seller publishes with temporalio's own API."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from pydantic import BaseModel, Field, TypeAdapter
from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.workflow_streams import (
        WorkflowStream,
        WorkflowStreamClient,
        WorkflowStreamState,
    )

LISTED = "events"
UNLISTED = "private"
#: The unlisted topic of the seam ⑤ seller (``tests/serve/test_streams.py``): its name must
#: be nowhere in the clear.
AUDIT = "private_audit"


@dataclass
class Burst:
    """``n`` items on each of ``topics``, from an activity; ``marker`` goes in every item,
    and ``pad`` bytes of filler."""

    n: int
    marker: str = ""
    topics: tuple[str, ...] = (LISTED, UNLISTED)
    pause: float = 0.02
    heartbeat: bool = False
    pad: int = 0


@activity.defn
async def burst(spec: Burst) -> None:
    client = WorkflowStreamClient.from_within_activity(batch_interval=timedelta(milliseconds=20))
    async with client:
        for i in range(spec.n):
            for topic in spec.topics:
                item = {"i": i, "secret": f"{spec.marker}-{topic}-activity-{i}"}
                if spec.pad:
                    item["pad"] = "p" * spec.pad
                client.topic(topic).publish(item)
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
class FilledJob:
    """Hosts its own stream and publishes ``n`` items of ``pad`` bytes as its run starts,
    then waits for ``finish`` (or returns at once with ``wait=False``): polls sent before
    its first workflow task are all answered in that one task."""

    @workflow.init
    def __init__(self, n: int, pad: int, wait: bool = True) -> None:
        self.stream = WorkflowStream()
        self.done = False

    @workflow.run
    async def run(self, n: int, pad: int, wait: bool = True) -> str:
        for i in range(n):
            self.stream.topic(LISTED).publish({"i": i, "pad": "p" * pad})
        if wait:
            await workflow.wait_condition(lambda: self.done)
        return "ok"

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.signal
    def more(self, n: int) -> None:
        """``n`` more items, published by the workflow."""
        for i in range(n):
            self.stream.topic(LISTED).publish({"more": i})

    @workflow.update
    def ping(self) -> str:
        """One of the seller's own updates (an approval, say)."""
        return "pong"


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


# --------------------------------------------------------------------------- an offering


class StreamOrder(BaseModel):
    """The seam ⑤ seller's input: ``n`` items per topic and place, ``rounds`` runs (it
    continues-as-new between them, its stream's state carried in ``resume``)."""

    marker: str = Field(max_length=200)
    n: int = Field(default=2, ge=1, le=20)
    rounds: int = Field(default=2, ge=1, le=3)
    resume: str | None = Field(default=None, max_length=1_000_000)


class StreamOut(BaseModel):
    published: int


_STATE = TypeAdapter(WorkflowStreamState)


@activity.defn
async def publish_both(order: StreamOrder) -> int:
    """Listed and unlisted items from an activity, and heartbeats: every one with a marker
    unique to its place."""
    client = WorkflowStreamClient.from_within_activity(batch_interval=timedelta(milliseconds=50))
    async with client:
        for i in range(order.n):
            run = activity.info().workflow_run_id
            client.topic(LISTED).publish({"s": f"{order.marker}-w-{LISTED}-activity-{run}-{i}"})
            client.topic(AUDIT).publish({"s": f"{order.marker}-w-{AUDIT}-activity-{run}-{i}"})
            activity.heartbeat({"message": f"{order.marker}-w-progress-heartbeat-{run}-{i}"})
    return 2 * order.n


@workflow.defn
class SellerStreamJob:
    """Hosts its own stream (``prior_state`` across continue-as-new, the contrib's recipe),
    publishes from the workflow and from an activity, then waits for ``finish``."""

    @workflow.init
    def __init__(self, order: StreamOrder) -> None:
        state = _STATE.validate_json(order.resume) if order.resume else None
        self.stream = WorkflowStream(state)
        self.done = False

    @workflow.run
    async def run(self, order: StreamOrder) -> StreamOut:
        run = workflow.info().run_id
        for i in range(order.n):
            self.stream.topic(LISTED).publish(
                {"s": f"{order.marker}-w-{LISTED}-workflow-{run}-{i}"}
            )
            self.stream.topic(AUDIT).publish({"s": f"{order.marker}-w-{AUDIT}-workflow-{run}-{i}"})
        published = await workflow.execute_activity(
            publish_both, order, start_to_close_timeout=timedelta(minutes=1)
        )
        if order.rounds > 1:
            nxt = order.rounds - 1
            await self.stream.continue_as_new(
                lambda st: [
                    order.model_copy(
                        update={"rounds": nxt, "resume": _STATE.dump_json(st).decode()}
                    )
                ]
            )
        await workflow.wait_condition(lambda: self.done)
        return StreamOut(published=published)

    @workflow.signal
    def finish(self) -> None:
        self.done = True
