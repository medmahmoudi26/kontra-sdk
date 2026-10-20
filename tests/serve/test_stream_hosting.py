"""The job's workflow stream, hosted, marked, guarded and drained by Kontra's workflow
interceptor (decisions I31), on a dev server: a root that hosts no stream gets Kontra's, one
that hosts its own keeps it; a poll in the run's first activation is answered; the tail of a
short job reaches the relay; the answers of one workflow task stay within its budget, and the
drain waits for the ones held back; a continue-as-new chain keeps offsets monotonic and loses
nothing; the history guard stops the relay; every history replays."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowHistory, WorkflowUpdateStage
from temporalio.common import RawValue
from temporalio.contrib.workflow_streams import PollInput, WorkflowStreamClient
from temporalio.worker import Replayer, Worker

from kontra import _relay
from kontra.codec import KontraCodec
from kontra.codec import stream as s
from kontra.codec.age import generate_identity
from kontra.contract.names import PROGRESS_TOPIC, STREAM_POLL_UPDATE, job_workflow_id, new_job_id
from kontra.serve import interceptors, seller_data_converter
from kontra.serve.interceptors import KontraInterceptor
from kontra.testing.devserver import DevServer

from .streamflows import (
    LISTED,
    UNLISTED,
    Burst,
    ChainJob,
    FilledJob,
    HostedJob,
    OwnInput,
    OwnStreamJob,
    burst,
)

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]

SELLER = generate_identity()
BUYER = generate_identity()
MARKER = "MARKER-hosting-41f0"


class Keys:
    async def buyer_key(self, job_id: str) -> str:
        return BUYER[1]


@dataclass
class Rig:
    dev: DevServer
    seller: Client
    plain: Client  # the relay's: no codec
    queue: str
    interceptor: KontraInterceptor
    started: list[str]

    async def start(self, wf: Any, *args: Any) -> tuple[str, Any]:
        job = new_job_id()
        handle = await self.seller.start_workflow(
            wf, args=list(args), id=job_workflow_id(job), task_queue=self.queue
        )
        self.started.append(job_workflow_id(job))
        return job, handle

    def closed(self, job: str) -> Any:
        async def closed() -> bool:
            d = await self.plain.get_workflow_handle(job_workflow_id(job)).describe()
            return d.status not in (
                WorkflowExecutionStatus.RUNNING,
                WorkflowExecutionStatus.CONTINUED_AS_NEW,
            )

        return closed

    def relay(self, job: str, **kw: Any) -> AsyncIterator[_relay.RelayBatch]:
        kw.setdefault("topics", [LISTED, PROGRESS_TOPIC])
        kw.setdefault("start", 0)
        kw.setdefault("min_interval", 0.0)
        kw.setdefault("rpc_timeout", 5.0)
        return _relay.relay(
            self.plain.workflow_service,
            namespace="default",
            job_id=job,
            closed=self.closed(job),
            **kw,
        )


def seller_client(dev: DevServer) -> Client:
    codec = KontraCodec(SELLER[0], [SELLER[1]], role="seller", buyer_keys=Keys())
    config = dev.client.config()
    config["data_converter"] = seller_data_converter(codec)
    return Client(**config)


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def rig(dev_server: DevServer) -> AsyncIterator[Rig]:
    queue = f"hosting-{uuid.uuid4().hex[:8]}"
    interceptor = KontraInterceptor(None, streams={queue: [LISTED]})
    seller = seller_client(dev_server)
    async with Worker(
        seller,
        task_queue=queue,
        workflows=[HostedJob, OwnStreamJob, ChainJob],
        activities=[burst],
        interceptors=[interceptor],
    ):
        yield Rig(dev_server, seller, dev_server.client, queue, interceptor, [])


async def collect(
    it: AsyncIterator[_relay.RelayBatch], want: int, within: float = 60
) -> list[_relay.RelayItem]:
    got: list[_relay.RelayItem] = []

    async def go() -> None:
        async for batch in it:
            got.extend(batch.items)
            if len(got) >= want:
                return

    await asyncio.wait_for(go(), within)
    return got


async def until_closed(
    it: AsyncIterator[_relay.RelayBatch], within: float = 60
) -> list[_relay.RelayItem]:
    got: list[_relay.RelayItem] = []

    async def go() -> None:
        async for batch in it:
            got.extend(batch.items)

    await asyncio.wait_for(go(), within)
    return got


def opened(item: _relay.RelayItem) -> Any:
    assert item.ciphertext is not None
    from temporalio.contrib.pydantic import PydanticPayloadConverter

    p = s.open_item(item.ciphertext, [BUYER[0]], topic=item.topic, offset=item.offset)
    return PydanticPayloadConverter().from_payload(p)


async def test_a_root_without_a_stream_gets_kontras(rig: Rig) -> None:
    job, handle = await rig.start(HostedJob.run, Burst(3, MARKER))
    items = await collect(rig.relay(job), 3)
    assert [(i.topic, i.offset) for i in items] == [(LISTED, 0), (LISTED, 2), (LISTED, 4)]
    assert [opened(i)["i"] for i in items] == [0, 1, 2]
    # The seller's stock subscriber reads every topic through the codec, unchanged.
    sub = WorkflowStreamClient.create(rig.seller, job_workflow_id(job))
    seen: list[tuple[str, Any]] = []

    async def read() -> None:
        async for item in sub.subscribe(None, from_offset=0, result_type=dict):
            seen.append((item.topic, item.data))
            if len(seen) >= 6:
                return

    await asyncio.wait_for(read(), 30)
    assert [t for t, _ in seen] == [LISTED, UNLISTED] * 3
    assert await sub.get_offset() == 6
    await handle.signal(HostedJob.finish)
    assert await handle.result() == "ok"
    raw = (await rig.plain.get_workflow_handle(job_workflow_id(job)).fetch_history()).to_json()
    assert MARKER not in raw
    assert UNLISTED not in raw


async def test_a_root_hosting_its_own_stream_keeps_it_across_its_chain(rig: Rig) -> None:
    job, handle = await rig.start(OwnStreamJob.run, OwnInput(2, MARKER, rounds=2))
    items = await collect(rig.relay(job), 8)
    assert [i.topic for i in items] == [LISTED] * 8
    offsets = [i.offset for i in items]
    assert offsets == sorted(offsets)
    assert len(set(offsets)) == 8
    await handle.signal(OwnStreamJob.finish)
    assert await handle.result() == "ok"


async def test_a_poll_in_the_first_activation_is_answered(rig: Rig, dev_server: DevServer) -> None:
    """The run's first activation applies updates before initialising the workflow: the
    early handler accepts the poll and the stream answers it."""
    queue = f"first-{uuid.uuid4().hex[:8]}"
    job = new_job_id()
    handle = await rig.seller.start_workflow(
        HostedJob.run, args=[Burst(1, MARKER), False], id=job_workflow_id(job), task_queue=queue
    )
    rig.started.append(job_workflow_id(job))
    plain = rig.plain.get_workflow_handle(job_workflow_id(job))
    poll = asyncio.create_task(
        plain.start_update(
            STREAM_POLL_UPDATE,
            PollInput(topics=[LISTED], from_offset=0),
            wait_for_stage=WorkflowUpdateStage.ACCEPTED,
            result_type=RawValue,
        )
    )
    await asyncio.sleep(0.5)  # the update waits with the first workflow task
    assert not poll.done()
    async with Worker(
        rig.seller,
        task_queue=queue,
        workflows=[HostedJob],
        activities=[burst],
        interceptors=[KontraInterceptor(None, streams={queue: [LISTED]})],
    ):
        update = await asyncio.wait_for(poll, 30)
        answer = await asyncio.wait_for(update.result(), 30)
        assert await handle.result() == "ok"
    envelope = s.parse_envelope(answer.payload)  # the relay's view: no key
    assert [(i.topic, i.offset) for i in envelope.items] == [(LISTED, 0)]


async def test_the_drain_delivers_the_tail_of_a_short_job(rig: Rig) -> None:
    """A relay paced at 2 s and a job that publishes and returns at once: without the drain
    the job closes before the next poll."""
    for _ in range(2):
        job, handle = await rig.start(HostedJob.run, Burst(6, topics=(LISTED,)), False)
        items = await until_closed(rig.relay(job, min_interval=2.0))
        assert [i.offset for i in items] == [0, 1, 2, 3, 4, 5]
        assert await handle.result() == "ok"


async def test_the_drain_counts_only_what_the_envelope_carried(rig: Rig) -> None:
    """A short job's tail whose sealed items pass the envelope's 512 KiB in one answer (8 x
    100 KB: one contrib answer, under its 1 MB): the interceptor cuts the answer where the
    envelope would, so the drain waits for the polls that carry the rest."""
    job, handle = await rig.start(HostedJob.run, Burst(8, topics=(LISTED,), pad=100_000), False)
    items = await until_closed(rig.relay(job, min_interval=2.0))
    assert [i.offset for i in items] == list(range(8))
    assert all(len(opened(i)["pad"]) == 100_000 for i in items)
    assert await handle.result() == "ok"


def first_task_answers(history: WorkflowHistory) -> list[Payload]:
    """The poll answers the run's first workflow task completed, as its history stores them
    (the outcomes of the updates completed in that task's reply)."""
    answers: list[Payload] = []
    tasks = 0
    for e in history.events:
        if e.HasField("workflow_task_completed_event_attributes"):
            tasks += 1
            if tasks > 1:
                break
        elif tasks and e.HasField("workflow_execution_update_completed_event_attributes"):
            [answer] = (
                e.workflow_execution_update_completed_event_attributes.outcome.success.payloads
            )
            answers.append(answer)
    return answers


async def polled_before_its_first_task(
    rig: Rig, relays: int, n: int, pad: int, *, wait: bool
) -> tuple[str, Any, list[AsyncIterator[_relay.RelayBatch]], str]:
    """A :class:`FilledJob` started on a queue no worker serves yet, and ``relays`` relays
    whose first polls the server holds for its first workflow task, which then completes
    them all, in one reply. The caller iterates the relays, then starts the worker."""
    queue = f"budget-{uuid.uuid4().hex[:8]}"
    job = new_job_id()
    handle = await rig.seller.start_workflow(
        FilledJob.run, args=[n, pad, wait], id=job_workflow_id(job), task_queue=queue
    )
    return job, handle, [rig.relay(job) for _ in range(relays)], queue


async def replays(rig: Rig, history: WorkflowHistory, interceptor: KontraInterceptor) -> None:
    replayer = Replayer(
        workflows=[FilledJob], interceptors=[interceptor], data_converter=rig.seller.data_converter
    )
    result = await replayer.replay_workflow(history, raise_on_replay_failure=False)
    assert result.replay_failure is None, result.replay_failure


async def test_the_answers_of_one_workflow_task_stay_within_its_budget(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The polls one workflow task completes answer in one reply to it: past
    ``STREAM_ANSWER_BUDGET`` a poll is answered with no items and ``more_ready``, and polls
    again. Four relays' first polls, all completed in the job's first task: one is answered
    (cut to an envelope), the others are held back at offset 0; the job keeps running, and
    every relay then reads the whole stream. The budget is scaled down to one and a half
    envelopes, so the case stays small."""
    budget = s.ENVELOPE_MAX * 3 // 2
    monkeypatch.setattr(interceptors, "STREAM_ANSWER_BUDGET", budget)
    job, handle, relays, queue = await polled_before_its_first_task(rig, 4, 6, 100_000, wait=True)
    reading = [asyncio.create_task(collect(r, 6)) for r in relays]
    await asyncio.sleep(1.0)  # the polls wait for the first workflow task
    interceptor = KontraInterceptor(None, streams={queue: [LISTED]})
    async with Worker(
        rig.seller, task_queue=queue, workflows=[FilledJob], interceptors=[interceptor]
    ):
        got = await asyncio.wait_for(asyncio.gather(*reading), 60)
        assert [[i.offset for i in items] for items in got] == [list(range(6))] * 4
        assert all(len(opened(i)["pad"]) == 100_000 for items in got for i in items)
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
        await handle.signal(FilledJob.finish)
        assert await handle.result() == "ok"
    history = await rig.plain.get_workflow_handle(job_workflow_id(job)).fetch_history()
    first = first_task_answers(history)
    assert len(first) == 4  # all four in one task: the case is not vacuous
    assert sum(len(p.data) for p in first) <= budget
    answered, *held = [s.parse_envelope(p) for p in first]
    assert answered.items  # cut to one envelope, as before
    assert answered.more_ready
    assert [(len(e.items), e.next_offset, e.more_ready) for e in held] == [(0, 0, True)] * 3
    await replays(rig, history, interceptor)


async def test_the_drain_waits_for_the_answers_held_back(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A job that publishes and returns in its first workflow task, which also completes four
    relays' first polls under a budget that holds all but one back: it still waits until a
    poll has answered to its stream's end, so the tail reaches a relay before it closes."""
    monkeypatch.setattr(interceptors, "STREAM_ANSWER_BUDGET", 1)  # one answer per task
    job, handle, relays, queue = await polled_before_its_first_task(rig, 4, 6, 100_000, wait=False)
    reading = [asyncio.create_task(until_closed(r)) for r in relays]
    await asyncio.sleep(1.0)
    interceptor = KontraInterceptor(None, streams={queue: [LISTED]})
    async with Worker(
        rig.seller, task_queue=queue, workflows=[FilledJob], interceptors=[interceptor]
    ):
        got = await asyncio.wait_for(asyncio.gather(*reading), 60)
        assert await handle.result() == "ok"
    offsets = [[i.offset for i in items] for items in got]
    assert max(offsets, key=len) == list(range(6))
    assert all(o == list(range(len(o))) for o in offsets)  # no gap, no repeat
    history = await rig.plain.get_workflow_handle(job_workflow_id(job)).fetch_history()
    answered, *held = [s.parse_envelope(p) for p in first_task_answers(history)]
    assert answered.items
    assert answered.more_ready
    assert [(len(e.items), e.more_ready) for e in held] == [(0, True)] * 3
    await replays(rig, history, interceptor)


async def test_a_continue_as_new_chain_keeps_offsets_and_loses_nothing(rig: Rig) -> None:
    job, handle = await rig.start(ChainJob.run, 3, 2)
    items = await until_closed(rig.relay(job, min_interval=1.0))
    assert [i.offset for i in items] == [0, 1, 2, 3, 4, 5]
    assert await handle.result() == "ok"


async def test_the_history_guard_refuses_polls_and_stops_the_relay(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    job, handle = await rig.start(HostedJob.run, Burst(2, topics=(LISTED,)))
    await collect(rig.relay(job), 2)  # running, its stream answering
    monkeypatch.setattr(interceptors, "STREAM_HISTORY_EVENTS", 1)
    with pytest.raises(_relay.RelayStopped) as stopped:
        await collect(rig.relay(job, start=2), 1)
    assert stopped.value.reason == "history_budget"
    monkeypatch.undo()
    before = len((await rig.plain.get_workflow_handle(job_workflow_id(job)).fetch_history()).events)
    monkeypatch.setattr(interceptors, "STREAM_HISTORY_BYTES", 1)
    with pytest.raises(_relay.RelayStopped):
        await collect(rig.relay(job, start=2), 1)
    added = (await rig.plain.get_workflow_handle(job_workflow_id(job)).fetch_history()).events
    # A refused poll writes no update event. Its speculative workflow task may still be kept
    # when another event lands during it, so the workflow-task events are not counted: how
    # many are kept is the server's timing, not the guard's.
    assert [e for e in added[before:] if "UPDATE" in EventType.Name(e.event_type)] == []
    monkeypatch.undo()
    await handle.signal(HostedJob.finish)
    assert await handle.result() == "ok"


async def test_every_history_replays(rig: Rig) -> None:
    replayer = Replayer(
        workflows=[HostedJob, OwnStreamJob, ChainJob],
        interceptors=[rig.interceptor],
        data_converter=rig.seller.data_converter,
    )
    checked = 0
    for wf_id in rig.started:
        async for run in rig.seller.list_workflows(f"WorkflowId = '{wf_id}'"):
            history = await rig.seller.get_workflow_handle(wf_id, run_id=run.run_id).fetch_history()
            result = await replayer.replay_workflow(history, raise_on_replay_failure=False)
            assert result.replay_failure is None, (wf_id, result.replay_failure)
            checked += 1
    assert checked >= len(rig.started)
