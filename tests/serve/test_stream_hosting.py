"""The job's workflow stream, hosted, marked, guarded and drained by Kontra's workflow
interceptor (decisions I31, I39), on a dev server: a root that hosts no stream gets Kontra's,
one that hosts its own keeps it; a poll in the run's first activation is answered, and goes
through the guard; every answer is a list of references to the job's blobs, cut at the item
cap; the tail of a short job reaches the relay; the answers of one workflow task stay within
their budget, and the drain waits for the ones held back; a continue-as-new chain keeps
offsets monotonic and loses nothing; the poll budget and the history guard stop the relay, and
refused polls write nothing; no poll waits past the stream's end and at most six wait at all,
so the seller's own updates go through; a slow bucket never holds a workflow task past its
timeout; every history replays."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest
import pytest_asyncio
from memory_blobs import MemoryBlobs
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType
from temporalio.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowHistory,
    WorkflowUpdateFailedError,
    WorkflowUpdateStage,
)
from temporalio.common import RawValue
from temporalio.contrib.workflow_streams import PollInput, WorkflowStreamClient
from temporalio.exceptions import ApplicationError
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
    BareHandlerJob,
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
    blobs: MemoryBlobs
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
        kw.setdefault("blobs", self.blobs.fetch)
        return _relay.relay(
            self.plain.workflow_service,
            namespace="default",
            job_id=job,
            closed=self.closed(job),
            **kw,
        )


def seller_client(dev: DevServer, blobs: MemoryBlobs) -> Client:
    codec = KontraCodec(SELLER[0], [SELLER[1]], role="seller", buyer_keys=Keys(), blobs=blobs)
    config = dev.client.config()
    config["data_converter"] = seller_data_converter(codec)
    return Client(**config)


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def rig(dev_server: DevServer) -> AsyncIterator[Rig]:
    queue = f"hosting-{uuid.uuid4().hex[:8]}"
    interceptor = KontraInterceptor(None, streams={queue: [LISTED]})
    blobs = MemoryBlobs()
    seller = seller_client(dev_server, blobs)
    async with Worker(
        seller,
        task_queue=queue,
        workflows=[HostedJob, OwnStreamJob, ChainJob, FilledJob, BareHandlerJob],
        activities=[burst],
        interceptors=[interceptor],
    ):
        yield Rig(dev_server, seller, dev_server.client, queue, interceptor, blobs, [])


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


async def history_of(rig: Rig, job: str) -> WorkflowHistory:
    return await rig.plain.get_workflow_handle(job_workflow_id(job)).fetch_history()


def count(history: WorkflowHistory, kind: EventType.ValueType) -> int:
    return sum(e.event_type == kind for e in history.events)


async def raw_poll(rig: Rig, job: str, offset: int) -> Any:
    """One poll with a fresh update id, as any client of the job's namespace could send it."""
    return await rig.plain.get_workflow_handle(job_workflow_id(job)).execute_update(
        STREAM_POLL_UPDATE, PollInput(topics=[LISTED], from_offset=offset), result_type=RawValue
    )


async def refused(rig: Rig, job: str, offset: int) -> str:
    """The failure type of a poll that must be refused."""
    with pytest.raises(WorkflowUpdateFailedError) as e:
        await raw_poll(rig, job, offset)
    cause = e.value.cause
    assert isinstance(cause, ApplicationError), cause
    return cause.type or ""


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
    raw = (await history_of(rig, job)).to_json()
    assert MARKER not in raw
    assert UNLISTED not in raw
    stored = rig.blobs.of(job)
    assert len(stored) == 6  # each item once, however many polls read it
    for data in stored.values():
        assert data.startswith(s.AGE_MAGIC)
        assert MARKER.encode() not in data
        assert UNLISTED.encode() not in data


async def test_a_root_hosting_its_own_stream_keeps_it_across_its_chain(rig: Rig) -> None:
    job, handle = await rig.start(OwnStreamJob.run, OwnInput(2, MARKER, rounds=2))
    items = await collect(rig.relay(job), 8)
    assert [i.topic for i in items] == [LISTED] * 8
    offsets = [i.offset for i in items]
    assert offsets == sorted(offsets)
    assert len(set(offsets)) == 8
    assert all(i.ciphertext is not None for i in items)
    await handle.signal(OwnStreamJob.finish)
    assert await handle.result() == "ok"


async def test_a_poll_in_the_first_activation_is_answered(rig: Rig, dev_server: DevServer) -> None:
    """The run's first activation applies updates before initialising the workflow: the
    early handler accepts the poll and the stream answers it, with references."""
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
    [item] = envelope.items
    assert (item.topic, item.offset) == (LISTED, 0)
    assert item.sha256 in rig.blobs.of(job)


async def test_a_poll_in_the_first_activation_goes_through_the_guard(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The placeholder has a validator, so the guard refuses an early poll as any other."""
    monkeypatch.setattr(interceptors, "STREAM_POLLS_MAX", 0)
    queue = f"first-{uuid.uuid4().hex[:8]}"
    job = new_job_id()
    handle = await rig.seller.start_workflow(
        HostedJob.run, args=[Burst(1, MARKER), False], id=job_workflow_id(job), task_queue=queue
    )
    rig.started.append(job_workflow_id(job))
    poll = asyncio.create_task(refused(rig, job, 0))
    await asyncio.sleep(0.5)
    async with Worker(
        rig.seller,
        task_queue=queue,
        workflows=[HostedJob],
        activities=[burst],
        interceptors=[KontraInterceptor(None, streams={queue: [LISTED]})],
    ):
        assert await asyncio.wait_for(poll, 30) == s.STREAM_FULL
        assert await handle.result() == "ok"


async def test_an_answer_lists_at_most_the_item_cap(rig: Rig) -> None:
    """A tail longer than one answer, published in one go: the first answer lists
    ``STREAM_ITEMS_MAX`` references and ``more_ready``; the relay reads on at once."""
    n = 2 * s.STREAM_ITEMS_MAX + 5
    job, handle = await rig.start(FilledJob.run, n, 0)
    loop = asyncio.get_running_loop()
    began = loop.time()
    items = await collect(rig.relay(job, min_interval=2.0), n)
    assert loop.time() - began < 2.0  # more_ready: polled again at once
    assert [i.offset for i in items] == list(range(n))
    assert all(opened(i)["i"] == i.offset for i in items)
    await handle.signal(FilledJob.finish)
    assert await handle.result() == "ok"
    history = await history_of(rig, job)
    sizes = [
        len(e.workflow_execution_update_completed_event_attributes.outcome.success.payloads[0].data)
        for e in history.events
        if e.HasField("workflow_execution_update_completed_event_attributes")
        and e.workflow_execution_update_completed_event_attributes.outcome.HasField("success")
    ]
    assert max(sizes) <= s.ENVELOPE_MAX
    assert len(rig.blobs.of(job)) == n


async def test_the_drain_delivers_the_tail_of_a_short_job(rig: Rig) -> None:
    """A relay paced at 2 s and a job that publishes and returns at once: without the drain
    the job closes before the next poll."""
    for _ in range(2):
        job, handle = await rig.start(HostedJob.run, Burst(6, topics=(LISTED,)), False)
        items = await until_closed(rig.relay(job, min_interval=2.0))
        assert [i.offset for i in items] == [0, 1, 2, 3, 4, 5]
        assert await handle.result() == "ok"


def first_task_answers(history: WorkflowHistory, task: int = 1) -> list[Payload]:
    """The poll answers the run's ``task``-th workflow task completed, as its history stores
    them (the outcomes of the updates completed in that task's reply)."""
    answers: list[Payload] = []
    tasks = 0
    for e in history.events:
        if e.HasField("workflow_task_completed_event_attributes"):
            tasks += 1
            if tasks > task:
                break
        elif tasks == task and e.HasField("workflow_execution_update_completed_event_attributes"):
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
    again. Four relays' first polls, all completed in the job's first task: one is answered,
    the others are held back at offset 0; the job keeps running, and every relay then reads
    the whole stream. The budget is scaled down to one and a half answers, so the case stays
    small (with references, ten full answers are under a tenth of the real one)."""
    budget = s.envelope_bound(6) * 3 // 2
    monkeypatch.setattr(interceptors, "STREAM_ANSWER_BUDGET", budget)
    job, handle, relays, queue = await polled_before_its_first_task(rig, 4, 6, 1_000, wait=True)
    reading = [asyncio.create_task(collect(r, 6)) for r in relays]
    await asyncio.sleep(1.0)  # the polls wait for the first workflow task
    interceptor = KontraInterceptor(None, streams={queue: [LISTED]})
    async with Worker(
        rig.seller, task_queue=queue, workflows=[FilledJob], interceptors=[interceptor]
    ):
        got = await asyncio.wait_for(asyncio.gather(*reading), 60)
        assert [[i.offset for i in items] for items in got] == [list(range(6))] * 4
        assert all(len(opened(i)["pad"]) == 1_000 for items in got for i in items)
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
        await handle.signal(FilledJob.finish)
        assert await handle.result() == "ok"
    history = await history_of(rig, job)
    first = first_task_answers(history)
    assert len(first) == 4  # all four in one task: the case is not vacuous
    assert sum(len(p.data) for p in first) <= budget
    answered, *held = [s.parse_envelope(p) for p in first]
    assert len(answered.items) == 6
    assert [(len(e.items), e.next_offset, e.more_ready) for e in held] == [(0, 0, True)] * 3
    await replays(rig, history, interceptor)


async def test_the_drain_waits_for_the_answers_held_back(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A job that publishes and returns in its first workflow task, which also completes four
    relays' first polls under a budget that holds all but one back: it still waits until a
    poll has answered to its stream's end, so the tail reaches a relay before it closes."""
    monkeypatch.setattr(interceptors, "STREAM_ANSWER_BUDGET", 1)  # one answer per task
    job, handle, relays, queue = await polled_before_its_first_task(rig, 4, 6, 100, wait=False)
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
    history = await history_of(rig, job)
    answered, *held = [s.parse_envelope(p) for p in first_task_answers(history)]
    assert answered.items
    assert [(len(e.items), e.more_ready) for e in held] == [(0, True)] * 3
    await replays(rig, history, interceptor)


async def test_a_continue_as_new_chain_keeps_offsets_and_loses_nothing(rig: Rig) -> None:
    job, handle = await rig.start(ChainJob.run, 3, 2)
    items = await until_closed(rig.relay(job, min_interval=1.0))
    assert [i.offset for i in items] == [0, 1, 2, 3, 4, 5]
    assert await handle.result() == "ok"


async def test_the_poll_budget_stops_the_relay_and_refused_polls_write_nothing(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A run accepts ``STREAM_POLLS_MAX`` polls (scaled down here to 12), from any offset,
    exactly; the next is refused at validation, ``KontraStreamFull``, and the relay stops with
    ``history_budget``. Refused polls add nothing to the history and spend no update."""
    monkeypatch.setattr(interceptors, "STREAM_POLLS_MAX", 12)
    job, handle = await rig.start(FilledJob.run, 3, 0)
    for _ in range(11):
        answer = await raw_poll(rig, job, 0)
        assert len(s.parse_envelope(answer.payload).items) == 3
    await collect(rig.relay(job), 3)  # the twelfth
    with pytest.raises(_relay.RelayStopped) as stopped:
        await collect(rig.relay(job), 1)
    assert stopped.value.reason == "history_budget"
    before = await history_of(rig, job)
    for _ in range(30):
        assert await refused(rig, job, 0) == s.STREAM_FULL
    after = await history_of(rig, job)
    assert len(after.events) == len(before.events)  # an idle job: not one event
    accepted = count(after, EventType.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)
    assert accepted == 12
    assert len(rig.blobs.of(job)) == 3  # twelve reads from 0, each item stored once
    monkeypatch.undo()
    await handle.signal(FilledJob.finish)
    assert await handle.result() == "ok"


async def test_no_poll_waits_past_the_end_and_at_most_six_wait(rig: Rig) -> None:
    """A poll whose offset is past the stream's end is refused, ``KontraStreamBusy``; at most
    ``STREAM_PARKED_MAX`` polls wait at the end, the next is refused the same way; the seller's
    own update goes through meanwhile, and the next publish answers the waiting polls."""
    job, handle = await rig.start(FilledJob.run, 3, 0)
    await collect(rig.relay(job), 3)
    before = await history_of(rig, job)
    assert await refused(rig, job, 4) == s.STREAM_BUSY
    assert await refused(rig, job, 10**18) == s.STREAM_BUSY
    assert len((await history_of(rig, job)).events) == len(before.events)
    plain = rig.plain.get_workflow_handle(job_workflow_id(job))
    waiting = [
        await plain.start_update(
            STREAM_POLL_UPDATE,
            PollInput(topics=[LISTED], from_offset=3),
            wait_for_stage=WorkflowUpdateStage.ACCEPTED,
            result_type=RawValue,
        )
        for _ in range(interceptors.STREAM_PARKED_MAX)
    ]
    assert await refused(rig, job, 3) == s.STREAM_BUSY
    assert await handle.execute_update(FilledJob.ping) == "pong"  # the seller's own slot
    await handle.signal(FilledJob.more, 1)
    for update in waiting:
        answer = await asyncio.wait_for(update.result(), 30)
        assert [i.offset for i in s.parse_envelope(answer.payload).items] == [3]
    await handle.signal(FilledJob.finish)
    assert await handle.result() == "ok"


async def test_a_slow_bucket_never_times_a_workflow_task_out(
    rig: Rig, dev_server: DevServer
) -> None:
    """The polls one workflow task answers share one upload budget: six polls on fresh
    windows of a stream, against a bucket that takes 0.2 s an item, completed in one task
    whose timeout is 2 s. Each answer alone would spend the budget (the codec's, 0.5 s here)
    and the task would time out; shared, the task completes in time, the late items are
    stubs, and later reads store them."""
    window = 8
    polls = interceptors.STREAM_PARKED_MAX
    n = window * polls
    queue = f"slow-{uuid.uuid4().hex[:8]}"
    job = new_job_id()
    workflows = [FilledJob]
    interceptor = KontraInterceptor(None, streams={queue: [LISTED]})
    handle = await rig.seller.start_workflow(
        FilledJob.run,
        args=[n, 0],
        id=job_workflow_id(job),
        task_queue=queue,
        task_timeout=timedelta(seconds=2),
    )
    rig.started.append(job_workflow_id(job))
    # Its first task runs (it publishes) on a worker that keeps nothing cached...
    async with Worker(
        rig.seller,
        task_queue=queue,
        workflows=workflows,
        interceptors=[interceptor],
        max_cached_workflows=0,
    ):

        async def published() -> bool:
            h = await history_of(rig, job)
            return count(h, EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED) >= 1

        for _ in range(100):
            if await published():
                break
            await asyncio.sleep(0.1)
    # ...then six polls on fresh windows wait for the next task, on a slow bucket.
    plain = rig.plain.get_workflow_handle(job_workflow_id(job))
    starts = [
        asyncio.create_task(
            plain.start_update(
                STREAM_POLL_UPDATE,
                PollInput(topics=[LISTED], from_offset=w * window),
                wait_for_stage=WorkflowUpdateStage.ACCEPTED,
                result_type=RawValue,
            )
        )
        for w in range(polls)
    ]
    await asyncio.sleep(1.0)
    slow = MemoryBlobs(delay=0.2)
    slow_codec = KontraCodec(
        SELLER[0],
        [SELLER[1]],
        role="seller",
        buyer_keys=Keys(),
        blobs=slow,
        stream_upload_budget=0.5,
    )
    config = dev_server.client.config()
    config["data_converter"] = seller_data_converter(slow_codec)
    slow_seller = Client(**config)
    async with Worker(
        slow_seller, task_queue=queue, workflows=workflows, interceptors=[interceptor]
    ):
        updates = await asyncio.wait_for(asyncio.gather(*starts), 30)
        answers = [s.parse_envelope((await u.result()).payload) for u in updates]
        stored = [i.sha256 is not None for a in answers for i in a.items]
        assert len(stored) >= n  # every window answered (an answer runs to the end)
        assert 0 < sum(stored) < len(stored)  # some in time, the rest stubs
        history = await history_of(rig, job)
        assert count(history, EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT) == 0
        assert count(history, EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED) == 0
        assert len(first_task_answers(history, task=2)) == polls  # one task answered them
        # Later reads store the stubs: each poll its own budget.
        read: set[int] = set()
        for _ in range(60):
            for batch in await _one(slow, rig, job):
                read |= {i.offset for i in batch.items if i.ciphertext is not None}
            if len(read) == n:
                break
        assert read == set(range(n))
        await handle.signal(FilledJob.finish)
        assert await handle.result() == "ok"
    history = await history_of(rig, job)
    assert count(history, EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT) == 0
    await replays(rig, history, interceptor)


async def _one(blobs: MemoryBlobs, rig: Rig, job: str) -> list[_relay.RelayBatch]:
    """One relay answer from offset 0, read through ``blobs``."""
    it = rig.relay(job, blobs=blobs.fetch)
    try:
        return [await anext(it)]
    finally:
        await it.aclose()  # pyright: ignore[reportAttributeAccessIssue]


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
    before = len((await history_of(rig, job)).events)
    monkeypatch.setattr(interceptors, "STREAM_HISTORY_BYTES", 1)
    with pytest.raises(_relay.RelayStopped):
        await collect(rig.relay(job, start=2), 1)
    after = len((await history_of(rig, job)).events)
    assert after - before <= 3  # a refused poll writes no update events (a workflow task, at most)
    monkeypatch.undo()
    await handle.signal(HostedJob.finish)
    assert await handle.result() == "ok"


async def test_a_poll_handler_without_a_validator_is_outside_the_guard_and_said_so(
    rig: Rig, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """temporalio runs a poll's validators only for a handler registered with one. A seller
    that registers its own poll handler without one gets every poll accepted -- written
    before anything could refuse it, past the run's budget too: such a job is outside I39's
    bound -- and is told so, once a run, in a warning."""
    monkeypatch.setattr(interceptors, "STREAM_POLLS_MAX", 2)
    job, handle = await rig.start(BareHandlerJob.run)
    with caplog.at_level(logging.WARNING):
        for _ in range(5):
            await raw_poll(rig, job, 0)
    history = await history_of(rig, job)
    assert count(history, EventType.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED) == 5
    warned = [r for r in caplog.records if "has no validator" in r.getMessage()]
    assert len(warned) == 1
    monkeypatch.undo()
    await handle.signal(BareHandlerJob.finish)
    assert await handle.result() == 5


async def test_every_history_replays(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every history replays; and a replay encodes, seals and stores nothing of the stream:
    each accepted poll's answer is the replayed constant (decisions I39)."""
    sealed: list[int] = []
    encoded: list[int] = []
    real_seal, real_encode = s.seal_poll, interceptors._encode_payload  # pyright: ignore[reportPrivateUsage]

    async def seal(*a: Any, **kw: Any) -> Any:
        sealed.append(1)
        return await real_seal(*a, **kw)

    def encode(p: Any) -> str:
        encoded.append(1)
        return real_encode(p)

    replayer = Replayer(
        workflows=[HostedJob, OwnStreamJob, ChainJob, FilledJob, BareHandlerJob],
        interceptors=[rig.interceptor],
        data_converter=rig.seller.data_converter,
    )
    stored = dict(rig.blobs.data)
    checked = polls = 0
    for wf_id in rig.started:
        async for run in rig.seller.list_workflows(f"WorkflowId = '{wf_id}'"):
            history = await rig.seller.get_workflow_handle(wf_id, run_id=run.run_id).fetch_history()
            polls += count(history, EventType.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED)
            monkeypatch.setattr(s, "seal_poll", seal)
            monkeypatch.setattr(interceptors, "_encode_payload", encode)
            result = await replayer.replay_workflow(history, raise_on_replay_failure=False)
            monkeypatch.undo()
            assert result.replay_failure is None, (wf_id, result.replay_failure)
            checked += 1
    assert checked >= len(rig.started)
    assert polls > 50  # the case is not vacuous
    assert (sealed, encoded) == ([], [])
    assert rig.blobs.data == stored
