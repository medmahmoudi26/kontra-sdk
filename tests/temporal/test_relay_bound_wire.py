"""The relay's bound over the wire (seam ①, decisions I39), against the real server binary,
its pinned dynamic config (``kontra.yaml``) and relay tokens minted as the gate mints them.

* **The rate, waited out.** ``kontra._relay``, refused with the Authorizer's
  ``RelayRateReason``, waits and reads on. The request's shape and the rate themselves, their
  deny-matrix cells and wire tests, are #59's (decisions I30, Q10 and Q11:
  ``test_streams_and_platform_wire.py``).
* **The bound.** Token holders re-polling a job's stream from offset 0, ten at once, with
  fresh update ids, as fast as the Authorizer and the server admit -- at a genuine request's
  size on job A, padded to exactly ``RelayRequestMax`` on job B -- while each job does its own
  work, and then on past the guard: at every sample both runs stay under
  ``limit.historyCount.error`` and ``limit.historySize.error``; what they added, measured
  against twins doing the same work with nobody polling, is within I39's formula; the runs
  accept at most ``STREAM_POLLS_MAX`` polls; a legitimate relay read the stream and was then
  told, explicitly, that the budget was spent; and both jobs complete, and replay.

The jobs survive: these tests show the attack failing. None ends a job."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pytest
import relay_bound
import temporal_isolation_app as app
from conftest import Cluster, connect_public
from memory_blobs import MemoryBlobs
from temporalio.api.common.v1 import Payloads, WorkflowExecution
from temporalio.api.enums.v1 import (
    EventType,
    UpdateWorkflowExecutionLifecycleStage,
    WorkflowExecutionStatus,
)
from temporalio.api.errordetails.v1 import PermissionDeniedFailure
from temporalio.api.query.v1 import WorkflowQuery
from temporalio.api.update.v1 import Input as UpdateInput
from temporalio.api.update.v1 import Meta as UpdateMeta
from temporalio.api.update.v1 import Request as UpdateRequest
from temporalio.api.update.v1 import WaitPolicy
from temporalio.api.workflowservice.v1 import (
    DescribeWorkflowExecutionRequest,
    QueryWorkflowRequest,
    UpdateWorkflowExecutionRequest,
)
from temporalio.client import Client, WorkflowHandle, WorkflowHistory
from temporalio.service import RPCError
from temporalio.worker import Replayer, Worker

from kontra import _relay
from kontra.codec import KontraCodec
from kontra.codec import stream as s
from kontra.codec.age import generate_identity
from kontra.contract import names
from kontra.serve import seller_data_converter
from kontra.serve.interceptors import KontraInterceptor

pytestmark = pytest.mark.temporal

SELLER = app.SELLER_NAMESPACE
SELLER_KEY = generate_identity()
BUYER_KEY = generate_identity()
LISTED = "events"
COMPLETED = (
    UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
)
#: One more than an answer lists: every answer from 0 carries the most references.
ITEMS = s.STREAM_ITEMS_MAX + 1
RATE = int(relay_bound.authorizer_constant("RelayRate"))
BURST = int(relay_bound.authorizer_constant("RelayBurst"))
REQUEST_MAX = int(relay_bound.authorizer_constant("RelayRequestMax"))


class _Keys:
    async def buyer_key(self, job_id: str) -> str:
        return BUYER_KEY[1]


@dataclass
class Seller:
    client: Client
    queue: str
    interceptor: KontraInterceptor
    blobs: MemoryBlobs


@contextlib.asynccontextmanager
async def seller_worker(cluster: Cluster) -> AsyncIterator[Seller]:
    """A seller's worker as ``kontra.serve`` runs one: Kontra's codec (a blob store for the
    stream's items) and interceptor, on the party's own token, its own task queue."""
    blobs = MemoryBlobs()
    codec = KontraCodec(
        SELLER_KEY[0], [SELLER_KEY[1]], role="seller", buyer_keys=_Keys(), blobs=blobs
    )
    client = await connect_public(
        cluster,
        SELLER,
        api_key=cluster.signer.api_key(SELLER),
        data_converter=seller_data_converter(codec),
    )
    queue = f"bound-{uuid.uuid4().hex[:8]}"
    interceptor = KontraInterceptor(None, streams={queue: [LISTED]})
    async with Worker(
        client,
        task_queue=queue,
        workflows=[app.BoundJob],
        activities=[app.bound_step],
        interceptors=[interceptor],
        max_concurrent_workflow_tasks=8,
    ):
        yield Seller(client, queue, interceptor, blobs)


async def start(seller: Seller, items: int = ITEMS) -> tuple[str, WorkflowHandle[Any, str]]:
    job = names.new_job_id()
    handle = await seller.client.start_workflow(
        app.BoundJob.run,
        app.BoundJobInput(items),
        id=names.job_workflow_id(job),
        task_queue=seller.queue,
    )
    return job, handle


async def offset_of(handle: WorkflowHandle[Any, str]) -> int:
    """The stream's length, as the seller reads it (its own query)."""
    return await handle.query(names.STREAM_OFFSET_QUERY, result_type=int)


async def until(check: Callable[[], Awaitable[bool]], within: float = 60.0) -> None:
    deadline = time.monotonic() + within
    while not await check():
        if time.monotonic() > deadline:
            raise TimeoutError("condition not reached")
        await asyncio.sleep(0.1)


def poll_request(workflow_id: str, *, identity: str = "kontra-relay") -> UpdateWorkflowExecutionRequest:  # fmt: skip
    """A poll from offset 0 with a fresh update id, shaped as ``kontra._relay`` builds it."""
    return UpdateWorkflowExecutionRequest(
        namespace=SELLER,
        workflow_execution=WorkflowExecution(workflow_id=workflow_id),
        wait_policy=WaitPolicy(lifecycle_stage=COMPLETED),
        request=UpdateRequest(
            meta=UpdateMeta(update_id=uuid.uuid4().hex, identity=identity),
            input=UpdateInput(
                name=names.STREAM_POLL_UPDATE,
                args=Payloads(
                    payloads=[_relay.poll_input_payload([LISTED, names.PROGRESS_TOPIC], 0)]
                ),
            ),
        ),
    )


def padded_identity(workflow_id: str) -> str:
    """The identity that makes a poll exactly ``RelayRequestMax`` bytes: the field a poll's
    accepted and completed events both echo, so the most a request can cost the history."""
    identity = "h"
    while poll_request(workflow_id, identity=identity + "h").ByteSize() <= REQUEST_MAX:
        identity += "h"
    assert poll_request(workflow_id, identity=identity).ByteSize() in (REQUEST_MAX, REQUEST_MAX - 1)
    return identity


def reason_of(e: RPCError) -> str:
    for detail in e.grpc_status.details:
        failure = PermissionDeniedFailure()
        if detail.Is(PermissionDeniedFailure.DESCRIPTOR) and detail.Unpack(failure):
            return failure.reason
    return ""


def count(history: WorkflowHistory, kind: EventType.ValueType) -> int:
    return sum(e.event_type == kind for e in history.events)


def accepted_polls(history: WorkflowHistory) -> int:
    return sum(
        e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED
        and e.workflow_execution_update_accepted_event_attributes.accepted_request.input.name
        == names.STREAM_POLL_UPDATE
        for e in history.events
    )


def largest_task(history: WorkflowHistory) -> int:
    """The largest workflow-task triple (scheduled, started, completed) of a history, in
    encoded bytes: what one task of the seller's own worker writes."""
    largest, current = 0, 0
    for e in history.events:
        if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
            current = e.ByteSize()
        elif e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED:
            current += e.ByteSize()
        elif e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
            largest = max(largest, current + e.ByteSize())
    return largest


async def replays(seller: Seller, history: WorkflowHistory) -> None:
    replayer = Replayer(
        workflows=[app.BoundJob],
        interceptors=[seller.interceptor],
        data_converter=seller.client.data_converter,
    )
    result = await replayer.replay_workflow(history, raise_on_replay_failure=False)
    assert result.replay_failure is None, result.replay_failure


def describe(control: Client, workflow_id: str) -> Callable[[], Awaitable[tuple[int, int, int]]]:
    """The run's (history length, history size, status), as the gate's observer reads it."""

    async def get() -> tuple[int, int, int]:
        d = await control.workflow_service.describe_workflow_execution(
            DescribeWorkflowExecutionRequest(
                namespace=SELLER, execution=WorkflowExecution(workflow_id=workflow_id)
            )
        )
        info = d.workflow_execution_info
        return info.history_length, info.history_size_bytes, info.status

    return get


async def _never() -> bool:
    return False


# --------------------------------------------------------------------------- the rate


class _RateSeen:
    """The relay token's two calls, counting the refusals for its rate."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.refused = 0

    async def update_workflow_execution(self, req: Any, retry: bool = False, metadata: Any = None, timeout: Any = None) -> Any:  # noqa: ASYNC109  # fmt: skip
        try:
            return await self._inner.update_workflow_execution(req, retry=retry, timeout=timeout)
        except RPCError as e:
            if reason_of(e) == _relay.RELAY_RATE_REASON:
                self.refused += 1
            raise

    async def query_workflow(self, req: Any, retry: bool = False, metadata: Any = None, timeout: Any = None) -> Any:  # noqa: ASYNC109  # fmt: skip
        return await self._inner.query_workflow(req, retry=retry, timeout=timeout)


async def test_the_relay_waits_out_its_rate_and_reads_on(cluster: Cluster) -> None:
    """``kontra._relay``, polling while its subject is past its rate, is refused with the
    Authorizer's ``RelayRateReason``, waits instead of stopping, and reads the stream on
    (decisions I39). The rate's own cells -- the burst, the reason, another job's relay not held
    back -- are #59's (``test_streams_and_platform_wire.py``)."""
    async with seller_worker(cluster) as seller:
        job, handle = await start(seller, items=2)
        relay = await connect_public(cluster, SELLER, api_key=cluster.signer.relay(SELLER, job))
        query = QueryWorkflowRequest(
            namespace=SELLER,
            execution=WorkflowExecution(workflow_id=handle.id),
            query=WorkflowQuery(query_type=names.STREAM_OFFSET_QUERY),
        )
        seen = _RateSeen(relay.workflow_service)
        empty = asyncio.Event()

        async def spend() -> None:
            # The same subject's offset queries until the relay's own poll is refused for the
            # rate: its bucket is empty when the relay starts, and stays so until then.
            async def one() -> None:
                while not seen.refused:
                    try:
                        await relay.workflow_service.query_workflow(
                            query, timeout=timedelta(seconds=10)
                        )
                    except RPCError as e:
                        if reason_of(e) == _relay.RELAY_RATE_REASON:
                            empty.set()

            await asyncio.gather(*(one() for _ in range(4)))

        items: list[_relay.RelayItem] = []
        async with asyncio.timeout(60):
            spender = asyncio.create_task(spend())
            try:
                await empty.wait()
                async for batch in _relay.relay(
                    seen,
                    namespace=SELLER,
                    job_id=job,
                    topics=[LISTED],
                    start=0,
                    closed=_never,
                    min_interval=0.0,
                    blobs=seller.blobs.fetch,
                ):
                    items += batch.items
                    if len(items) >= 2:
                        break
            finally:
                spender.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await spender
        assert seen.refused > 0  # refused for its rate, and it read on
        assert [i.offset for i in items] == [0, 1]
        await handle.signal(app.BoundJob.finish)
        assert await handle.result() == "done"


# --------------------------------------------------------------------------- the bound


@dataclass
class Attack:
    """Ten token holders on one job, polling from 0 with fresh ids as fast as admitted."""

    relay: Client
    workflow_id: str
    identity: str
    tally: Counter[str] = field(default_factory=Counter[str])
    full: asyncio.Event = field(default_factory=asyncio.Event)
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    refused_after_full: int = 0

    async def holder(self) -> None:
        while not self.stop.is_set():
            req = poll_request(self.workflow_id, identity=self.identity)
            try:
                resp = await self.relay.workflow_service.update_workflow_execution(
                    req, timeout=timedelta(seconds=10)
                )
            except RPCError as e:
                limited = reason_of(e) == _relay.RELAY_RATE_REASON
                self.tally["rate" if limited else f"rpc:{e.status.name}"] += 1
                await asyncio.sleep(0.05)  # the Authorizer's rate binds, not this loop
                continue
            if not resp.HasField("outcome"):
                self.tally["no outcome"] += 1
                continue
            if resp.outcome.HasField("failure"):
                kind = resp.outcome.failure.application_failure_info.type
                self.tally[kind] += 1
                if kind == s.STREAM_FULL:
                    if self.full.is_set():
                        self.refused_after_full += 1
                    self.full.set()
                continue
            self.tally["answered"] += 1


@dataclass
class Samples:
    worst: dict[str, tuple[int, int]] = field(default_factory=dict[str, tuple[int, int]])
    statuses: dict[str, set[int]] = field(default_factory=dict[str, set[int]])
    taken: int = 0


async def sample(control: Client, jobs: dict[str, str], into: Samples, stop: asyncio.Event) -> None:
    """Every half second, each run's history length, size and status (the gate's observer)."""
    while not stop.is_set():
        for label, workflow_id in jobs.items():
            length, size, status = await describe(control, workflow_id)()
            worst = into.worst.get(label, (0, 0))
            into.worst[label] = (max(worst[0], length), max(worst[1], size))
            into.statuses.setdefault(label, set()).add(status)
        into.taken += 1
        await asyncio.sleep(0.5)


async def test_a_token_holder_re_polling_from_offset_0_cannot_end_the_job(
    cluster: Cluster, control: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The owner's test on #59 (decisions I39): see the module docstring. Scaled from the
    formula: each job's stream starts with one more item than an answer lists, so every poll
    from 0 carries the most references; the polls the run accepts (P) and the bytes guard are
    the real ones, so the run is pushed as far as any token holder can push it. (The
    legitimate relay's longest wait after its rate's refusals is shortened, so it gets a
    request in between the holders' sooner.)"""
    monkeypatch.setattr(_relay, "_EXHAUSTED_MAX", 2.0)
    pinned = relay_bound.pinned()
    events_limit, bytes_limit = (
        pinned["limit.historyCount.error"],
        pinned["limit.historySize.error"],
    )
    bound = relay_bound.bound()
    async with seller_worker(cluster) as seller:
        started = {label: await start(seller) for label in ("A", "B", "A0", "B0")}
        jobs = {label: handle.id for label, (_, handle) in started.items()}
        handles = {label: handle for label, (_, handle) in started.items()}
        for h in handles.values():
            await until(lambda h=h: _at(h, ITEMS))
        job_a, job_b = started["A"][0], started["B"][0]
        relay_a = await connect_public(cluster, SELLER, api_key=cluster.signer.relay(SELLER, job_a))
        relay_b = await connect_public(cluster, SELLER, api_key=cluster.signer.relay(SELLER, job_b))

        # The seller's own work on all four jobs, in step: a signal, an activity, an item.
        cycles = 0
        working = asyncio.Event()

        async def drive() -> None:
            nonlocal cycles
            while not working.is_set():
                cycles += 1
                for h in handles.values():
                    await h.signal(app.BoundJob.work, cycles)
                for h in handles.values():
                    await until(lambda h=h, n=ITEMS + cycles: _at(h, n))
                await asyncio.sleep(1.0)

        # A legitimate relay on A, from offset 0: it reads the stream, then the budget's stop.
        legit: list[_relay.RelayItem] = []
        legit_stop: list[BaseException] = []
        legit_read = asyncio.Event()

        async def listen() -> None:
            try:
                async for batch in _relay.relay(
                    relay_a.workflow_service,
                    namespace=SELLER,
                    job_id=job_a,
                    topics=[LISTED, names.PROGRESS_TOPIC],
                    start=0,
                    closed=_closed(control, jobs["A"]),
                    min_interval=0.5,
                    blobs=seller.blobs.fetch,
                ):
                    legit.extend(batch.items)
                    if len(legit) >= ITEMS:
                        legit_read.set()
            except _relay.RelayStopped as e:
                legit_stop.append(e)

        samples, sampling = Samples(), asyncio.Event()
        sampler = asyncio.create_task(sample(control, jobs, samples, sampling))
        driver = asyncio.create_task(drive())
        listener = asyncio.create_task(listen())
        await asyncio.wait_for(legit_read.wait(), 60)

        attacks = {
            "A": Attack(relay_a, jobs["A"], "kontra-relay"),
            "B": Attack(relay_b, jobs["B"], padded_identity(jobs["B"])),
        }
        assert poll_request(jobs["B"], identity=attacks["B"].identity).ByteSize() >= REQUEST_MAX - 1
        began = time.monotonic()
        holders = [
            asyncio.create_task(attack.holder()) for attack in attacks.values() for _ in range(10)
        ]
        try:
            # Until each run refuses polls (its poll budget, or its bytes guard)...
            await asyncio.wait_for(asyncio.gather(*(a.full.wait() for a in attacks.values())), 240)
            attack_secs = time.monotonic() - began
            # ...and on past it, while the seller works: refused polls write nothing but a
            # task beside one of the seller's own, at most.
            await asyncio.sleep(10)
        finally:
            for attack in attacks.values():
                attack.stop.set()
            await asyncio.gather(*holders, return_exceptions=True)
        try:
            await asyncio.wait_for(listener, 60)  # told the budget is spent, explicitly
        finally:
            working.set()
            await driver
        sampling.set()
        await sampler
        for h in handles.values():
            await h.signal(app.BoundJob.finish)
        results = {label: await h.result() for label, h in handles.items()}
        histories = {label: await h.fetch_history() for label, h in handles.items()}
        final = {label: await describe(control, wid)() for label, wid in jobs.items()}

    # Survival: at every sample, under both limits and running; then completed, by itself.
    assert samples.taken > 20
    for label, (length, size) in samples.worst.items():
        assert length < events_limit, (label, length)
        assert size < bytes_limit, (label, size)
        assert samples.statuses[label] == {
            WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
        }, label
    assert results == {"A": "done", "B": "done", "A0": "done", "B0": "done"}
    for label, (_, _, status) in final.items():
        assert status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED, label
        # No poll ever reached the codec misshapen, nor held a task past its timeout.
        assert count(histories[label], EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED) == 0, label
        assert count(histories[label], EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT) == 0, label

    # The formula: what the polls added (the attacked run less its twin, which did the same
    # work with nobody polling) is within I39's bound for the polls the run accepted.
    measured: dict[str, dict[str, int]] = {}
    for label, twin in (("A", "A0"), ("B", "B0")):
        accepted = accepted_polls(histories[label])
        tasks = count(histories[twin], EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
        triple = largest_task(histories[twin])
        assert triple <= relay_bound.TASK_BYTES_MAX  # the precondition on w_t
        assert accepted_polls(histories[twin]) == 0
        events = final[label][0] - final[twin][0]
        size = final[label][1] - final[twin][1]
        assert accepted <= s.STREAM_POLLS_MAX, (label, accepted)
        scaled = relay_bound.bound(polls_max=accepted)
        assert events <= scaled.events_with(tasks), (label, events, accepted, tasks)
        assert size <= scaled.bytes_with(tasks, triple), (label, size, accepted, tasks)
        assert events <= bound.events_with(tasks)
        assert size <= bound.bytes_with(tasks, triple)
        assert final[twin][0] + bound.events_with(tasks) < events_limit
        measured[label] = {
            "accepted": accepted,
            "events_added": events,
            "bytes_added": size,
            "seller_tasks": tasks,
            "largest_task": triple,
            "worst_length": samples.worst[label][0],
            "worst_size": samples.worst[label][1],
        }
    # A, at a genuine request's size: the poll budget binds, exactly.
    assert measured["A"]["accepted"] == s.STREAM_POLLS_MAX
    # B, padded to the Authorizer's cap: the bytes guard binds first, under half the limit
    # plus the polls in flight.
    assert measured["B"]["accepted"] < s.STREAM_POLLS_MAX
    assert samples.worst["B"][1] >= relay_bound.interceptors.STREAM_HISTORY_BYTES
    # Past the guard, polls were refused for as long as the holders went on.
    for attack in attacks.values():
        assert attack.refused_after_full > 0
        assert attack.tally["rate"] > 0  # the Authorizer's rate held them, not the loop
        authorized = sum(n for k, n in attack.tally.items() if k != "rate" and not k.startswith("rpc:"))  # fmt: skip
        assert authorized <= BURST + RATE * (attack_secs + 12) * 1.2 + 20

    # The legitimate relay read the stream from 0, every item its buyer's, in order, and was
    # then told the budget is spent: one explicit stop, no silent loss.
    assert [i.offset for i in legit] == list(range(len(legit)))
    assert len(legit) >= ITEMS
    assert all(i.ciphertext is not None for i in legit)
    for item in legit:
        s.open_item(item.ciphertext or b"", [BUYER_KEY[0]], topic=item.topic, offset=item.offset)
    assert [e.reason for e in legit_stop if isinstance(e, _relay.RelayStopped)] == [
        "history_budget"
    ]
    # Re-reads stored nothing new: one blob per item answered, each uploaded once.
    stored = seller.blobs.of(job_a)
    assert len(stored) <= ITEMS + cycles
    assert all(data.startswith(s.AGE_MAGIC) for data in stored.values())
    # And both runs replay.
    for label in ("A", "B"):
        await replays(seller, histories[label])
    print("relay bound, measured:", json.dumps(measured, sort_keys=True), "cycles", cycles)


async def _at(handle: WorkflowHandle[Any, str], n: int) -> bool:
    return await offset_of(handle) >= n


def _closed(control: Client, workflow_id: str) -> Callable[[], Awaitable[bool]]:
    async def closed() -> bool:
        return (await describe(control, workflow_id)())[2] != (
            WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
        )

    return closed
