"""The relay's bound over the wire (seam ①, decisions I39), against the real server binary,
its pinned dynamic config (``kontra.yaml``) and relay tokens minted as the gate mints them.

* **The rate, waited out.** ``kontra._relay``, refused with the Authorizer's
  ``RelayRateReason``, waits and reads on. The request's shape and the rate themselves, their
  deny-matrix cells and wire tests, are #59's (decisions I30, Q10 and Q11:
  ``test_streams_and_platform_wire.py``).
* **The bound.** Token holders re-polling a job's stream from offset 0 with fresh update ids,
  as fast as the Authorizer and the server admit, while the job does its own work, and then
  on past the guard -- the poll budget's and the bytes guard's terms at their most, and a
  long stream, one attacked job a test: job A at a genuine request's size (the poll budget
  binds), then its worker restarted and the run replayed cold within the task timeout; job B
  padded to ``RelayRequestMax``, with update ids of ``limit.maxIDLength`` and a 64-character
  listed topic, on a history the seller grew (the bytes guard binds); job D a stream of 5,000
  small items (a poll costs its answer, never the log). At every sample the runs stay under
  ``limit.historyCount.error`` and ``limit.historySize.error``; what they added, measured
  against twins doing the same work with nobody polling, is within the formula; a legitimate
  relay read the stream and was then told, explicitly, that the budget was spent; and every
  job completes with no workflow task failed or timed out, and replays.

The jobs survive: these tests show the attack failing. None ends a job."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from collections import Counter
from collections.abc import AsyncGenerator, Awaitable, Callable
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
#: The longest topic ``check_topic`` admits, listed: every reference to it at its longest.
LONG_TOPIC = "t" * 64
#: ``limit.maxIDLength``: the longest update id the server takes.
ID_MAX = int(relay_bound.pinned()["limit.maxIDLength"])
#: Temporal's default workflow task timeout, which nothing in Kontra sets otherwise: a worker
#: that lost its cache replays a run's accepted polls within it (decisions I39).
TASK_TIMEOUT = 10.0


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
async def seller_worker(
    cluster: Cluster, *, blobs: MemoryBlobs | None = None, queue: str | None = None
) -> AsyncGenerator[Seller]:
    """A seller's worker as ``kontra.serve`` runs one: Kontra's codec (a blob store for the
    stream's items) and interceptor, on the party's own token, its own task queue. Given the
    ``blobs`` and ``queue`` of one before it: that worker restarted, its caches empty."""
    blobs = blobs if blobs is not None else MemoryBlobs()
    codec = KontraCodec(
        SELLER_KEY[0], [SELLER_KEY[1]], role="seller", buyer_keys=_Keys(), blobs=blobs
    )
    client = await connect_public(
        cluster,
        SELLER,
        api_key=cluster.signer.api_key(SELLER),
        data_converter=seller_data_converter(codec),
    )
    queue = queue or f"bound-{uuid.uuid4().hex[:8]}"
    interceptor = KontraInterceptor(None, streams={queue: [LISTED, LONG_TOPIC]})
    async with Worker(
        client,
        task_queue=queue,
        workflows=[app.BoundJob],
        activities=[app.bound_step],
        interceptors=[interceptor],
        max_concurrent_workflow_tasks=8,
    ):
        yield Seller(client, queue, interceptor, blobs)


async def start(
    seller: Seller, items: int = ITEMS, **kw: Any
) -> tuple[str, WorkflowHandle[Any, str]]:
    job = names.new_job_id()
    handle = await seller.client.start_workflow(
        app.BoundJob.run,
        app.BoundJobInput(items, **kw),
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


def poll_request(
    workflow_id: str,
    *,
    identity: str = "kontra-relay",
    topic: str = LISTED,
    update_id: str | None = None,
) -> UpdateWorkflowExecutionRequest:
    """A poll from offset 0 with a fresh update id, shaped as ``kontra._relay`` builds it."""
    return UpdateWorkflowExecutionRequest(
        namespace=SELLER,
        workflow_execution=WorkflowExecution(workflow_id=workflow_id),
        wait_policy=WaitPolicy(lifecycle_stage=COMPLETED),
        request=UpdateRequest(
            meta=UpdateMeta(update_id=update_id or uuid.uuid4().hex, identity=identity),
            input=UpdateInput(
                name=names.STREAM_POLL_UPDATE,
                args=Payloads(
                    payloads=[_relay.poll_input_payload([topic, names.PROGRESS_TOPIC], 0)]
                ),
            ),
        ),
    )


def long_update_id() -> str:
    """A fresh update id of ``limit.maxIDLength`` characters: the poll's accepted event
    writes it three times."""
    return (uuid.uuid4().hex * 8)[:ID_MAX]


def padded_identity(workflow_id: str, *, topic: str = LISTED, long_ids: bool = False) -> str:
    """The identity that makes a poll exactly ``RelayRequestMax`` bytes: the field a poll's
    accepted and completed events both echo, so the most a request can cost the history."""
    update_id = long_update_id() if long_ids else None

    def size(identity: str) -> int:
        return poll_request(
            workflow_id, identity=identity, topic=topic, update_id=update_id
        ).ByteSize()

    identity = "h"
    while size(identity + "h") <= REQUEST_MAX:
        identity += "h"
    assert size(identity) in (REQUEST_MAX, REQUEST_MAX - 1)
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


async def cold_replay(seller: Seller, history: WorkflowHistory) -> float:
    """Replays ``history`` as a worker that lost its cache does -- a fresh codec, nothing
    remembered -- and returns the seconds it took: what one workflow task's timeout must
    hold after a restart, a deploy or an eviction."""
    codec = KontraCodec(
        SELLER_KEY[0], [SELLER_KEY[1]], role="seller", buyer_keys=_Keys(), blobs=seller.blobs
    )
    replayer = Replayer(
        workflows=[app.BoundJob],
        interceptors=[seller.interceptor],
        data_converter=seller_data_converter(codec),
    )
    began = time.monotonic()
    result = await replayer.replay_workflow(history, raise_on_replay_failure=False)
    took = time.monotonic() - began
    assert result.replay_failure is None, result.replay_failure
    return took


def describe(
    control: Client, workflow_id: str, run_id: str = ""
) -> Callable[[], Awaitable[tuple[int, int, int]]]:
    """The run's (history length, history size, status), as the gate's observer reads it:
    the chain's current run, or ``run_id``."""

    async def get() -> tuple[int, int, int]:
        d = await control.workflow_service.describe_workflow_execution(
            DescribeWorkflowExecutionRequest(
                namespace=SELLER,
                execution=WorkflowExecution(workflow_id=workflow_id, run_id=run_id),
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
    """Token holders on one job, polling from 0 with fresh ids as fast as admitted."""

    relay: Client
    workflow_id: str
    identity: str = "kontra-relay"
    topic: str = LISTED
    long_ids: bool = False
    tally: Counter[str] = field(default_factory=Counter[str])
    full: asyncio.Event = field(default_factory=asyncio.Event)
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    refused_after_full: int = 0

    async def holder(self) -> None:
        while not self.stop.is_set():
            req = poll_request(
                self.workflow_id,
                identity=self.identity,
                topic=self.topic,
                update_id=long_update_id() if self.long_ids else None,
            )
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

    def run(self, holders: int) -> list[asyncio.Task[None]]:
        return [asyncio.create_task(self.holder()) for _ in range(holders)]

    def authorized(self) -> int:
        return sum(n for k, n in self.tally.items() if k != "rate" and not k.startswith("rpc:"))


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


class Driver:
    """The seller's own work on every job, in step: a signal, an activity, an item."""

    def __init__(self, handles: dict[str, WorkflowHandle[Any, str]], items: dict[str, int]):
        self.handles, self.items, self.cycles = handles, items, 0
        self.stop = asyncio.Event()

    async def cycle(self) -> None:
        self.cycles += 1
        for h in self.handles.values():
            await h.signal(app.BoundJob.work, self.cycles)
        for label, h in self.handles.items():
            await until(lambda h=h, n=self.items[label] + self.cycles: _at(h, n), within=120)

    async def drive(self) -> None:
        while not self.stop.is_set():
            await self.cycle()
            await asyncio.sleep(1.0)


def assert_survived(samples: Samples, results: dict[str, str], runs: dict[str, list[Run]]) -> None:  # fmt: skip
    """At every sample under both limits and running; every run then completed, by itself,
    and not one workflow task failed or timed out."""
    pinned = relay_bound.pinned()
    assert samples.taken > 20
    for label, (length, size) in samples.worst.items():
        assert length < pinned["limit.historyCount.error"], (label, length)
        assert size < pinned["limit.historySize.error"], (label, size)
        assert samples.statuses[label] == {
            WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
        }, label
    assert set(results.values()) == {"done"}, results
    for label, chain in runs.items():
        for run in chain:
            assert count(run.history, EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED) == 0, label
            assert count(run.history, EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT) == 0, label


@dataclass
class Run:
    """One run's history and its length and size as the server counts them."""

    history: WorkflowHistory
    length: int
    size: int


def within_formula(attacked: Run, twin: Run, label: str) -> dict[str, int]:
    """What the polls added to one run (it less its twin's run, which did the same work with
    nobody polling) against I39's formula for the polls the run accepted, ``W_s`` the twin's
    workflow tasks, ``w_t`` the largest triple of either run."""
    pinned = relay_bound.pinned()
    accepted = accepted_polls(attacked.history)
    assert accepted_polls(twin.history) == 0
    assert accepted <= s.STREAM_POLLS_MAX, (label, accepted)
    tasks = count(twin.history, EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
    triple = max(largest_task(attacked.history), largest_task(twin.history))
    assert triple <= relay_bound.TASK_BYTES_MAX, (label, triple)  # the precondition on w_t
    events, size = attacked.length - twin.length, attacked.size - twin.size
    scaled, bound = relay_bound.bound(polls_max=accepted), relay_bound.bound()
    assert events <= scaled.events_with(tasks), (label, events, accepted, tasks)
    assert size <= scaled.bytes_with(tasks, triple), (label, size, accepted, tasks)
    assert events <= bound.events_with(tasks)
    assert size <= bound.bytes_with(tasks, triple)
    assert twin.length + bound.events_with(tasks) < pinned["limit.historyCount.error"]
    assert twin.size + bound.bytes_with(tasks, triple) < pinned["limit.historySize.error"]
    return {
        "accepted": accepted,
        "events_added": events,
        "bytes_added": size,
        "seller_tasks": tasks,
        "largest_task": triple,
    }


async def runs_of(client: Client, control: Client, workflow_id: str) -> list[Run]:
    """Every run of the chain, oldest first."""
    found = [r async for r in client.list_workflows(f"WorkflowId = '{workflow_id}'")]
    found.sort(key=lambda r: r.start_time)
    runs: list[Run] = []
    for r in found:
        history = await client.get_workflow_handle(workflow_id, run_id=r.run_id).fetch_history()
        length, size, _ = await describe(control, workflow_id, r.run_id)()
        runs.append(Run(history, length, size))
    return runs


class Bench:
    """A seller's worker, an attacked job beside a twin doing the same work with nobody
    polling, the seller's own work driven on both, and every run sampled as the gate's
    observer samples it. One attacked job a bench: the worker keeps up with one relay
    token's rate (decisions I39's precondition), whatever else runs on the machine."""

    def __init__(self, cluster: Cluster, control: Client) -> None:
        self.cluster, self.control = cluster, control
        self.samples, self._sampling = Samples(), asyncio.Event()
        self._worker = contextlib.AsyncExitStack()
        self.jobs: dict[str, str] = {}
        self.job_of: dict[str, str] = {}
        self.handles: dict[str, WorkflowHandle[Any, str]] = {}
        self.driver = Driver({}, {})
        self._tasks: list[asyncio.Task[None]] = []

    async def __aenter__(self) -> Bench:
        self.seller = await self._worker.enter_async_context(seller_worker(self.cluster))
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.driver.stop.set()
        self._sampling.set()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._worker.aclose()

    async def start(self, shapes: dict[str, dict[str, Any]]) -> None:
        """The jobs, each with its stream published, then the seller's work and the sampling
        under way."""
        items: dict[str, int] = {}
        for label, kw in shapes.items():
            job, handle = await start(self.seller, **kw)
            self.jobs[label], self.job_of[label], self.handles[label] = handle.id, job, handle
            items[label] = kw.get("items", ITEMS)
        for label, h in self.handles.items():
            await until(lambda h=h, n=items[label]: _at(h, n), within=120)
        self.driver = Driver(self.handles, items)
        self._tasks = [
            asyncio.create_task(self.driver.drive()),
            asyncio.create_task(sample(self.control, self.jobs, self.samples, self._sampling)),
        ]

    async def relay(self, label: str) -> Client:
        """A client on the job's relay token, as the gate mints it."""
        token = self.cluster.signer.relay(SELLER, self.job_of[label])
        return await connect_public(self.cluster, SELLER, api_key=token)

    async def stop(self) -> None:
        """The seller's work and the sampling, stopped."""
        self.driver.stop.set()
        self._sampling.set()
        await asyncio.gather(*self._tasks)
        self._tasks = []

    async def restart(self) -> None:
        """The seller's worker restarts: its workflow cache and its codec's are empty."""
        queue, blobs = self.seller.queue, self.seller.blobs
        await self._worker.aclose()
        self._worker = contextlib.AsyncExitStack()
        self.seller = await self._worker.enter_async_context(
            seller_worker(self.cluster, blobs=blobs, queue=queue)
        )
        self.handles = {
            label: self.seller.client.get_workflow_handle(wid) for label, wid in self.jobs.items()
        }
        self.driver.handles = self.handles

    async def finish(self) -> tuple[dict[str, str], dict[str, list[Run]]]:
        """Every job finished by its seller; its result, and each run of its chain."""
        await self.stop()
        for h in self.handles.values():
            await h.signal(app.BoundJob.finish)
        results = {label: await h.result() for label, h in self.handles.items()}
        runs = {
            label: await runs_of(self.seller.client, self.control, wid)
            for label, wid in self.jobs.items()
        }
        return results, runs


async def attack_until_full(attack: Attack, holders: int, *, past: float = 10.0) -> float:
    """``holders`` token holders until the run refuses polls (its poll budget, or its bytes
    guard), and on past it for ``past`` seconds while the seller works: refused polls write
    nothing but a task beside one of the seller's own, at most. The attack's seconds."""
    began = time.monotonic()
    tasks = attack.run(holders)
    try:
        await asyncio.wait_for(attack.full.wait(), 240)
        took = time.monotonic() - began
        await asyncio.sleep(past)
    finally:
        attack.stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert attack.refused_after_full > 0  # past the guard, refused for as long as it went on
    assert attack.authorized() <= BURST + RATE * (took + past + 2) * 1.2 + 20
    return took


async def test_a_token_holder_re_polling_from_offset_0_cannot_end_the_job(
    cluster: Cluster, control: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The owner's test on #59 (decisions I39): ten token holders re-poll job A's stream from
    offset 0 with fresh update ids, at a genuine request's size, as fast as the Authorizer
    admits, while the seller works and a legitimate relay listens; then on past the poll
    budget. Each poll from 0 carries the most references (the stream starts with one more
    item than an answer lists), and P is the real one. Then the seller's worker restarts, and
    A's next task replays its whole history -- every accepted poll -- on a cold worker within
    the task timeout. (The legitimate relay's longest wait after its rate's refusals is
    shortened, so it gets a request in between the holders' sooner.)"""
    monkeypatch.setattr(_relay, "_EXHAUSTED_MAX", 2.0)
    async with Bench(cluster, control) as b:
        await b.start({"A": {}, "A0": {}})
        relay = await b.relay("A")
        legit: list[_relay.RelayItem] = []
        legit_stop: list[BaseException] = []
        legit_read = asyncio.Event()

        async def listen() -> None:
            try:
                async for batch in _relay.relay(
                    relay.workflow_service,
                    namespace=SELLER,
                    job_id=b.job_of["A"],
                    topics=[LISTED, names.PROGRESS_TOPIC],
                    start=0,
                    closed=_closed(control, b.jobs["A"]),
                    min_interval=0.5,
                    blobs=b.seller.blobs.fetch,
                ):
                    legit.extend(batch.items)
                    if len(legit) >= ITEMS:
                        legit_read.set()
            except _relay.RelayStopped as e:
                legit_stop.append(e)

        listener = asyncio.create_task(listen())
        await asyncio.wait_for(legit_read.wait(), 60)
        attack = Attack(relay, b.jobs["A"])
        attack_secs = await attack_until_full(attack, 10)
        await asyncio.wait_for(listener, 60)  # told the budget is spent, explicitly
        await b.stop()
        await b.restart()
        await b.driver.cycle()  # each job's next task: its whole history, replayed cold
        results, runs = await b.finish()
        replay_secs = round(await cold_replay(b.seller, runs["A"][0].history), 2)
        blobs = b.seller.blobs

    assert_survived(b.samples, results, runs)
    measured = within_formula(runs["A"][0], runs["A0"][0], "A")
    measured["worst_length"], measured["worst_size"] = b.samples.worst["A"]
    assert measured["accepted"] == s.STREAM_POLLS_MAX  # the poll budget binds, exactly
    assert attack.tally["rate"] > 0  # the Authorizer's rate held them, not the loop
    # A worker that lost its cache replays the attacked run well within a task's timeout.
    assert replay_secs < TASK_TIMEOUT / 2, replay_secs
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
    # Re-reads stored nothing new: one blob per item answered, each uploaded once -- the
    # restart's replay included.
    stored = blobs.of(b.job_of["A"])
    assert len(stored) <= ITEMS + b.driver.cycles
    assert all(data.startswith(s.AGE_MAGIC) for data in stored.values())
    print(
        "relay bound A, measured:",
        json.dumps(measured, sort_keys=True),
        f"attack {attack_secs:.1f} s, cold replay {replay_secs} s, cycles {b.driver.cycles}",
    )


async def test_the_bytes_guard_binds_for_the_largest_requests_and_answers(
    cluster: Cluster, control: Client
) -> None:
    """The bytes term at its most (decisions I39): ten holders on job B, each request padded
    to ``RelayRequestMax`` with update ids of ``limit.maxIDLength``, polling a listed topic of
    64 characters (every reference at its longest), on a history the seller grew first: the
    bytes guard stops the run before its poll budget, at half the limit plus the polls in
    flight, and what the polls added is within the formula."""
    # Under the codec's claim-check threshold (256 KiB), so each stays in the history:
    # twenty-eight are about 7 MB of it.
    ballast = "b" * 250_000
    async with Bench(cluster, control) as b:
        await b.start({"B": {"topic": LONG_TOPIC}, "B0": {"topic": LONG_TOPIC}})
        for label in ("B", "B0"):
            for _ in range(28):
                await b.handles[label].signal(app.BoundJob.ballast, ballast)
        identity = padded_identity(b.jobs["B"], topic=LONG_TOPIC, long_ids=True)
        attack = Attack(await b.relay("B"), b.jobs["B"], identity, topic=LONG_TOPIC, long_ids=True)
        padded = poll_request(
            attack.workflow_id, identity=identity, topic=LONG_TOPIC, update_id=long_update_id()
        )
        assert padded.ByteSize() >= REQUEST_MAX - 1
        attack_secs = await attack_until_full(attack, 10)
        results, runs = await b.finish()
        await cold_replay(b.seller, runs["B"][0].history)

    assert_survived(b.samples, results, runs)
    measured = within_formula(runs["B"][0], runs["B0"][0], "B")
    measured["worst_length"], measured["worst_size"] = b.samples.worst["B"]
    assert measured["accepted"] < s.STREAM_POLLS_MAX
    assert b.samples.worst["B"][1] >= relay_bound.interceptors.STREAM_HISTORY_BYTES
    assert attack.tally["rate"] > 0
    print(
        "relay bound B, measured:",
        json.dumps(measured, sort_keys=True),
        f"attack {attack_secs:.1f} s",
    )


async def test_a_long_stream_costs_a_poll_its_answer(cluster: Cluster, control: Client) -> None:
    """A stream of 5,000 small items, polled from 0 by ten holders -- half naming the listed
    topic, half a topic the log does not hold -- so that a poll would cost the log's length
    if anything scanned or encoded it (the contrib alone encodes all 5,000 for each poll from
    0; decisions I39): job D accepts its budget, runs on, and its history replays on a cold
    worker well within a task's timeout -- what the polls add to that replay, against its
    twin's, a small part of it."""
    big = 5_000
    async with Bench(cluster, control) as b:
        await b.start({"D": {"items": big, "pad": 0}, "D0": {"items": big, "pad": 0}})
        relay = await b.relay("D")
        listed, absent = Attack(relay, b.jobs["D"]), Attack(relay, b.jobs["D"], topic="absent")
        absent_tasks = absent.run(5)
        try:
            attack_secs = await attack_until_full(listed, 5)
        finally:
            absent.stop.set()
            await asyncio.gather(*absent_tasks, return_exceptions=True)
        results, runs = await b.finish()
        twin_secs = round(await cold_replay(b.seller, runs["D0"][0].history), 2)
        replay_secs = round(await cold_replay(b.seller, runs["D"][0].history), 2)

    assert_survived(b.samples, results, runs)
    measured = within_formula(runs["D"][0], runs["D0"][0], "D")
    assert measured["accepted"] == s.STREAM_POLLS_MAX
    assert absent.tally["answered"] > 0  # answered: bounded scans, nothing in them
    assert replay_secs < TASK_TIMEOUT / 2, replay_secs
    assert replay_secs - twin_secs < TASK_TIMEOUT / 4, (replay_secs, twin_secs)
    print(
        "relay bound D, measured:",
        json.dumps(measured, sort_keys=True),
        f"attack {attack_secs:.1f} s, cold replay {replay_secs} s (its twin {twin_secs} s)",
    )


async def _at(handle: WorkflowHandle[Any, str], n: int) -> bool:
    return await offset_of(handle) >= n


def _closed(control: Client, workflow_id: str) -> Callable[[], Awaitable[bool]]:
    async def closed() -> bool:
        return (await describe(control, workflow_id)())[2] != (
            WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
        )

    return closed
