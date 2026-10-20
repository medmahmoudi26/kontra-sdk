"""``ProgressForwarder`` (decisions I31): each forwarded heartbeat is a ``ProgressRecord``
published on the ``kontra.progress`` topic of the job's own stream (``job-<id>``) with
temporalio's ``WorkflowStreamClient``; latest wins per interval, every heartbeat in order at
0, the interval backs off and the budget ends forwarding (each item costs the job's
history); ``settle`` before an activity returns; never an error for the activity; a closed
job dropped."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pytest
from temporalio.api.common.v1 import Payload
from temporalio.contrib.workflow_streams import PublishInput
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode

from kontra.contract.names import PROGRESS_TOPIC, STREAM_PUBLISH_SIGNAL, job_workflow_id, new_job_id
from kontra.serve import ProgressForwarder
from kontra.serve import progress as fwd
from kontra.serve.progress import DEFAULT_INTERVAL, INTERVAL_ENV, progress_interval
from kontra.types import ProgressRecord


@dataclass
class Handle:
    temporal: Temporal
    workflow_id: str

    @property
    def id(self) -> str:
        return self.workflow_id

    async def signal(self, name: str, arg: Any) -> None:
        await self.temporal.signalled(self.workflow_id, name, arg)


@dataclass
class Temporal:
    """The worker's client as the forwarder uses it: publish signals to ``job-<id>``."""

    delay: float = 0.0
    fail: Exception | None = None
    #: (workflow id, the batch's items as (topic, record)).
    signals: list[tuple[str, list[tuple[str, ProgressRecord]]]] = field(
        default_factory=list[tuple[str, list[tuple[str, ProgressRecord]]]]
    )
    data_converter: DataConverter = DataConverter.default

    def get_workflow_handle(self, workflow_id: str) -> Handle:
        return Handle(self, workflow_id)

    async def signalled(self, workflow_id: str, name: str, arg: Any) -> None:
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail is not None:
            raise self.fail
        assert name == STREAM_PUBLISH_SIGNAL
        assert isinstance(arg, PublishInput)
        items: list[tuple[str, ProgressRecord]] = []
        for entry in arg.items:
            p = Payload()
            p.ParseFromString(base64.b64decode(entry.data))
            assert p.metadata["encoding"] == b"json/plain"
            items.append((entry.topic, ProgressRecord.model_validate_json(p.data)))
        self.signals.append((workflow_id, items))

    def records(self, job_id: str | None = None) -> list[ProgressRecord]:
        return [
            r
            for wf, items in self.signals
            if job_id is None or wf == job_workflow_id(job_id)
            for _, r in items
        ]

    def details(self, job_id: str | None = None) -> list[Any]:
        return [r.details for r in self.records(job_id)]


def forwarder(temporal: Temporal, **kw: Any) -> ProgressForwarder:
    fw = ProgressForwarder(**kw)
    fw.bind(temporal)  # type: ignore[arg-type]  # the part of a Client the forwarder uses
    return fw


async def _until(cond: Any) -> None:
    """Poll ``cond`` (a fake client has no event to wait on) for up to five seconds."""
    async with asyncio.timeout(5.0):
        while not cond():  # noqa: ASYNC110
            await asyncio.sleep(0.005)


def test_the_interval_comes_from_the_environment() -> None:
    assert DEFAULT_INTERVAL == 5.0
    assert progress_interval({}) == DEFAULT_INTERVAL
    assert progress_interval({INTERVAL_ENV: "0"}) == 0
    assert progress_interval({INTERVAL_ENV: "2.5"}) == 2.5
    for bad in ("-1", "soon", "nan", "inf"):
        assert progress_interval({INTERVAL_ENV: bad}) == DEFAULT_INTERVAL
    with pytest.raises(ValueError, match=">= 0"):
        ProgressForwarder(interval=-1)
    with pytest.raises(ValueError, match="budget"):
        ProgressForwarder(budget=-1)


async def test_records_are_published_on_the_jobs_progress_topic() -> None:
    temporal = Temporal()
    fw = forwarder(temporal, interval=0)
    fw.start()
    job = new_job_id()
    fw.offer(job, "fetch_batch", ({"percent": 10, "message": "10 records"},))
    await fw.flush(job)
    [(wf, [(topic, record)])] = temporal.signals
    assert (wf, topic) == (job_workflow_id(job), PROGRESS_TOPIC)
    assert (record.v, record.activity) == (1, "fetch_batch")
    assert record.details == {"percent": 10, "message": "10 records"}
    assert record.at.tzinfo is not None
    await fw.aclose()


async def test_interval_zero_publishes_every_heartbeat_in_order_in_few_signals() -> None:
    temporal = Temporal(delay=0.001)
    fw = forwarder(temporal, interval=0)
    fw.start()
    job = new_job_id()
    for i in range(100):
        fw.offer(job, "beat", ({"percent": i},))
    await fw.flush(job)
    assert [d["percent"] for d in temporal.details()] == list(range(100))
    assert len(temporal.signals) < 100  # what is queued goes in one signal: history is the cost
    await fw.aclose()
    assert (fw.sent, fw.dropped) == (100, 0)


async def test_an_interval_keeps_only_the_latest_and_paces_each_job() -> None:
    now = [0.0]
    temporal = Temporal()
    fw = forwarder(temporal, interval=10, clock=lambda: now[0])
    fw.start()
    a, b = new_job_id(), new_job_id()
    fw.offer(a, "x", (1,))
    await _until(lambda: len(temporal.signals) == 1)  # the first one goes at once
    for i in range(2, 6):
        fw.offer(a, "x", (i,))
    fw.offer(b, "y", ("b",))
    await _until(lambda: len(temporal.signals) == 2)  # b is not held back by a's pace
    await asyncio.sleep(0.05)
    assert temporal.details(a) == [1]  # a waits for its interval
    now[0] += 10
    fw.offer(a, "x", (6,))  # a heartbeat wakes the sender; only the latest is sent
    await _until(lambda: len(temporal.details(a)) == 2)
    assert temporal.details(a) == [1, 6]
    assert temporal.details(b) == ["b"]
    fw.offer(a, "x", (7,))
    await fw.flush(a)  # the worker stopping: sent now, whatever the interval
    assert temporal.details(a) == [1, 6, 7]
    await fw.aclose()


def test_the_interval_backs_off() -> None:
    """Every forwarded item costs the job's history: the job's interval doubles after every
    BACKOFF_EVERY items, up to INTERVAL_MAX; at 0 it stays 0."""
    fw = ProgressForwarder(interval=5)
    job = fwd._Job()  # pyright: ignore[reportPrivateUsage]
    seen: list[float] = []
    for forwarded in (0, 359, 360, 719, 720, 1080, 10**6):
        job.forwarded = forwarded
        seen.append(fw._interval(job))  # pyright: ignore[reportPrivateUsage]
    assert fwd.BACKOFF_EVERY == 360
    assert seen == [5, 5, 10, 10, 20, 40, fwd.INTERVAL_MAX]
    assert ProgressForwarder(interval=0)._interval(job) == 0  # pyright: ignore[reportPrivateUsage]


async def test_the_budget_ends_forwarding(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, "kontra.serve.progress")
    now = [0.0]
    temporal = Temporal()
    fw = forwarder(temporal, interval=0, budget=3, clock=lambda: now[0])
    fw.start()
    job = new_job_id()
    for i in range(6):
        fw.offer(job, "x", (i,))
        await fw.settle(job)
        now[0] += 1
    assert temporal.details(job) == [0, 1, 2]
    assert "no more are published" in caplog.text
    assert caplog.text.count("no more are published") == 1  # said once
    assert fw.dropped == 3
    fw.offer(job, "x", ("late",))
    await fw.flush(job)
    assert len(temporal.records(job)) == 3
    await fw.aclose()


async def test_interval_zero_is_budgeted_too() -> None:
    temporal = Temporal()
    fw = forwarder(temporal, interval=0, budget=10)
    fw.start()
    job = new_job_id()
    for i in range(25):
        fw.offer(job, "beat", (i,))
    await fw.flush(job)
    assert temporal.details(job) == list(range(10))
    assert fw.dropped == 15
    await fw.aclose()


async def test_multiple_details_are_a_list() -> None:
    temporal = Temporal()
    fw = forwarder(temporal, interval=0)
    fw.start()
    job = new_job_id()
    fw.offer(job, "x", (1, "two", {"three": 3}))
    fw.offer(job, "x", ())
    await fw.flush(job)
    assert temporal.details() == [[1, "two", {"three": 3}], []]
    await fw.aclose()


async def test_offers_from_threads_arrive() -> None:
    temporal = Temporal()
    fw = forwarder(temporal, interval=0)
    fw.start()
    job = new_job_id()

    def worker(n: int) -> None:
        for i in range(20):
            fw.offer(job, "t", ({"t": n, "i": i},))

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        await asyncio.to_thread(t.join)
    await _until(lambda: len(temporal.records()) == 80)
    for n in range(4):  # each thread's heartbeats stay in its order
        assert [d["i"] for d in temporal.details() if d["t"] == n] == list(range(20))
    await fw.aclose()


async def test_failures_never_reach_the_activity() -> None:
    temporal = Temporal()
    fw = forwarder(temporal, interval=0)
    job = new_job_id()
    fw.offer(job, "x", (1,))  # not started: ignored, quietly
    fw.start()
    fw.offer(job, "x", (object(),))  # not JSON
    fw.offer(job, "x", ("x" * 100_000,))  # too big
    fw.offer(job, "x\x1b[2J", (1,))  # an activity name the record refuses
    await fw.flush(job)
    assert fw.dropped == 3
    temporal.fail = RPCError("workflow execution already completed", RPCStatusCode.NOT_FOUND, b"")
    fw.offer(job, "x", (2,))
    await fw.flush(job)  # the job closed: dropped, not retried
    assert temporal.signals == []
    assert fw.dropped == 4
    temporal.fail = None
    fw.offer(job, "x", (4,))
    await fw.flush(job)
    assert temporal.details() == [4]  # a fresh publisher: the failed batch is not sent again
    await fw.aclose()
    fw.offer(job, "x", (5,))  # closed: ignored
    assert len(temporal.signals) == 1


async def test_an_unbound_forwarder_drops_and_says_so(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, "kontra.serve.progress")
    fw = ProgressForwarder(interval=0)
    fw.start()
    job = new_job_id()
    fw.offer(job, "x", (1,))
    await fw.flush(job)
    assert (fw.sent, fw.dropped) == (0, 1)
    assert "no client" in caplog.text
    await fw.aclose()


async def test_close_sends_what_is_queued() -> None:
    temporal = Temporal()
    now = [0.0]
    fw = forwarder(temporal, interval=60, clock=lambda: now[0])
    fw.start()
    job = new_job_id()
    fw.offer(job, "x", (1,))
    await _until(lambda: len(temporal.signals) == 1)
    fw.offer(job, "x", (2,))
    await fw.aclose()
    assert temporal.details() == [1, 2]
    assert all(isinstance(r.at, datetime) for r in temporal.records())


async def test_close_is_not_lost_to_a_heartbeat_in_the_same_pass() -> None:
    """A heartbeat's poke and aclose() in one loop pass, while the sender waits out the
    interval. On Python 3.11 asyncio.wait_for dropped that cancel: the sender then waited for
    a heartbeat forever, and aclose() -- a worker's shutdown -- with it."""
    temporal = Temporal()
    fw = forwarder(temporal, interval=60, clock=lambda: 0.0)
    fw.start()
    fw.offer(new_job_id(), "x", (1,))
    await _until(lambda: len(temporal.signals) == 1)
    await asyncio.sleep(0.05)  # the sender is back to waiting, with the interval as timeout
    fw._poke()  # pyright: ignore[reportPrivateUsage]
    async with asyncio.timeout(5.0):
        await fw.aclose()
    assert temporal.details() == [1]


async def test_a_slow_temporal_does_not_break_the_pace() -> None:
    """Heartbeats that arrive while a publish is in flight wait for the interval: one item
    per interval per job, however slow the signal is."""
    now = [0.0]
    temporal = Temporal(delay=0.05)
    fw = forwarder(temporal, interval=10, clock=lambda: now[0])
    fw.start()
    job = new_job_id()
    fw.offer(job, "x", (1,))
    await asyncio.sleep(0.01)  # the first item is on its way
    fw.offer(job, "x", (2,))
    fw.offer(job, "x", (3,))
    await _until(lambda: len(temporal.signals) == 1)
    await asyncio.sleep(0.2)
    assert temporal.details(job) == [1]  # 3 is due 10 s after 1, not when 1 arrived
    now[0] += 10
    fw.offer(job, "x", (4,))
    await _until(lambda: len(temporal.signals) == 2)
    assert temporal.details(job) == [1, 4]
    await fw.aclose()


async def test_a_hung_temporal_holds_activities_up_once_at_most(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A returning activity waits for its due heartbeats, but only up to the settle bound,
    and after a publish was that slow the next activities do not wait at all: forwarding
    never sets the pace of the seller's work (D1)."""
    from types import SimpleNamespace

    from temporalio import activity
    from temporalio.worker import (
        ActivityInboundInterceptor,
        ActivityOutboundInterceptor,
        ExecuteActivityInput,
    )

    from kontra.serve import JOB_HEADER, KontraInterceptor
    from kontra.serve.interceptors import job_header

    class Sdk(ActivityOutboundInterceptor):
        def __init__(self) -> None:
            self.beats: list[tuple[Any, ...]] = []

        def heartbeat(self, *details: Any) -> None:
            self.beats.append(details)

    class Activity(ActivityInboundInterceptor):
        def __init__(self) -> None:
            self.outbound: ActivityOutboundInterceptor | None = None

        def init(self, outbound: ActivityOutboundInterceptor) -> None:
            self.outbound = outbound

        async def execute_activity(self, input: ExecuteActivityInput) -> Any:
            assert self.outbound is not None
            for i in range(3):
                self.outbound.heartbeat({"percent": i})
            return "done"

    monkeypatch.setattr(activity, "info", lambda: SimpleNamespace(activity_type="beat"))
    temporal = Temporal(delay=30)  # a signal that hangs
    fw = forwarder(temporal, interval=0, settle_timeout=0.2)
    fw.start()
    job = new_job_id()
    loop = asyncio.get_running_loop()

    async def run_activity() -> float:
        inner, sdk = Activity(), Sdk()
        inbound = KontraInterceptor(fw).intercept_activity(inner)
        inbound.init(sdk)
        started = loop.time()
        result = await inbound.execute_activity(
            ExecuteActivityInput(
                fn=lambda: None, args=[], executor=None, headers={JOB_HEADER: job_header(job)}
            )
        )
        assert result == "done"
        assert len(sdk.beats) == 3  # passed on to the SDK unchanged
        return loop.time() - started

    first = await run_activity()
    assert 0.15 < first < 1.0  # waited for the bound, not for the signal
    assert await run_activity() < 0.1  # Temporal is known slow: no waiting now
    assert temporal.signals == []
    await fw.aclose(timeout=0.1)


async def test_settle_publishes_the_latest_before_the_activity_returns() -> None:
    """Interval 0: a returning activity's heartbeats are all in the job's stream when settle
    returns. With an interval, its latest one is, whatever the interval: the activity's last
    word reaches the buyer (acceptance 13), and the pace holds while it runs."""
    temporal = Temporal(delay=0.002)
    fw = forwarder(temporal, interval=0)
    fw.start()
    job = new_job_id()
    for i in range(50):
        fw.offer(job, "beat", ({"percent": i},))
    await fw.settle(job)
    assert [d["percent"] for d in temporal.details(job)] == list(range(50))
    await fw.aclose()

    now = [0.0]
    temporal = Temporal()
    fw = forwarder(temporal, interval=10, clock=lambda: now[0])
    fw.start()
    fw.offer(job, "beat", (1,))
    await _until(lambda: temporal.details(job) == [1])  # the first goes at once
    for i in range(2, 6):
        fw.offer(job, "beat", (i,))
    await asyncio.sleep(0.05)
    assert temporal.details(job) == [1]  # paced while the activity runs
    await fw.settle(job)  # the activity returns: its latest, now
    assert temporal.details(job) == [1, 5]
    await fw.settle(job)  # nothing queued: nothing sent
    assert temporal.details(job) == [1, 5]
    await fw.aclose()


def test_the_record_is_json_the_buyer_reads() -> None:
    record = ProgressRecord.model_validate_json(
        json.dumps({"v": 1, "at": "2026-10-20T00:00:00Z", "activity": "a", "details": [1, None]})
    )
    assert record.details == [1, None]
