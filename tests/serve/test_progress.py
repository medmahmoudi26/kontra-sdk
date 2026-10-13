"""``ProgressForwarder``: latest wins per interval, every heartbeat in order at 0, only
ciphertext to the gate, never an error for the activity."""

from __future__ import annotations

import asyncio
import base64
import json
import threading
from datetime import datetime
from typing import Any

import pytest

from kontra.codec.age import decrypt, generate_identity
from kontra.contract.names import new_job_id
from kontra.gate_client import models as m
from kontra.serve import ProgressForwarder
from kontra.serve.progress import DEFAULT_INTERVAL, INTERVAL_ENV, progress_interval

BUYER = generate_identity()


class Keys:
    def __init__(self) -> None:
        self.fail = False

    async def buyer_key(self, job_id: str) -> str:
        if self.fail:
            raise LookupError("gate down")
        return BUYER[1]


class Gate:
    def __init__(self, delay: float = 0.0) -> None:
        self.posts: list[tuple[str, m.ProgressPost]] = []
        self.delay = delay
        self.fail = False

    async def post_progress(self, job_id: str, event: m.ProgressPost) -> None:
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail:
            raise ConnectionError("gate down")
        self.posts.append((job_id, event))

    def details(self, job_id: str | None = None) -> list[Any]:
        return [
            json.loads(decrypt(base64.b64decode(p.ciphertext_b64), BUYER[0]))
            for j, p in self.posts
            if job_id is None or j == job_id
        ]


async def _until(cond: Any) -> None:
    """Poll ``cond`` (a fake gate has no event to wait on) for up to five seconds."""
    async with asyncio.timeout(5.0):
        while not cond():  # noqa: ASYNC110
            await asyncio.sleep(0.005)


def test_the_interval_comes_from_the_environment() -> None:
    assert progress_interval({}) == DEFAULT_INTERVAL
    assert progress_interval({INTERVAL_ENV: "0"}) == 0
    assert progress_interval({INTERVAL_ENV: "2.5"}) == 2.5
    for bad in ("-1", "soon", "nan", "inf"):
        assert progress_interval({INTERVAL_ENV: bad}) == DEFAULT_INTERVAL
    with pytest.raises(ValueError, match=">= 0"):
        ProgressForwarder(Gate(), Keys(), interval=-1)


async def test_interval_zero_sends_every_heartbeat_in_order() -> None:
    gate = Gate(delay=0.001)
    fw = ProgressForwarder(gate, Keys(), interval=0)
    fw.start()
    job = new_job_id()
    for i in range(100):
        fw.offer(job, "beat", ({"percent": i},))
    await fw.flush(job)
    assert [d["percent"] for d in gate.details()] == list(range(100))
    assert {p.activity for _, p in gate.posts} == {"beat"}
    assert all(b"percent" not in base64.b64decode(p.ciphertext_b64) for _, p in gate.posts)
    await fw.aclose()
    assert (fw.sent, fw.dropped) == (100, 0)


async def test_an_interval_keeps_only_the_latest_and_paces_each_job() -> None:
    now = [0.0]
    gate = Gate()
    fw = ProgressForwarder(gate, Keys(), interval=10, clock=lambda: now[0])
    fw.start()
    a, b = new_job_id(), new_job_id()
    fw.offer(a, "x", (1,))
    await _until(lambda: len(gate.posts) == 1)  # the first one goes at once
    for i in range(2, 6):
        fw.offer(a, "x", (i,))
    fw.offer(b, "y", ("b",))
    await _until(lambda: len(gate.posts) == 2)  # b is not held back by a's pace
    await asyncio.sleep(0.05)
    assert gate.details(a) == [1]  # a waits for its interval
    now[0] += 10
    fw.offer(a, "x", (6,))  # a heartbeat wakes the sender; only the latest is sent
    await _until(lambda: len(gate.details(a)) == 2)
    assert gate.details(a) == [1, 6]
    assert gate.details(b) == ["b"]
    fw.offer(a, "x", (7,))
    await fw.flush(a)  # the worker stopping: sent now, whatever the interval
    assert gate.details(a) == [1, 6, 7]
    await fw.aclose()


async def test_multiple_details_are_a_list() -> None:
    gate = Gate()
    fw = ProgressForwarder(gate, Keys(), interval=0)
    fw.start()
    job = new_job_id()
    fw.offer(job, "x", (1, "two", {"three": 3}))
    fw.offer(job, "x", ())
    await fw.flush(job)
    assert gate.details() == [[1, "two", {"three": 3}], []]
    await fw.aclose()


async def test_offers_from_threads_arrive() -> None:
    gate = Gate()
    fw = ProgressForwarder(gate, Keys(), interval=0)
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
    await _until(lambda: len(gate.posts) == 80)
    for n in range(4):  # each thread's heartbeats stay in its order
        assert [d["i"] for d in gate.details() if d["t"] == n] == list(range(20))
    await fw.aclose()


async def test_failures_never_reach_the_activity(caplog: pytest.LogCaptureFixture) -> None:
    gate, keys = Gate(), Keys()
    fw = ProgressForwarder(gate, keys, interval=0)
    job = new_job_id()
    fw.offer(job, "x", (1,))  # not started: ignored, quietly
    fw.start()
    fw.offer(job, "x", (object(),))  # not JSON
    fw.offer(job, "x", ("x" * 100_000,))  # too big
    assert fw.dropped == 2
    gate.fail = True
    fw.offer(job, "x", (2,))
    await fw.flush(job)
    keys.fail, gate.fail = True, False
    fw.offer(job, "x", (3,))
    await fw.flush(job)
    assert gate.posts == []
    assert fw.dropped == 4
    keys.fail = False
    fw.offer(job, "x", (4,))
    await fw.flush(job)
    assert gate.details() == [4]
    await fw.aclose()
    fw.offer(job, "x", (5,))  # closed: ignored
    assert len(gate.posts) == 1


async def test_close_sends_what_is_queued() -> None:
    gate = Gate()
    now = [0.0]
    fw = ProgressForwarder(gate, Keys(), interval=60, clock=lambda: now[0])
    fw.start()
    job = new_job_id()
    fw.offer(job, "x", (1,))
    await _until(lambda: len(gate.posts) == 1)
    fw.offer(job, "x", (2,))
    await fw.aclose()
    assert gate.details() == [1, 2]
    assert all(isinstance(p.at, datetime) for _, p in gate.posts)


async def test_close_is_not_lost_to_a_heartbeat_in_the_same_pass() -> None:
    """A heartbeat's poke and aclose() in one loop pass, while the sender waits out the
    interval. On Python 3.11 asyncio.wait_for dropped that cancel: the sender then waited for
    a heartbeat forever, and aclose() -- a worker's shutdown -- with it."""
    gate = Gate()
    fw = ProgressForwarder(gate, Keys(), interval=60, clock=lambda: 0.0)
    fw.start()
    fw.offer(new_job_id(), "x", (1,))
    await _until(lambda: len(gate.posts) == 1)
    await asyncio.sleep(0.05)  # the sender is back to waiting, with the interval as timeout
    fw._poke()
    async with asyncio.timeout(5.0):
        await fw.aclose()
    assert gate.details() == [1]


async def test_a_slow_gate_does_not_break_the_pace() -> None:
    """Heartbeats that arrive while an event is in flight wait for the interval: one event
    per interval per job (PRD §8.4), however slow the gate is."""
    now = [0.0]
    gate = Gate(delay=0.05)
    fw = ProgressForwarder(gate, Keys(), interval=10, clock=lambda: now[0])
    fw.start()
    job = new_job_id()
    fw.offer(job, "x", (1,))
    await asyncio.sleep(0.01)  # the first event is on its way
    fw.offer(job, "x", (2,))
    fw.offer(job, "x", (3,))
    await _until(lambda: len(gate.posts) == 1)
    await asyncio.sleep(0.2)
    assert gate.details(job) == [1]  # 3 is due 10 s after 1, not when 1 arrived
    now[0] += 10
    fw.offer(job, "x", (4,))
    await _until(lambda: len(gate.posts) == 2)
    assert gate.details(job) == [1, 4]
    await fw.aclose()


async def test_a_hung_gate_holds_activities_up_once_at_most(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A returning activity waits for its due heartbeats, but only up to the settle bound,
    and after a gate was that slow the next activities do not wait at all: the gate's
    health never sets the pace of the seller's work (D1)."""
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
    gate = Gate(delay=30)  # a gate that hangs
    fw = ProgressForwarder(gate, Keys(), interval=0, settle_timeout=0.2)
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
    assert 0.15 < first < 1.0  # waited for the bound, not for the gate
    assert await run_activity() < 0.1  # the gate is known slow: no waiting now
    assert gate.posts == []
    await fw.aclose(timeout=0.1)


async def test_settle_waits_for_due_heartbeats_only() -> None:
    """Interval 0: a returning activity's heartbeats are all at the gate when settle
    returns. With an interval, settle never sends ahead of it."""
    gate = Gate(delay=0.002)
    fw = ProgressForwarder(gate, Keys(), interval=0)
    fw.start()
    job = new_job_id()
    for i in range(50):
        fw.offer(job, "beat", ({"percent": i},))
    await fw.settle(job)
    assert [d["percent"] for d in gate.details(job)] == list(range(50))
    await fw.aclose()

    now = [0.0]
    gate = Gate()
    fw = ProgressForwarder(gate, Keys(), interval=10, clock=lambda: now[0])
    fw.start()
    fw.offer(job, "beat", (1,))
    await fw.settle(job)  # due (nothing sent yet): sent before settle returns
    assert gate.details(job) == [1]
    fw.offer(job, "beat", (2,))
    await fw.settle(job)  # not due for 10 s: settle does not jump the queue
    await asyncio.sleep(0.05)
    assert gate.details(job) == [1]
    now[0] += 10
    await fw.settle(job)
    assert gate.details(job) == [1, 2]
    await fw.aclose()
