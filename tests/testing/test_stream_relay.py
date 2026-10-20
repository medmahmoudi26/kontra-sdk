"""A job's stream on the fake gate (decisions I31), over a served gate so the SSE stream is
real: the relay (``kontra._relay``) started once the job is pinned and its first listener
attaches, from offset 0 (OD2); items, gaps and history events interleaved by cursor; the
offset kept across listener gaps; replay from the store (``after``, ``Last-Event-ID``); the
caps (256 KiB per item, per-topic and per-job budgets, eight listeners); unknown topics
refused; a relay stop recorded as a gap; and the stream ending once the job is closed and its
relay finished.

The seller's Temporal is scripted here (:class:`FakeTemporal` answers the relay's two calls
from a log of sealed items); the same relay against a real worker is
``tests/serve/test_streams.py`` and ``tests/hire``."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field, replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from temporalio.api.common.v1 import Payload, Payloads
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.api.update.v1 import Outcome
from temporalio.api.workflow.v1 import WorkflowExecutionInfo
from temporalio.api.workflowservice.v1 import (
    DescribeWorkflowExecutionResponse,
    UpdateWorkflowExecutionResponse,
)

from kontra._control import HistorySummary
from kontra._relay import STREAM_FULL
from kontra.auth import Session
from kontra.codec import stream as s
from kontra.codec.age import encrypt, generate_identity
from kontra.contract.names import PROGRESS_TOPIC
from kontra.escrow import ObservedStatus
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.testing.fake_gate import FakeGate

from .support import ScriptedControl, json_payload, logged_in, ok_quote, wire

#: A buyer key for a direct `GateClient.hire` call (a test drives the route itself).
_A_BUYER_KEY = "age1e0q4lyexsefmh6hurumvgl3cmmsfnz4zyfhkd7lfrjcpqc2e250qs6j4el"
LISTED = "events"
KEY = generate_identity()


def _doc(**facts: Any) -> dict[str, Any]:
    from .support import offering_doc

    return offering_doc("acme", **facts)


@dataclass
class FakeTemporal:
    """The seller's Temporal as the relay sees it: its two calls answered from a log of
    sealed items (topic None: one the buyer may not read), and the gate's Describe."""

    log: list[tuple[str | None, bytes | None, int]] = field(
        default_factory=list[tuple[str | None, bytes | None, int]]
    )
    #: Every poll: (topics, from_offset, first_execution_run_id).
    polls: list[tuple[list[str], int, str]] = field(
        default_factory=list[tuple[list[str], int, str]]
    )
    failure: str | None = None
    raw: Payload | None = None
    history: tuple[int, int] = (10, 1000)

    @property
    def workflow_service(self) -> FakeTemporal:
        return self

    def publish(self, topic: str | None, n: int = 1, *, size: int | None = None) -> None:
        for _ in range(n):
            if size is not None:
                self.log.append((topic, None, size))  # claim-checked: too large to relay
                continue
            ct = encrypt(f"item {len(self.log)}".encode(), [KEY[1]])
            self.log.append((topic, ct, len(ct)))

    async def update_workflow_execution(self, req: Any, retry: bool = False, metadata: Any = None, timeout: Any = None) -> UpdateWorkflowExecutionResponse:  # noqa: ASYNC109  # fmt: skip
        args = json.loads(req.request.input.args.payloads[0].data)
        offset: int = args["from_offset"]
        self.polls.append((args["topics"], offset, req.first_execution_run_id))
        if self.failure is not None:
            info = ApplicationFailureInfo(type=self.failure)
            return UpdateWorkflowExecutionResponse(
                outcome=Outcome(failure=Failure(message="no", application_failure_info=info))
            )
        if self.raw is not None:
            return _answer(self.raw)
        deadline = time.monotonic() + 0.3
        while len(self.log) <= offset and time.monotonic() < deadline:  # noqa: ASYNC110
            await asyncio.sleep(0.02)  # a long poll: the stream waits for items
        if len(self.log) <= offset:
            return UpdateWorkflowExecutionResponse()  # returned at ACCEPTED: sent again
        items: list[dict[str, Any]] = []
        for i, (topic, ct, size) in enumerate(self.log):
            if i < offset or (topic is not None and topic not in args["topics"]):
                continue
            if ct is None:
                items.append({"topic": topic, "offset": i, "size": size, "ct": None})
            else:
                items.append({"topic": topic, "offset": i, "ct": base64.b64encode(ct).decode()})
        env = {"v": 1, "items": items, "next_offset": len(self.log), "more_ready": False}
        return _answer(
            Payload(
                metadata={"encoding": s.STREAM_ENCODING, "kontra-v": b"1"},
                data=json.dumps(env).encode(),
            )
        )

    async def describe_workflow_execution(self, req: Any, retry: bool = False, metadata: Any = None, timeout: Any = None) -> DescribeWorkflowExecutionResponse:  # noqa: ASYNC109  # fmt: skip
        length, size = self.history
        info = WorkflowExecutionInfo(history_length=length, history_size_bytes=size)
        return DescribeWorkflowExecutionResponse(workflow_execution_info=info)


def _answer(p: Payload) -> UpdateWorkflowExecutionResponse:
    resp = UpdateWorkflowExecutionResponse(outcome=Outcome(success=Payloads(payloads=[p])))
    resp.update_ref.workflow_execution.run_id = "run-1"
    return resp


@dataclass
class Live:
    gate: FakeGate
    control: ScriptedControl
    temporal: FakeTemporal
    seller: Session
    buyer: Session
    job: m.HireView
    root: Path

    @property
    def wf(self) -> str:
        return f"job-{self.job.job_id}"

    def running(self, **summary: Any) -> None:
        """The job's run as escrow's observer sees it (pinned: ``run-1``)."""
        self.control.observe_as("acme", self.wf, ObservedStatus.RUNNING)
        self.control.summaries[("acme", self.wf)] = HistorySummary(
            state=ObservedStatus.RUNNING, last_event="ActivityTaskScheduled", **summary
        )

    def completed(self) -> None:
        self.control.summaries[("acme", self.wf)] = HistorySummary(
            state=ObservedStatus.COMPLETED, last_event="WorkflowExecutionCompleted"
        )
        self.control.observe_as("acme", self.wf, ObservedStatus.COMPLETED, b"result")

    async def stored(self) -> list[m.StreamEvent]:
        async def get() -> list[m.StreamEvent]:
            return list(self.gate.jobs[self.job.job_id].events)

        return await self.gate.run(get)


async def make_live(tmp_path: Path, **options: Any) -> AsyncIterator[Live]:
    control = ScriptedControl(responder=lambda p, h: ok_quote(500))
    temporal = FakeTemporal()
    control.client = temporal  # type: ignore[attr-defined]  # what the gate relays through
    options.setdefault("relay_interval", 0.05)
    options.setdefault("relay_grace", 0.2)
    gate = FakeGate(control, sweep_interval=0.05, keepalive=0.2, **options)
    base = await gate.serve()
    seller = await logged_in(base, tmp_path / "acme", "acme")
    buyer = await logged_in(base, tmp_path / "bob", "bob")
    await seller.gate.publish(_doc(), streams=[LISTED])
    q = await buyer.gate.quote("acme/enrich", wire(json_payload({"text": "x"})))
    assert q.quote_id is not None
    job = await buyer.gate.hire(q.quote_id, buyer_key=_A_BUYER_KEY)
    try:
        yield Live(gate, control, temporal, seller, buyer, job, tmp_path)
    finally:
        await seller.aclose()
        await buyer.aclose()
        await gate.stop()


@pytest.fixture
async def live(tmp_path: Path) -> AsyncIterator[Live]:
    async for one in make_live(tmp_path):
        yield one


async def _until(predicate: Callable[[], bool], within: float = 10.0) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + within
    while not predicate():
        if loop.time() > end:
            raise AssertionError("condition never held")
        await asyncio.sleep(0.02)


def _count(items: list[m.StreamEvent], n: int) -> Callable[[], bool]:
    return lambda: len(items) == n


def _follow(live: Live, into: list[m.StreamEvent], **kw: Any) -> asyncio.Task[None]:
    async def follow() -> None:
        async for event in live.buyer.gate.stream(live.job.job_id, **kw):
            into.append(event)

    return asyncio.create_task(follow())


@pytest.mark.timeout(60)
async def test_items_and_history_stream_in_order_until_the_job_closes_and_drains(
    live: Live,
) -> None:
    live.temporal.publish(PROGRESS_TOPIC)
    live.temporal.publish(None)  # a topic the buyer may not read: never stored
    live.temporal.publish(LISTED)
    assert live.temporal.polls == []  # nobody listened yet: no polling
    received: list[m.StreamEvent] = []
    task = _follow(live, received, history=True)
    live.running(activities_running=("fetch_batch",))
    await _until(lambda: len([e for e in received if e.kind is m.StreamEventKind.ITEM]) == 2)
    live.temporal.publish(LISTED, 2)
    await _until(lambda: len([e for e in received if e.kind is m.StreamEventKind.ITEM]) == 4)
    live.completed()
    await asyncio.wait_for(task, 20)  # it ends by itself: closed, and its relay finished

    assert [e.cursor for e in received] == list(range(1, len(received) + 1))
    items = [e for e in received if e.kind is m.StreamEventKind.ITEM]
    assert [(e.topic, e.offset) for e in items] == [
        (PROGRESS_TOPIC, 0),
        (LISTED, 2),
        (LISTED, 3),
        (LISTED, 4),
    ]
    history = [e for e in received if e.kind is m.StreamEventKind.HISTORY]
    assert history[0].activities_running == ["fetch_batch"]
    assert history[-1].state is m.JobState.COMPLETED
    # The relay: from offset 0 (OD2), the offering's streams and progress, the pinned chain.
    topics, first, run = live.temporal.polls[0]
    assert (topics, first, run) == (sorted([LISTED, PROGRESS_TOPIC]), 0, "run-1")
    assert {offset for _, offset, _ in live.temporal.polls} <= {0, 3, 5}
    state = await live.gate.run(_relay_of(live))
    assert (state.done, state.stopped, state.dropped) == (True, None, 1)
    # Stored: ciphertext only, and the same events replay from the store, to either party.
    stored = await live.stored()
    for e in stored:
        if e.ciphertext_b64 is not None:
            assert base64.b64decode(e.ciphertext_b64).startswith(s.AGE_MAGIC)
    replay = [e.cursor async for e in live.seller.gate.stream(live.job.job_id, history=True)]
    assert replay == [e.cursor for e in received]


def _relay_of(live: Live) -> Callable[[], Any]:
    async def get() -> Any:
        return replace(live.gate.jobs[live.job.job_id].relay, task=None, pause=None)

    return get


@pytest.mark.timeout(60)
async def test_the_relay_waits_for_the_pin_and_keeps_its_offset_across_listeners(
    live: Live,
) -> None:
    live.temporal.publish(LISTED, 2)
    first: list[m.StreamEvent] = []
    task = _follow(live, first, topics=[LISTED])
    await asyncio.sleep(0.5)
    assert live.temporal.polls == []  # not pinned: the relay never polls an unpinned job-<id>
    assert first == []
    live.running()
    await _until(lambda: len(first) == 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await _until(lambda: not live.gate.jobs[live.job.job_id].relay.running)  # paused (grace)
    polls = len(live.temporal.polls)
    live.temporal.publish(LISTED, 3)
    await asyncio.sleep(0.3)
    assert len(live.temporal.polls) == polls  # nobody listens: no polls
    again: list[m.StreamEvent] = []
    task = _follow(live, again, topics=[LISTED], after=first[-1].cursor)
    await _until(lambda: len(again) == 3)
    assert [e.offset for e in again] == [2, 3, 4]
    assert all(offset != 0 for _, offset, _ in live.temporal.polls[polls:])  # never re-read
    assert live.temporal.polls[polls][1] == 2
    live.completed()
    await asyncio.wait_for(task, 20)


@pytest.mark.timeout(60)
async def test_last_event_id_resumes_from_the_store(live: Live) -> None:
    live.temporal.publish(LISTED, 4)
    live.running()
    seen: list[m.StreamEvent] = []
    task = _follow(live, seen, topics=[LISTED])
    await _until(lambda: len(seen) == 4)
    live.completed()
    await asyncio.wait_for(task, 20)
    token = await live.buyer.access_token()
    lines: list[str] = []
    async with (
        httpx.AsyncClient(base_url=live.gate.base_url, timeout=10) as http,
        http.stream(
            "GET",
            f"/v1/jobs/{live.job.job_id}/stream",
            params={"topic": LISTED},
            headers={"Authorization": f"Bearer {token}", "Last-Event-ID": str(seen[1].cursor)},
        ) as resp,
    ):
        assert resp.status_code == 200
        lines = [line async for line in resp.aiter_lines() if line.startswith("id: ")]
    assert lines == [f"id: {e.cursor}" for e in seen[2:]]
    for bad in ("x", "-1", "1x", "01"):
        async with httpx.AsyncClient(base_url=live.gate.base_url, timeout=10) as http:
            resp = await http.get(
                f"/v1/jobs/{live.job.job_id}/stream",
                headers={"Authorization": f"Bearer {token}", "Last-Event-ID": bad},
            )
        assert (resp.status_code, resp.json()["code"]) == (422, "invalid_cursor")


@pytest.mark.timeout(60)
async def test_too_large_items_and_spent_budgets_are_gaps(tmp_path: Path) -> None:
    # Each item here is 206 or 207 bytes of ciphertext.
    async for live in make_live(tmp_path, stream_topic_budget=450, stream_job_budget=750):
        live.temporal.publish(LISTED, size=300_000)  # claim-checked by the codec
        live.temporal.publish(LISTED, 3)
        live.temporal.publish(PROGRESS_TOPIC, 2)
        live.running()
        seen: list[m.StreamEvent] = []
        task = _follow(live, seen)
        await _until(_count(seen, 6))
        live.completed()
        await asyncio.wait_for(task, 20)
        assert [(e.kind.value, e.topic, e.offset, e.reason) for e in seen] == [
            ("gap", LISTED, 0, m.StreamGapReason.TOO_LARGE),
            ("item", LISTED, 1, None),
            ("item", LISTED, 2, None),
            ("gap", LISTED, 3, m.StreamGapReason.TOPIC_BUDGET),
            ("item", PROGRESS_TOPIC, 4, None),  # another topic: its own budget
            ("gap", PROGRESS_TOPIC, 5, m.StreamGapReason.JOB_BUDGET),
        ]
        assert seen[0].size == 300_000


@pytest.mark.timeout(60)
async def test_a_spent_budget_is_one_gap_and_the_relay_reads_no_more_of_it(
    tmp_path: Path,
) -> None:
    """Past a topic's budget the gate stores one gap and polls that topic no more; past the
    job's, one gap and the relay stops: the store does not grow by a row per item the
    seller goes on publishing (each relayed poll would also cost the seller's history)."""
    # Each item is 206 or 207 bytes of ciphertext: four fit a topic, six the job.
    async for live in make_live(tmp_path, stream_topic_budget=1_000, stream_job_budget=1_300):
        live.temporal.publish(LISTED, 6)
        live.running()
        seen: list[m.StreamEvent] = []
        task = _follow(live, seen)
        await _until(_count(seen, 5))  # four items and the topic's gap
        polls = len(live.temporal.polls)
        live.temporal.publish(LISTED, 50)
        live.temporal.publish(PROGRESS_TOPIC, 2)
        await _until(_count(seen, 7))
        assert all(topics == [PROGRESS_TOPIC] for topics, _, _ in live.temporal.polls[polls:])
        live.temporal.publish(PROGRESS_TOPIC, 200)  # past the job's budget
        relay = live.gate.jobs[live.job.job_id].relay
        await _until(lambda: relay.done)  # noqa: B023 -- the loop runs once
        polls = len(live.temporal.polls)
        live.temporal.publish(PROGRESS_TOPIC, 50)
        await asyncio.sleep(0.3)
        assert len(live.temporal.polls) == polls  # stopped: no more polls
        live.completed()
        await asyncio.wait_for(task, 20)
        gaps = [(e.topic, e.reason) for e in seen if e.kind is m.StreamEventKind.GAP]
        assert gaps == [
            (LISTED, m.StreamGapReason.TOPIC_BUDGET),
            (PROGRESS_TOPIC, m.StreamGapReason.JOB_BUDGET),
        ]
        stored = await live.stored()
        assert len(stored) < 20  # not one row per item published (300 of them)
        state = await live.gate.run(_relay_of(live))
        assert (state.stopped, state.spent_topics, state.job_spent) == (
            "job_budget",
            {LISTED},
            True,
        )


@pytest.mark.timeout(60)
@pytest.mark.parametrize(
    ("failure", "raw", "reason"),
    [
        (STREAM_FULL, None, m.StreamGapReason.HISTORY_BUDGET),
        (None, Payload(metadata={"encoding": b"binary/kontra-age", "kontra-v": b"1"}, data=b"x"), m.StreamGapReason.RELAY_FAILED),
    ],
    ids=["history-budget", "not-an-envelope"],
)  # fmt: skip
async def test_a_relay_that_stops_records_a_gap_without_an_item(
    tmp_path: Path, failure: str | None, raw: Payload | None, reason: m.StreamGapReason
) -> None:
    async for live in make_live(tmp_path):
        live.temporal.failure, live.temporal.raw = failure, raw
        live.running()
        seen: list[m.StreamEvent] = []
        task = _follow(live, seen)
        await _until(_count(seen, 1))
        [gap] = seen
        assert (gap.kind, gap.reason, gap.topic, gap.offset) == (
            m.StreamEventKind.GAP,
            reason,
            None,
            None,
        )
        live.completed()
        await asyncio.wait_for(task, 20)


@pytest.mark.timeout(60)
async def test_the_pace_stops_the_relay_past_half_the_history_limits(tmp_path: Path) -> None:
    async for live in make_live(tmp_path, history_limits=(1000, 10**9)):
        live.temporal.history = (600, 0)
        live.temporal.publish(LISTED)
        live.running()
        seen: list[m.StreamEvent] = []
        task = _follow(live, seen)
        await _until(_count(seen, 2))
        assert [(e.kind, e.reason) for e in seen] == [
            (m.StreamEventKind.ITEM, None),
            (m.StreamEventKind.GAP, m.StreamGapReason.HISTORY_BUDGET),
        ]
        live.completed()
        await asyncio.wait_for(task, 20)


async def test_topics_listeners_and_parties(live: Live, tmp_path: Path) -> None:
    async def first_error(session: Session, **kw: Any) -> GateError:
        with pytest.raises(GateError) as e:
            async for _ in session.gate.stream(live.job.job_id, **kw):
                pass
        return e.value

    err = await first_error(live.buyer, topics=["private"])
    assert (err.status, err.code) == (422, "unknown_topic")
    err = await first_error(live.buyer, topics=["kontra.other"])
    assert (err.status, err.code) == (422, "unknown_topic")
    eve = await logged_in(live.gate.base_url, tmp_path / "eve", "eve")
    try:
        err = await first_error(eve)
        assert (err.status, err.code) == (404, "job_not_found")
    finally:
        await eve.aclose()
    # Eight listeners at most; the ninth is refused until one leaves.
    token = await live.buyer.access_token()
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(base_url=live.gate.base_url, timeout=10) as http:
        streams = [
            await http.send(
                http.build_request("GET", f"/v1/jobs/{live.job.job_id}/stream", headers=headers),
                stream=True,
            )
            for _ in range(m.STREAM_LISTENERS_MAX)
        ]
        try:
            assert all(r.status_code == 200 for r in streams)
            refused = await http.get(f"/v1/jobs/{live.job.job_id}/stream", headers=headers)
            assert (refused.status_code, refused.json()["code"]) == (429, "too_many_listeners")
        finally:
            for r in streams:
                await r.aclose()
        await _until(lambda: live.gate.jobs[live.job.job_id].relay.listeners == 0)


@pytest.mark.timeout(60)
async def test_a_listener_reads_only_what_it_asked_for(live: Live) -> None:
    live.temporal.publish(LISTED)
    live.temporal.publish(PROGRESS_TOPIC)
    live.running()
    progress: list[m.StreamEvent] = []
    listed: list[m.StreamEvent] = []
    tasks = [
        _follow(live, progress, topics=[PROGRESS_TOPIC], history=True),
        _follow(live, listed, topics=[LISTED]),
    ]
    await _until(lambda: any(e.topic == PROGRESS_TOPIC for e in progress) and len(listed) == 1)
    live.completed()
    await asyncio.wait_for(asyncio.gather(*tasks), 20)
    assert {e.topic for e in progress if e.kind is m.StreamEventKind.ITEM} == {PROGRESS_TOPIC}
    assert any(e.kind is m.StreamEventKind.HISTORY for e in progress)
    assert [(e.kind, e.topic) for e in listed] == [(m.StreamEventKind.ITEM, LISTED)]


@pytest.mark.timeout(60)
async def test_relaying_never_changes_what_escrow_records(live: Live) -> None:
    """Seam ④: escrow reads no stream. The job's record is the same with or without the
    relay's items."""
    live.running()
    await live.gate.sweep()
    before = await live.gate.run(_record(live))
    live.temporal.publish(LISTED, 5)
    seen: list[m.StreamEvent] = []
    task = _follow(live, seen, topics=[LISTED])
    await _until(lambda: len(seen) == 5)
    assert await live.gate.run(_record(live)) == before
    live.completed()
    await asyncio.wait_for(task, 20)


def _record(live: Live) -> Callable[[], Any]:
    async def get() -> Any:
        return live.gate.jobs[live.job.job_id].record

    return get


async def test_streams_are_fixed_per_version(live: Live) -> None:
    entry = await live.buyer.gate.get_offering("acme/enrich")
    assert entry.streams == [LISTED]
    assert (await live.seller.gate.publish(_doc(), streams=[LISTED])).streams == [LISTED]
    with pytest.raises(GateError) as e:
        await live.seller.gate.publish(_doc(), streams=[LISTED, "more"])
    assert (e.value.status, e.value.code) == (409, "streams_fixed")
    with pytest.raises(ValueError, match="reserved"):  # the client refuses it first
        await live.seller.gate.publish(_doc(version="9.9.9"), streams=["kontra.progress"])
    token = await live.seller.access_token()
    async with httpx.AsyncClient(base_url=live.gate.base_url, timeout=10) as http:
        resp = await http.post(
            "/v1/catalog",
            json={"document": _doc(version="9.9.9"), "streams": ["kontra.progress"]},
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 422


@pytest.mark.timeout(60)
async def test_an_idle_stream_sends_keepalives_and_ends_when_the_gate_stops(live: Live) -> None:
    lines: list[str] = []
    token = await live.buyer.access_token()
    async with (
        httpx.AsyncClient(base_url=live.gate.base_url, timeout=10) as http,
        http.stream(
            "GET",
            f"/v1/jobs/{live.job.job_id}/stream",
            headers={"Authorization": f"Bearer {token}", "Accept": "text/event-stream"},
        ) as resp,
    ):
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        async for line in resp.aiter_lines():
            lines.append(line)
            if line.startswith(": keepalive"):
                break
    assert ": keepalive" in lines
    seen: list[m.StreamEvent] = []
    task = _follow(live, seen)
    await asyncio.sleep(0.3)
    assert not task.done()  # the job is open: the stream waits for more
    await live.gate.stop()
    await asyncio.wait_for(task, 10)  # a clean end, not a hang


async def test_run_reaches_gate_state_on_its_own_loop(live: Live) -> None:
    async def held() -> int:
        return (await live.gate.ledger.balance("bob", "USD")).held_minor

    assert await live.gate.run(held) == 500
    live.gate.advance(timedelta(hours=2))
    await live.gate.sweep()  # never started: refunded at the deadline, on the gate's loop
    assert await live.gate.run(held) == 0
