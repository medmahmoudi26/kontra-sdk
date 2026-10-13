"""Progress on the fake gate (§8.4), over a served gate so the SSE stream is real: history
metadata from the sweeper and the seller's encrypted heartbeats, merged in order, replayed
after a reconnect, and the stream ending once the job is closed and drained."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import httpx
import pytest

from kontra._control import HistorySummary
from kontra.auth import Session
from kontra.codec.age import decrypt, encrypt
from kontra.escrow import ObservedStatus
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.testing.fake_gate import FakeGate

from .support import ScriptedControl, json_payload, logged_in, offering_doc, ok_quote, wire


@dataclass
class Live:
    gate: FakeGate
    control: ScriptedControl
    seller: Session
    buyer: Session
    worker: GateClient
    job: m.HireView

    @property
    def wf(self) -> str:
        return f"job-{self.job.job_id}"

    async def beat(self, details: dict[str, object], activity: str = "fetch_batch") -> None:
        sealed = encrypt(json.dumps(details).encode(), [self.buyer.profile.public_key or ""])
        await self.worker.post_progress(
            self.job.job_id,
            m.ProgressPost(
                at=self.gate.now(),
                activity=activity,
                ciphertext_b64=base64.b64encode(sealed).decode(),
            ),
        )


@pytest.fixture
async def live(tmp_path: Path) -> AsyncIterator[Live]:
    control = ScriptedControl(responder=lambda p, h: ok_quote(500))
    gate = FakeGate(control, sweep_interval=0.05, keepalive=0.2)
    base = await gate.serve()
    seller = await logged_in(base, tmp_path / "acme", "acme")
    buyer = await logged_in(base, tmp_path / "bob", "bob")
    await seller.gate.publish(offering_doc("acme", seller.profile.public_key or ""))
    q = await buyer.gate.quote("acme/enrich", wire(json_payload({"text": "x"})))
    assert q.quote_id is not None
    job = await buyer.gate.hire(q.quote_id)
    token = (await seller.gate.temporal_token()).token

    async def bearer() -> str:
        return token

    worker = GateClient(base, token=bearer)
    try:
        yield Live(gate, control, seller, buyer, worker, job)
    finally:
        await worker.aclose()
        await seller.aclose()
        await buyer.aclose()
        await gate.stop()


async def _until(predicate: Callable[[], bool], within: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + within
    while not predicate():
        if loop.time() > end:
            raise AssertionError("condition never held")
        await asyncio.sleep(0.02)


@pytest.mark.timeout(60)
async def test_history_and_heartbeats_stream_in_order_until_the_job_closes(live: Live) -> None:
    received: list[m.ProgressEvent] = []

    async def follow() -> None:
        async for event in live.buyer.gate.progress(live.job.job_id):
            received.append(event)

    task = asyncio.create_task(follow())
    live.control.observe_as("acme", live.wf, ObservedStatus.RUNNING)
    live.control.summaries[("acme", live.wf)] = HistorySummary(
        state=ObservedStatus.RUNNING,
        last_event="ActivityTaskScheduled",
        activities_running=("fetch_batch",),
    )
    await _until(lambda: len(received) == 1)
    for pct in (10, 50):
        await live.beat({"percent": pct, "message": f"{pct}%"})
    await _until(lambda: len(received) == 3)
    live.control.summaries[("acme", live.wf)] = HistorySummary(
        state=ObservedStatus.COMPLETED,
        last_event="WorkflowExecutionCompleted",
        activities_completed=2,
    )
    live.control.observe_as("acme", live.wf, ObservedStatus.COMPLETED, b"result")
    await asyncio.wait_for(task, 10)  # the stream ends by itself once the job is closed

    assert [e.seq for e in received] == [1, 2, 3, 4]
    first, beat1, beat2, last = received
    assert (first.source, first.state, first.activities_running) == (
        m.ProgressSource.HISTORY,
        m.JobState.WORKING,
        ["fetch_batch"],
    )
    assert (beat1.source, beat1.activity) == (m.ProgressSource.HEARTBEAT, "fetch_batch")
    secret = live.buyer.identity()
    shown = [
        json.loads(decrypt(base64.b64decode(b.ciphertext_b64 or ""), secret))
        for b in (beat1, beat2)
    ]
    assert shown == [{"percent": 10, "message": "10%"}, {"percent": 50, "message": "50%"}]
    assert (last.state, last.last_event, last.activities_completed) == (
        m.JobState.COMPLETED,
        "WorkflowExecutionCompleted",
        2,
    )
    # a reconnect after seq 2 replays the rest, and a closed job's stream ends at once
    assert [e.seq async for e in live.buyer.gate.progress(live.job.job_id, after=2)] == [3, 4]
    assert [e.seq async for e in live.seller.gate.progress(live.job.job_id)] == [1, 2, 3, 4]
    # nothing in the gate's events is the heartbeat's plaintext
    assert b"percent" not in json.dumps([e.model_dump(mode="json") for e in received]).encode()


@pytest.mark.timeout(60)
async def test_an_idle_stream_sends_keepalives_and_ends_when_the_gate_stops(live: Live) -> None:
    lines: list[str] = []
    token = await live.buyer.access_token()
    async with (
        httpx.AsyncClient(base_url=live.gate.base_url, timeout=10) as http,
        http.stream(
            "GET",
            f"/v1/jobs/{live.job.job_id}/progress",
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


async def test_heartbeat_ingest_rules(live: Live) -> None:
    for i in range(12):  # a burst: at most five per second are kept
        await live.beat({"percent": i})
    assert len(live.gate.jobs[live.job.job_id].events) == 5
    await asyncio.sleep(1.05)
    await live.beat({"percent": 99})
    assert len(live.gate.jobs[live.job.job_id].events) == 6

    async def post(client: GateClient, b64: str, activity: str = "a") -> GateError:
        with pytest.raises(GateError) as e:
            await client.post_progress(
                live.job.job_id,
                m.ProgressPost(at=live.gate.now(), activity=activity, ciphertext_b64=b64),
            )
        return e.value

    big = base64.b64encode(b"x" * (64 * 1024 + 1)).decode()
    assert (await post(live.worker, big)).code == "too_large"
    assert (await post(live.worker, "not base64!")).code == "invalid_progress"
    assert (await post(live.worker, "AAAA", activity="a" * 256)).code == "invalid_progress"
    err = await post(live.buyer.gate, "AAAA")
    assert (err.status, err.code) == (403, "forbidden")
    live.control.observe_as("acme", f"job-{live.job.job_id}", ObservedStatus.FAILED)
    await _until(lambda: not live.gate.jobs[live.job.job_id].record.open)
    err = await post(live.worker, "AAAA")
    assert (err.status, err.code) == (409, "not_open")


async def test_a_stranger_cannot_follow_a_job(live: Live, tmp_path: Path) -> None:
    eve = await logged_in(live.gate.base_url, tmp_path / "eve", "eve")
    try:
        with pytest.raises(GateError) as e:
            async for _ in eve.gate.progress(live.job.job_id):
                pass
        assert (e.value.status, e.value.code) == (404, "job_not_found")
    finally:
        await eve.aclose()


async def test_run_reaches_gate_state_on_its_own_loop(live: Live) -> None:
    async def held() -> int:
        return (await live.gate.ledger.balance("bob", "USD")).held_minor

    assert await live.gate.run(held) == 500
    live.gate.advance(timedelta(hours=2))
    await live.gate.sweep()  # never started: refunded at the deadline, on the gate's loop
    assert await live.gate.run(held) == 0


@pytest.mark.timeout(60)
async def test_an_open_stream_ends_when_the_gate_stops(live: Live) -> None:
    seen: list[m.ProgressEvent] = []

    async def follow() -> None:
        async for event in live.buyer.gate.progress(live.job.job_id):
            seen.append(event)

    await live.beat({"percent": 1})
    task = asyncio.create_task(follow())
    await _until(lambda: len(seen) == 1)
    assert not task.done()  # the job is open: the stream waits for more
    await live.gate.stop()
    await asyncio.wait_for(task, 10)  # a clean end, not a hang


async def test_only_ciphertext_progress_is_kept(live: Live) -> None:
    """As on the platform gate: the body must be an age file, so a client that forgot to
    encrypt fails here instead of leaving the seller's numbers readable to Kontra."""

    async def post(b64: str, activity: str = "fetch") -> GateError:
        with pytest.raises(GateError) as e:
            await live.worker.post_progress(
                live.job.job_id,
                m.ProgressPost(at=live.gate.now(), activity=activity, ciphertext_b64=b64),
            )
        return e.value

    plain = base64.b64encode(json.dumps({"percent": 50}).encode()).decode()
    err = await post(plain)
    assert (err.status, err.code) == (422, "not_ciphertext")
    sealed = encrypt(b"{}", [live.buyer.profile.public_key or ""])
    b64 = base64.b64encode(sealed).decode()
    assert (await post(b64, activity="fetch\nInjected: 1")).code == "invalid_progress"
    assert live.gate.jobs[live.job.job_id].events == []
    naive = live.gate.now().replace(tzinfo=None)
    await live.worker.post_progress(
        live.job.job_id, m.ProgressPost(at=naive, activity="fetch", ciphertext_b64=b64)
    )
    [event] = live.gate.jobs[live.job.job_id].events
    assert event.at.tzinfo is not None
