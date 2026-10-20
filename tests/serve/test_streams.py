"""Seam ⑤ for a job's workflow stream (decisions I31): what a seller publishes reaches its
buyer sealed, only on the topics the offering lists, and nothing of it -- no item, no
heartbeat, no unlisted topic's name -- is readable where the platform can read.

Two sellers on a dev server and the fake gate, each item marked with a string unique to its
place (``<marker>-<offering>-<topic>-<where>-<run>-<i>``):

* **W**, a workflow entry that hosts its own ``WorkflowStream`` (``prior_state``) and
  continues-as-new once, publishing ``events`` (listed) and ``private_audit`` (not) from the
  workflow and from an activity that heartbeats with markers;
* **H**, a bound handler (``streams: [turn_events]``) whose agent publishes a note on a topic
  of its own, ``private_notes``, and whose reply carries a marker.

Scanned: every run's history as Temporal stores it (publish signals, poll answers,
continue-as-new input), the raw SSE bytes the buyer receives, the gate's store; and a
misbehaving relay (asking for unlisted topics, or every topic) gets unlisted items it cannot
open and whose names it cannot read. The agent child's poll answers are sealed to the seller
alone."""

from __future__ import annotations

import asyncio
import base64
import json
import textwrap
import uuid
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.contrib.pydantic import PydanticPayloadConverter

from kontra import _relay
from kontra.cli._project import load_worker
from kontra.codec import ENCODING
from kontra.codec import stream as s
from kontra.codec.age import AgeError, decrypt
from kontra.contract.names import PROGRESS_TOPIC, agent_workflow_id, job_workflow_id
from kontra.gate_client import models as m
from kontra.serve import Worker
from kontra.testing import Marketplace, TestParty
from kontra.testing.devserver import DevServer

from . import seller
from .streamflows import AUDIT, LISTED, StreamOut, publish_both
from .support import Buyer, write_offering

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]

MARKER = f"MARKER-{uuid.uuid4().hex[:8]}"
CONVERTER = PydanticPayloadConverter()


@dataclass
class Job:
    mp: Marketplace
    seller: TestParty
    buyer: Buyer
    job_id: str

    @property
    def wf(self) -> str:
        return job_workflow_id(self.job_id)

    @property
    def plain(self) -> Client:
        """The seller's namespace on the dev server's client: no codec, as the platform
        reads Temporal."""
        return self.mp.dev_server.client_for(self.seller.namespace)

    async def stored(self) -> list[m.StreamEvent]:
        async def get() -> list[m.StreamEvent]:
            return list(self.mp.gate.jobs[self.job_id].events)

        return await self.mp.gate.run(get)

    async def relay_state(self) -> Any:
        async def get() -> Any:
            r = self.mp.gate.jobs[self.job_id].relay
            return (r.offset, r.batches, r.dropped, r.stopped, dict(r.topic_bytes), r.job_bytes)

        return await self.mp.gate.run(get)


async def raw_sse(party: TestParty, job_id: str) -> bytes:
    """``GET /v1/jobs/{id}/stream`` (every topic, history too) as the buyer, byte for byte,
    until the gate ends it."""
    token = await party.session.access_token()
    out = bytearray()
    async with (
        httpx.AsyncClient(base_url=party.marketplace.base_url, timeout=120) as http,
        http.stream(
            "GET",
            f"/v1/jobs/{job_id}/stream",
            params={"history": "true"},
            headers={"Authorization": f"Bearer {token}", "Accept": "text/event-stream"},
        ) as resp,
    ):
        assert resp.status_code == 200
        async for chunk in resp.aiter_raw():
            out += chunk
    return bytes(out)


def sse_items(raw: bytes) -> list[m.StreamEvent]:
    events: list[m.StreamEvent] = []
    for block in raw.decode().split("\n\n"):
        data = [line[6:] for line in block.split("\n") if line.startswith("data: ")]
        if data:
            events.append(m.StreamEvent.model_validate_json("\n".join(data)))
    return events


async def histories(plain: Client, workflow_id: str, runs: int = 1) -> list[str]:
    """Every run's history of ``workflow_id`` (at least ``runs`` of them: visibility lags
    a little), as Temporal stores it (no codec)."""

    async def listed() -> list[str]:
        return [r.run_id async for r in plain.list_workflows(f"WorkflowId = '{workflow_id}'")]

    async def enough() -> list[str] | None:
        found = await listed()
        return found if len(found) >= runs else None

    out: list[str] = []
    for run_id in await until(enough):
        history = await plain.get_workflow_handle(workflow_id, run_id=run_id).fetch_history()
        out.append(history.to_json())
    return out


async def until(probe: Any, within: float = 60.0) -> Any:
    loop = asyncio.get_running_loop()
    end = loop.time() + within
    while True:
        got = await probe()
        if got:
            return got
        if loop.time() > end:
            raise AssertionError("never got there")
        await asyncio.sleep(0.1)


#: The identity a misbehaving relay's polls carry here (so a scan can tell them apart).
MISBEHAVING = "misbehaving-relay"


def _without_misbehaving_polls(history_json: str) -> str:
    """A history's JSON without the polls a misbehaving relay sent (what the gate names in
    a poll is in history in the clear: the platform's own words, not the seller's)."""
    history = json.loads(history_json)
    history["events"] = [
        e
        for e in history.get("events", [])
        if e.get("workflowExecutionUpdateAcceptedEventAttributes", {})
        .get("acceptedRequest", {})
        .get("meta", {})
        .get("identity")
        != MISBEHAVING
    ]
    return json.dumps(history)


async def raw_poll(job: Job, topics: list[str]) -> s.Envelope:
    """One poll of the job's stream as a misbehaving relay would send it (any topics, from
    offset 0), through the strict shape: the answer's envelope."""
    req = _relay._request(job.seller.namespace, job.wf, "", topics, 0, MISBEHAVING)  # pyright: ignore[reportPrivateUsage]
    service = job.plain.workflow_service
    while True:
        resp = await service.update_workflow_execution(req, timeout=timedelta(seconds=20))
        if resp.HasField("outcome"):
            assert resp.outcome.HasField("success"), resp.outcome
            return s.parse_envelope(resp.outcome.success.payloads[0])


# --------------------------------------------------------------------------- W: a workflow entry


async def test_a_workflow_entrys_stream_leaks_nothing(
    dev_server: DevServer, tmp_path: Path
) -> None:
    path = write_offering(tmp_path, name="acme/streamer", description="Streams", streams=[LISTED])
    bound = seller.bind_stream(path)
    async with Marketplace(dev_server=dev_server, relay_interval=0.2) as mp:
        acme = await mp.party("acme")
        async with mp.serve(Worker(offerings=[bound], activities=[publish_both]), acme):
            await _w(mp, acme, bound)


async def _w(mp: Marketplace, acme: TestParty, bound: Any) -> None:
    bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="10.00")
    buyer = Buyer(mp, bob)
    entry = await bob.gate.get_offering(bound.key)
    assert entry.streams == [LISTED]
    value = {"marker": MARKER, "n": 2, "rounds": 2}
    hired = await buyer.hire(entry, value)
    handle = await buyer.start(hired, value, result_type=StreamOut)
    job = Job(mp, acme, buyer, hired.job_id)
    sse = asyncio.create_task(raw_sse(bob, hired.job_id))

    # Both runs published (the second waits for finish): 4 + 4 listed items, relayed.
    async def relayed() -> bool:
        return len([e for e in await job.stored() if e.topic == LISTED]) >= 8

    await until(relayed)

    # A misbehaving relay. Asking for the unlisted topic by name: every item comes back
    # unnamed, and the relay drops it.
    async for batch in _relay.relay(
        job.plain.workflow_service,
        namespace=acme.namespace,
        job_id=hired.job_id,
        topics=[AUDIT],
        start=0,
        closed=_always,
        min_interval=0,
        identity=MISBEHAVING,
    ):
        assert batch.items == ()
        assert batch.dropped == 8
        break
    # Asking for every topic: the unlisted items, unnamed; the buyer's key opens none of
    # them, the seller's opens all.
    env = await raw_poll(job, [])
    unlisted = [i for i in env.items if i.topic is None]
    assert len(unlisted) == 8
    assert {i.topic for i in env.items} == {None, LISTED, PROGRESS_TOPIC}
    for item in unlisted:
        assert item.ciphertext is not None
        with pytest.raises(AgeError):
            decrypt(item.ciphertext, bob.identity)
        sealed = s.open_item(item.ciphertext, [acme.identity], topic=AUDIT, offset=item.offset)
        assert AUDIT in json.dumps(CONVERTER.from_payload(sealed))

    await mp.client(acme.namespace).get_workflow_handle(job.wf).signal("finish")
    assert (await handle.result()).published == 4
    raw = await asyncio.wait_for(sse, 120)

    # Every run's history, as Temporal stores it: no marker, no unlisted topic's name --
    # but in the misbehaving relay's own poll, which named it.
    runs = await histories(job.plain, job.wf, runs=2)
    assert len(runs) == 2  # it continued-as-new
    for text in runs:
        assert MARKER not in text
        assert AUDIT not in _without_misbehaving_polls(text)
    # The SSE bytes and the gate's store: no marker, no unlisted name, ciphertext only.
    assert MARKER.encode() not in raw
    assert AUDIT.encode() not in raw
    stored = json.dumps([e.model_dump(mode="json") for e in await job.stored()])
    state = json.dumps(str(await job.relay_state()))
    for text in (stored, state):
        assert MARKER not in text
        assert AUDIT not in text
    # Each relayed item opens with the job's key (here the buyer's own) and is what the
    # gate says it is: its topic and offset are sealed inside.
    events = sse_items(raw)
    items = [e for e in events if e.kind is m.StreamEventKind.ITEM]
    assert {e.topic for e in items} == {LISTED, PROGRESS_TOPIC}
    seen = []
    for e in items:
        assert e.ciphertext_b64 is not None
        assert e.topic is not None
        assert e.offset is not None
        p = s.open_item(base64.b64decode(e.ciphertext_b64), [bob.identity], topic=e.topic, offset=e.offset)  # fmt: skip
        seen.append(json.dumps(CONVERTER.from_payload(p)))
    assert sum(f"{MARKER}-w-{LISTED}-" in v for v in seen) == 8
    assert any(f"{MARKER}-w-progress-heartbeat-" in v for v in seen)
    assert [e.offset for e in items] == sorted(e.offset or 0 for e in items)


async def _always() -> bool:
    return True


# --------------------------------------------------------------------------- H: a bound handler

_H_WORKER = """
from temporal_agent_harness.plugin import AgentHarnessPlugin

from kontra import serve
from tests.serve.harness_agents import StreamyAgent, quote

serve(StreamyAgent.sweep, quote=quote, plugins=[AgentHarnessPlugin()])
"""


async def test_a_bound_handlers_stream_leaks_nothing(dev_server: DevServer, tmp_path: Path) -> None:
    pytest.importorskip("temporal_agent_harness", reason="the dev group's temporal-agent-harness")
    from .harness_agents import NOTES_TOPIC, Report

    root = tmp_path / "h"
    write_offering(
        root,
        name="acme/recon",
        description="Sweeps a host, one turn of an agent",
        streams=["turn_events"],
    ).rename(root / "offering.yaml")
    (root / "worker.py").write_text(textwrap.dedent(_H_WORKER).lstrip())
    with load_worker(root) as worker:
        [bound] = worker.offerings
        async with Marketplace(dev_server=dev_server, relay_interval=0.2) as mp:
            acme = await mp.party("acme")
            bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="10.00")
            buyer_key, seller_key = bob.identity, acme.identity
            async with mp.serve(worker, acme):
                buyer = Buyer(mp, bob)
                entry = await bob.gate.get_offering(bound.key)
                host = f"{MARKER}-h-reply.example"
                hired = await buyer.hire(entry, {"host": host})
                handle = await buyer.start(hired, {"host": host}, result_type=Report)
                job = Job(mp, acme, buyer, hired.job_id)
                sse = asyncio.create_task(raw_sse(bob, hired.job_id))

                async def mirrored() -> bool:
                    return any(e.topic == "turn_events" for e in await job.stored())

                await until(mirrored)
                # Every topic of the job's own log, as a misbehaving relay would ask: the
                # agent's unlisted topic was never mirrored there.
                env = await raw_poll(job, [])
                assert env.items
                assert {i.topic for i in env.items} <= {"turn_events", PROGRESS_TOPIC}
                out = await handle.result()
                raw = await asyncio.wait_for(sse, 120)
                stored = json.dumps([e.model_dump(mode="json") for e in await job.stored()])
                child = agent_workflow_id(hired.job_id)
                child_runs = await histories(job.plain, child)
                wrapper_runs = await histories(job.plain, job.wf)
    assert out.host == host
    for text in (*child_runs, *wrapper_runs, stored, raw.decode()):
        assert MARKER not in text
        assert NOTES_TOPIC not in text
    # The buyer reads the agent's events, the reply among them, from the job's stream.
    items = [e for e in sse_items(raw) if e.kind is m.StreamEventKind.ITEM]
    replies = []
    for e in items:
        assert e.topic == "turn_events"
        p = s.open_item(base64.b64decode(e.ciphertext_b64 or ""), [buyer_key], topic=e.topic, offset=e.offset or 0)  # fmt: skip
        event = json.loads(p.data)
        if event["event"]["type"] == "message_handler_end":
            replies.append(event["event"]["output"])
    assert replies == [{"host": host, "open": len(host)}]
    # The agent child is the seller's alone: its poll answers do not open with the buyer's
    # key (and are whole ciphertext, never an envelope).
    outcomes = [
        payload
        for text in child_runs
        for payload in _update_results(json.loads(text))
    ]  # fmt: skip
    assert outcomes
    for p in outcomes:
        assert p.metadata["encoding"] == ENCODING
        with pytest.raises(AgeError):
            decrypt(p.data, buyer_key)
        decrypt(p.data, seller_key)


def _update_results(history: dict[str, Any]) -> list[Payload]:
    out: list[Payload] = []
    for e in history.get("events", []):
        done = e.get("workflowExecutionUpdateCompletedEventAttributes")
        if not done:
            continue
        for p in done.get("outcome", {}).get("success", {}).get("payloads", []):
            out.append(
                Payload(
                    metadata={k: base64.b64decode(v) for k, v in p["metadata"].items()},
                    data=base64.b64decode(p.get("data", "")),
                )
            )
    return out
