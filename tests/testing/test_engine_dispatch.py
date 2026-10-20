"""The fake gate dispatches every job op through seam ⑥ (decisions I32): to the engine each
offering version was published on, recorded at its first publish and fixed. On
:class:`~kontra.testing.FakeEngine` a whole lifecycle runs with no Temporal at all -- hire,
deliver, accept, paid; a cancel before any run; a deadline refund; the seller's own run --
and two engines get exactly their own versions' jobs."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from temporalio.api.common.v1 import Payload

from kontra.auth import Session
from kontra.engine import JobHandle
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.testing import FakeEngine, fake_gate
from kontra.testing.engine import RunCanceled, RunContext
from kontra.testing.fake_gate import FakeGate, GateHTTPError, Principal

from .support import json_payload, logged_in, offering_doc, sealed, wire

_A_BUYER_KEY = "age1e0q4lyexsefmh6hurumvgl3cmmsfnz4zyfhkd7lfrjcpqc2e250qs6j4el"
QUOTE = b'{"status":"ok","amount_minor":1000,"currency":"USD","max_duration_seconds":3600}'


async def program(ctx: RunContext, raw: bytes) -> bytes:
    """The seller's code: ``ok`` delivers, ``hang`` runs until stopped."""
    mode = json.loads(Payload.FromString(raw).data).get("mode", "ok")
    if mode == "hang":
        await ctx.cancelled()
        raise RunCanceled
    ctx.step("upper")
    return sealed(b"DONE")


async def quote(raw: bytes) -> bytes:
    return QUOTE


@dataclass
class Bench:
    gate: FakeGate
    engines: dict[str, FakeEngine]
    transport: httpx.ASGITransport
    root: Path
    sessions: list[Session] = field(default_factory=list[Session])

    async def party(self, name: str) -> Session:
        s = await logged_in(self.gate.base_url, self.root / name, name, self.transport)
        self.sessions.append(s)
        return s

    async def publish(
        self, seller: Session, *, engine: str = "temporal", **facts: Any
    ) -> m.CatalogEntry:
        doc = offering_doc(seller.profile.party, **facts)
        entry = await self.gate.publish(_who(self.gate, seller.profile.party), doc, (), engine)
        oid = self.gate.offerings[entry.offering].id
        self.engines[engine].serve(oid, program)
        self.engines[engine].serve_quote(oid, quote)
        return entry

    async def hire(self, buyer: Session, entry: m.CatalogEntry) -> m.HireView:
        q = await buyer.gate.quote(entry.offering, wire(json_payload({"text": "hi"})))
        assert q.quote_id is not None, q
        return await buyer.gate.hire(q.quote_id, buyer_key=_A_BUYER_KEY)

    def handle(self, job_id: str) -> JobHandle:
        h = self.gate.jobs[job_id].handle
        assert h is not None
        return h


def _who(gate: FakeGate, party: str) -> Principal:
    """The party as the gate's methods take it (a route would check its confirmation)."""
    sid = next(sid for sid, s in gate.sessions.items() if s.party == party)
    return Principal(party=party, session_id=sid, auth_time=gate.now(), kind="api")


@pytest.fixture
async def bench(tmp_path: Path) -> AsyncIterator[Bench]:
    gate: FakeGate | None = None

    def clock() -> Any:
        assert gate is not None
        return gate.now()

    engines = {"temporal": FakeEngine(clock=clock), "other": FakeEngine(clock=clock)}
    gate = fake_gate(engines=engines, sweep_interval=0, grace=timedelta(seconds=1))
    async with gate:
        b = Bench(gate, engines, httpx.ASGITransport(app=gate.app), tmp_path)
        try:
            yield b
        finally:
            for s in b.sessions:
                await s.aclose()


async def _settle(gate: FakeGate) -> None:
    for _ in range(5):
        await asyncio.sleep(0.02)
        await gate.sweep()


async def test_a_whole_lifecycle_runs_on_the_fake_engine(bench: Bench) -> None:
    seller, buyer = await bench.party("acme"), await bench.party("bob")
    engine = bench.engines["temporal"]
    assert {"acme", "bob"} <= engine.homes  # every party has a home in every engine
    entry = await bench.publish(seller)
    assert engine.routes == {entry.endpoint: entry.offering}  # exposed under its route
    hired = await bench.hire(buyer, entry)
    handle = bench.handle(hired.job_id)
    assert handle.startswith(b"fake/1\n")  # the engine's own handle, bound at hire
    await buyer.gate.start(hired.job_id, wire(json_payload({"text": "hi"})))
    await _settle(bench.gate)
    view = await buyer.gate.job(hired.job_id)
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.HELD)
    result = await buyer.gate.result(hired.job_id)
    assert base64.b64decode(result.payload.b64) == sealed(b"DONE")
    assert (await buyer.gate.accept(hired.job_id)).escrow is m.Escrow.RELEASED
    assert bench.gate.jobs[hired.job_id].record.pin is not None
    assert engine.runs(handle) == 1
    events = [e async for e in buyer.gate.stream(hired.job_id, history=True)]
    assert events[-1].state is m.JobState.COMPLETED  # liveness, mapped onto the wire
    assert events[-1].activities_completed == 1


async def test_a_cancel_before_any_run_fences_the_job(bench: Bench) -> None:
    seller, buyer = await bench.party("acme"), await bench.party("bob")
    entry = await bench.publish(seller)
    hired = await bench.hire(buyer, entry)
    view = await buyer.gate.cancel(hired.job_id)
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    engine, handle = bench.engines["temporal"], bench.handle(hired.job_id)
    # The hold is still valid, but the fenced job can begin no lineage.
    hold = bench.gate.hold_token(bench.gate.jobs[hired.job_id])
    deadline = bench.gate.jobs[hired.job_id].record.deadline
    await engine.start(handle, json_payload({}).SerializeToString(), hold=hold, deadline=deadline)
    assert engine.runs(handle) == 0


async def test_an_open_job_refunds_at_its_deadline_and_its_run_is_ended(bench: Bench) -> None:
    seller, buyer = await bench.party("acme"), await bench.party("bob")
    entry = await bench.publish(seller)
    hired = await bench.hire(buyer, entry)
    await buyer.gate.start(hired.job_id, wire(json_payload({"mode": "hang"})))
    await _settle(bench.gate)
    assert (await buyer.gate.job(hired.job_id)).state is m.JobState.WORKING
    bench.gate.advance(timedelta(hours=1, seconds=5))
    await _settle(bench.gate)
    view = await buyer.gate.job(hired.job_id)
    assert (view.state, view.escrow, view.reason) == (
        m.JobState.FAILED,
        m.Escrow.REFUNDED,
        "max_duration exceeded",
    )
    engine = bench.engines["temporal"]
    seen = await engine.observe(bench.handle(hired.job_id), pin=None, holds=())
    assert seen.observation.status.value == "terminated"  # END, from STOPS


async def test_the_sellers_own_run_never_delivers(bench: Bench) -> None:
    seller, buyer = await bench.party("acme"), await bench.party("bob")
    entry = await bench.publish(seller)
    hired = await bench.hire(buyer, entry)
    assert bench.engines["temporal"].seller_start(bench.handle(hired.job_id), sealed(b"mine"))
    await _settle(bench.gate)
    view = await buyer.gate.job(hired.job_id)
    assert (view.state, view.delivery) == (m.JobState.SUBMITTED, None)
    bench.gate.advance(timedelta(hours=1, seconds=5))
    await _settle(bench.gate)
    assert (await buyer.gate.job(hired.job_id)).escrow is m.Escrow.REFUNDED


async def test_each_job_goes_to_its_versions_engine(bench: Bench) -> None:
    """Risk 11 of #55's design: one engine standing in under one name could hide a dispatch
    bug, so two engine objects, under ``temporal`` and a name reached at method level."""
    seller, buyer = await bench.party("acme"), await bench.party("bob")
    on_temporal = await bench.publish(seller, version="1.0.0")
    on_other = await bench.publish(seller, engine="other", version="2.0.0")
    assert bench.gate.offerings[on_other.offering].engine == "other"
    a = await bench.hire(buyer, on_temporal)
    b = await bench.hire(buyer, on_other)
    for job in (a, b):
        await buyer.gate.start(job.job_id, wire(json_payload({"text": "hi"})))
    await _settle(bench.gate)
    temporal, other = bench.engines["temporal"], bench.engines["other"]
    assert (temporal.runs(bench.handle(a.job_id)), other.runs(bench.handle(a.job_id))) == (1, 0)
    assert (temporal.runs(bench.handle(b.job_id)), other.runs(bench.handle(b.job_id))) == (0, 1)
    for job in (a, b):
        assert (await buyer.gate.job(job.job_id)).state is m.JobState.COMPLETED


async def test_the_engine_is_fixed_per_version(bench: Bench) -> None:
    seller = await bench.party("acme")
    entry = await bench.publish(seller)
    who = _who(bench.gate, "acme")
    doc = offering_doc("acme")
    again = await bench.gate.publish(who, doc, (), "temporal")  # the same engine: idempotent
    assert again.offering == entry.offering
    with pytest.raises(GateHTTPError) as e:
        await bench.gate.publish(who, doc, (), "other")
    assert (e.value.status, e.value.code, e.value.detail) == (
        409,
        "engine_fixed",
        {"engine": "temporal"},
    )
    with pytest.raises(GateHTTPError) as e:
        await bench.gate.publish(who, offering_doc("acme", version="3.0.0"), (), "nope")
    assert (e.value.status, e.value.code) == (422, "unsupported_engine")


async def test_a_publish_naming_no_engine_runs_on_temporal(bench: Bench) -> None:
    """Owner decision OD11: an SDK omits the default engine from the request; the gate reads
    its absence as ``temporal`` -- as every client before #55 sends it."""
    seller = await bench.party("acme")
    entry = await seller.gate.publish(offering_doc("acme"))
    assert bench.gate.offerings[entry.offering].engine == "temporal"
    assert "engine" not in entry.model_dump()  # never in what anyone reads back


def _keys(value: Any) -> set[str]:
    """Every object key anywhere in a JSON value."""
    if isinstance(value, dict):
        return {str(k) for k in value} | {k for v in value.values() for k in _keys(v)}
    if isinstance(value, list):
        return {k for v in value for k in _keys(v)}
    return set()


async def test_no_catalog_read_carries_the_engine(bench: Bench) -> None:
    """Decisions I32: the engine is never in what a buyer reads. The model walk in
    ``tests/gate_client/test_routes.py`` covers every modelled response; these are the served
    bytes of every catalog read, the A2A agent card (a route with no response model) among
    them, for a version published on an engine other than the default."""
    seller = await bench.party("acme")
    entry = await bench.publish(seller, engine="other")
    party, name = entry.offering.split("@")[0].split("/")
    version = entry.version
    async with httpx.AsyncClient(transport=bench.transport, base_url=bench.gate.base_url) as c:
        reads = {
            "search": await c.get("/v1/catalog"),
            "get_offering": await c.get(f"/v1/catalog/{party}/{name}/{version}"),
            "card": await c.get(f"/v1/catalog/{party}/{name}/{version}/card"),
            "by_endpoint": await c.get(f"/v1/endpoints/{entry.endpoint}"),
        }
    for route, answer in reads.items():
        assert answer.status_code == 200, (route, answer.text)
        assert "engine" not in _keys(answer.json()), route
    assert reads["card"].json()  # the card is served, and is the document's alone


async def test_unsupported_engine_is_a_name_this_gate_neither_knows_nor_runs() -> None:
    """``PublishRequest.engine`` (decisions I32): a name that is not an engine and that the
    gate runs no adapter for is 422 ``unsupported_engine``; a known engine a gate has no
    adapter for (a gate with no Temporal) is recorded, and its sellers are offline."""
    async with fake_gate(control=None) as gate:
        assert gate.engines == {}
        await gate.ensure_party("acme")
        who = Principal(party="acme", session_id="s", auth_time=gate.now(), kind="api")
        entry = await gate.publish(who, offering_doc("acme"), (), "temporal")
        assert gate.offerings[entry.offering].engine == "temporal"
        with pytest.raises(GateHTTPError) as e:
            await gate.publish(who, offering_doc("acme", version="2.0.0"), (), "golem")
        assert (e.value.status, e.value.code) == (422, "unsupported_engine")


async def test_the_workers_view_is_temporals_own(bench: Bench) -> None:
    """Owner decision OD8: the Workers view is a Temporal-only extension of its adapter; a
    version on any other adapter has none."""
    seller = await bench.party("acme")
    entry = await bench.publish(seller)
    with pytest.raises(GateError) as e:
        await seller.gate.workers(entry.offering)
    assert (e.value.status, e.value.code) == (404, "workers_unavailable")


async def test_the_temporal_token_is_the_engines_credential(bench: Bench) -> None:
    seller = await bench.party("acme")
    token = await seller.gate.temporal_token()
    engine = bench.engines["temporal"]
    assert engine.credentials[token.token] == "acme"
    assert (token.namespace, token.temporal_address) == ("acme", engine.address)


async def test_an_unreachable_engine_is_never_a_fact(bench: Bench) -> None:
    seller, buyer = await bench.party("acme"), await bench.party("bob")
    entry = await bench.publish(seller)
    hired = await bench.hire(buyer, entry)
    bench.engines["temporal"].unreachable = True
    with pytest.raises(GateError) as e:
        await buyer.gate.start(hired.job_id, wire(json_payload({})))
    assert (e.value.status, e.value.code) == (503, "temporal_unavailable")
    with pytest.raises(GateError) as e:
        await buyer.gate.cancel(hired.job_id)  # cannot look first: refused, not guessed
    assert e.value.status == 503
    await bench.gate.sweep()
    assert (await buyer.gate.job(hired.job_id)).state is m.JobState.SUBMITTED
