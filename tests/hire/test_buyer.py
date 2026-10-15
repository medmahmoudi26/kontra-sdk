"""The script door's refusals and the rest of its surface: max_price, rejected and busy
quotes, contract validation before encryption, a seller that goes offline (acceptance 4),
a start the handler refuses, a run the seller started itself (never a delivery), progress
(acceptance 13), artifacts in and out, re-attaching to a job, the profile's own connection,
the gate standing in for Temporal, and every way a start can fail after the hold is locked
(the job is cancelled at the gate, or named so it can be recovered)."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from kontra.codec.age import AgeError, decrypt
from kontra.contract.names import job_workflow_id
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.hire import (
    HireRefused,
    InvalidDelivery,
    InvalidInput,
    JobFailed,
    PriceAboveMax,
    Progress,
    QuoteRejected,
    Record,
    SellerBusy,
    TemporalUnavailable,
)
from kontra.profile import Profile
from kontra.testing.devserver import DevServer
from kontra.testing.fake_gate import AGE_MAGIC
from kontra.types import Artifact

from . import sellers
from .models import TextOut
from .support import Buyer, World, open_world, until

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]

START = 10_000


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def world(
    dev_server: DevServer, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[World]:
    async with open_world(dev_server, tmp_path_factory.mktemp("acme")) as w:
        yield w


# --------------------------------------------------------------------------- refusals


async def test_max_price_refuses_to_lock(world: World) -> None:
    buyer = await world.buyer()
    with pytest.raises(PriceAboveMax) as above:
        await buyer.k.hire(world.key(), text="hello world", max_price="0.10")
    assert (above.value.price, above.value.max_price) == (Decimal("0.11"), Decimal("0.10"))
    assert above.value.currency == "USD"
    q = await buyer.k.quote(world.key(), text="hello world")
    with pytest.raises(PriceAboveMax):
        await q.hire(max_price=0.109)  # a cap is rounded down: 0.10 < 0.11
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START, 0)
    assert await buyer.k.jobs() == []  # nothing was hired


async def test_a_rejected_quote(world: World) -> None:
    buyer = await world.buyer()
    q = await buyer.k.quote(world.key(), text="reject me")
    assert (q.status, q.ok, q.reason) == (
        m.QuoteStatus.REJECTED,
        False,
        "this text is not for sale",
    )
    assert (q.quote_id, q.price) == (None, None)
    with pytest.raises(QuoteRejected) as rejected:
        await q.hire()
    assert rejected.value.reason == "this text is not for sale"
    with pytest.raises(QuoteRejected):
        await buyer.k.hire(world.key(), text="reject me")
    assert (await buyer.balance()).held_minor == 0


async def test_busy_from_the_seller_and_from_the_concurrency_limit(world: World) -> None:
    buyer = await world.buyer()
    q = await buyer.k.quote(world.key(), text="busy")
    assert (q.status, q.eta) == (m.QuoteStatus.BUSY, timedelta(seconds=30))
    with pytest.raises(SellerBusy) as busy:
        await q.hire()
    assert busy.value.eta == timedelta(seconds=30)

    first = await buyer.k.hire(world.key("limited"), text="hog", mode="slow")
    second = await buyer.k.quote(world.key("limited"), text="me too")
    assert second.status is m.QuoteStatus.BUSY  # the gate's max_concurrent_jobs (I11)
    assert second.eta is not None
    with pytest.raises(SellerBusy):
        await second.hire()
    await first.cancel()
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START, 0)


async def test_the_input_is_validated_before_it_is_encrypted(world: World) -> None:
    buyer = await world.buyer()
    with pytest.raises(InvalidInput) as extra:
        await buyer.k.quote(world.key(), text="x", sneaky=1)
    assert extra.value.offering == world.key()
    with pytest.raises(InvalidInput) as long:
        await buyer.k.quote(world.key(), {"text": "x" * 2001})
    assert long.value.path == "input.text"
    with pytest.raises(TypeError):
        await buyer.k.quote(world.key(), {"text": "x"}, mode="ok")  # one object or keywords

    party = buyer.party.handle

    async def quotes_relayed() -> object:
        return await world.gate(lambda g: g._quote_rate.get(party))

    assert await quotes_relayed() is None  # nothing reached the gate's relay


# --------------------------------------------------------------------------- deadlines


async def test_an_offline_seller_refunds_by_the_deadline(world: World, tmp_path: Path) -> None:
    """Acceptance 4, with a 4 s max_duration: the seller quotes, then goes offline."""
    buyer = await world.buyer(start_timeout=0.5)
    bound = sellers.text_offering(tmp_path, max_duration="4s")
    async with world.mp.serve(sellers.text_worker(bound), world.seller):
        q = await buyer.k.quote(bound.key, text="hello")
    loop = asyncio.get_running_loop()
    hired_at = loop.time()
    job = await q.hire()
    with pytest.raises(JobFailed) as failed:
        await job.result()
    assert failed.value.state == "failed"
    assert "max_duration" in failed.value.reason
    view = await world.job(buyer, job.job_id, lambda v: v.escrow is m.Escrow.REFUNDED)
    refunded_after = loop.time() - hired_at
    assert view.state is m.JobState.FAILED
    assert view.reason == "seller never started the job"
    assert refunded_after < 4 + 60  # max_duration + 1 minute, with no human action
    assert refunded_after < 4 + 10  # in practice: max_duration + the gate's grace + a sweep
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START, 0)


async def test_a_start_the_handler_refuses_refunds_at_once(world: World) -> None:
    """The seller's re-quote (I10) prices above the hold: the handler refuses the start,
    and the hire is cancelled at the gate rather than left to the deadline."""
    buyer = await world.buyer()
    late = await world.buyer(start_timeout=0)  # sees the refusal in result() instead
    q = await buyer.k.quote(world.key(), text="pricey")
    q_late = await late.k.quote(world.key(), text="pricey")
    sellers.SURCHARGE["cents"] = 50
    try:
        with pytest.raises(JobFailed) as refused:
            await q.hire()
        job = await q_late.hire()
        with pytest.raises(JobFailed) as refused_late:
            await job.result()
    finally:
        sellers.SURCHARGE["cents"] = 0
    for err, who in ((refused.value, buyer), (refused_late.value, late)):
        assert "the seller refused to start the job" in err.reason
        assert "prices above the held" in err.reason
        assert err.state == "canceled"
        assert err.job_id is not None
        view = await who.k.gate.job(err.job_id)  # no waiting: refunded by the time we know
        assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
        bal = await who.balance()
        assert (bal.available_minor, bal.held_minor) == (START, 0)


# --------------------------------------------------------------------------- the escrow decides


async def test_a_run_the_seller_started_itself_refunds_the_buyer_and_pays_nobody(
    world: World,
) -> None:
    """The seller starts ``job-<id>`` itself and completes it, so the buyer's own start is
    refused ("already ran"). That run is no delivery: only the run the buyer's own call
    started is (decisions I15). The refused start is cancelled at the gate at once: the
    buyer is refunded, the seller is paid nothing, and there is no delivery to read."""
    buyer = await world.buyer()
    seller_before = await world.ledger_balance(world.seller.handle)
    world.forge_on_hire(buyer.k, "not what you asked for")
    with pytest.raises(JobFailed) as refused:
        await buyer.k.hire(world.key(), text="genuine")
    err = refused.value
    assert "the seller refused to start the job" in err.reason
    assert "already ran" in err.reason
    assert err.state == "canceled"
    assert err.job_id is not None
    view = await buyer.k.gate.job(err.job_id)  # no waiting: refunded by the time we know
    assert (view.state, view.escrow, view.delivery) == (
        m.JobState.CANCELED,
        m.Escrow.REFUNDED,
        None,
    )
    with pytest.raises(GateError) as nothing:
        await buyer.k.gate.result(err.job_id)
    assert nothing.value.code == "not_delivered"
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START, 0)
    assert await world.ledger_balance(world.seller.handle) == seller_before


async def test_a_start_whose_answer_was_lost_never_hides_the_delivery_the_gate_recorded(
    world: World,
) -> None:
    """What the escrow-decides rule guards: the buyer's own call starts the job and the
    seller delivers, but the answer to the start is lost (an error from Temporal that is not
    "unavailable", so the SDK cannot tell the start was made). The SDK settles the job at
    the gate rather than leave the hold locked; the gate, which counted the delivery,
    refuses the cancel, and the job is returned with that delivery -- for the buyer to read,
    and to reject if it is not what was hired -- not a JobFailed saying it refunds."""
    buyer = await world.buyer()
    hired: list[str] = []
    real_hire = buyer.k.gate.hire

    async def remember(
        quote_id: str, *, buyer_key: str, max_price_minor: int | None = None
    ) -> m.HireView:
        view = await real_hire(quote_id, buyer_key=buyer_key, max_price_minor=max_price_minor)
        hired.append(view.job_id)
        return view

    buyer.k.gate.hire = remember  # type: ignore[method-assign]
    client = await buyer.k.temporal()
    real_nexus = client.create_nexus_client
    seller = world.mp.client(world.seller.namespace)

    def losing(*args: Any, **kwargs: Any) -> Any:
        nexus = real_nexus(*args, **kwargs)
        real_start = nexus.start_operation

        async def start_then_lose(*a: Any, **kw: Any) -> Any:
            await real_start(*a, **kw)
            [job_id] = hired
            run = seller.get_workflow_handle(job_workflow_id(job_id))

            async def status() -> WorkflowExecutionStatus | None:
                try:
                    return (await run.describe()).status
                except RPCError:  # not started yet
                    return None

            await until(status, lambda s: s is WorkflowExecutionStatus.COMPLETED)
            raise RPCError("the answer was lost", RPCStatusCode.INTERNAL, b"")

        nexus.start_operation = start_then_lose
        return nexus

    client.create_nexus_client = losing  # type: ignore[method-assign]
    job = await buyer.k.hire(world.key(), text="genuine")  # returns: there is a delivery
    assert job.job_id == hired[0]
    view = await buyer.k.gate.job(job.job_id)
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.HELD)
    result = await job.result(timeout=60)
    assert result.text == "GENUINE"
    disputed = await job.reject("not what I hired")
    assert disputed.escrow is m.Escrow.DISPUTED


async def test_a_delivery_after_the_operation_timed_out_is_still_the_buyers(
    world: World, tmp_path: Path
) -> None:
    """The seller starts the job late and delivers after the buyer's operation timed out
    (``max_duration`` from the buyer's start) but before the gate's deadline (lock +
    max_duration + grace): the gate counts it and releases the money at the review window,
    so ``result()`` returns it instead of raising JobFailed."""
    buyer = await world.buyer()
    bound = sellers.text_offering(tmp_path, max_duration="8s")
    grace = await world.gate(lambda g: g.grace)
    await world.gate(lambda g: setattr(g, "grace", timedelta(seconds=4)))
    try:
        async with world.mp.serve(sellers.text_worker(bound), world.seller):
            # The handler takes 3 s to start it (the operation's 8 s run from before that);
            # the workflow then works 6 s: done ~9 s after the hold, before 8 + 4.
            text = f"late-{uuid.uuid4().hex[:6]}"
            job = await buyer.k.hire(bound.key, text=text, mode="quiet", beats=6)
            result = await job.result(timeout=60)
    finally:
        await world.gate(lambda g: setattr(g, "grace", grace))
    assert result.text == text.upper()
    view = await buyer.k.gate.job(job.job_id)
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.HELD)
    assert (await job.accept()).escrow is m.Escrow.RELEASED


# --------------------------------------------------------------------------- progress


async def test_progress_shows_100_heartbeats_in_order_decrypted(world: World) -> None:
    """Acceptance 13: heartbeats with {percent, message} from a plain activity, in order,
    and the gate holds only ciphertext for them."""
    buyer = await world.buyer()
    job = await buyer.k.hire(world.key(), text="go", mode="beats", beats=100)
    events: list[Progress] = [p async for p in job.progress()]
    beats = [p for p in events if p.source == "heartbeat"]
    assert [p.message for p in beats] == [f"beat {i}" for i in range(1, 101)]
    assert [p.percent for p in beats] == [float(i) for i in range(1, 101)]
    assert {p.activity for p in beats} == {"beat"}
    assert beats[0].details == {"percent": 1, "message": "beat 1"}
    assert str(beats[0]) == "1% beat 1"
    assert [p.seq for p in events] == sorted(p.seq for p in events)
    assert any(p.source == "history" for p in events)
    assert events[-1].state is m.JobState.COMPLETED
    assert (await job.result()).text == "GO"

    stored = await world.gate(lambda g: list(g.jobs[job.job_id].events))
    sealed = [base64.b64decode(e.ciphertext_b64) for e in stored if e.ciphertext_b64]
    assert len(sealed) == 100
    for raw in sealed:
        assert raw.startswith(AGE_MAGIC)
        assert b"beat" not in raw
        with pytest.raises(AgeError):
            decrypt(raw, world.seller.identity)  # for the buyer only


async def test_stopping_progress_early_closes_the_stream(world: World) -> None:
    buyer = await world.buyer()
    job = await buyer.k.hire(world.key(), text="go", mode="beats", beats=5)
    real_progress = buyer.k.gate.progress
    closed: list[bool] = []

    async def recorded(job_id: str, *, after: int = 0) -> AsyncIterator[m.ProgressEvent]:
        try:
            async for event in real_progress(job_id, after=after):
                yield event
        finally:
            closed.append(True)

    buyer.k.gate.progress = recorded  # type: ignore[method-assign]
    async with contextlib.aclosing(job.progress()) as events:
        async for _ in events:
            break
    assert closed == [True]  # the gate's stream is closed with the caller's iterator
    assert (await job.result()).text == "GO"


async def test_a_job_without_heartbeat_details_still_shows_liveness(world: World) -> None:
    buyer = await world.buyer()
    job = await buyer.k.hire(world.key(), text="shh", mode="quiet")
    events = [p async for p in job.progress()]
    assert events
    assert all(p.source == "history" for p in events)
    assert any("quiet" in p.activities_running for p in events)
    running = next(p for p in events if "quiet" in p.activities_running)
    assert str(running).startswith("alive, in quiet, at ")
    assert events[-1].state is m.JobState.COMPLETED
    assert (events[-1].activities_completed or 0) >= 2


# --------------------------------------------------------------------------- artifacts


async def test_artifacts_in_and_out(world: World, tmp_path: Path) -> None:
    buyer = await world.buyer()
    doc = tmp_path / "doc.txt"
    doc.write_text("a document for the seller")
    sent = await buyer.k.upload(world.key(), doc)
    assert isinstance(sent, Artifact)
    assert (sent.size, sent.content_type) == (len("a document for the seller"), "text/plain")

    job = await buyer.k.hire(world.key(), text="files", mode="artifact", beats=3, doc=sent)
    result = await job.result()
    assert result.text == "a document for the seller"  # the seller decrypted our input
    assert isinstance(result.file, Artifact)
    assert isinstance(result.parts[1].file, Artifact)
    assert result.parts[0].file is None
    assert isinstance(result.files["extra"], Artifact)
    assert result.files.extra == result.parts[1].file

    lines = [json.loads(line) async for line in result.file.lines()]
    assert lines == [{"line": i, "of": "main"} for i in range(3)]
    out = await result.files.extra.download(tmp_path / "extra.jsonl")
    assert out.read_text().splitlines() == [
        '{"line": 0, "of": "extra"}',
        '{"line": 1, "of": "extra"}',
    ]
    assert result.to_dict()["file"] == result.file.model_dump()

    typed = await job.result(output_type=TextOut)  # e.g. a model from `kontra pull`
    assert isinstance(typed, TextOut)
    assert typed.file is not None
    assert typed.file.sha256 == result.file.sha256
    plain = await job.result(output_type=dict[str, Any])  # a typing form, not a class
    assert plain == result.to_dict()


# --------------------------------------------------------------------------- re-attach & connect


async def test_a_job_can_be_picked_up_by_id(world: World) -> None:
    buyer = await world.buyer()
    job = await buyer.k.hire(world.key(), text="later")
    other = buyer.party.kontra(client=world.mp.client(buyer.party.namespace))
    again = await other.job(job.job_id)
    assert (again.offering, again.price, again.operation_id) == (
        job.offering,
        job.price,
        job.operation_id,
    )
    result = await again.result(timeout=30)
    assert isinstance(result, Record)
    assert result.text == "LATER"
    listed = await other.jobs()
    assert [v.job_id for v in listed] == [job.job_id]


async def test_the_profile_connection_and_money(world: World) -> None:
    buyer = await world.buyer()
    k = buyer.party.kontra()  # no client given: connects with the profile's worker token
    top = await k.topup("5.00")
    assert top.credited
    assert (await k.balance()).available_minor == START + 500
    job = await k.hire(world.key(), text="direct", max_price=1)
    assert (await job.result()).text == "DIRECT"


async def test_the_gate_stands_in_when_temporal_is_unreachable(world: World) -> None:
    buyer = await world.buyer()

    async def unreachable() -> object:
        raise TemporalUnavailable("no route to the frontend")

    buyer.k.temporal = unreachable  # type: ignore[method-assign]
    job = await buyer.k.hire(world.key(), text="via the gate")  # the gate starts it
    result = await job.result(timeout=60)  # the stored delivery, decrypted here
    assert result.text == "VIA THE GATE"
    failing = await buyer.k.hire(world.key(), text="boom", mode="fail")
    with pytest.raises(JobFailed) as failed:
        await failing.result(timeout=60)
    assert failed.value.state == "failed"


# --------------------------------------------------------------------------- after the lock


async def _unreachable() -> object:
    raise TemporalUnavailable("no route to the frontend")


async def _only_job(buyer: Buyer) -> m.JobView:
    [view] = await buyer.k.jobs()
    return view


async def test_temporal_refusing_the_connection_cancels_the_hold(world: World) -> None:
    """The hold is locked, then Temporal refuses the connection outright (a permission
    error, not "unavailable", so the gate cannot stand in): the job is cancelled at the gate
    at once, and the error names it, with Temporal's as its cause."""
    buyer = await world.buyer(client=None)  # connects with the profile's own session
    denied = RPCError("namespace access denied", RPCStatusCode.PERMISSION_DENIED, b"")

    async def refuse(*args: Any, **kwargs: Any) -> object:
        raise denied

    buyer.k.session.connect_temporal = refuse  # type: ignore[method-assign]
    with pytest.raises(JobFailed) as failed:
        await buyer.k.hire(world.key(), text="denied")
    err = failed.value
    assert err.__cause__ is denied
    assert "could not be started" in err.reason
    assert "PERMISSION_DENIED" in err.reason or "access denied" in err.reason
    assert err.job_id == (await _only_job(buyer)).job_id
    view = await buyer.k.gate.job(err.job_id)
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START, 0)


async def test_a_lost_answer_from_the_gates_start_cancels_the_hold(world: World) -> None:
    """Temporal is unreachable, so the gate is asked to start the job, and its answer is
    lost: the start may or may not have happened. The job is cancelled at the gate rather
    than left holding the buyer's money until its deadline."""
    buyer = await world.buyer()
    buyer.k.temporal = _unreachable  # type: ignore[method-assign]
    lost = httpx.ReadError("the connection dropped before the answer")

    async def start(job_id: str, wire: m.WirePayload) -> m.JobView:
        raise lost

    buyer.k.gate.start = start  # type: ignore[method-assign]
    with pytest.raises(JobFailed) as failed:
        await buyer.k.hire(world.key(), text="lost")
    err = failed.value
    assert err.__cause__ is lost
    assert "ReadError" in err.reason
    assert err.job_id is not None
    view = await buyer.k.gate.job(err.job_id)
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    assert (await buyer.balance()).held_minor == 0


async def test_a_cancel_that_fails_too_names_the_job_and_keeps_the_first_error(
    world: World,
) -> None:
    """The start's answer is lost and the gate cannot be told to cancel either: the error
    raised is still about the start (its cause), and it names the job, which refunds at its
    deadline unless the gate saw a delivery -- the caller can look it up, or cancel it."""
    buyer = await world.buyer()
    buyer.k.temporal = _unreachable  # type: ignore[method-assign]
    lost = httpx.ReadError("the connection dropped before the answer")

    async def start(job_id: str, wire: m.WirePayload) -> m.JobView:
        raise lost

    async def cancel(job_id: str) -> m.JobView:
        raise httpx.ConnectError("the gate is gone")

    real_cancel = buyer.k.gate.cancel
    buyer.k.gate.start = start  # type: ignore[method-assign]
    buyer.k.gate.cancel = cancel  # type: ignore[method-assign]
    with pytest.raises(JobFailed) as failed:
        await buyer.k.hire(world.key(), text="stranded")
    err = failed.value
    assert err.__cause__ is lost  # not the failed cancel
    assert "ReadError" in err.reason
    assert "refunds at the job's deadline" in err.reason
    assert err.job_id == (await _only_job(buyer)).job_id
    view = await buyer.k.gate.job(err.job_id)
    assert (view.state, view.escrow) == (m.JobState.SUBMITTED, m.Escrow.HELD)
    assert (await real_cancel(err.job_id)).escrow is m.Escrow.REFUNDED  # recoverable


async def test_a_hire_cancelled_while_it_starts_cancels_its_job(world: World) -> None:
    """A hire abandoned after the lock (a timeout around it, Ctrl-C) never returns the job,
    so nobody would follow it: the job is cancelled at the gate on the way out."""
    buyer = await world.buyer()
    buyer.k.temporal = _unreachable  # type: ignore[method-assign]
    started = asyncio.Event()

    async def start(job_id: str, wire: m.WirePayload) -> m.JobView:
        started.set()
        await asyncio.Event().wait()  # never answers
        raise AssertionError("unreachable")

    buyer.k.gate.start = start  # type: ignore[method-assign]
    hiring = asyncio.create_task(buyer.k.hire(world.key(), text="abandoned"))
    await asyncio.wait_for(started.wait(), 30)
    hiring.cancel()
    with pytest.raises(asyncio.CancelledError):
        await hiring
    view = await _only_job(buyer)
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    assert (await buyer.balance()).held_minor == 0


async def test_a_used_quote_names_the_job_it_made(world: World) -> None:
    """The gate hires a quote once; hiring it again is refused with the job it became, so a
    hire retried after a lost answer finds the money it locked (``HireRefused.detail``)."""
    buyer = await world.buyer()
    q = await buyer.k.quote(world.key(), text="once")
    job = await q.hire()
    with pytest.raises(HireRefused) as used:
        await q.hire()
    assert (used.value.code, used.value.status) == ("quote_used", 409)
    assert used.value.detail == {"job_id": job.job_id}
    assert used.value.fields["detail"] == {"job_id": job.job_id}  # crosses Temporal too
    # Cleanup. "once" is quick: its run may have started, and delivered, before the cancel
    # looks (decisions I16: a run found then is stopped or, delivered, refuses the cancel).
    try:
        await job.cancel()
    except GateError as e:
        refused = (e.status, e.code)
    else:
        refused = None
    if refused is not None:
        assert refused == (409, "not_open")
        assert (await job.status()).state is m.JobState.COMPLETED


def _answer_lost(
    buyer: Buyer,
    lost: BaseException | None,
    *,
    reached: bool = True,
    keys: list[str] | None = None,
) -> list[str]:
    """The gate's answer to this buyer's next ``hire`` goes missing: the call raises
    ``lost`` (or never returns, when ``lost`` is None) after the gate locked the hold -- or,
    unless ``reached``, without reaching the gate. Later calls (the SDK asking what the
    quote became) reach the gate. The jobs the lost call locked; ``keys`` gets the job key
    it sent."""
    locked: list[str] = []
    real = buyer.k.gate.hire
    calls = 0

    async def hire(
        quote_id: str, *, buyer_key: str, max_price_minor: int | None = None
    ) -> m.HireView:
        nonlocal calls
        calls += 1
        if calls > 1:
            return await real(quote_id, buyer_key=buyer_key, max_price_minor=max_price_minor)
        if keys is not None:
            keys.append(buyer_key)
        if reached:
            hired = await real(quote_id, buyer_key=buyer_key, max_price_minor=max_price_minor)
            locked.append(hired.job_id)
        if lost is None:
            await asyncio.Event().wait()
        raise lost or AssertionError("unreachable")

    buyer.k.gate.hire = hire  # type: ignore[method-assign]
    return locked


def _job_key(buyer: Buyer, job_id: str) -> str:
    """The public half of the key this buyer holds on disk for ``job_id``."""
    return Profile.job_identity(job_id, buyer.k.session.identity_home)[1]


@pytest.mark.parametrize(
    "lost",
    [
        httpx.ReadError("the connection dropped before the answer"),
        GateError(201, "invalid_response", "the gate's answer to hire is not a HireView"),
        GateError(502, "bad_gateway", "the proxy lost the gate's answer"),
    ],
    ids=["transport", "unreadable", "proxy"],
)
async def test_a_lost_answer_to_hire_cancels_the_job_it_locked(
    world: World, lost: Exception
) -> None:
    """The gate locks the hold, and its answer to ``hire`` never arrives (or cannot be
    read). ``Kontra.hire`` quoted internally, so its caller has no quote to retry by and no
    job id: left alone, the money would stay held until the job's deadline with nobody
    knowing which job holds it. The SDK asks the gate what the quote became, cancels that
    job, and names it, with the lost answer as the cause."""
    buyer = await world.buyer()
    sent: list[str] = []
    locked = _answer_lost(buyer, lost, keys=sent)
    with pytest.raises(JobFailed) as failed:
        await buyer.k.hire(world.key(), text="lost")
    err = failed.value
    assert err.__cause__ is lost
    assert err.job_id == locked[0]
    assert "the gate's answer to the hire was lost" in err.reason
    # The job was locked with the key written before the call: it is filed under the job.
    assert _job_key(buyer, err.job_id) == sent[0]
    view = await buyer.k.gate.job(err.job_id)
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START, 0)


async def test_a_hire_that_never_reached_the_gate_keeps_its_error(world: World) -> None:
    """The same failure before the gate locked anything: the SDK's question finds no job,
    nothing is locked by asking, and the original error is raised as it was."""
    buyer = await world.buyer()
    lost = httpx.ConnectError("the gate is not there")
    _answer_lost(buyer, lost, reached=False)
    with pytest.raises(httpx.ConnectError) as failed:
        await buyer.k.hire(world.key(), text="never sent")
    assert failed.value is lost
    assert await buyer.k.jobs() == []
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START, 0)


async def test_a_quote_whose_hire_answer_was_lost_is_hired_again_to_find_its_job(
    world: World,
) -> None:
    """Holding the quote, the caller has a way back (as ``kontra mcp`` does): the lost
    answer's error is raised as it came, and hiring the same quote again is refused with the
    job the lost call locked -- never a second lock. The key that job was locked with
    survives the retry and is filed under the job, so its delivery stays readable."""
    buyer = await world.buyer()
    q = await buyer.k.quote(world.key(), text="again")
    lost = httpx.ReadError("the connection dropped before the answer")
    sent: list[str] = []
    locked = _answer_lost(buyer, lost, keys=sent)
    with pytest.raises(httpx.ReadError) as failed:
        await q.hire()
    assert failed.value is lost
    with pytest.raises(HireRefused) as used:
        await q.hire()
    assert (used.value.code, used.value.detail) == ("quote_used", {"job_id": locked[0]})
    assert _job_key(buyer, locked[0]) == sent[0]
    assert len(await buyer.k.jobs()) == 1  # locked once
    assert (await buyer.balance()).held_minor > 0
    assert (await buyer.k.gate.cancel(locked[0])).escrow is m.Escrow.REFUNDED


async def test_a_hire_cancelled_while_the_gate_locks_cancels_the_job(world: World) -> None:
    """A hire abandoned (a timeout around it, Ctrl-C) while the gate's answer to ``hire``
    is still on its way: the job the quote became is cancelled on the way out."""
    buyer = await world.buyer()
    locked = _answer_lost(buyer, None)
    hiring = asyncio.create_task(buyer.k.hire(world.key(), text="abandoned"))

    async def jobs() -> list[str]:
        return list(locked)

    await until(jobs, bool)
    hiring.cancel()
    with pytest.raises(asyncio.CancelledError):
        await hiring
    view = await buyer.k.gate.job(locked[0])
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    assert (await buyer.balance()).held_minor == 0


async def test_a_job_the_gate_had_closed_is_never_reported_cancelled(
    world: World, caplog: pytest.LogCaptureFixture
) -> None:
    """A hire abandoned while its job starts, whose job the gate has closed already (its
    cancel answers 409: delivered, perhaps). The log must not tell the buyer it was
    cancelled -- a delivered job is paid at the review window unless the buyer looks."""
    buyer = await world.buyer()
    buyer.k.temporal = _unreachable  # type: ignore[method-assign]
    started = asyncio.Event()

    async def start(job_id: str, wire: m.WirePayload) -> m.JobView:
        started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def closed(job_id: str) -> m.JobView:
        raise GateError(409, "not_open", "job is completed")

    real_cancel = buyer.k.gate.cancel
    buyer.k.gate.start = start  # type: ignore[method-assign]
    buyer.k.gate.cancel = closed  # type: ignore[method-assign]
    caplog.set_level(logging.WARNING, logger="kontra.hire")
    hiring = asyncio.create_task(buyer.k.hire(world.key(), text="closed"))
    await asyncio.wait_for(started.wait(), 30)
    hiring.cancel()
    with pytest.raises(asyncio.CancelledError):
        await hiring
    said = "\n".join(r.getMessage() for r in caplog.records if r.name == "kontra.hire")
    assert "had already closed it" in said
    assert "it may have delivered" in said
    assert "it was cancelled" not in said
    [view] = await buyer.k.jobs()
    await real_cancel(view.job_id)


# --------------------------------------------------------------------------- trust nothing


async def test_unknown_and_malformed_offerings(world: World) -> None:
    buyer = await world.buyer()
    with pytest.raises(HireRefused) as missing:
        await buyer.k.quote("acme/nothing-here", text="x")
    assert (missing.value.code, missing.value.status) == ("offering_not_found", 404)
    with pytest.raises(HireRefused) as malformed:
        await buyer.k.quote("Acme/Enrich\n", text="x")
    assert malformed.value.code == "invalid_offering"


async def test_a_catalog_entry_must_agree_with_its_sealed_contract(world: World) -> None:
    buyer = await world.buyer()
    entry = await buyer.k.offering(world.key())
    assert buyer.k.contract(entry).offering.key == world.key()
    for change in (
        {"party": "mallory"},  # the first component of the sealed name
        {"service": "evil.text"},
        {"offering": "acme/text@9.9.9"},
        {"contract_hash": "sha256:" + "0" * 64},
    ):
        with pytest.raises(HireRefused) as refused:
            buyer.k.contract(entry.model_copy(update=change))
        assert refused.value.code == "invalid_contract"


async def test_a_seller_entry_from_the_endpoint_lookup_is_checked_too(world: World) -> None:
    """On a cache miss (a buyer worker after a restart) the codec asks the catalog for the
    recipients behind an endpoint: that entry must agree with the contract it seals, like
    any other catalog entry, before anything is encrypted to its recipients."""
    buyer = await world.buyer()
    entry = await buyer.k.offering(world.key())  # resolving does not fill the key cache
    real_by_endpoint = buyer.k.gate.by_endpoint

    async def lying_by_endpoint(endpoint: str) -> m.CatalogEntry:
        found = await real_by_endpoint(endpoint)
        return found.model_copy(update={"service": "evil.text"})

    buyer.k.gate.by_endpoint = lying_by_endpoint  # type: ignore[method-assign]
    with pytest.raises(HireRefused) as refused:
        await buyer.k.seller_keys.seller_recipients(entry.endpoint)
    assert refused.value.code == "invalid_contract"
    buyer.k.gate.by_endpoint = real_by_endpoint  # type: ignore[method-assign]
    found = await buyer.k.seller_keys.seller_recipients(entry.endpoint)
    assert list(found) == entry.recipients


async def test_a_hold_that_names_other_seller_recipients_is_cancelled_not_used(
    world: World,
) -> None:
    """The job input is encrypted to the seller recipients the quote named; a hire answer
    naming any other set is refused and its hold cancelled at once. The contract seals no
    key (decisions I7), so the quote is what the hire answer is held to."""
    buyer = await world.buyer()
    real_hire = buyer.k.gate.hire

    async def lying_hire(
        quote_id: str, *, buyer_key: str, max_price_minor: int | None = None
    ) -> m.HireView:
        view = await real_hire(quote_id, buyer_key=buyer_key, max_price_minor=max_price_minor)
        return view.model_copy(update={"seller_recipients": [buyer.k.public_key]})

    buyer.k.gate.hire = lying_hire  # type: ignore[method-assign]
    with pytest.raises(HireRefused) as refused:
        await buyer.k.hire(world.key(), text="secret")
    assert refused.value.code == "hold_mismatch"
    [view] = await buyer.k.jobs()
    assert refused.value.detail == {"job_id": view.job_id}  # the hold it refused, named
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    entry = await buyer.k.offering(world.key())
    found = await buyer.k.seller_keys.seller_recipients(entry.endpoint)
    assert list(found) == entry.recipients  # not cached


async def test_a_hold_with_another_max_duration_is_cancelled_not_used(world: World) -> None:
    """The operation's schedule-to-close comes from the hire answer: it must be the quoted
    max_duration, like every other term of the hold."""
    buyer = await world.buyer()
    real_hire = buyer.k.gate.hire

    async def lying_hire(
        quote_id: str, *, buyer_key: str, max_price_minor: int | None = None
    ) -> m.HireView:
        view = await real_hire(quote_id, buyer_key=buyer_key, max_price_minor=max_price_minor)
        return view.model_copy(update={"max_duration_seconds": view.max_duration_seconds * 10})

    buyer.k.gate.hire = lying_hire  # type: ignore[method-assign]
    with pytest.raises(HireRefused) as refused:
        await buyer.k.hire(world.key(), text="patience")
    assert refused.value.code == "hold_mismatch"
    [view] = await buyer.k.jobs()
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)


async def test_a_delivery_that_breaks_the_contract_is_refused(world: World) -> None:
    buyer = await world.buyer()
    job = await buyer.k.hire(world.key(), text="ok")
    await job.result()
    with pytest.raises(InvalidDelivery) as invalid:
        await buyer.k._deliver(job, {"text": 1}, None)
    assert invalid.value.value == {"text": 1}
    assert invalid.value.job_id == job.job_id
