"""``kontra.serve`` end to end: a real dev server, the fake gate, a seller's Worker, and a
buyer with nothing but temporalio (until ``kontra.hire`` exists)."""

from __future__ import annotations

import asyncio
import base64
import json
import time
import uuid
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import jwt
import pytest
import pytest_asyncio
from cryptography.hazmat.primitives.asymmetric import ec
from temporalio.api.common.v1 import Payload, WorkflowExecution, WorkflowType
from temporalio.api.enums.v1 import EventType, WorkflowIdConflictPolicy, WorkflowIdReusePolicy
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    DescribeNexusOperationExecutionRequest,
    DescribeWorkflowExecutionRequest,
    StartWorkflowExecutionRequest,
)
from temporalio.client import NexusOperationFailureError, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from kontra._control import OCCUPY_WORKFLOW_TYPE
from kontra.auth import Session
from kontra.codec import ENCODING
from kontra.codec.age import decrypt
from kontra.codec.stream import STREAM_ENCODING, open_item, parse_envelope
from kontra.contract.names import PROGRESS_TOPIC, job_workflow_id, new_job_id
from kontra.gate_client import models as m
from kontra.profile import Profile
from kontra.serve import BoundOffering, Worker
from kontra.serve.progress import INTERVAL_ENV
from kontra.testing import Marketplace, TestParty
from kontra.testing.devserver import DevServer
from kontra.testing.marketplace import SERVE_UPLOAD_BUDGET
from kontra.types import ProgressRecord

from . import seller
from .flows import EchoChild, beat, count, shout, sync_beat
from .models import Shout
from .probes import where
from .support import (
    MARKER,
    Buyer,
    CountingTransport,
    failure_text,
    failure_traces,
    write_offering,
)

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]


@dataclass
class World:
    mp: Marketplace
    seller: TestParty
    buyer: Buyer
    bound: BoundOffering
    worker: Worker
    entry: m.CatalogEntry
    transport: CountingTransport

    async def gate_job(self, job_id: str) -> Any:
        gate = self.mp.gate

        async def get() -> Any:
            return gate.jobs[job_id]

        return await gate.run(get)


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def world(
    dev_server: DevServer, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[World]:
    with pytest.MonkeyPatch.context() as env:
        env.setenv(INTERVAL_ENV, "0")  # every heartbeat, in order (acceptance 13)
        async with Marketplace(dev_server=dev_server) as mp:
            acme = await mp.party("acme")
            bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="1000.00")
            bound = seller.bind(write_offering(tmp_path_factory.mktemp("acme")))
            transport = CountingTransport()
            session = Session(Profile.load(acme.home), transport=transport, home=acme.home)
            worker = Worker(
                offerings=[bound],
                activities=[shout, count, beat, sync_beat, where],
                workflows=[EchoChild],
                activity_executor=ThreadPoolExecutor(4),
            )
            try:
                async with mp.serve(worker, acme, session=session):
                    entry = await bob.gate.get_offering(bound.key)
                    yield World(mp, acme, Buyer(mp, bob), bound, worker, entry, transport)
            finally:
                await session.aclose()


async def test_the_worker_published_its_offering_on_a_dev_profile(world: World) -> None:
    assert world.entry.contract_hash == world.bound.contract_hash()
    assert world.entry.task_queue == world.bound.offering_id.task_queue
    assert world.entry.service == "acme.upper"
    [inner] = world.worker.inner_workers
    assert inner.task_queue == world.bound.key


async def test_quote_through_the_gate_relay(world: World) -> None:
    q = await world.buyer.quote(world.entry, {"text": "hello", "copies": 2})
    assert (q.status, q.amount_minor, q.currency) == (m.QuoteStatus.OK, 10, "USD")
    assert q.max_duration_seconds == 600
    rejected = await world.buyer.quote(world.entry, {"text": "reject me"})
    assert (rejected.status, rejected.reason) == (
        m.QuoteStatus.REJECTED,
        "this text is not for sale",
    )
    busy = await world.buyer.quote(world.entry, {"text": "busy"})
    assert (busy.status, busy.eta_seconds) == (m.QuoteStatus.BUSY, 30)


async def test_quote_refuses_an_extra_field_the_model_would_ignore(world: World) -> None:
    assert world.bound.input_type.model_validate({"text": "x", "sneaky": 1})  # pydantic ignores it
    q = await world.buyer.quote(world.entry, {"text": "x", "sneaky": 1})
    assert q.status == m.QuoteStatus.REJECTED
    assert q.reason is not None
    assert "does not match the contract" in q.reason
    assert "sneaky" not in q.reason  # paths and values stay on the seller's machine


async def test_a_full_execute_with_the_codec_on_both_sides(world: World) -> None:
    text = f"hi {MARKER}"
    hired = await world.buyer.hire(world.entry, {"text": text, "copies": 2})
    handle = await world.buyer.start(hired, {"text": text, "copies": 2}, result_type=Shout)
    out = await handle.result()
    assert isinstance(out, Shout)
    assert out.text == text.upper() * 2
    assert out.length == len(text)
    # the job context reached the root's activity, local activity and the child's activity
    assert out.job == hired.job_id
    assert out.local_job == hired.job_id
    assert out.child_job == hired.job_id
    # the delivery the gate stored is ciphertext the buyer can open
    seller_client = world.mp.client("acme")
    desc = await seller_client.get_workflow_handle(job_workflow_id(hired.job_id)).describe()
    assert desc.status is not None
    assert world.worker.jobs is not None
    assert world.worker.jobs.cached(hired.job_id) is not None


# --------------------------------------------------------------------------- hold tokens


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _claims(token: str) -> dict[str, Any]:
    return jwt.decode(token, options={"verify_signature": False})


def _tampered(token: str, **changes: Any) -> str:
    """The payload edited, the gate's signature kept (what a buyer can do to a token)."""
    header, payload, signature = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    claims.update(changes)
    return f"{header}.{_b64url(json.dumps(claims).encode())}.{signature}"


async def _refused(world: World, hired: m.HireView, value: Any, token: str, job_id: str) -> str:
    """Start ``execute`` with ``token``; it must fail, with no gate call from the worker and
    no workflow in the seller's namespace. Returns the refusal text the buyer sees."""
    before = len(world.transport.requests)
    handle = await world.buyer.start(hired, value, token=token)
    with pytest.raises(NexusOperationFailureError) as failed:
        await handle.result()
    assert world.transport.paths(before) == []  # verified offline: no gate call
    with pytest.raises(RPCError) as missing:
        await world.mp.client("acme").get_workflow_handle(job_workflow_id(job_id)).describe()
    assert missing.value.status == RPCStatusCode.NOT_FOUND  # nothing started
    # the refusal is plaintext in the buyer's history: no traceback of the seller's machine
    traces = failure_traces(failed.value)
    assert traces, "the refusal reached the buyer without a handler error"
    assert all('File "' not in t and "site-packages" not in t for t in traces), traces
    return failure_text(failed.value)


async def test_tampered_tokens_are_refused_offline(world: World) -> None:
    """Acceptance 7: amount, seller, expiry, offering, contract hash edited; another key;
    no token at all -- each refused by the handler without contacting the gate."""
    value = {"text": "abc"}
    hired = await world.buyer.hire(world.entry, value)
    token, job = hired.hold_token, hired.job_id
    claims = _claims(token)
    other_key = ec.generate_private_key(ec.SECP256R1())
    cases = {
        "amount": _tampered(token, amount_minor=claims["amount_minor"] * 100),
        "seller": _tampered(token, seller="evil", seller_ns="evil"),
        "expiry": _tampered(token, exp=claims["exp"] + 86_400),
        "offering": _tampered(token, offering="acme/upper@9.9.9"),
        "contract hash": _tampered(token, contract_hash="sha256:" + "0" * 64),
        "buyer key": _tampered(token, buyer_key=world.seller.public_key),
        "another key, the gate's kid": jwt.encode(
            claims, other_key, algorithm="ES256", headers={"kid": world.mp.gate.keys.kid}
        ),
        "another key, unknown kid": jwt.encode(
            claims, other_key, algorithm="ES256", headers={"kid": "not-the-gates"}
        ),
        "no signature": jwt.encode(claims, None, algorithm="none"),
        "not a JWT": "hold-me",
    }
    for case, bad in cases.items():
        text = await _refused(world, hired, value, bad, job)
        assert "hold token refused" in text, (case, text)
    text = await _refused(world, hired, value, "", job)
    assert "execute needs the kontra-hold header" in text
    # the untouched token still works afterwards
    out = await (await world.buyer.start(hired, value, result_type=Shout)).result()
    assert out.text == "ABC"


async def test_validly_signed_tokens_for_something_else_are_refused(world: World) -> None:
    """The gate's own signature on the wrong claims: other seller, other offering, expired,
    not a hold (audience) -- and acceptance 18, a token for contract X at a handler serving
    Y, refused before any workflow starts."""
    value = {"text": "abc"}
    hired = await world.buyer.hire(world.entry, value)
    base = _claims(hired.hold_token)
    sign = world.mp.gate.keys.sign

    def minted(**changes: Any) -> tuple[str, str]:
        job = new_job_id()
        return sign({**base, "job_id": job, "sub": job, **changes}), job

    served = world.bound.contract_hash()
    other_hash = "sha256:" + "ab" * 32
    token, job = minted(contract_hash=other_hash)
    text = await _refused(world, hired, value, token, job)
    assert f"it is for contract {other_hash}" in text
    assert f"serves {served}" in text

    for changes, expect in (
        ({"seller": "zed", "seller_ns": "zed"}, "for the seller 'zed'"),
        ({"offering": "acme/upper@0.0.1"}, "it is for acme/upper@0.0.1"),
        ({"exp": base["iat"] - 1}, "expired"),
        ({"aud": "kontra:api"}, "not a hold token"),
        ({"max_duration_seconds": 10**6}, "max_duration"),
        ({"sub": new_job_id()}, "names no valid job"),
    ):
        token, job = minted(**changes)
        text = await _refused(world, hired, value, token, job)
        assert expect in text, (changes, text)


async def test_the_requote_guard(world: World) -> None:
    """Decisions I10: quote a small input, execute a large one -> refused; a hold in another
    currency, or shorter than this input needs -> refused. Before anything starts."""
    small = {"text": "ab"}
    hired = await world.buyer.hire(world.entry, small)
    assert hired.amount_minor == 2
    text = await _refused(world, hired, {"text": "abcdefgh"}, hired.hold_token, hired.job_id)
    assert "prices above the held 0.02 USD" in text
    assert "abcdefgh" not in text
    text = await _refused(world, hired, {"text": "reject me"}, hired.hold_token, hired.job_id)
    assert "does not accept this input now" in text
    job = new_job_id()
    usdc = world.mp.gate.keys.sign(
        {
            **_claims(hired.hold_token),
            "job_id": job,
            "sub": job,
            "currency": "USDC",
            "amount_minor": 10**9,
        }
    )
    text = await _refused(world, hired, small, usdc, job)
    assert "now prices in USD, but the hold is in USDC" in text
    # quoted with a short max_duration, executed on an input the seller needs longer for
    job = new_job_id()
    short = world.mp.gate.keys.sign(
        {**_claims(hired.hold_token), "job_id": job, "sub": job, "max_duration_seconds": 60}
    )
    text = await _refused(world, hired, small, short, job)
    assert "needs longer than the held max_duration of 60s" in text
    assert "600" not in text  # the re-quoted duration describes the input: not in plaintext
    # the input itself is validated against the contract, extra fields included
    text = await _refused(world, hired, {**small, "sneaky": True}, hired.hold_token, hired.job_id)
    assert "does not match the contract" in text
    assert "sneaky" not in text
    # an equal or lower price is fine
    out = await (await world.buyer.start(hired, {"text": "a"}, result_type=Shout)).result()
    assert out.text == "A"


async def test_a_job_runs_once_per_hold(world: World) -> None:
    """``job-<id>`` is started with REJECT_DUPLICATE: a second execute with the same token
    after the job finished is refused, not run again."""
    value = {"text": "once"}
    hired = await world.buyer.hire(world.entry, value)
    assert (
        await (await world.buyer.start(hired, value, result_type=Shout)).result()
    ).text == "ONCE"
    again = await world.buyer.start(hired, value)
    with pytest.raises(NexusOperationFailureError) as failed:
        await again.result()
    assert f"job {hired.job_id} already ran" in failure_text(failed.value)


async def test_a_second_execute_while_the_job_runs_is_refused_not_attached(
    world: World,
) -> None:
    """``job-<id>`` is started with conflict policy FAIL: a second ``execute`` with the same
    hold while the job runs is refused, not attached to the running job (an attached call
    did not start the run, so escrow would never count what it returns: decisions I15). The
    first call is unaffected, and a retry of the handler's own start -- the same request
    id, as Nexus retries it -- still finds its run rather than failing."""
    value = {"text": "only once", "hold": True}
    hired = await world.buyer.hire(world.entry, value)
    first = await world.buyer.start(hired, value, result_type=Shout)
    seller = world.mp.client(world.seller.namespace)
    wf = job_workflow_id(hired.job_id)
    for _ in range(200):
        try:
            if (await seller.get_workflow_handle(wf).describe()).status is (
                WorkflowExecutionStatus.RUNNING
            ):
                break
        except RPCError:
            pass
        await asyncio.sleep(0.05)
    again = await world.buyer.start(hired, value)
    with pytest.raises(NexusOperationFailureError) as failed:
        await again.result()
    assert f"job {hired.job_id} already ran (or is running)" in failure_text(failed.value)

    described = await seller.workflow_service.describe_workflow_execution(
        DescribeWorkflowExecutionRequest(
            namespace=seller.namespace, execution=WorkflowExecution(workflow_id=wf)
        )
    )
    run_id = described.workflow_execution_info.execution.run_id
    infos = described.workflow_extended_info.request_id_infos
    [started_with] = [
        rid
        for rid, info in infos.items()
        if info.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
    ]
    assert len(infos) == 1  # nothing was attached to the run
    retried = await seller.workflow_service.start_workflow_execution(
        StartWorkflowExecutionRequest(
            namespace=seller.namespace,
            workflow_id=wf,
            workflow_type=WorkflowType(name="UpperFlow"),
            task_queue=TaskQueue(name=world.bound.offering_id.task_queue),
            request_id=started_with,
            workflow_id_reuse_policy=WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
            workflow_id_conflict_policy=WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
        )
    )
    assert retried.run_id == run_id  # the first start's own answer: no error, no new run

    await seller.get_workflow_handle(wf).signal("release")
    assert (await first.result()).text == "ONLY ONCE"


# --------------------------------------------------------------------------- decisions I16


async def test_a_canceled_job_cannot_be_started_with_its_still_valid_hold(world: World) -> None:
    """The buyer cancels through the gate before starting (refunded), then runs execute
    with the hold it still has. The cancel occupied ``job-<id>``, so kontra.serve's handler
    refuses the start and no work is done for free."""
    value = {"text": "free work"}
    hired = await world.buyer.hire(world.entry, value)
    view = await world.buyer.party.gate.cancel(hired.job_id)
    assert (view.state, view.escrow) == (m.JobState.CANCELED, m.Escrow.REFUNDED)
    handle = await world.buyer.start(hired, value)
    with pytest.raises(NexusOperationFailureError) as failed:
        await handle.result()
    assert f"job {hired.job_id} already ran" in failure_text(failed.value)
    desc = (
        await world.mp.client("acme").get_workflow_handle(job_workflow_id(hired.job_id)).describe()
    )
    assert (desc.workflow_type, desc.status) == (
        OCCUPY_WORKFLOW_TYPE,
        WorkflowExecutionStatus.TERMINATED,
    )
    view = await world.buyer.party.gate.job(hired.job_id)
    assert (view.state, view.delivery) == (m.JobState.CANCELED, None)


def _minted(world: World, hired: m.HireView, **changes: Any) -> tuple[str, str]:
    """A token the gate signed for a new job, ``changes`` applied to the hire's claims."""
    job = new_job_id()
    claims = {**_claims(hired.hold_token), "job_id": job, "sub": job, **changes}
    return world.mp.gate.keys.sign(claims), job


async def test_a_late_start_ends_by_the_holds_expiry(world: World) -> None:
    """A start late in the hold's life runs until the gate's refund deadline, which the
    hold carries (this gate's grace is one second), not for a full max_duration from the
    start nor for an escrow grace this gate does not give; a token without the claim gets
    the expiry plus the default grace; a start too late for what the input needs is
    refused before anything runs."""
    value = {"text": "quick one"}  # the seller quotes a minute for it
    hired = await world.buyer.hire(world.entry, value)
    view = await world.buyer.party.gate.job(hired.job_id)
    assert _claims(hired.hold_token)["deadline"] == int(view.deadline.timestamp())

    async def timeout_of(**changes: Any) -> timedelta:
        token, job = _minted(world, hired, max_duration_seconds=600, **changes)
        started = await world.buyer.start(hired, value, token=token, result_type=Shout)
        assert (await started.result()).text == "QUICK ONE"
        desc = await world.mp.client("acme").get_workflow_handle(job_workflow_id(job)).describe()
        return desc.raw_description.execution_config.workflow_execution_timeout.ToTimedelta()

    # A 10-minute hold with two minutes left: the job runs for those (and the gate's grace).
    exp = int(time.time()) + 120
    cut = await timeout_of(exp=exp, deadline=exp + 1)
    assert timedelta(seconds=110) <= cut <= timedelta(seconds=121)
    no_claim = await timeout_of(exp=exp, deadline=None)  # 120 s + the default 30 s grace
    assert timedelta(seconds=140) <= no_claim <= timedelta(seconds=150)

    exp = int(time.time()) + 20  # + 30 s tolerance < the 60 s the seller quotes
    token, job = _minted(world, hired, exp=exp, deadline=exp + 1)
    text = await _refused(world, hired, value, token, job)
    assert "sooner than this input needs" in text


async def test_execute_is_bounded_by_the_input_the_hold_was_quoted_for(world: World) -> None:
    """Quote a small input, execute a huge one: refused at once, naming the bound, before
    the seller fetches a byte of it -- the answer comes within one Nexus attempt, so the
    seller is never recorded as absent. The quoted input itself, claim-checked, runs."""
    buyer = Buyer(world.mp, world.buyer.party, claim_check_threshold=1024)
    big = {"text": "q" * 1500}
    before = len(world.transport.requests)
    hired = await buyer.hire(world.entry, big)  # the quote's input is claim-checked too
    assert [p for p in world.transport.paths(before) if p.startswith("GET /v1/blobs/")]
    assert (await (await buyer.start(hired, big, result_type=Shout)).result()).text == ("Q" * 1500)

    small = {"text": "ab"}
    hired = await buyer.hire(world.entry, small)
    before = len(world.transport.requests)
    huge = {"text": "h" * 40_000}
    began = time.monotonic()
    handle = await buyer.start(hired, huge)
    with pytest.raises(NexusOperationFailureError) as failed:
        await handle.result()
    assert time.monotonic() - began < 5
    text = failure_text(failed.value)
    assert "larger than the input this hold was quoted for" in text
    assert [p for p in world.transport.paths(before) if p.startswith("GET /v1/blobs/")] == []
    desc = await buyer.client.workflow_service.describe_nexus_operation_execution(
        DescribeNexusOperationExecutionRequest(
            namespace=buyer.party.namespace, operation_id=handle.operation_id
        )
    )
    assert desc.info.last_attempt_failure.nexus_handler_failure_info.type == "BAD_REQUEST"
    history = await world.mp.gate.run(lambda: _blob_count(world))
    assert history >= 2  # the huge input was uploaded (claim-checked) but never fetched


async def _blob_count(world: World) -> int:
    return len(world.mp.gate.blobs)


# --------------------------------------------------------------------------- progress & history


async def _events(world: World, job_id: str) -> list[m.StreamEvent]:
    gate = world.mp.gate

    async def get() -> list[m.StreamEvent]:
        return list(gate.jobs[job_id].events)

    return await gate.run(get)


async def _drain(events: Any) -> None:
    async for _ in events:
        pass


async def _relaying(world: World, job_id: str) -> None:
    """Until the gate relays the job's stream (its buyer listens and it is pinned)."""
    gate = world.mp.gate

    async def running() -> bool:
        return gate.jobs[job_id].relay.running

    for _ in range(200):
        if await gate.run(running):
            return
        await asyncio.sleep(0.05)
    raise AssertionError("the gate never relayed the job's stream")


async def test_heartbeats_reach_the_buyer_in_order_and_only_as_ciphertext(world: World) -> None:
    """Acceptance 13, the seller's half: 100 heartbeats (interval 0) from an async activity,
    then 3 from a sync one in a thread, all published in order on the job stream's
    kontra.progress topic (decisions I31) and relayed by the gate while the buyer listens,
    sealed to the job's parties."""
    value = {"text": MARKER, "beats": 100, "sync_beats": 3, "hold": True}
    hired = await world.buyer.hire(world.entry, value)
    handle = await world.buyer.start(hired, value, result_type=Shout)
    events: list[m.StreamEvent] = []

    async def listen() -> None:
        stream = world.buyer.party.gate.stream(hired.job_id, topics=[PROGRESS_TOPIC])
        async for event in stream:
            events.append(event)

    listener = asyncio.create_task(listen())
    await _relaying(world, hired.job_id)
    await asyncio.sleep(0.5)  # the relay's first poll waits in the job
    root = world.mp.client("acme").get_workflow_handle(job_workflow_id(hired.job_id))
    await root.signal("release")
    await handle.result()
    await asyncio.wait_for(listener, 60)  # ends once the job is closed and its relay drained
    beats = [e for e in events if e.kind is m.StreamEventKind.ITEM]
    assert [e.topic for e in beats] == [PROGRESS_TOPIC] * 103
    seen = []
    for e in beats:
        assert e.ciphertext_b64 is not None
        assert e.offset is not None
        raw = base64.b64decode(e.ciphertext_b64)
        assert MARKER.encode() not in raw
        record = ProgressRecord.model_validate_json(
            open_item(raw, [world.buyer.party.identity], topic=PROGRESS_TOPIC, offset=e.offset).data
        )
        seen.append((record.activity, record.details))
    assert [a for a, _ in seen] == ["beat"] * 100 + ["sync_beat"] * 3
    assert [d["percent"] for _, d in seen] == [*range(1, 101), 1001, 1002, 1003]
    assert seen[0][1] == {"percent": 1, "message": f"1 {MARKER}"}
    open_item(  # the seller's whole set reads its own items too; nobody else does
        base64.b64decode(beats[0].ciphertext_b64 or ""),
        [world.seller.identity],
        topic=PROGRESS_TOPIC,
        offset=beats[0].offset or 0,
    )
    assert MARKER not in json.dumps([e.model_dump(mode="json") for e in events])
    stored = await _events(world, hired.job_id)
    assert MARKER not in json.dumps([e.model_dump(mode="json") for e in stored])


async def test_the_marketplace_stores_stream_items_within_a_test_machines_budget(
    world: World,
) -> None:
    """``mp.serve`` gives the worker's codec ``SERVE_UPLOAD_BUDGET`` (8 s) where ``kontra
    serve`` gives 2 s: the heartbeat test above counts every item, and a loaded test machine
    can take longer than 2 s to store an answer's 64 items in the fake gate's bucket (an item
    not stored in time is a gap). The stubs keep their own tests, with a small budget
    (``tests/codec/test_stream_envelope.py``, ``tests/serve/test_stream_hosting.py``)."""
    plugin = world.worker.plugin
    assert plugin is not None
    assert plugin.codec is not None
    assert plugin.codec.stream_upload_budget == SERVE_UPLOAD_BUDGET == 8.0


def _payloads(node: Any, path: str = "") -> list[tuple[str, dict[str, Any]]]:
    """Every Payload in a history's JSON (``{"metadata": ..., "data": ...}``), with its path."""
    out: list[tuple[str, dict[str, Any]]] = []
    if isinstance(node, dict):
        if (
            "metadata" in node
            and isinstance(node["metadata"], dict)
            and "encoding" in node["metadata"]
        ):
            out.append((path, node))
        for k, v in node.items():
            out += _payloads(v, f"{path}.{k}")
    elif isinstance(node, list):
        for i, v in enumerate(node):
            out += _payloads(v, f"{path}[{i}]")
    return out


_LA_MARKER_KEYS = {
    "seq",
    "attempt",
    "activity_id",
    "activity_type",
    "complete_time",
    "backoff",
    "original_schedule_time",
    "activation_index",
}


async def test_every_payload_in_the_sellers_history_is_encrypted(world: World) -> None:
    """The root handler workflow, its activities, its local activity and its child: no
    plaintext in history (the marker scan of seam ⑤). Only the ``kontra-job`` header is
    plain, by design, and it carries the job id alone; Temporal's local-activity marker
    metadata is Temporal's, not ours. The job's stream is there too (decisions I31): the
    heartbeats' publish signals are encrypted like any job payload, and a poll's answer is
    the stream envelope, each item sealed on its own."""
    value = {"text": f"{MARKER} history", "beats": 2, "hold": True}
    hired = await world.buyer.hire(world.entry, value)
    handle = await world.buyer.start(hired, value, result_type=Shout)
    listener = asyncio.create_task(_drain(world.buyer.party.gate.stream(hired.job_id)))
    await _relaying(world, hired.job_id)
    seller_client = world.mp.client("acme")
    root = job_workflow_id(hired.job_id)
    await seller_client.get_workflow_handle(root).signal("release")
    await handle.result()
    await asyncio.wait_for(listener, 60)
    encrypted = base64.b64encode(ENCODING).decode()
    envelope = base64.b64encode(STREAM_ENCODING).decode()
    envelopes = 0
    checked = 0
    for wf_id in (root, f"{root}-echo"):
        history = await seller_client.get_workflow_handle(wf_id).fetch_history()
        raw = b"".join(e.SerializeToString() for e in history.events)
        assert MARKER.encode() not in raw
        text = history.to_json()
        assert MARKER not in text
        for path, p in _payloads(json.loads(text)):
            if path.endswith("markerRecordedEventAttributes.details.data.payloads[0]"):
                # Temporal Core's own local-activity bookkeeping (sequence, attempt, type,
                # times): metadata, not a payload of ours. The result is in "result".
                meta = json.loads(base64.b64decode(p["data"]))
                assert set(meta) <= _LA_MARKER_KEYS, meta
                continue
            if ".header." in path:
                assert set(p["metadata"]) == {"encoding"}
                assert json.loads(base64.b64decode(p["data"])) == hired.job_id
                continue
            if path.endswith(
                "UpdateAcceptedEventAttributes.acceptedRequest.input.args.payloads[0]"
            ):
                # The relay's poll: the gate sends it, and has no key -- the topics a buyer
                # may read (public, in the catalog) and an offset, nothing else.
                assert json.loads(base64.b64decode(p["data"])) == {
                    "from_offset": json.loads(base64.b64decode(p["data"]))["from_offset"],
                    "topics": [PROGRESS_TOPIC],
                }
                continue
            if p["metadata"]["encoding"] == envelope:
                assert "workflowExecutionUpdateCompletedEventAttributes" in path, path
                parse_envelope(
                    Payload(
                        metadata={k: base64.b64decode(v) for k, v in p["metadata"].items()},
                        data=base64.b64decode(p["data"]),
                    )
                )  # strict: every item's ciphertext is an age file
                envelopes += 1
                continue
            assert p["metadata"]["encoding"] == encrypted, path
            checked += 1
    assert checked >= 8  # inputs, results, activity and child payloads were all seen
    assert envelopes >= 1  # the relay polled: its answers are envelopes


async def test_the_gate_observes_the_delivery_as_ciphertext_to_the_buyer(world: World) -> None:
    value = {"text": "deliver"}
    hired = await world.buyer.hire(world.entry, value)
    await (await world.buyer.start(hired, value, result_type=Shout)).result()
    gate = world.buyer.party.gate
    for _ in range(100):
        view = await gate.job(hired.job_id)
        if view.state is m.JobState.COMPLETED:
            break
        await world.mp.gate.sweep()
    assert view.state is m.JobState.COMPLETED
    stored = await gate.result(hired.job_id)

    payload = Payload()
    payload.ParseFromString(base64.b64decode(stored.payload.b64))
    assert payload.metadata["encoding"] == ENCODING
    inner = Payload()
    inner.ParseFromString(decrypt(payload.data, world.buyer.party.identity))
    assert json.loads(inner.data)["text"] == "DELIVER"


# --------------------------------------------------------------------------- the hash lock


async def test_an_edited_type_after_publish_refuses_to_start(world: World, tmp_path: Path) -> None:
    """Acceptance 17: publish a version, edit its input type, start a worker: it refuses,
    naming both hashes."""
    from kontra.serve import ServeError

    path = write_offering(tmp_path)
    published = seller.bind(path)
    await world.seller.gate.publish(published.document())
    edited = seller.bind_v2(path)
    ours, theirs = edited.contract_hash(), published.contract_hash()
    assert ours != theirs
    worker = Worker(offerings=[edited], activities=[shout])
    with pytest.raises(ServeError) as refused:
        async with world.mp.serve(worker, world.seller):
            pytest.fail("the worker started")
    message = str(refused.value)
    assert ours in message
    assert theirs in message
    assert "republish or bump the version" in message
    assert worker.inner_workers == ()
    # the worker already serving in this process kept its runtime
    from kontra import _runtime

    assert _runtime.current().party == "acme"


async def test_wait_ready_returns_when_the_worker_refuses_to_start(
    world: World, tmp_path: Path
) -> None:
    """Whoever waits for a worker to be ready learns that it never will be: a hash-lock
    refusal ends ``wait_ready()`` instead of leaving it waiting forever."""
    from kontra.serve import ServeError

    path = write_offering(tmp_path)
    await world.seller.gate.publish(seller.bind(path).document())
    worker = Worker(offerings=[seller.bind_v2(path)], activities=[shout])
    ready = asyncio.create_task(worker.wait_ready())
    await asyncio.sleep(0)  # already waiting when the worker starts
    with pytest.raises(ServeError, match="republish or bump the version"):
        await worker.run_async(session=world.seller.session, handle_signals=False)
    await asyncio.wait_for(ready, 5)
    assert not worker.serving


async def test_a_restarted_worker_asks_the_gate_for_the_buyers_key(world: World) -> None:
    """Decisions I7: the buyer key is cached per job by the handler; a worker that lost it
    (a restart, another process) gets it from the gate (``GET /v1/jobs/{id}/seller``) to
    encrypt the delivery -- never from the payload, never a fallback to plaintext."""
    value = {"text": "later", "hold": True}
    hired = await world.buyer.hire(world.entry, value)
    handle = await world.buyer.start(hired, value, result_type=Shout)
    root = world.mp.client("acme").get_workflow_handle(job_workflow_id(hired.job_id))
    for _ in range(200):
        try:
            await root.describe()
            break
        except RPCError:
            await asyncio.sleep(0.05)
    jobs = world.worker.jobs
    assert jobs is not None
    assert jobs.cached(hired.job_id) is not None
    jobs._entries = type(jobs._entries)(100)  # forget everything, as a restart would
    before = len(world.transport.requests)
    await root.signal("release")
    out = await handle.result()
    assert out.text == "LATER"
    assert f"GET /v1/jobs/{hired.job_id}/seller" in world.transport.paths(before)
    assert jobs.cached(hired.job_id) is not None
