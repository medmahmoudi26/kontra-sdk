"""``kontra._relay``: the gate's stream relay, the strict wire shape of decisions I30 (I31).

Every classification row against a scripted service (what it sends, what it waits, when it
stops), then over a dev server: the same update id re-sent while a poll waits (one
``UpdateAccepted`` per logical poll), a look-alike chain refused by ``first_execution_run_id``,
only the two calls, and no ``temporalio.contrib`` import."""

from __future__ import annotations

import asyncio
import base64
import json
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import Any

import pytest
from temporalio.api.common.v1 import Payload, Payloads
from temporalio.api.enums.v1 import EventType
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.api.update.v1 import Outcome
from temporalio.api.workflowservice.v1 import (
    QueryWorkflowRequest,
    QueryWorkflowResponse,
    UpdateWorkflowExecutionRequest,
    UpdateWorkflowExecutionResponse,
)
from temporalio.client import Client
from temporalio.contrib.workflow_streams import PollInput
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker

from kontra import _relay
from kontra.codec import stream as s
from kontra.codec.age import encrypt, generate_identity
from kontra.contract.names import (
    PROGRESS_TOPIC,
    STREAM_OFFSET_QUERY,
    STREAM_POLL_UPDATE,
    job_workflow_id,
    new_job_id,
)
from kontra.serve.interceptors import KontraInterceptor
from kontra.testing.devserver import DevServer

from .streamflows import LISTED, Burst, HostedJob, burst
from .test_stream_hosting import seller_client

JOB = new_job_id()
KEY = generate_identity()
CT = base64.b64encode(encrypt(b"sealed", [KEY[1]])).decode()


def envelope(items: list[dict[str, Any]], next_offset: int, more_ready: bool = False) -> Payload:
    env = {"v": 1, "items": items, "next_offset": next_offset, "more_ready": more_ready}
    return Payload(
        metadata={"encoding": s.STREAM_ENCODING, "kontra-v": b"1"}, data=json.dumps(env).encode()
    )


def success(p: Payload) -> UpdateWorkflowExecutionResponse:
    resp = UpdateWorkflowExecutionResponse(outcome=Outcome(success=Payloads(payloads=[p])))
    resp.update_ref.workflow_execution.run_id = "run-1"
    return resp


def failure(kind: str) -> UpdateWorkflowExecutionResponse:
    info = ApplicationFailureInfo(type=kind)
    return UpdateWorkflowExecutionResponse(
        outcome=Outcome(failure=Failure(message=kind, application_failure_info=info))
    )


def no_outcome() -> UpdateWorkflowExecutionResponse:
    return UpdateWorkflowExecutionResponse()  # the long poll returned at ACCEPTED


def rpc(status: RPCStatusCode) -> RPCError:
    return RPCError(status.name, status, b"")


Step = UpdateWorkflowExecutionResponse | RPCError


@dataclass
class Scripted:
    """A service that answers the relay's updates from a script, recording each request."""

    script: list[Step]
    offset: QueryWorkflowResponse | RPCError | None = None
    sent: list[UpdateWorkflowExecutionRequest] = field(
        default_factory=list[UpdateWorkflowExecutionRequest]
    )
    queries: list[QueryWorkflowRequest] = field(default_factory=list[QueryWorkflowRequest])
    calls: list[str] = field(default_factory=list[str])

    async def update_workflow_execution(
        self,
        req: UpdateWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Any = None,
        timeout: Any = None,  # noqa: ASYNC109 -- temporalio's signature
    ) -> UpdateWorkflowExecutionResponse:
        self.calls.append("UpdateWorkflowExecution")
        copy = UpdateWorkflowExecutionRequest()
        copy.CopyFrom(req)
        self.sent.append(copy)
        if not self.script:
            raise asyncio.CancelledError  # the script is over: end the test's iteration
        step = self.script.pop(0)
        if isinstance(step, RPCError):
            raise step
        return step

    async def query_workflow(
        self,
        req: QueryWorkflowRequest,
        retry: bool = False,
        metadata: Any = None,
        timeout: Any = None,  # noqa: ASYNC109 -- temporalio's signature
    ) -> QueryWorkflowResponse:
        self.calls.append("QueryWorkflow")
        self.queries.append(req)
        if isinstance(self.offset, RPCError):
            raise self.offset
        assert self.offset is not None
        return self.offset

    def ids(self) -> list[str]:
        return [r.request.meta.update_id for r in self.sent]

    def offsets(self) -> list[int]:
        return [json.loads(r.request.input.args.payloads[0].data)["from_offset"] for r in self.sent]


@pytest.fixture
def waits(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Every wait the relay makes, without waiting."""
    seen: list[float] = []

    async def sleep(delay: float) -> None:
        seen.append(delay)

    monkeypatch.setattr(_relay, "_sleep", sleep)
    return seen


def closes_after(n: int) -> Callable[[], Any]:
    """``closed()`` that says open ``n`` times, then closed."""
    asked = [0]

    async def closed() -> bool:
        asked[0] += 1
        return asked[0] > n

    return closed


async def never() -> bool:
    return False


async def run(service: Scripted, **kw: Any) -> tuple[list[_relay.RelayBatch], BaseException | None]:
    kw.setdefault("closed", never)
    kw.setdefault("start", 0)
    kw.setdefault("topics", [LISTED, PROGRESS_TOPIC])
    kw.setdefault("min_interval", 0.0)
    batches: list[_relay.RelayBatch] = []
    try:
        async for batch in _relay.relay(service, namespace="acme", job_id=JOB, **kw):
            batches.append(batch)
    except (asyncio.CancelledError, _relay.RelayStopped) as e:
        return batches, e
    return batches, None


ITEM = {"topic": LISTED, "offset": 0, "ct": CT}


async def test_the_poll_request_is_the_strict_shape(waits: list[float]) -> None:
    service = Scripted([success(envelope([ITEM], 1))])
    await run(service, first_execution_run_id=lambda: "first-run")
    req = service.sent[0]
    assert req.namespace == "acme"
    assert req.workflow_execution.workflow_id == job_workflow_id(JOB)
    assert req.workflow_execution.run_id == ""
    assert req.first_execution_run_id == "first-run"
    assert req.request.input.name == STREAM_POLL_UPDATE
    assert req.wait_policy.lifecycle_stage == 3  # COMPLETED
    assert req.request.meta.identity == "kontra-relay"
    [arg] = req.request.input.args.payloads
    assert json.loads(arg.data) == {"topics": sorted([LISTED, PROGRESS_TOPIC]), "from_offset": 0}


def test_the_poll_input_is_the_contribs_bytes() -> None:
    """Golden: what the relay builds without the contrib is what temporalio's converter
    writes for the contrib's PollInput."""
    for topics, offset in (([LISTED, PROGRESS_TOPIC], 0), (["a"], 12345)):
        [theirs] = DataConverter.default.payload_converter.to_payloads([PollInput(topics, offset)])
        assert _relay.poll_input_payload(topics, offset) == theirs


async def test_the_same_request_is_sent_again_until_it_has_an_outcome(waits: list[float]) -> None:
    service = Scripted(
        [
            no_outcome(),
            rpc(RPCStatusCode.DEADLINE_EXCEEDED),
            rpc(RPCStatusCode.CANCELLED),
            success(envelope([ITEM], 1)),
            success(envelope([], 1)),
        ]
    )
    batches, _ = await run(service)
    ids = service.ids()
    assert ids[0] == ids[1] == ids[2] == ids[3]  # one logical poll
    assert ids[4] != ids[3]  # the next one
    assert service.offsets()[:5] == [0, 0, 0, 0, 1]
    assert [b.next_offset for b in batches] == [1, 1]
    assert waits == []  # re-sends wait for nothing: the server held each one


async def test_a_waiting_poll_stops_when_the_job_closes(waits: list[float]) -> None:
    service = Scripted([no_outcome(), rpc(RPCStatusCode.DEADLINE_EXCEEDED), no_outcome()])
    batches, stopped = await run(service, closed=closes_after(1))
    assert (batches, stopped) == ([], None)
    assert len(service.sent) == 2


@pytest.mark.parametrize(
    ("kind", "offsets", "cooldown"),
    [("StreamDraining", [5, 5], 0.2), ("TruncatedOffset", [5, 0], None), ("AcceptedUpdateCompletedWorkflow", [5, 5], 0.2)],
)  # fmt: skip
async def test_outcome_failures_poll_again_with_a_new_id(
    kind: str, offsets: list[int], cooldown: float | None, waits: list[float]
) -> None:
    service = Scripted([failure(kind), success(envelope([], 9))])
    await run(service, start=5)
    assert service.offsets()[:2] == offsets
    assert service.ids()[0] != service.ids()[1]
    if cooldown is not None:
        assert waits[0] == cooldown


async def test_truncated_at_offset_0_is_a_refusal_not_a_spin(waits: list[float]) -> None:
    """A stream never answers offset 0 as truncated: a worker that does (for free: a refused
    update costs it no history) is backed off from like any other refusal, not polled again
    at once; the first ``TruncatedOffset`` past 0 still re-reads from the base at once."""
    truncated: list[Step] = [failure("TruncatedOffset")] * 6
    service = Scripted([*truncated, success(envelope([ITEM], 1))])
    batches, _ = await run(service, start=5)
    assert service.offsets()[:7] == [5, 0, 0, 0, 0, 0, 0]
    assert waits[:5] == [0.5, 1.0, 2.0, 4.0, 8.0]
    assert batches[0].items[0].offset == 0
    # A successful answer resets the back-off.
    service = Scripted(
        [failure("TruncatedOffset"), success(envelope([], 0)), failure("TruncatedOffset")]
    )
    waits.clear()
    await run(service)
    assert waits == [0.5, 0.5]


async def test_a_workflow_that_completed_under_an_accepted_poll_ends_the_relay_when_closed(
    waits: list[float],
) -> None:
    service = Scripted([failure("AcceptedUpdateCompletedWorkflow")])
    batches, stopped = await run(service, closed=closes_after(0))
    assert (batches, stopped) == ([], None)


async def test_the_jobs_history_budget_stops_the_relay(waits: list[float]) -> None:
    service = Scripted([failure(_relay.STREAM_FULL)])
    _, stopped = await run(service)
    assert isinstance(stopped, _relay.RelayStopped)
    assert stopped.reason == "history_budget"


async def test_any_other_rejection_backs_off_while_the_job_is_open(waits: list[float]) -> None:
    rejected: list[Step] = [failure("NoHandler")] * 8
    service = Scripted([*rejected, success(envelope([ITEM], 1))])
    batches, _ = await run(service)
    assert waits[:8] == [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]
    assert len(set(service.ids()[:9])) == 9
    assert batches[0].items[0].offset == 0
    service = Scripted([failure("NoHandler")])
    assert await run(service, closed=closes_after(0)) == ([], None)


@pytest.mark.parametrize("status", [RPCStatusCode.NOT_FOUND, RPCStatusCode.FAILED_PRECONDITION])
async def test_a_job_that_is_gone_stops_or_backs_off(
    status: RPCStatusCode, waits: list[float]
) -> None:
    service = Scripted([rpc(status)])
    assert await run(service, closed=closes_after(0)) == ([], None)
    service = Scripted([rpc(status), rpc(status), success(envelope([ITEM], 1))])
    batches, _ = await run(service)
    assert waits[:2] == [0.5, 1.0]
    assert len(set(service.ids()[:3])) == 3  # not admitted: a new logical poll each time
    assert len(batches) == 1


async def test_resource_exhausted_backs_off_to_a_minute(waits: list[float]) -> None:
    exhausted: list[Step] = [rpc(RPCStatusCode.RESOURCE_EXHAUSTED)] * 9
    service = Scripted([*exhausted, success(envelope([], 0))])
    await run(service)
    assert waits[:9] == [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]
    assert len(set(service.ids()[:10])) == 1  # the same request: it may have been admitted


@pytest.mark.parametrize("status", [RPCStatusCode.PERMISSION_DENIED, RPCStatusCode.UNAUTHENTICATED])
async def test_a_refused_token_stops_the_relay(status: RPCStatusCode, waits: list[float]) -> None:
    _, stopped = await run(Scripted([rpc(status)]))
    assert isinstance(stopped, _relay.RelayStopped)
    assert stopped.reason == "denied"


@pytest.mark.parametrize(
    "answer",
    [
        Payload(metadata={"encoding": b"binary/kontra-age", "kontra-v": b"1"}, data=b"age-encryption.org/v1\n..."),
        Payload(metadata={"encoding": b"json/plain"}, data=b'{"items": [], "next_offset": 0, "more_ready": false}'),
        envelope([{"topic": LISTED, "offset": 0, "ct": base64.b64encode(b'{"plain": true}').decode()}], 1),
        Payload(metadata={"encoding": s.STREAM_ENCODING, "kontra-v": b"1"}, data=b"[" * 100_000),
    ],
    ids=["whole-ciphertext", "plaintext", "plaintext-item", "nested-past-the-stack"],
)  # fmt: skip
async def test_an_answer_that_is_not_an_envelope_stops_the_relay(
    answer: Payload, waits: list[float]
) -> None:
    """A worker without #52 answers whole ciphertext, a seller who replaced the handler
    anything: the relay stores nothing of it."""
    batches, stopped = await run(Scripted([success(answer)]))
    assert batches == []
    assert isinstance(stopped, _relay.RelayStopped)
    assert stopped.reason == "protocol"


async def test_unlisted_and_unasked_items_are_dropped(waits: list[float]) -> None:
    items = [
        {"topic": LISTED, "offset": 0, "ct": CT},
        {"topic": None, "offset": 1, "ct": CT},
        {"topic": "other", "offset": 2, "ct": CT},
        {"topic": PROGRESS_TOPIC, "offset": 3, "size": 300_000, "ct": None},
    ]
    [batch], _ = await run(Scripted([success(envelope(items, 4))]))
    assert [(i.topic, i.offset, i.ciphertext is None) for i in batch.items] == [
        (LISTED, 0, False),
        (PROGRESS_TOPIC, 3, True),
    ]
    assert batch.items[1].size == 300_000
    assert batch.dropped == 2
    assert batch.run_id == "run-1"


async def test_pacing(waits: list[float]) -> None:
    paced: list[float | None] = [10.0, None]

    async def pace() -> float | None:
        return paced.pop(0)

    service = Scripted(
        [
            success(envelope([ITEM], 1, more_ready=True)),  # at once
            success(envelope([], 1)),  # then max(min_interval, pace) = 10
            success(envelope([], 1)),  # pace says stop
        ]
    )
    _, stopped = await run(service, pace=pace, min_interval=2.0)
    assert len(waits) == 1
    assert waits[0] == 10.0
    assert isinstance(stopped, _relay.RelayStopped)
    assert stopped.reason == "history_budget"
    service = Scripted([success(envelope([], 1)), success(envelope([], 1))])
    await run(service, min_interval=2.0)
    assert 1.9 < waits[1] <= 2.0  # min_interval less what the poll took


def test_pace_for() -> None:
    limits = {"events_limit": 20_480, "bytes_limit": 20 * 1024 * 1024}
    assert _relay.pace_for(0, 0, **limits) == _relay.MIN_INTERVAL
    assert _relay.pace_for(5_119, 0, **limits) == _relay.MIN_INTERVAL
    assert _relay.pace_for(5_120, 0, **limits) == 10.0
    assert _relay.pace_for(0, 9 * 1024 * 1024, **limits) == 60.0
    assert _relay.pace_for(10_240, 0, **limits) is None
    assert _relay.pace_for(0, 10 * 1024 * 1024, **limits) is None


async def test_a_tail_start_reads_the_marked_offset(waits: list[float]) -> None:
    answer = QueryWorkflowResponse(query_result=Payloads(payloads=[s.offset_payload(42)]))
    service = Scripted([success(envelope([], 42))], offset=answer)
    await run(service, start="tail")
    assert service.queries[0].query.query_type == STREAM_OFFSET_QUERY
    assert service.queries[0].execution.workflow_id == job_workflow_id(JOB)
    assert service.offsets()[0] == 42
    unmarked = QueryWorkflowResponse(
        query_result=Payloads(payloads=[Payload(metadata={"encoding": b"json/plain"}, data=b"42")])
    )
    _, stopped = await run(Scripted([], offset=unmarked), start="tail")
    assert isinstance(stopped, _relay.RelayStopped)
    assert stopped.reason == "protocol"


async def test_the_relay_names_its_topics() -> None:
    for bad in ([], ["bad topic"], "events"):
        with pytest.raises((ValueError, TypeError)):
            await run(Scripted([]), topics=bad)
    with pytest.raises(ValueError, match="start"):
        await run(Scripted([]), start=-1)


async def test_the_start_is_the_callers_to_say() -> None:
    """The gate relays a job from offset 0 (owner, OD2); ``"tail"`` stays available, but a
    gate that says nothing gets an error, not the tail start the owner rejected."""
    import inspect

    assert inspect.signature(_relay.relay).parameters["start"].default is inspect.Parameter.empty
    with pytest.raises(TypeError, match="start"):
        async for _ in _relay.relay(  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]
            Scripted([]), namespace="acme", job_id=JOB, topics=[LISTED], closed=never
        ):
            pass


def test_the_relay_imports_no_contrib() -> None:
    code = (
        "import sys, kontra._relay; "
        "bad = sorted(m for m in sys.modules if m.startswith('temporalio.contrib')); "
        "print(bad); sys.exit(1 if bad else 0)"
    )
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert done.returncode == 0, done.stdout + done.stderr


# --------------------------------------------------------------------------- over the wire


class Recording:
    """The plain client's service, recording every call's name and status."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.calls: list[tuple[str, str]] = []
        self.ids: list[str] = []

    async def update_workflow_execution(self, req: Any, retry: bool = False, metadata: Any = None, timeout: Any = None) -> Any:  # noqa: ASYNC109  # fmt: skip
        return await self._call(
            "UpdateWorkflowExecution", self.inner.update_workflow_execution, req, timeout
        )

    async def query_workflow(self, req: Any, retry: bool = False, metadata: Any = None, timeout: Any = None) -> Any:  # noqa: ASYNC109  # fmt: skip
        return await self._call("QueryWorkflow", self.inner.query_workflow, req, timeout)

    async def _call(self, name: str, call: Any, req: Any, timeout: Any) -> Any:  # noqa: ASYNC109
        if name == "UpdateWorkflowExecution":
            self.ids.append(req.request.meta.update_id)
        try:
            resp = await call(req, retry=False, timeout=timeout)
        except RPCError as e:
            self.calls.append((name, e.status.name))
            raise
        self.calls.append((name, "OK"))
        return resp


@dataclass
class Wire:
    seller: Client
    plain: Client
    queue: str


@pytest.fixture
async def wire(dev_server: DevServer) -> AsyncIterator[Wire]:
    queue = f"relay-{uuid.uuid4().hex[:8]}"
    seller = seller_client(dev_server)
    async with Worker(
        seller,
        task_queue=queue,
        workflows=[HostedJob],
        activities=[burst],
        interceptors=[KontraInterceptor(None, streams={queue: [LISTED]})],
    ):
        yield Wire(seller, dev_server.client, queue)


@pytest.mark.temporal
async def test_a_waiting_poll_is_one_update_however_often_it_is_resent(wire: Wire) -> None:
    job = new_job_id()
    handle = await wire.seller.start_workflow(
        HostedJob.run, Burst(1, topics=(LISTED,)), id=job_workflow_id(job), task_queue=wire.queue
    )
    service = Recording(wire.plain.workflow_service)
    got: list[_relay.RelayItem] = []
    first = asyncio.Event()

    async def read() -> None:
        async for batch in _relay.relay(
            service, namespace="default", job_id=job, topics=[LISTED], start=0,
            closed=never, min_interval=0.0, rpc_timeout=1.0,
        ):  # fmt: skip
            got.extend(batch.items)
            first.set()

    task = asyncio.create_task(read())
    try:
        async with asyncio.timeout(30):
            await first.wait()
        await asyncio.sleep(3.5)  # the second poll waits: re-sent with its own id
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    await handle.signal(HostedJob.finish)
    await handle.result()
    # The server answers a short deadline at ACCEPTED (no outcome) rather than let it pass:
    # either way, the same update id goes again.
    waiting = service.ids[1]
    assert service.ids.count(waiting) >= 3
    assert len(set(service.ids)) == 2
    assert {name for name, _ in service.calls} == {"UpdateWorkflowExecution"}
    history = await wire.plain.get_workflow_handle(job_workflow_id(job)).fetch_history()
    accepted = [
        e for e in history.events
        if e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED
    ]  # fmt: skip
    assert len(accepted) == 2  # the answered poll and the waiting one, never one per re-send


@pytest.mark.temporal
async def test_a_look_alike_chain_is_refused_by_the_first_run_id(wire: Wire) -> None:
    job = new_job_id()
    handle = await wire.seller.start_workflow(
        HostedJob.run, Burst(1, topics=(LISTED,)), id=job_workflow_id(job), task_queue=wire.queue
    )
    service = Recording(wire.plain.workflow_service)
    batches = [
        b
        async for b in _relay.relay(
            service, namespace="default", job_id=job, topics=[LISTED], start=0,
            closed=closes_after(0), first_execution_run_id=lambda: str(uuid.uuid4()),
        )
    ]  # fmt: skip
    assert batches == []
    assert service.calls == [("UpdateWorkflowExecution", "NOT_FOUND")]
    pinned = handle.first_execution_run_id or ""
    assert pinned
    async with asyncio.timeout(30):
        async for batch in _relay.relay(
            service, namespace="default", job_id=job, topics=[LISTED], start=0,
            closed=never, first_execution_run_id=lambda: pinned, min_interval=0.0,
        ):  # fmt: skip
            assert [i.offset for i in batch.items] == [0]
            break
    await handle.signal(HostedJob.finish)
    assert await handle.result() == "ok"
