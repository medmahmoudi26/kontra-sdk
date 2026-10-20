"""The Temporal adapter without a server (seam ⑥): its handle (a golden, and the strict parse),
each member as the exact control-plane call it makes, the claims of the credentials it mints,
what it withholds (an unsealed delivery), how failures map, and the stream relay mapped onto
``kontra._relay``. The same adapter passes the engine contract on a dev server
(``test_temporal_engine_contract.py``) and, over the wire, on Kontra's own server
(``tests/temporal``)."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from temporalio.api.common.v1 import Payload, Payloads
from temporalio.api.update.v1 import Outcome
from temporalio.api.workflow.v1 import WorkflowExecutionInfo
from temporalio.api.workflowservice.v1 import (
    DescribeWorkflowExecutionResponse,
    UpdateWorkflowExecutionResponse,
)

from kontra._control import (
    BuyerCall,
    BuyerLookupError,
    EndpointConflict,
    EndpointInfo,
    HistorySummary,
    NexusSyncKind,
    NexusSyncResult,
    Occupation,
    TemporalError,
)
from kontra._control import ObservationIncomplete as ChainIncomplete
from kontra.codec import stream as s
from kontra.codec.age import encrypt, generate_identity
from kontra.contract.names import OfferingId, job_workflow_id, relay_subject
from kontra.engine import (
    AGE_MAGIC,
    EngineError,
    Fenced,
    InvalidHandle,
    JobHandle,
    JobPlan,
    ObservationIncomplete,
    PartyLookupError,
    Pin,
    QuoteKind,
    RouteConflict,
    Stop,
    StreamStopped,
)
from kontra.engine.temporal import AUD_TEMPORAL, HANDLE_PREFIX, TemporalEngine, worker_token
from kontra.escrow import Observation, ObservedStatus, Provenance
from kontra.gate_client import models as m

JOB = "0123456789abcdef0123456789abcdef"
ROUTE = "k" + "a" * 32
ACME = OfferingId.parse("acme/enrich@1.2.0")
PLAN = JobPlan(JOB, ACME, ROUTE, "bob", "acme")
NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
#: The handle of PLAN, byte for byte: a change here changes every job row (kontra-cloud#45).
GOLDEN = (
    b'temporal/1\n{"buyer":"bob","job":"0123456789abcdef0123456789abcdef",'
    b'"offering":"acme/enrich@1.2.0","route":"kaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    b'"seller":"acme"}'
)


class Signer:
    def __init__(self) -> None:
        self.key = ec.generate_private_key(ec.SECP256R1())

    @property
    def issuer(self) -> str:
        return "https://gate.test"

    def sign(self, claims: Mapping[str, Any]) -> str:
        return jwt.encode(dict(claims), self.key, algorithm="ES256")

    def claims(self, token: str) -> dict[str, Any]:
        return jwt.decode(
            token,
            self.key.public_key(),
            algorithms=["ES256"],
            audience=AUD_TEMPORAL,
            options={"verify_exp": False, "verify_iat": False},  # the tests' own clock
        )


def sealed_result(text: bytes = b"ciphertext") -> bytes:
    return Payload(
        metadata={"encoding": b"binary/kontra-age", "kontra-v": b"1"}, data=AGE_MAGIC + text
    ).SerializeToString(deterministic=True)


@dataclass
class Recorder:
    """A TemporalControl that records each call and answers from what a test sets."""

    calls: list[tuple[Any, ...]] = field(default_factory=list[tuple[Any, ...]])
    observation: tuple[Observation, bytes | None] = (Observation(ObservedStatus.NOT_FOUND), None)
    summary: HistorySummary | None = None
    sync: NexusSyncResult = field(
        default_factory=lambda: NexusSyncResult(NexusSyncKind.UNAVAILABLE)
    )
    occupation: Occupation = Occupation.OCCUPIED
    fail: Exception | None = None

    def _call(self, *call: Any) -> None:
        self.calls.append(call)
        if self.fail is not None:
            raise self.fail

    async def ensure_namespace(self, namespace: str, retention: timedelta = timedelta(0)) -> None:
        self._call("ensure_namespace", namespace)

    async def find_endpoint(self, name: str) -> EndpointInfo | None:
        self._call("find_endpoint", name)
        return None

    async def create_endpoint(self, name: str, namespace: str, task_queue: str) -> EndpointInfo:
        self._call("create_endpoint", name, namespace, task_queue)
        return EndpointInfo("id", name, namespace, task_queue)

    async def nexus_sync(
        self,
        namespace: str,
        task_queue: str,
        service: str,
        operation: str,
        payload: Payload,
        headers: Mapping[str, str],
        timeout: float,  # noqa: ASYNC109
    ) -> NexusSyncResult:
        self._call(
            "nexus_sync", namespace, task_queue, service, operation, payload, dict(headers), timeout
        )
        return self.sync

    async def observe(
        self, namespace: str, workflow_id: str, call: BuyerCall, pinned: str | None
    ) -> tuple[Observation, bytes | None]:
        self._call("observe", namespace, workflow_id, call, pinned)
        return self.observation

    async def history_summary(
        self, namespace: str, workflow_id: str, pinned: str | None = None
    ) -> HistorySummary | None:
        self._call("history_summary", namespace, workflow_id, pinned)
        return self.summary

    async def cancel_workflow(
        self, namespace: str, workflow_id: str, reason: str, first_run_id: str = ""
    ) -> None:
        self._call("cancel_workflow", namespace, workflow_id, reason, first_run_id)

    async def terminate_workflow(
        self, namespace: str, workflow_id: str, reason: str, first_run_id: str = ""
    ) -> None:
        self._call("terminate_workflow", namespace, workflow_id, reason, first_run_id)

    async def occupy_job(self, namespace: str, workflow_id: str, reason: str) -> Occupation:
        self._call("occupy_job", namespace, workflow_id, reason)
        return self.occupation

    async def describe_task_queue(
        self, namespace: str, task_queue: str
    ) -> dict[m.TaskQueueType, m.TaskQueueTypeInfo]:
        self._call("describe_task_queue", namespace, task_queue)
        return {}

    async def start_operation(
        self,
        namespace: str,
        operation_id: str,
        endpoint: str,
        service: str,
        operation: str,
        payload: Payload,
        headers: Mapping[str, str],
        schedule_to_close: timedelta,
    ) -> None:
        self._call(
            "start_operation",
            namespace,
            operation_id,
            endpoint,
            service,
            operation,
            payload,
            dict(headers),
            schedule_to_close,
        )


def engine(control: Recorder | None = None, **kw: Any) -> tuple[TemporalEngine, Recorder, Signer]:
    control = control or Recorder()
    signer = Signer()
    kw.setdefault("clock", lambda: NOW)
    return TemporalEngine(control, signer=signer, address="frontend:7233", **kw), control, signer


# --------------------------------------------------------------------------- the handle


def test_the_handle_is_its_golden() -> None:
    e, control, _ = engine()
    assert e.bind(PLAN) == GOLDEN
    assert control.calls == []  # pure


@pytest.mark.parametrize(
    "bad",
    [
        GOLDEN[:-1],
        GOLDEN + b" ",
        b" " + GOLDEN,
        GOLDEN.replace(b"temporal/1", b"temporal/2"),
        GOLDEN.replace(b'","', b'", "', 1),  # not canonical
        GOLDEN.replace(b'"buyer":"bob",', b""),  # a key missing
        GOLDEN[:-1] + b',"extra":"x"}',  # a key too many
        GOLDEN.replace(b'"buyer":"bob"', b'"buyer":"bob","buyer":"eve"'),  # a key twice
        GOLDEN.replace(b'"bob"', b'"Bob"'),  # not a handle
        GOLDEN.replace(b'"seller":"acme"', b'"seller":"other"'),  # not the offering's party
        GOLDEN.replace(b"@1.2.0", b"@1.2"),  # not an exact version
        GOLDEN.replace(b"@1.2.0", b"@01.2.0"),  # not the offering's own spelling
        GOLDEN.replace(ROUTE.encode(), b"k" + b"A" * 32),  # not a route this catalog assigns
        GOLDEN.replace(JOB.encode(), b"job-" + JOB.encode()[4:]),  # not a job id
        GOLDEN.replace(b'"bob"', b"7"),  # not a string
        HANDLE_PREFIX + b"[]",
        HANDLE_PREFIX + b"not json",
        HANDLE_PREFIX + b'{"buyer":"bob"' + b" " * 2000 + b"}",  # too large
        b"",
    ],
)
async def test_a_handle_is_parsed_strictly_before_any_io(bad: bytes) -> None:
    e, control, _ = engine()
    with pytest.raises(InvalidHandle):
        await e.observe(JobHandle(bad), pin=None, holds=())
    with pytest.raises(InvalidHandle):
        await e.cancel(JobHandle(bad), stop=Stop.END, pin=None, reason="x")
    assert control.calls == []


def test_bind_refuses_a_plan_it_cannot_run() -> None:
    e, _, _ = engine()
    with pytest.raises(ValueError, match="route"):
        e.bind(JobPlan(JOB, ACME, "not-a-route", "bob", "acme"))


# --------------------------------------------------------------------------- the members


async def test_isolate_provisions_once_and_mints_a_worker_token() -> None:
    e, control, signer = engine()
    first = await e.isolate("acme")
    again = await e.isolate("acme")
    assert (first.home, first.address, first.credential) == ("acme", "frontend:7233", None)
    assert again == first
    assert control.calls == [("ensure_namespace", "acme")]  # once per process
    iso = await e.isolate("acme", session="sid-1")
    assert iso.credential is not None
    assert iso.expires_at == NOW + timedelta(minutes=15)
    claims = signer.claims(iso.credential)
    assert claims == {
        "iss": "https://gate.test",
        "aud": AUD_TEMPORAL,
        "sub": "acme",
        "sid": "sid-1",
        "permissions": ["acme:worker"],
        "iat": int(NOW.timestamp()),
        "exp": int((NOW + timedelta(minutes=15)).timestamp()),
    }
    token, expires = worker_token(signer, "acme", "sid-1", now=NOW)
    assert (signer.claims(token), expires) == (claims, iso.expires_at)
    with pytest.raises(ValueError, match="party handle"):
        await e.isolate("Not a handle")


async def test_expose_is_the_nexus_endpoint() -> None:
    e, control, _ = engine()
    await e.expose(ACME, ROUTE)
    assert control.calls == [("create_endpoint", ROUTE, "acme", "acme/enrich@1.2.0")]
    control.fail = EndpointConflict(ROUTE)
    with pytest.raises(RouteConflict):
        await e.expose(ACME, ROUTE)


async def test_quote_is_a_sync_nexus_call_and_relays_only_the_price() -> None:
    e, control, _ = engine()
    raw = Payload(metadata={"encoding": b"binary/kontra-age"}, data=b"buyer's input")
    doc = b'{"status":"ok"}'
    control.sync = NexusSyncResult(
        NexusSyncKind.OK, Payload(metadata={"encoding": b"json/plain"}, data=doc)
    )
    answer = await e.quote(ACME, raw.SerializeToString(), timeout=4.0)
    assert (answer.kind, answer.document) == (QuoteKind.OK, doc)
    assert control.calls == [
        ("nexus_sync", "acme", "acme/enrich@1.2.0", "acme.enrich", "quote", raw, {}, 4.0)
    ]
    control.sync = NexusSyncResult(NexusSyncKind.OK, Payload(metadata={"encoding": b"binary/x"}))
    assert (await e.quote(ACME, b"", timeout=1.0)).document is None  # not a plaintext price
    for kind, want in [
        (NexusSyncKind.HANDLER_ERROR, QuoteKind.REFUSED),
        (NexusSyncKind.TIMEOUT, QuoteKind.TIMEOUT),
        (NexusSyncKind.UNAVAILABLE, QuoteKind.UNAVAILABLE),
    ]:
        control.sync = NexusSyncResult(kind, message="why")
        answer = await e.quote(ACME, b"", timeout=1.0)
        assert (answer.kind, answer.message) == (want, "why")
    with pytest.raises(ValueError, match="Payload"):
        await e.quote(ACME, b"\xff\xff", timeout=1.0)


async def test_start_is_a_standalone_operation_in_the_buyers_namespace() -> None:
    e, control, _ = engine()
    raw = Payload(metadata={"encoding": b"binary/kontra-age"}, data=b"input")
    await e.start(
        GOLDEN, raw.SerializeToString(), hold="hold.token", deadline=NOW + timedelta(hours=1)
    )
    assert control.calls == [
        (
            "start_operation",
            "bob",
            job_workflow_id(JOB),
            ROUTE,
            "acme.enrich",
            "execute",
            raw,
            {"kontra-hold": "hold.token"},
            timedelta(hours=1),
        )
    ]
    control.calls.clear()
    await e.start(GOLDEN, b"", hold="h", deadline=NOW - timedelta(hours=1))
    assert control.calls[0][-1] == timedelta(seconds=1)  # never zero or negative


async def test_observe_reads_the_sellers_run_against_the_buyers_call() -> None:
    e, control, _ = engine()
    data = sealed_result()
    control.observation = (
        Observation(ObservedStatus.COMPLETED, provenance=Provenance.VERIFIED, pin="run-1"),
        data,
    )
    seen = await e.observe(GOLDEN, pin=Pin("run-1"), holds={"d1", "d2"})
    call = BuyerCall("bob", ROUTE, "acme.enrich", frozenset({"d1", "d2"}))
    assert control.calls == [("observe", "acme", job_workflow_id(JOB), call, "run-1")]
    assert seen.result is not None
    assert seen.result.data == data
    o = seen.observation
    assert (o.result_sha256, o.result_size) == (sha256(data).hexdigest(), len(data))


async def test_an_unsealed_delivery_is_withheld() -> None:
    """Owner decision OD3: a verified completion whose result is not the codec's envelope is
    UNSEALED, its bytes dropped here: the gate never holds them."""
    e, control, _ = engine()
    plain = Payload(metadata={"encoding": b"json/plain"}, data=b'"secret"').SerializeToString()
    control.observation = (
        Observation(
            ObservedStatus.COMPLETED,
            provenance=Provenance.VERIFIED,
            pin="run-1",
            result_sha256=sha256(plain).hexdigest(),
            result_size=len(plain),
        ),
        plain,
    )
    seen = await e.observe(GOLDEN, pin=None, holds=())
    assert seen.result is None
    o = seen.observation
    assert (o.status, o.result_sha256, o.result_size, o.pin) == (
        ObservedStatus.UNSEALED,
        None,
        None,
        "run-1",
    )


async def test_only_a_verified_completion_hands_over_bytes() -> None:
    e, control, _ = engine()
    for status, provenance in [
        (ObservedStatus.COMPLETED, Provenance.NOT_FROM_BUYER),
        (ObservedStatus.RUNNING, Provenance.VERIFIED),
    ]:
        control.observation = (Observation(status, provenance=provenance, pin="r"), sealed_result())
        seen = await e.observe(GOLDEN, pin=None, holds=())
        assert (seen.result, seen.observation.status) == (None, status)


@pytest.mark.parametrize(
    ("failure", "raised"),
    [
        (BuyerLookupError("busy"), PartyLookupError),
        (ChainIncomplete("long"), ObservationIncomplete),
        (TemporalError("down"), EngineError),
        (RuntimeError("anything"), EngineError),
    ],
)
async def test_cannot_say_is_an_exception_never_a_status(
    failure: Exception, raised: type[Exception]
) -> None:
    e, control, _ = engine()
    control.fail = failure
    with pytest.raises(raised):
        await e.observe(GOLDEN, pin=None, holds=())
    for call in (
        e.isolate("acme"),
        e.expose(ACME, ROUTE),
        e.quote(ACME, b"", timeout=1),
        e.start(GOLDEN, b"", hold="h", deadline=NOW),
        e.liveness(GOLDEN, pin=Pin("r")),
        e.cancel(GOLDEN, stop=Stop.ASK, pin=None, reason="x"),
        e.fence(GOLDEN, reason="x"),
        e.workers(ACME),
    ):
        with pytest.raises(EngineError):
            await call


async def test_liveness_is_the_pinned_chains_history_summary() -> None:
    e, control, _ = engine()
    assert await e.liveness(GOLDEN, pin=Pin("run-1")) is None
    control.summary = HistorySummary(
        ObservedStatus.RUNNING, "ActivityTaskScheduled", 3, ("fetch",), 2
    )
    live = await e.liveness(GOLDEN, pin=Pin("run-1"))
    assert live is not None
    assert (live.state, live.last_step, live.steps_done, live.steps_running, live.restarts) == (
        ObservedStatus.RUNNING,
        "ActivityTaskScheduled",
        3,
        ("fetch",),
        2,
    )
    assert control.calls[-1] == ("history_summary", "acme", job_workflow_id(JOB), "run-1")


async def test_cancel_maps_ask_and_end_inside_the_pin() -> None:
    e, control, _ = engine()
    wf = job_workflow_id(JOB)
    await e.cancel(GOLDEN, stop=Stop.ASK, pin=None, reason="a")
    await e.cancel(GOLDEN, stop=Stop.ASK, pin=Pin("run-1"), reason="b")
    await e.cancel(GOLDEN, stop=Stop.END, pin=None, reason="c")
    await e.cancel(GOLDEN, stop=Stop.END, pin=Pin("run-1"), reason="d")
    assert control.calls == [
        ("cancel_workflow", "acme", wf, "a", ""),
        ("cancel_workflow", "acme", wf, "b", "run-1"),
        ("terminate_workflow", "acme", wf, "c", ""),
        ("terminate_workflow", "acme", wf, "d", "run-1"),
    ]


async def test_fence_is_the_placeholder() -> None:
    e, control, _ = engine()
    assert await e.fence(GOLDEN, reason="cancel") is Fenced.CLOSED
    control.occupation = Occupation.STARTED
    assert await e.fence(GOLDEN, reason="cancel") is Fenced.STARTED
    assert control.calls[0] == ("occupy_job", "acme", job_workflow_id(JOB), "cancel")


async def test_workers_is_the_task_queues_view() -> None:
    e, control, _ = engine()
    assert await e.workers(ACME) == {}
    assert control.calls == [("describe_task_queue", "acme", "acme/enrich@1.2.0")]


# --------------------------------------------------------------------------- the stream


@dataclass
class Frontend:
    """The relay's two calls and the observer's describe, answered from a log of items."""

    log: list[tuple[str | None, bytes]]
    polls: list[tuple[list[str], int, str]] = field(
        default_factory=list[tuple[list[str], int, str]]
    )
    history: tuple[int, int] = (10, 1000)
    failure: str | None = None

    @property
    def workflow_service(self) -> Frontend:
        return self

    async def update_workflow_execution(self, req: Any, retry: bool = False, metadata: Any = None, timeout: Any = None) -> UpdateWorkflowExecutionResponse:  # noqa: ASYNC109  # fmt: skip
        args = json.loads(req.request.input.args.payloads[0].data)
        self.polls.append((args["topics"], args["from_offset"], req.first_execution_run_id))
        if self.failure is not None:
            from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure

            info = ApplicationFailureInfo(type=self.failure)
            return UpdateWorkflowExecutionResponse(
                outcome=Outcome(failure=Failure(message="no", application_failure_info=info))
            )
        if args["from_offset"] >= len(self.log):
            return UpdateWorkflowExecutionResponse()  # the long poll returned at ACCEPTED
        items: list[dict[str, Any]] = []
        for i, (topic, ct) in enumerate(self.log):
            if i >= args["from_offset"] and (topic is None or topic in args["topics"]):
                items.append({"topic": topic, "offset": i, "ct": base64.b64encode(ct).decode()})
        env = {"v": 1, "items": items, "next_offset": len(self.log), "more_ready": False}
        p = Payload(
            metadata={"encoding": s.STREAM_ENCODING, "kontra-v": b"1"},
            data=json.dumps(env).encode(),
        )
        resp = UpdateWorkflowExecutionResponse(outcome=Outcome(success=Payloads(payloads=[p])))
        resp.update_ref.workflow_execution.run_id = "run-1"
        return resp

    async def describe_workflow_execution(self, req: Any, retry: bool = False, metadata: Any = None, timeout: Any = None) -> DescribeWorkflowExecutionResponse:  # noqa: ASYNC109  # fmt: skip
        length, size = self.history
        info = WorkflowExecutionInfo(history_length=length, history_size_bytes=size)
        return DescribeWorkflowExecutionResponse(workflow_execution_info=info)


class WithClient(Recorder):
    def __init__(self, frontend: Frontend) -> None:
        super().__init__()
        self.client = frontend


async def test_stream_relays_the_pinned_chain_as_sealed_items() -> None:
    _secret, recipient = generate_identity()
    ct = encrypt(b"item", [recipient])
    frontend = Frontend([("events", ct), (None, ct), ("events", ct)])
    e, _, _ = engine(WithClient(frontend), relay_interval=0.01)
    polls = 0

    async def closed() -> bool:
        nonlocal polls
        polls += 1
        return polls > 1

    batches = [
        b
        async for b in e.stream(
            GOLDEN, pin=Pin("run-1"), topics=["events"], after=0, closed=closed, session="s"
        )
    ]
    first = batches[0]
    assert [(i.topic, i.offset) for i in first.items] == [("events", 0), ("events", 2)]
    assert all(i.sealed is not None and i.sealed.data == ct for i in first.items)
    assert (first.next_offset, first.dropped) == (3, 1)
    assert frontend.polls[0] == (["events"], 0, "run-1")


async def test_the_relay_token_is_the_jobs_own_and_never_a_partys() -> None:
    """With a public frontend (``relay_service``), the relay dials it with a JWT whose subject
    the authorizer rebuilds for this one job, with no permission (decisions I30)."""
    frontend = Frontend([])
    frontend.failure = "KontraStreamFull"
    tokens: list[str] = []

    async def relay_service(token: str) -> Frontend:
        tokens.append(token)
        return frontend

    e, _, signer = engine(relay_service=relay_service)

    async def never() -> bool:
        return False

    with pytest.raises(StreamStopped) as stopped:
        async for _ in e.stream(
            GOLDEN, pin=Pin("run-1"), topics=["events"], after=0, closed=never, session="relay-sid"
        ):
            pass
    assert stopped.value.reason == "budget"
    claims = signer.claims(tokens[0])
    assert claims["sub"] == relay_subject("acme", JOB)
    assert claims["sid"] == "relay-sid"
    assert "permissions" not in claims


async def test_an_adapter_with_no_frontend_relays_no_streams() -> None:
    e, _, _ = engine()

    async def never() -> bool:
        return False

    with pytest.raises(StreamStopped) as stopped:
        async for _ in e.stream(
            GOLDEN, pin=Pin("r"), topics=["t"], after=0, closed=never, session="s"
        ):
            pass
    assert stopped.value.reason == "unsupported"
