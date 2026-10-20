"""The Temporal adapter without a server (seam ⑥): its handle (a golden, and the strict parse),
each member as the exact control-plane call it makes, the claims of the credentials it mints,
what it withholds (an unsealed delivery), how failures map, and the stream relay mapped onto
``kontra._relay``. The same adapter passes the engine contract on a dev server
(``test_temporal_engine_contract.py``) and, over the wire, on Kontra's own server
(``tests/temporal``)."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from temporalio.api.common.v1 import Payload, Payloads
from temporalio.api.update.v1 import Outcome
from temporalio.api.workflowservice.v1 import (
    UpdateWorkflowExecutionResponse,
)

from kontra._control import (
    HANDLER_BAD_REQUEST,
    HANDLER_INTERNAL,
    HANDLER_UPSTREAM_TIMEOUT,
    BuyerCall,
    BuyerLookupError,
    CallFailure,
    CallRecord,
    CallStatus,
    EndpointConflict,
    EndpointInfo,
    HistorySummary,
    NexusSyncKind,
    NexusSyncResult,
    Occupation,
    TemporalError,
    internal_relay_service,
)
from kontra._control import ObservationIncomplete as ChainIncomplete
from kontra.codec import stream as s
from kontra.codec.age import encrypt, generate_identity
from kontra.contract.names import OfferingId, job_workflow_id, relay_subject
from kontra.engine import (
    BuyerStart,
    EngineError,
    Fenced,
    InvalidHandle,
    JobHandle,
    JobPlan,
    ObservationIncomplete,
    PartyLookupError,
    Pin,
    QuoteKind,
    Reach,
    RouteConflict,
    Stop,
    StreamBatch,
    StreamStopped,
    hold_digest,
)
from kontra.engine.temporal import (
    AUD_TEMPORAL,
    HANDLE_PREFIX,
    TemporalEngine,
    reach_of,
    worker_token,
)
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
        metadata={"encoding": b"binary/kontra-age", "kontra-v": b"1"},
        data=encrypt(text, [generate_identity()[1]]),
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
    #: What history_size answers: (events, bytes) of the run's history (the relay's pace).
    history: tuple[int, int] | None = (10, 1000)
    #: What describe_call answers: the buyer's standalone call of the job, or none.
    call: CallRecord | None = None
    #: What describe_call raises (it alone: the observation it follows stands).
    fail_call: Exception | None = None
    #: describe_call never answers: a buyer's namespace that stalls reads.
    stall_call: bool = False

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

    async def history_size(self, namespace: str, workflow_id: str) -> tuple[int, int] | None:
        return self.history

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

    async def describe_call(self, namespace: str, operation_id: str) -> CallRecord | None:
        self._call("describe_call", namespace, operation_id)
        if self.fail_call is not None:
            raise self.fail_call
        if self.stall_call:
            await asyncio.Event().wait()
        return self.call


def engine(control: Recorder | None = None, **kw: Any) -> tuple[TemporalEngine, Recorder, Signer]:
    control = control or Recorder()
    signer: Signer = kw.pop("signer", None) or Signer()
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
    assert (seen.reach, seen.buyer_start) == (Reach.STARTED, None)  # the pinned lineage: no read


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


@pytest.mark.parametrize("digest", [None, "a" * 64], ids=["no-digest", "a-digest"])
async def test_a_verified_completion_without_bytes_is_no_delivery(digest: str | None) -> None:
    """A control plane (a double, a future one) that answers a verified completion with no
    stored bytes -- with or without a digest of some -- never has escrow pay a delivery the
    gate does not hold: it is UNSEALED, refunded against the seller."""
    e, control, _ = engine()
    control.observation = (
        Observation(
            ObservedStatus.COMPLETED,
            provenance=Provenance.VERIFIED,
            pin="run-1",
            result_sha256=digest,
            result_size=None if digest is None else 5,
        ),
        None,
    )
    seen = await e.observe(GOLDEN, pin=None, holds=())
    o = seen.observation
    assert (seen.result, o.status, o.result_sha256, o.result_size) == (
        None,
        ObservedStatus.UNSEALED,
        None,
        None,
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


# --------------------------------------------------------------------------- reach (I40)

#: The job's hold, as the buyer's start carries it, and another job's (other signed bytes).
TOKEN = "eyJhbGciOiJFUzI1NiJ9.eyJqb2JfaWQiOiIxIn0.c2lnbmF0dXJl"
OTHER_TOKEN = "eyJhbGciOiJFUzI1NiJ9.eyJqb2JfaWQiOiIyIn0.c2lnbmF0dXJl"
CALL = BuyerCall("bob", ROUTE, "acme.enrich", frozenset({hold_digest(TOKEN)}))
#: When the job's hold expires (an hour after the hire) and the window kontra-cloud's gate
#: judges an unanswered call by (``CallWindow.of(hold expiry, max_duration 1 h)``: made at
#: least ten minutes before the hold expires, live until it does).
HOLD_EXPIRES = datetime(2026, 10, 1, 13, 0, tzinfo=UTC)
WINDOW = (HOLD_EXPIRES - timedelta(minutes=10), HOLD_EXPIRES)
#: A call made right after the hire that waits until the job's deadline, as ``start`` makes it.
FULL: dict[str, Any] = {
    "scheduled_at": HOLD_EXPIRES - timedelta(hours=1),
    "expires_at": HOLD_EXPIRES + timedelta(seconds=30),
}
#: A buyer's own call during a seller's worker restart, ended by its own 15 s
#: schedule-to-close: ``TIMED_OUT`` with the last attempt's ``UPSTREAM_TIMEOUT`` (measured).
EARLY_END: dict[str, Any] = {
    "scheduled_at": HOLD_EXPIRES - timedelta(minutes=50),
    "expires_at": HOLD_EXPIRES - timedelta(minutes=50) + timedelta(seconds=15),
}
#: A call made in the hold's last minutes, waiting until the deadline.
LATE: dict[str, Any] = {
    "scheduled_at": HOLD_EXPIRES - timedelta(minutes=10) + timedelta(seconds=1),
    "expires_at": HOLD_EXPIRES + timedelta(seconds=30),
}
TIMEOUT = CallFailure(HANDLER_UPSTREAM_TIMEOUT)


def _call(
    status: CallStatus = CallStatus.RUNNING,
    *,
    started: bool = False,
    failure: CallFailure | None = None,
    **fields: Any,
) -> CallRecord:
    values: dict[str, Any] = {
        "endpoint": CALL.endpoint,
        "service": CALL.service,
        "operation": "execute",
        "headers": {"kontra-hold": TOKEN},
        **FULL,
    }
    values.update(fields)
    return CallRecord(status=status, started=started, failure=failure, **values)


def _gate(reach: Reach | None, start: BuyerStart | None) -> tuple[str, str]:
    """What kontra-cloud's gate makes of a reach once it reads ``observed.reach`` instead of
    ``judge_call`` (the tracking issue's port): who the refund is blamed on
    (``BuyerCallSeen``) and how the job's reason words it (``call_reason``). The window and
    the wording are the gate's; the facts are the engine's."""
    made_by, live_until = WINDOW
    waited = (
        start is not None
        and start.made_at is not None
        and start.made_at <= made_by
        and (start.gives_up_at is None or start.gives_up_at >= live_until)
    )
    if reach in (Reach.STARTED, Reach.FAILED) or (reach is Reach.UNANSWERED and waited):
        return "reached", "seller never started the job"
    if reach is Reach.REFUSED:
        return "refused", "seller refused the buyer's call"
    if reach is Reach.INCONCLUSIVE:
        return "inconclusive", "the buyer's call failed before the job started"
    if start is not None and start.withdrawal_asked:
        return "unseen", "canceled by buyer"
    return "unseen", "buyer never started the job"


REACHED = ("reached", "seller never started the job")
REFUSED = ("refused", "seller refused the buyer's call")
INCONCLUSIVE = ("inconclusive", "the buyer's call failed before the job started")
UNSEEN = ("unseen", "buyer never started the job")
WITHDRAWN = ("unseen", "canceled by buyer")


#: kontra-cloud's ``tests/jobs/test_attribution.py::test_judge_call`` table, row for row (the
#: wording from ``call_reason``): the reach each record maps to, and what the gate makes of it.
JUDGE_CALL: list[tuple[CallRecord | None, Reach, tuple[str, str]]] = [
    # Nothing to see.
    (None, Reach.NEVER_DISPATCHED, UNSEEN),
    # Not this job's call: another endpoint, service, operation or hold.
    (_call(failure=TIMEOUT, endpoint="k" + "2" * 32), Reach.NEVER_DISPATCHED, UNSEEN),
    (_call(failure=TIMEOUT, service="acme.other"), Reach.NEVER_DISPATCHED, UNSEEN),
    (_call(failure=TIMEOUT, operation="quote"), Reach.NEVER_DISPATCHED, UNSEEN),
    (
        _call(failure=TIMEOUT, headers={"kontra-hold": OTHER_TOKEN}),
        Reach.NEVER_DISPATCHED,
        UNSEEN,
    ),
    (_call(failure=TIMEOUT, headers={}), Reach.NEVER_DISPATCHED, UNSEEN),
    # Any spelling the seller's handler accepts is the job's call, as for escrow.
    (
        _call(failure=TIMEOUT, headers={"Kontra-Hold": f" {TOKEN}\t"}),
        Reach.UNANSWERED,
        REACHED,
    ),
    (_call(failure=TIMEOUT, headers={"kontra-hold": TOKEN + "x"}), Reach.UNANSWERED, REACHED),
    # The seller's handler took the call, and no run of it verified.
    (_call(started=True), Reach.STARTED, REACHED),
    (_call(CallStatus.TERMINATED, started=True), Reach.STARTED, REACHED),
    (_call(CallStatus.COMPLETED), Reach.STARTED, REACHED),
    # No worker of the seller's answered a call that waited from early on until the hold
    # expired: while the call was live, or until it timed out.
    (_call(failure=TIMEOUT), Reach.UNANSWERED, REACHED),
    (_call(CallStatus.TIMED_OUT, failure=TIMEOUT), Reach.UNANSWERED, REACHED),
    (_call(failure=TIMEOUT, expires_at=None), Reach.UNANSWERED, REACHED),
    (_call(failure=TIMEOUT, scheduled_at=WINDOW[0]), Reach.UNANSWERED, REACHED),
    (
        _call(CallStatus.TIMED_OUT, failure=TIMEOUT, expires_at=HOLD_EXPIRES),
        Reach.UNANSWERED,
        REACHED,
    ),
    # ... but not one the buyer let end itself before the hold expired, one made in the
    # hold's last minutes, or one whose times are unknown.
    (_call(CallStatus.TIMED_OUT, failure=TIMEOUT, **EARLY_END), Reach.UNANSWERED, UNSEEN),
    (
        _call(
            CallStatus.TIMED_OUT, failure=TIMEOUT, expires_at=HOLD_EXPIRES - timedelta(seconds=1)
        ),
        Reach.UNANSWERED,
        UNSEEN,
    ),
    (_call(failure=TIMEOUT, **LATE), Reach.UNANSWERED, UNSEEN),
    (_call(failure=TIMEOUT, scheduled_at=None), Reach.UNANSWERED, UNSEEN),
    # The window is about unanswered calls only.
    (_call(CallStatus.TERMINATED, started=True, **EARLY_END), Reach.STARTED, REACHED),
    (
        _call(CallStatus.FAILED, failure=CallFailure(HANDLER_INTERNAL, final=True), **LATE),
        Reach.FAILED,
        REACHED,
    ),
    # The handler declared its own (the seller's code's) failure final.
    (
        _call(CallStatus.FAILED, failure=CallFailure(HANDLER_INTERNAL, final=True)),
        Reach.FAILED,
        REACHED,
    ),
    # Refused: the hold, the input or its price. No blame, by design.
    (
        _call(CallStatus.FAILED, failure=CallFailure(HANDLER_BAD_REQUEST, final=True)),
        Reach.REFUSED,
        REFUSED,
    ),
    # A retryable handler error may be the buyer's doing (a claim-check it never uploaded).
    (_call(failure=CallFailure(HANDLER_INTERNAL)), Reach.INCONCLUSIVE, INCONCLUSIVE),
    (_call(failure=CallFailure("UNAVAILABLE")), Reach.INCONCLUSIVE, INCONCLUSIVE),
    (_call(failure=CallFailure("")), Reach.INCONCLUSIVE, INCONCLUSIVE),
    # Withdrawn by the buyer before the seller took it: the brief-outage grief.
    (_call(CallStatus.TERMINATED, failure=TIMEOUT), Reach.WITHDRAWN, UNSEEN),
    (
        _call(CallStatus.CANCELED, failure=TIMEOUT, cancel_requested=True),
        Reach.WITHDRAWN,
        WITHDRAWN,
    ),
    # ... but a call the seller's handler ended canceled, unasked, is its refusal.
    (_call(CallStatus.CANCELED, failure=CallFailure("")), Reach.REFUSED, REFUSED),
    (_call(CallStatus.CANCELED), Reach.REFUSED, REFUSED),
    # A buyer's cancel of a call no worker took leaves it live, retried: a seller away for
    # the whole window is still the one that never answered.
    (_call(failure=TIMEOUT, cancel_requested=True), Reach.UNANSWERED, REACHED),
    (_call(failure=TIMEOUT, cancel_requested=True, **LATE), Reach.UNANSWERED, WITHDRAWN),
    # No attempt finished (started a moment before its deadline).
    (_call(CallStatus.TIMED_OUT), Reach.NEVER_DISPATCHED, UNSEEN),
    (_call(), Reach.NEVER_DISPATCHED, UNSEEN),
    # call_reason's wording: a withdrawal the buyer asked for, by any record that blames no one.
    (_call(CallStatus.TERMINATED, cancel_requested=True), Reach.WITHDRAWN, WITHDRAWN),
    (_call(cancel_requested=True), Reach.NEVER_DISPATCHED, WITHDRAWN),
]


@pytest.mark.parametrize(("record", "reach", "gate"), JUDGE_CALL)
def test_reach_is_judge_calls_case_analysis(
    record: CallRecord | None, reach: Reach, gate: tuple[str, str]
) -> None:
    """Decisions I40: the mapping from the buyer's standalone call, as the frontend records
    it, is kontra-cloud's ``judge_call`` case for case (FIXUPS #22), and the record that goes
    with it is enough for the gate to reach the same verdict and the same wording."""
    got, start = reach_of(record, CALL)
    assert got is reach
    if record is None or not record.is_call_of(CALL):
        assert start is None
    else:
        assert start == BuyerStart(record.scheduled_at, record.expires_at, record.cancel_requested)
    assert _gate(got, start) == gate


async def test_observe_reads_how_far_the_buyers_start_got() -> None:
    """A job with no verified run: the buyer's standalone operation ``job-<id>`` is described
    in the buyer's namespace, after the observation, and mapped (decisions I40)."""
    e, control, _ = engine()
    control.call = _call(failure=TIMEOUT)
    seen = await e.observe(GOLDEN, pin=None, holds=CALL.holds)
    wf = job_workflow_id(JOB)
    assert control.calls == [
        ("observe", "acme", wf, CALL, None),
        ("describe_call", "bob", wf),
    ]
    assert seen.observation == Observation(ObservedStatus.NOT_FOUND)
    assert (seen.reach, seen.buyer_start) == (
        Reach.UNANSWERED,
        BuyerStart(FULL["scheduled_at"], FULL["expires_at"], False),
    )
    control.call = None
    assert (await e.observe(GOLDEN, pin=None, holds=CALL.holds)).reach is Reach.NEVER_DISPATCHED
    # A run the seller started itself is no start of the buyer's: read from the call alone.
    control.observation = (
        Observation(ObservedStatus.COMPLETED, provenance=Provenance.NOT_FROM_BUYER, pin="r"),
        sealed_result(),
    )
    own = await e.observe(GOLDEN, pin=None, holds=CALL.holds)
    assert (own.reach, own.buyer_start, own.result) == (Reach.NEVER_DISPATCHED, None, None)


async def test_a_verified_or_pinned_lineage_started_and_nothing_is_read() -> None:
    """The buyer's start began a verified lineage (and a pinned one, which escrow pinned when
    it was verified): STARTED, with no read of the buyer's namespace."""
    e, control, _ = engine()
    control.observation = (
        Observation(ObservedStatus.RUNNING, provenance=Provenance.VERIFIED, pin="run-1"),
        None,
    )
    seen = await e.observe(GOLDEN, pin=None, holds=CALL.holds)
    control.observation = (Observation(ObservedStatus.NOT_FOUND, pin="run-1"), None)
    gone = await e.observe(GOLDEN, pin=Pin("run-1"), holds=CALL.holds)
    assert [c[0] for c in control.calls] == ["observe", "observe"]
    for lineage in (seen, gone):
        assert (lineage.reach, lineage.buyer_start) == (Reach.STARTED, None)


async def test_a_fenced_jobs_placeholder_is_not_read() -> None:
    """A fenced job's placeholder (OCCUPIED) is observed until the job's deadline (a cancel's
    watch) and never judged: its reach is None, with no read of the buyer's namespace."""
    e, control, _ = engine()
    control.call = _call(failure=TIMEOUT)
    placeholder = Observation(ObservedStatus.TERMINATED, provenance=Provenance.OCCUPIED, pin="p")
    control.observation = (placeholder, None)
    seen = await e.observe(GOLDEN, pin=None, holds=CALL.holds)
    assert [c[0] for c in control.calls] == ["observe"]
    assert (seen.observation, seen.reach, seen.buyer_start) == (placeholder, None, None)


async def test_a_reach_read_that_stalls_is_cut_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """A buyer's namespace that never answers delays the observation escrow needs by the
    read's own bound at most (``CALL_READ_TIMEOUT``, 2 s; shortened here), whatever the
    control plane does, and the reach is None."""
    import kontra.engine.temporal as adapter

    assert adapter._REACH_WAIT == 2.0  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(adapter, "_REACH_WAIT", 0.05)
    e, control, _ = engine()
    obs = Observation(ObservedStatus.NOT_FOUND)
    control.stall_call = True
    began = time.monotonic()
    async with asyncio.timeout(5):
        seen = await e.observe(GOLDEN, pin=None, holds=CALL.holds)
    assert time.monotonic() - began < 1
    assert (seen.observation, seen.reach, seen.buyer_start) == (obs, None, None)


@pytest.mark.parametrize(
    "failure",
    [BuyerLookupError("RESOURCE_EXHAUSTED"), TemporalError("down"), RuntimeError("anything")],
)
async def test_a_reach_that_cannot_be_read_is_none_and_the_observation_stands(
    failure: Exception,
) -> None:
    """Reach is reputation: a buyer's namespace that cannot be read is "cannot say" (None,
    which blames no one), never an error that would hold up the observation escrow needs."""
    e, control, _ = engine()
    obs = Observation(ObservedStatus.RUNNING, provenance=Provenance.NOT_FROM_BUYER, pin="r")
    control.observation = (obs, None)
    control.fail_call = failure
    seen = await e.observe(GOLDEN, pin=None, holds=CALL.holds)
    assert (seen.observation, seen.result, seen.reach, seen.buyer_start) == (obs, None, None, None)


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
    """The relay's two calls, answered from a log of items, each a reference to its blob in
    the job's stream blobs (:meth:`read_stream_blob`, what the adapter's ``stream_blobs``
    reads, recording what it was asked)."""

    log: list[tuple[str | None, bytes]]
    polls: list[tuple[list[str], int, str]] = field(
        default_factory=list[tuple[list[str], int, str]]
    )
    failure: str | None = None
    read: list[tuple[str, str, int]] = field(default_factory=list[tuple[str, str, int]])
    missing: set[int] = field(default_factory=set[int])

    @property
    def workflow_service(self) -> Frontend:
        return self

    async def read_stream_blob(self, job_id: str, sha256: str, size: int) -> bytes | None:
        self.read.append((job_id, sha256, size))
        for i, (_, ct) in enumerate(self.log):
            if sha256_of(ct) == sha256 and i not in self.missing:
                return ct
        return None

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
                items.append(
                    {"topic": topic, "offset": i, "size": len(ct), "sha256": sha256_of(ct)}
                )
        env = {"v": 2, "items": items, "next_offset": len(self.log), "more_ready": False}
        p = Payload(
            metadata={"encoding": s.STREAM_ENCODING, "kontra-v": b"1"},
            data=json.dumps(env).encode(),
        )
        resp = UpdateWorkflowExecutionResponse(outcome=Outcome(success=Payloads(payloads=[p])))
        resp.update_ref.workflow_execution.run_id = "run-1"
        return resp


class WithClient(Recorder):
    """A control plane whose client is a dev server's one frontend."""

    def __init__(self, frontend: Frontend) -> None:
        super().__init__()
        self.client = frontend


@dataclass
class Dialled:
    """The relay's frontend as one token dialled it: every call records that token's expiry
    and the gate's clock when the call was made."""

    frontend: Frontend
    expires: int
    clock: Callable[[], datetime]
    calls: list[tuple[int, datetime]]

    async def update_workflow_execution(self, req: Any, retry: bool = False, metadata: Any = None, timeout: Any = None) -> UpdateWorkflowExecutionResponse:  # noqa: ASYNC109  # fmt: skip
        self.calls.append((self.expires, self.clock()))
        return await self.frontend.update_workflow_execution(req, retry, metadata, timeout)

    async def query_workflow(self, req: Any, retry: bool = False, metadata: Any = None, timeout: Any = None) -> Any:  # noqa: ASYNC109  # fmt: skip
        raise AssertionError("not asked for the tail")


def sha256_of(data: bytes) -> str:
    return sha256(data).hexdigest()


async def test_stream_relays_the_pinned_chain_as_sealed_items() -> None:
    """Each item dereferenced from the job's stream blobs (decisions I39), scoped to the job,
    and handed to the gate as the same ``Sealed`` items as before."""
    _secret, recipient = generate_identity()
    ct = encrypt(b"item", [recipient])
    frontend = Frontend([("events", ct), (None, ct), ("events", ct)])
    control = WithClient(frontend)
    e, _, _ = engine(
        control,
        relay_service=internal_relay_service(control),
        stream_blobs=frontend,
        relay_interval=0.01,
    )
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
    assert {job for job, _, _ in frontend.read} == {JOB}  # the job's own blobs only


async def test_an_item_too_large_or_not_stored_is_handed_over_without_ciphertext() -> None:
    """Above ``stream_item_max`` an item is not even fetched; a blob the reader does not have
    is an item without ``sealed``: the gate records a gap with its size."""
    _secret, recipient = generate_identity()
    small, big = encrypt(b"item", [recipient]), encrypt(b"x" * 5000, [recipient])
    frontend = Frontend(
        [("events", small), ("events", big), ("events", encrypt(b"y", [recipient]))]
    )
    frontend.missing = {2}
    control = WithClient(frontend)
    e, _, _ = engine(
        control,
        relay_service=internal_relay_service(control),
        stream_blobs=frontend,
        stream_item_max=1000,
        relay_interval=0.01,
    )
    polls = 0

    async def closed() -> bool:
        nonlocal polls
        polls += 1
        return polls > 1

    [first, *_] = [
        b
        async for b in e.stream(
            GOLDEN, pin=Pin("run-1"), topics=["events"], after=0, closed=closed, session="s"
        )
    ]
    assert [(i.offset, i.sealed is not None, i.size) for i in first.items] == [
        (0, True, len(small)),
        (1, False, len(big)),
        (2, False, len(frontend.log[2][1])),
    ]
    assert sha256_of(big) not in {d for _, d, _ in frontend.read}


async def test_the_relay_token_is_the_jobs_own_and_never_a_partys() -> None:
    """With a public frontend (``relay_service``), the relay dials it with a JWT whose subject
    the authorizer rebuilds for this one job, with no permission (decisions I30)."""
    frontend = Frontend([])
    frontend.failure = "KontraStreamFull"
    tokens: list[str] = []

    async def relay_service(token: str) -> Frontend:
        tokens.append(token)
        return frontend

    e, _, signer = engine(relay_service=relay_service, stream_blobs=frontend)

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


async def test_the_relay_token_is_minted_again_before_it_expires() -> None:
    """Over the public frontend a token is refused once it expires (decisions I30: 15
    minutes), and a job's stream lasts as long as the job: the adapter dials again with a
    fresh token once half the last one's life is spent, so no poll is ever made with an
    expired token, however long the relay runs."""
    _secret, recipient = generate_identity()
    frontend = Frontend([("events", encrypt(b"item", [recipient]))])
    now = NOW
    calls: list[tuple[int, datetime]] = []
    claims: list[dict[str, Any]] = []
    signer = Signer()

    async def relay_service(token: str) -> Dialled:
        claims.append(signer.claims(token))
        return Dialled(frontend, claims[-1]["exp"], lambda: now, calls)

    e, _, _ = engine(
        signer=signer,
        relay_service=relay_service,
        stream_blobs=frontend,
        relay_interval=0.01,
        clock=lambda: now,
    )
    polls = 0

    async def closed() -> bool:  # each poll answered, the gate's clock moves 5 minutes on
        nonlocal polls, now
        polls += 1
        now += timedelta(minutes=5)
        return polls > 12

    batches = [
        b
        async for b in e.stream(
            GOLDEN, pin=Pin("run-1"), topics=["events"], after=0, closed=closed, session="sid"
        )
    ]
    assert [i.offset for b in batches for i in b.items] == [0]
    assert len(calls) > 10  # the relay ran for about an hour of the gate's clock
    assert all(at.timestamp() < expires for expires, at in calls), calls
    assert len(claims) >= 4  # one 15-minute token could not have carried it
    assert {c["sub"] for c in claims} == {relay_subject("acme", JOB)}
    assert {c["sid"] for c in claims} == {"sid"}  # a revoked session still ends the relay


async def test_the_relay_is_paced_through_the_control_plane() -> None:
    """The relay's pace reads the run's history through the control plane
    (``Control.history_size``), never around it: a control plane with no client of its own
    still stops a relay past half the server's limits."""
    _secret, recipient = generate_identity()
    frontend = Frontend([("events", encrypt(b"item", [recipient]))])
    control = Recorder(history=(600, 0))
    assert not hasattr(control, "client")

    async def relay_service(token: str) -> Frontend:
        return frontend

    e, _, _ = engine(
        control,
        relay_service=relay_service,
        stream_blobs=frontend,
        relay_interval=0.01,
        history_limits=(1000, 10**9),
    )
    polls = 0

    async def closed() -> bool:
        nonlocal polls
        polls += 1
        return polls > 50

    got: list[StreamBatch] = []

    async def drain() -> None:
        async for b in e.stream(
            GOLDEN, pin=Pin("run-1"), topics=["events"], after=0, closed=closed, session="s"
        ):
            got.append(b)

    with pytest.raises(StreamStopped) as stopped:
        await drain()
    assert stopped.value.reason == "budget"
    assert [i.offset for b in got for i in b.items] == [0]


async def test_an_adapter_with_no_frontend_or_bucket_relays_no_streams() -> None:
    """No ``relay_service`` (and a control plane without a frontend of its own gives none:
    ``internal_relay_service``), or no ``stream_blobs`` to dereference items from: no
    streams."""
    assert internal_relay_service(Recorder()) is None

    async def never() -> bool:
        return False

    async def relay_service(token: str) -> Frontend:
        return Frontend([])

    for e, _, _ in (
        engine(),
        engine(relay_service=relay_service),
        engine(stream_blobs=Frontend([])),
    ):
        with pytest.raises(StreamStopped) as stopped:
            async for _ in e.stream(
                GOLDEN, pin=Pin("r"), topics=["t"], after=0, closed=never, session="s"
            ):
                pass
        assert stopped.value.reason == "unsupported"
