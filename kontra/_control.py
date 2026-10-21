"""Control-plane calls against Temporal's INTERNAL frontend, shared by both gates.

The gate (``kontra-cloud``) and the fake gate (:mod:`kontra.testing.fake_gate`) need the
same handful of operations that no party's token may perform (decisions I3, I4, I6):
register a party's namespace, create and look up the Nexus endpoint of a published
version, relay a quote as a synchronous Nexus call, watch the seller's handler workflow
``job-<id>``, stop it when escrow says so, start a standalone Nexus operation in a
buyer's namespace on its behalf, and describe that operation for how far a job that never
ran got (:meth:`Control.describe_call`, decisions I40). :class:`Control` is that set, in one
place, so the two gates cannot drift.

Every call names its namespace explicitly in a raw ``workflow_service`` /
``operator_service`` request: the client's own default namespace is never used, so one
client on the internal frontend serves every party. Nothing here reads a payload: results
are returned as the bytes the history holds (ciphertext), and progress is derived from
history *metadata* only (PRD §8.4).

Errors: a failure of the platform (frontend unreachable, an RPC refused) raises
:class:`TemporalError`; a failed read of a *buyer's* namespace raises its subclass
:class:`BuyerLookupError`, because the buyer controls that namespace's load and a gate must
not refund blind on it. A seller's own failure is never an exception:
:meth:`Control.nexus_sync` classifies it into a :class:`NexusSyncResult`.

Provenance (decisions I15): :meth:`Control.observe` reports a run of ``job-<id>`` as the
buyer's only after looking the buyer's call up in the buyer's namespace and comparing
structured fields for exact equality; no operation token, callback URL or other encoded
string is ever decoded. The same lookup says whether the buyer asked to cancel its call
(``Observation.buyer_canceled``), and :meth:`Control.occupy_job` takes ``job-<id>`` for a
cancel before any run exists (decisions I16).
"""

from __future__ import annotations

import asyncio
import json
import math
import re
import uuid
from collections import OrderedDict
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Hashable,
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import aclosing
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Any, Protocol, TypeVar, runtime_checkable
from urllib.parse import quote

import httpx
from google.protobuf.message import DecodeError
from google.protobuf.timestamp_pb2 import Timestamp
from temporalio.api.common.v1 import Link, Memo, Payload, WorkflowExecution, WorkflowType
from temporalio.api.enums.v1 import (
    EventType,
    HistoryEventFilterType,
    NexusHandlerErrorRetryBehavior,
    NexusOperationExecutionStatus,
    NexusOperationIdConflictPolicy,
    NexusOperationIdReusePolicy,
    PendingNexusOperationState,
    TaskQueueType,
    WorkflowExecutionStatus,
    WorkflowIdConflictPolicy,
    WorkflowIdReusePolicy,
)
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.nexus.v1 import (
    Endpoint,
    EndpointSpec,
    EndpointTarget,
    NexusOperationExecutionInfo,
)
from temporalio.api.operatorservice.v1 import (
    CreateNexusEndpointRequest,
    ListNexusEndpointsRequest,
)
from temporalio.api.taskqueue.v1 import PollerInfo, TaskQueue
from temporalio.api.worker.v1 import WorkerListInfo
from temporalio.api.workflowservice.v1 import (
    DescribeNamespaceRequest,
    DescribeNamespaceResponse,
    DescribeNexusOperationExecutionRequest,
    DescribeNexusOperationExecutionResponse,
    DescribeTaskQueueRequest,
    DescribeTaskQueueResponse,
    DescribeWorkflowExecutionRequest,
    DescribeWorkflowExecutionResponse,
    GetWorkflowExecutionHistoryRequest,
    ListWorkersRequest,
    ListWorkersResponse,
    RegisterNamespaceRequest,
    RequestCancelWorkflowExecutionRequest,
    StartNexusOperationExecutionRequest,
    StartWorkflowExecutionRequest,
    TerminateWorkflowExecutionRequest,
    UpdateNamespaceRequest,
)
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

from kontra._relay import RelayService
from kontra.codec import CLAIM_CHECK_ENCODING
from kontra.codec import _parse_reference as _reference  # pyright: ignore[reportPrivateUsage]
from kontra.contract.names import is_endpoint_name
from kontra.contract.offering import MAX_DURATION_RANGE
from kontra.engine import hold_digest  # re-exported: kontra-cloud imports it from here
from kontra.escrow import Observation, ObservedStatus, Provenance
from kontra.gate_client import models as m

__all__ = [
    "CALL_READ_TIMEOUT",
    "CHAIN_READS",
    "DEFAULT_RETENTION",
    "EXECUTE_OPERATION",
    "FRONTEND_RESERVE",
    "HANDLER_BAD_REQUEST",
    "HANDLER_INTERNAL",
    "HANDLER_UPSTREAM_TIMEOUT",
    "HOLD_HEADER",
    "IDENTITY",
    "OCCUPY_TASK_QUEUE",
    "OCCUPY_WORKFLOW_TYPE",
    "RETENTION_MARGIN",
    "BuyerCall",
    "BuyerLookupError",
    "CallFailure",
    "CallRecord",
    "CallStatus",
    "Control",
    "EndpointConflict",
    "EndpointInfo",
    "HistorySummary",
    "NexusSyncKind",
    "NexusSyncResult",
    "ObservationIncomplete",
    "Occupation",
    "TemporalControl",
    "TemporalError",
    "call_record",
    "hold_digest",
    "input_size",
    "internal_relay_service",
]

#: Above the contract's max_duration cap (see :data:`DEFAULT_RETENTION`): the gate's grace
#: and clock skew are seconds, so days of margin are plenty and cost only history storage.
RETENTION_MARGIN = timedelta(days=5)
#: Party namespaces keep closed workflows and standalone operations this long. A hold token
#: can start ``job-<id>`` until it expires, up to the max_duration cap after the hold was
#: locked, and the only thing refusing a second start is the first one's record
#: (``REJECT_DUPLICATE`` on the workflow id and on the operation id): were it dropped before
#: the token expired, the same token could start the job again. So retention outlasts the
#: longest token, derived from the cap in one place (FIXUPS #19).
DEFAULT_RETENTION = MAX_DURATION_RANGE[1] + RETENTION_MARGIN
#: The Nexus operation that runs a job, and the header its hold token travels in (the
#: seller's handler reads the same names: ``kontra.serve``).
EXECUTE_OPERATION = "execute"
HOLD_HEADER = "kontra-hold"
#: Who the control plane says it is in histories and on standalone operations.
IDENTITY = "kontra-control"
#: The frontend keeps about this much of a Nexus ``Request-Timeout`` for itself: the
#: handler's deadline is the header minus ~2 s, and a header of 2 s or less is answered
#: ``520 upstream timeout`` at once (server 1.32, measured). The relay adds it to the
#: caller's timeout, so the handler gets the whole of it while the caller still waits no
#: longer than its timeout (enforced on our side of the connection).
FRONTEND_RESERVE = 2.0

#: The placeholder :meth:`Control.occupy_job` starts as ``job-<id>``: a workflow type and a
#: task queue no worker serves (an offering's task queue always has an ``@`` in it). It is
#: never handed to a worker in any case: its start is delayed (:data:`_OCCUPY_DELAY`) past
#: its own execution timeout, so it closes before a workflow task could ever be dispatched.
OCCUPY_WORKFLOW_TYPE = "kontra.occupied"
OCCUPY_TASK_QUEUE = "kontra.occupied"
#: The memo key that names why ``job-<id>`` was occupied (the seller sees it in its history).
_OCCUPY_MEMO = "kontra-occupied"
#: The placeholder closes on its own this long after it was started if its termination never
#: lands (the gate crashed between the two calls)...
_OCCUPY_TIMEOUT = timedelta(minutes=1)
#: ...because its first workflow task is not due until this much later (server 1.32,
#: measured: the execution timeout runs from the start, delay included).
_OCCUPY_DELAY = timedelta(hours=1)

_RPC_TIMEOUT = timedelta(seconds=15)
#: How long :meth:`Control.describe_call` waits: a describe answers in milliseconds, and the
#: read is made on every observation of a job with no verified run, in a namespace whose load
#: its buyer controls (decisions I40) -- one that hangs costs at most this, not the RPC's 15 s.
CALL_READ_TIMEOUT = timedelta(seconds=2)
_NAMESPACE_WAIT = 30.0
_JSON_PLAIN = b"json/plain"
_MESSAGE_MAX = 1000
#: Prior runs of a continue-as-new chain are immutable once closed, so what they contribute
#: to a summary is computed once and remembered (bounded).
_CHAIN_CACHE = 4096
#: Histories of earlier runs one summary may read (see :meth:`Control._chain_before`), and
#: runs one observation may step through along a pinned chain the latest run left.
CHAIN_READS = 64
#: Links into the buyer's namespace one run's provenance check resolves. A genuine run has
#: exactly one (the caller's); each costs a read of a buyer history up to the event it
#: names, so a seller attaching more only makes the gate read more.
_LINKS_CHECKED = 2
_HTTP_TOKEN = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]{1,128}", re.ASCII)
#: Headers the relay itself sets, or that carry Nexus protocol meaning; a caller's header
#: may not replace them.
_RESERVED_HEADERS = frozenset(
    {"content-type", "content-length", "request-timeout", "host", "connection", "transfer-encoding"}
)
_RESERVED_HEADER_PREFIXES = ("nexus-", "temporal-")
_WORKFLOW_TASK_EVENTS = frozenset(
    {
        EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
        EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED,
        EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
        EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED,
        EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
    }
)

_SCHEDULED = EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
_STARTED_EVENT = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
#: Closes that may hand the chain over to a successor run (continue-as-new; a retry or the
#: next cron run after a completion, failure or timeout). Termination and cancellation end
#: a chain.
_MAY_HAND_OVER = frozenset(
    {
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
    }
)
_K = TypeVar("_K", bound=Hashable)
_V = TypeVar("_V")

_STATUS: Mapping[int, ObservedStatus] = {
    WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED: ObservedStatus.COMPLETED,
    WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED: ObservedStatus.FAILED,
    WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CANCELED: ObservedStatus.CANCELED,
    WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TERMINATED: ObservedStatus.TERMINATED,
    WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TIMED_OUT: ObservedStatus.TIMED_OUT,
    # Still open: running, paused, or (a race on describe) just continued as new.
    WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING: ObservedStatus.RUNNING,
    WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_PAUSED: ObservedStatus.RUNNING,
    WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW: ObservedStatus.RUNNING,
}


class TemporalError(Exception):
    """A control-plane call failed: the frontend is unreachable or refused the request."""


class ObservationIncomplete(TemporalError):
    """:meth:`Control.observe` could not reach the end of the pinned chain within one
    call's budget (:data:`CHAIN_READS` runs); the next call continues where this one
    stopped. Not a fact about the job: like an unreachable frontend it means "cannot say",
    so the sweeper retries and a buyer's cancel is refused. Reporting the chain as still
    running instead would let a buyer cancel, and be refunded, after the job delivered."""


class BuyerLookupError(TemporalError):
    """:meth:`Control.observe` read the seller's side but could not read the *buyer's*
    namespace (the provenance or cancellation lookup of decisions I15/I16).

    Distinct from a seller-side or Temporal-wide failure because the buyer controls that
    namespace's load: persistence limits are per namespace and shared with every caller, so
    a buyer that saturates its own namespace can keep this lookup failing. A gate that
    refunds a job it has not been able to observe for a while (the deadline plus a give-up
    period) must not do so on this error, or a buyer could take a delivery and have the job
    refunded by keeping its own namespace busy past the deadline. Like every
    :class:`TemporalError` it means "cannot say": the sweeper asks again, and a buyer's
    cancel is refused meanwhile."""


class EndpointConflict(TemporalError):
    """:meth:`Control.create_endpoint` for a name that exists with another target."""


class Occupation(StrEnum):
    """What :meth:`Control.occupy_job` found."""

    #: ``job-<id>`` now holds the closed placeholder (or already did): no start with
    #: ``REJECT_DUPLICATE`` / ``FAIL`` -- the seller's handler's -- can succeed for the id
    #: while the namespace retains it.
    OCCUPIED = "occupied"
    #: Some other run of ``job-<id>`` exists (running or closed): a call, or the seller,
    #: started it first. The cancel must go through the running-job path (observe it, then
    #: stop it), as for any job that started.
    STARTED = "started"


@dataclass(frozen=True, slots=True)
class EndpointInfo:
    """A Nexus endpoint as the operator API reports it. ``namespace`` / ``task_queue`` are
    the worker target (empty for an endpoint with another kind of target)."""

    id: str
    name: str
    namespace: str
    task_queue: str


def input_size(payload: Payload) -> int:
    """The size, in bytes, of an ``execute`` input as the seller's handler receives it: what
    a claim-check reference says the stored ciphertext is (its ``size``: what the handler
    would fetch), else the serialized payload. The one measure both sides use for a hold's
    ``input_size`` (decisions I16): the gate on the input the quote priced, the handler on
    the input ``execute`` brings, before fetching anything.

    The gate reads a reference only after the seller's own ``quote`` fetched the blob it
    names and checked it against that size and digest (a quote on a reference that lies
    fails), so the size it records is the seller's verified figure, not the buyer's word.
    Raises :class:`~kontra.codec.InvalidPayload` for a malformed reference."""
    if payload.metadata.get("encoding") == CLAIM_CHECK_ENCODING:
        _digest, size = _reference(payload.data)
        return size
    return payload.ByteSize()


@dataclass(frozen=True, slots=True)
class BuyerCall:
    """The buyer's ``execute`` call of one job as the gate's own records describe it: what
    a run of ``job-<id>`` must trace back to before its result can be a delivery
    (decisions I15). Built from the job row, never from anything a history says.

    ``namespace``: the buyer's. ``endpoint``: the offering version's endpoint name (the
    catalog's). ``service``: the offering's Nexus service name. ``holds``:
    :func:`hold_digest` of every hold token the gate issued for this job. The call must
    carry one of them: the token is what the seller's handler checked before it started
    the job, so it is what ties a call to a job (an operation id is the buyer's choice
    and binds nothing)."""

    namespace: str
    endpoint: str
    service: str
    holds: frozenset[str]


#: Nexus handler error types (``nexus_handler_failure_info.type``) that tell how far a buyer's
#: call of a job that never started got (decisions I40; kontra-cloud's FIXUPS #22).
#:
#: ``BAD_REQUEST``: ``kontra.serve`` refuses a call with it, final, before any workflow
#: exists (the hold, the input or its price): "refused now, it refunds with no blame".
HANDLER_BAD_REQUEST = "BAD_REQUEST"
#: ``UPSTREAM_TIMEOUT``: no handler answered the attempt in time: no worker of the seller's
#: polls the endpoint's task queue, or its handler took too long (server 1.32, measured:
#: ``{message: "upstream timeout", nexus_handler_failure_info {type: UPSTREAM_TIMEOUT}}``
#: about 10 s after the attempt, in both cases).
HANDLER_UPSTREAM_TIMEOUT = "UPSTREAM_TIMEOUT"
#: ``INTERNAL``: the handler failed. ``kontra.serve`` declares it final when the seller's own
#: code is at fault (its quote function), and leaves it retryable when the cause may be
#: anyone's (a claim-check the bucket or the gate did not serve, a Temporal RPC).
HANDLER_INTERNAL = "INTERNAL"


class CallStatus(StrEnum):
    """A standalone Nexus operation's status (``NexusOperationExecutionStatus``)."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    TERMINATED = "terminated"
    TIMED_OUT = "timed_out"


_CALL_STATUS: Mapping[int, CallStatus] = {
    NexusOperationExecutionStatus.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING: CallStatus.RUNNING,
    NexusOperationExecutionStatus.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED: (
        CallStatus.COMPLETED
    ),
    NexusOperationExecutionStatus.NEXUS_OPERATION_EXECUTION_STATUS_FAILED: CallStatus.FAILED,
    NexusOperationExecutionStatus.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED: CallStatus.CANCELED,
    NexusOperationExecutionStatus.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED: (
        CallStatus.TERMINATED
    ),
    NexusOperationExecutionStatus.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT: (
        CallStatus.TIMED_OUT
    ),
}
_FINAL = NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE


@dataclass(frozen=True, slots=True)
class CallFailure:
    """How the last finished attempt of a Nexus call failed, as the server recorded it
    (``last_attempt_failure``): the handler error's ``type`` ("" for a failure that is not a
    handler error) and whether the handler declared it ``final`` (non-retryable)."""

    type: str
    final: bool = False


@dataclass(frozen=True, slots=True)
class CallRecord:
    """A standalone Nexus operation in a buyer's namespace, as the frontend describes it:
    structured fields the server wrote, nothing decoded. What the Temporal adapter reads to
    tell how far the buyer's ``execute`` call of a job got when no run of it verified
    (``kontra.engine.Reach``, decisions I40; ported unchanged from kontra-cloud's
    ``BuyerOperation``, FIXUPS #22).

    ``started``: the handler took the call (it answered with an asynchronous start: the
    operation has a token, or is in state ``STARTED``). ``failure``: how the last finished
    attempt to deliver the call failed, or None when none failed (none finished yet, or the
    call was taken). What each outcome looked like on server 1.32 (measured): no worker or a
    handler slower than an attempt's ~10 s -- ``RUNNING`` / ``TIMED_OUT`` with an
    ``UPSTREAM_TIMEOUT`` failure (also when a buyer's own schedule-to-close of 12 s or more ran
    out: the last attempt's failure stays); a schedule-to-close shorter than one attempt --
    ``TIMED_OUT`` with a failure that is no handler error; a refusal -- ``FAILED``,
    ``BAD_REQUEST`` final; a retryable handler error -- ``RUNNING``, ``INTERNAL``; a buyer
    that terminated its call before any attempt finished -- ``TERMINATED`` with no failure.

    ``scheduled_at`` / ``expires_at``: when the call was made and when its schedule-to-close
    runs out (None: no limit), on the server's clock. The buyer chooses both for a call its
    own client starts.

    ``cancel_requested``: the operation's ``cancellation_info`` is set -- someone asked to
    cancel the call, which only the buyer can (nobody else acts in its namespace; the gate
    never cancels a buyer's call), as :meth:`Control.observe` reads it for
    ``buyer_canceled``. Not the same as ``status`` ``CANCELED``: the seller's handler ends a
    call ``CANCELED`` by failing its start with a canceled ``OperationError``, with no
    cancellation info; and a call the buyer cancels before any worker took it stays
    ``RUNNING`` (then ``TIMED_OUT``) with the info set (server 1.32, measured)."""

    endpoint: str
    service: str
    operation: str
    headers: Mapping[str, str]
    status: CallStatus
    started: bool = False
    failure: CallFailure | None = None
    scheduled_at: datetime | None = None
    expires_at: datetime | None = None
    cancel_requested: bool = False

    def is_call_of(self, call: BuyerCall) -> bool:
        """Whether this is ``call``'s job's ``execute`` call: its endpoint and service, and one
        of its hold tokens in the header, matched exactly as :meth:`Control.observe` matches
        a run's call (:func:`_carries_hold`)."""
        return (
            self.endpoint == call.endpoint
            and self.service == call.service
            and self.operation == EXECUTE_OPERATION
            and _carries_hold(self.headers, call.holds)
        )


def call_record(info: NexusOperationExecutionInfo) -> CallRecord:
    """The facts of a ``DescribeNexusOperationExecution`` answer the adapter reads."""
    failure: CallFailure | None = None
    if info.HasField("last_attempt_failure"):
        f = info.last_attempt_failure
        if f.HasField("nexus_handler_failure_info"):
            h = f.nexus_handler_failure_info
            failure = CallFailure(h.type, final=h.retry_behavior == _FINAL)
        else:
            failure = CallFailure("")
    return CallRecord(
        endpoint=info.endpoint,
        service=info.service,
        operation=info.operation,
        headers=dict(info.nexus_header),
        status=_CALL_STATUS.get(info.status, CallStatus.RUNNING),
        started=bool(info.operation_token)
        or info.state == PendingNexusOperationState.PENDING_NEXUS_OPERATION_STATE_STARTED,
        failure=failure,
        scheduled_at=_time(info.schedule_time, info.HasField("schedule_time")),
        expires_at=_time(info.expiration_time, info.HasField("expiration_time")),
        cancel_requested=info.HasField("cancellation_info"),
    )


class NexusSyncKind(StrEnum):
    #: The handler answered; ``payload`` is its result.
    OK = "ok"
    #: The handler (or the seller's worker) refused or crashed; ``message`` says why.
    HANDLER_ERROR = "handler_error"
    #: A worker is polling but no answer came within the timeout.
    TIMEOUT = "timeout"
    #: Nobody can answer: no worker polls the task queue, or the frontend is unreachable.
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class NexusSyncResult:
    kind: NexusSyncKind
    payload: Payload | None = None
    message: str = ""


@dataclass(frozen=True, slots=True)
class HistorySummary:
    """Platform-observable progress (§8.4), read from history *metadata* only: event types,
    activity type names and counts, never a payload. Counts cover the whole continue-as-new
    chain."""

    state: ObservedStatus
    last_event: str | None = None
    activities_completed: int = 0
    activities_running: tuple[str, ...] = ()
    continue_as_new_count: int = 0


@dataclass(slots=True)
class _RunScan:
    """One read of a run's history: its length (last event id) and what the summary takes
    from it. Valid while the run's ``history_length`` is unchanged."""

    run_id: str
    length: int
    completed: int
    last: str | None
    previous: str


@dataclass(frozen=True, slots=True)
class _Skip:
    """From a closed run back to ``target`` (exclusive; "" is the chain's start): ``runs``
    runs with ``completed`` activities completed among them."""

    target: str
    runs: int
    completed: int


@runtime_checkable
class TemporalControl(Protocol):
    """What a gate needs of the control plane: :class:`Control`, or a test double."""

    async def ensure_namespace(
        self, namespace: str, retention: timedelta = DEFAULT_RETENTION
    ) -> None: ...

    async def find_endpoint(self, name: str) -> EndpointInfo | None: ...

    async def create_endpoint(self, name: str, namespace: str, task_queue: str) -> EndpointInfo: ...

    async def nexus_sync(
        self,
        namespace: str,
        task_queue: str,
        service: str,
        operation: str,
        payload: Payload,
        headers: Mapping[str, str],
        timeout: float,  # noqa: ASYNC109 -- the Nexus request timeout, not an asyncio deadline
    ) -> NexusSyncResult: ...

    async def observe(
        self, namespace: str, workflow_id: str, call: BuyerCall, pinned: str | None
    ) -> tuple[Observation, bytes | None]: ...

    async def history_summary(
        self, namespace: str, workflow_id: str, pinned: str | None = None
    ) -> HistorySummary | None: ...

    async def history_size(self, namespace: str, workflow_id: str) -> tuple[int, int] | None: ...

    async def cancel_workflow(
        self, namespace: str, workflow_id: str, reason: str, first_run_id: str = ""
    ) -> None: ...

    async def terminate_workflow(
        self, namespace: str, workflow_id: str, reason: str, first_run_id: str = ""
    ) -> None: ...

    async def occupy_job(self, namespace: str, workflow_id: str, reason: str) -> Occupation: ...

    async def describe_task_queue(
        self, namespace: str, task_queue: str
    ) -> dict[m.TaskQueueType, m.TaskQueueTypeInfo]: ...

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
    ) -> None: ...

    async def describe_call(self, namespace: str, operation_id: str) -> CallRecord | None: ...


def internal_relay_service(
    control: TemporalControl,
) -> Callable[[str], Awaitable[RelayService]] | None:
    """The stream relay's frontend where it is the control plane's own: a dev server (and a
    fake gate on one), whose single frontend judges no relay token, so the token is not sent
    and the relay reads through this control plane's client. None for a control plane
    without a client of its own (a test double). A production gate relays on the PUBLIC
    frontend with the relay token instead (decisions I30); this is the one place the relay
    is handed the internal frontend, in the only code that talks to it (seam ⑥: the
    ``relay_service`` of :class:`kontra.engine.temporal.TemporalEngine`)."""
    client: Any = getattr(control, "client", None)
    if client is None:
        return None
    service: RelayService = client.workflow_service

    async def connect(token: str) -> RelayService:
        return service

    return connect


def _time(ts: Timestamp, present: bool) -> datetime | None:
    return ts.ToDatetime(tzinfo=UTC) if present else None


#: Temporal's task-queue types, by their names on the gate's wire.
_TASK_QUEUE_TYPES: Mapping[m.TaskQueueType, TaskQueueType.ValueType] = {
    m.TaskQueueType.WORKFLOW: TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
    m.TaskQueueType.ACTIVITY: TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY,
    m.TaskQueueType.NEXUS: TaskQueueType.TASK_QUEUE_TYPE_NEXUS,
}


def _at_least_zero(value: float) -> float:
    """A rate or an age as reported, or zero for one below zero or not finite (Temporal's
    approximations; the wire model refuses either)."""
    return value if math.isfinite(value) and value > 0 else 0.0


#: The longest poller identity and deployment name, and build id, SDK name and SDK version,
#: the gate's wire carries (:class:`kontra.gate_client.models.PollerInfo`: Temporal's default
#: ID limit, and 255). Temporal bounds a poll's identity and build id itself; a deprecated
#: deployment series name and a worker's heartbeat are not bounded everywhere, so what is
#: longer is cut (an identity) or left out rather than failing the whole description.
_POLLER_ID_MAX = 1000
_POLLER_NAME_MAX = 255
#: A task queue ``ListWorkers``' query can name between single quotes as it is: every one
#: :mod:`kontra.contract.names` builds (``acme/enrich@1.2.0``).
_QUOTABLE_TASK_QUEUE = re.compile(r"[A-Za-z0-9@./_:-]+", re.ASCII)
#: The workers of one task queue ``ListWorkers`` is asked for (one page).
_WORKERS_PAGE = 1000

#: What a worker's heartbeat says it runs: (``sdk_name``, ``sdk_version``), by identity.
_Sdks = Mapping[str, tuple[str, str]]


def _fit(value: str, longest: int) -> str | None:
    """``value``, or None when it is empty or longer than the wire carries."""
    return value if value and len(value) <= longest else None


def _poller(p: PollerInfo, sdks: _Sdks) -> m.PollerInfo:
    """Temporal's ``PollerInfo``, with the version the worker named when it polled (its
    deployment's, else its version capabilities': an empty string is no version) and the SDK
    its heartbeat named (``sdks``, from ``ListWorkers``)."""
    deployment, capabilities = p.deployment_options, p.worker_version_capabilities
    sdk_name, sdk_version = sdks.get(p.identity, ("", ""))
    return m.PollerInfo(
        identity=p.identity[:_POLLER_ID_MAX],
        last_access_time=p.last_access_time.ToDatetime(tzinfo=UTC),
        rate_per_second=p.rate_per_second if math.isfinite(p.rate_per_second) else 0.0,
        build_id=_fit(deployment.build_id or capabilities.build_id, _POLLER_NAME_MAX),
        deployment_name=_fit(
            deployment.deployment_name or capabilities.deployment_series_name, _POLLER_ID_MAX
        ),
        sdk_name=_fit(sdk_name, _POLLER_NAME_MAX),
        sdk_version=_fit(sdk_version, _POLLER_NAME_MAX),
    )


def _sdks(task_queue: str, workers: Iterable[WorkerListInfo]) -> dict[str, tuple[str, str]]:
    """The SDK each worker of ``task_queue`` named in its heartbeat, by identity; of two
    workers with one identity, the one started last."""
    found: dict[str, tuple[int, str, str]] = {}
    for w in workers:
        if w.task_queue != task_queue or not (w.sdk_name or w.sdk_version):
            continue
        started = w.start_time.ToNanoseconds()
        seen = found.get(w.worker_identity)
        if seen is None or started > seen[0]:
            found[w.worker_identity] = (started, w.sdk_name, w.sdk_version)
    return {identity: (name, version) for identity, (_, name, version) in found.items()}


def _type_info(resp: DescribeTaskQueueResponse, sdks: _Sdks) -> m.TaskQueueTypeInfo:
    """One type's answer: the most recently seen pollers first, and the stats with
    ``backlog_increase_rate`` as Temporal's SDKs derive it (add rate minus dispatch rate)."""
    s = resp.stats
    add, dispatch = _at_least_zero(s.tasks_add_rate), _at_least_zero(s.tasks_dispatch_rate)
    recent = sorted(resp.pollers, key=lambda p: p.last_access_time.ToNanoseconds(), reverse=True)
    return m.TaskQueueTypeInfo(
        pollers=[_poller(p, sdks) for p in recent[: m.POLLERS_MAX]],
        stats=m.TaskQueueStats(
            approximate_backlog_count=max(0, s.approximate_backlog_count),
            approximate_backlog_age=_at_least_zero(
                s.approximate_backlog_age.ToTimedelta().total_seconds()
            ),
            tasks_add_rate=add,
            tasks_dispatch_rate=dispatch,
            backlog_increase_rate=add - dispatch,
        ),
    )


def _event_name(event_type: int) -> str:
    """``EVENT_TYPE_ACTIVITY_TASK_COMPLETED`` -> ``ActivityTaskCompleted`` (the name the
    Temporal UI and CLI show)."""
    try:
        raw = EventType.Name(event_type)  # type: ignore[arg-type]
    except ValueError:
        return f"Event{event_type}"
    return "".join(p.capitalize() for p in raw.removeprefix("EVENT_TYPE_").split("_"))


def _check_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Caller headers for a Nexus request: HTTP tokens, no CR/LF, never one the relay
    owns. Lowercased, as the handler sees them (``ctx.headers``)."""
    out: dict[str, str] = {}
    for name, value in headers.items():
        lower = name.lower()
        if (
            not _HTTP_TOKEN.fullmatch(name)
            or lower in _RESERVED_HEADERS
            or lower.startswith(_RESERVED_HEADER_PREFIXES)
        ):
            raise ValueError(f"header {name!r} cannot be relayed")
        if any(c in value for c in "\r\n\0") or len(value) > 16 * 1024:
            raise ValueError(f"header {name!r} has an invalid value")
        out[lower] = value
    return out


def _failure_message(body: bytes, fallback: str) -> str:
    """The message of a Nexus failure body (``{"message", "details": {"cause": ...}}``),
    with the innermost cause's message when the outer one is generic."""
    try:
        parsed: object = json.loads(body)
    except ValueError:
        text = body.decode("utf-8", "replace").strip()
        return (text or fallback)[:_MESSAGE_MAX]
    if not isinstance(parsed, dict):
        return fallback
    node: dict[str, Any] = {str(k): v for k, v in parsed.items()}  # pyright: ignore[reportUnknownVariableType]
    parts: list[str] = []
    for _ in range(8):
        msg = node.get("message")
        if isinstance(msg, str) and msg and msg not in parts:
            parts.append(msg)
        details = node.get("details")
        cause = details.get("cause") if isinstance(details, dict) else None  # pyright: ignore[reportUnknownMemberType]
        if not isinstance(cause, dict):
            break
        node = {str(k): v for k, v in cause.items()}  # pyright: ignore[reportUnknownVariableType]
    return (": ".join(parts) or fallback)[:_MESSAGE_MAX]


def _payload_from(resp: httpx.Response) -> Payload | None:
    """The handler's sync result, from the body the frontend serialized it to. The
    frontend sends a ``json/plain`` payload as ``application/json``, ``binary/plain`` as
    ``application/octet-stream``, ``binary/null`` as an empty body, and anything else as
    the serialized proto (``application/x-temporal-payload``)."""
    ctype = resp.headers.get("content-type", "")
    media, *params = (p.strip() for p in ctype.split(";"))
    media = media.lower()
    body = resp.content
    if media == "application/x-temporal-payload":
        payload = Payload()
        try:
            payload.ParseFromString(body)
        except DecodeError:
            return None
        return payload
    if media == "application/json":
        options = dict(p.partition("=")[::2] for p in params)
        if options.get("format") == "protobuf":
            meta = {"encoding": b"json/protobuf"}
            if options.get("message-type"):
                meta["messageType"] = options["message-type"].encode()
            return Payload(metadata=meta, data=body)
        return Payload(metadata={"encoding": _JSON_PLAIN}, data=body)
    if media == "application/octet-stream":
        return Payload(metadata={"encoding": b"binary/plain"}, data=body)
    if not media and not body:
        return Payload(metadata={"encoding": b"binary/null"})
    return None


def _namespace_missing(e: RPCError) -> bool:
    """A NOT_FOUND about the namespace rather than the workflow: a platform fault (the
    namespace is the gate's to create), never a fact about a job."""
    if e.status != RPCStatusCode.NOT_FOUND:
        return False
    details = [d.type_url for d in e.grpc_status.details]
    if details:
        return any(t.endswith("NamespaceNotFoundFailure") for t in details)
    return e.message.lower().startswith("namespace")


class _Gone(Exception):
    """A history that does not exist (any more): never a platform fault."""


class _Unnamed(Exception):
    """A history named by ids the frontend refuses as malformed (``INVALID_ARGUMENT``):
    such ids come only from a link someone wrote, never from the server, so they name
    nothing. Raising instead would make every observation of the run fail, and a gate that
    cannot observe refuses the buyer's cancel: a seller could make its job uncancelable by
    starting a run whose callback links carry an invalid run id."""


@dataclass(frozen=True, slots=True)
class _Target:
    """The seller-side run whose provenance is being established: its chain's first run
    and the request id that run was started with."""

    namespace: str
    workflow_id: str
    root: str
    request_id: str


@dataclass(frozen=True, slots=True)
class _Found:
    """What resolving one caller link in the buyer's namespace found: whether the call it
    names started the run (``verdict``) and, when asked, whether the buyer requested that
    call's cancellation (``cancel_requested``)."""

    verdict: Provenance
    cancel_requested: bool = False


#: History events that close a workflow's Nexus operation (each names its scheduled event).
_OPERATION_CLOSES = (
    "nexus_operation_completed_event_attributes",
    "nexus_operation_failed_event_attributes",
    "nexus_operation_canceled_event_attributes",
    "nexus_operation_timed_out_event_attributes",
)


def _cancel_requested(e: HistoryEvent, scheduled_event_id: int) -> bool:
    """``e`` is the calling workflow's request to cancel the operation it scheduled at
    ``scheduled_event_id``."""
    if not e.HasField("nexus_operation_cancel_requested_event_attributes"):
        return False
    return e.nexus_operation_cancel_requested_event_attributes.scheduled_event_id == (
        scheduled_event_id
    )


def _closes_operation(e: HistoryEvent, scheduled_event_id: int) -> bool:
    return any(
        e.HasField(name) and getattr(e, name).scheduled_event_id == scheduled_event_id
        for name in _OPERATION_CLOSES
    )


def _remember(cache: OrderedDict[_K, _V], key: _K, value: _V) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > _CHAIN_CACHE:
        cache.popitem(last=False)


def _successor(close: HistoryEvent) -> str:
    """The run a closed run handed over to, as the server recorded it; "" at a chain's end."""
    if close.HasField("workflow_execution_continued_as_new_event_attributes"):
        return close.workflow_execution_continued_as_new_event_attributes.new_execution_run_id
    if close.HasField("workflow_execution_completed_event_attributes"):
        return close.workflow_execution_completed_event_attributes.new_execution_run_id
    if close.HasField("workflow_execution_failed_event_attributes"):
        return close.workflow_execution_failed_event_attributes.new_execution_run_id
    if close.HasField("workflow_execution_timed_out_event_attributes"):
        return close.workflow_execution_timed_out_event_attributes.new_execution_run_id
    return ""


def _namespace_of(link: Link) -> str:
    """The namespace a caller's link points into, for the two kinds a Nexus caller leaves
    (a standalone operation; an event of the calling workflow)."""
    if link.HasField("nexus_operation"):
        return link.nexus_operation.namespace
    if link.HasField("workflow_event"):
        return link.workflow_event.namespace
    return ""


def _carries_hold(header: Mapping[str, str], holds: frozenset[str]) -> bool:
    """Whether a call's Nexus header carries one of the job's hold tokens. Nexus header
    names reach the handler lowercased, so every ASCII spelling of the name counts: a buyer
    cannot send the token under ``Kontra-Hold`` and have escrow refuse a run the handler
    accepted. Only ASCII folds: ``str.lower`` also maps the Kelvin sign to ``k``, and a
    name HTTP refuses never reaches a handler. The value is matched by
    :func:`hold_digest`; it is never decoded."""
    return any(
        hold_digest(value) in holds
        for name, value in header.items()
        if name.isascii() and name.lower() == HOLD_HEADER
    )


def _is_the_call(
    endpoint: str,
    service: str,
    operation: str,
    request_id: str,
    header: Mapping[str, str],
    target: _Target,
    call: BuyerCall,
) -> bool:
    """A recorded Nexus call (a standalone operation, or a workflow's scheduled one) is
    this job's ``execute`` call and the one ``target`` was started with: every field equal."""
    return (
        endpoint == call.endpoint
        and service == call.service
        and operation == EXECUTE_OPERATION
        and request_id == target.request_id
        and _carries_hold(header, call.holds)
    )


def _names_only(links: Iterable[Link], target: _Target) -> bool:
    """False when the caller recorded some other run of the job's workflow as the one its
    call started (the handler's response link, written once when the start was answered).
    No such link at all is not evidence either way."""
    for link in links:
        if link.HasField("workflow_event"):
            ns, wf, run = (
                link.workflow_event.namespace,
                link.workflow_event.workflow_id,
                link.workflow_event.run_id,
            )
        elif link.HasField("workflow"):
            ns, wf, run = link.workflow.namespace, link.workflow.workflow_id, link.workflow.run_id
        else:
            continue
        if (ns, wf) == (target.namespace, target.workflow_id) and run != target.root:
            return False
    return True


class Control:
    """``Control(client, http_base)``: ``client`` is connected to the internal frontend
    (or a dev server); ``http_base`` is that frontend's HTTP API (``http://host:port``),
    used for the quote relay.

    ``http``: an ``httpx.AsyncClient`` to use for the relay. Without one, Control makes its
    own on first use and closes it in :meth:`aclose`; a call from an event loop other than
    the one that client was made on (a gate serving from its own thread) uses a client of
    its own for that call, because pooled connections belong to one loop. :meth:`aclose`
    closes the client on the loop that made it.
    """

    def __init__(
        self,
        client: Client,
        http_base: str,
        *,
        http: httpx.AsyncClient | None = None,
        identity: str = IDENTITY,
    ) -> None:
        self._client = client
        self._http_base = http_base.rstrip("/")
        self._given_http = http
        self._own_http: httpx.AsyncClient | None = None
        self._own_loop: asyncio.AbstractEventLoop | None = None
        self._identity = identity
        #: closed run id -> what lies between it and an earlier point of its chain
        self._skips: OrderedDict[str, _Skip] = OrderedDict()
        #: (namespace, workflow id) -> what the last full read of its current run found
        self._runs: OrderedDict[tuple[str, str], _RunScan] = OrderedDict()
        #: (namespace, workflow id, root, call) -> a definite provenance
        self._verdicts: OrderedDict[tuple[str, str, str, BuyerCall], Provenance] = OrderedDict()
        #: (namespace, workflow id, pinned root) -> the latest run of the chain reached so far
        self._heads: OrderedDict[tuple[str, str, str], str] = OrderedDict()
        #: (namespace, workflow id, root, call) -> whether the buyer asked to cancel the call
        #: that started the (canceled, so closed) run
        self._cancels: OrderedDict[tuple[str, str, str, BuyerCall], bool] = OrderedDict()

    @property
    def client(self) -> Client:
        return self._client

    @property
    def http_base(self) -> str:
        return self._http_base

    async def aclose(self) -> None:
        """Close the relay's own HTTP client. Pooled connections belong to the loop that
        made the client: from another loop, the close is scheduled on that one (if it is
        still running) rather than left to the garbage collector."""
        own, loop = self._own_http, self._own_loop
        self._own_http = self._own_loop = None
        if own is None:
            return
        if loop is asyncio.get_running_loop():
            await own.aclose()
        elif loop is not None and loop.is_running():
            asyncio.run_coroutine_threadsafe(own.aclose(), loop)

    # ------------------------------------------------------------------ plumbing

    @property
    def _wf(self) -> Any:
        return self._client.workflow_service

    @property
    def _op(self) -> Any:
        return self._client.operator_service

    async def _rpc(self, what: str, call: Any, req: Any, *, retry: bool = True) -> Any:
        try:
            return await call(req, retry=retry, timeout=_RPC_TIMEOUT)
        except RPCError as e:
            raise TemporalError(f"{what}: {e.status.name}: {e.message}") from e

    # ------------------------------------------------------------------ namespaces

    async def ensure_namespace(
        self, namespace: str, retention: timedelta = DEFAULT_RETENTION
    ) -> None:
        """Register ``namespace`` if it is missing, then wait until the frontend both
        describes it and serves requests in it. Idempotent; concurrent callers converge.

        An existing namespace's retention is raised to ``retention`` when it is shorter and
        never lowered: a namespace registered with a shorter one (an older gate's default)
        would let a still-valid hold token start ``job-<id>`` again once its first run was
        dropped (:data:`DEFAULT_RETENTION`), and a longer one is someone's decision.

        Registration comes first, without a "does it exist?" read: asking the frontend
        about a namespace that does not exist yet makes it remember the absence for about
        a second (server 1.32, measured), so a namespace registered right after such a
        read is refused by workflow calls for that long."""
        if not namespace:
            raise ValueError("namespace must not be empty")
        if retention <= timedelta(0):
            raise ValueError("retention must be positive")
        req = RegisterNamespaceRequest(namespace=namespace)
        req.workflow_execution_retention_period.FromTimedelta(retention)
        existed = False
        try:
            await self._wf.register_namespace(req, retry=True, timeout=_RPC_TIMEOUT)
        except RPCError as e:
            if e.status != RPCStatusCode.ALREADY_EXISTS:
                raise TemporalError(f"register namespace {namespace}: {e.message}") from e
            existed = True
        delay, waited = 0.02, 0.0
        while not await self._usable(namespace):
            if waited >= _NAMESPACE_WAIT:
                raise TemporalError(f"namespace {namespace} was registered but never appeared")
            await asyncio.sleep(delay)
            waited += delay
            delay = min(delay * 2, 0.5)
        if existed:
            await self._keep_at_least(namespace, retention)

    async def _keep_at_least(self, namespace: str, retention: timedelta) -> None:
        d: DescribeNamespaceResponse = await self._rpc(
            f"describe namespace {namespace}",
            self._wf.describe_namespace,
            DescribeNamespaceRequest(namespace=namespace),
        )
        if d.config.workflow_execution_retention_ttl.ToTimedelta() >= retention:
            return
        # Only the retention is set: UpdateNamespace leaves every config field it is not
        # given (archival, bad binaries) as it was.
        update = UpdateNamespaceRequest(namespace=namespace)
        update.config.workflow_execution_retention_ttl.FromTimedelta(retention)
        await self._rpc(f"update namespace {namespace}", self._wf.update_namespace, update)

    async def _usable(self, namespace: str) -> bool:
        """Describable, and a workflow call in it is answered for the workflow rather than
        refused for the namespace (the frontend's namespace cache has it)."""
        try:
            await self._wf.describe_namespace(
                DescribeNamespaceRequest(namespace=namespace), retry=True, timeout=_RPC_TIMEOUT
            )
            await self._wf.describe_workflow_execution(
                DescribeWorkflowExecutionRequest(
                    namespace=namespace,
                    execution=WorkflowExecution(workflow_id="kontra-probe"),
                ),
                retry=True,
                timeout=_RPC_TIMEOUT,
            )
        except RPCError as e:
            if e.status != RPCStatusCode.NOT_FOUND:
                raise TemporalError(f"probe namespace {namespace}: {e.message}") from e
            return not _namespace_missing(e)
        return True

    # ------------------------------------------------------------------ endpoints

    @staticmethod
    def _info(e: Endpoint) -> EndpointInfo:
        target = e.spec.target
        worker = target.worker if target.HasField("worker") else None
        return EndpointInfo(
            id=e.id,
            name=e.spec.name,
            namespace=worker.namespace if worker is not None else "",
            task_queue=worker.task_queue if worker is not None else "",
        )

    async def find_endpoint(self, name: str) -> EndpointInfo | None:
        """The endpoint called exactly ``name``, or None."""
        resp = await self._rpc(
            "list nexus endpoints",
            self._op.list_nexus_endpoints,
            ListNexusEndpointsRequest(name=name, page_size=10),
        )
        for e in resp.endpoints:
            if e.spec.name == name:
                return self._info(e)
        return None

    async def create_endpoint(self, name: str, namespace: str, task_queue: str) -> EndpointInfo:
        """Create the endpoint ``name`` targeting ``namespace`` / ``task_queue``.

        ``name`` must be one :func:`~kontra.contract.names.new_endpoint_name` made: the
        catalog assigns it, and nothing else may ever name an endpoint (decisions I1). The
        call is idempotent for the same name and target and refuses a different target
        (:class:`EndpointConflict`): an endpoint is never repointed."""
        if not is_endpoint_name(name):
            raise ValueError("endpoint names come from kontra.contract.names.new_endpoint_name()")
        if not namespace or not task_queue:
            raise ValueError("an endpoint needs a target namespace and task queue")
        existing = await self.find_endpoint(name)
        if existing is None:
            spec = EndpointSpec(
                name=name,
                target=EndpointTarget(
                    worker=EndpointTarget.Worker(namespace=namespace, task_queue=task_queue)
                ),
            )
            try:
                resp = await self._op.create_nexus_endpoint(
                    CreateNexusEndpointRequest(spec=spec), retry=True, timeout=_RPC_TIMEOUT
                )
            except RPCError as e:
                if e.status != RPCStatusCode.ALREADY_EXISTS:
                    raise TemporalError(f"create nexus endpoint: {e.message}") from e
                existing = await self.find_endpoint(name)  # a concurrent create won
                if existing is None:
                    raise TemporalError(f"endpoint {name} exists but cannot be read") from e
            else:
                return self._info(resp.endpoint)
        if (existing.namespace, existing.task_queue) != (namespace, task_queue):
            raise EndpointConflict(f"endpoint {name} exists with another target")
        return existing

    # ------------------------------------------------------------------ quote relay

    def _http_for_call(self) -> tuple[httpx.AsyncClient, bool]:
        """(client, close it after the call)."""
        if self._given_http is not None:
            return self._given_http, False
        loop = asyncio.get_running_loop()
        if self._own_http is None:
            self._own_http, self._own_loop = httpx.AsyncClient(trust_env=False), loop
        if self._own_loop is loop:
            return self._own_http, False
        return httpx.AsyncClient(trust_env=False), True

    async def nexus_sync(
        self,
        namespace: str,
        task_queue: str,
        service: str,
        operation: str,
        payload: Payload,
        headers: Mapping[str, str],
        timeout: float,  # noqa: ASYNC109 -- the Nexus request timeout, not an asyncio deadline
    ) -> NexusSyncResult:
        """Call a synchronous Nexus operation through the frontend's HTTP API, relaying
        ``payload`` byte for byte (it is the buyer's ciphertext), and classify the outcome.

        Never raises for something the seller did: a handler refusal or crash is
        ``HANDLER_ERROR``, no answer in time is ``TIMEOUT`` (or ``UNAVAILABLE`` when no
        worker polls the task queue at all), and an unreachable frontend is ``UNAVAILABLE``.
        """
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a positive number of seconds")
        hdrs = _check_headers(headers)
        url = (
            f"{self._http_base}/namespaces/{quote(namespace, safe='')}"
            f"/task-queues/{quote(task_queue, safe='')}"
            f"/nexus-services/{quote(service, safe='')}/{quote(operation, safe='')}"
        )
        header_ms = math.ceil((timeout + FRONTEND_RESERVE) * 1000)
        hdrs.update(
            {
                "content-type": "application/x-temporal-payload",
                "request-timeout": f"{header_ms}ms",
            }
        )
        http, close = self._http_for_call()
        try:
            resp = await http.post(
                url,
                content=payload.SerializeToString(),
                headers=hdrs,
                timeout=httpx.Timeout(timeout, connect=min(timeout, 5.0)),
            )
        except httpx.TimeoutException:
            return await self._timed_out(namespace, task_queue, "no answer within the timeout")
        except httpx.TransportError as e:
            return NexusSyncResult(
                NexusSyncKind.UNAVAILABLE, message=f"frontend unreachable: {type(e).__name__}"
            )
        finally:
            if close:
                await http.aclose()
        return await self._classify(namespace, task_queue, resp)

    async def _classify(
        self, namespace: str, task_queue: str, resp: httpx.Response
    ) -> NexusSyncResult:
        status = resp.status_code
        if status == 200:
            payload = _payload_from(resp)
            if payload is None:
                return NexusSyncResult(
                    NexusSyncKind.HANDLER_ERROR,
                    message=f"unreadable result ({resp.headers.get('content-type', 'no type')})",
                )
            return NexusSyncResult(NexusSyncKind.OK, payload=payload)
        message = _failure_message(resp.content, resp.reason_phrase or f"HTTP {status}")
        if status == 201:
            return NexusSyncResult(
                NexusSyncKind.HANDLER_ERROR,
                message="the operation started asynchronously; this call must be synchronous",
            )
        if status in (520, 504):
            return await self._timed_out(namespace, task_queue, message)
        if status in (502, 503, 429, 401, 403):
            return NexusSyncResult(NexusSyncKind.UNAVAILABLE, message=message)
        if status == 404 and "temporal-nexus-failure-source" not in resp.headers:
            # The frontend itself: no such namespace. The worker says "no handler" with
            # the failure-source header set.
            return NexusSyncResult(NexusSyncKind.UNAVAILABLE, message=message)
        # 424 (operation failed or canceled), 400, 404 (no handler), 500, 501, ...
        state = resp.headers.get("nexus-operation-state")
        if state:
            message = f"operation {state}: {message}"
        return NexusSyncResult(NexusSyncKind.HANDLER_ERROR, message=message)

    async def _timed_out(self, namespace: str, task_queue: str, message: str) -> NexusSyncResult:
        """A timeout looks the same whether the worker was slow or absent; the task queue's
        pollers tell them apart."""
        try:
            resp = await self._wf.describe_task_queue(
                DescribeTaskQueueRequest(
                    namespace=namespace,
                    task_queue=TaskQueue(name=task_queue),
                    task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_NEXUS,
                ),
                retry=False,
                timeout=timedelta(seconds=5),
            )
        except RPCError:
            return NexusSyncResult(NexusSyncKind.TIMEOUT, message=message)
        if not resp.pollers:
            return NexusSyncResult(
                NexusSyncKind.UNAVAILABLE, message=f"no worker is polling {task_queue!r}"
            )
        return NexusSyncResult(NexusSyncKind.TIMEOUT, message=message)

    # ------------------------------------------------------------------ task queues

    async def describe_task_queue(
        self, namespace: str, task_queue: str
    ) -> dict[m.TaskQueueType, m.TaskQueueTypeInfo]:
        """``DescribeTaskQueue`` of ``task_queue`` in ``namespace`` with ``report_stats``,
        once per task-queue type: each type's stats and pollers, Temporal metadata only (the
        Workers view, :mod:`kontra.workers`). The default mode lists pollers in every answer;
        ``report_pollers`` belongs to the deprecated enhanced mode. A task queue nobody has
        used is described all the same, with zeros.

        ``DescribeTaskQueue`` names no poller's SDK; when anything polls, ``ListWorkers``
        is asked for the workers' heartbeats on ``task_queue`` (:meth:`_worker_sdks`)."""
        answers: dict[m.TaskQueueType, DescribeTaskQueueResponse] = {}
        for kind, value in _TASK_QUEUE_TYPES.items():
            answers[kind] = await self._rpc(
                f"describe the {kind} task queue {task_queue} in {namespace}",
                self._wf.describe_task_queue,
                DescribeTaskQueueRequest(
                    namespace=namespace,
                    task_queue=TaskQueue(name=task_queue),
                    task_queue_type=value,
                    report_stats=True,
                ),
            )
        polled = any(resp.pollers for resp in answers.values())
        sdks = await self._worker_sdks(namespace, task_queue) if polled else {}
        return {kind: _type_info(resp, sdks) for kind, resp in answers.items()}

    async def _worker_sdks(self, namespace: str, task_queue: str) -> _Sdks:
        """What the workers of ``task_queue`` said they run in their last heartbeat
        (``ListWorkers``, Temporal's ``WorkerHeartbeat.sdk_name`` and ``sdk_version``), by
        identity: one page of workers. Empty for a task queue the query cannot name as it is,
        and from a server without ``ListWorkers`` (``UNIMPLEMENTED``): the SDK is then not
        known, and the rest of the description stands."""
        if _QUOTABLE_TASK_QUEUE.fullmatch(task_queue) is None:
            return {}
        try:
            resp: ListWorkersResponse = await self._wf.list_workers(
                ListWorkersRequest(
                    namespace=namespace,
                    query=f"TaskQueue = '{task_queue}'",
                    page_size=_WORKERS_PAGE,
                ),
                retry=True,
                timeout=_RPC_TIMEOUT,
            )
        except RPCError as e:
            if e.status == RPCStatusCode.UNIMPLEMENTED:
                return {}
            raise TemporalError(
                f"list the workers of {task_queue} in {namespace}: {e.status.name}: {e.message}"
            ) from e
        return _sdks(task_queue, resp.workers)

    # ------------------------------------------------------------------ observation

    async def _describe(
        self, namespace: str, workflow_id: str, run_id: str = ""
    ) -> DescribeWorkflowExecutionResponse | None:
        """The run ``run_id`` of ``workflow_id`` (its latest run when ""), or None."""
        try:
            return await self._wf.describe_workflow_execution(
                DescribeWorkflowExecutionRequest(
                    namespace=namespace,
                    execution=WorkflowExecution(workflow_id=workflow_id, run_id=run_id),
                ),
                retry=True,
                timeout=_RPC_TIMEOUT,
            )
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND and not _namespace_missing(e):
                return None
            raise TemporalError(f"describe {workflow_id} in {namespace}: {e.message}") from e

    async def _events(
        self, namespace: str, workflow_id: str, run_id: str, *, close_only: bool = False
    ) -> list[HistoryEvent]:
        filter_type = (
            HistoryEventFilterType.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
            if close_only
            else HistoryEventFilterType.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
        )
        events: list[HistoryEvent] = []
        token = b""
        while True:
            resp = await self._rpc(
                f"history of {workflow_id} in {namespace}",
                self._wf.get_workflow_execution_history,
                GetWorkflowExecutionHistoryRequest(
                    namespace=namespace,
                    execution=WorkflowExecution(workflow_id=workflow_id, run_id=run_id),
                    maximum_page_size=1000,
                    next_page_token=token,
                    history_event_filter_type=filter_type,
                    skip_archival=True,
                ),
            )
            events.extend(resp.history.events)
            token = resp.next_page_token
            if not token or (close_only and events):
                return events

    async def _pages(
        self, namespace: str, workflow_id: str, run_id: str, *, page_size: int = 1000
    ) -> AsyncGenerator[Sequence[HistoryEvent], None]:
        """A run's history page by page, so a reader that finds what it wants early stops
        reading. Raises :class:`_Gone` when the run does not exist (any more)."""
        token = b""
        while True:
            try:
                resp = await self._wf.get_workflow_execution_history(
                    GetWorkflowExecutionHistoryRequest(
                        namespace=namespace,
                        execution=WorkflowExecution(workflow_id=workflow_id, run_id=run_id),
                        maximum_page_size=page_size,
                        next_page_token=token,
                        skip_archival=True,
                    ),
                    retry=True,
                    timeout=_RPC_TIMEOUT,
                )
            except RPCError as e:
                if e.status == RPCStatusCode.NOT_FOUND and not _namespace_missing(e):
                    raise _Gone from e
                if e.status == RPCStatusCode.INVALID_ARGUMENT:
                    raise _Unnamed from e
                raise TemporalError(f"history of {workflow_id} in {namespace}: {e.message}") from e
            yield resp.history.events
            token = resp.next_page_token
            if not token:
                return

    async def _started_event(
        self, namespace: str, workflow_id: str, run_id: str
    ) -> HistoryEvent | None:
        """The run's ``WorkflowExecutionStarted`` event (one small page), or None when the
        run is gone."""
        try:
            async with aclosing(self._pages(namespace, workflow_id, run_id, page_size=1)) as pages:
                async for events in pages:
                    for e in events:
                        if e.HasField("workflow_execution_started_event_attributes"):
                            return e
                        return None
        except (_Gone, _Unnamed):
            return None
        return None

    async def _close_event(self, namespace: str, workflow_id: str, run_id: str) -> HistoryEvent:
        events = await self._events(namespace, workflow_id, run_id, close_only=True)
        if not events:
            raise TemporalError(f"{workflow_id} in {namespace} is closed but has no close event")
        return events[-1]

    async def observe(
        self, namespace: str, workflow_id: str, call: BuyerCall, pinned: str | None
    ) -> tuple[Observation, bytes | None]:
        """What escrow needs of ``workflow_id`` in ``namespace`` (a seller's): its status and
        times, whether the buyer's ``call`` started it (``provenance``, decisions I15) and,
        once a verified run completed, the result payload exactly as stored -- a serialized
        ``Payload``, ciphertext -- with that byte string's sha256 and size.

        ``pinned``: the chain escrow pinned (``JobRecord.pin``). With it, only that chain
        is reported: the latest run of the id when the server says it descends from
        ``pinned`` (its ``first_run_id``), else the chain's own last run, reached by following
        each continue-as-new, retry or cron successor the server recorded, each accepted only
        if its start names the run before it (:class:`ObservationIncomplete` when that takes
        more than one call). Any other run of the id is never reported. Without it, the latest
        run of the id is reported with the provenance of its chain's first run, and
        ``pin`` is that first run's id: escrow pins it when it is verified.

        A chain's first run is the server's ``first_run_id``, which only the server sets (on
        a continue-as-new, retry or cron successor; no start request carries it), read in one
        call however long the chain: a walk with a budget would leave a long chain's
        delivery unseen while a buyer cancels.

        A verified run that ended canceled carries ``buyer_canceled`` (decisions I16), and
        :meth:`occupy_job`'s placeholder is ``OCCUPIED``. Raises :class:`BuyerLookupError`
        when the seller's side was read but the buyer's namespace could not be, and
        :class:`TemporalError` for anything else that leaves it unable to say."""
        d = await self._describe(namespace, workflow_id)
        if pinned:
            return await self._observe_chain(namespace, workflow_id, call, pinned, d)
        if d is None:
            return Observation(status=ObservedStatus.NOT_FOUND), None
        root = d.workflow_execution_info.first_run_id
        if not root:
            return await self._report(namespace, workflow_id, call, d, None, Provenance.UNVERIFIED)
        provenance = await self._provenance(namespace, workflow_id, root, call)
        return await self._report(namespace, workflow_id, call, d, root, provenance)

    async def _report(
        self,
        namespace: str,
        workflow_id: str,
        call: BuyerCall,
        d: DescribeWorkflowExecutionResponse,
        root: str | None,
        provenance: Provenance,
        close: HistoryEvent | None = None,
    ) -> tuple[Observation, bytes | None]:
        """The observation of run ``d``. The result is read only for a verified completion:
        nothing else can be a delivery. A verified run that ended canceled says whether the
        buyer asked for it (:meth:`_buyer_canceled`)."""
        info = d.workflow_execution_info
        status = _STATUS.get(info.status, ObservedStatus.RUNNING)
        obs = Observation(
            status=status,
            started_at=_time(info.start_time, info.HasField("start_time")),
            closed_at=_time(info.close_time, info.HasField("close_time")),
            provenance=provenance,
            pin=root,
        )
        if status is ObservedStatus.CANCELED and provenance is Provenance.VERIFIED and root:
            canceled = await self._buyer_canceled(namespace, workflow_id, root, call)
            return replace(obs, buyer_canceled=canceled), None
        if status is not ObservedStatus.COMPLETED or provenance is not Provenance.VERIFIED:
            return obs, None
        if close is None:
            close = await self._close_event(namespace, workflow_id, info.execution.run_id)
        result = b""
        if close.HasField("workflow_execution_completed_event_attributes"):
            payloads = close.workflow_execution_completed_event_attributes.result.payloads
            if payloads:
                result = payloads[0].SerializeToString(deterministic=True)
        return replace(
            obs, result_sha256=sha256(result).hexdigest(), result_size=len(result)
        ), result

    async def _observe_chain(
        self,
        namespace: str,
        workflow_id: str,
        call: BuyerCall,
        root: str,
        latest: DescribeWorkflowExecutionResponse | None,
    ) -> tuple[Observation, bytes | None]:
        """The latest run of the chain that starts at ``root`` (pinned, so verified).

        Usually that is ``latest``, the latest run of the id, which the server says descends
        from ``root``. Otherwise the chain closed and the seller started another run of the
        id since: the chain is followed forward from the last run reached before (closed
        runs never change), at most :data:`CHAIN_READS` runs per call, and a longer stretch
        raises :class:`ObservationIncomplete` rather than reporting a delivery as unmade."""
        if latest is not None and latest.workflow_execution_info.first_run_id == root:
            return await self._report(
                namespace, workflow_id, call, latest, root, Provenance.VERIFIED
            )
        key = (namespace, workflow_id, root)
        run = self._heads.get(key, root)
        for _ in range(CHAIN_READS):
            d = await self._describe(namespace, workflow_id, run)
            if d is None:
                return Observation(status=ObservedStatus.NOT_FOUND, pin=root), None
            if d.workflow_execution_info.status not in _MAY_HAND_OVER:
                return await self._report(
                    namespace, workflow_id, call, d, root, Provenance.VERIFIED
                )
            close = await self._close_event(namespace, workflow_id, run)
            successor = _successor(close)
            if not successor:
                return await self._report(
                    namespace, workflow_id, call, d, root, Provenance.VERIFIED, close
                )
            started = await self._started_event(namespace, workflow_id, successor)
            back = (
                started.workflow_execution_started_event_attributes.continued_execution_run_id
                if started is not None
                else None
            )
            if back != run:
                # A hand-over the successor does not confirm: never seen from a real server,
                # and nothing past it is known to descend from the buyer's call.
                return await self._report(
                    namespace, workflow_id, call, d, root, Provenance.UNVERIFIED, close
                )
            run = successor
            _remember(self._heads, key, run)
        raise ObservationIncomplete(
            f"{workflow_id} in {namespace}: the pinned chain is longer than {CHAIN_READS} "
            "runs past the last one reached; the next observation continues"
        )

    # ------------------------------------------------------------------ provenance

    async def _provenance(
        self, namespace: str, workflow_id: str, root: str, call: BuyerCall
    ) -> Provenance:
        """Whether the buyer's ``call`` started the chain whose first run is ``root``.
        A definite answer never changes (every field compared is immutable once
        written), so it is remembered; ``UNVERIFIED`` is asked again next time."""
        key = (namespace, workflow_id, root, call)
        known = self._verdicts.get(key)
        if known is not None:
            self._verdicts.move_to_end(key)
            return known
        verdict = await self._establish(namespace, workflow_id, root, call)
        if verdict is not Provenance.UNVERIFIED:
            _remember(self._verdicts, key, verdict)
        return verdict

    async def _establish(
        self, namespace: str, workflow_id: str, root: str, call: BuyerCall
    ) -> Provenance:
        """The lookup itself (decisions I15).

        From the seller's side, two facts only the server records: the request id the run
        was *started* with (``request_id_infos``, the entry pointing at its started event;
        a request id attached to an already-running run does not count) and the links of
        its Nexus completion callbacks, which name the caller. Both are the seller's to
        choose when it starts a workflow itself, so neither proves anything alone: a link
        into the buyer's namespace must resolve there to a Nexus ``execute`` call on this
        job's endpoint and service, carrying one of this job's hold tokens, whose request
        id is the run's start request id. The run is then the one that call started (or a
        restart with the call's own request id, which the pin and the call's record of the
        run it started rule out where they can; decisions I15 has the residue).

        The control plane's own placeholder (:meth:`occupy_job`) is told apart first: it is
        ``OCCUPIED``, not the seller's run."""
        found = await self._call_links(namespace, workflow_id, root, call)
        if isinstance(found, Provenance):
            return found
        target, links = found
        verdicts: list[Provenance] = []
        for link in links[:_LINKS_CHECKED]:
            verdict = (await self._resolve(link, target, call)).verdict
            if verdict is Provenance.VERIFIED:
                return verdict
            verdicts.append(verdict)
        return (
            Provenance.UNVERIFIED
            if Provenance.UNVERIFIED in verdicts
            else Provenance.NOT_FROM_BUYER
        )

    async def _call_links(
        self, namespace: str, workflow_id: str, root: str, call: BuyerCall
    ) -> tuple[_Target, list[Link]] | Provenance:
        """The seller-side half of a lookup: the run ``root`` as a :class:`_Target` (its
        start request id) and its callback links into the buyer's namespace -- or the
        verdict when there is nothing to follow (the run is gone, has no single start
        request id, or is the control plane's placeholder)."""
        d = await self._describe(namespace, workflow_id, root)
        started = await self._started_event(namespace, workflow_id, root)
        if d is None or started is None:
            return Provenance.UNVERIFIED
        if self._is_placeholder(started):
            return Provenance.OCCUPIED
        request_ids = [
            request_id
            for request_id, info in d.workflow_extended_info.request_id_infos.items()
            if info.event_type == _STARTED_EVENT and info.event_id == started.event_id
        ]
        if len(request_ids) != 1 or not request_ids[0]:
            return Provenance.UNVERIFIED
        links = [
            link
            for callback in started.workflow_execution_started_event_attributes.completion_callbacks
            if callback.HasField("nexus")
            for link in callback.links
            if _namespace_of(link) == call.namespace
        ]
        return _Target(namespace, workflow_id, root, request_ids[0]), links

    def _is_placeholder(self, started: HistoryEvent) -> bool:
        """Whether a run's started event is :meth:`occupy_job`'s placeholder: every field it
        sets, equal. A seller can start a run that looks the same (any start request can say
        these), so this only names what a run is; it never makes one a delivery and moves no
        money (decisions I16)."""
        a = started.workflow_execution_started_event_attributes
        return (
            a.workflow_type.name == OCCUPY_WORKFLOW_TYPE
            and a.task_queue.name == OCCUPY_TASK_QUEUE
            and a.identity == self._identity
            and not a.completion_callbacks
            and not a.continued_execution_run_id
            and not a.HasField("parent_workflow_execution")
        )

    async def _buyer_canceled(
        self, namespace: str, workflow_id: str, root: str, call: BuyerCall
    ) -> bool:
        """Whether the buyer asked to cancel the call that started the (verified, canceled)
        chain ``root``, from the buyer's own records of that very call: the standalone
        operation's cancellation info, or a ``NexusOperationCancelRequested`` event for the
        scheduled event the run's link names. Only the buyer (and the server) writes either.

        Remembered once known: the run is closed, and a buyer's cancel request that caused
        the cancel is recorded before the seller's handler is asked to cancel, so a later
        look can only add a request made after the seller's own cancel -- which must not
        move the blame. Known means read from the call itself, a link that resolves to it
        again. A look that cannot find the call (its record reads as not found) is not an
        answer: escrow closes the job on the first one it gets, so "the buyer did not ask"
        there would count the refund against the seller on a missing read. It raises
        :class:`TemporalError` ("cannot say") instead, and the next look asks again. Raises
        :class:`BuyerLookupError` when the buyer's namespace cannot be read: blaming the
        seller then would let a buyer that cancels its own call and keeps its namespace busy
        have the refund counted against the seller."""
        key = (namespace, workflow_id, root, call)
        known = self._cancels.get(key)
        if known is not None:
            self._cancels.move_to_end(key)
            return known
        found = await self._call_links(namespace, workflow_id, root, call)
        if isinstance(found, Provenance):
            return False  # nothing to follow: no evidence the buyer asked
        target, links = found
        unseen = False
        for link in links[:_LINKS_CHECKED]:
            seen = await self._resolve(link, target, call, want_cancel=True)
            if seen.verdict is Provenance.VERIFIED:
                _remember(self._cancels, key, seen.cancel_requested)
                return seen.cancel_requested
            unseen = unseen or seen.verdict is Provenance.UNVERIFIED
        if unseen:
            raise TemporalError(
                f"{workflow_id} in {namespace} ended canceled, and the buyer's call that "
                f"started it is not found in {call.namespace} just now: who canceled it "
                "cannot be said yet"
            )
        # Every link now names some other call: whatever the earlier verdict, none of the
        # buyer's records asks for this run's cancellation (immutable, so remembered).
        _remember(self._cancels, key, False)
        return False

    async def _resolve(
        self, link: Link, target: _Target, call: BuyerCall, *, want_cancel: bool = False
    ) -> _Found:
        if link.HasField("nexus_operation"):
            return await self._resolve_operation(link.nexus_operation, target, call)
        if link.HasField("workflow_event"):
            return await self._resolve_scheduled(
                link.workflow_event, target, call, want_cancel=want_cancel
            )
        return _Found(Provenance.NOT_FROM_BUYER)

    async def _resolve_operation(
        self, op: Link.NexusOperation, target: _Target, call: BuyerCall
    ) -> _Found:
        """The script door: a standalone operation in the buyer's namespace, by its id and
        run id (the run id is the server's, so a link written before the operation existed
        cannot name it). Its ``cancellation_info`` is set once a cancel was requested (by the
        buyer: nobody else acts in its namespace; the gate never cancels a buyer's call)."""
        if not op.operation_id or not op.run_id:
            return _Found(Provenance.NOT_FROM_BUYER)
        try:
            resp: DescribeNexusOperationExecutionResponse = (
                await self._wf.describe_nexus_operation_execution(
                    DescribeNexusOperationExecutionRequest(
                        namespace=call.namespace, operation_id=op.operation_id, run_id=op.run_id
                    ),
                    retry=True,
                    timeout=_RPC_TIMEOUT,
                )
            )
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND and not _namespace_missing(e):
                return _Found(Provenance.UNVERIFIED)
            if e.status == RPCStatusCode.INVALID_ARGUMENT:
                return _Found(Provenance.NOT_FROM_BUYER)  # malformed ids name nothing (_Unnamed)
            raise BuyerLookupError(
                f"describe operation {op.operation_id} in {call.namespace}: {e.message}"
            ) from e
        info = resp.info
        if (info.operation_id, info.run_id) != (op.operation_id, op.run_id):
            return _Found(Provenance.NOT_FROM_BUYER)
        if not _is_the_call(
            info.endpoint,
            info.service,
            info.operation,
            info.request_id,
            info.nexus_header,
            target,
            call,
        ):
            return _Found(Provenance.NOT_FROM_BUYER)
        if not _names_only(info.links, target):
            return _Found(Provenance.NOT_FROM_BUYER)
        return _Found(Provenance.VERIFIED, info.HasField("cancellation_info"))

    async def _resolve_scheduled(
        self,
        ref: Link.WorkflowEvent,
        target: _Target,
        call: BuyerCall,
        *,
        want_cancel: bool = False,
    ) -> _Found:
        """The workflow door: the ``NexusOperationScheduled`` event in the buyer workflow's
        history the link points at, by event id, then that operation's
        ``NexusOperationStarted`` event, whose links say which run the call started.

        Only an event id is followed: it is how the server links a caller's scheduled event
        (server 1.32, measured). A link by request id would have the gate read a buyer's
        whole history looking for a string the seller chose.

        ``want_cancel``: read on past the started event, to the operation's close, for a
        ``NexusOperationCancelRequested`` event naming the same scheduled event."""
        if not ref.workflow_id or not ref.run_id or not ref.HasField("event_ref"):
            return _Found(Provenance.NOT_FROM_BUYER)
        if ref.event_ref.event_type != _SCHEDULED or ref.event_ref.event_id <= 0:
            return _Found(Provenance.NOT_FROM_BUYER)
        event_id = ref.event_ref.event_id
        scheduled: HistoryEvent | None = None
        cancel_requested = started_ok = False
        try:
            async with aclosing(self._pages(call.namespace, ref.workflow_id, ref.run_id)) as pages:
                async for events in pages:
                    for e in events:
                        if scheduled is None:
                            if e.event_id < event_id:
                                continue
                            if e.event_id != event_id:
                                return _Found(Provenance.NOT_FROM_BUYER)
                            if not e.HasField("nexus_operation_scheduled_event_attributes"):
                                return _Found(Provenance.NOT_FROM_BUYER)
                            a = e.nexus_operation_scheduled_event_attributes
                            if not _is_the_call(
                                a.endpoint,
                                a.service,
                                a.operation,
                                a.request_id,
                                a.nexus_header,
                                target,
                                call,
                            ):
                                return _Found(Provenance.NOT_FROM_BUYER)
                            scheduled = e
                        elif (
                            e.HasField("nexus_operation_started_event_attributes")
                            and e.nexus_operation_started_event_attributes.scheduled_event_id
                            == scheduled.event_id
                        ):
                            if not _names_only(e.links, target):
                                return _Found(Provenance.NOT_FROM_BUYER)
                            started_ok = True
                            if not want_cancel or cancel_requested:
                                return _Found(Provenance.VERIFIED, cancel_requested)
                        elif _cancel_requested(e, scheduled.event_id):
                            cancel_requested = True
                            if started_ok:
                                return _Found(Provenance.VERIFIED, True)
                        elif _closes_operation(e, scheduled.event_id):
                            break
                    else:
                        continue
                    break
        except _Gone:
            return _Found(Provenance.UNVERIFIED)
        except _Unnamed:
            return _Found(Provenance.NOT_FROM_BUYER)
        except TemporalError as e:  # a page of the buyer's history could not be read
            raise BuyerLookupError(str(e)) from e
        if scheduled is None:
            return _Found(Provenance.NOT_FROM_BUYER)
        # Scheduled, and no Started event says otherwise (none yet, or it named this run).
        return _Found(Provenance.VERIFIED, cancel_requested)

    async def history_size(self, namespace: str, workflow_id: str) -> tuple[int, int] | None:
        """How long (events) and how large (bytes) the latest run of ``workflow_id``'s
        history is, as the server describes it, or None when there is no run: what a job's
        stream relay is paced against (:func:`kontra._relay.pace_for`, decisions I31). Raises
        :class:`TemporalError` when it cannot say."""
        d = await self._describe(namespace, workflow_id)
        if d is None:
            return None
        info = d.workflow_execution_info
        return info.history_length, info.history_size_bytes

    async def history_summary(
        self, namespace: str, workflow_id: str, pinned: str | None = None
    ) -> HistorySummary | None:
        """Progress from history metadata (§8.4), or None when the workflow does not exist.

        ``last_event`` skips workflow-task bookkeeping (it would almost always be
        ``WorkflowTaskCompleted``); ``activities_running`` are the pending activities'
        type names; ``activities_completed`` and ``continue_as_new_count`` cover the whole
        chain (lower bounds while a very long chain is still being counted).

        ``pinned``: the chain escrow pinned (``JobRecord.pin``, decisions I15). The
        summary is then of that chain only, read where :meth:`observe` reads it (the latest
        run of the id when the server says it descends from ``pinned``, else the chain's own
        last run), so another run of the id -- one the seller started itself -- never feeds
        the buyer's progress. Without it, the latest run of the id, whoever started it, so a
        gate must pass the pin as soon as it has one, and show a job's history progress to
        its buyer only from a pinned chain."""
        d = await self._describe(namespace, workflow_id)
        if pinned:
            d = await self._pinned_run(namespace, workflow_id, pinned, d)
        if d is None:
            return None
        info = d.workflow_execution_info
        run_id = info.execution.run_id
        key = (namespace, workflow_id)
        seen = self._runs.get(key)
        if seen is not None and seen.run_id == run_id and seen.length == info.history_length:
            # Nothing was appended since the last read: the seller's history (up to Temporal's
            # 50 MB per run) is not downloaded again on every sweep.
            self._runs.move_to_end(key)
        else:
            seen = _RunScan(run_id, 0, 0, None, "")
            for e in await self._events(namespace, workflow_id, run_id):
                seen.length = max(seen.length, e.event_id)
                if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
                    seen.completed += 1
                if e.event_type not in _WORKFLOW_TASK_EVENTS:
                    seen.last = _event_name(e.event_type)
                if e.HasField("workflow_execution_started_event_attributes"):
                    attrs = e.workflow_execution_started_event_attributes
                    seen.previous = attrs.continued_execution_run_id
            _remember(self._runs, key, seen)
        completed, last, previous = seen.completed, seen.last, seen.previous
        runs, earlier = await self._chain_before(namespace, workflow_id, previous)
        return HistorySummary(
            state=_STATUS.get(info.status, ObservedStatus.RUNNING),
            last_event=last,
            activities_completed=completed + earlier,
            activities_running=tuple(p.activity_type.name for p in d.pending_activities),
            continue_as_new_count=runs,
        )

    async def _pinned_run(
        self,
        namespace: str,
        workflow_id: str,
        root: str,
        latest: DescribeWorkflowExecutionResponse | None,
    ) -> DescribeWorkflowExecutionResponse | None:
        """The latest run reached of the chain that starts at ``root``, or None when that
        chain is gone (past retention).

        Usually ``latest``, the latest run of the id, whose ``first_run_id`` (set by the
        server only) is ``root``. Otherwise the chain closed and the seller started another
        run of the id since: the chain is followed forward as :meth:`observe` follows it,
        through each successor whose start names the run before it, from the furthest run
        either of them reached (the walk is shared), at most :data:`CHAIN_READS` runs per
        call. Progress is not money: past that budget, or at a hand-over the successor does
        not confirm, the furthest run reached is summarised and the next call goes on."""
        if latest is not None and latest.workflow_execution_info.first_run_id == root:
            return latest
        key = (namespace, workflow_id, root)
        run = self._heads.get(key, root)
        d = await self._describe(namespace, workflow_id, run)
        for _ in range(CHAIN_READS):
            if d is None or d.workflow_execution_info.status not in _MAY_HAND_OVER:
                return d
            successor = _successor(await self._close_event(namespace, workflow_id, run))
            if not successor:
                return d
            started = await self._started_event(namespace, workflow_id, successor)
            if (
                started is None
                or started.workflow_execution_started_event_attributes.continued_execution_run_id
                != run
            ):
                return d
            ahead = await self._describe(namespace, workflow_id, successor)
            if ahead is None:
                return d
            run, d = successor, ahead
            _remember(self._heads, key, run)
        return d

    async def _chain_before(self, namespace: str, workflow_id: str, run_id: str) -> tuple[int, int]:
        """(runs, completed activities) of the closed runs from ``run_id`` back to the start
        of the chain.

        Closed runs never change, so each walk leaves a skip pointer at the run it started
        from (``run -> (where the walk stopped, runs, activities in between)``) and the next
        walk jumps over everything already counted: the steady cost is the runs added since.
        A walk reads at most :data:`CHAIN_READS` histories; a longer chain (a seller can
        continue-as-new thousands of times) is counted over several calls, with lower
        bounds meanwhile, instead of stalling the sweeper that asks."""
        start = run_id
        runs = completed = reads = 0
        visited: set[str] = set()
        while run_id and run_id not in visited:
            visited.add(run_id)
            skip = self._skips.get(run_id)
            if skip is not None:
                self._skips.move_to_end(run_id)
                run_id, n, c = skip.target, skip.runs, skip.completed
                runs, completed = runs + n, completed + c
                continue
            if reads >= CHAIN_READS:
                break
            reads += 1
            previous, count = "", 0
            for e in await self._events(namespace, workflow_id, run_id):
                if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
                    count += 1
                elif e.HasField("workflow_execution_started_event_attributes"):
                    attrs = e.workflow_execution_started_event_attributes
                    previous = attrs.continued_execution_run_id
            runs, completed = runs + 1, completed + count
            run_id = previous
        if start:
            _remember(self._skips, start, _Skip(run_id, runs, completed))
        return runs, completed

    # ------------------------------------------------------------------ stopping

    async def cancel_workflow(
        self, namespace: str, workflow_id: str, reason: str, first_run_id: str = ""
    ) -> None:
        """Ask ``workflow_id`` to cancel. A workflow that is gone or already closed is not
        an error: there is nothing left to stop. A missing namespace is. With
        ``first_run_id`` (a job's pinned run) only the open run of THAT chain is asked:
        Temporal refuses when the open run started a different chain."""
        req = RequestCancelWorkflowExecutionRequest(
            namespace=namespace,
            workflow_execution=WorkflowExecution(workflow_id=workflow_id),
            identity=self._identity,
            request_id=str(uuid.uuid4()),
            reason=reason,
            first_execution_run_id=first_run_id,
        )
        try:
            await self._wf.request_cancel_workflow_execution(req, retry=True, timeout=_RPC_TIMEOUT)
        except RPCError as e:
            if e.status != RPCStatusCode.NOT_FOUND or _namespace_missing(e):
                raise TemporalError(f"cancel {workflow_id} in {namespace}: {e.message}") from e

    async def terminate_workflow(
        self, namespace: str, workflow_id: str, reason: str, first_run_id: str = ""
    ) -> None:
        """Terminate ``workflow_id``; gone or already closed is not an error. With
        ``first_run_id`` (a job's pinned run) only the open run of THAT chain is terminated:
        Temporal refuses when the open run started a different chain."""
        req = TerminateWorkflowExecutionRequest(
            namespace=namespace,
            workflow_execution=WorkflowExecution(workflow_id=workflow_id),
            identity=self._identity,
            reason=reason,
            first_execution_run_id=first_run_id,
        )
        try:
            await self._wf.terminate_workflow_execution(req, retry=True, timeout=_RPC_TIMEOUT)
        except RPCError as e:
            if e.status != RPCStatusCode.NOT_FOUND or _namespace_missing(e):
                raise TemporalError(f"terminate {workflow_id} in {namespace}: {e.message}") from e

    async def occupy_job(self, namespace: str, workflow_id: str, reason: str) -> Occupation:
        """Take ``workflow_id`` (a job's ``job-<id>``) in ``namespace`` (the seller's) so that
        no start can succeed for it any more: what a gate does when a buyer cancels a job
        before any run of it exists (decisions I16).

        The hold token stays valid until it expires and the seller's handler checks it
        offline (decisions I10), so after such a cancel the buyer could still run
        ``execute`` with it and have a short job delivered before anyone noticed: refunded,
        and the work for free. The handler starts ``job-<id>`` with ``REJECT_DUPLICATE`` and
        ``FAIL``, so one existing run of the id, open or closed, refuses that start for as
        long as the namespace retains it (:data:`DEFAULT_RETENTION`, longer than any hold).
        This is that run: a placeholder (:data:`OCCUPY_WORKFLOW_TYPE` on
        :data:`OCCUPY_TASK_QUEUE`, ``reason`` in its memo) started with the same policies
        and terminated at once. No worker ever runs it: its first workflow task is delayed
        past its own execution timeout, so it closes by itself should the termination never
        land. :meth:`observe` reports it as :attr:`~kontra.escrow.Provenance.OCCUPIED`.

        Returns :attr:`Occupation.STARTED` when some other run of the id exists already (a
        call, or the seller, got there first): the caller must then stop that job as a
        running one. Idempotent: a placeholder found from an earlier call is (terminated if
        need be and) reported ``OCCUPIED``. Raises :class:`TemporalError` when it cannot
        tell, never a guess."""
        req = StartWorkflowExecutionRequest(
            namespace=namespace,
            workflow_id=workflow_id,
            workflow_type=WorkflowType(name=OCCUPY_WORKFLOW_TYPE),
            task_queue=TaskQueue(name=OCCUPY_TASK_QUEUE),
            identity=self._identity,
            request_id=str(uuid.uuid4()),
            workflow_id_reuse_policy=WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
            workflow_id_conflict_policy=WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
            memo=Memo(
                fields={
                    _OCCUPY_MEMO: Payload(
                        metadata={"encoding": _JSON_PLAIN}, data=json.dumps(reason).encode()
                    )
                }
            ),
        )
        req.workflow_start_delay.FromTimedelta(_OCCUPY_DELAY)
        req.workflow_execution_timeout.FromTimedelta(_OCCUPY_TIMEOUT)
        try:
            resp = await self._wf.start_workflow_execution(req, retry=True, timeout=_RPC_TIMEOUT)
        except RPCError as e:
            if e.status != RPCStatusCode.ALREADY_EXISTS:
                raise TemporalError(f"occupy {workflow_id} in {namespace}: {e.message}") from e
            return await self._occupied_before(namespace, workflow_id, reason)
        await self._close_placeholder(namespace, workflow_id, resp.run_id, reason)
        return Occupation.OCCUPIED

    async def _occupied_before(self, namespace: str, workflow_id: str, reason: str) -> Occupation:
        """``workflow_id`` has a run already: ours from an earlier call (closed now, or
        closed here), or someone else's."""
        d = await self._describe(namespace, workflow_id)
        if d is None:
            raise TemporalError(f"{workflow_id} in {namespace} exists but cannot be described")
        run = d.workflow_execution_info.execution.run_id
        started = await self._started_event(namespace, workflow_id, run)
        if started is None or not self._is_placeholder(started):
            return Occupation.STARTED
        if (
            d.workflow_execution_info.status
            == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
        ):
            await self._close_placeholder(namespace, workflow_id, run, reason)
        return Occupation.OCCUPIED

    async def _close_placeholder(
        self, namespace: str, workflow_id: str, run_id: str, reason: str
    ) -> None:
        """Terminate the placeholder run ``run_id`` -- by its run id, so that a run someone
        else started meanwhile is never the one stopped. Closed already (its own timeout)
        is fine."""
        req = TerminateWorkflowExecutionRequest(
            namespace=namespace,
            workflow_execution=WorkflowExecution(workflow_id=workflow_id, run_id=run_id),
            identity=self._identity,
            reason=reason,
        )
        try:
            await self._wf.terminate_workflow_execution(req, retry=True, timeout=_RPC_TIMEOUT)
        except RPCError as e:
            if e.status != RPCStatusCode.NOT_FOUND or _namespace_missing(e):
                raise TemporalError(
                    f"terminate the placeholder {workflow_id} in {namespace}: {e.message}"
                ) from e

    # ------------------------------------------------------------------ standalone operations

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
        """Start a standalone Nexus operation in ``namespace`` (a buyer's) on its behalf,
        with ``payload`` relayed as is (the buyer's ciphertext) and ``headers`` as Nexus
        headers (the hold token).

        Idempotent: an operation that already exists under ``operation_id`` -- running or
        closed -- is success. The id may never be reused (while the buyer's namespace
        retains the operation: :data:`DEFAULT_RETENTION`), so a hold token starts at most
        one execution, and a retried request carries the same request id."""
        if schedule_to_close <= timedelta(0):
            raise ValueError("schedule_to_close must be positive")
        req = StartNexusOperationExecutionRequest(
            namespace=namespace,
            identity=self._identity,
            # The idempotency key of this RPC only: the request id the handler is sent, and
            # that the run proves its call by (decisions I15), is one the server generates
            # for the operation (server 1.32, measured). A retry of this request reuses it; a
            # later call that finds the operation started is success whatever its own id.
            request_id=str(uuid.uuid4()),
            operation_id=operation_id,
            endpoint=endpoint,
            service=service,
            operation=operation,
            input=payload,
            id_reuse_policy=NexusOperationIdReusePolicy.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE,
            id_conflict_policy=NexusOperationIdConflictPolicy.NEXUS_OPERATION_ID_CONFLICT_POLICY_FAIL,
        )
        req.schedule_to_close_timeout.FromTimedelta(schedule_to_close)
        for name, value in _check_headers(headers).items():
            req.nexus_header[name] = value
        try:
            await self._wf.start_nexus_operation_execution(req, retry=True, timeout=_RPC_TIMEOUT)
        except RPCError as e:
            if e.status != RPCStatusCode.ALREADY_EXISTS:
                raise TemporalError(
                    f"start {operation} operation {operation_id} in {namespace}: {e.message}"
                ) from e

    async def describe_call(self, namespace: str, operation_id: str) -> CallRecord | None:
        """The standalone operation ``operation_id`` in ``namespace`` (a buyer's; its latest
        run), or None when there is none: how far a job's buyer's call got
        (``kontra.engine.Reach``, decisions I40; kontra-cloud's ``describe_operation``, ported
        with the same answers). A missing namespace, or any other refusal, is "cannot say"
        (:class:`BuyerLookupError`, as for the other reads of a buyer's namespace), never "the
        buyer never called". Ids the frontend refuses as malformed name nothing (None), as in
        the other lookups: an error there would be asked again forever.

        One RPC, not retried, and given :data:`CALL_READ_TIMEOUT` (2 s), not the 15 s of the
        other calls: the Temporal adapter reads it on every observation of a job with no
        verified run, and a buyer can keep its own namespace refusing or stalling reads -- a
        retry loop, or a long wait, there would hold up every other job's observation. What
        it cannot read in time is "cannot say", asked again at the next observation."""
        try:
            resp: DescribeNexusOperationExecutionResponse = (
                await self._wf.describe_nexus_operation_execution(
                    DescribeNexusOperationExecutionRequest(
                        namespace=namespace, operation_id=operation_id
                    ),
                    retry=False,
                    timeout=CALL_READ_TIMEOUT,
                )
            )
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND and not _namespace_missing(e):
                return None
            if e.status == RPCStatusCode.INVALID_ARGUMENT:
                return None
            raise BuyerLookupError(
                f"describe operation {operation_id} in {namespace}: {e.status.name}: {e.message}"
            ) from e
        return call_record(resp.info)
