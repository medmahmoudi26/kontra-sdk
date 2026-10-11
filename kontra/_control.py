"""Control-plane calls against Temporal's INTERNAL frontend, shared by both gates.

The gate (``kontra-cloud``) and the fake gate (:mod:`kontra.testing.fake_gate`) need the
same handful of operations that no party's token may perform (decisions I3, I4, I6):
register a party's namespace, create and look up the Nexus endpoint of a published
version, relay a quote as a synchronous Nexus call, watch the seller's handler workflow
``job-<id>``, stop it when escrow says so, and start a standalone Nexus operation in a
buyer's namespace on its behalf. :class:`Control` is that set, in one place, so the two
gates cannot drift.

Every call names its namespace explicitly in a raw ``workflow_service`` /
``operator_service`` request: the client's own default namespace is never used, so one
client on the internal frontend serves every party. Nothing here reads a payload: results
are returned as the bytes the history holds (ciphertext), and progress is derived from
history *metadata* only (PRD §8.4).

Errors: a failure of the platform (frontend unreachable, an RPC refused) raises
:class:`TemporalError`. A seller's own failure is never an exception: :meth:`Control.nexus_sync`
classifies it into a :class:`NexusSyncResult`.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
import uuid
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote

import httpx
from google.protobuf.duration_pb2 import Duration
from google.protobuf.message import DecodeError
from google.protobuf.timestamp_pb2 import Timestamp
from temporalio.api.common.v1 import Payload, WorkflowExecution
from temporalio.api.enums.v1 import (
    EventType,
    HistoryEventFilterType,
    NexusOperationIdConflictPolicy,
    NexusOperationIdReusePolicy,
    TaskQueueType,
    WorkflowExecutionStatus,
)
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.nexus.v1 import Endpoint, EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import (
    CreateNexusEndpointRequest,
    ListNexusEndpointsRequest,
)
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    DescribeNamespaceRequest,
    DescribeTaskQueueRequest,
    DescribeWorkflowExecutionRequest,
    DescribeWorkflowExecutionResponse,
    GetWorkflowExecutionHistoryRequest,
    RegisterNamespaceRequest,
    RequestCancelWorkflowExecutionRequest,
    StartNexusOperationExecutionRequest,
    TerminateWorkflowExecutionRequest,
)
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

from kontra.contract.names import is_endpoint_name
from kontra.escrow import Observation, ObservedStatus

__all__ = [
    "CHAIN_READS",
    "DEFAULT_RETENTION",
    "FRONTEND_RESERVE",
    "IDENTITY",
    "Control",
    "EndpointConflict",
    "EndpointInfo",
    "HistorySummary",
    "NexusSyncKind",
    "NexusSyncResult",
    "TemporalControl",
    "TemporalError",
]

#: Party namespaces keep closed workflows this long: long enough for the sweeper to see
#: every close, short enough that history is not a second archive of ciphertext.
DEFAULT_RETENTION = timedelta(days=3)
#: Who the control plane says it is in histories and on standalone operations.
IDENTITY = "kontra-control"
#: The frontend keeps about this much of a Nexus ``Request-Timeout`` for itself: the
#: handler's deadline is the header minus ~2 s, and a header of 2 s or less is answered
#: ``520 upstream timeout`` at once (server 1.32, measured). The relay adds it to the
#: caller's timeout, so the handler gets the whole of it while the caller still waits no
#: longer than its timeout (enforced on our side of the connection).
FRONTEND_RESERVE = 2.0

_RPC_TIMEOUT = timedelta(seconds=15)
_NAMESPACE_WAIT = 30.0
_JSON_PLAIN = b"json/plain"
_MESSAGE_MAX = 1000
#: Prior runs of a continue-as-new chain are immutable once closed, so what they contribute
#: to a summary is computed once and remembered (bounded).
_CHAIN_CACHE = 4096
#: Histories of earlier runs one summary may read (see :meth:`Control._chain_before`).
CHAIN_READS = 64
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


class EndpointConflict(TemporalError):
    """:meth:`Control.create_endpoint` for a name that exists with another target."""


@dataclass(frozen=True, slots=True)
class EndpointInfo:
    """A Nexus endpoint as the operator API reports it. ``namespace`` / ``task_queue`` are
    the worker target (empty for an endpoint with another kind of target)."""

    id: str
    name: str
    namespace: str
    task_queue: str


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
        self, namespace: str, workflow_id: str
    ) -> tuple[Observation, bytes | None]: ...

    async def history_summary(self, namespace: str, workflow_id: str) -> HistorySummary | None: ...

    async def cancel_workflow(self, namespace: str, workflow_id: str, reason: str) -> None: ...

    async def terminate_workflow(self, namespace: str, workflow_id: str, reason: str) -> None: ...

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


def _time(ts: Timestamp, present: bool) -> datetime | None:
    return ts.ToDatetime(tzinfo=UTC) if present else None


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
        describes it and serves requests in it. Idempotent (an existing namespace keeps
        its retention); concurrent callers converge.

        Registration comes first, without a "does it exist?" read: asking the frontend
        about a namespace that does not exist yet makes it remember the absence for about
        a second (server 1.32, measured), so a namespace registered right after such a
        read is refused by workflow calls for that long."""
        if not namespace:
            raise ValueError("namespace must not be empty")
        req = RegisterNamespaceRequest(
            namespace=namespace,
            workflow_execution_retention_period=Duration(seconds=int(retention.total_seconds())),
        )
        try:
            await self._wf.register_namespace(req, retry=True, timeout=_RPC_TIMEOUT)
        except RPCError as e:
            if e.status != RPCStatusCode.ALREADY_EXISTS:
                raise TemporalError(f"register namespace {namespace}: {e.message}") from e
        delay, waited = 0.02, 0.0
        while not await self._usable(namespace):
            if waited >= _NAMESPACE_WAIT:
                raise TemporalError(f"namespace {namespace} was registered but never appeared")
            await asyncio.sleep(delay)
            waited += delay
            delay = min(delay * 2, 0.5)

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
            self._own_http, self._own_loop = httpx.AsyncClient(), loop
        if self._own_loop is loop:
            return self._own_http, False
        return httpx.AsyncClient(), True

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

    # ------------------------------------------------------------------ observation

    async def _describe(
        self, namespace: str, workflow_id: str
    ) -> DescribeWorkflowExecutionResponse | None:
        try:
            return await self._wf.describe_workflow_execution(
                DescribeWorkflowExecutionRequest(
                    namespace=namespace, execution=WorkflowExecution(workflow_id=workflow_id)
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

    async def observe(self, namespace: str, workflow_id: str) -> tuple[Observation, bytes | None]:
        """What escrow needs of ``workflow_id`` (the latest run of its chain) in
        ``namespace``: its status and times and, once completed, the result payload exactly
        as stored -- a serialized ``Payload``, ciphertext -- with that byte string's sha256
        and size in the observation."""
        d = await self._describe(namespace, workflow_id)
        if d is None:
            return Observation(status=ObservedStatus.NOT_FOUND), None
        info = d.workflow_execution_info
        status = _STATUS.get(info.status, ObservedStatus.RUNNING)
        started = _time(info.start_time, info.HasField("start_time"))
        closed = _time(info.close_time, info.HasField("close_time"))
        if status is not ObservedStatus.COMPLETED:
            return Observation(status=status, started_at=started, closed_at=closed), None
        events = await self._events(namespace, workflow_id, info.execution.run_id, close_only=True)
        result = b""
        for e in events:
            if e.HasField("workflow_execution_completed_event_attributes"):
                payloads = e.workflow_execution_completed_event_attributes.result.payloads
                if payloads:
                    result = payloads[0].SerializeToString(deterministic=True)
                break
        return (
            Observation(
                status=status,
                started_at=started,
                closed_at=closed,
                result_sha256=sha256(result).hexdigest(),
                result_size=len(result),
            ),
            result,
        )

    async def history_summary(self, namespace: str, workflow_id: str) -> HistorySummary | None:
        """Progress from history metadata (§8.4), or None when the workflow does not exist.

        ``last_event`` skips workflow-task bookkeeping (it would almost always be
        ``WorkflowTaskCompleted``); ``activities_running`` are the pending activities'
        type names; ``activities_completed`` and ``continue_as_new_count`` cover the whole
        chain (lower bounds while a very long chain is still being counted)."""
        d = await self._describe(namespace, workflow_id)
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
            self._runs[key] = seen
            self._runs.move_to_end(key)
            while len(self._runs) > _CHAIN_CACHE:
                self._runs.popitem(last=False)
        completed, last, previous = seen.completed, seen.last, seen.previous
        runs, earlier = await self._chain_before(namespace, workflow_id, previous)
        return HistorySummary(
            state=_STATUS.get(info.status, ObservedStatus.RUNNING),
            last_event=last,
            activities_completed=completed + earlier,
            activities_running=tuple(p.activity_type.name for p in d.pending_activities),
            continue_as_new_count=runs,
        )

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
            self._skips[start] = _Skip(run_id, runs, completed)
            self._skips.move_to_end(start)
            while len(self._skips) > _CHAIN_CACHE:
                self._skips.popitem(last=False)
        return runs, completed

    # ------------------------------------------------------------------ stopping

    async def cancel_workflow(self, namespace: str, workflow_id: str, reason: str) -> None:
        """Ask ``workflow_id`` to cancel. A workflow that is gone or already closed is not
        an error: there is nothing left to stop. A missing namespace is."""
        req = RequestCancelWorkflowExecutionRequest(
            namespace=namespace,
            workflow_execution=WorkflowExecution(workflow_id=workflow_id),
            identity=self._identity,
            request_id=str(uuid.uuid4()),
            reason=reason,
        )
        try:
            await self._wf.request_cancel_workflow_execution(req, retry=True, timeout=_RPC_TIMEOUT)
        except RPCError as e:
            if e.status != RPCStatusCode.NOT_FOUND or _namespace_missing(e):
                raise TemporalError(f"cancel {workflow_id} in {namespace}: {e.message}") from e

    async def terminate_workflow(self, namespace: str, workflow_id: str, reason: str) -> None:
        """Terminate ``workflow_id``; gone or already closed is not an error."""
        req = TerminateWorkflowExecutionRequest(
            namespace=namespace,
            workflow_execution=WorkflowExecution(workflow_id=workflow_id),
            identity=self._identity,
            reason=reason,
        )
        try:
            await self._wf.terminate_workflow_execution(req, retry=True, timeout=_RPC_TIMEOUT)
        except RPCError as e:
            if e.status != RPCStatusCode.NOT_FOUND or _namespace_missing(e):
                raise TemporalError(f"terminate {workflow_id} in {namespace}: {e.message}") from e

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
        closed -- is success. The id may never be reused, so a hold token starts at most
        one execution, and a retried request carries the same request id."""
        if schedule_to_close <= timedelta(0):
            raise ValueError("schedule_to_close must be positive")
        req = StartNexusOperationExecutionRequest(
            namespace=namespace,
            identity=self._identity,
            request_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"kontra:{namespace}/{operation_id}")),
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
