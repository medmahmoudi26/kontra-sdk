"""The graph page of ``kontra dev up --ui`` (decisions I28; kontra-sdk#43): the routes the
dev codec server (:mod:`kontra.cli._codec_server`) serves beside Temporal's codec protocol,
on the same port and behind the same ``Host`` check.

=============================  ==============================================================
``GET /graph``                 the page (``_console.html``): a run's graph, live; without
                               ``?w=`` the party's recent runs
``GET /api/workflows``         the latest :data:`WORKFLOWS_LIMIT` executions in the party's
                               namespace (batch children left out): metadata only
``GET /api/graph?w=&r=``       :func:`kontra.graph.links` of the run painted on the static
                               graph ``kontra serve`` drew for its task queue
                               (:func:`kontra._graphs.load_static`, decisions I35);
                               ``static_graph: false`` when there is none, and
                               ``handler: true`` when the run is a handler offering's (its
                               job, ``kontra.turn.v1``, or the agent that job started), which
                               has none. A ``kontra.turn.v1`` job's agent child is drawn
                               nested, its tool activities real nodes (I36)
``GET /api/session?w=&r=``     ``{"graph", "session"}`` from one snapshot: that graph and the
                               run's agent session (:mod:`kontra.cli._session`), rows and
                               short summaries; only with ``X-Kontra-Console: 1``
``GET /api/events?w=&r=``      server-sent ``event: session-stat`` frames, a change signal and
                               nothing else (``history_length``, ``count``, ``last_offset``,
                               ``status``, ``turn``, ``rev``), each time the snapshot changed,
                               about once a second, then ``event: end`` once the run closed; at
                               most :data:`MAX_STREAMS` at once. The page fetches
                               ``/api/session`` on each (OD2)
``GET /api/io?w=&r=&node=``    a node's (``node=`` empty: the workflow's) input, result and
                               failure, decoded here; only with ``X-Kontra-Console: 1``
``GET /api/io?w=&r=&event=``   one agent event's verbatim body, by its stream offset; the same
=============================  ==============================================================

The page is served to the console's own origin alone (``http://127.0.0.1:P`` and
``http://localhost:P``), with a policy that runs nothing but its own nonce'd script and style.
Every answer is ``no-store`` and ``nosniff``. Payloads reach the page through the
header-gated routes alone (seam ⑤), verbatim through ``/api/io`` alone: :func:`kontra.graph.parse` emits none, so the graph routes carry no input,
result, failure message or heartbeat, encrypted or not; ``/api/io`` decodes the node asked
for in this process, with this machine's codec, and shows what it cannot open as a
placeholder -- ``{"encrypted": true}`` for a payload encrypted to other keys,
``{"claim_check": true}`` for a reference to the bucket (this process holds no gate session
to fetch it) -- never its bytes. Its header is one a cross-origin page cannot send without a
preflight, which the codec server refuses.

Temporal is read as the dev party, with the profile's worker token (``DescribeWorkflowExecution``,
``GetWorkflowExecutionHistory``, ``ListWorkflowExecutions`` in its own namespace, which the
Authorizer allows any worker token): one event loop on a daemon thread owns the session and
the client, connected at the first request that needs them, so the codec server starts
without Temporal and ``/decode`` answers while it is down. Request threads hand that loop
coroutines and wait a bounded time. An error is a JSON ``{"error": ...}`` of one line, never a
traceback.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import secrets
import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Coroutine, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import parse_qs

from google.protobuf import json_format
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import WorkflowExecutionStatus
from temporalio.converter import PayloadCodec
from temporalio.service import RPCError

from kontra import graph
from kontra.cli import _agent, _session
from kontra.codec import CLAIM_CHECK_ENCODING, ENCODING, CodecError, KontraCodec
from kontra.serve import _agent_events as ae

if TYPE_CHECKING:
    from temporalio.client import Client, WorkflowExecutionDescription

__all__ = [
    "API_ROUTES",
    "IO_HEADER",
    "LOG_SERVICE",
    "LOKI_UID",
    "MAX_STREAMS",
    "PAGE_ROUTE",
    "ROUTES",
    "TEMPO_UID",
    "WORKFLOWS_LIMIT",
    "Connected",
    "Console",
    "ConsoleError",
    "connect_dev",
    "handle",
]

PAGE_ROUTE = "/graph"
#: ``/api/events`` was ``/api/graph/events`` (I28); the graph is ``/api/session``'s now (OD2).
API_ROUTES = frozenset({"/api/workflows", "/api/graph", "/api/session", "/api/events", "/api/io"})
ROUTES = frozenset({PAGE_ROUTE, *API_ROUTES})
#: What ``/api/io`` and ``/api/session`` require: a header no cross-origin page sends without
#: a preflight.
IO_HEADER = "X-Kontra-Console"
#: Server-sent event streams open at once; one more is refused (503).
MAX_STREAMS = 16
WORKFLOWS_LIMIT = 50
#: The ``service.name`` ``kontra serve --otlp`` gives a worker's logs when
#: ``OTEL_SERVICE_NAME`` names none (``kontra.serve._tracing.SERVICE_NAME``; that module
#: needs the opentelemetry extra, this one does not).
LOG_SERVICE = "kontra-worker"
#: The Grafana data sources ``observability/grafana/datasources.yaml`` provisions.
LOKI_UID = "loki"
TEMPO_UID = "tempo"

_PAGE_FILE = "_console.html"
_NONCE = "__NONCE__"
_BATCH = "kontra.batch.v1"
#: A handler offering's job workflow (``kontra.serve.turn.TURN_WORKFLOW``, without importing
#: the serving side): it has no static graph.
_TURN = "kontra.turn.v1"
#: Parent runs whose type was looked up, remembered (a run's type never changes).
_PARENT_CACHE = 256
#: How long a request waits for Temporal (and the gate, for a token) before it gives up.
_TIMEOUT = 30.0
#: How often a stream asks Temporal whether the run changed, and says it is alive.
_POLL = 1.0
_KEEPALIVE = 15.0
#: What EventSource waits before it reconnects a stream that ended without ``end``.
_RETRY_MS = 3000
#: Batch children whose histories one graph reads, and closed ones remembered.
_CHILDREN_MAX = 64
_CHILD_CACHE = 256
#: Snapshots kept for ``/api/session`` to reuse the stream's (one fetch for both panes).
_SNAPSHOTS = 64
#: Temporal's default limit for a workflow id, and a node id is a name of one.
_ID_MAX = 1000
_RUN_ID = re.compile(r"[0-9A-Fa-f-]{1,64}", re.ASCII)
_CLOSED_EVENTS = frozenset(
    f"EVENT_TYPE_WORKFLOW_EXECUTION_{kind}"
    for kind in ("COMPLETED", "FAILED", "TIMED_OUT", "CANCELED", "TERMINATED", "CONTINUED_AS_NEW")
)
_SAID = 300
_OPEN = frozenset({"scheduled", "running"})

_T = TypeVar("_T")


class ConsoleError(Exception):
    """A request the console answers with ``status`` and the one line ``message``."""

    def __init__(self, status: HTTPStatus, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass
class Connected:
    """A Temporal client for the party's namespace, and what closes what keeps it fresh."""

    client: Client
    namespace: str
    close: Callable[[], Awaitable[None]] | None = None


async def connect_dev(home: Path) -> Connected:
    """The dev profile in ``home``'s session and a client of its namespace as ``worker``
    (:meth:`kontra.auth.Session.connect_temporal`, which keeps the token fresh until the
    session closes). Only a dev profile: whoever reaches the codec server's port reads
    through it."""
    from kontra.auth import Session

    session = Session.load(home)
    try:
        if not session.profile.dev:
            raise ConsoleError(
                HTTPStatus.FORBIDDEN, f"{home} holds a login that is not a dev one: no graph page"
            )
        client = await session.connect_temporal("worker")
    except BaseException:
        await session.aclose()
        raise
    return Connected(client, session.profile.namespace, session.aclose)


def _placeholder(value: Mapping[str, bool]) -> Payload:
    return Payload(
        metadata={"encoding": b"json/plain"},
        data=json.dumps(value, separators=(",", ":")).encode(),
    )


class _Shown(PayloadCodec):
    """What the page is shown of each payload: decrypted when it is encrypted to this
    machine's keys, else a placeholder; plaintext as it is. Decode only."""

    def __init__(self, codec: KontraCodec) -> None:
        self._codec = codec

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        raise NotImplementedError("the graph page decodes only")

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out: list[Payload] = []
        for p in payloads:
            encoding = p.metadata.get("encoding")
            if encoding == CLAIM_CHECK_ENCODING:
                out.append(_placeholder({"claim_check": True}))
            elif encoding == ENCODING:
                try:
                    [opened] = await self._codec.decode([p])
                except CodecError:  # another party's, a job key no longer here, tampered
                    opened = _placeholder({"encrypted": True})
                out.append(opened)
            else:
                out.append(p)
        return out


@dataclass
class _Snapshot:
    graph: graph.Graph
    history: dict[str, Any]
    key: tuple[Any, ...]
    closed: bool
    #: An open batch child not named yet: it is named from its own history, which grows
    #: while the run's does not (see :meth:`Console.stream`).
    unnamed: bool
    #: ``DescribeWorkflowExecution``'s pending activities: a retrying activity's last
    #: failure is there alone until it closes.
    pending: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    #: The run's agent session (:mod:`kontra.cli._session`), from the same reads.
    session: dict[str, Any] = field(default_factory=dict[str, Any])
    #: A child drawn nested, by its node id: its history and pending activities, which
    #: ``/api/io`` reads that child's nodes' payloads from.
    nested: dict[str, tuple[dict[str, Any], list[dict[str, Any]]]] = field(
        default_factory=dict[str, tuple[dict[str, Any], list[dict[str, Any]]]]
    )
    #: The agent's decoded events, by offset (``/api/io?event=``).
    items: dict[int, dict[str, Any]] = field(default_factory=dict[int, dict[str, Any]])
    #: Events in every history this snapshot read.
    history_length: int = 0
    #: The static graph it was painted on (a file ``kontra serve`` redraws at will).
    static: graph.Graph | None = None


#: A snapshot's key: the run's :func:`_change_key`, and each open workflow it read besides
#: (the agent child, a subagent) with its history's length -- those grow while the run's
#: history does not.
_Key = tuple[tuple[Any, ...], tuple[tuple[str, str, int], ...]]


def _unnamed_batch(live: graph.Graph) -> bool:
    return any(
        n.get("kind") == "child" and n.get("name") == _BATCH and n.get("status") in _OPEN
        for n in live.get("nodes") or []
    )


def _change_key(desc: WorkflowExecutionDescription) -> tuple[Any, ...]:
    """What changes when the graph does: the history's length, the status and close time,
    and each pending activity's state and attempt (a retry or a start writes no history
    event until the activity closes). Not a heartbeat."""
    raw = desc.raw_description
    info = raw.workflow_execution_info
    closed = info.close_time.ToNanoseconds() if info.HasField("close_time") else None
    pending = tuple(
        (
            p.activity_id,
            int(p.state),
            p.attempt,
            p.last_started_time.ToNanoseconds() if p.HasField("last_started_time") else None,
            p.HasField("last_failure"),
        )
        for p in raw.pending_activities
    )
    return (info.history_length, int(info.status), closed, pending)


def _is_open(desc: WorkflowExecutionDescription) -> bool:
    status = desc.raw_description.workflow_execution_info.status
    return status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING


def _closed_history(history: Mapping[str, Any]) -> bool:
    events = history.get("events") or []
    return bool(events) and str(events[-1].get("eventType")) in _CLOSED_EVENTS


def _batch_children(history: Mapping[str, Any]) -> list[tuple[str, str]]:
    """The ``kontra.batch.v1`` children the run started, by the workflow id and first run
    id their ``ChildWorkflowExecutionStarted`` names (never built or parsed, I1): the
    parser names a batch node from its child's history."""
    found: list[tuple[str, str]] = []
    for event in history.get("events") or []:
        attrs = event.get("childWorkflowExecutionStartedEventAttributes")
        if not isinstance(attrs, dict):
            continue
        execution: Any = attrs.get("workflowExecution") or {}
        kind: Any = attrs.get("workflowType") or {}
        workflow_id, run_id = execution.get("workflowId"), execution.get("runId")
        if kind.get("name") == _BATCH and isinstance(workflow_id, str) and isinstance(run_id, str):
            found.append((workflow_id, run_id))
    return found[:_CHILDREN_MAX]


def _said(e: BaseException) -> str:
    """One line for an error, never a traceback, and nothing of a payload: Temporal's and the
    gate's own words, else the error's kind."""
    import httpx

    from kontra.auth import LoginError
    from kontra.gate_client import GateError
    from kontra.profile import ProfileError

    if isinstance(e, ConsoleError):
        said = e.message
    elif isinstance(e, RPCError):
        said = f"Temporal answered {e.status.name}: {e.message}"
    elif isinstance(
        e, ProfileError | LoginError | GateError | httpx.HTTPError | OSError | RuntimeError
    ):
        # RuntimeError: temporalio's "Failed client connect: ..." (an address, a transport
        # error); the profile's, the gate's and the gate client's are their own words.
        said = f"{type(e).__name__}: {e}"
    else:
        said = f"{type(e).__name__} (the codec server's log has no more)"
    line = said.strip().splitlines()[0] if said.strip() else type(e).__name__
    return line if len(line) <= _SAID else line[: _SAID - 3] + "..."


def _status_of(e: BaseException) -> HTTPStatus:
    from temporalio.service import RPCStatusCode

    if isinstance(e, ConsoleError):
        return e.status
    if isinstance(e, RPCError) and e.status == RPCStatusCode.NOT_FOUND:
        return HTTPStatus.NOT_FOUND
    if isinstance(e, RPCError) and e.status == RPCStatusCode.INVALID_ARGUMENT:
        return HTTPStatus.BAD_REQUEST
    return HTTPStatus.BAD_GATEWAY


def _dumps(value: object) -> bytes:
    try:
        return json.dumps(value, separators=(",", ":"), allow_nan=False).encode()
    except ValueError:
        raise ConsoleError(
            HTTPStatus.UNPROCESSABLE_ENTITY, "a value JSON cannot carry (NaN or Infinity)"
        ) from None


def _event(name: str, value: object) -> bytes:
    return b"event: " + name.encode() + b"\ndata: " + _dumps(value) + b"\n\n"


class Console:
    """What the console routes read: Temporal as the dev party (``connect``; default
    :func:`connect_dev` on ``home``), the static graphs under ``home``, ``codec`` (made per
    request: job keys come and go), and where the links point: ``temporal_ui`` and
    ``grafana`` base URLs (None leaves those links out), Loki and Tempo by their provisioned
    uids, and ``service.name`` ``log_service`` (default ``OTEL_SERVICE_NAME``, else
    :data:`LOG_SERVICE`)."""

    def __init__(
        self,
        home: Path,
        codec: Callable[[], KontraCodec],
        *,
        temporal_ui: str | None = None,
        grafana: str | None = None,
        connect: Callable[[], Awaitable[Connected]] | None = None,
        log_service: str | None = None,
    ) -> None:
        graph.LinkConfig(namespace="-", temporal_ui=temporal_ui, grafana=grafana)  # checks them
        self.home = home
        self._codec = codec
        self._temporal_ui = temporal_ui
        self._grafana = grafana
        self._connect = connect or (lambda: connect_dev(home))
        service = log_service or os.environ.get("OTEL_SERVICE_NAME") or LOG_SERVICE
        # A LogQL string is a Go string literal: JSON's escapes are a subset of its.
        self._selector = "{service_name=" + json.dumps(service) + "}"
        self._page = resources.files("kontra.cli").joinpath(_PAGE_FILE).read_text("utf-8")
        if self._page.count(_NONCE) < 2:
            raise RuntimeError(f"kontra/cli/{_PAGE_FILE} lost its nonce placeholders")
        self.streams = threading.BoundedSemaphore(MAX_STREAMS)
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._closing = threading.Event()
        # Owned by the loop's thread alone:
        self._temporal: Connected | None = None
        self._connecting: asyncio.Lock | None = None
        self._children: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()
        self._turns: OrderedDict[tuple[str, str], bool] = OrderedDict()
        #: What the session pane reads of a run's agent (decisions I36), with this codec.
        self._reader = _agent.Reader(lambda: self._codec())
        self._snapshots: OrderedDict[tuple[str, str], _Snapshot] = OrderedDict()

    # ------------------------------------------------------------------ the loop

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._closing.is_set():
                raise ConsoleError(HTTPStatus.SERVICE_UNAVAILABLE, "the codec server is stopping")
            if self._loop is None:
                loop = asyncio.new_event_loop()
                thread = threading.Thread(
                    target=loop.run_forever, name="kontra-console", daemon=True
                )
                thread.start()
                self._loop, self._thread = loop, thread
            return self._loop

    def _run(self, coro: Coroutine[Any, Any, _T], timeout: float = _TIMEOUT) -> _T:
        try:
            loop = self._ensure_loop()
        except BaseException:
            coro.close()
            raise
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            return future.result(timeout)
        except TimeoutError:
            future.cancel()
            raise ConsoleError(
                HTTPStatus.GATEWAY_TIMEOUT, f"Temporal did not answer within {timeout:.0f}s"
            ) from None

    def close(self) -> None:
        """End the streams and close the session (its token refreshers with it)."""
        with self._lock:
            self._closing.set()
            loop, thread = self._loop, self._thread
        if loop is None or thread is None:
            return

        async def shut() -> None:
            temporal, self._temporal = self._temporal, None
            if temporal is not None and temporal.close is not None:
                await temporal.close()

        with contextlib.suppress(Exception):  # closing anyway
            asyncio.run_coroutine_threadsafe(shut(), loop).result(10)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(10)
        if not thread.is_alive():
            loop.close()

    async def _client(self) -> Connected:
        if self._temporal is None:
            if self._connecting is None:
                self._connecting = asyncio.Lock()
            async with self._connecting:
                if self._temporal is None:
                    self._temporal = await self._connect()
        return self._temporal

    # ------------------------------------------------------------------ Temporal

    async def _child_histories(self, client: Client, history: Mapping[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for workflow_id, run_id in _batch_children(history):
            key = (workflow_id, run_id)
            cached = self._children.get(key)
            if cached is None:
                handle = client.get_workflow_handle(workflow_id, run_id=run_id)
                cached = (await handle.fetch_history()).to_json_dict()
                if _closed_history(cached):  # a closed run's history is final
                    self._children[key] = cached
                    while len(self._children) > _CHILD_CACHE:
                        self._children.popitem(last=False)
            else:
                self._children.move_to_end(key)
            out[workflow_id] = cached
        return out

    def _links(self, namespace: str) -> graph.LinkConfig:
        return graph.LinkConfig(
            namespace=namespace,
            temporal_ui=self._temporal_ui,
            grafana=self._grafana,
            loki_uid=LOKI_UID,
            tempo_uid=TEMPO_UID,
            log_selector=self._selector,
        )

    async def _handler_run(self, client: Client, desc: WorkflowExecutionDescription) -> bool:
        """Whether the run is a handler offering's: its job (``kontra.turn.v1``), or the agent
        that job started (its parent is one). Told by the types Temporal reports, never by
        an id."""
        if desc.workflow_type == _TURN:
            return True
        if not desc.parent_id or not desc.parent_run_id:
            return False
        key = (desc.parent_id, desc.parent_run_id)
        turn = self._turns.get(key)
        if turn is None:
            try:
                parent = await client.get_workflow_handle(key[0], run_id=key[1]).describe()
            except RPCError:
                return False
            turn = parent.workflow_type == _TURN
            self._turns[key] = turn
            while len(self._turns) > _PARENT_CACHE:
                self._turns.popitem(last=False)
        else:
            self._turns.move_to_end(key)
        return turn

    async def _snapshot(
        self,
        workflow_id: str,
        run_id: str | None,
        desc: WorkflowExecutionDescription | None = None,
    ) -> _Snapshot:
        """The run's graph and its agent session, from one read of each history they need
        (the run's, its batch children's, its agent's and its subagents'). Kept for
        ``/api/session`` to reuse while nothing it read changed."""
        from kontra._graphs import load_static

        temporal = await self._client()
        client = temporal.client
        if desc is None:
            desc = await client.get_workflow_handle(workflow_id, run_id=run_id).describe()
        run_id = desc.run_id
        handle = client.get_workflow_handle(desc.id, run_id=run_id)
        history = (await handle.fetch_history()).to_json_dict()
        pending = [json_format.MessageToDict(p) for p in desc.raw_description.pending_activities]
        children = await self._child_histories(client, history)
        agent = await self._reader.agent_of(client, desc, history)
        watched: list[tuple[str, str, int]] = []
        nested: dict[str, Any] | None = None
        read = len(history.get("events") or [])
        if agent.case == "a" and agent.started:
            # The agent child is drawn inside its node, its tool activities real nodes a
            # row links to (OD4); its own batch children are named as the run's are.
            children = {
                **children,
                **await self._child_histories(client, agent.history),
                agent.workflow_id: agent.history,
            }
            nested = {agent.workflow_id: {"pendingActivities": agent.pending}}
            read += len(agent.history.get("events") or [])
            if agent.open and agent.run_id is not None:
                watched.append(
                    (agent.workflow_id, agent.run_id, len(agent.history.get("events") or []))
                )
        live, index = graph.parse_indexed(
            history,
            workflow_id=desc.id,
            run_id=run_id,
            pending={"pendingActivities": pending},
            children=children,
            nested=nested,
        )
        # The cache stores the task queue it was saved for; the file name is only its hash.
        static = load_static(self.home, desc.task_queue)
        painted = graph.merge(static, live) if static is not None else live
        painted["static_graph"] = static is not None
        painted["handler"] = static is None and await self._handler_run(client, desc)
        await self._reader.batch_progress(client, painted, children, watched)
        subagents: list[tuple[str, str, int]] = []
        session, items = await self._session_of(client, desc, agent, index, subagents)
        hosts = {n["parent"] for n in live["nodes"] if isinstance(n.get("parent"), str)}
        snap = _Snapshot(
            graph=graph.links(painted, self._links(temporal.namespace)),
            history=history,
            key=(_change_key(desc), (*watched, *subagents)),
            closed=not _is_open(desc),
            unnamed=_unnamed_batch(live),
            pending=pending,
            session=session,
            nested={host: (agent.history, agent.pending) for host in hosts},
            items=dict(items),
            history_length=read + sum(n for _, _, n in subagents),
            static=static,
        )
        self._snapshots[(desc.id, run_id)] = snap
        self._snapshots.move_to_end((desc.id, run_id))
        while len(self._snapshots) > _SNAPSHOTS:
            self._snapshots.popitem(last=False)
        return snap

    async def _session_of(
        self,
        client: Client,
        desc: WorkflowExecutionDescription,
        agent: _agent.AgentRun,
        index: Mapping[graph.CommandAt, str],
        watched: list[tuple[str, str, int]],
    ) -> tuple[dict[str, Any], list[tuple[int, dict[str, Any]]]]:
        """The run's agent session (:mod:`kontra.cli._session`), or an empty one saying why:
        no agent, no events yet, or events in no history event (a case-``b`` run nobody
        polled; OD1). An open subagent it read is added to ``watched``."""
        meta: dict[str, Any] = {
            "workflow_id": agent.workflow_id,
            "run_id": agent.run_id,
            "type": agent.desc.workflow_type if agent.desc is not None else None,
            "name": None,
            "offering": desc.task_queue,
            "task_queue": desc.task_queue,
            "status": agent.status if agent.started else "pending",
            "case": agent.case,
            # Approvals and callbacks are the seller's to answer here: the page shows their
            # rows read-only and says so (OD7).
            "interactive": False,
        }
        if not agent.started:
            return _empty(meta, "no-events"), []
        try:
            return await self._rendered(client, agent, meta, index, watched)
        except Exception as e:  # an item the renderer cannot take: the graph still answers
            _log(f"the session of {agent.workflow_id}: {_said(e)}")
            return _empty(meta, "undecodable"), []

    async def _rendered(
        self,
        client: Client,
        agent: _agent.AgentRun,
        meta: dict[str, Any],
        index: Mapping[graph.CommandAt, str],
        watched: list[tuple[str, str, int]],
    ) -> tuple[dict[str, Any], list[tuple[int, dict[str, Any]]]]:
        items = await self._reader.items(agent.workflow_id, agent.run_id, agent.history)
        if not items:
            polled = bool(_session.stream_poll_outcomes(agent.history))
            if self._reader.in_bucket(agent.history):
                return _empty(meta, "in-bucket"), []
            if agent.case == "b" and not polled:
                is_agent = _session.has_update(agent.history, ae.SEND_AGENT_MESSAGE_UPDATE)
                return _empty(meta, "not-in-history" if is_agent else "no-agent"), []
            return _empty(meta, "no-events"), []
        nodes = await self._reader.tool_links(agent, items, index)
        handoffs, subagents = await self._reader.subagents(
            client, agent, items, index, watched=watched
        )
        # The turn header's sweep(SweepRequest): the agent's own word for its input's type.
        types = await self._reader.input_types(client, agent)
        session = _session.build(
            items,
            agent=meta,
            nodes=nodes,
            handoffs=handoffs,
            subagents=subagents,
            input_types=types,
        )
        handler = next((t["handler"] for t in session["turns"] if t["handler"]), None)
        if handler and meta["type"]:
            session["agent"]["name"] = f"{meta['type']}.{handler}"
        session["empty"] = False
        return session, items

    async def _same(self, client: Client, desc: WorkflowExecutionDescription, key: _Key) -> bool:
        """Whether nothing a snapshot of ``key`` read has changed: the run (its
        :func:`_change_key`), and each open workflow it read besides (its history's length)."""
        if _change_key(desc) != key[0]:
            return False
        for workflow_id, run_id, length in key[1]:
            seen = await client.get_workflow_handle(workflow_id, run_id=run_id).describe()
            if seen.raw_description.workflow_execution_info.history_length != length:
                return False
        return True

    async def _changed(self, workflow_id: str, run_id: str, last: _Key | None) -> _Snapshot | None:
        """The run's snapshot if it changed since ``last`` (None: in any case)."""
        temporal = await self._client()
        desc = await temporal.client.get_workflow_handle(workflow_id, run_id=run_id).describe()
        if last is not None and await self._same(temporal.client, desc, last):
            return None
        return await self._snapshot(workflow_id, run_id, desc)

    async def _view(self, workflow_id: str, run_id: str | None) -> _Snapshot:
        """The run's snapshot: the one a stream (or an earlier request) built while nothing it
        read has changed since -- so the page's ``/api/session`` after a ``session-stat``
        reads nothing again -- else a fresh one."""
        from kontra._graphs import load_static

        temporal = await self._client()
        desc = await temporal.client.get_workflow_handle(workflow_id, run_id=run_id).describe()
        cached = self._snapshots.get((desc.id, desc.run_id))
        if (
            cached is not None
            and not cached.unnamed
            and cached.static == load_static(self.home, desc.task_queue)
            and await self._same(temporal.client, desc, cached.key)
        ):
            self._snapshots.move_to_end((desc.id, desc.run_id))
            return cached
        return await self._snapshot(workflow_id, run_id, desc)

    async def _workflows(self) -> list[dict[str, Any]]:
        temporal = await self._client()
        found: list[dict[str, Any]] = []
        async for ex in temporal.client.list_workflows(
            f"WorkflowType != '{_BATCH}'", limit=WORKFLOWS_LIMIT, page_size=WORKFLOWS_LIMIT
        ):
            found.append(
                {
                    "workflow_id": ex.id,
                    "run_id": ex.run_id,
                    "type": ex.workflow_type,
                    "status": ex.status.name.lower() if ex.status is not None else None,
                    "start_time": ex.start_time.isoformat() if ex.start_time else None,
                    "close_time": ex.close_time.isoformat() if ex.close_time else None,
                    "task_queue": ex.task_queue,
                }
            )
        return found

    async def _io(self, workflow_id: str, run_id: str | None, node: str) -> dict[str, Any]:
        snap = await self._view(workflow_id, run_id)
        history, pending = snap.history, snap.pending
        if node:
            nodes = [n for n in snap.graph.get("nodes") or [] if n.get("id") == node]
            if not nodes:
                raise ConsoleError(HTTPStatus.NOT_FOUND, "no such node in this run's graph")
            parent = nodes[0].get("parent")
            if isinstance(parent, str) and parent in snap.nested:
                # A node of the agent child drawn nested: its event is in the child's history.
                history, pending = snap.nested[parent]
            # That node's payloads alone: no other node's, nor the workflow's.
            asked = {**snap.graph, "nodes": nodes, "workflow": None}
        else:
            asked = {**snap.graph, "nodes": []}
        out = await graph.with_io(
            asked, history, _Shown(self._codec()), pending={"pendingActivities": pending}
        )
        return dict(out["nodes"][0]["io"] if node else out["workflow"]["io"])

    async def _io_event(self, workflow_id: str, run_id: str | None, offset: int) -> dict[str, Any]:
        """One agent event's verbatim body, kept off ``/api/session`` (seam ⑤)."""
        snap = await self._view(workflow_id, run_id)
        event = snap.items.get(offset)
        if event is None:
            raise ConsoleError(HTTPStatus.NOT_FOUND, "no agent event at that offset")
        return {"offset": offset, "body": event.get(ae.EVENT)}

    # ------------------------------------------------------------------ the routes

    def page(self) -> tuple[bytes, str]:
        """The page with a fresh nonce, and the nonce."""
        nonce = secrets.token_urlsafe(18)
        return self._page.replace(_NONCE, nonce).encode(), nonce

    def workflows(self) -> list[dict[str, Any]]:
        return self._run(self._workflows())

    def graph(self, workflow_id: str, run_id: str | None) -> graph.Graph:
        return self._run(self._view(workflow_id, run_id)).graph

    def session(self, workflow_id: str, run_id: str | None) -> dict[str, Any]:
        """``{"graph", "session"}``: both panes from one snapshot (OD2)."""
        snap = self._run(self._view(workflow_id, run_id))
        return {"graph": snap.graph, "session": snap.session}

    def io(self, workflow_id: str, run_id: str | None, node: str) -> dict[str, Any]:
        return self._run(self._io(workflow_id, run_id, node))

    def io_event(self, workflow_id: str, run_id: str | None, offset: int) -> dict[str, Any]:
        return self._run(self._io_event(workflow_id, run_id, offset))

    def stream(self, workflow_id: str, run_id: str | None) -> Iterator[bytes]:
        """The frames of ``/api/events`` (OD2): a ``session-stat`` -- a change signal, no
        payload and no summary, so the page's one ``X-Kontra-Console`` fetch of
        ``/api/session`` is where content reaches it -- for the run's snapshot (computed
        before anything is sent, so an error is still an answer of its own), again each time
        it changes, a keepalive comment when nothing did for :data:`_KEEPALIVE` seconds, and
        ``end`` after a closed run's. An error midway ends the stream (EventSource
        reconnects).

        A batch child is named from its own history (its input is encrypted), which names
        nothing until the child schedules its first activity, and then grows while the
        run's does not: while one is unnamed, the snapshot is read again at every poll, and
        signalled when it differs. An agent child's or a subagent's growth is in the
        snapshot's key."""
        snap = self._run(self._snapshot(workflow_id, run_id))
        run_id = str(snap.graph["workflow"]["run_id"])
        rev, sent = 0, _shown(snap)
        yield f"retry: {_RETRY_MS}\n\n".encode() + _event("session-stat", _stat(snap, rev))
        last, beat, closed, unnamed = snap.key, time.monotonic(), snap.closed, snap.unnamed
        while not closed:
            if self._closing.wait(_POLL):
                return
            fresh = self._run(self._changed(workflow_id, run_id, None if unnamed else last))
            if fresh is not None:
                last, closed, unnamed = fresh.key, fresh.closed, fresh.unnamed
                shown = _shown(fresh)
                if shown != sent:
                    rev, sent, beat = rev + 1, shown, time.monotonic()
                    yield _event("session-stat", _stat(fresh, rev))
                    continue
            if time.monotonic() - beat >= _KEEPALIVE:
                beat = time.monotonic()
                yield b": keepalive\n\n"
        yield _event("end", {})


def _shown(snap: _Snapshot) -> bytes:
    """What the page would be shown of ``snap``: a stat is sent when this differs."""
    return _dumps({"graph": snap.graph, "session": snap.session})


def _stat(snap: _Snapshot, rev: int) -> dict[str, Any]:
    """A ``session-stat`` frame (OD2): how much there is, never what -- the events in the
    histories read, the stream's event count and last offset, the run's status, the last
    turn's number, and ``rev``, which moves each time the snapshot does."""
    footer = snap.session.get("footer") or {}
    turns = snap.session.get("turns") or []
    return {
        "history_length": snap.history_length,
        "count": footer.get("count", 0),
        "last_offset": footer.get("last_offset"),
        "status": (snap.graph.get("workflow") or {}).get("status"),
        "turn": turns[-1].get("turn_number") if turns else None,
        "rev": rev,
    }


def _empty(agent: dict[str, Any], reason: str) -> dict[str, Any]:
    """A session with no rows, and why: ``no-agent`` (the run is no agent), ``no-events``
    (none yet), ``not-in-history`` (a run nobody polled: its events are in its in-memory log
    alone), ``in-bucket`` (a job root's stream: its items are in the bucket, I39),
    ``undecodable`` (the renderer could not take what was read; the log says why)."""
    session = _session.build([], agent=agent)
    session.update(empty=True, reason=reason)
    return session


# --------------------------------------------------------------------------- HTTP


_PAGE_POLICY = (
    "default-src 'none'; script-src 'nonce-{n}'; style-src 'nonce-{n}'; connect-src 'self'; "
    "img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)


def _send(
    handler: BaseHTTPRequestHandler,
    status: HTTPStatus,
    body: bytes,
    kind: str,
    headers: Mapping[str, str] | None = None,
) -> None:
    handler.send_response(status)
    handler.send_header("Content-Type", kind)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("X-Content-Type-Options", "nosniff")
    handler.send_header("Cache-Control", "no-store")
    for name, value in (headers or {}).items():
        handler.send_header(name, value)
    handler.end_headers()
    handler.wfile.write(body)


def _json(handler: BaseHTTPRequestHandler, status: HTTPStatus, value: object) -> None:
    _send(handler, status, _dumps(value), "application/json")


def _error(handler: BaseHTTPRequestHandler, e: BaseException) -> None:
    _json(handler, _status_of(e), {"error": _said(e)})


def _log(line: str) -> None:
    """One line to the codec server's log (``$KONTRA_HOME/codec.log``): never a payload."""
    print(f"console {line}", file=sys.stderr, flush=True)


def _query(raw: str) -> dict[str, str]:
    try:
        parsed = parse_qs(raw, keep_blank_values=True, strict_parsing=False, max_num_fields=8)
    except ValueError:
        raise ConsoleError(HTTPStatus.BAD_REQUEST, "too many query parameters") from None
    out: dict[str, str] = {}
    for name, values in parsed.items():
        if len(values) != 1:
            raise ConsoleError(HTTPStatus.BAD_REQUEST, f"{name} is given more than once")
        out[name] = values[0]
    return out


def _run_args(query: Mapping[str, str]) -> tuple[str, str | None]:
    workflow_id, run_id = query.get("w", ""), query.get("r") or None
    if not workflow_id or len(workflow_id) > _ID_MAX:
        raise ConsoleError(HTTPStatus.BAD_REQUEST, "w is a workflow id")
    if run_id is not None and _RUN_ID.fullmatch(run_id) is None:
        raise ConsoleError(HTTPStatus.BAD_REQUEST, "r is a run id")
    return workflow_id, run_id


#: A stream offset: a small non-negative integer, nothing else.
_OFFSET = re.compile(r"0|[1-9][0-9]{0,17}", re.ASCII)


def _offset(query: Mapping[str, str]) -> int:
    given = query.get("event", "")
    if _OFFSET.fullmatch(given) is None:
        raise ConsoleError(HTTPStatus.BAD_REQUEST, "event is a stream offset")
    return int(given)


def handle(handler: BaseHTTPRequestHandler, console: Console, path: str, query: str) -> None:
    """Answer ``GET path?query`` for one of :data:`ROUTES`; the codec server has checked the
    ``Host`` and that an ``Origin``, if any, is the console's own."""
    if path in API_ROUTES:
        # The page's own requests are same-origin; a navigation or another site's are not.
        site = handler.headers.get("Sec-Fetch-Site")
        if site is not None and site not in ("same-origin", "none"):
            _json(handler, HTTPStatus.FORBIDDEN, {"error": "same-origin requests only"})
            return
    try:
        args = _query(query)
        if path == PAGE_ROUTE:
            body, nonce = console.page()
            policy = _PAGE_POLICY.format(n=nonce)
            headers = {
                "Content-Security-Policy": policy,
                "Referrer-Policy": "no-referrer",
                "X-Frame-Options": "DENY",
                "Cross-Origin-Opener-Policy": "same-origin",
            }
            _send(handler, HTTPStatus.OK, body, "text/html; charset=utf-8", headers)
        elif path == "/api/workflows":
            _json(handler, HTTPStatus.OK, console.workflows())
        elif path == "/api/graph":
            _json(handler, HTTPStatus.OK, console.graph(*_run_args(args)))
        elif path in ("/api/session", "/api/io"):
            # Structure and summaries, or verbatim bodies: behind the header either way.
            if handler.headers.get(IO_HEADER) != "1":
                raise ConsoleError(HTTPStatus.FORBIDDEN, f"{IO_HEADER}: 1 is required")
            if path == "/api/session":
                _json(handler, HTTPStatus.OK, console.session(*_run_args(args)))
            elif "event" in args:
                _json(handler, HTTPStatus.OK, console.io_event(*_run_args(args), _offset(args)))
            else:
                if len(node := args.get("node", "")) > _ID_MAX:
                    raise ConsoleError(HTTPStatus.BAD_REQUEST, "node is a node id")
                _json(handler, HTTPStatus.OK, console.io(*_run_args(args), node))
        else:  # /api/events
            _events(handler, console, *_run_args(args))
    except (BrokenPipeError, ConnectionResetError):
        return
    except Exception as e:
        _log(f"{path}: {_said(e)}")
        _error(handler, e)


def _events(
    handler: BaseHTTPRequestHandler, console: Console, workflow_id: str, run_id: str | None
) -> None:
    if not console.streams.acquire(blocking=False):
        raise ConsoleError(
            HTTPStatus.SERVICE_UNAVAILABLE, f"{MAX_STREAMS} live graphs are open already"
        )
    try:
        frames = console.stream(workflow_id, run_id)
        first = next(frames)  # an error here is still an answer of its own
        handler.send_response(HTTPStatus.OK)
        handler.send_header("Content-Type", "text/event-stream")
        handler.send_header("X-Content-Type-Options", "nosniff")
        handler.send_header("Cache-Control", "no-store")
        handler.end_headers()
        handler.wfile.write(first)
        handler.wfile.flush()
        try:
            for frame in frames:
                handler.wfile.write(frame)
                handler.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception as e:  # midway: the stream ends, and EventSource tries again
            _log(f"/api/events: {_said(e)}")
    finally:
        console.streams.release()
