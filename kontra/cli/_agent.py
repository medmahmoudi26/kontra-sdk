"""What the dev console reads of a run's agent (``docs/decisions.md`` I36): the agent workflow,
its ``turn_events`` stream as history holds it, decoded with this machine's codec, the graph
node each tool row links to, and its subagents' own sessions nested under their handoffs.

:mod:`kontra.cli._session` renders; this module fetches and decodes. It holds no Temporal
client of its own: :class:`Reader` is handed one per snapshot (the console's), and keeps only
what history makes final -- a closed run's history, a poll outcome's items, an activity input's
``tool_id`` -- so a live stream re-reads nothing it already opened.

The agent of a viewed run is (names constructed, never parsed -- I1):

* case ``a``, a bound-handler job (``kontra.turn.v1``): the child
  :func:`~kontra.contract.names.agent_workflow_id` builds from the job id, read only when the
  job's own history says it started that child;
* case ``b``, any other run: the run itself, an agent when it hosts the stream (a harness
  agent's ``send_agent_message`` front door is in its history, or a poll read ``turn_events``).

Items are in history only as stream-poll Update outcomes (I31): a ``kontra.turn.v1`` job's
turn activity polls its agent child for the whole turn, a subagent's parent's turn activity
polls the subagent; a case-``b`` run nobody polled has its events in its in-memory log alone.
"""

from __future__ import annotations

import base64
import json
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format
from temporalio.api.common.v1 import Payload
from temporalio.converter import PayloadCodec, PayloadConverter

from kontra import graph
from kontra.cli import _session
from kontra.codec import ENCODING, STREAM_ENCODING
from kontra.contract.names import agent_workflow_id, job_id_from_workflow_id
from kontra.gate_client.models import STREAM_ITEM_MAX
from kontra.serve import _agent_events as ae

if TYPE_CHECKING:
    from temporalio.client import Client, WorkflowExecutionDescription

__all__ = [
    "SUBAGENT_DEPTH",
    "SUBAGENT_HISTORIES",
    "TURN_WORKFLOW",
    "AgentRun",
    "Reader",
    "batch_progress",
    "started_children",
]

#: The wrapper workflow of a bound-handler job (``kontra.serve.turn``).
TURN_WORKFLOW = "kontra.turn.v1"
#: Subagent nesting bounds (OD5): how deep, and how many subagent histories one view reads.
SUBAGENT_DEPTH = 3
SUBAGENT_HISTORIES = 16
#: A stream item's base64 over this is not shown: #52's per-item cap (``STREAM_ITEM_MAX``).
_ITEM_B64_MAX = (STREAM_ITEM_MAX + 2) // 3 * 4
#: What a reader remembers of what history made final.
_CACHE = 4096
_BATCH = "kontra.batch.v1"


@dataclass
class AgentRun:
    """The agent workflow of a viewed run, and what was read of it."""

    workflow_id: str
    run_id: str | None
    #: ``a``: a ``kontra.turn.v1`` job's agent child; ``b``: the viewed run itself.
    case: str
    history: dict[str, Any] = field(default_factory=dict[str, Any])
    #: Its own description (case ``a``: the child's; ``b``: the viewed run's).
    desc: WorkflowExecutionDescription | None = None
    #: Not started yet: a turn job whose history has not started its agent child.
    started: bool = True

    @property
    def status(self) -> str:
        if self.desc is None or self.desc.status is None:
            return "running"
        return self.desc.status.name.lower()

    @property
    def open(self) -> bool:
        return self.started and self.status == "running"

    @property
    def pending(self) -> list[dict[str, Any]]:
        if self.desc is None:
            return []
        return [json_format.MessageToDict(p) for p in self.desc.raw_description.pending_activities]


def started_children(history: Mapping[str, Any]) -> dict[str, tuple[int, str | None]]:
    """Each child workflow the run's history says it started: its workflow id (as the run's
    own command named it, never built or parsed) -> the initiating event id and the first run
    id ``ChildWorkflowExecutionStarted`` names (None until it started)."""
    found: dict[str, tuple[int, str | None]] = {}
    by_event: dict[int, str] = {}
    for event in history.get("events") or []:
        event_id = _int(event.get("eventId"))
        initiated = event.get("startChildWorkflowExecutionInitiatedEventAttributes")
        if isinstance(initiated, dict) and event_id is not None:
            workflow_id = initiated.get("workflowId")
            if isinstance(workflow_id, str):
                found[workflow_id] = (event_id, None)
                by_event[event_id] = workflow_id
        started = event.get("childWorkflowExecutionStartedEventAttributes")
        if isinstance(started, dict):
            workflow_id = by_event.get(_int(started.get("initiatedEventId")) or 0)
            run_id = (started.get("workflowExecution") or {}).get("runId")
            if workflow_id is not None and isinstance(run_id, str):
                found[workflow_id] = (found[workflow_id][0], run_id)
    return found


def batch_progress(painted: graph.Graph, children: Mapping[str, Any]) -> None:
    """Each batch node's ``progress`` (OD3), from its child's history the console already read:
    ``{"done", "total", "pages"}`` -- the item activity's completions of its schedulings, and
    whether a ``pages=`` activity ran beside it (the batch's own first run says it)."""
    for node in painted.get("nodes") or []:
        ref = node.get("ref") or {}
        name = node.get("name")
        if node.get("kind") != "child" or not isinstance(name, str) or not name.startswith(_BATCH):
            continue
        history = children.get(ref.get("workflow_id") or "")
        if not isinstance(history, Mapping):
            continue
        scheduled: dict[int, str] = {}
        done: dict[str, int] = {}
        for event in history.get("events") or []:
            attrs = event.get("activityTaskScheduledEventAttributes")
            event_id = _int(event.get("eventId"))
            if isinstance(attrs, dict) and event_id is not None:
                scheduled[event_id] = str((attrs.get("activityType") or {}).get("name"))
            closed = event.get("activityTaskCompletedEventAttributes")
            if isinstance(closed, dict):
                kind = scheduled.get(_int(closed.get("scheduledEventId")) or 0)
                if kind is not None:
                    done[kind] = done.get(kind, 0) + 1
        totals: dict[str, int] = {}
        for kind in scheduled.values():
            totals[kind] = totals.get(kind, 0) + 1
        if not totals:
            continue
        item = max(totals, key=lambda k: (totals[k], k))
        node["progress"] = {
            "done": done.get(item, 0),
            "total": totals[item],
            "pages": len(totals) > 1,
        }


class _Budget:
    def __init__(self, left: int) -> None:
        self.left = left


class Reader:
    """Reads agent sessions with ``codec`` (made per use: job keys come and go), remembering
    what history made final. One per console; used on the console's event loop alone."""

    def __init__(self, codec: Callable[[], PayloadCodec]) -> None:
        self._codec = codec
        self._histories: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()
        self._polls: OrderedDict[tuple[str, str | None, int], list[tuple[int, dict[str, Any]]]] = (
            OrderedDict()
        )
        self._tool_ids: OrderedDict[tuple[str, str | None, int], list[str]] = OrderedDict()

    # ------------------------------------------------------------------ the agent

    async def agent_of(
        self, client: Client, desc: WorkflowExecutionDescription, history: dict[str, Any]
    ) -> AgentRun:
        """The viewed run's agent: its ``kontra.turn.v1`` job's agent child (case ``a``),
        else the run itself (case ``b``)."""
        job_id = job_id_from_workflow_id(desc.id)
        if desc.workflow_type != TURN_WORKFLOW or job_id is None:
            return AgentRun(desc.id, desc.run_id, "b", history, desc)
        workflow_id = agent_workflow_id(job_id)
        started = started_children(history).get(workflow_id)
        if started is None or started[1] is None:  # the wrapper has not started it yet
            return AgentRun(workflow_id, None, "a", started=False)
        run_id = started[1]
        child = await client.get_workflow_handle(workflow_id, run_id=run_id).describe()
        return AgentRun(
            workflow_id, run_id, "a", await self.history(client, workflow_id, run_id), child
        )

    async def history(self, client: Client, workflow_id: str, run_id: str) -> dict[str, Any]:
        """One run's history; a closed one is final, so it is remembered."""
        key = (workflow_id, run_id)
        cached = self._histories.get(key)
        if cached is not None:
            self._histories.move_to_end(key)
            return cached
        history = (
            await client.get_workflow_handle(workflow_id, run_id=run_id).fetch_history()
        ).to_json_dict()
        if _closed(history):
            _remember(self._histories, key, history)
        return history

    # ------------------------------------------------------------------ the stream

    async def items(
        self, workflow_id: str, run_id: str | None, history: Mapping[str, Any]
    ) -> list[tuple[int, dict[str, Any]]]:
        """The ``(offset, AgentEvent)`` of ``history``'s ``turn_events`` stream, decoded with
        this machine's codec -- the whole age ``PollResult`` on an agent child, the
        ``binary/kontra-stream`` envelope on a job root: one ``kontra.codec`` path. An outcome
        sealed to other keys yields nothing (its events are not this machine's to show)."""
        codec = self._codec()
        out: list[tuple[int, dict[str, Any]]] = []
        for event_id, outcome in _session.stream_poll_outcomes(history):
            key = (workflow_id, run_id, event_id)
            found = self._polls.get(key)
            if found is None:
                poll = await _open_poll(codec, outcome)
                found = (
                    []
                    if poll is None
                    else _session.events_in_poll_result(poll, _decode_item, item_max=_ITEM_B64_MAX)
                )
                _remember(self._polls, key, found)
            out += found
        return out

    @staticmethod
    def in_bucket(history: Mapping[str, Any]) -> bool:
        """Whether ``history``'s stream is a job root's, its items in the bucket (I39): the
        console holds no gate session to fetch them, so it shows none and says so."""
        return any(
            _envelope_encoding(outcome) == STREAM_ENCODING
            for _, outcome in _session.stream_poll_outcomes(history)
        )

    # ------------------------------------------------------------------ linking (OD4)

    async def tool_links(
        self,
        agent: AgentRun,
        items: Sequence[tuple[int, Mapping[str, Any]]],
        index: Mapping[graph.CommandAt, str],
    ) -> dict[str, dict[str, Any]]:
        """``tool_id`` -> ``{node, event_id, fallback}``: the agent's tool activity that ran
        that call. Exact by the ``tool_id`` the harness carries in each tool activity's input
        (``AgentToolContext``), decrypted here; when an input is sealed to another key, the
        k-th call of a name is the k-th unclaimed activity of that name, marked a fallback."""
        calls: list[tuple[str, str]] = []
        for _offset, event in sorted(items, key=lambda it: it[0]):
            body = event.get(ae.EVENT)
            if isinstance(body, Mapping) and body.get(ae.EVENT_TYPE) == ae.TOOL_REQUESTED:
                tool_id, name = body.get("tool_id"), body.get("tool_name")
                if isinstance(tool_id, str) and isinstance(name, str):
                    calls.append((tool_id, name))
        names = {name for _, name in calls}
        where = agent.workflow_id if agent.case == "a" else None
        exact: dict[str, int] = {}
        by_name: dict[str, list[int]] = {}
        for event_id, name, inputs in _scheduled(agent.history):
            if name not in names or (where, event_id) not in index:
                continue
            by_name.setdefault(name, []).append(event_id)
            for tool_id in await self._ids_in(agent, event_id, inputs):
                exact.setdefault(tool_id, event_id)
        claimed = set(exact.values())
        links: dict[str, dict[str, Any]] = {}
        for tool_id, name in calls:
            if tool_id in exact:
                event_id, fallback = exact[tool_id], False
            else:
                free = [e for e in by_name.get(name, []) if e not in claimed]
                if not free:
                    continue
                event_id, fallback = free[0], True
                claimed.add(event_id)
            links[tool_id] = {
                "node": index[(where, event_id)],
                "event_id": event_id,
                "fallback": fallback,
            }
        return links

    async def _ids_in(self, agent: AgentRun, event_id: int, inputs: object) -> list[str]:
        key = (agent.workflow_id, agent.run_id, event_id)
        found = self._tool_ids.get(key)
        if found is None:
            found = _tool_ids(await _decode_inputs(self._codec(), inputs))
            _remember(self._tool_ids, key, found)
        return found

    # ------------------------------------------------------------------ subagents (OD5)

    async def subagents(
        self,
        client: Client,
        agent: AgentRun,
        items: Sequence[tuple[int, Mapping[str, Any]]],
        index: Mapping[graph.CommandAt, str] | None,
        *,
        depth: int = 1,
        budget: _Budget | None = None,
        watched: list[tuple[str, str, int]] | None = None,
    ) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
        """Each subagent's child node (``handoffs``) and its view (``{"rows"}`` nested, or
        ``{"reason"}`` linking out), by the workflow id its ``subagent_started`` event names --
        whole and opaque, and only one the agent's own history says it started. Bounded:
        :data:`SUBAGENT_DEPTH` deep, :data:`SUBAGENT_HISTORIES` histories in all. An open
        subagent read is added to ``watched`` (its history grows while the agent's does not)."""
        budget = budget or _Budget(SUBAGENT_HISTORIES)
        started = started_children(agent.history)
        where = agent.workflow_id if agent.case == "a" else None
        handoffs: dict[str, str] = {}
        views: dict[str, dict[str, Any]] = {}
        for _offset, event in sorted(items, key=lambda it: it[0]):
            body = event.get(ae.EVENT)
            if not isinstance(body, Mapping) or body.get(ae.EVENT_TYPE) != ae.SUBAGENT_STARTED:
                continue
            workflow_id = body.get("workflow_id")
            if not isinstance(workflow_id, str) or workflow_id in views:
                continue
            if workflow_id not in started:
                views[workflow_id] = {"reason": "not a child the agent's history started"}
                continue
            initiated, run_id = started[workflow_id]
            node = index.get((where, initiated)) if index is not None else None
            if node is not None:
                handoffs[workflow_id] = node
            if depth > SUBAGENT_DEPTH:
                views[workflow_id] = {"reason": f"nested deeper than {SUBAGENT_DEPTH} subagents"}
            elif budget.left <= 0:
                views[workflow_id] = {"reason": f"past {SUBAGENT_HISTORIES} subagents in one view"}
            elif run_id is None:
                views[workflow_id] = {"reason": "not started yet"}
            else:
                budget.left -= 1
                views[workflow_id] = await self._nested(
                    client, workflow_id, run_id, depth, budget, watched
                )
        return handoffs, views

    async def _nested(
        self,
        client: Client,
        workflow_id: str,
        run_id: str,
        depth: int,
        budget: _Budget,
        watched: list[tuple[str, str, int]] | None,
    ) -> dict[str, Any]:
        history = await self.history(client, workflow_id, run_id)
        if watched is not None and not _closed(history):
            watched.append((workflow_id, run_id, len(history.get("events") or [])))
        sub = AgentRun(workflow_id, run_id, "a", history)
        items = await self.items(workflow_id, run_id, history)
        if not items:
            return {"rows": []}
        _, views = await self.subagents(
            client, sub, items, None, depth=depth + 1, budget=budget, watched=watched
        )
        return {"rows": _session.nested_rows(_session.build(items, agent={}, subagents=views))}


# --------------------------------------------------------------------------- decoding

#: The events that close a run: a closed run's history is final.
_CLOSED = frozenset(
    f"EVENT_TYPE_WORKFLOW_EXECUTION_{kind}"
    for kind in ("COMPLETED", "FAILED", "TIMED_OUT", "CANCELED", "TERMINATED", "CONTINUED_AS_NEW")
)


def _closed(history: Mapping[str, Any]) -> bool:
    events = history.get("events") or []
    return bool(events) and str(events[-1].get("eventType")) in _CLOSED


async def _open_poll(codec: PayloadCodec, outcome: Mapping[str, Any]) -> dict[str, Any] | None:
    """A poll outcome's ``PollResult`` as JSON, opened with this machine's codec; None for
    anything it cannot open or that is not one (never its bytes)."""
    try:
        payload = json_format.ParseDict(dict(outcome), Payload(), ignore_unknown_fields=True)
    except json_format.ParseError:
        return None
    if payload.metadata.get("encoding") not in (ENCODING, STREAM_ENCODING):
        return None  # not sealed: a poll outcome no Kontra codec wrote
    try:
        [opened] = await codec.decode([payload])
    except Exception:  # another key's, a job key gone, tampered, junk: no events
        return None
    if opened.metadata.get("encoding") != b"json/plain":
        return None
    try:
        value = json.loads(opened.data)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def _envelope_encoding(outcome: Mapping[str, Any]) -> bytes | None:
    encoding = (outcome.get("metadata") or {}).get("encoding")
    try:
        return base64.b64decode(encoding) if isinstance(encoding, str) else None
    except ValueError:
        return None


def _decode_item(b64: str) -> object:
    """One stream item's base64 ``Payload`` as its value (the ``AgentEvent`` JSON)."""
    payload = Payload()
    payload.ParseFromString(base64.b64decode(b64))
    return PayloadConverter.default.from_payloads([payload])[0]


def _scheduled(history: Mapping[str, Any]) -> list[tuple[int, str, object]]:
    found: list[tuple[int, str, object]] = []
    for event in history.get("events") or []:
        attrs = event.get("activityTaskScheduledEventAttributes")
        event_id = _int(event.get("eventId"))
        if isinstance(attrs, dict) and event_id is not None:
            name = (attrs.get("activityType") or {}).get("name")
            if isinstance(name, str):
                found.append((event_id, name, attrs.get("input")))
    return found


async def _decode_inputs(codec: PayloadCodec, inputs: object) -> list[object]:
    """An activity's input values, opened with this machine's codec: what it cannot open
    (sealed to another key) is left out, so its ``tool_id`` stays unknown."""
    raw = (inputs or {}).get("payloads") if isinstance(inputs, Mapping) else None
    if not isinstance(raw, list):
        return []
    values: list[object] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        try:
            payload = json_format.ParseDict(dict(item), Payload(), ignore_unknown_fields=True)
            if payload.metadata.get("encoding") == ENCODING:
                [payload] = await codec.decode([payload])
            if payload.metadata.get("encoding") == b"json/plain":
                values.append(PayloadConverter.default.from_payloads([payload])[0])
        except Exception:  # noqa: S112 - sealed to another key, tampered: nothing to read
            continue
    return values


def _tool_ids(values: object) -> list[str]:
    """Every ``tool_id`` string in a decoded activity input: the harness carries one in the
    ``AgentToolContext`` it passes each tool activity."""
    found: list[str] = []
    stack: list[object] = [values]
    while stack:
        cur = stack.pop()
        if isinstance(cur, Mapping):
            tool_id = cur.get("tool_id")
            if isinstance(tool_id, str):
                found.append(tool_id)
            stack.extend(cur.values())
        elif isinstance(cur, list | tuple):
            stack.extend(cur)
    return found


def _remember(cache: OrderedDict[Any, Any], key: Any, value: Any) -> None:
    cache[key] = value
    while len(cache) > _CACHE:
        cache.popitem(last=False)


def _int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isascii() and value.isdigit() and len(value) <= 19:
        return int(value)
    return None
