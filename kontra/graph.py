"""The workflow graph (decisions I26; kontra-sdk#40, #41): a run's Temporal history read
back as nodes, edges and a status per node.

Temporal keeps no definition of a workflow, only what each run did, so the structure is
read from the commands a run's workflow tasks made. Commands one workflow task made share
a ``workflowTaskCompletedEventId`` and are one parallel group (a batch), split only where a
local activity resolved inside the task: what the task commanded after a local activity's
marker is a batch of its own. A batch's predecessors are the completions it observed: the
close events written between the previous batch's first command and its own (a local
activity closes at its marker), since what arrives while a task is in flight is written
after that task's commands. So an edge means "this waited on that", never only "came
later". When no close is in that window (a signal, an empty task) the previous batch is
the predecessor; the first batch's is ``start``. The terminal event the workflow commanded
(completed, failed, canceled, continued as new) leads to ``end`` from what it observed:
from its own batch's predecessors when it was commanded with that batch, never from what
that batch started. A run terminated, timed out or still running has no edge to ``end``;
what a closed run left open ended with it.

Repeats fold before nodes are named. Each batch has a signature, the set of
``(kind, name)`` it holds, and tandem repeats of signatures fold innermost first into
loops, a trailing partial iteration included; every batch then sits at a position of the
folded sequence, and a node is one ``(kind, name)`` at one position. Its id is the name,
``name#k`` from the second position holding it on: a loop over one call site is one node
(with a self or back edge), ``a; b; a`` is ``a``, ``b``, ``a#2``. The same call site
therefore has the same id in every run of the same code, which is what :func:`union` (the
runs a mock explored, structure only) and :func:`merge` (a live run painted on them) match
by.

:func:`parse` is total over anything history-shaped and never emits a payload -- no input,
result, heartbeat, memo or failure message, encrypted or not (seam ⑤). A failure is its
kind. The payloads it reads are the ``_tracer-data`` header (for the trace and span ids),
a local activity marker's own record (for its type and attempt), and a batch child's
input when it is plain JSON (for the names of the activities it was asked to run; an
encrypted one is never opened). Inputs, results and
failure messages are :func:`with_io`'s, a separate step that decodes them through a codec.
:func:`links` adds Temporal UI and Grafana URLs, and :func:`mermaid` draws a graph as text.

Nothing here imports Temporal at module level: histories arrive as JSON
(``WorkflowHistory.to_json_dict()``, ``temporal workflow show --output json``, the UI's
API), so the fake gate, the CLI and a page server read them without loading the SDK.
"""

from __future__ import annotations

import base64
import binascii
import bisect
import copy
import json
import operator
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import compress
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote, urlsplit

if TYPE_CHECKING:
    from temporalio.api.common.v1 import Payload
    from temporalio.converter import PayloadCodec

__all__ = [
    "END",
    "START",
    "Graph",
    "HistoryJSON",
    "LinkConfig",
    "links",
    "merge",
    "mermaid",
    "parse",
    "union",
    "with_io",
]

#: The pseudo-node every run starts from; it appears only in edges.
START = "start"
#: The pseudo-node a run that closed by its own command ends at; only in edges.
END = "end"

#: A graph as JSON-able data: ``{"version", "workflow", "nodes", "edges"}`` (decisions I26).
Graph = dict[str, Any]
#: What :func:`parse` reads: a history's proto3 JSON (``{"events": [...]}``, or the UI's
#: ``{"history": {"events": [...]}}``), or its list of events.
HistoryJSON = Mapping[str, Any] | Sequence[Any]

_VERSION = 1
#: ``kontra._parallel.BATCH_WORKFLOW``, written out: that module imports Temporal.
_BATCH_WORKFLOW = "kontra.batch.v1"
_BATCH_LABEL = "execute_activities_in_parallel"
#: The marker sdk-core records for a local activity; no other marker is a node.
_LOCAL_ACTIVITY = "core_local_activity"
#: The header ``TracingInterceptor`` writes its carrier under (stored unencoded: kontra
#: keeps headers out of the codec).
_TRACER_HEADER = "_tracer-data"
#: The longest loop body folded, in batches.
_PERIOD_MAX = 16
#: How deep a failure's causes are followed to find its kind.
_CAUSE_DEPTH = 32
#: The longest header or marker payload (base64) read.
_PAYLOAD_MAX = 1 << 16
#: The longest batch child's input (base64) read: Temporal's 2 MiB payload, base64'd.
_SPEC_MAX = 3 << 20
#: The fields of ``kontra._parallel._Spec`` that name what a batch runs: its item activity
#: and its ``pages=`` activity.
_SPEC_NAMES = ("activity", "pages")
#: Grafana Explore's pane key: three of [0-9a-z], one a letter.
_PANE = "kon"
_NO_IO: dict[str, Any] = {"input": None, "result": None, "failure": None}

#: Node kinds; when two share a name, the first keeps it (the others get ``~kind``).
_KINDS = ("activity", "local_activity", "child", "nexus", "timer")
#: A node's status among its latest batch's occurrences: the first of these present.
_PRECEDENCE = ("running", "scheduled", "failed", "timed_out", "canceled", "terminated", "completed")
_OPEN = frozenset({"scheduled", "running"})

_TERMINAL = {
    "WorkflowExecutionCompleted": "completed",
    "WorkflowExecutionFailed": "failed",
    "WorkflowExecutionTimedOut": "timed_out",
    "WorkflowExecutionCanceled": "canceled",
    "WorkflowExecutionTerminated": "terminated",
    "WorkflowExecutionContinuedAsNew": "continued_as_new",
}
#: The terminal events the workflow's own code commanded: only these lead to ``end``.
_COMMANDED = frozenset(
    {
        "WorkflowExecutionCompleted",
        "WorkflowExecutionFailed",
        "WorkflowExecutionCanceled",
        "WorkflowExecutionContinuedAsNew",
    }
)
#: The events that close a command: (its kind, the attribute naming it, the status).
_CLOSES = {
    "ActivityTaskCompleted": ("activity", "scheduledEventId", "completed"),
    "ActivityTaskFailed": ("activity", "scheduledEventId", "failed"),
    "ActivityTaskTimedOut": ("activity", "scheduledEventId", "timed_out"),
    "ActivityTaskCanceled": ("activity", "scheduledEventId", "canceled"),
    "TimerFired": ("timer", "startedEventId", "completed"),
    "TimerCanceled": ("timer", "startedEventId", "canceled"),
    "StartChildWorkflowExecutionFailed": ("child", "initiatedEventId", "failed"),
    "ChildWorkflowExecutionCompleted": ("child", "initiatedEventId", "completed"),
    "ChildWorkflowExecutionFailed": ("child", "initiatedEventId", "failed"),
    "ChildWorkflowExecutionCanceled": ("child", "initiatedEventId", "canceled"),
    "ChildWorkflowExecutionTimedOut": ("child", "initiatedEventId", "timed_out"),
    "ChildWorkflowExecutionTerminated": ("child", "initiatedEventId", "terminated"),
    "NexusOperationCompleted": ("nexus", "scheduledEventId", "completed"),
    "NexusOperationFailed": ("nexus", "scheduledEventId", "failed"),
    "NexusOperationCanceled": ("nexus", "scheduledEventId", "canceled"),
    "NexusOperationTimedOut": ("nexus", "scheduledEventId", "timed_out"),
}
#: The events that start a command without closing it: (its kind, the attribute naming it).
_STARTS = {
    "ActivityTaskStarted": ("activity", "scheduledEventId"),
    "ChildWorkflowExecutionStarted": ("child", "initiatedEventId"),
    "NexusOperationStarted": ("nexus", "scheduledEventId"),
}
_COMMANDS = (
    "ActivityTaskScheduled",
    "StartChildWorkflowExecutionInitiated",
    "TimerStarted",
    "NexusOperationScheduled",
    "MarkerRecorded",
)
_TYPES = ("WorkflowExecutionStarted", *_TERMINAL, *_CLOSES, *_STARTS, *_COMMANDS)


def _upper_snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).upper()


#: Both spellings of each event type read (``EVENT_TYPE_TIMER_FIRED``, ``TimerFired``).
_EVENT_TYPES = {spelling: t for t in _TYPES for spelling in (t, f"EVENT_TYPE_{_upper_snake(t)}")}
#: Failure-info keys that say what a failure is.
_FAILURE_KINDS = {
    "applicationFailureInfo": "application",
    "timeoutFailureInfo": "timeout",
    "canceledFailureInfo": "canceled",
    "terminatedFailureInfo": "terminated",
    "serverFailureInfo": "server",
}
#: Failure-info keys that wrap another failure: the kind is their cause's.
_FAILURE_WRAPPERS = ("activityFailureInfo", "childWorkflowExecutionFailureInfo")

_INT = re.compile(r"[0-9]{1,19}")
_ENUM = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,63}")
_TIME = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt]([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.([0-9]{1,9}))?([Zz]|[+-][0-9]{2}:[0-9]{2})"
)
_DURATION = re.compile(r"([0-9]{1,12})(?:\.([0-9]{1,9}))?s")
_TRACEPARENT = re.compile(r"00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}")
_HEX32 = re.compile(r"[0-9a-f]{32}")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

_Slot = tuple[str, str]  # (kind, name)
_Signature = tuple[_Slot, ...]  # sorted


# --------------------------------------------------------------------------- reading JSON


def _map(value: object) -> Mapping[str, object] | None:
    return cast("Mapping[str, object]", value) if isinstance(value, Mapping) else None


def _seq(value: object) -> Sequence[object]:
    """``value`` as a JSON array; empty for anything else."""
    return cast("Sequence[object]", value) if isinstance(value, list | tuple) else ()


def _at(value: object, *path: str) -> object:
    """``value[path[0]][path[1]]...``, None where a step is no object or lacks the key."""
    for key in path:
        obj = _map(value)
        if obj is None:
            return None
        value = obj.get(key)
    return value


def _str(value: object) -> str | None:
    """A non-empty string Temporal could have written: proto strings are valid UTF-8, so
    one holding a lone surrogate (only hand-built JSON has one) cannot be read."""
    if not isinstance(value, str) or not value:
        return None
    if not value.isascii():
        try:
            value.encode()
        except UnicodeEncodeError:
            return None
    return value


def _int(value: object) -> int | None:
    """A non-negative integer, as proto3 JSON writes an int64 (a string) or as a number."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and _INT.fullmatch(value):
        return int(value)
    return None


def _enum(value: object, prefix: str) -> str | None:
    """A proto enum value's name, lower case, without its prefix (``TIMEOUT_TYPE_HEARTBEAT``
    and ``Heartbeat`` are ``heartbeat``); None for anything else, UNSPECIFIED included."""
    if not isinstance(value, str) or not _ENUM.fullmatch(value):
        return None
    if value.startswith(prefix):
        value = value[len(prefix) :]
    elif not value.isupper():
        value = _upper_snake(value)
    name = value.lower()
    return None if name == "unspecified" else name


def _nanos(value: object) -> int | None:
    """An RFC 3339 time (``eventTime``: three fractional digits from one server, nine from
    another) in nanoseconds since the epoch, None if it is not one. Parsed here rather than
    relying on ``fromisoformat``, which keeps microseconds at most."""
    if not isinstance(value, str):
        return None
    m = _TIME.fullmatch(value)
    if m is None:
        return None
    year, month, day, hour, minute, second, fraction, zone = m.groups()
    try:
        when = datetime(
            int(year), int(month), int(day), int(hour), int(minute), int(second), tzinfo=UTC
        )
    except ValueError:
        return None
    offset = 0
    if zone not in ("Z", "z"):
        hours, minutes = int(zone[1:3]), int(zone[4:6])
        if hours > 23 or minutes > 59:
            return None
        offset = (1 if zone[0] == "+" else -1) * (hours * 60 + minutes) * 60
    seconds = (when - _EPOCH) // timedelta(seconds=1) - offset
    return seconds * 10**9 + int((fraction or "").ljust(9, "0"))


def _time(value: object) -> str | None:
    """``value`` when it is an RFC 3339 time, else None: times are emitted as written."""
    return value if isinstance(value, str) and _nanos(value) is not None else None


def _duration(value: object) -> str | None:
    """A proto3 JSON duration (``"30s"``, ``"1.500s"``) as people write one: ``30s``,
    ``1.5s``, ``5m``, ``2h``, ``1h30m``."""
    m = _DURATION.fullmatch(value) if isinstance(value, str) else None
    if m is None:
        return None
    whole, fraction = int(m.group(1)), (m.group(2) or "").rstrip("0")
    hours, rest = divmod(whole, 3600)
    minutes, seconds = divmod(rest, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or fraction or not parts:
        parts.append(f"{seconds}.{fraction}s" if fraction else f"{seconds}s")
    return "".join(parts)


def _b64(value: object, limit: int = _PAYLOAD_MAX) -> bytes | None:
    if not isinstance(value, str) or len(value) > limit:
        return None
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return None


def _json_payload(value: object, limit: int = _PAYLOAD_MAX) -> object:
    """The JSON value a ``json/plain`` payload holds; None for any other payload."""
    if _b64(_at(value, "metadata", "encoding")) != b"json/plain":
        return None
    data = _b64(_at(value, "data"), limit)
    if data is None:
        return None
    try:
        return json.loads(data)
    except (ValueError, RecursionError):
        return None


def _trace(attrs: Mapping[str, object]) -> tuple[str | None, str | None]:
    """The trace and span ids of a command's ``_tracer-data`` header: a ``json/plain``
    payload of ``{"traceparent": "00-<trace>-<span>-<flags>", ...}``. Nothing else of it is
    read, and anything that is not exactly that shape is no trace."""
    carrier = _map(_json_payload(_at(attrs, "header", "fields", _TRACER_HEADER)))
    traceparent = carrier.get("traceparent") if carrier is not None else None
    m = _TRACEPARENT.fullmatch(traceparent) if isinstance(traceparent, str) else None
    if m is None or not m.group(1).strip("0") or not m.group(2).strip("0"):
        return None, None
    return m.group(1), m.group(2)


def _failure_info(
    kind: str, timeout_type: str | None = None, retry_state: object = None
) -> dict[str, Any]:
    return {
        "kind": kind,
        "timeout_type": timeout_type,
        "retry_state": _enum(retry_state, "RETRY_STATE_"),
    }


def _failure(value: object, retry_state: object = None) -> dict[str, Any] | None:
    """A failure reduced to its kind (seam ⑤: never its message, stack trace or encoded
    attributes): the failure-info it carries, or, through an activity or child failure,
    its cause's. ``retry_state`` is the event's own, and only that: a wrapper's
    ``retryState`` is the inner activity's or child's, and the time-skipping server writes
    none on a workflow's or a child's failure, so falling back to it would report another
    retry state on one server than on the other."""
    failure = _map(value)
    if failure is None:
        return None
    for _ in range(_CAUSE_DEPTH):
        for key, kind in _FAILURE_KINDS.items():
            info = _map(failure.get(key))
            if info is not None:
                timeout_type = _enum(info.get("timeoutType"), "TIMEOUT_TYPE_")
                return _failure_info(kind, timeout_type if kind == "timeout" else None, retry_state)
        wrapped = any(_map(failure.get(k)) is not None for k in _FAILURE_WRAPPERS)
        cause = _map(failure.get("cause"))
        if not wrapped or cause is None:
            break
        failure = cause
    return _failure_info("unknown", None, retry_state)


# --------------------------------------------------------------------------- events


@dataclass(frozen=True)
class _Event:
    id: int
    type: str  # one of _TYPES
    time: str | None
    attrs: Mapping[str, object]


def _raw_events(history: object) -> Sequence[object]:
    if isinstance(history, Mapping):
        found = _at(history, "events") if "events" in history else _at(history, "history", "events")
    elif isinstance(history, Sequence) and not isinstance(history, str | bytes | bytearray):
        found = history
    else:
        found = None
    if not isinstance(found, list | tuple):
        raise ValueError(
            "a history is {'events': [...]}, {'history': {'events': [...]}} or a list of "
            f"events; got {type(history).__name__}"
        )
    return cast("Sequence[object]", found)


def _events(history: object) -> list[_Event]:
    """The events this module reads, by id (the first of a repeated id), each with its
    ``<lowerCamelType>EventAttributes``. ``ValueError`` if ``history`` is not
    history-shaped; any event that cannot be read is left out."""
    by_id: dict[int, _Event] = {}
    for item in _raw_events(history):
        event = _map(item)
        if event is None:
            continue
        event_id = _int(event.get("eventId"))
        kind = _EVENT_TYPES.get(_str(event.get("eventType")) or "")
        if not event_id or kind is None or event_id in by_id:
            continue
        attrs = _map(event.get(f"{kind[0].lower()}{kind[1:]}EventAttributes"))
        by_id[event_id] = _Event(event_id, kind, _time(event.get("eventTime")), attrs or {})
    return [by_id[i] for i in sorted(by_id)]


# --------------------------------------------------------------------------- occurrences


def _ref(event_id: int, **known: str | None) -> dict[str, Any]:
    ref: dict[str, Any] = {
        "event_id": event_id,
        "activity_id": None,
        "task_queue": None,
        "trace_id": None,
        "span_id": None,
        "workflow_id": None,
        "run_id": None,
        "timer_id": None,
    }
    ref.update(known)
    return ref


@dataclass
class _Occurrence:
    """One command a workflow task made, and what became of it."""

    kind: str
    name: str
    label: str
    event: _Event
    ref: dict[str, Any]
    status: str
    attempt: int | None = None
    started: str | None = None
    closed: str | None = None
    start: _Event | None = None
    close: _Event | None = None
    failure: dict[str, Any] | None = None
    #: The run's terminal event, for one still open when the run closed (no close of its own).
    ended: _Event | None = None


def _task(event: _Event) -> tuple[str, str]:
    """The workflow task that commanded ``event``: its ``workflowTaskCompletedEventId`` value
    as written (the dev server's points at the WorkflowTaskCompleted event, the
    time-skipping server's at WorkflowTaskStarted; never looked up), or, without one, the
    event itself."""
    wtc = event.attrs.get("workflowTaskCompletedEventId")
    if isinstance(wtc, int) and not isinstance(wtc, bool):
        wtc = str(wtc)
    return ("wtc", wtc) if isinstance(wtc, str) else ("event", str(event.id))


def _command(event: _Event, batch_names: Mapping[str, str]) -> _Occurrence | None:
    """The occurrence a command event starts; None for one this module cannot name."""
    a = event.attrs
    if event.type == "ActivityTaskScheduled":
        name = _str(_at(a, "activityType", "name"))
        if name is None:
            return None
        trace_id, span_id = _trace(a)
        ref = _ref(
            event.id,
            activity_id=_str(a.get("activityId")),
            task_queue=_str(_at(a, "taskQueue", "name")),
            trace_id=trace_id,
            span_id=span_id,
        )
        return _Occurrence("activity", name, name, event, ref, "scheduled")
    if event.type == "StartChildWorkflowExecutionInitiated":
        name = _str(_at(a, "workflowType", "name"))
        workflow_id = _str(a.get("workflowId"))
        if name is None:
            return None
        label = name
        if name == _BATCH_WORKFLOW:
            # Named from the child's own history, never from its id (decisions I1).
            activities = batch_names.get(workflow_id) if workflow_id else None
            name = f"{name}[{activities}]" if activities else name
            label = f"{_BATCH_LABEL}({activities})" if activities else _BATCH_LABEL
        trace_id, span_id = _trace(a)
        ref = _ref(
            event.id,
            workflow_id=workflow_id,
            task_queue=_str(_at(a, "taskQueue", "name")),
            trace_id=trace_id,
            span_id=span_id,
        )
        return _Occurrence("child", name, label, event, ref, "scheduled")
    if event.type == "TimerStarted":
        duration = _duration(a.get("startToFireTimeout"))
        label = f"wait {duration}" if duration else "wait"
        ref = _ref(event.id, timer_id=_str(a.get("timerId")))
        return _Occurrence("timer", "wait", label, event, ref, "running", started=event.time)
    if event.type == "NexusOperationScheduled":
        service, operation = _str(a.get("service")), _str(a.get("operation"))
        if service is None or operation is None:
            return None
        name = f"{service}/{operation}"
        return _Occurrence("nexus", name, name, event, _ref(event.id), "scheduled")
    if event.type == "MarkerRecorded" and a.get("markerName") == _LOCAL_ACTIVITY:
        # sdk-core's own record of the call, not the activity's input or result.
        payloads = _seq(_at(a, "details", "data", "payloads"))
        record = (_map(_json_payload(payloads[0])) if payloads else None) or {}
        name = _str(record.get("activity_type")) or "local activity"
        failure = _failure(a.get("failure"))
        occurrence = _Occurrence(
            "local_activity",
            name,
            name,
            event,
            _ref(event.id),
            "completed" if "failure" not in a else "failed",
            attempt=_int(record.get("attempt")) or None,
            closed=event.time,
            close=event,
            failure=failure,
        )
        return occurrence
    return None


#: Where a close sits in event order: ``(event id, 0)``, or ``(marker id, 1)`` for a local
#: activity, which closes just after its own marker (before anything written after it).
_At = tuple[int, int]


@dataclass
class _Run:
    """What one run's history says: its occurrences in event order, the closes a later
    workflow task (or a later activation of the same one) can have observed, in event
    order, and the run's own first and terminal events."""

    occurrences: list[_Occurrence] = field(default_factory=list[_Occurrence])
    observed: list[tuple[_At, _Occurrence]] = field(default_factory=list[tuple[_At, _Occurrence]])
    started: _Event | None = None
    terminal: _Event | None = None


def _read(events: Sequence[_Event], batch_names: Mapping[str, str]) -> _Run:
    run = _Run()
    by_event: dict[int, _Occurrence] = {}
    for event in events:
        a = event.attrs
        if event.type == "WorkflowExecutionStarted":
            run.started = run.started or event
        elif event.type in _TERMINAL:
            run.terminal = run.terminal or event
        elif event.type in _STARTS:
            kind, key = _STARTS[event.type]
            occ = by_event.get(_int(a.get(key)) or 0)
            if occ is None or occ.kind != kind or occ.close is not None or occ.start is not None:
                continue
            occ.start, occ.started = event, event.time
            if kind == "activity":
                # Written when the activity closes, with its last attempt (Temporal writes
                # no event per retry).
                occ.attempt = _int(a.get("attempt")) or None
                occ.failure = _failure(a.get("lastFailure"))
            else:
                occ.status = "running"
                if kind == "child":
                    occ.ref["run_id"] = _str(_at(a, "workflowExecution", "runId"))
        elif event.type in _CLOSES:
            kind, key, status = _CLOSES[event.type]
            occ = by_event.get(_int(a.get(key)) or 0)
            if occ is None or occ.kind != kind or occ.close is not None or event.id <= occ.event.id:
                continue
            occ.close, occ.closed, occ.status = event, event.time, status
            if "failure" in a:
                occ.failure = _failure(a.get("failure"), a.get("retryState"))
            elif event.type == "StartChildWorkflowExecutionFailed":
                occ.failure = _failure_info("server")
            if kind == "child":
                # The run the close names is the last of the child's continue-as-new chain.
                occ.ref["run_id"] = _str(_at(a, "workflowExecution", "runId")) or occ.ref["run_id"]
            # A timer the workflow canceled is its own command, not something it waited on.
            if event.type != "TimerCanceled":
                run.observed.append(((event.id, 0), occ))
        elif event.type in _COMMANDS:
            occ = _command(event, batch_names)
            if occ is not None:
                run.occurrences.append(occ)
                by_event[event.id] = occ
                if occ.kind == "local_activity":
                    # It ran inside its workflow task: its marker is its command and its
                    # close at once, and what the task commanded after it waited on it.
                    run.observed.append(((event.id, 1), occ))
    return run


def _settle(run: _Run) -> None:
    """What a closed run says of the commands still open at its close: they ended with it
    (no event of their own says so). Temporal drops a closed run's activities, timers and
    Nexus operations (an activity heartbeating after gets NotFound, which the SDK turns
    into a cancel), so they are ``canceled``, or ``terminated`` / ``timed_out`` with a run
    the server ended so. A child follows its parent close policy: terminated (the default),
    canceled (request cancel), or still open (abandon: it may well be running)."""
    terminal = run.terminal
    if terminal is None:
        return
    ending = {
        "WorkflowExecutionTerminated": "terminated",
        "WorkflowExecutionTimedOut": "timed_out",
    }.get(terminal.type, "canceled")
    for occ in run.occurrences:
        if occ.status not in _OPEN or terminal.id <= occ.event.id:
            continue
        status = ending
        if occ.kind == "child":
            policy = _enum(occ.event.attrs.get("parentClosePolicy"), "PARENT_CLOSE_POLICY_")
            if policy == "abandon":
                continue
            status = "canceled" if policy == "request_cancel" else "terminated"
        occ.status, occ.closed, occ.ended = status, terminal.time, terminal


def _asked(events: Sequence[_Event]) -> set[str]:
    """What a ``kontra.batch.v1`` run was asked to run, from its input: the item activity
    and the ``pages=`` activity its spec names. Read only from a ``json/plain`` input (a
    mock run's, or any run no codec encoded): an encrypted one is never opened."""
    started = next((e for e in events if e.type == "WorkflowExecutionStarted"), None)
    if started is None or _str(_at(started.attrs, "workflowType", "name")) != _BATCH_WORKFLOW:
        return set()
    payloads = _seq(_at(started.attrs, "input", "payloads"))
    spec = _map(_json_payload(payloads[0], _SPEC_MAX)) if payloads else None
    return {name for key in _SPEC_NAMES if (name := _str((spec or {}).get(key))) is not None}


def _batch_names(children: object) -> dict[str, str]:
    """Each ``kontra.batch.v1`` child's activity names, sorted, unique, comma-joined: the
    ones its input asked for when that is readable (so a batch over no items, or whose
    first page was empty, has the name of every other run of the same call), else the
    ones its history scheduled."""
    if children is None:
        return {}
    given = _map(children)
    if given is None:
        raise ValueError(f"children maps a child workflow id to its history, got {children!r}")
    names: dict[str, str] = {}
    for workflow_id, history in given.items():
        events = _events(history)
        found = _asked(events) or {
            name
            for event in events
            if event.type == "ActivityTaskScheduled"
            and (name := _str(_at(event.attrs, "activityType", "name"))) is not None
        }
        if isinstance(workflow_id, str) and found:  # pyright: ignore[reportUnnecessaryIsInstance]
            names[workflow_id] = ",".join(sorted(found))
    return names


def _overlay(occurrences: Sequence[_Occurrence], pending: object) -> None:
    """What ``DescribeWorkflowExecution`` says of the activities still open (history has
    nothing of an attempt until it closes): running, the attempt, the last failure's kind.
    ``pending`` is its ``pendingActivities``, or the whole answer."""
    if pending is None:
        return
    entries = _at(pending, "pendingActivities") if _map(pending) is not None else pending
    if not isinstance(entries, list | tuple) and entries is not None:
        raise ValueError(
            f"pending is pendingActivities (or the DescribeWorkflowExecution answer), got {pending!r}"
        )
    open_activities = {
        o.ref["activity_id"]: o
        for o in occurrences
        if o.kind == "activity" and o.close is None and o.ref["activity_id"] is not None
    }
    for entry in _seq(entries):
        info = _map(entry)
        occ = open_activities.get(_str(info.get("activityId"))) if info is not None else None
        if info is None or occ is None:
            continue
        if _enum(info.get("state"), "PENDING_ACTIVITY_STATE_") in ("started", "cancel_requested"):
            occ.status = "running"
            occ.started = occ.started or _time(info.get("lastStartedTime"))
        attempt = _int(info.get("attempt"))
        if attempt:
            occ.attempt = max(attempt, occ.attempt or 0)
        failure = _failure(info.get("lastFailure"))
        if failure is not None:
            occ.failure = failure


# --------------------------------------------------------------------------- folding


def _fold(signatures: Sequence[_Signature]) -> tuple[list[_Signature], list[int]]:
    """The positions of the folded signature sequence (its leaves, in order) and each
    batch's position.

    Tandem repeats fold innermost first: a block of 1 to 16 signatures repeated at least
    twice in a row is one loop, and a run right after it equal to a proper prefix of the
    block (a last iteration cut short: ``check, wait, check``) is part of it. Period 1 is
    tried first, then 2, and so on, starting again from 1 after anything folds, until
    nothing does: nested loops fold, and a loop repeated is the same loop. Iterations share
    their block's positions. ``a, b, a`` does not fold."""
    symbols: dict[object, int] = {}
    leaves: list[list[_Signature]] = []  # by symbol: its positions, in order
    loops: set[int] = set()

    def intern(key: object, symbol_leaves: list[_Signature], loop: bool) -> int:
        if key not in symbols:
            symbols[key] = len(leaves)
            leaves.append(symbol_leaves)
            if loop:
                loops.add(symbols[key])
        return symbols[key]

    # An element: (symbol, [(batch, offset among the symbol's leaves)]).
    seq = [(intern(("leaf", s), [s], False), [(i, 0)]) for i, s in enumerate(signatures)]
    period = 1
    while period <= _PERIOD_MAX:
        out: list[tuple[int, list[tuple[int, int]]]] = []
        n, folded = len(seq), False
        symbols_now = [symbol for symbol, _ in seq]
        # A block repeats at i only if the symbol one period on is the same: those places
        # are found in one pass, and only they are looked at (most places repeat nothing,
        # and every fold starts the periods again).
        candidates = compress(
            range(n - 2 * period + 1), map(operator.eq, symbols_now, symbols_now[period:])
        )
        i = 0
        for start in candidates:
            if start < i:
                continue  # inside the loop just folded
            reps = 1
            while start + (reps + 1) * period <= n and all(
                symbols_now[start + reps * period + j] == symbols_now[start + j]
                for j in range(period)
            ):
                reps += 1
            if reps < 2:
                continue
            out.extend(seq[i:start])
            i = start
            block = symbols_now[i : i + period]
            end = i + reps * period
            while end - i - reps * period < period - 1 and end < n:
                if seq[end][0] != block[end - i - reps * period]:
                    break
                end += 1
            if period == 1 and block[0] in loops:
                symbol, offsets = block[0], [0]
            else:
                offsets = [0]
                for s in block[:-1]:
                    offsets.append(offsets[-1] + len(leaves[s]))
                body = [leaf for s in block for leaf in leaves[s]]
                symbol = intern(("loop", tuple(block)), body, True)
            cover = [
                (batch, offsets[(k - i) % period] + offset)
                for k in range(i, end)
                for batch, offset in seq[k][1]
            ]
            out.append((symbol, cover))
            i, folded = end, True
        out.extend(seq[i:])
        seq = out
        period = 1 if folded else period + 1

    positions: list[_Signature] = []
    where = [0] * len(signatures)
    for symbol, cover in seq:
        for batch, offset in cover:
            where[batch] = len(positions) + offset
        positions.extend(leaves[symbol])
    return positions, where


def _id_name(name: str) -> str:
    """``name`` as an id starts: ``%``, ``#`` and ``~`` percent-escaped, so that ``#k`` and
    ``~kind`` only ever come from the suffixes and two call sites never share an id across
    runs (an activity named ``a#2`` is not the second ``a``). Built, never parsed back."""
    return name.replace("%", "%25").replace("#", "%23").replace("~", "%7E")


def _ids(positions: Sequence[_Signature]) -> dict[tuple[int, _Slot], str]:
    """Each node's id: its name, then ``name#k`` at the k-th position holding the same
    ``(kind, name)``. A name two kinds share stays with the first of :data:`_KINDS` and is
    ``name~kind`` for the others, as is a name that is ``start`` or ``end``. The name part
    is escaped (:func:`_id_name`), so an id is one ``(kind, name, k)`` in every run."""
    seen: Counter[_Slot] = Counter()
    wanted: list[tuple[int, _Slot, str]] = []
    for position, signature in enumerate(positions):
        for slot in signature:
            seen[slot] += 1
            index = seen[slot]
            name = _id_name(slot[1])
            wanted.append((position, slot, name if index == 1 else f"{name}#{index}"))
    kinds: dict[str, set[str]] = {}
    for _, (kind, _), base in wanted:
        kinds.setdefault(base, set()).add(kind)
    ids: dict[tuple[int, _Slot], str] = {}
    taken = {START, END}
    for position, slot, base in wanted:
        kind = slot[0]
        first = min(kinds[base], key=_KINDS.index)
        candidate = base if kind == first and base not in taken else f"{base}~{kind}"
        unique, n = candidate, 1
        while unique in taken:
            n += 1
            unique = f"{candidate}{n}"
        taken.add(unique)
        ids[position, slot] = unique
    return ids


# --------------------------------------------------------------------------- parse


def _batches(run: _Run) -> tuple[list[list[_Occurrence]], int | None]:
    """The run's batches in event order, and the one its terminal event was commanded with
    (None: one of its own).

    A batch is what one activation of a workflow task commanded. Commands one task made
    share a ``workflowTaskCompletedEventId``; a local activity runs inside its task, and
    what the task commanded after its marker was commanded once it resolved (sdk-core
    writes ``x = await local(); await b(x)`` as the marker, then ``b``, and
    ``gather(local(), b())`` as ``b``, then the marker). So a task's commands split before
    each command that follows a marker. The terminal event joins its task's last batch
    unless a marker comes right before it: the run ended in the activation that made that
    batch (``workflow.start_activity(c); return x``), so it waited on nothing the batch
    started."""
    tasks: dict[tuple[str, str], list[_Occurrence]] = {}
    for occ in run.occurrences:
        tasks.setdefault(_task(occ.event), []).append(occ)
    terminal = run.terminal
    ends_in = _task(terminal) if terminal is not None and terminal.type in _COMMANDED else None
    batches: list[list[_Occurrence]] = []
    joining: list[_Occurrence] | None = None
    for task, commands in tasks.items():
        batch: list[_Occurrence] = []
        for occ in commands:
            if batch and batch[-1].kind == "local_activity" and occ.kind != "local_activity":
                batches.append(batch)
                batch = []
            batch.append(occ)
        batches.append(batch)
        last = commands[-1]
        if (
            task == ends_in
            and terminal is not None
            and terminal.id > last.event.id
            and last.kind != "local_activity"
        ):
            joining = batch
    batches.sort(key=lambda b: b[0].event.id)
    joined = next((k for k, b in enumerate(batches) if b is joining), None)
    return batches, joined


def parse(
    history: HistoryJSON,
    *,
    workflow_id: str | None = None,
    run_id: str | None = None,
    pending: Mapping[str, Any] | Sequence[Any] | None = None,
    children: Mapping[str, HistoryJSON] | None = None,
) -> Graph:
    """One run's graph from its history (see the module docstring for the rules).

    ``workflow_id`` and ``run_id`` name the run (the history names its workflow id only
    on some servers, and never its run id). ``pending`` is ``DescribeWorkflowExecution``'s
    ``pendingActivities`` (or its whole answer): history alone cannot tell an activity
    running, or its attempt, until it closes. ``children`` maps a child workflow id to its
    history (its first run is enough): a ``kontra.batch.v1`` child is named after the
    activities its input names when no codec encoded it, else the ones it scheduled.

    Total: ``ValueError`` only when ``history`` (or ``pending``, ``children``) is not
    history-shaped at the top level; whatever the events hold, what cannot be read is
    skipped. Deterministic, and the inputs are never changed."""
    run = _read(_events(history), _batch_names(children))
    _overlay(run.occurrences, pending)
    _settle(run)

    batches, joined = _batches(run)
    signatures = [tuple(sorted({(o.kind, o.name) for o in b})) for b in batches]
    positions, where = _fold(signatures)
    ids = _ids(positions)
    node_of = {
        o.event.id: ids[where[k], (o.kind, o.name)] for k, b in enumerate(batches) for o in b
    }

    # Causal edges: what each batch's activation observed closing since the previous batch.
    observed_at = [at for at, _ in run.observed]
    firsts = [b[0].event.id for b in batches]

    def before(k: int, first: int) -> set[str]:
        lo = (firsts[k - 1] if k else 0, 0)
        window = run.observed[
            bisect.bisect_right(observed_at, lo) : bisect.bisect_left(observed_at, (first, 0))
        ]
        if window:
            return {node_of[o.event.id] for _, o in window}
        return {node_of[o.event.id] for o in batches[k - 1]} if k else {START}

    # Each source and target once: a fan-out of thousands into a fan-out of thousands is a
    # handful of edges, never their product.
    edges: set[tuple[str, str]] = set()
    sources_of: list[set[str]] = []
    for k, batch in enumerate(batches):
        sources_of.append(before(k, firsts[k]))
        targets = {node_of[o.event.id] for o in batch}
        edges.update((s, t) for s in sources_of[k] for t in targets)
    terminal = run.terminal
    if terminal is not None and terminal.type in _COMMANDED:
        if joined is not None:
            # Commanded with that batch: it observed what the batch observed, and nothing
            # the batch itself started can have closed before it.
            ends = sources_of[joined]
        else:
            ends = before(bisect.bisect_left(firsts, terminal.id), terminal.id)
        edges.update((s, END) for s in ends)

    members: dict[str, list[tuple[int, _Occurrence]]] = {}
    for k, batch in enumerate(batches):
        for occ in batch:
            members.setdefault(node_of[occ.event.id], []).append((k, occ))
    nodes = [
        _node(ids[position, slot], slot, position, members[ids[position, slot]])
        for position, signature in enumerate(positions)
        for slot in signature
    ]
    return {
        "version": _VERSION,
        "workflow": _workflow(run, workflow_id, run_id),
        "nodes": nodes,
        "edges": _edge_list(edges),
    }


def _node(
    node_id: str, slot: _Slot, group: int, members: Sequence[tuple[int, _Occurrence]]
) -> dict[str, Any]:
    occurrences = [o for _, o in members]
    latest = max(occurrences, key=lambda o: o.event.id)
    first = min(occurrences, key=lambda o: o.event.id)
    last_batch = max(k for k, _ in members)
    status = min((o.status for k, o in members if k == last_batch), key=_PRECEDENCE.index)
    attempts = [o.attempt for o in occurrences if o.attempt is not None]
    ends = [(end.id, o) for o in occurrences if (end := o.close or o.ended) is not None]
    is_open = any(o.status in _OPEN for o in occurrences)
    last_close = max(ends, key=lambda pair: pair[0])[1] if ends else None
    return {
        "id": node_id,
        "name": slot[1],
        "kind": slot[0],
        "label": latest.label,
        "group": group,
        "status": status,
        "count": len(occurrences),
        "parallel": max(Counter(k for k, _ in members).values()),
        "attempt": max(attempts) if attempts else None,
        "counts": dict(sorted(Counter(o.status for o in occurrences).items())),
        "t": {"scheduled": latest.event.time, "started": latest.started, "closed": latest.closed},
        "span": {
            "from": first.event.time,
            "to": None if is_open or last_close is None else last_close.closed,
        },
        "ref": dict(latest.ref),
        "failure": dict(latest.failure) if latest.failure else None,
    }


def _workflow(run: _Run, workflow_id: str | None, run_id: str | None) -> dict[str, Any]:
    started: Mapping[str, object] = run.started.attrs if run.started else {}
    terminal = run.terminal
    a: Mapping[str, object] = terminal.attrs if terminal else {}
    failure = None
    if terminal is not None:
        if terminal.type == "WorkflowExecutionFailed":
            failure = _failure(a.get("failure"), a.get("retryState"))
        elif terminal.type == "WorkflowExecutionTimedOut":
            failure = _failure_info("timeout", None, a.get("retryState"))
        elif terminal.type in ("WorkflowExecutionCanceled", "WorkflowExecutionTerminated"):
            failure = _failure_info(_TERMINAL[terminal.type])
    return {
        "workflow_id": workflow_id or _str(started.get("workflowId")),
        "run_id": run_id,
        "type": _str(_at(started, "workflowType", "name")),
        "task_queue": _str(_at(started, "taskQueue", "name")),
        "status": _TERMINAL[terminal.type] if terminal else "running",
        "t": {
            "started": run.started.time if run.started else None,
            "closed": terminal.time if terminal else None,
        },
        "next_run_id": _str(a.get("newExecutionRunId")),
        "first_run_id": _str(started.get("firstExecutionRunId")),
        "trace_id": _trace(started)[0],
        "failure": failure,
    }


def _edge_list(edges: Iterable[tuple[str, str]]) -> list[dict[str, Any]]:
    return [{"from": a, "to": b} for a, b in sorted(set(edges))]


# --------------------------------------------------------------------------- union, merge


def _structure(node: Mapping[str, Any]) -> dict[str, Any]:
    """A node without anything one run says of it."""
    return {
        "id": node.get("id"),
        "name": node.get("name"),
        "kind": node.get("kind"),
        "label": node.get("label"),
        "group": node.get("group"),
        "status": None,
        "count": None,
        "parallel": None,
        "attempt": None,
        "counts": {},
        "t": None,
        "span": None,
        "ref": None,
        "failure": None,
    }


def union(graphs: Iterable[Graph]) -> Graph:
    """The structure several runs of one workflow share (the mock runs of each branch):
    every node by id, the first run's name, kind and label winning, and every edge.
    Nothing of any run stays: no status, counts, times, references or failures, and the
    workflow keeps its type and task queue only.

    A run's ``group`` is a position in that run, so two runs that took different branches
    at the same point number them alike. Here two nodes share a group only when some run
    commanded them in one batch (or they are chained by such runs): the groups are the
    sets of nodes ever scheduled together, numbered in node order. A fan-out stays one
    group; an ``if``'s two branches never are."""
    nodes: dict[str, dict[str, Any]] = {}
    together: dict[Any, Any] = {}  # union-find over node ids: what shared a batch

    def root(node_id: Any) -> Any:
        while together[node_id] != node_id:
            together[node_id] = together[together[node_id]]
            node_id = together[node_id]
        return node_id

    edges: set[tuple[str, str]] = set()
    workflow: dict[str, Any] = {"type": None, "task_queue": None}
    for graph in graphs:
        seen = graph.get("workflow") or {}
        for key in ("type", "task_queue"):
            workflow[key] = workflow[key] or seen.get(key)
        first_in: dict[Any, Any] = {}  # this run's group -> its first node
        for node in graph.get("nodes") or []:
            node_id = node.get("id")
            if node_id not in nodes:
                nodes[node_id] = _structure(node)
                together[node_id] = node_id
            group = node.get("group")
            if group is None:
                continue
            other = first_in.setdefault(group, node_id)
            together[root(node_id)] = root(other)
        edges.update((e["from"], e["to"]) for e in graph.get("edges") or [])
    numbers: dict[Any, int] = {}
    for node_id, node in nodes.items():
        node["group"] = numbers.setdefault(root(node_id), len(numbers))
    return {
        "version": _VERSION,
        "workflow": {
            "workflow_id": None,
            "run_id": None,
            "type": workflow["type"],
            "task_queue": workflow["task_queue"],
            "status": None,
            "t": None,
            "next_run_id": None,
            "first_run_id": None,
            "trace_id": None,
            "failure": None,
        },
        "nodes": list(nodes.values()),
        "edges": _edge_list(edges),
    }


def merge(static: Graph, live: Graph) -> Graph:
    """``live`` painted on ``static``: every node of both, a live one in place of the
    static one with ``reached: true``, a static-only one (a branch this run did not take,
    or not yet) with ``reached: false``; every edge of both, ``reached`` when the live run
    has it; the live run's workflow."""
    live_nodes = {n.get("id"): n for n in live.get("nodes") or []}
    nodes: list[dict[str, Any]] = []
    for node in static.get("nodes") or []:
        found = live_nodes.get(node.get("id"))
        nodes.append({**copy.deepcopy(found or node), "reached": found is not None})
    static_ids = {n.get("id") for n in static.get("nodes") or []}
    nodes += [
        {**copy.deepcopy(n), "reached": True} for i, n in live_nodes.items() if i not in static_ids
    ]
    live_edges = {(e["from"], e["to"]) for e in live.get("edges") or []}
    every = live_edges | {(e["from"], e["to"]) for e in static.get("edges") or []}
    return {
        "version": _VERSION,
        "workflow": copy.deepcopy(live.get("workflow")),
        "nodes": nodes,
        "edges": [{"from": a, "to": b, "reached": (a, b) in live_edges} for a, b in sorted(every)],
    }


# --------------------------------------------------------------------------- links


def _check_base(what: str, url: object) -> None:
    ok = (
        isinstance(url, str)
        and url.isascii()
        and url.isprintable()
        and not any(c in url for c in " ?#")
    )
    if ok:
        parts = urlsplit(cast("str", url))
        ok = parts.scheme in ("http", "https") and bool(parts.netloc)
    if not ok:
        raise ValueError(f"{what} must be an http(s) base URL without a query or fragment: {url!r}")


@dataclass(frozen=True)
class LinkConfig:
    """Where :func:`links` points. ``temporal_ui`` and ``grafana`` are base URLs (http or
    https, no query or fragment; None leaves their links out), ``namespace`` is the runs'
    Temporal namespace, ``loki_uid`` and ``tempo_uid`` are Grafana datasource uids, and
    ``log_selector`` is the LogQL stream selector a run's logs are found under."""

    namespace: str
    temporal_ui: str | None = None
    grafana: str | None = None
    loki_uid: str | None = None
    tempo_uid: str | None = None
    log_selector: str = '{service_name=~".+"}'

    def __post_init__(self) -> None:
        if _str(self.namespace) is None:
            raise ValueError(f"namespace is required, as a UTF-8 string: {self.namespace!r}")
        for what in ("temporal_ui", "grafana"):
            url = getattr(self, what)
            if url is not None:
                _check_base(what, url)


def _logql(value: str) -> str:
    """``value`` inside a LogQL double-quoted string."""
    out: list[str] = []
    for c in value:
        if c in '\\"':
            out.append("\\" + c)
        elif c == "\n":
            out.append("\\n")
        elif ord(c) < 0x20 or ord(c) == 0x7F:
            out.append(f"\\x{ord(c):02x}")
        else:
            out.append(c)
    return "".join(out)


def _range(start: object, end: object) -> dict[str, str] | None:
    """Explore's range for ``start``..``end`` (RFC 3339; ``end`` None while open): a
    second either side, as epoch-millisecond strings (Explore drops numbers), ``now`` for
    an open end."""
    begin = _nanos(start)
    if begin is None:
        return None
    finish = _nanos(end)
    return {
        "from": str(begin // 10**6 - 1000),
        "to": "now" if finish is None else str(-(-finish // 10**6) + 1000),
    }


def _explore(cfg: LinkConfig, uid: str, query: dict[str, Any], span: dict[str, str]) -> str:
    assert cfg.grafana is not None
    panes = {_PANE: {"datasource": uid, "queries": [query], "range": span}}
    encoded = quote(json.dumps(panes, separators=(",", ":")), safe="")
    return f"{cfg.grafana.rstrip('/')}/explore?schemaVersion=1&orgId=1&panes={encoded}"


def _ui(cfg: LinkConfig, *segments: str) -> str:
    assert cfg.temporal_ui is not None
    path = "/".join(
        quote(s, safe="") for s in ("namespaces", cfg.namespace, "workflows", *segments)
    )
    return f"{cfg.temporal_ui.rstrip('/')}/{path}"


def _observability(
    cfg: LinkConfig,
    workflow_id: str | None,
    run_id: str | None,
    activity_type: str | None,
    start: object,
    end: object,
    trace_id: object,
) -> dict[str, str]:
    """The ``logs`` and ``trace`` links of a run or a node, as far as they are known."""
    out: dict[str, str] = {}
    span = _range(start, end)
    if cfg.grafana is None or span is None:
        return out
    if cfg.loki_uid and workflow_id:
        expr = f'{cfg.log_selector} | workflow_id="{_logql(workflow_id)}"'
        if run_id:
            expr += f' | run_id="{_logql(run_id)}"'
        if activity_type:
            expr += f' | activity_type="{_logql(activity_type)}"'
        query = {
            "refId": "A",
            "datasource": {"type": "loki", "uid": cfg.loki_uid},
            "expr": expr,
            "queryType": "range",
        }
        out["logs"] = _explore(cfg, cfg.loki_uid, query, span)
    if cfg.tempo_uid and isinstance(trace_id, str) and _HEX32.fullmatch(trace_id):
        query = {
            "refId": "A",
            "datasource": {"type": "tempo", "uid": cfg.tempo_uid},
            "queryType": "traceql",
            "query": trace_id,
        }
        out["trace"] = _explore(cfg, cfg.tempo_uid, query, span)
    return out


def links(graph: Graph, cfg: LinkConfig) -> Graph:
    """``graph`` with ``links`` on its workflow and on each node: ``temporal`` (the run's
    history; a node's own event; a child's own run), ``logs`` (Grafana Explore on Loki,
    over the node's span) and ``trace`` (Tempo, by the command's trace id). A link is left
    out when its base URL or datasource is not configured or the graph lacks what it needs:
    a node only a static graph has gets none."""
    out = copy.deepcopy(graph)
    has_workflow = isinstance(out.get("workflow"), dict)
    workflow: dict[str, Any] = out["workflow"] if has_workflow else {}
    workflow_id, run_id = _str(workflow.get("workflow_id")), _str(workflow.get("run_id"))
    t = workflow.get("t") or {}
    found: dict[str, str] = {}
    if cfg.temporal_ui and workflow_id:
        found["temporal"] = (
            _ui(cfg, workflow_id, run_id, "history") if run_id else _ui(cfg, workflow_id)
        )
    found.update(
        _observability(
            cfg,
            workflow_id,
            run_id,
            None,
            t.get("started"),
            t.get("closed"),
            workflow.get("trace_id"),
        )
    )
    if has_workflow:
        workflow["links"] = found
    for node in out.get("nodes") or []:
        ref, span = node.get("ref") or {}, node.get("span") or {}
        kind = node.get("kind")
        found = {}
        if kind == "child":
            # The child's own run, and its logs: the child logs under its own id.
            child_id, child_run = _str(ref.get("workflow_id")), _str(ref.get("run_id"))
            if cfg.temporal_ui and child_id:
                if node.get("status") in _OPEN:
                    found["temporal"] = _ui(cfg, child_id)  # the UI follows the chain
                elif child_run:
                    found["temporal"] = _ui(cfg, child_id, child_run, "history")
            logs_for: tuple[str | None, str | None] = (child_id, None)
        else:
            event_id = _int(ref.get("event_id"))
            if cfg.temporal_ui and workflow_id and run_id and event_id:
                found["temporal"] = _ui(
                    cfg, workflow_id, run_id, "history", "events", str(event_id)
                )
            logs_for = (workflow_id, run_id)
        activity_type = _str(node.get("name")) if kind == "activity" else None
        found.update(
            _observability(
                cfg,
                *logs_for,
                activity_type,
                span.get("from"),
                span.get("to"),
                ref.get("trace_id"),
            )
        )
        node["links"] = found
    return out


# --------------------------------------------------------------------------- mermaid

_SHAPES = {
    "activity": ('["', '"]'),
    "local_activity": ('["', '"]'),
    "child": ('[["', '"]]'),
    "timer": ('(["', '"])'),
    "nexus": ('{{"', '"}}'),
}
_STYLES = {
    "completed": "fill:#e6f4ea,stroke:#1e8e3e",
    "running": "fill:#e8f0fe,stroke:#1a73e8",
    "scheduled": "fill:#f1f3f4,stroke:#5f6368",
    "failed": "fill:#fce8e6,stroke:#d93025",
    "timed_out": "fill:#fef7e0,stroke:#e37400",
    "canceled": "fill:#f1f3f4,stroke:#9aa0a6",
    "terminated": "fill:#fce8e6,stroke:#a50e0e",
    "local": "stroke-dasharray:4 3",
    "unreached": "opacity:0.45",
}
#: Characters Mermaid reads inside a quoted label, as its entity codes. ``%`` too: Mermaid
#: looks for ``%%{...}%%`` directives anywhere in the text, quoted labels included, and
#: applies them (theme, themeCSS, flowchart config) before it parses the chart.
_ENTITIES = {
    "#": "#35;",
    '"': "#quot;",
    "<": "#lt;",
    ">": "#gt;",
    "&": "#amp;",
    "`": "#96;",
    "%": "#37;",
}


def _mermaid_text(value: str) -> str:
    return "".join(_ENTITIES.get(c, c if c.isprintable() else " ") for c in value)


def mermaid(graph: Graph) -> str:
    """``graph`` as a Mermaid flowchart. Nodes are ``n0``, ``n1``... in graph order (a
    name is only ever a label); an activity is a box, a local activity a dashed one, a
    child a subroutine, a timer a stadium and a Nexus operation a hexagon; a label ends in
    ``×n`` for the attempt past the first and ``∥n`` for a fan-out. A class per status,
    ``unreached`` for a node the live run did not reach, and a dotted edge for one it did
    not take."""  # noqa: RUF002 -- the multiplication sign is the label's
    nodes = [n for n in graph.get("nodes") or [] if isinstance(n.get("id"), str)]
    names = {n["id"]: f"n{i}" for i, n in enumerate(nodes)}
    names.update({START: "s", END: "e"})
    edges = [e for e in graph.get("edges") or [] if e.get("from") in names and e.get("to") in names]
    ends = {e["from"] for e in edges} | {e["to"] for e in edges}
    lines = ["flowchart TD"]
    if START in ends:
        lines.append("    s((start))")
    classes: dict[str, list[str]] = {}
    for node in nodes:
        name = names[node["id"]]
        label = str(node.get("label") or node.get("name") or node["id"])
        attempt, parallel = node.get("attempt"), node.get("parallel")
        if isinstance(attempt, int) and attempt > 1:
            label += f" \u00d7{attempt}"  # MULTIPLICATION SIGN
        if isinstance(parallel, int) and parallel > 1:
            label += f" ∥{parallel}"
        left, right = _SHAPES.get(node.get("kind") or "", _SHAPES["activity"])
        lines.append(f"    {name}{left}{_mermaid_text(label)}{right}")
        marks = [node.get("status")]
        marks += ["local" if node.get("kind") == "local_activity" else None]
        marks += ["unreached" if node.get("reached") is False else None]
        for mark in marks:
            if mark in _STYLES:
                classes.setdefault(mark, []).append(name)
    if END in ends:
        lines.append("    e((end))")
    for e in edges:
        arrow = "-.->" if e.get("reached") is False else "-->"
        lines.append(f"    {names[e['from']]} {arrow} {names[e['to']]}")
    for mark, style in _STYLES.items():
        if mark in classes:
            lines.append(f"    classDef {mark} {style}")
            lines.append(f"    class {','.join(classes[mark])} {mark}")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- with_io


class _Decoder:
    """Payloads from history JSON, through a codec, to JSON-able values."""

    def __init__(self, codec: PayloadCodec) -> None:
        from google.protobuf import json_format
        from temporalio.api.common.v1 import Payload
        from temporalio.converter import PayloadConverter

        self._codec = codec
        self._parse = json_format.ParseDict
        self._payload = Payload
        self._converter = PayloadConverter.default

    def _value(self, payload: Payload) -> object:
        encoding = payload.metadata.get("encoding", b"")
        if encoding == b"binary/null":
            return None
        if encoding == b"json/plain":
            return self._converter.from_payloads([payload])[0]
        return {"encoding": encoding.decode("utf-8", "replace"), "bytes": len(payload.data)}

    async def _values(self, payloads: Sequence[object]) -> list[object]:
        parsed = [
            self._parse(dict(_map(p) or {}), self._payload(), ignore_unknown_fields=True)
            for p in payloads
        ]
        decoded = await self._codec.decode(parsed)
        if len(decoded) != len(parsed):
            # A result the codec dropped would read as null, a doubled input as two.
            raise ValueError(f"the codec gave {len(decoded)} payloads for {len(parsed)}")
        return [self._value(p) for p in decoded]

    async def payloads(self, value: object, *, single: bool) -> object:
        """``{"payloads": [...]}`` (or one payload, ``single``) decoded: the list of values,
        or ``{"error": "undecodable"}`` -- never what the codec could not open."""
        if value is None:
            return None
        try:
            return await self._values([value] if single else _seq(_at(value, "payloads")))
        except Exception:
            return {"error": "undecodable"}

    async def failure(self, value: object) -> object:
        """A failure's message and stack trace: from its encoded attributes when the
        failure converter moved them there (``message`` then reads ``Encoded failure``)."""
        failure = _map(value)
        if failure is None:
            return None
        if "encodedAttributes" in failure:
            try:
                [attributes] = await self._values([failure["encodedAttributes"]])
            except Exception:
                return {"error": "undecodable"}
            failure = _map(attributes) or {}
            return {"message": failure.get("message"), "stack_trace": failure.get("stack_trace")}
        return {"message": failure.get("message"), "stack_trace": failure.get("stackTrace")}

    async def occurrence(self, occ: _Occurrence) -> dict[str, Any]:
        command = occ.event.attrs
        close = occ.close.attrs if occ.close else {}
        if occ.kind == "local_activity":
            result = await self.payloads(_at(command, "details", "result"), single=False)
            return {
                "input": None,
                "result": _first(result),
                "failure": await self.failure(command.get("failure")),
            }
        single = occ.kind == "nexus"
        failure = close.get("failure")
        if failure is None and occ.start is not None:
            failure = occ.start.attrs.get("lastFailure")
        completed = occ.close is not None and occ.status == "completed"
        return {
            "input": await self.payloads(command.get("input"), single=single),
            "result": _first(await self.payloads(close.get("result"), single=single))
            if completed
            else None,
            "failure": await self.failure(failure),
        }

    async def workflow(self, run: _Run) -> dict[str, Any]:
        started = run.started.attrs if run.started else {}
        closed = run.terminal.attrs if run.terminal else {}
        return {
            "input": await self.payloads(started.get("input"), single=False),
            "result": _first(await self.payloads(closed.get("result"), single=False)),
            "failure": await self.failure(closed.get("failure")),
        }


def _first(values: object) -> object:
    """A result: the one value its payloads hold (an error stays the error)."""
    if isinstance(values, list):
        return cast("list[object]", values)[0] if values else None
    return values


async def with_io(graph: Graph, history: HistoryJSON, codec: PayloadCodec) -> Graph:
    """``graph`` (parsed from ``history``) with ``io`` -- ``{"input", "result", "failure"}``
    -- on its workflow and on each node's latest occurrence, decoded through ``codec``.

    A separate step on purpose (seam ⑤): :func:`parse` emits no payload, encrypted or
    not, so only a caller holding the codec sees one. Values come out as Temporal's
    default converter reads them untyped: JSON for ``json/plain``, None for
    ``binary/null``, ``{"encoding", "bytes"}`` for any other encoding; a failure is
    ``{"message", "stack_trace"}``. A field the codec cannot decode is
    ``{"error": "undecodable"}``, never its bytes."""
    if codec is None:  # pyright: ignore[reportUnnecessaryComparison]
        raise TypeError("with_io decodes payloads through a codec: pass one (parse emits none)")
    decoder = _Decoder(codec)
    run = _read(_events(history), {})
    by_event = {occ.event.id: occ for occ in run.occurrences}
    out = copy.deepcopy(graph)
    for node in out.get("nodes") or []:
        occ = by_event.get(_int(_at(node.get("ref"), "event_id")) or 0)
        node["io"] = await decoder.occurrence(occ) if occ else dict(_NO_IO)
    if isinstance(out.get("workflow"), dict):
        out["workflow"]["io"] = await decoder.workflow(run)
    return out
