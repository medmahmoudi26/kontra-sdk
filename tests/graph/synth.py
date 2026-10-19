"""Synthesized Temporal histories for the graph tests (``tests/graph/fixtures``).

Each fixture is the proto3 JSON ``WorkflowHistory.to_json_dict()`` writes: camelCase keys,
int64 values as strings, ``EVENT_TYPE_*`` names, base64 payloads, and a workflow task
(scheduled, started, completed) around every batch of commands. :class:`History` writes a
run the way one of two servers does: the dev server (``server="dev"``: commands carry the
WorkflowTaskCompleted id, nine fractional digits, the workflow id, an activity's last
failure, a ``taskId`` on every event and a workflow failure's own retry state) or the
time-skipping test server (``"test"``: the WorkflowTaskStarted id, three digits, none of
the others, and no Started event for an activity attempt that timed out). Both shapes were
read off real histories of both servers; nothing here is a copy of one, and a test parses
every fixture strictly into Temporal's own protos.

``python -m tests.graph.synth`` rewrites the fixtures; ``test_graph`` checks the files are
what this module builds.
"""

from __future__ import annotations

import base64
import json
import re
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from temporalio.api.common.v1 import Payload

FIXTURES_DIR = Path(__file__).parent / "fixtures"

#: Planted in every payload of the leak fixture, in plaintext (seam ⑤).
MARKER = "KONTRA-PLAINTEXT-MARKER-7f3a"
#: What the test codec prefixes its ciphertext with (anything else does not decode).
SEALED = b"KX1"
_KEY = 0x5A

NAMESPACE = "acme"
TASK_QUEUE = "acme/enrich@1.0.0"
WORKFLOW_ID = "job-5f2c2bee7e154e03a4f4ebde4bdb09d1"
RUN_ID = "0199f0a1-5c3e-7d2a-9b41-6e0c2f8d4a10"
#: A batch child's id, as ``batch_workflow_id`` builds it: parent / batch / activity / token.
BATCH_ID = f"{WORKFLOW_ID}/batch/enrich/2f0b6c1e-8d4a-4b7e-9c3f-5a1d0e7b9c22"
BATCH_FIRST_RUN = "0199f0a1-6a10-7c4e-8f22-1b3d5e7f9a01"
BATCH_LAST_RUN = "0199f0a1-7b21-7d5f-9a33-2c4e6f8a0b12"
TRACE_ID = "4a765c1fe964fbaae8408c8d3f2b7dc1"
SPAN_ID = "93bf4b64a4854a27"
CLIENT_TRACE_ID = "8db43cc56d2200c5e719a5b0ea81b74a"
CHILD_TRACE_ID = "1a917cfe468786574bde169b5e009a99"
_START = datetime(2026, 10, 18, 9, 0, 0, tzinfo=UTC)


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def payload(value: Any, encoding: str = "json/plain") -> dict[str, Any]:
    """A payload as history JSON holds it (``json/plain`` data compact, as Temporal's
    JSON converter writes it)."""
    data = b"" if value is None else json.dumps(value, separators=(",", ":")).encode()
    return {"metadata": {"encoding": _b64(encoding.encode())}, "data": _b64(data)}


def payloads(*values: Any) -> dict[str, Any]:
    return {"payloads": [payload(v) for v in values]}


def seal(inner: Payload) -> bytes:
    """The test codec's ciphertext of ``inner``: a prefix and the payload XOR-ed."""
    return SEALED + bytes(b ^ _KEY for b in inner.SerializeToString(deterministic=True))


def open_sealed(data: bytes) -> Payload:
    if not data.startswith(SEALED):
        raise ValueError("not sealed by the test codec")
    return Payload.FromString(bytes(b ^ _KEY for b in data[len(SEALED) :]))


def sealed(value: Any) -> tuple[dict[str, Any], bytes]:
    """A ``binary/encrypted`` payload holding ``value``, and its ciphertext."""
    inner = Payload(
        metadata={"encoding": b"json/plain"}, data=json.dumps(value, separators=(",", ":")).encode()
    )
    ciphertext = seal(inner)
    return {
        "metadata": {"encoding": _b64(b"binary/encrypted")},
        "data": _b64(ciphertext),
    }, ciphertext


def traceparent(trace_id: str, span_id: str, flags: str = "03", **extra: str) -> dict[str, Any]:
    """A ``_tracer-data`` header as ``TracingInterceptor`` writes it."""
    return {
        "fields": {
            "_tracer-data": payload({"traceparent": f"00-{trace_id}-{span_id}-{flags}", **extra})
        }
    }


def failure(
    message: str = "Encoded failure",
    *,
    info: str = "applicationFailureInfo",
    info_value: dict[str, Any] | None = None,
    encoded: dict[str, Any] | None = None,
    cause: dict[str, Any] | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    """A failure as kontra's converter leaves it: the message is ``Encoded failure`` and
    the real one is in ``encodedAttributes``. The Python SDK writes no ``source``; the dev
    server writes ``Server`` on the failures it makes."""
    out: dict[str, Any] = {"message": message}
    if source is not None:
        out["source"] = source
    if encoded is not None:
        out["encodedAttributes"] = encoded
    if cause is not None:
        out["cause"] = cause
    out[info] = info_value if info_value is not None else {"type": "RuntimeError"}
    return out


def _camel(kind: str) -> str:
    return f"{kind[0].lower()}{kind[1:]}EventAttributes"


def _snake(kind: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", kind).upper()


class History:
    """One run's events, written as ``server`` writes them; every method returns the id
    of the event it wrote."""

    def __init__(
        self,
        workflow_type: str = "EnrichFlow",
        *,
        server: str = "dev",
        workflow_id: str = WORKFLOW_ID,
        run_id: str = RUN_ID,
        input: Sequence[Any] = ({"batch": 3},),
        header: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        assert server in ("dev", "test")
        self.server = server
        self.events: list[dict[str, Any]] = []
        self._ns = int(_START.timestamp()) * 10**9 + 123_456_789
        self._identity = "4242@worker-a"
        attrs: dict[str, Any] = {
            "workflowType": {"name": workflow_type},
            "taskQueue": {"name": TASK_QUEUE, "kind": "TASK_QUEUE_KIND_NORMAL"},
            "input": payloads(*input),
            "workflowTaskTimeout": "10s",
            "originalExecutionRunId": run_id,
            "identity": self._identity,
            "firstExecutionRunId": run_id,
            "attempt": 1,
            "firstWorkflowTaskBackoff": "0s",
        }
        if header is not None:
            attrs["header"] = header
        if server == "dev":
            attrs["workflowId"] = workflow_id
        attrs.update(extra or {})
        self.add("WorkflowExecutionStarted", attrs)
        self._activities = 0
        self._timers = 0

    # ----------------------------------------------------------------- plumbing

    def _stamp(self) -> str:
        seconds, nanos = divmod(self._ns, 10**9)
        whole = datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
        if self.server == "dev":
            return f"{whole}.{nanos:09d}Z"
        return f"{whole}.{nanos // 10**6:03d}Z"

    def wait(self, seconds: float) -> None:
        self._ns += int(seconds * 10**9)

    def add(self, kind: str, attrs: dict[str, Any], **event: Any) -> int:
        """An event; the dev server writes a ``taskId`` on each, the test server none."""
        self._ns += 1_234_567
        event_id = len(self.events) + 1
        task_id = {"taskId": str(1_048_576 + event_id)} if self.server == "dev" else {}
        self.events.append(
            {
                "eventId": str(event_id),
                "eventTime": self._stamp(),
                "eventType": f"EVENT_TYPE_{_snake(kind)}",
                **task_id,
                **event,
                _camel(kind): attrs,
            }
        )
        return event_id

    def json(self) -> dict[str, Any]:
        return {"events": self.events}

    # ----------------------------------------------------------------- workflow tasks

    def task(self) -> str:
        """A workflow task: the ``workflowTaskCompletedEventId`` its commands carry."""
        scheduled = self.add(
            "WorkflowTaskScheduled",
            {
                "taskQueue": {"name": TASK_QUEUE, "kind": "TASK_QUEUE_KIND_NORMAL"},
                "startToCloseTimeout": "10s",
                "attempt": 1,
            },
        )
        started_attrs: dict[str, Any] = {
            "scheduledEventId": str(scheduled),
            "identity": self._identity,
            "historySizeBytes": str(400 * len(self.events)),
        }
        if self.server == "dev":
            started_attrs["requestId"] = f"8f1c2d3e-0000-4000-8000-{len(self.events):012d}"
        started = self.add("WorkflowTaskStarted", started_attrs)
        completed_attrs: dict[str, Any] = {"scheduledEventId": str(scheduled)}
        if self.server == "dev":
            completed_attrs["startedEventId"] = str(started)
        completed = self.add(
            "WorkflowTaskCompleted",
            {
                **completed_attrs,
                "identity": self._identity,
                "sdkMetadata": {"sdkName": "temporal-python", "sdkVersion": "1.34.0"},
                "meteringMetadata": {},
            },
        )
        return str(completed if self.server == "dev" else started)

    # ----------------------------------------------------------------- activities

    def activity(
        self,
        wtc: str,
        name: str,
        *args: Any,
        header: dict[str, Any] | None = None,
        input: dict[str, Any] | None = None,
        **event: Any,
    ) -> int:
        self._activities += 1
        attrs: dict[str, Any] = {
            "activityId": str(self._activities),
            "activityType": {"name": name},
            "taskQueue": {"name": TASK_QUEUE, "kind": "TASK_QUEUE_KIND_NORMAL"},
            "input": input if input is not None else payloads(*(args or (name,))),
            "scheduleToCloseTimeout": "0s",
            "scheduleToStartTimeout": "0s",
            "startToCloseTimeout": "30s",
            "heartbeatTimeout": "0s",
            "workflowTaskCompletedEventId": wtc,
            "retryPolicy": {
                "initialInterval": "1s",
                "backoffCoefficient": 2,
                "maximumInterval": "100s",
            },
        }
        if header is not None:
            attrs["header"] = header
        return self.add("ActivityTaskScheduled", attrs, **event)

    def started(self, scheduled: int, *, attempt: int = 1, last_failure: Any = None) -> int:
        attrs: dict[str, Any] = {
            "scheduledEventId": str(scheduled),
            "identity": self._identity,
            "attempt": attempt,
        }
        if self.server == "dev":
            attrs["requestId"] = f"2bd6a337-0000-4000-8000-{scheduled:012d}"
            if last_failure is not None:
                attrs["lastFailure"] = last_failure
        return self.add("ActivityTaskStarted", attrs)

    def completed(self, scheduled: int, started: int, result: dict[str, Any] | None = None) -> int:
        return self.add(
            "ActivityTaskCompleted",
            {
                "result": result if result is not None else payloads("ok"),
                "scheduledEventId": str(scheduled),
                "startedEventId": str(started),
                "identity": self._identity,
            },
        )

    def finish(self, scheduled: int, *, seconds: float = 0.05, **started: Any) -> int:
        """The activity scheduled at ``scheduled`` runs and completes."""
        self.wait(seconds)
        return self.completed(scheduled, self.started(scheduled, **started))

    def failed(
        self,
        scheduled: int,
        started: int,
        how: dict[str, Any],
        retry_state: str,
        *,
        kind: str = "Failed",
    ) -> int:
        """An activity's failure or timeout (``ActivityTaskTimedOut`` has no identity)."""
        attrs: dict[str, Any] = {
            "failure": how,
            "scheduledEventId": str(scheduled),
            "startedEventId": str(started),
        }
        if kind != "TimedOut":
            attrs["identity"] = self._identity
        return self.add(f"ActivityTask{kind}", {**attrs, "retryState": retry_state})

    def timed_out(
        self,
        scheduled: int,
        timeout_type: str = "START_TO_CLOSE",
        retry_state: str = "RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED",
        *,
        attempt: int = 1,
    ) -> int:
        """The server times the activity out. The dev server writes the attempt's Started
        event and a failure with a message and ``source: Server``; the test server writes no
        Started event (``startedEventId: -1``) and a bare ``timeoutFailureInfo``."""
        info = {"timeoutFailureInfo": {"timeoutType": f"TIMEOUT_TYPE_{timeout_type}"}}
        if self.server == "dev":
            started = self.started(scheduled, attempt=attempt)
            words = timeout_type.title().replace("_", "")
            how = {"message": f"activity {words} timeout", "source": "Server", **info}
            return self.failed(scheduled, started, how, retry_state, kind="TimedOut")
        return self.add(
            "ActivityTaskTimedOut",
            {
                "failure": info,
                "scheduledEventId": str(scheduled),
                "startedEventId": "-1",
                "retryState": retry_state,
            },
        )

    def local(self, wtc: str, activity_type: str, *, attempt: int = 1, result: Any = "ok") -> int:
        """A local activity's marker (sdk-core's record in ``data``, the result beside it)."""
        self._activities += 1
        record = {
            "seq": self._activities,
            "attempt": attempt,
            "activity_id": str(self._activities),
            "activity_type": activity_type,
            "complete_time": {"seconds": 1792314000, "nanos": 1000},
            "backoff": None,
            "original_schedule_time": {"seconds": 1792314000, "nanos": 0},
        }
        return self.add(
            "MarkerRecorded",
            {
                "markerName": "core_local_activity",
                "details": {"result": payloads(result), "data": {"payloads": [payload(record)]}},
                "workflowTaskCompletedEventId": wtc,
            },
        )

    # ----------------------------------------------------------------- timers

    def timer(self, wtc: str, duration: str = "30s") -> int:
        self._timers += 1
        return self.add(
            "TimerStarted",
            {
                "timerId": str(self._timers),
                "startToFireTimeout": duration,
                "workflowTaskCompletedEventId": wtc,
            },
        )

    def fired(self, timer: int, seconds: float = 30) -> int:
        self.wait(seconds)
        timer_id = self.events[timer - 1]["timerStartedEventAttributes"]["timerId"]
        return self.add("TimerFired", {"timerId": timer_id, "startedEventId": str(timer)})

    # ----------------------------------------------------------------- children

    def child(
        self,
        wtc: str,
        workflow_type: str,
        workflow_id: str,
        *,
        header: dict[str, Any] | None = None,
        input: dict[str, Any] | None = None,
        **extra: Any,
    ) -> int:
        attrs: dict[str, Any] = {
            "namespace": NAMESPACE,
            "workflowId": workflow_id,
            "workflowType": {"name": workflow_type},
            "taskQueue": {"name": TASK_QUEUE, "kind": "TASK_QUEUE_KIND_NORMAL"},
            "input": input if input is not None else payloads({"items": 3}),
            "workflowRunTimeout": "0s",
            "workflowTaskTimeout": "10s",
            "parentClosePolicy": "PARENT_CLOSE_POLICY_TERMINATE",
            "workflowTaskCompletedEventId": wtc,
            "workflowIdReusePolicy": "WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE",
            **extra,
        }
        if header is not None:
            attrs["header"] = header
        return self.add("StartChildWorkflowExecutionInitiated", attrs)

    def _execution(self, initiated: int, run_id: str) -> dict[str, Any]:
        attrs = self.events[initiated - 1]["startChildWorkflowExecutionInitiatedEventAttributes"]
        return {
            "namespace": NAMESPACE,
            "workflowExecution": {"workflowId": attrs["workflowId"], "runId": run_id},
            "workflowType": attrs["workflowType"],
            "initiatedEventId": str(initiated),
            "namespaceId": "0199f000-0000-7000-8000-00000000acme",
        }

    def child_started(self, initiated: int, run_id: str) -> int:
        return self.add(
            "ChildWorkflowExecutionStarted", {**self._execution(initiated, run_id), "header": {}}
        )

    def child_closed(
        self, initiated: int, started: int, run_id: str, kind: str = "Completed", **attrs: Any
    ) -> int:
        """The child's close: its run is the last of its continue-as-new chain."""
        if kind == "Completed":
            attrs.setdefault("result", payloads({"enriched": 3}))
        return self.add(
            f"ChildWorkflowExecution{kind}",
            {**attrs, **self._execution(initiated, run_id), "startedEventId": str(started)},
        )

    def timer_canceled(self, timer: int, wtc: str) -> int:
        """The workflow cancels the timer started at ``timer`` (its own command)."""
        timer_id = self.events[timer - 1]["timerStartedEventAttributes"]["timerId"]
        attrs = {
            "timerId": timer_id,
            "startedEventId": str(timer),
            "workflowTaskCompletedEventId": wtc,
        }
        if self.server == "dev":
            attrs["identity"] = self._identity
        return self.add("TimerCanceled", attrs)

    # ----------------------------------------------------------------- the end

    def complete(self, wtc: str, result: Any = "done") -> int:
        return self.add(
            "WorkflowExecutionCompleted",
            {"result": payloads(result), "workflowTaskCompletedEventId": wtc},
        )

    def fail(self, wtc: str, how: dict[str, Any]) -> int:
        """The workflow fails. The dev server writes the run's own retry state on the event
        (``RETRY_POLICY_NOT_SET`` for a run without a policy); the test server writes none."""
        attrs: dict[str, Any] = {"failure": how}
        if self.server == "dev":
            attrs["retryState"] = "RETRY_STATE_RETRY_POLICY_NOT_SET"
        return self.add("WorkflowExecutionFailed", {**attrs, "workflowTaskCompletedEventId": wtc})


def _then(h: History, name: str, **finish: Any) -> int:
    """``await name()`` in a workflow task of its own."""
    scheduled = h.activity(h.task(), name)
    h.finish(scheduled, **finish)
    return scheduled


# --------------------------------------------------------------------------- fixtures


def sequence(server: str = "dev") -> dict[str, Any]:
    """``await fetch_batch(); await enrich(); await write_records()``."""
    h = History(server=server)
    for name in ("fetch_batch", "enrich", "write_records"):
        _then(h, name)
    h.complete(h.task())
    return h.json()


def fan_out() -> dict[str, Any]:
    """Three lookups in one gather, then five of one activity in another."""
    h = History(server="test")
    wtc = h.task()
    lookups = [h.activity(wtc, n) for n in ("lookup_company", "lookup_domain", "lookup_person")]
    for scheduled in lookups:
        h.finish(scheduled)
    wtc = h.task()
    scores = [h.activity(wtc, "score", i) for i in range(5)]
    for scheduled in scores:
        h.finish(scheduled)
    h.complete(h.task())
    return h.json()


def gather_join() -> dict[str, Any]:
    """``gather(a(), b(), c()); await d()``: the join waits on all three."""
    h = History(server="dev")
    wtc = h.task()
    started = [h.activity(wtc, n) for n in ("a", "b", "c")]
    h.finish(started[0])
    h.task()  # woke on a; still waiting on b and c: no command
    for scheduled in started[1:]:
        h.finish(scheduled)
    _then(h, "d")
    h.complete(h.task())
    return h.json()


def retry() -> dict[str, Any]:
    """``fetch_batch`` completes on its third attempt (one Started event, written then)."""
    h = History(server="dev")
    last = failure(
        encoded=payload({"message": "rate limited (attempt 2)", "stack_trace": "  File ..."}),
        info_value={"type": "RateLimited"},
    )
    _then(h, "fetch_batch", attempt=3, last_failure=last, seconds=3.0)
    _then(h, "write_records")
    h.complete(h.task())
    return h.json()


def branch(decision: str, server: str = "dev") -> dict[str, Any]:
    """``check``, then ``approve`` or ``reject`` on its result, then ``ship``."""
    h = History(server=server)
    for name in ("check", decision, "ship"):
        _then(h, name)
    h.complete(h.task())
    return h.json()


def child() -> dict[str, Any]:
    """``fetch_batch``, ``execute_activities_in_parallel(enrich, pages=fetch_page)`` (the
    ``kontra.batch.v1`` child, which continued as new: its Started names its first run, its
    Completed its last), then a seller's own child workflow ``Settle``."""
    h = History(server="dev")
    _then(h, "fetch_batch")
    initiated = h.child(h.task(), "kontra.batch.v1", BATCH_ID)
    started = h.child_started(initiated, BATCH_FIRST_RUN)
    h.task()
    h.wait(4)
    h.child_closed(initiated, started, BATCH_LAST_RUN)
    initiated = h.child(h.task(), "Settle", f"{WORKFLOW_ID}/settle")
    started = h.child_started(initiated, "0199f0a1-8c32-7e60-8b44-3d5f7a9b1c23")
    h.task()
    h.child_closed(initiated, started, "0199f0a1-8c32-7e60-8b44-3d5f7a9b1c23")
    h.complete(h.task())
    return h.json()


def child_batch() -> dict[str, Any]:
    """The batch child's first run: a page, three items, the next page, continue-as-new."""
    h = History(
        "kontra.batch.v1", server="dev", workflow_id=BATCH_ID, run_id=BATCH_FIRST_RUN, input=()
    )
    _then(h, "fetch_page")
    wtc = h.task()
    for scheduled in [h.activity(wtc, "enrich", i) for i in range(3)]:
        h.finish(scheduled)
    _then(h, "fetch_page")
    wtc = h.task()
    h.add(
        "WorkflowExecutionContinuedAsNew",
        {
            "newExecutionRunId": "0199f0a1-6f00-7000-8000-000000000002",
            "workflowType": {"name": "kontra.batch.v1"},
            "taskQueue": {"name": TASK_QUEUE, "kind": "TASK_QUEUE_KIND_NORMAL"},
            "input": payloads({"cursor": 2}),
            "workflowRunTimeout": "0s",
            "workflowTaskTimeout": "10s",
            "workflowTaskCompletedEventId": wtc,
        },
    )
    return h.json()


def traced() -> dict[str, Any]:
    """Started by a traced client (the run's own header), then: ``greet`` with a valid
    ``_tracer-data`` (flags ``03``); five malformed ones in one gather; a child with one;
    a timer, which never has one."""
    h = History(server="dev", header=traceparent(CLIENT_TRACE_ID, "69e2bdcb2b3ab1f4"))
    wtc = h.task()
    greet = h.activity(wtc, "greet", header=traceparent(TRACE_ID, SPAN_ID))
    h.finish(greet)
    bad: dict[str, dict[str, Any]] = {
        "version_01": {
            "fields": {"_tracer-data": payload({"traceparent": f"01-{TRACE_ID}-{SPAN_ID}-01"})}
        },
        "upper_hex": traceparent(TRACE_ID.upper(), SPAN_ID),
        "zero_trace": traceparent("0" * 32, SPAN_ID),
        "binary": {
            "fields": {
                "_tracer-data": payload(
                    {"traceparent": f"00-{TRACE_ID}-{SPAN_ID}-03"}, "binary/plain"
                )
            }
        },
        "trailing": {
            "fields": {"_tracer-data": payload({"traceparent": f"00-{TRACE_ID}-{SPAN_ID}-03\n"})}
        },
    }
    wtc = h.task()
    for scheduled in [h.activity(wtc, name, header=header) for name, header in bad.items()]:
        h.finish(scheduled)
    initiated = h.child(
        h.task(),
        "Notify",
        f"{WORKFLOW_ID}/notify",
        header=traceparent(CHILD_TRACE_ID, "6926ece2461d492a"),
    )
    started = h.child_started(initiated, "0199f0a1-9d43-7f71-9c55-4e6f8a0b2d34")
    h.task()
    h.child_closed(initiated, started, "0199f0a1-9d43-7f71-9c55-4e6f8a0b2d34")
    h.fired(h.timer(h.task(), "5s"), 5)
    h.complete(h.task())
    return h.json()


def continue_as_new() -> dict[str, Any]:
    """One page fetched, then the run continues as new."""
    h = History(server="dev")
    _then(h, "fetch_page")
    h.add(
        "WorkflowExecutionContinuedAsNew",
        {
            "newExecutionRunId": "0199f0a1-a054-7082-8d66-5f7a9b1c3e45",
            "workflowType": {"name": "EnrichFlow"},
            "taskQueue": {"name": TASK_QUEUE, "kind": "TASK_QUEUE_KIND_NORMAL"},
            "input": payloads({"cursor": 1}),
            "workflowRunTimeout": "0s",
            "workflowTaskTimeout": "10s",
            "workflowTaskCompletedEventId": h.task(),
        },
    )
    return h.json()


def running() -> dict[str, Any]:
    """Still running: ``fetch_batch`` done, ``enrich`` scheduled (history has nothing of
    an attempt until it closes)."""
    h = History(server="dev")
    _then(h, "fetch_batch")
    h.activity(h.task(), "enrich")
    return h.json()


def pending() -> dict[str, Any]:
    """Running, with ``enrich`` (retrying, attempt 3 started) and ``audit`` (in backoff)
    open: what the pending overlay paints."""
    h = History(server="dev")
    _then(h, "fetch_batch")
    wtc = h.task()
    h.activity(wtc, "enrich")
    h.activity(wtc, "audit")
    return h.json()


def pending_describe() -> dict[str, Any]:
    """``DescribeWorkflowExecution`` of :func:`pending` (``pendingActivities``)."""
    return {
        "workflowExecutionInfo": {
            "execution": {"workflowId": WORKFLOW_ID, "runId": RUN_ID},
            "type": {"name": "EnrichFlow"},
            "status": "WORKFLOW_EXECUTION_STATUS_RUNNING",
        },
        "pendingActivities": [
            {
                "activityId": "2",
                "activityType": {"name": "enrich"},
                "state": "PENDING_ACTIVITY_STATE_STARTED",
                "attempt": 3,
                "maximumAttempts": 5,
                "scheduledTime": "2026-10-18T09:00:00.150000000Z",
                "lastStartedTime": "2026-10-18T09:00:07.500000000Z",
                "lastFailure": failure(encoded=payload({"message": "upstream 503"})),
                "lastWorkerIdentity": "4242@worker-a",
                "lastAttemptCompleteTime": "2026-10-18T09:00:05.250000000Z",
            },
            {
                "activityId": "3",
                "activityType": {"name": "audit"},
                "state": "PENDING_ACTIVITY_STATE_SCHEDULED",
                "attempt": 2,
                # In backoff, the next attempt's: neither the first schedule nor the failure.
                "scheduledTime": "2026-10-18T09:01:00.150000000Z",
                "lastFailure": failure(
                    info="timeoutFailureInfo",
                    info_value={"timeoutType": "TIMEOUT_TYPE_START_TO_CLOSE"},
                ),
                "lastAttemptCompleteTime": "2026-10-18T09:00:30.150000000Z",
                "nextAttemptScheduleTime": "2026-10-18T09:01:00.150000000Z",
            },
            {
                "activityId": "99",
                "activityType": {"name": "stranger"},
                "state": "PENDING_ACTIVITY_STATE_STARTED",
            },
        ],
    }


def loop() -> dict[str, Any]:
    """``for x in xs: await a(x)`` over four items."""
    h = History(server="test")
    for _ in range(4):
        _then(h, "a")
    h.complete(h.task())
    return h.json()


def loop_body() -> dict[str, Any]:
    """``for x in xs: await a(x); await b(x)`` over three items."""
    h = History(server="dev")
    for _ in range(3):
        _then(h, "a")
        _then(h, "b")
    h.complete(h.task())
    return h.json()


def two_call_sites() -> dict[str, Any]:
    """``await a(); await b(); await a()``: two call sites of ``a``."""
    h = History(server="dev")
    for name in ("a", "b", "a"):
        _then(h, name)
    h.complete(h.task())
    return h.json()


def polling() -> dict[str, Any]:
    """``while not await check(): await sleep(30)``: three checks, two waits."""
    h = History(server="test")
    _then(h, "check")
    for _ in range(2):
        h.fired(h.timer(h.task(), "30s"))
        _then(h, "check")
    h.complete(h.task())
    return h.json()


def gather_chains() -> dict[str, Any]:
    """``gather(chain(x), chain(y))``, each chain ``await a(); await b()``: both ``a`` in
    one task, then each ``b`` in a task of its own as its ``a`` completes."""
    h = History(server="dev")
    wtc = h.task()
    ax, ay = h.activity(wtc, "a", "x"), h.activity(wtc, "a", "y")
    h.finish(ax)
    bx = h.activity(h.task(), "b", "x")
    h.finish(ay)
    by = h.activity(h.task(), "b", "y")
    h.finish(bx)
    h.finish(by)
    h.complete(h.task())
    return h.json()


def local_activity() -> dict[str, Any]:
    """``fetch_batch``, then a local activity (its second attempt) and the result in the
    same task; a ``core_patch`` marker is no node."""
    h = History(server="dev")
    _then(h, "fetch_batch")
    wtc = h.task()
    h.add(
        "MarkerRecorded",
        {
            "markerName": "core_patch",
            "details": {"patch_data": payloads({"id": "v2-normalise", "deprecated": False})},
            "workflowTaskCompletedEventId": wtc,
        },
    )
    h.local(wtc, "normalise", attempt=2)
    h.complete(wtc)
    return h.json()


def nexus() -> dict[str, Any]:
    """A Nexus operation (an endpoint is an opaque name), started and completed."""
    h = History(server="dev")
    scheduled = h.add(
        "NexusOperationScheduled",
        {
            "endpoint": "k3f9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e",
            "service": "acme.enrich",
            "operation": "enrich",
            "input": payload({"items": 3}),
            "scheduleToCloseTimeout": "0s",
            "workflowTaskCompletedEventId": h.task(),
            "requestId": "5d1c7e2a-0000-4000-8000-000000000001",
            "endpointId": "0199f000-0000-7000-8000-0000000000e1",
        },
    )
    h.add(
        "NexusOperationStarted",
        {"scheduledEventId": str(scheduled), "operationToken": "dG9rZW4"},
    )
    h.task()
    h.wait(2)
    h.add(
        "NexusOperationCompleted",
        {"scheduledEventId": str(scheduled), "result": payload({"enriched": 3})},
    )
    h.complete(h.task())
    return h.json()


def tail_forget() -> dict[str, Any]:
    """``x = await a(); workflow.start_activity(c); return x``: the last task schedules
    ``c`` and completes the run in one go, so the run never waited on ``c`` (the test
    server's shape)."""
    h = History(server="test")
    _then(h, "a")
    wtc = h.task()
    h.activity(wtc, "c")
    h.complete(wtc)
    return h.json()


def local_sequence() -> dict[str, Any]:
    """``x = await local_one(); await b(x)``: one task, the marker first, then ``b`` (sdk-core
    commands ``b`` once the local activity resolved)."""
    h = History(server="dev")
    wtc = h.task()
    h.local(wtc, "local_one")
    h.finish(h.activity(wtc, "b"))
    h.complete(h.task())
    return h.json()


def local_gather() -> dict[str, Any]:
    """``await a(); gather(local_one(), b())``: one task, ``b`` first, then the marker (``b``
    was commanded before the local activity resolved), and the run waits on both."""
    h = History(server="dev")
    _then(h, "a")
    wtc = h.task()
    b = h.activity(wtc, "b")
    h.local(wtc, "local_one")
    h.finish(b)
    h.complete(h.task())
    return h.json()


def timer_race() -> dict[str, Any]:
    """``wait([sleep(3600), a()], FIRST_COMPLETED)``, the timer canceled, then ``b``: the
    task that saw ``a`` close schedules ``b`` and cancels the timer, in that order (the
    test server's shape)."""
    h = History(server="test")
    wtc = h.task()
    timer = h.timer(wtc, "3600s")
    h.finish(h.activity(wtc, "a"))
    wtc = h.task()
    b = h.activity(wtc, "b")
    h.timer_canceled(timer, wtc)
    h.finish(b)
    h.complete(h.task())
    return h.json()


def canceled() -> dict[str, Any]:
    """``a``, then ``watch`` (heartbeating forever) until the run is canceled: the workflow
    requests ``watch``'s cancel and closes as canceled in the same task, before ``watch``
    is ever canceled."""
    h = History(server="dev")
    _then(h, "a")
    watch = h.activity(h.task(), "watch")
    h.wait(5)
    h.add("WorkflowExecutionCancelRequested", {"identity": "admin@cli"})
    wtc = h.task()
    h.add(
        "ActivityTaskCancelRequested",
        {"scheduledEventId": str(watch), "workflowTaskCompletedEventId": wtc},
    )
    h.add("WorkflowExecutionCanceled", {"workflowTaskCompletedEventId": wtc})
    return h.json()


def failed(server: str = "test") -> dict[str, Any]:
    """``fetch_batch`` times out for good, and the workflow fails with it. The test server
    writes no Started event for the attempt that timed out, and no retry state on the
    workflow's failure (its activity failure's ``retryState`` is the activity's)."""
    h = History(server=server)
    scheduled = h.activity(h.task(), "fetch_batch")
    h.wait(30)
    h.timed_out(scheduled, retry_state="RETRY_STATE_TIMEOUT", attempt=5)
    timeout = {"timeoutFailureInfo": {"timeoutType": "TIMEOUT_TYPE_START_TO_CLOSE"}}
    if server == "dev":
        timeout = {"message": "activity StartToClose timeout", "source": "Server", **timeout}
    wrapped = failure(
        "Activity task timed out",
        info="activityFailureInfo",
        info_value={
            "scheduledEventId": str(scheduled),
            "activityType": {"name": "fetch_batch"},
            "activityId": "1",
            "retryState": "RETRY_STATE_TIMEOUT",
        },
        cause=timeout,
    )
    h.fail(h.task(), wrapped)
    return h.json()


def terminated() -> dict[str, Any]:
    """``fetch_batch`` done, ``enrich`` scheduled, and the run terminated by an operator."""
    h = History(server="dev")
    _then(h, "fetch_batch")
    h.activity(h.task(), "enrich")
    h.add("WorkflowExecutionTerminated", {"reason": "operator", "identity": "admin@cli"})
    return h.json()


def leak() -> tuple[dict[str, Any], bytes]:
    """Every payload a history holds carries :data:`MARKER` in plaintext, and one result is
    sealed by the test codec (returned: its ciphertext). Nothing of either may leave
    ``parse``."""
    m = MARKER
    secret, ciphertext = sealed({"secret": f"sealed {m}"})
    h = History(
        server="dev",
        input=({"note": m},),
        header={
            "fields": {
                "kontra-job": payload({"job": m}),
                "_tracer-data": payload(
                    {
                        "traceparent": f"00-{CLIENT_TRACE_ID}-69e2bdcb2b3ab1f4-03",
                        "baggage": f"note={m}",
                    }
                ),
            }
        },
        extra={
            "memo": {"fields": {"note": payload(m)}},
            "searchAttributes": {"indexedFields": {"CustomKeywordField": payload(m)}},
            "lastCompletionResult": payloads(m),
            "continuedFailure": failure(m),
        },
    )
    summary = {"summary": payload(m), "details": payload(m)}
    encoded = payload({"message": m, "stack_trace": f'  File "{m}.py"'})
    plain_failure = failure(
        m,
        encoded=encoded,
        info_value={"type": "RuntimeError", "details": payloads(m)},
        cause=failure(m, encoded=encoded),
    )
    plain_failure["stackTrace"] = f'  File "{m}.py", line 1'

    # An activity whose last failure, input and summary carry it, and whose result is sealed.
    wtc = h.task()
    scheduled = h.activity(
        wtc,
        "fetch_batch",
        input=payloads({"q": m}),
        header={
            "fields": {
                "_tracer-data": payload(
                    {"traceparent": f"00-{TRACE_ID}-{SPAN_ID}-03", "baggage": m}
                ),
                "other": payload(m),
            }
        },
        userMetadata=summary,
    )
    started = h.started(scheduled, attempt=2, last_failure=plain_failure)
    h.completed(scheduled, started, result={"payloads": [secret]})
    # One that fails, one that times out with heartbeat details, one canceled with details.
    wtc = h.task()
    fails = h.activity(wtc, "enrich", input=payloads(m))
    beats = h.activity(wtc, "watch", input=payloads(m))
    cancels = h.activity(wtc, "audit", input=payloads(m))
    h.failed(fails, h.started(fails), plain_failure, "RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED")
    heartbeat = failure(
        m,
        source="Server",
        info="timeoutFailureInfo",
        info_value={"timeoutType": "TIMEOUT_TYPE_HEARTBEAT", "lastHeartbeatDetails": payloads(m)},
    )
    h.failed(beats, h.started(beats), heartbeat, "RETRY_STATE_TIMEOUT", kind="TimedOut")
    h.add(
        "ActivityTaskCancelRequested",
        {"scheduledEventId": str(cancels), "workflowTaskCompletedEventId": h.task()},
    )
    h.add(
        "ActivityTaskCanceled",
        {
            "details": payloads(m),
            "scheduledEventId": str(cancels),
            "startedEventId": str(h.started(cancels)),
        },
    )
    # A child (input, memo, its failure), a signal, a local activity's result, a patch.
    initiated = h.child(
        h.task(),
        "Notify",
        f"{WORKFLOW_ID}/notify",
        input=payloads(m),
        memo={"fields": {"note": payload(m)}},
    )
    started = h.child_started(initiated, "0199f0a1-9d43-7f71-9c55-4e6f8a0b2d34")
    h.add(
        "WorkflowExecutionSignaled",
        {"signalName": "nudge", "input": payloads(m), "identity": "cli"},
    )
    h.task()
    h.child_closed(
        initiated,
        started,
        "0199f0a1-9d43-7f71-9c55-4e6f8a0b2d34",
        "Failed",
        failure=plain_failure,
        retryState="RETRY_STATE_NON_RETRYABLE_FAILURE",
    )
    wtc = h.task()
    h.add(
        "MarkerRecorded",
        {
            "markerName": "core_patch",
            "details": {"patch_data": payloads(m)},
            "workflowTaskCompletedEventId": wtc,
        },
    )
    h.local(wtc, "normalise", result=m)
    # A Nexus operation's input and result, then an operator ends it all.
    nexus_op = h.add(
        "NexusOperationScheduled",
        {
            "endpoint": "k3f9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e",
            "service": "acme.enrich",
            "operation": "enrich",
            "input": payload(m),
            "workflowTaskCompletedEventId": wtc,
        },
    )
    # One still running when it ends (pending: leak_pending).
    h.activity(wtc, "slow", input=payloads(m))
    h.task()
    h.add("NexusOperationCompleted", {"scheduledEventId": str(nexus_op), "result": payload(m)})
    h.add(
        "WorkflowExecutionTerminated",
        {"reason": m, "details": payloads(m), "identity": "admin@cli"},
    )
    return h.json(), ciphertext


def leak_pending() -> dict[str, Any]:
    """``DescribeWorkflowExecution`` of :func:`leak` before the operator ended it: ``slow``
    is running its third attempt, and its heartbeat details and last failure carry
    :data:`MARKER` (what the pending overlay reads, and must not pass on)."""
    m = MARKER
    last = failure(
        m,
        encoded=payload({"message": m, "stack_trace": f'  File "{m}.py"'}),
        info_value={"type": "RuntimeError", "details": payloads(m)},
    )
    last["stackTrace"] = f'  File "{m}.py", line 1'
    return {
        "executionConfig": {"taskQueue": {"name": TASK_QUEUE}},
        "workflowExecutionInfo": {
            "execution": {"workflowId": WORKFLOW_ID, "runId": RUN_ID},
            "type": {"name": "EnrichFlow"},
            "memo": {"fields": {"note": payload(m)}},
            "status": "WORKFLOW_EXECUTION_STATUS_RUNNING",
        },
        "pendingActivities": [
            {
                "activityId": "6",
                "activityType": {"name": "slow"},
                "state": "PENDING_ACTIVITY_STATE_STARTED",
                "heartbeatDetails": payloads(m),
                "lastHeartbeatTime": "2026-10-18T09:01:00.000000000Z",
                "lastStartedTime": "2026-10-18T09:00:59.000000000Z",
                "attempt": 3,
                "lastFailure": last,
                "lastWorkerIdentity": "4242@worker-a",
            }
        ],
    }


def undecodable() -> dict[str, Any]:
    """An activity whose input the test codec cannot open (``binary/encrypted``, not its
    ciphertext) and whose result it can."""
    h = History(server="dev")
    broken = {"metadata": {"encoding": _b64(b"binary/encrypted")}, "data": _b64(b"not sealed")}
    secret, _ = sealed({"rows": 3})
    scheduled = h.activity(h.task(), "fetch_batch", input={"payloads": [broken]})
    h.completed(scheduled, h.started(scheduled), result={"payloads": [secret]})
    h.complete(h.task())
    return h.json()


FIXTURES: dict[str, Callable[[], dict[str, Any]]] = {
    "sequence": sequence,
    "sequence_test_server": lambda: sequence("test"),
    "fan_out": fan_out,
    "gather_join": gather_join,
    "retry": retry,
    "branch_approve": lambda: branch("approve"),
    "branch_reject": lambda: branch("reject", "test"),
    "child": child,
    "child_batch": child_batch,
    "traced": traced,
    "continue_as_new": continue_as_new,
    "running": running,
    "pending": pending,
    "pending_describe": pending_describe,
    "loop": loop,
    "loop_body": loop_body,
    "two_call_sites": two_call_sites,
    "polling": polling,
    "gather_chains": gather_chains,
    "local_activity": local_activity,
    "nexus": nexus,
    "failed": failed,
    "terminated": terminated,
    "canceled": canceled,
    "tail_forget": tail_forget,
    "local_sequence": local_sequence,
    "local_gather": local_gather,
    "timer_race": timer_race,
    "leak": lambda: leak()[0],
    "undecodable": undecodable,
}


def dumps(value: Any) -> str:
    return json.dumps(value, indent=1) + "\n"


def main() -> None:
    FIXTURES_DIR.mkdir(exist_ok=True)
    for name, build in FIXTURES.items():
        (FIXTURES_DIR / f"{name}.json").write_text(dumps(build()), encoding="utf-8")


if __name__ == "__main__":
    main()
