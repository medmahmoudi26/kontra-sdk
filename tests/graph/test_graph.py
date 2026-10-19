"""kontra.graph: a run's history read back as nodes, edges and status (decisions I26).

Every history here is synthesized (``synth.py``, checked in under ``fixtures/``) in the
proto3 JSON shape both servers write, the dev server's and the time-skipping test
server's. No Temporal server runs."""

from __future__ import annotations

import base64
import copy
import itertools
import json
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

import pytest
from google.protobuf import json_format
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from temporalio.api.common.v1 import Payload
from temporalio.api.history.v1 import History
from temporalio.api.workflowservice.v1 import DescribeWorkflowExecutionResponse
from temporalio.converter import PayloadCodec

import kontra.graph
from kontra.graph import END, START, LinkConfig, links, merge, mermaid, parse, union, with_io

from . import synth
from .synth import (
    BATCH_FIRST_RUN,
    BATCH_ID,
    BATCH_LAST_RUN,
    CHILD_TRACE_ID,
    CLIENT_TRACE_ID,
    MARKER,
    RUN_ID,
    SPAN_ID,
    TRACE_ID,
    WORKFLOW_ID,
)

HISTORIES = [name for name in synth.FIXTURES if name != "pending_describe"]
CFG = LinkConfig(
    namespace="acme",
    temporal_ui="http://localhost:8233",
    grafana="http://localhost:3000/",
    loki_uid="loki-uid",
    tempo_uid="tempo-uid",
)
UI = "http://localhost:8233/namespaces/acme/workflows"
EXPLORE = "http://localhost:3000/explore?schemaVersion=1&orgId=1&panes="


def fixture(name: str) -> dict[str, Any]:
    return json.loads((synth.FIXTURES_DIR / f"{name}.json").read_text(encoding="utf-8"))


def graph(name: str, **kwargs: Any) -> dict[str, Any]:
    return parse(fixture(name), **kwargs)


def edges(g: dict[str, Any]) -> set[tuple[str, str]]:
    return {(e["from"], e["to"]) for e in g["edges"]}


def node(g: dict[str, Any], node_id: str) -> dict[str, Any]:
    [found] = [n for n in g["nodes"] if n["id"] == node_id]
    return found


def ids(g: dict[str, Any]) -> list[str]:
    return [n["id"] for n in g["nodes"]]


def chain(*names: str) -> set[tuple[str, str]]:
    path = [START, *names, END]
    return set(itertools.pairwise(path))


def ms(time: str, *, up: bool = False) -> int:
    """Epoch milliseconds of an RFC 3339 time with nine fractional digits, rounded down
    (or up)."""
    whole = datetime.fromisoformat(time[:23] + "+00:00").replace(tzinfo=UTC)
    floor = round(whole.timestamp() * 1000)
    return floor + (up and time[23:29] != "000000")


def panes(url: str) -> dict[str, Any]:
    assert url.startswith(EXPLORE)
    [encoded] = parse_qs(urlsplit(url).query)["panes"]
    return json.loads(encoded)


class SealCodec(PayloadCodec):
    """Seals every payload (``binary/encrypted``); opens what it sealed and passes the
    rest, and refuses a ``binary/encrypted`` payload it did not seal."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(metadata={"encoding": b"binary/encrypted"}, data=synth.seal(p))
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            synth.open_sealed(p.data) if p.metadata.get("encoding") == b"binary/encrypted" else p
            for p in payloads
        ]


# --------------------------------------------------------------------------- fixtures


def test_the_fixtures_are_what_synth_builds() -> None:
    on_disk = sorted(p.stem for p in synth.FIXTURES_DIR.glob("*.json"))
    assert on_disk == sorted(synth.FIXTURES)
    for name, build in synth.FIXTURES.items():
        path = synth.FIXTURES_DIR / f"{name}.json"
        assert path.read_text(encoding="utf-8") == synth.dumps(build()), (
            f"{path} is stale: run `python -m tests.graph.synth`"
        )


def test_the_fixtures_write_both_servers_workflow_task_ids() -> None:
    """The dev server's commands point at the WorkflowTaskCompleted event, the
    time-skipping server's at WorkflowTaskStarted (one before it). The test server writes
    no ``taskId`` and no WorkflowTaskCompleted ``startedEventId``."""
    for name, expected in (
        ("sequence", "WORKFLOW_TASK_COMPLETED"),
        ("loop", "WORKFLOW_TASK_STARTED"),
    ):
        events = fixture(name)["events"]
        scheduled = next(
            e for e in events if e["eventType"] == "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"
        )
        wtc = int(scheduled["activityTaskScheduledEventAttributes"]["workflowTaskCompletedEventId"])
        assert events[wtc - 1]["eventType"] == f"EVENT_TYPE_{expected}"
        completed = events[wtc - 1 if expected.endswith("COMPLETED") else wtc]
        has_started = "startedEventId" in completed["workflowTaskCompletedEventAttributes"]
        assert has_started == ("taskId" in events[0]) == (name == "sequence")


def test_every_fixture_is_what_temporals_own_protos_read() -> None:
    """Parsed strictly (an unknown field fails) into Temporal's ``History`` and
    ``DescribeWorkflowExecutionResponse``: a synthesized event cannot carry a field the
    real one does not have (``ActivityTaskTimedOut`` has no ``identity``)."""
    for name in HISTORIES:
        json_format.ParseDict(fixture(name), History(), ignore_unknown_fields=False)
    for describe in (fixture("pending_describe"), synth.leak_pending()):
        json_format.ParseDict(
            describe, DescribeWorkflowExecutionResponse(), ignore_unknown_fields=False
        )
    timed_out = fixture("failed")
    event = next(e for e in timed_out["events"] if "activityTaskTimedOutEventAttributes" in e)
    event["activityTaskTimedOutEventAttributes"]["identity"] = "4242@worker-a"
    with pytest.raises(json_format.ParseError, match="identity"):
        json_format.ParseDict(timed_out, History(), ignore_unknown_fields=False)


# --------------------------------------------------------------------------- #41 Done when


def test_done_when_sequence() -> None:
    g = graph("sequence")
    assert ids(g) == ["fetch_batch", "enrich", "write_records"]
    assert edges(g) == chain("fetch_batch", "enrich", "write_records")
    assert [n["group"] for n in g["nodes"]] == [0, 1, 2]
    assert g["workflow"] == {
        "workflow_id": WORKFLOW_ID,
        "run_id": None,
        "type": "EnrichFlow",
        "task_queue": "acme/enrich@1.0.0",
        "status": "completed",
        "t": {
            "started": fixture("sequence")["events"][0]["eventTime"],
            "closed": fixture("sequence")["events"][-1]["eventTime"],
        },
        "next_run_id": None,
        "first_run_id": RUN_ID,
        "trace_id": None,
        "failure": None,
    }


def test_done_when_fan_out() -> None:
    """Three different activities in one gather are one group; five of one are one node."""
    g = graph("fan_out")
    lookups = ["lookup_company", "lookup_domain", "lookup_person"]
    assert ids(g) == [*lookups, "score"]
    assert {node(g, n)["group"] for n in lookups} == {0}
    assert {(START, n) for n in lookups} | {(n, "score") for n in lookups} | {
        ("score", END)
    } == edges(g)
    score = node(g, "score")
    assert (score["count"], score["parallel"], score["counts"]) == (5, 5, {"completed": 5})


def test_done_when_retry_attempt_count() -> None:
    """Temporal writes no event per retry: the attempt is the Started event's, written
    when the activity closed, and the failure that caused the retry is its kind only."""
    g = graph("retry")
    fetch = node(g, "fetch_batch")
    assert fetch["attempt"] == 3
    assert fetch["status"] == "completed"
    assert fetch["failure"] == {"kind": "application", "timeout_type": None, "retry_state": None}
    assert node(g, "write_records")["attempt"] == 1


def test_done_when_two_branches_unioned() -> None:
    static = union([graph("branch_approve"), graph("branch_reject")])
    assert ids(static) == ["check", "approve", "ship", "reject"]
    assert edges(static) == chain("check", "approve", "ship") | chain("check", "reject", "ship")
    assert {a for a, b in edges(static) if b == "ship"} == {"approve", "reject"}


def test_done_when_unreached_marking() -> None:
    static = union([graph("branch_approve"), graph("branch_reject")])
    live = graph("branch_approve", run_id=RUN_ID)
    painted = merge(static, live)
    reached = {n["id"]: n["reached"] for n in painted["nodes"]}
    assert reached == {"check": True, "approve": True, "ship": True, "reject": False}
    assert node(painted, "reject")["status"] is None
    assert node(painted, "approve") == {**node(live, "approve"), "reached": True}
    untaken = {(e["from"], e["to"]) for e in painted["edges"] if not e["reached"]}
    assert untaken == {("check", "reject"), ("reject", "ship")}
    assert painted["workflow"] == live["workflow"]


def test_done_when_child_run_id() -> None:
    """A child's Started event names the first run of its continue-as-new chain, its close
    the last: a closed child refers to the last."""
    g = graph("child", children={BATCH_ID: fixture("child_batch")})
    batch = node(g, "kontra.batch.v1[enrich,fetch_page]")
    assert batch["kind"] == "child"
    assert batch["label"] == "execute_activities_in_parallel(enrich,fetch_page)"
    assert batch["ref"]["workflow_id"] == BATCH_ID
    assert batch["ref"]["run_id"] == BATCH_LAST_RUN != BATCH_FIRST_RUN
    assert batch["status"] == "completed"
    settle = node(g, "Settle")
    assert (settle["name"], settle["label"]) == ("Settle", "Settle")
    # Cut before the child closes: running, on the first run.
    events = fixture("child")["events"]
    cut = next(
        i
        for i, e in enumerate(events)
        if e["eventType"].endswith("CHILD_WORKFLOW_EXECUTION_COMPLETED")
    )
    open_child = node(parse(events[:cut]), "kontra.batch.v1")
    assert (open_child["status"], open_child["ref"]["run_id"]) == ("running", BATCH_FIRST_RUN)
    assert open_child["span"]["to"] is None


def test_done_when_tracer_data_extraction() -> None:
    """``_tracer-data`` is read exactly: a valid traceparent with flags ``03`` gives the
    trace and span ids; anything else gives none."""
    g = graph("traced")
    greet = node(g, "greet")
    assert (greet["ref"]["trace_id"], greet["ref"]["span_id"]) == (TRACE_ID, SPAN_ID)
    for bad in ("version_01", "upper_hex", "zero_trace", "binary", "trailing"):
        assert (node(g, bad)["ref"]["trace_id"], node(g, bad)["ref"]["span_id"]) == (None, None), (
            bad
        )
    assert node(g, "Notify")["ref"]["trace_id"] == CHILD_TRACE_ID
    assert node(g, "wait")["ref"]["trace_id"] is None  # a timer never carries one
    # Per command unless a traced client started the run: then the run has its own.
    assert g["workflow"]["trace_id"] == CLIENT_TRACE_ID
    assert graph("sequence")["workflow"]["trace_id"] is None


def test_done_when_continue_as_new_terminal() -> None:
    g = graph("continue_as_new")
    assert g["workflow"]["status"] == "continued_as_new"
    assert g["workflow"]["next_run_id"] == "0199f0a1-a054-7082-8d66-5f7a9b1c3e45"
    assert g["workflow"]["failure"] is None
    assert edges(g) == chain("fetch_page")


def test_done_when_parse_is_total_a_truncated_history_is_running_with_no_end_edges() -> None:
    g = graph("running")
    assert g["workflow"]["status"] == "running"
    assert g["workflow"]["t"]["closed"] is None
    assert not any(b == END for _, b in edges(g))
    assert node(g, "enrich")["status"] == "scheduled"
    assert node(g, "enrich")["span"]["to"] is None


async def test_done_when_with_io_only_with_a_codec() -> None:
    """``parse`` has no io at all; ``with_io`` is a separate step, and refuses to run
    without a codec (seam ⑤)."""
    history = fixture("sequence")
    g = parse(history)
    assert all("io" not in n for n in g["nodes"])
    assert "io" not in g["workflow"]
    with pytest.raises(TypeError, match="codec"):
        await with_io(g, history, None)


def test_done_when_same_name_from_two_call_sites_gets_distinct_ids() -> None:
    g = graph("two_call_sites")
    assert ids(g) == ["a", "b", "a#2"]
    assert edges(g) == chain("a", "b", "a#2")
    assert node(g, "a#2")["name"] == node(g, "a#2")["label"] == "a"


def test_done_when_a_loop_over_one_call_site_is_one_node() -> None:
    g = graph("loop")
    assert ids(g) == ["a"]
    assert edges(g) == chain("a") | {("a", "a")}
    assert (node(g, "a")["count"], node(g, "a")["parallel"]) == (4, 1)


# --------------------------------------------------------------------------- identity


def test_a_gather_then_a_join() -> None:
    g = graph("gather_join")
    assert {node(g, n)["group"] for n in "abc"} == {0}
    assert edges(g) == {(START, n) for n in "abc"} | {(n, "d") for n in "abc"} | {("d", END)}


def test_a_loop_body_has_a_back_edge() -> None:
    g = graph("loop_body")
    assert ids(g) == ["a", "b"]
    assert edges(g) == chain("a", "b") | {("b", "a")}


def test_polling_folds_its_last_partial_iteration() -> None:
    """``check, wait, check, wait, check``: one ``check`` and one ``wait``."""
    g = graph("polling")
    assert ids(g) == ["check", "wait"]
    assert edges(g) == chain("check") | {("check", "wait"), ("wait", "check")}
    assert node(g, "wait")["label"] == "wait 30s"
    assert (node(g, "check")["count"], node(g, "wait")["count"]) == (3, 2)


def test_a_gather_of_chains_is_a_then_b() -> None:
    """Each ``b`` waits on its own ``a`` only: no edge from what the task never waited on,
    and the second ``b`` (in a task of its own) folds into the first."""
    g = graph("gather_chains")
    assert ids(g) == ["a", "b"]
    assert edges(g) == chain("a", "b")
    assert (node(g, "a")["parallel"], node(g, "b")["parallel"], node(g, "b")["count"]) == (2, 1, 2)


def test_ids_are_stable_across_runs_of_the_same_code() -> None:
    """The same call site has the same id in every run, whatever the branch, the server or
    the number of iterations: what union and merge match by."""
    assert ids(graph("sequence")) == ids(graph("sequence_test_server"))
    approve, reject = graph("branch_approve"), graph("branch_reject")
    assert node(approve, "ship")["group"] == node(reject, "ship")["group"] == 2
    two = synth.History(server="test")
    for name in ("a", "a"):
        synth._then(two, name)
    two.complete(two.task())
    assert ids(parse(two.json())) == ids(graph("loop")) == ["a"]


def test_folding_nests_loops() -> None:
    """``for x: (for y in 2: a); b``, three times: ``a`` and ``b``, each once. And
    ``for x: (for y in 2: a; b); c``: the inner loop folds at period 2, and only folding
    again from period 1 lets the outer one (now ``L c L c L c``) fold at all."""
    h = synth.History()
    for _ in range(3):
        synth._then(h, "a")
        synth._then(h, "a")
        synth._then(h, "b")
    h.complete(h.task())
    g = parse(h.json())
    assert ids(g) == ["a", "b"]
    assert edges(g) == chain("a", "b") | {("a", "a"), ("b", "a")}
    h = synth.History()
    for _ in range(3):
        for name in ("a", "b", "a", "b", "c"):
            synth._then(h, name)
    h.complete(h.task())
    g = parse(h.json())
    assert ids(g) == ["a", "b", "c"]
    assert edges(g) == chain("a", "b", "c") | {("b", "a"), ("c", "a")}


def test_a_name_two_kinds_share_keeps_ids_unique() -> None:
    """An activity and a local activity of one function, an activity called ``wait`` and
    one called ``start``: the activity keeps the name, the others are ``name~kind``."""
    h = synth.History()
    synth._then(h, "start")
    synth._then(h, "wait")
    h.fired(h.timer(h.task(), "1s"), 1)
    wtc = h.task()
    h.local(wtc, "fetch")
    h.finish(h.activity(wtc, "fetch"))
    h.complete(h.task())
    g = parse(h.json())
    assert ids(g) == ["start~activity", "wait", "wait~timer", "fetch~local_activity", "fetch"]
    assert len(set(ids(g))) == len(ids(g))
    assert {(START, "start~activity"), ("fetch~local_activity", "fetch")} <= edges(g)


def test_ids_are_one_to_one_across_runs() -> None:
    """A name holding ``#`` or ``~`` never takes another call site's id: those characters
    (and ``%``) are escaped in the name part, so ``#k`` and ``~kind`` only ever come from
    the suffixes, and union never merges two call sites of two runs."""
    a_b_a = graph("two_call_sites")
    named = synth.History()
    for name in ("a", "a#2"):
        synth._then(named, name)
    named.complete(named.task())
    other = parse(named.json())
    assert ids(other) == ["a", "a%232"]
    static = union([a_b_a, other])
    assert ids(static) == ["a", "b", "a#2", "a%232"]
    assert [node(static, i)["name"] for i in ("a#2", "a%232")] == ["a", "a#2"]
    assert edges(static) == chain("a", "b", "a#2") | chain("a", "a%232")
    runs = []
    for name in ("start", "start~activity", "50%"):
        h = synth.History()
        synth._then(h, name)
        h.complete(h.task())
        runs.append(parse(h.json()))
    assert [ids(g) for g in runs] == [["start~activity"], ["start%7Eactivity"], ["50%25"]]
    assert [n["name"] for n in union(runs)["nodes"]] == ["start", "start~activity", "50%"]


def test_what_a_task_commands_after_a_local_activity_waited_on_it() -> None:
    """A local activity runs inside its workflow task: ``x = await local_one(); await b(x)``
    writes the marker, then ``b`` (commanded once it resolved), and ``b`` follows it;
    ``gather(local_one(), b())`` writes ``b``, then the marker, one parallel group the run
    waits on both of. A marker is a close at its own place in the history."""
    g = graph("local_sequence")
    assert ids(g) == ["local_one", "b"]
    assert edges(g) == chain("local_one", "b")
    assert [n["group"] for n in g["nodes"]] == [0, 1]
    g = graph("local_gather")
    assert edges(g) == chain("a", "b") | {("a", "local_one"), ("local_one", END)}
    assert node(g, "b")["group"] == node(g, "local_one")["group"] == 1
    # gather(chain(local_one -> c), b): c waits on the local activity, the end on b and c.
    h = synth.History()
    wtc = h.task()
    b = h.activity(wtc, "b")
    h.local(wtc, "local_one")
    c = h.activity(wtc, "c")
    h.finish(b)
    h.finish(c)
    h.complete(h.task())
    assert edges(parse(h.json())) == chain("b") | chain("local_one", "c") - {("local_one", END)}
    # gather(local_one(), local_two()); await b(): both lead to b (and a sequence of two
    # local activities reads the same: their markers are all history has of them).
    h = synth.History()
    wtc = h.task()
    h.local(wtc, "local_one")
    h.local(wtc, "local_two")
    h.finish(h.activity(wtc, "b"))
    h.complete(h.task())
    assert edges(parse(h.json())) == chain("local_one", "b") | chain("local_two", "b")


def test_the_end_never_hangs_off_what_its_own_task_started() -> None:
    """``x = await a(); workflow.start_activity(c); return x``: ``c`` and the completion are
    one task's commands, so the run waited on ``a`` and never on ``c``."""
    g = graph("tail_forget")
    assert edges(g) == {(START, "a"), ("a", "c"), ("a", END)}
    # A local activity in that task ran before the end was commanded: the end waited on it.
    assert edges(graph("local_activity")) == chain("fetch_batch", "normalise")


def test_a_timer_the_workflow_canceled_is_nothing_it_waited_on() -> None:
    """``wait([sleep(3600), a()], FIRST_COMPLETED)``, the timer canceled, then ``b``: the
    timer's cancel is the workflow's own command, not a close it observed."""
    g = graph("timer_race")
    assert edges(g) == {(START, "a"), (START, "wait"), ("a", "b"), ("b", END)}
    assert (node(g, "wait")["status"], node(g, "wait")["label"]) == ("canceled", "wait 1h")
    # Canceled by a task that commands nothing else, before the one that schedules b.
    h = synth.History()
    wtc = h.task()
    timer = h.timer(wtc, "1h")
    h.finish(h.activity(wtc, "a"))
    h.timer_canceled(timer, h.task())
    h.add("WorkflowExecutionSignaled", {"signalName": "go", "identity": "cli"})
    h.finish(h.activity(h.task(), "b"))
    h.complete(h.task())
    assert edges(parse(h.json())) == {(START, "a"), (START, "wait"), ("a", "b"), ("b", END)}


def test_a_childs_start_is_nothing_waited_on() -> None:
    """A child and ``a`` in one task; the child starts, ``a`` completes, ``b`` follows:
    ``b`` waited on ``a`` only (``start_child_workflow`` resolves on the start, but an
    edge from it would say the code waited on the child)."""
    h = synth.History()
    wtc = h.task()
    initiated = h.child(wtc, "Settle", f"{WORKFLOW_ID}/settle")
    a = h.activity(wtc, "a")
    started = h.child_started(initiated, "0199f0a1-8c32-7e60-8b44-3d5f7a9b1c23")
    h.finish(a)
    b = h.activity(h.task(), "b")
    h.finish(b)
    h.child_closed(initiated, started, "0199f0a1-8c32-7e60-8b44-3d5f7a9b1c23")
    h.complete(h.task())
    assert edges(parse(h.json())) == chain("a", "b") | chain("Settle")


# --------------------------------------------------------------------------- parse


def test_workflow_task_ids_are_compared_by_value() -> None:
    """The time-skipping server's commands carry the WorkflowTaskStarted id: grouped by
    the value, never looked up, the graph is the dev server's."""
    dev, test = graph("sequence"), graph("sequence_test_server")
    assert edges(dev) == edges(test)
    assert [(n["id"], n["group"], n["status"]) for n in dev["nodes"]] == [
        (n["id"], n["group"], n["status"]) for n in test["nodes"]
    ]
    assert test["workflow"]["workflow_id"] is None  # the test server does not write it


@pytest.mark.parametrize(
    "shape",
    [
        lambda h: h,
        lambda h: {"history": h},
        lambda h: h["events"],
        lambda h: tuple(h["events"]),
    ],
    ids=["events", "ui-api", "list", "tuple"],
)
def test_every_history_shape_reads_alike(shape: Any) -> None:
    assert parse(shape(fixture("loop_body"))) == graph("loop_body")


def test_older_pascal_case_event_types_read_alike() -> None:
    """Older UI and CLI JSON spell event types ``ActivityTaskScheduled``."""
    history = fixture("child")
    for event in history["events"]:
        event["eventType"] = "".join(
            w.title() for w in event["eventType"][len("EVENT_TYPE_") :].split("_")
        )
    assert parse(history) == graph("child")


@pytest.mark.parametrize(
    "bad", [None, 3, "events", b"[]", {"events": None}, {"events": "x"}, {"history": []}, {}]
)
def test_what_is_not_history_shaped_is_refused(bad: Any) -> None:
    with pytest.raises(ValueError, match="history"):
        parse(bad)


def test_an_empty_history_is_an_empty_running_graph() -> None:
    g = parse({"events": []})
    assert (g["nodes"], g["edges"], g["workflow"]["status"]) == ([], [], "running")


def test_the_run_ids_are_the_callers() -> None:
    """``workflow_id`` and ``run_id`` win; the run id is never guessed from the history."""
    g = graph("sequence", workflow_id="other", run_id=RUN_ID)
    assert (g["workflow"]["workflow_id"], g["workflow"]["run_id"]) == ("other", RUN_ID)
    assert graph("sequence")["workflow"]["run_id"] is None


def test_a_node_has_every_key_whatever_its_kind() -> None:
    keys = {"id", "name", "kind", "label", "group", "status", "count", "parallel", "attempt"}
    keys |= {"counts", "t", "span", "ref", "failure"}
    ref = {"event_id", "activity_id", "task_queue", "trace_id", "span_id", "workflow_id", "run_id"}
    for name in HISTORIES:
        for n in graph(name)["nodes"]:
            assert set(n) == keys, (name, n["id"])
            assert set(n["ref"]) == ref | {"timer_id"}
            assert set(n["t"]) == {"scheduled", "started", "closed"}
            assert n["status"] is not None


def test_a_local_activity_is_a_node_and_a_patch_marker_is_not() -> None:
    g = graph("local_activity")
    assert ids(g) == ["fetch_batch", "normalise"]
    local = node(g, "normalise")
    assert (local["kind"], local["attempt"], local["status"]) == ("local_activity", 2, "completed")
    assert edges(g) == chain("fetch_batch", "normalise")
    # Its record read only when it is json/plain; otherwise the node is unnamed.
    history = fixture("local_activity")
    marker = history["events"][-2]["markerRecordedEventAttributes"]
    marker["details"]["data"]["payloads"][0]["metadata"]["encoding"] = "YmluYXJ5L3BsYWlu"
    assert node(parse(history), "local activity")["attempt"] is None


def test_a_failed_local_activity() -> None:
    history = fixture("local_activity")
    marker = history["events"][-2]["markerRecordedEventAttributes"]
    marker["failure"] = synth.failure(info="applicationFailureInfo")
    local = node(parse(history), "normalise")
    assert local["status"] == "failed"
    assert local["failure"]["kind"] == "application"


def test_a_nexus_operation_is_a_node() -> None:
    g = graph("nexus")
    nexus = node(g, "acme.enrich/enrich")
    assert (nexus["kind"], nexus["status"]) == ("nexus", "completed")
    assert nexus["t"]["started"] is not None
    events = fixture("nexus")["events"]
    started = next(
        i for i, e in enumerate(events) if e["eventType"].endswith("NEXUS_OPERATION_STARTED")
    )
    assert node(parse(events[: started + 1]), "acme.enrich/enrich")["status"] == "running"
    assert node(parse(events[:started]), "acme.enrich/enrich")["status"] == "scheduled"


def test_pending_activities_paint_what_history_cannot_tell() -> None:
    """History has nothing of an activity until it closes: ``DescribeWorkflowExecution``'s
    pending activities make it running, with its attempt and its last failure's kind."""
    bare = graph("pending")
    assert node(bare, "enrich")["status"] == node(bare, "audit")["status"] == "scheduled"
    describe = fixture("pending_describe")
    for overlay in (describe, describe["pendingActivities"]):
        g = graph("pending", pending=overlay)
        enrich, audit = node(g, "enrich"), node(g, "audit")
        assert (enrich["status"], enrich["attempt"]) == ("running", 3)
        assert enrich["failure"]["kind"] == "application"
        assert enrich["t"]["started"] == "2026-10-18T09:00:07.500000000Z"
        assert (audit["status"], audit["attempt"]) == ("scheduled", 2)
        assert audit["failure"] == {
            "kind": "timeout",
            "timeout_type": "start_to_close",
            "retry_state": None,
        }
        assert node(g, "fetch_batch") == node(bare, "fetch_batch")  # closed: not pending
    with pytest.raises(ValueError, match="pending"):
        graph("pending", pending=3)


def test_a_failure_is_its_kind() -> None:
    """Through an activity failure, the kind is its cause's; never a message."""
    g = graph("failed")
    fetch = node(g, "fetch_batch")
    assert fetch["status"] == "timed_out"
    assert fetch["failure"] == {
        "kind": "timeout",
        "timeout_type": "start_to_close",
        "retry_state": "timeout",
    }
    assert g["workflow"]["status"] == "failed"
    assert g["workflow"]["failure"] == {
        "kind": "timeout",
        "timeout_type": "start_to_close",
        "retry_state": None,
    }
    assert edges(g) == chain("fetch_batch")  # the workflow's own failure ends it


def test_a_retry_state_is_the_events_own_never_a_wrappers() -> None:
    """The test server writes no ``retryState`` on a workflow's or a child's failure, the
    dev server does. The ``activityFailureInfo`` the failure wraps carries the activity's
    retry state, which is not the workflow's: it is never taken in its place, so the same
    run reads alike from both servers but for what one of them does not write."""
    test, dev = graph("failed"), parse(synth.failed("dev"))
    assert test["workflow"]["failure"]["retry_state"] is None
    assert dev["workflow"]["failure"]["retry_state"] == "retry_policy_not_set"
    assert node(test, "fetch_batch")["failure"] == node(dev, "fetch_batch")["failure"]
    assert edges(test) == edges(dev)
    for server, expected in (("test", None), ("dev", "retry_policy_not_set")):
        h = synth.History(server=server)
        initiated = h.child(h.task(), "Settle", f"{WORKFLOW_ID}/settle")
        started = h.child_started(initiated, "0199f0a1-8c32-7e60-8b44-3d5f7a9b1c23")
        inner = synth.failure(
            "Activity task failed",
            info="activityFailureInfo",
            info_value={"retryState": "RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED"},
            cause=synth.failure(),
        )
        own = {"retryState": "RETRY_STATE_RETRY_POLICY_NOT_SET"} if server == "dev" else {}
        h.child_closed(
            initiated,
            started,
            "0199f0a1-8c32-7e60-8b44-3d5f7a9b1c23",
            "Failed",
            failure=inner,
            **own,
        )
        settle = node(parse(h.json()), "Settle")
        assert settle["failure"] == {
            "kind": "application",
            "timeout_type": None,
            "retry_state": expected,
        }


def test_terminated_and_timed_out_runs_have_no_end_edge() -> None:
    g = graph("terminated")
    assert g["workflow"]["status"] == "terminated"
    assert g["workflow"]["failure"]["kind"] == "terminated"
    assert not any(b == END for _, b in edges(g))
    history = fixture("running")
    history["events"].append(
        {
            "eventId": str(len(history["events"]) + 1),
            "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT",
            "workflowExecutionTimedOutEventAttributes": {"retryState": "RETRY_STATE_TIMEOUT"},
        }
    )
    g = parse(history)
    assert g["workflow"]["status"] == "timed_out"
    assert g["workflow"]["failure"] == {
        "kind": "timeout",
        "timeout_type": None,
        "retry_state": "timeout",
    }
    assert not any(b == END for _, b in edges(g))


def test_what_a_closed_run_left_open_ended_with_it() -> None:
    """No event closes a command still open when its run closes; the run's close does, so a
    run closed long ago never reads as in flight, nor queries its logs up to now. Temporal
    drops a closed run's activities and timers: canceled, or terminated / timed out with
    a run the server ended so."""
    g = graph("terminated", run_id=RUN_ID)
    enrich, closed = node(g, "enrich"), g["workflow"]["t"]["closed"]
    assert (enrich["status"], enrich["counts"]) == ("terminated", {"terminated": 1})
    assert enrich["t"]["closed"] == enrich["span"]["to"] == closed
    assert enrich["failure"] is None
    pane = panes(node(links(g, CFG), "enrich")["links"]["logs"])["kon"]
    assert pane["range"]["to"] == str(ms(closed, up=True) + 1000)
    assert "class n1 terminated" in mermaid(g)
    # Canceled: the workflow requested watch's cancel and closed before it was canceled.
    g = graph("canceled")
    assert (g["workflow"]["status"], node(g, "watch")["status"]) == ("canceled", "canceled")
    assert edges(g) == chain("a", "watch")  # woken by the cancel request: what came before
    # Completed, with an activity it started and never awaited.
    c = node(graph("tail_forget"), "c")
    assert (c["status"], c["span"]["to"]) == ("canceled", c["t"]["closed"])
    # Timed out by the server.
    history = fixture("running")
    history["events"].append(
        {
            "eventId": str(len(history["events"]) + 1),
            "eventTime": "2026-10-18T10:00:00.000000000Z",
            "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT",
            "workflowExecutionTimedOutEventAttributes": {"retryState": "RETRY_STATE_TIMEOUT"},
        }
    )
    enrich = node(parse(history), "enrich")
    assert (enrich["status"], enrich["span"]["to"]) == (
        "timed_out",
        "2026-10-18T10:00:00.000000000Z",
    )
    # Still running: nothing is closed for it.
    assert node(graph("running"), "enrich")["status"] == "scheduled"


@pytest.mark.parametrize(
    ("policy", "status"),
    [
        ("PARENT_CLOSE_POLICY_TERMINATE", "terminated"),
        ("PARENT_CLOSE_POLICY_REQUEST_CANCEL", "canceled"),
        ("PARENT_CLOSE_POLICY_UNSPECIFIED", "terminated"),
        ("PARENT_CLOSE_POLICY_ABANDON", "running"),
    ],
)
def test_a_child_left_open_follows_its_parent_close_policy(policy: str, status: str) -> None:
    """A child still running when its parent closes is terminated (Temporal's default),
    canceled, or, abandoned, left running: it may well be, and its own run says."""
    h = synth.History()
    initiated = h.child(h.task(), "Notify", f"{WORKFLOW_ID}/notify", parentClosePolicy=policy)
    h.child_started(initiated, "0199f0a1-9d43-7f71-9c55-4e6f8a0b2d34")
    h.complete(h.task())
    notify = node(links(parse(h.json(), run_id=RUN_ID), CFG), "Notify")
    assert notify["status"] == status
    assert (notify["span"]["to"] is None) == (status == "running")
    child = f"{UI}/{quote(f'{WORKFLOW_ID}/notify', safe='')}"
    expected = (
        child if status == "running" else f"{child}/0199f0a1-9d43-7f71-9c55-4e6f8a0b2d34/history"
    )
    assert notify["links"]["temporal"] == expected


def _fan_out_of_children(outcomes: Sequence[str]) -> dict[str, Any]:
    """One task starts a ``Settle`` child per outcome, in one parallel group; each then
    ends that way (``scheduled``: never started; ``running``: started, still open). The
    run is still running."""
    h = synth.History()
    wtc = h.task()
    initiated = [h.child(wtc, "Settle", f"{WORKFLOW_ID}/settle/{i}") for i in range(len(outcomes))]
    for i, (child, outcome) in enumerate(zip(initiated, outcomes, strict=True)):
        if outcome == "scheduled":
            continue
        run = f"0199f0a1-8c32-7e60-8b44-{i:012d}"
        started = h.child_started(child, run)
        if outcome != "running":
            kind = {"timed_out": "TimedOut"}.get(outcome, outcome.title())
            h.child_closed(child, started, run, kind)
    return h.json()


@pytest.mark.parametrize(
    ("outcomes", "status"),
    [
        (("completed", "running", "scheduled", "failed"), "running"),
        (("completed", "scheduled", "failed"), "scheduled"),
        (("completed", "timed_out", "failed"), "failed"),
        (("canceled", "timed_out", "completed"), "timed_out"),
        (("terminated", "canceled", "completed"), "canceled"),
        (("completed", "terminated"), "terminated"),
        (("completed", "completed"), "completed"),
    ],
)
def test_a_fan_outs_status_is_its_most_pressing_members(
    outcomes: Sequence[str], status: str
) -> None:
    """Among the occurrences of a node's latest batch the status is the first present of
    running, scheduled, failed, timed out, canceled, terminated, completed: a fan-out with
    one member still open is open, one with a member failed failed."""
    settle = node(parse(_fan_out_of_children(outcomes)), "Settle")
    assert settle["status"] == status
    assert settle["parallel"] == len(outcomes)
    assert settle["counts"] == dict(sorted(Counter(outcomes).items()))


def test_an_activity_fan_out_with_one_open_is_open() -> None:
    """Two of three scores completed: scheduled, running once pending says so, failed if
    the third failed; its span stays open while any is, and its logs run up to now."""
    h = synth.History()
    wtc = h.task()
    scores = [h.activity(wtc, "score", i) for i in range(3)]
    h.finish(scores[0])
    h.finish(scores[1])
    g = parse(h.json(), run_id=RUN_ID)
    score = node(g, "score")
    assert (score["status"], score["counts"]) == ("scheduled", {"completed": 2, "scheduled": 1})
    assert score["span"]["to"] is None
    assert panes(node(links(g, CFG), "score")["links"]["logs"])["kon"]["range"]["to"] == "now"
    pending = [{"activityId": "3", "state": "PENDING_ACTIVITY_STATE_STARTED", "attempt": 2}]
    running = node(parse(h.json(), pending=pending), "score")
    assert (running["status"], running["attempt"]) == ("running", 2)
    h.failed(scores[2], h.started(scores[2]), synth.failure(), "RETRY_STATE_NON_RETRYABLE_FAILURE")
    assert node(parse(h.json()), "score")["status"] == "failed"


def test_a_loops_status_is_its_last_iterations() -> None:
    """``a`` failed, then succeeded on the next iteration: the loop node is completed (its
    latest batch), the failure only in ``counts``; the other way round it is failed."""
    for first, last, status in (
        ("failed", "completed", "completed"),
        ("completed", "failed", "failed"),
    ):
        h = synth.History()
        for outcome in (first, last):
            scheduled = h.activity(h.task(), "a")
            if outcome == "completed":
                h.finish(scheduled)
            else:
                h.failed(
                    scheduled,
                    h.started(scheduled),
                    synth.failure(),
                    "RETRY_STATE_NON_RETRYABLE_FAILURE",
                )
        h.complete(h.task())
        g = parse(h.json())
        assert ids(g) == ["a"]
        assert (node(g, "a")["status"], node(g, "a")["counts"]) == (
            status,
            {"completed": 1, "failed": 1},
        )


def test_a_loop_node_describes_its_latest_iteration() -> None:
    """``t``, ``ref`` and ``failure`` are the latest occurrence's, ``attempt`` the most any
    took, and ``span`` runs from the first one scheduled to the last close."""
    h = synth.History()
    first = synth._then(h, "a", attempt=4, last_failure=synth.failure())
    last = synth._then(h, "a", attempt=1)
    h.complete(h.task())
    g = parse(h.json(), run_id=RUN_ID)
    a, times = node(g, "a"), {int(e["eventId"]): e["eventTime"] for e in h.events}
    assert a["attempt"] == 4
    assert a["failure"] is None  # the latest attempt 1 had no failure before it
    assert a["ref"]["event_id"] == last
    assert a["t"] == {
        "scheduled": times[last],
        "started": times[last + 1],
        "closed": times[last + 2],
    }
    assert a["span"] == {"from": times[first], "to": times[last + 2]}
    assert node(links(g, CFG), "a")["links"]["temporal"].endswith(f"/history/events/{last}")


@pytest.mark.parametrize(
    ("duration", "label"),
    [
        ("30s", "wait 30s"),
        ("1.500s", "wait 1.5s"),
        ("0.250s", "wait 0.25s"),
        ("300s", "wait 5m"),
        ("7200s", "wait 2h"),
        ("5400s", "wait 1h30m"),
        ("3605s", "wait 1h5s"),
        ("0s", "wait 0s"),
        ("-1s", "wait"),
        ("30", "wait"),
    ],
)
def test_a_timer_is_labelled_with_its_duration(duration: str, label: str) -> None:
    h = synth.History()
    h.fired(h.timer(h.task(), duration), 1)
    h.complete(h.task())
    timer = node(parse(h.json()), "wait")
    assert (timer["kind"], timer["label"]) == ("timer", label)


def test_a_timer_node_shows_its_latest_duration() -> None:
    h = synth.History()
    for duration in ("10s", "20s", "40s"):
        h.fired(h.timer(h.task(), duration), 1)
    h.complete(h.task())
    g = parse(h.json())
    assert ids(g) == ["wait"]
    assert node(g, "wait")["label"] == "wait 40s"


def test_a_child_that_never_started_failed() -> None:
    h = synth.History()
    initiated = h.child(h.task(), "Settle", "taken")
    h.add(
        "StartChildWorkflowExecutionFailed",
        {
            "namespace": "acme",
            "workflowId": "taken",
            "workflowType": {"name": "Settle"},
            "cause": "START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS",
            "initiatedEventId": str(initiated),
            "workflowTaskCompletedEventId": "4",
        },
    )
    settle = node(parse(h.json()), "Settle")
    assert (settle["status"], settle["failure"]["kind"], settle["ref"]["run_id"]) == (
        "failed",
        "server",
        None,
    )


def test_a_batch_child_is_named_from_its_own_history_never_from_its_id() -> None:
    """The id embeds the activity's name (``batch_workflow_id`` builds it), but ids are
    constructed, never parsed (decisions I1): without the child's history the node is
    ``kontra.batch.v1``, and with it the names are what the child scheduled."""
    decoy = f"{WORKFLOW_ID}/batch/decoy/0199f0a1"
    history = fixture("child")
    for event in history["events"]:
        for attrs in event.values():
            if isinstance(attrs, dict):
                if attrs.get("workflowId") == BATCH_ID:
                    attrs["workflowId"] = decoy
                if attrs.get("workflowExecution", {}).get("workflowId") == BATCH_ID:
                    attrs["workflowExecution"]["workflowId"] = decoy
    bare = node(parse(history), "kontra.batch.v1")
    assert (bare["name"], bare["label"]) == ("kontra.batch.v1", "execute_activities_in_parallel")
    named = parse(history, children={decoy: fixture("child_batch")})
    assert ids(named)[1] == "kontra.batch.v1[enrich,fetch_page]"
    assert "decoy" not in json.dumps([n["name"] for n in named["nodes"]])
    assert node(parse(history, children={BATCH_ID: fixture("child_batch")}), "kontra.batch.v1")


def batch_child(*scheduled: str, spec: Any = None, encrypted: bool = False) -> dict[str, Any]:
    """A batch child's first run: started with ``spec`` (none: no input), plain or
    encrypted, scheduling ``scheduled`` one after the other."""
    given: tuple[Any, ...] = () if spec is None else (spec, {"next": None, "acc": None})
    extra = None
    if encrypted:
        extra = {"input": {"payloads": [synth.sealed(v)[0] for v in given]}}
    h = synth.History(
        "kontra.batch.v1",
        workflow_id=BATCH_ID,
        run_id=BATCH_FIRST_RUN,
        input=given,
        extra=extra,
    )
    for name in scheduled:
        h.finish(h.activity(h.task(), name))
    h.complete(h.task())
    return h.json()


def spec(activity: str, pages: str | None = None, items: list[Any] | None = None) -> dict[str, Any]:
    """``kontra._parallel._Spec`` as the pydantic converter writes it."""
    return {
        "activity": activity,
        "activity_ref": {"module": "activities", "qualname": activity},
        "pages": pages,
        "pages_ref": None if pages is None else {"module": "activities", "qualname": pages},
        "items": items,
        "max_concurrent": 50,
        "options": {"start_to_close_timeout": "PT30S"},
        "reduce": None,
    }


def test_a_batch_is_named_from_what_its_input_asked_for_when_that_is_plain() -> None:
    """A run whose batch got no items, or an empty first page, schedules no item activity:
    named from its history alone it would be another node than every other run of the same
    call. A mock run's batch input is plain JSON (no codec), and names both activities; an
    encrypted one is never opened, and the names are what the child scheduled."""
    full = parse(fixture("child"), children={BATCH_ID: fixture("child_batch")})
    name = "kontra.batch.v1[enrich,fetch_page]"
    assert name in ids(full)
    asked = spec("enrich", pages="fetch_page")
    for child in (
        batch_child("fetch_page", spec=asked),  # an empty first page: no item activity
        batch_child("fetch_page", "enrich", "enrich", "fetch_page", spec=asked),
    ):
        named = parse(fixture("child"), children={BATCH_ID: child})
        assert ids(named) == ids(full)
        assert node(named, name)["label"] == "execute_activities_in_parallel(enrich,fetch_page)"
    no_items = parse(
        fixture("child"), children={BATCH_ID: batch_child(spec=spec("check", items=[]))}
    )
    assert (node(no_items, "kontra.batch.v1[check]")["label"]) == (
        "execute_activities_in_parallel(check)"
    )
    sealed = batch_child("fetch_page", spec=asked, encrypted=True)
    assert "kontra.batch.v1[fetch_page]" in ids(
        parse(fixture("child"), children={BATCH_ID: sealed})
    )
    # Only a kontra.batch.v1 input is read, and only names that are strings.
    for odd in ({"activity": 7, "pages": ["x"]}, "not an object", None):
        child = batch_child("enrich", spec=odd)
        assert "kontra.batch.v1[enrich]" in ids(parse(fixture("child"), children={BATCH_ID: child}))


def test_the_batch_workflow_name_is_kontras() -> None:
    from kontra._parallel import BATCH_WORKFLOW

    assert BATCH_WORKFLOW == kontra.graph._BATCH_WORKFLOW


def test_children_must_map_ids_to_histories() -> None:
    with pytest.raises(ValueError, match="children"):
        graph("child", children=[fixture("child_batch")])
    with pytest.raises(ValueError, match="history"):
        graph("child", children={BATCH_ID: "not a history"})


def test_parse_is_deterministic_and_leaves_its_input_alone() -> None:
    for name in HISTORIES:
        history = fixture(name)
        before = copy.deepcopy(history)
        first = json.dumps(parse(history), sort_keys=True)
        assert json.dumps(parse(history), sort_keys=True) == first
        assert history == before


def test_the_module_imports_only_the_standard_library() -> None:
    """Loaded alone (not through the ``kontra`` package, which imports Temporal), the
    module parses a history and no ``temporalio`` or protobuf module is loaded."""
    script = f"""
import importlib.util, json, sys
spec = importlib.util.spec_from_file_location("graph_alone", {kontra.graph.__file__!r})
module = importlib.util.module_from_spec(spec)
sys.modules["graph_alone"] = module
spec.loader.exec_module(module)
g = module.parse(json.loads(open({str(synth.FIXTURES_DIR / "child.json")!r}).read()))
assert [n["id"] for n in g["nodes"]] == ["fetch_batch", "kontra.batch.v1", "Settle"], g
module.mermaid(g)
module.links(g, module.LinkConfig(namespace="acme", temporal_ui="http://x"))
loaded = sorted(m for m in sys.modules if m.split(".")[0] in ("temporalio", "google", "kontra"))
assert not loaded, loaded
"""
    subprocess.run([sys.executable, "-c", script], check=True)


# --------------------------------------------------------------------------- totality


@pytest.mark.parametrize("name", HISTORIES)
def test_every_truncation_parses_as_a_running_run(name: str) -> None:
    events = fixture(name)["events"]
    terminal = next(
        (
            i
            for i, e in enumerate(events)
            if e["eventType"].startswith("EVENT_TYPE_WORKFLOW_EXECUTION_")
            and not e["eventType"].endswith(("_STARTED", "_SIGNALED"))
        ),
        len(events),
    )
    for cut in range(len(events) + 1):
        g = parse(events[:cut])
        _check_shape(g)
        if cut <= terminal:
            assert g["workflow"]["status"] == "running", (name, cut)
            assert not any(b == END for _, b in edges(g)), (name, cut)


def _check_shape(g: dict[str, Any]) -> None:
    """What every parsed graph is, however damaged its history."""
    json.dumps(g, sort_keys=True, allow_nan=False)
    known = ids(g)
    assert len(set(known)) == len(known)
    assert all(n["status"] is not None for n in g["nodes"])
    assert {a for e in edges(g) for a in e} <= {*known, START, END}
    assert [e for e in g["edges"]] == sorted(g["edges"], key=lambda e: (e["from"], e["to"]))
    static = union([g, g])
    painted = merge(static, g)
    json.dumps([static, painted, links(painted, CFG), links(static, CFG)], allow_nan=False)
    mermaid(painted)


_EVENT_TYPES = sorted(kontra.graph._EVENT_TYPES)
_ATTRIBUTE_KEYS = [
    "activityId",
    "activityType",
    "attempt",
    "details",
    "failure",
    "header",
    "initiatedEventId",
    "lastFailure",
    "markerName",
    "name",
    "newExecutionRunId",
    "operation",
    "retryState",
    "runId",
    "scheduledEventId",
    "service",
    "startToFireTimeout",
    "startedEventId",
    "taskQueue",
    "timeoutType",
    "workflowExecution",
    "workflowId",
    "workflowTaskCompletedEventId",
    "workflowType",
]
_json = st.recursive(
    st.none()
    | st.booleans()
    | st.integers(-(2**70), 2**70)
    | st.floats()
    | st.text(max_size=10)
    | st.sampled_from(["4", "5", "core_local_activity", "json/plain", "anNvbi9wbGFpbg==", "30s"]),
    lambda inner: (
        st.lists(inner, max_size=4)
        | st.dictionaries(st.sampled_from(_ATTRIBUTE_KEYS) | st.text(max_size=6), inner, max_size=4)
    ),
    max_leaves=12,
)
_junk_event = st.fixed_dictionaries(
    {
        "eventId": st.integers(-1, 80).map(str) | _json,
        "eventType": st.sampled_from(_EVENT_TYPES) | st.text(max_size=12),
        "eventTime": st.sampled_from(["2026-10-18T09:00:00Z", "2026-02-30T09:00:00Z", "x"]) | _json,
    }
).flatmap(
    lambda e: st.builds(
        lambda attrs: {**e, _attributes_key(e["eventType"]): attrs},
        st.dictionaries(st.sampled_from(_ATTRIBUTE_KEYS), _json, max_size=6) | _json,
    )
)


def _attributes_key(event_type: object) -> str:
    kind = kontra.graph._EVENT_TYPES.get(event_type) if isinstance(event_type, str) else None
    return f"{kind[0].lower()}{kind[1:]}EventAttributes" if kind else "junkEventAttributes"


def _damage(data: st.DataObject, value: Any) -> None:
    """Deletes a key or swaps a value somewhere inside ``value``."""
    while True:
        keys = list(value) if isinstance(value, dict) else list(range(len(value)))
        if not keys:
            return
        key = data.draw(st.sampled_from(keys))
        inner = value[key]
        if isinstance(inner, dict | list) and inner and data.draw(st.booleans()):
            value = inner
            continue
        if data.draw(st.booleans()):
            del value[key]
        else:
            value[key] = data.draw(_json)
        return


_JSON_PLAIN = base64.b64encode(b"json/plain").decode()
_GARBAGE = st.binary(max_size=48) | st.sampled_from(
    [
        b"{",
        b"\xff\xfe",
        b"[" * 3000,
        b"9" * 4301,
        b'"\\ud800"',
        b'{"traceparent": "00-\\ud800", "activity_type": "\\udfff", "attempt": -1}',
        f'{{"traceparent": "00-{TRACE_ID}-{SPAN_ID}-03", "activity_type": 7}}'.encode(),
    ]
)


def _garble_payloads(data: st.DataObject, value: Any) -> None:
    """Replaces the data of ``json/plain`` payloads anywhere in ``value`` -- the
    ``_tracer-data`` headers and local activity records, the payloads ``parse`` decodes --
    with arbitrary bytes."""
    if isinstance(value, dict):
        metadata = value.get("metadata")
        if (
            isinstance(metadata, dict)
            and metadata.get("encoding") == _JSON_PLAIN
            and "data" in value
            and data.draw(st.booleans())
        ):
            value["data"] = base64.b64encode(data.draw(_GARBAGE)).decode()
        for inner in value.values():
            _garble_payloads(data, inner)
    elif isinstance(value, list):
        for inner in value:
            _garble_payloads(data, inner)


@settings(max_examples=400, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(data=st.data())
def test_parse_never_raises_whatever_the_events_hold(data: st.DataObject) -> None:
    name = data.draw(st.sampled_from(HISTORIES))
    events = fixture(name)["events"]
    if data.draw(st.booleans()):
        _garble_payloads(data, events)
    for _ in range(data.draw(st.integers(0, 8))):
        _damage(data, data.draw(st.sampled_from(events)))
    for junk in data.draw(st.lists(_junk_event | _json, max_size=4)):
        events.insert(data.draw(st.integers(0, len(events))), junk)
    pending = data.draw(
        st.none()
        | st.lists(_json, max_size=3)
        | st.fixed_dictionaries({"pendingActivities": st.lists(_json, max_size=3) | st.none()})
        | st.just(fixture("pending_describe"))
    )
    g = parse(events, pending=pending, children={BATCH_ID: fixture("child_batch")})
    _check_shape(g)


@pytest.mark.parametrize(
    ("data", "attempt"),
    [
        (b"{", None),
        (b"\xff\xfe\x00{", None),
        (b"[" * 20_000 + b"]" * 20_000, None),
        (b"1" * 4301, None),
        (b'"\\ud800"', None),
        (b'{"traceparent": 5, "activity_type": ["x"], "attempt": "x"}', None),
        (b'{"traceparent": "00-\\ud800", "activity_type": "\\ud800", "attempt": 2}', 2),
    ],
    ids=["truncated", "not-utf8", "deep", "huge-int", "surrogate", "wrong-types", "surrogates"],
)
def test_a_tracer_header_or_marker_record_that_is_no_json_is_no_data(
    data: bytes, attempt: int | None
) -> None:
    """The two payloads ``parse`` reads (``_tracer-data`` and a local activity's record) are
    ``json/plain`` written by Temporal; what is not valid JSON of the right shape is no
    trace and no name, and parsing goes on."""
    encoded = base64.b64encode(data).decode()
    traced = fixture("traced")
    greet = traced["events"][4]["activityTaskScheduledEventAttributes"]
    greet["header"]["fields"]["_tracer-data"]["data"] = encoded
    g = parse(traced)
    assert (node(g, "greet")["ref"]["trace_id"], node(g, "greet")["ref"]["span_id"]) == (None, None)
    local = fixture("local_activity")
    marker = local["events"][-2]["markerRecordedEventAttributes"]
    marker["details"]["data"]["payloads"][0]["data"] = encoded
    assert node(parse(local), "local activity")["attempt"] == attempt


# --------------------------------------------------------------------------- scale


def test_wide_fan_outs_parse_in_linear_time() -> None:
    """Two 10,000-wide fan-outs of timers (40,000 events): every timer of the first leads
    to every timer of the second, which is one edge, never 10^8 pairs built and then
    deduplicated (about 10 s, where this takes well under one)."""
    h = synth.History()
    for _ in range(2):
        wtc = h.task()
        for timer in [h.timer(wtc, "1s") for _ in range(10_000)]:
            h.fired(timer, 0)
    h.complete(h.task())
    history = h.json()
    started = time.perf_counter()
    g = parse(history)
    elapsed = time.perf_counter() - started
    assert edges(g) == chain("wait") | {("wait", "wait")}
    assert (node(g, "wait")["count"], node(g, "wait")["parallel"]) == (20_000, 10_000)
    assert elapsed < 5, elapsed


def test_deeply_nested_loops_fold_in_bounded_time() -> None:
    """50,000 batches: distinct ones, then 16-wide loop bodies nested eight deep (every
    fold starts the periods again). Only places where a block can repeat are looked at:
    about 9 s before, about 1 s now."""
    counter = itertools.count(10**6)
    body = [next(counter) for _ in range(16)]
    for _ in range(8):
        body = body + body + [next(counter) for _ in range(15)]
    names = [*range(50_000 - len(body)), *body]
    signatures = [(("activity", str(n)),) for n in names]
    started = time.perf_counter()
    positions, where = kontra.graph._fold(signatures)
    elapsed = time.perf_counter() - started
    assert len(where) == 50_000
    assert len(positions) == 50_000 - len(body) + 16 + 8 * 15
    assert elapsed < 5, elapsed


# --------------------------------------------------------------------------- seam ⑤


def _payload_data(value: Any) -> list[str]:
    """Every payload's ``data`` (base64) anywhere in ``value``."""
    found: list[str] = []
    if isinstance(value, dict):
        if isinstance(value.get("data"), str) and "metadata" in value:
            found.append(value["data"])
        for inner in value.values():
            found += _payload_data(inner)
    elif isinstance(value, list):
        for inner in value:
            found += _payload_data(inner)
    return found


async def test_nothing_of_a_payload_leaves_parse() -> None:
    """Every payload of the leak fixture carries the marker in plaintext, one result is
    sealed, and the pending overlay's heartbeat details and last failure carry it too:
    neither the marker, nor the ciphertext, nor any payload's bytes reach a graph, a union,
    a merge, links or Mermaid, of the run ended or of it still running. ``with_io`` with
    the codec does show them (the control)."""
    history, ciphertext = synth.leak()
    pending = synth.leak_pending()
    g = parse(history, workflow_id=WORKFLOW_ID, run_id=RUN_ID, pending=pending)
    painted = merge(union([g]), g)
    # Before the operator ended it, slow is running, as pending says.
    terminated = next(
        i for i, e in enumerate(history["events"]) if e["eventType"].endswith("_TERMINATED")
    )
    live = parse(history["events"][:terminated], run_id=RUN_ID, pending=pending)
    assert (node(live, "slow")["status"], node(live, "slow")["attempt"]) == ("running", 3)
    assert node(live, "slow")["failure"] == {
        "kind": "application",
        "timeout_type": None,
        "retry_state": None,
    }
    assert (node(g, "slow")["status"], node(g, "slow")["failure"]["kind"]) == (
        "terminated",
        "application",
    )
    outputs = [
        json.dumps(g),
        json.dumps(union([g])),
        json.dumps(painted),
        json.dumps(links(painted, CFG)),
        mermaid(painted),
        mermaid(g),
        json.dumps(links(merge(union([g]), live), CFG)),
        mermaid(live),
    ]
    data = [d for d in _payload_data([history, pending]) if len(d) >= 8]
    assert len(data) > 30
    forbidden = [MARKER, MARKER.upper(), MARKER.lower(), ciphertext.hex(), *data]
    forbidden += [base64.b64encode(ciphertext).decode(), ciphertext.decode("latin-1")]
    forbidden += [quote(d, safe="") for d in data]
    for text in outputs:
        for secret in forbidden:
            assert secret not in text
    # Still the run's structure and ids.
    assert ids(g) == [
        "fetch_batch",
        "audit",
        "enrich",
        "watch",
        "Notify",
        "normalise",
        "slow",
        "acme.enrich/enrich",
    ]
    assert node(g, "fetch_batch")["ref"]["trace_id"] == TRACE_ID
    assert node(g, "enrich")["failure"] == {
        "kind": "application",
        "timeout_type": None,
        "retry_state": "maximum_attempts_reached",
    }
    assert node(g, "watch")["failure"]["timeout_type"] == "heartbeat"
    # The control: through the codec, the payloads are there.
    io = await with_io(g, history, SealCodec())
    fetch = node(io, "fetch_batch")["io"]
    assert fetch["result"] == {"secret": f"sealed {MARKER}"}
    assert fetch["input"] == [{"q": MARKER}]
    assert fetch["failure"] == {"message": MARKER, "stack_trace": f'  File "{MARKER}.py"'}
    assert io["workflow"]["io"]["input"] == [{"note": MARKER}]
    assert node(io, "normalise")["io"]["result"] == MARKER
    assert node(io, "acme.enrich/enrich")["io"] == {
        "input": [MARKER],
        "result": MARKER,
        "failure": None,
    }
    assert node(io, "Notify")["io"]["failure"]["message"] == MARKER


# --------------------------------------------------------------------------- with_io


async def test_the_test_codec_round_trips() -> None:
    codec = SealCodec()
    original = [Payload(metadata={"encoding": b"json/plain"}, data=b'{"a":1}')]
    sealed = await codec.encode(original)
    assert sealed[0].data != original[0].data
    assert await codec.decode(sealed) == original


async def test_with_io_decodes_through_the_codec() -> None:
    history = fixture("retry")
    io = await with_io(parse(history), history, SealCodec())
    fetch = node(io, "fetch_batch")["io"]
    assert fetch == {
        "input": ["fetch_batch"],
        "result": "ok",
        "failure": {"message": "rate limited (attempt 2)", "stack_trace": "  File ..."},
    }
    assert io["workflow"]["io"] == {"input": [{"batch": 3}], "result": "done", "failure": None}
    # A timer has none.
    history = fixture("polling")
    assert node(await with_io(parse(history), history, SealCodec()), "wait")["io"] == {
        "input": None,
        "result": None,
        "failure": None,
    }


async def test_a_payload_the_codec_cannot_open_is_undecodable_alone() -> None:
    history = fixture("undecodable")
    g = parse(history)
    io = node(await with_io(g, history, SealCodec()), "fetch_batch")["io"]
    assert io == {"input": {"error": "undecodable"}, "result": {"rows": 3}, "failure": None}
    assert "bm90IHNlYWxlZA" not in json.dumps(io)  # base64 of what it could not open


class PlaceholderCodec(SealCodec):
    """Shows what it cannot open as ``{"encrypted": true}``, as the graph page's does."""

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(metadata={"encoding": b"json/plain"}, data=b'{"encrypted":true}')
            for _ in payloads
        ]


async def test_a_failure_the_codec_shows_a_placeholder_for_is_that_placeholder() -> None:
    """Encoded failure attributes that decode to something other than a message and a
    stack trace are shown as they decoded, not as a failure without a message."""
    history = fixture("retry")
    fetch = node(await with_io(parse(history), history, PlaceholderCodec()), "fetch_batch")
    assert fetch["io"]["failure"] == {"encrypted": True}
    assert fetch["io"]["input"] == [{"encrypted": True}]


class MiscountingCodec(SealCodec):
    """Opens what it sealed, then drops the last payload (``extra`` < 0) or repeats every
    one (``extra`` > 0)."""

    def __init__(self, extra: int) -> None:
        self.extra = extra

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        opened = await super().decode(payloads)
        return opened[: self.extra] if self.extra < 0 else opened * (1 + self.extra)


@pytest.mark.parametrize("extra", [-1, 1])
async def test_a_codec_giving_the_wrong_number_of_payloads_is_undecodable(extra: int) -> None:
    """A result the codec dropped would read as null, an input it doubled as two values:
    a decode that does not give one payload per payload is undecodable."""
    history = fixture("retry")
    io = await with_io(parse(history), history, MiscountingCodec(extra))
    fetch = node(io, "fetch_batch")["io"]
    assert fetch == {
        "input": {"error": "undecodable"},
        "result": {"error": "undecodable"},
        "failure": {"error": "undecodable"},
    }
    assert io["workflow"]["io"]["input"] == {"error": "undecodable"}


async def test_with_io_shows_other_encodings_by_size_only() -> None:
    history = fixture("sequence")
    scheduled = history["events"][4]["activityTaskScheduledEventAttributes"]
    scheduled["input"] = {
        "payloads": [
            synth.payload(None, "binary/null"),
            {
                "metadata": {"encoding": base64.b64encode(b"binary/plain").decode()},
                "data": base64.b64encode(b"\x00\x01\x02").decode(),
            },
        ]
    }
    io = node(await with_io(parse(history), history, SealCodec()), "fetch_batch")["io"]
    assert io["input"] == [None, {"encoding": "binary/plain", "bytes": 3}]


async def test_with_io_leaves_its_inputs_alone_and_static_nodes_empty() -> None:
    history = fixture("branch_approve")
    painted = merge(union([graph("branch_approve"), graph("branch_reject")]), parse(history))
    before = copy.deepcopy(painted)
    io = await with_io(painted, history, SealCodec())
    assert painted == before
    assert node(io, "reject")["io"] == {"input": None, "result": None, "failure": None}
    assert node(io, "approve")["io"]["input"] == ["approve"]


# --------------------------------------------------------------------------- union, merge


def test_union_keeps_structure_only() -> None:
    g = graph("retry", workflow_id=WORKFLOW_ID, run_id=RUN_ID)
    static = union([g])
    assert edges(static) == edges(g)
    assert static["workflow"] == {
        "workflow_id": None,
        "run_id": None,
        "type": "EnrichFlow",
        "task_queue": "acme/enrich@1.0.0",
        "status": None,
        "t": None,
        "next_run_id": None,
        "first_run_id": None,
        "trace_id": None,
        "failure": None,
    }
    assert node(static, "fetch_batch") == {
        "id": "fetch_batch",
        "name": "fetch_batch",
        "kind": "activity",
        "label": "fetch_batch",
        "group": 0,
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


def test_merge_never_groups_an_untaken_branch_with_a_step_of_the_run() -> None:
    """The run's groups are its positions, the static graph's union's: ``approve`` (the
    run's) and ``reject`` (static only) number alike, and would be drawn as one fan-out."""
    static = union([graph("branch_approve"), graph("branch_reject")])
    live = parse(fixture("branch_approve"))
    painted = merge(static, live)
    groups = {n["id"]: n["group"] for n in painted["nodes"]}
    live_groups = {n["id"]: n["group"] for n in live["nodes"]}
    assert {i: groups[i] for i in live_groups} == live_groups  # the run's, as they were
    assert groups["reject"] > max(live_groups.values())
    assert len(set(groups.values())) == len(groups)
    # Static-only nodes that shared a group still do.
    two = {"nodes": [{"id": "x", "group": 0}, {"id": "y", "group": 0}], "edges": []}
    shared = merge(two, {"nodes": [{"id": "a", "group": 3}], "edges": []})
    assert [n["group"] for n in shared["nodes"]] == [4, 4, 3]


def test_union_groups_only_what_some_run_commanded_together() -> None:
    """A run's group is a position in that run: an ``if``'s two branches, taken at the same
    point by two runs, number alike there, and would read as one fan-out in the union. There
    two nodes share a group only when one run commanded them in one batch."""
    approve, reject = graph("branch_approve"), graph("branch_reject")
    assert node(approve, "approve")["group"] == node(reject, "reject")["group"]
    static = union([approve, reject])
    assert [(n["id"], n["group"]) for n in static["nodes"]] == [
        ("check", 0),
        ("approve", 1),
        ("ship", 2),
        ("reject", 3),
    ]
    fan = union([graph("sequence"), graph("fan_out")])
    groups = {n["id"]: n["group"] for n in fan["nodes"]}
    assert groups["lookup_company"] == groups["lookup_domain"] == groups["lookup_person"]
    assert len(set(groups.values())) == 5  # the three sequence steps, the fan-out, score
    # A node scheduled with one node in one run and another in the next joins both.
    a = {"nodes": [{"id": "x", "group": 0}, {"id": "y", "group": 0}], "edges": []}
    b = {"nodes": [{"id": "z", "group": 0}, {"id": "y", "group": 0}, {"id": "w", "group": 1}]}
    assert [n["group"] for n in union([a, b])["nodes"]] == [0, 0, 0, 1]


def test_union_of_nothing_is_empty() -> None:
    empty = union([])
    assert (empty["nodes"], empty["edges"], empty["workflow"]["type"]) == ([], [], None)


def test_union_and_merge_leave_their_inputs_alone() -> None:
    a, b = graph("branch_approve"), graph("branch_reject")
    before = copy.deepcopy((a, b))
    static = union(iter([a, b]))
    painted = merge(static, a)
    painted["nodes"][0]["status"] = "changed"
    painted["workflow"]["status"] = "changed"
    assert (a, b) == before
    assert edges(static) == edges(union([b, a]))
    assert ids(static) == ["check", "approve", "ship", "reject"]  # in the order first seen


def test_merge_keeps_a_live_node_the_static_graph_lacks() -> None:
    painted = merge(union([graph("branch_approve")]), graph("branch_reject"))
    assert {n["id"]: n["reached"] for n in painted["nodes"]} == {
        "check": True,
        "approve": False,
        "ship": True,
        "reject": True,
    }


# --------------------------------------------------------------------------- links


def test_links_point_at_the_run_its_events_and_its_children() -> None:
    g = links(graph("child", run_id=RUN_ID, children={BATCH_ID: fixture("child_batch")}), CFG)
    assert g["workflow"]["links"]["temporal"] == f"{UI}/{WORKFLOW_ID}/{RUN_ID}/history"
    fetch = node(g, "fetch_batch")
    assert fetch["links"]["temporal"] == f"{UI}/{WORKFLOW_ID}/{RUN_ID}/history/events/5"
    batch = node(g, "kontra.batch.v1[enrich,fetch_page]")
    child_id = quote(BATCH_ID, safe="")
    assert "%2Fbatch%2Fenrich%2F" in child_id
    assert batch["links"]["temporal"] == f"{UI}/{child_id}/{BATCH_LAST_RUN}/history"
    # Open, a child is linked without a run: the UI follows its chain to the latest.
    events = fixture("child")["events"]
    cut = next(
        i
        for i, e in enumerate(events)
        if e["eventType"].endswith("CHILD_WORKFLOW_EXECUTION_COMPLETED")
    )
    open_graph = links(parse(events[:cut], run_id=RUN_ID), CFG)
    assert node(open_graph, "kontra.batch.v1")["links"]["temporal"] == f"{UI}/{child_id}"
    # Without a run id there is no event to point at; the run itself is run-less.
    runless = links(graph("sequence"), CFG)
    assert runless["workflow"]["links"]["temporal"] == f"{UI}/{WORKFLOW_ID}"
    assert "temporal" not in node(runless, "fetch_batch")["links"]


def test_logs_are_loki_over_the_nodes_span() -> None:
    g = links(graph("sequence", run_id=RUN_ID), CFG)
    fetch = node(g, "fetch_batch")
    assert fetch["links"]["logs"].startswith(EXPLORE)
    selector = f'{{service_name=~".+"}} | workflow_id="{WORKFLOW_ID}" | run_id="{RUN_ID}"'
    assert panes(fetch["links"]["logs"]) == {
        "kon": {
            "datasource": "loki-uid",
            "queries": [
                {
                    "refId": "A",
                    "datasource": {"type": "loki", "uid": "loki-uid"},
                    "expr": f'{selector} | activity_type="fetch_batch"',
                    "queryType": "range",
                }
            ],
            "range": {
                "from": str(ms(fetch["span"]["from"]) - 1000),
                "to": str(ms(fetch["span"]["to"], up=True) + 1000),
            },
        }
    }
    workflow = panes(g["workflow"]["links"]["logs"])["kon"]
    assert workflow["queries"][0]["expr"] == selector
    assert workflow["range"]["from"] == str(ms(g["workflow"]["t"]["started"]) - 1000)
    # Open: up to now.
    running = links(graph("running", run_id=RUN_ID), CFG)
    assert panes(node(running, "enrich")["links"]["logs"])["kon"]["range"]["to"] == "now"
    assert panes(running["workflow"]["links"]["logs"])["kon"]["range"]["to"] == "now"


def test_a_child_logs_under_its_own_workflow_id() -> None:
    g = links(graph("child", run_id=RUN_ID), CFG)
    expr = panes(node(g, "kontra.batch.v1")["links"]["logs"])["kon"]["queries"][0]["expr"]
    assert expr == f'{{service_name=~".+"}} | workflow_id="{BATCH_ID}"'


def test_a_trace_is_tempo_by_the_commands_trace_id() -> None:
    g = links(graph("traced", run_id=RUN_ID), CFG)
    greet = node(g, "greet")
    pane = panes(greet["links"]["trace"])["kon"]
    assert pane["datasource"] == "tempo-uid"
    assert pane["queries"] == [
        {
            "refId": "A",
            "datasource": {"type": "tempo", "uid": "tempo-uid"},
            "queryType": "traceql",
            "query": TRACE_ID,
        }
    ]
    assert panes(g["workflow"]["links"]["trace"])["kon"]["queries"][0]["query"] == CLIENT_TRACE_ID
    assert "trace" not in node(g, "upper_hex")["links"]
    assert "trace" not in node(g, "wait")["links"]


def test_links_encode_every_segment_and_escape_logql() -> None:
    workflow_id, run_id = 'acme/flow "x" \\ y\nz', "r/1"
    g = links(graph("sequence", workflow_id=workflow_id, run_id=run_id), CFG)
    encoded = quote(workflow_id, safe="")
    assert g["workflow"]["links"]["temporal"] == f"{UI}/{encoded}/r%2F1/history"
    assert node(g, "enrich")["links"]["temporal"] == f"{UI}/{encoded}/r%2F1/history/events/11"
    expr = panes(node(g, "enrich")["links"]["logs"])["kon"]["queries"][0]["expr"]
    assert expr == (
        '{service_name=~".+"} | workflow_id="acme/flow \\"x\\" \\\\ y\\nz" | run_id="r/1"'
        ' | activity_type="enrich"'
    )
    special = LinkConfig(namespace="a b/c", temporal_ui="https://ui.example/temporal/")
    assert links(graph("sequence"), special)["workflow"]["links"] == {
        "temporal": f"https://ui.example/temporal/namespaces/a%20b%2Fc/workflows/{WORKFLOW_ID}"
    }


def test_a_string_no_server_writes_is_unreadable() -> None:
    """Proto strings are UTF-8, so a lone surrogate exists only in hand-built JSON: such an
    id is not read (as anything unreadable), and links, Mermaid and JSON never fail on it."""
    history = fixture("child")
    for event in history["events"]:
        for attrs in event.values():
            if isinstance(attrs, dict) and attrs.get("workflowId") == BATCH_ID:
                attrs["workflowId"] = "child-\udfff"
            if isinstance(attrs, dict) and "workflowExecution" in attrs:
                attrs["workflowExecution"]["workflowId"] = "child-\udfff"
    history["events"][0]["workflowExecutionStartedEventAttributes"]["workflowId"] = "wf-\ud800"
    g = parse(history, run_id=RUN_ID)
    assert g["workflow"]["workflow_id"] is None
    assert node(g, "kontra.batch.v1")["ref"]["workflow_id"] is None
    json.dumps(links(g, CFG), ensure_ascii=False).encode()
    mermaid(g).encode()
    # A graph built by hand with one: no link from it, and no error.
    hand = graph("sequence", workflow_id="wf-\ud800", run_id=RUN_ID)
    linked = links(hand, CFG)
    assert "temporal" not in linked["workflow"]["links"]
    assert "temporal" not in node(linked, "enrich")["links"]
    with pytest.raises(ValueError, match="namespace"):
        LinkConfig(namespace="acme-\ud800")


def test_static_only_nodes_get_no_links() -> None:
    painted = merge(
        union([graph("branch_approve"), graph("branch_reject")]),
        graph("branch_approve", run_id=RUN_ID),
    )
    g = links(painted, CFG)
    assert node(g, "reject")["links"] == {}
    assert set(node(g, "approve")["links"]) == {"temporal", "logs"}
    assert links(union([graph("sequence")]), CFG)["workflow"]["links"] == {}


def test_only_the_configured_links_are_made() -> None:
    bare = LinkConfig(namespace="acme")
    g = links(graph("traced", run_id=RUN_ID), bare)
    assert g["workflow"]["links"] == {}
    assert all(n["links"] == {} for n in g["nodes"])
    no_tempo = LinkConfig(namespace="acme", grafana="http://g", loki_uid="l")
    assert set(node(links(graph("traced", run_id=RUN_ID), no_tempo), "greet")["links"]) == {"logs"}


def test_links_leave_the_graph_alone() -> None:
    g = graph("sequence", run_id=RUN_ID)
    before = copy.deepcopy(g)
    links(g, CFG)
    assert g == before


@pytest.mark.parametrize(
    "url",
    [
        "ftp://grafana.local",
        "javascript:alert(1)",
        "http://grafana.local/?orgId=2",
        "http://grafana.local/#x",
        "http://",
        "grafana.local",
        "http://grafana local",
        "http://grafana.local/\n",
        "",
    ],
)
def test_link_config_takes_http_bases_only(url: str) -> None:
    with pytest.raises(ValueError, match="http"):
        LinkConfig(namespace="acme", grafana=url)
    with pytest.raises(ValueError, match="http"):
        LinkConfig(namespace="acme", temporal_ui=url)


def test_link_config_needs_a_namespace() -> None:
    with pytest.raises(ValueError, match="namespace"):
        LinkConfig(namespace="")


# --------------------------------------------------------------------------- mermaid


def test_mermaid_golden() -> None:
    assert mermaid(graph("sequence")) == (
        "flowchart TD\n"
        "    s((start))\n"
        '    n0["fetch_batch"]\n'
        '    n1["enrich"]\n'
        '    n2["write_records"]\n'
        "    e((end))\n"
        "    n1 --> n2\n"
        "    n0 --> n1\n"
        "    s --> n0\n"
        "    n2 --> e\n"
        "    classDef completed fill:#e6f4ea,stroke:#1e8e3e\n"
        "    class n0,n1,n2 completed\n"
    )


def test_mermaid_shapes_marks_and_reach() -> None:
    painted = merge(
        union([graph("branch_approve"), graph("branch_reject")]),
        graph("branch_approve"),
    )
    text = mermaid(painted)
    assert "    n3 -.-> n2\n" in text  # reject -> ship, not taken
    assert "    classDef unreached opacity:0.45\n    class n3 unreached\n" in text
    retry = mermaid(graph("retry"))
    assert '    n0["fetch_batch \N{MULTIPLICATION SIGN}3"]\n' in retry
    fan = mermaid(graph("fan_out"))
    assert '    n3["score \N{PARALLEL TO}5"]\n' in fan
    shapes = mermaid(graph("child")) + mermaid(graph("polling")) + mermaid(graph("nexus"))
    assert '    n1[["execute_activities_in_parallel"]]\n' in shapes
    assert '    n1(["wait 30s"])\n' in shapes
    assert '    n0{{"acme.enrich/enrich"}}\n' in shapes
    local = mermaid(graph("local_activity"))
    assert '    n1["normalise \N{MULTIPLICATION SIGN}2"]\n' in local
    assert "    class n1 local\n" in local
    running = mermaid(graph("running"))
    assert "e((end))" not in running
    assert "    class n1 scheduled\n" in running


@pytest.mark.parametrize(
    "name",
    [
        "fetch %%{init: {'theme':'dark','themeCSS':'.node rect{fill:red}'}}%%",
        "fetch %%{wrap}%%",
        "%%{wrap}%%",
        "fetch %% a comment",
    ],
)
def test_mermaid_labels_carry_no_directive(name: str) -> None:
    """Mermaid applies a ``%%{...}%%`` directive found anywhere in the text, inside a quoted
    label too (a theme, ``themeCSS`` fetching a remote image, the flowchart's config), and
    one that empties a label breaks the whole chart: ``%`` is an entity code."""
    h = synth.History()
    synth._then(h, name)
    h.complete(h.task())
    text = mermaid(parse(h.json()))
    assert "%" not in text
    assert f'    n0["{name.replace("%", "#37;")}"]\n' in text


def test_mermaid_escapes_labels() -> None:
    """A name is only ever a label, and what Mermaid would read in one is an entity code."""
    nasty = 'a"b<c>d`e\nf#g&h'
    h = synth.History()
    synth._then(h, nasty)
    h.complete(h.task())
    text = mermaid(parse(h.json()))
    assert '    n0["a#quot;b#lt;c#gt;d#96;e f#35;g#amp;h"]\n' in text
    assert len(text.splitlines()) == 8
    assert nasty not in text
