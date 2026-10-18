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
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from temporalio.api.common.v1 import Payload
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
    time-skipping server's at WorkflowTaskStarted (one before it)."""
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
    """``for x: (for y in 2: a); b``, three times: ``a`` and ``b``, each once."""
    h = synth.History()
    for _ in range(3):
        synth._then(h, "a")
        synth._then(h, "a")
        synth._then(h, "b")
    h.complete(h.task())
    g = parse(h.json())
    assert ids(g) == ["a", "b"]
    assert edges(g) == chain("a", "b") | {("a", "a"), ("b", "a")}


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
    assert ids(g) == ["start~activity", "wait", "wait~timer", "fetch", "fetch~local_activity"]
    assert len(set(ids(g))) == len(ids(g))
    assert (START, "start~activity") in edges(g)


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
        "retry_state": "retry_policy_not_set",
    }
    assert edges(g) == chain("fetch_batch")  # the workflow's own failure ends it


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


@settings(max_examples=400, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(data=st.data())
def test_parse_never_raises_whatever_the_events_hold(data: st.DataObject) -> None:
    name = data.draw(st.sampled_from(HISTORIES))
    events = fixture(name)["events"]
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
    sealed: neither the marker, nor the ciphertext, nor any payload's bytes reach a graph,
    a union, a merge, links or Mermaid. ``with_io`` with the codec does (the control)."""
    history, ciphertext = synth.leak()
    g = parse(history, workflow_id=WORKFLOW_ID, run_id=RUN_ID, pending=fixture("pending_describe"))
    painted = merge(union([g]), g)
    outputs = [
        json.dumps(g),
        json.dumps(union([g])),
        json.dumps(painted),
        json.dumps(links(painted, CFG)),
        mermaid(painted),
        mermaid(g),
    ]
    data = [d for d in _payload_data(history) if len(d) >= 8]
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
