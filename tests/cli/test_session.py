"""The agent-session renderer (``kontra/cli/_session.py``), driven from fixtures with no
server and no codec: every ``AgentEventType`` maps to the row, fold or hidden kind of the
vocabulary (``kontra/serve/_agent_events.py``); an unknown type is a generic row, never
dropped; deltas coalesce; dedupe is by the stream offset; a ``tool_id`` links to its graph
node and its scheduled event (two parallel same-name tools stay distinct); a handoff links to
its child node and nests the subagent's own rows; every string read from an event is capped;
one row and byte budget covers turns, rows and nested rows, the overflow one explicit row; a
timestamp that is no finite time is none; and the canonical recon sequence reproduces the
golden. Every fixture is one the installed harness's own ``AgentEvent`` model accepts."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest

from kontra.cli import _session
from kontra.serve import _agent_events as ae

FIXTURES = Path(__file__).parent / "fixtures" / "agent_events"
#: The two files that are not one event: the canonical sequence and the session it renders.
SEQUENCES = {"golden_session", "recon_session"}

AGENT = {
    "workflow_id": "job-x/agent",
    "run_id": "r1",
    "type": "ReconHarnessAgent",
    "name": "ReconHarnessAgent.sweep",
    "offering": "acme/recon@1.0.0",
    "status": "running",
    "interactive": False,
}

ROW_KINDS = sorted(k for k, v in ae.VOCAB.items() if v.role in (ae.Role.ROW, ae.Role.ROW_FAILED))
HIDDEN_KINDS = sorted(k for k, v in ae.VOCAB.items() if v.role == ae.Role.HIDDEN)


def _fixture(kind: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"{kind}.json").read_text())


def _envelope(kind: str, body: dict[str, Any] | None = None, **top: Any) -> dict[str, Any]:
    env: dict[str, Any] = {
        "agent_id": "a1",
        "turn_id": "t1",
        "turn_number": 1,
        "message_id": "m1",
        "timestamp": 0.0,
    }
    env.update(top)
    env["event"] = {"type": kind, **(body or {})}
    return env


def _build(items: list[tuple[int, dict[str, Any]]], **kw: Any) -> dict[str, Any]:
    return _session.build(items, agent=AGENT, **kw)


def _rows(session: dict[str, Any]) -> list[dict[str, Any]]:
    return [r for t in session["turns"] for r in t["rows"]]


# --------------------------------------------------------------------------- the vocabulary


def test_a_fixture_exists_for_every_event_type() -> None:
    present = {p.stem for p in FIXTURES.glob("*.json")} - SEQUENCES
    assert present == ae.AGENT_EVENT_TYPES


def test_every_fixture_is_an_event_the_installed_harness_writes() -> None:
    """The fixtures are the schema, not a guess at it: the harness's own model reads each one
    back unchanged (the canonical sequences too)."""
    events = pytest.importorskip("temporal_agent_harness.harness.agent_protocol.events")
    for path in sorted(FIXTURES.glob("*.json")):
        if path.stem in SEQUENCES:
            continue
        raw = json.loads(path.read_text())
        assert events.AgentEvent.model_validate(raw).model_dump(mode="json") == raw, path.stem
        assert raw["event"]["type"] == path.stem
    data = json.loads((FIXTURES / "recon_session.json").read_text())
    for _, raw in [*data["events"], *data["subagent_events"]]:
        assert events.AgentEvent.model_validate(raw).model_dump(mode="json") == raw


@pytest.mark.parametrize("kind", ROW_KINDS)
def test_a_row_event_opens_one_row_with_its_glyph_and_label(kind: str) -> None:
    session = _build([(0, _fixture(kind))])
    rows = _rows(session)
    assert len(rows) == 1, kind
    spec = ae.VOCAB[kind]
    assert (rows[0]["glyph"], rows[0]["label"]) == (spec.glyph, spec.label)
    if spec.role == ae.Role.ROW_FAILED:
        assert rows[0]["state"] == "failed"


@pytest.mark.parametrize("kind", HIDDEN_KINDS)
def test_a_hidden_event_opens_no_row_but_is_counted(kind: str) -> None:
    session = _build([(0, _fixture(kind))])
    assert _rows(session) == []
    assert session["footer"]["count"] == 1


def test_the_wireframe_labels_and_glyphs() -> None:
    """The eight row labels of the owner's wireframe, with their glyphs, and the one added
    (``callback.requested``, OD6)."""
    shown = {(v.glyph, v.label) for v in ae.VOCAB.values() if v.label}
    assert shown == {
        ("▸", "turn.started"),
        ("○", "model.call"),
        ("○", "model.response"),
        ("▣", "tool.call"),
        ("▣", "tool.result"),
        ("◆", "handoff"),
        ("▢", "approval.requested"),
        ("▢", "callback.requested"),
        ("▸", "response"),
    }


def test_message_accepted_is_the_turn_header_not_a_row() -> None:
    session = _build([(0, _fixture("message_accepted")), (1, _fixture("turn_started"))])
    [turn] = session["turns"]
    assert turn["handler"] == "sweep"
    assert turn["input_keys"] == ["domains"]
    assert turn["input_summary"] == "2 domains"
    [row] = _rows(session)
    assert (row["label"], row["summary"]) == ("turn.started", "input: 2 domains")


def test_turn_end_closes_the_turn() -> None:
    session = _build([(0, _fixture("turn_started")), (1, _fixture("turn_end"))])
    assert session["turns"][0]["status"] == "completed"


# --------------------------------------------------------------------------- folds


def test_reply_deltas_coalesce_into_the_model_response_and_close_the_call() -> None:
    items = [
        (0, _envelope("model_interaction_started", {"model": "gpt-4o"})),
        (1, _envelope("reply_delta", {"text": "plan "})),
        (2, _envelope("text_annotation", {"delta": {"type": "url_citation"}})),
        (3, _envelope("reply_delta", {"text": "first"})),
        (
            4,
            _envelope(
                "model_interaction_ended",
                {"model": "gpt-4o", "usage": {"input_tokens": 8, "output_tokens": 2}},
            ),
        ),
    ]
    call, response = _rows(_build(items))
    assert (call["label"], call["summary"], call["state"]) == ("model.call", "gpt-4o", "completed")
    assert response["label"] == "model.response"
    assert response["summary"] == "“plan first”"  # an annotation is not text
    assert response["usage"] == "8→2"


def test_thought_summary_folds_off_the_inline_summary() -> None:
    items = [
        (0, _envelope("model_interaction_started", {"model": "m"})),
        (1, _envelope("thought_summary", {"delta": {"text": "weighing the scope"}})),
        (2, _envelope("model_interaction_ended", {"model": "m"})),
    ]
    [_call, response] = _rows(_build(items))
    assert response["summary"] is None
    assert response["thought"] == "weighing the scope"


def test_a_tool_call_runs_then_closes_with_its_result() -> None:
    items = [
        (0, _envelope("tool_requested", {"tool_id": "x", "tool_name": "plan", "tool_input": {}})),
        (1, _envelope("tool_start", {"tool_id": "x", "tool_name": "plan", "tool_input": {}})),
    ]
    [call] = _rows(_build(items))
    assert call["state"] == "working"
    items.append(
        (
            2,
            _envelope(
                "tool_end", {"tool_id": "x", "tool_name": "plan", "tool_output": "12 targets"}
            ),
        )
    )
    call, result = _rows(_build(items))
    assert call["state"] == "completed"
    assert (result["label"], result["summary"]) == ("tool.result", "plan 12 targets")
    failed = [
        *items[:2],
        (2, _envelope("tool_error", {"tool_id": "x", "tool_name": "plan", "message": "no"})),
    ]
    call, result = _rows(_build(failed))
    assert (call["state"], result["state"], result["summary"]) == ("failed", "failed", "plan: no")


def test_approval_resolution_folds_into_the_approval_row() -> None:
    def approval(tool: str) -> tuple[int, dict[str, Any]]:
        return (
            0,
            _envelope(
                "tool_approval_requested",
                {"tool_id": tool, "tool_name": "post", "tool_input": {"to": "slack #sec"}},
            ),
        )

    [pending] = _rows(_build([approval("a")]))
    assert (pending["pending"], pending["resolution"], pending["hint"]) == (
        True,
        None,
        "to: slack #sec",
    )
    allow = [
        approval("a"),
        (1, _envelope("auto_approval_evaluation_ended", {"tool_id": "a", "verdict": "escalate"})),
        (2, _envelope("tool_approval_resolved", {"tool_id": "a", "approved": True})),
    ]
    [row] = _rows(_build(allow))
    assert (row["pending"], row["resolution"], row["auto"]) == (False, "allowed", "escalate")
    deny = [
        approval("b"),
        (1, _envelope("tool_approval_resolved", {"tool_id": "b", "approved": False})),
    ]
    [denied] = _rows(_build(deny))
    assert (denied["resolution"], denied["state"]) == ("denied", "failed")


@pytest.mark.parametrize(
    ("outcome", "state"), [("ok", "completed"), ("error", "failed"), ("timeout", "failed")]
)
def test_callback_resolution_folds_into_the_callback_row(outcome: str, state: str) -> None:
    items = [
        (0, _fixture("callback_requested")),
        (
            1,
            _envelope(
                "callback_resolved",
                {"tool_id": "call-confirm-0", "tool_name": "confirm", "outcome": outcome},
            ),
        ),
    ]
    [row] = _rows(_build(items))
    assert row["label"] == "callback.requested"
    assert (row["pending"], row["resolution"], row["state"]) == (False, outcome, state)


# --------------------------------------------------------------------------- subagents (OD5)


def _handoff_items() -> list[tuple[int, dict[str, Any]]]:
    return [
        (0, _fixture("subagent_started")),
        (1, _fixture("subagent_message_sent")),
        (2, _fixture("subagent_reply_received")),
        (3, _fixture("subagent_stopped")),
    ]


def test_without_its_history_a_handoff_nests_the_stream_s_own_lines_and_links_out() -> None:
    [row] = _rows(_build(_handoff_items()))
    assert (row["label"], row["summary"], row["state"]) == (
        "handoff",
        "→ cert-checker",
        "completed",
    )
    assert [(c["label"], c["summary"]) for c in row["children"]] == [
        ("check", None),
        ("response", "ok"),
    ]
    workflow_id = _fixture("subagent_started")["event"]["workflow_id"]
    assert row["link"] == workflow_id  # the opaque id, whole
    assert row["link_reason"] == "opens the subagent's own session"


def test_a_handoff_links_its_child_node_and_nests_the_subagent_s_history() -> None:
    workflow_id = _fixture("subagent_started")["event"]["workflow_id"]
    nested = [
        {"offset": 3, "glyph": "○", "label": "model.call", "summary": "gpt-4o", "nested": True}
    ]
    [row] = _rows(
        _build(
            _handoff_items(),
            handoffs={workflow_id: {"node": "Agent/CertChecker", "event_id": 9}},
            subagents={workflow_id: {"rows": nested}},
        )
    )
    assert (row["node"], row["event_id"]) == ("Agent/CertChecker", 9)  # the child it started
    # The subagent's own rows, not the stream's summary lines, linked to its child node.
    assert row["children"] == [{**nested[0], "node": "Agent/CertChecker"}]
    assert "link" not in row


def test_past_a_bound_the_handoff_says_why_it_links_out() -> None:
    workflow_id = _fixture("subagent_started")["event"]["workflow_id"]
    [row] = _rows(
        _build(_handoff_items()[:1], subagents={workflow_id: {"reason": "deeper than 3 subagents"}})
    )
    assert (row["link"], row["link_reason"]) == (workflow_id, "deeper than 3 subagents")


def test_nested_rows_are_the_calls_and_the_response_with_repeats_folded() -> None:
    data = json.loads((FIXTURES / "recon_session.json").read_text())
    sub = _session.build([(o, e) for o, e in data["subagent_events"]], agent={})
    rows = _session.nested_rows(sub)
    assert [(r["label"], r["summary"]) for r in rows] == [
        ("model.call", "gpt-4o"),
        ("tool.call", "fetch_cert \u00d712"),
        ("response", "{expired:1, weak:2}"),
    ]
    assert all(r["nested"] for r in rows)


def test_stream_unavailable_is_a_dim_note_under_the_handoff() -> None:
    items = [(0, _fixture("subagent_started")), (1, _fixture("subagent_stream_unavailable"))]
    [row] = _rows(_build(items))
    assert row["note"] == "the subagent's stream was not readable"


# --------------------------------------------------------------------------- the invariants


def test_an_unknown_type_is_a_generic_row_never_dropped() -> None:
    [row] = _rows(_build([(0, _envelope("quantum_entangled", {"spooky": True}))]))
    assert (row["glyph"], row["label"], row["raw_type"], row["type"]) == (
        ae.GENERIC_GLYPH,
        "quantum_entangled",
        "quantum_entangled",
        "unknown",
    )


def _poll(*items: tuple[int, object]) -> dict[str, Any]:
    def data(value: object) -> str:
        if isinstance(value, bytes):
            return base64.b64encode(value).decode()
        return base64.b64encode(json.dumps(value).encode()).decode()

    return {"items": [{"topic": "turn_events", "offset": off, "data": data(v)} for off, v in items]}


def test_a_poll_result_routes_by_topic_and_marks_what_it_will_not_inline() -> None:
    def inner(b64: str) -> object:
        return json.loads(base64.b64decode(b64))

    big = _envelope("reply_delta", {"text": "x" * 4000})
    poll = _poll((0, _fixture("turn_started")), (1, big), (2, b"\xff not json"))
    poll["items"].append({"topic": "private_notes", "offset": 3, "data": "e30="})
    got = _session.events_in_poll_result(poll, inner, item_max=1000)
    assert [off for off, _ in got] == [0, 1, 2]  # another topic is not this pane's
    assert got[0][1] == _fixture("turn_started")
    assert got[1][1]["event"]["type"] == _session.TOO_LARGE
    assert got[2][1]["event"]["type"] == "kontra.undecodable"
    rows = _rows(_build(got))
    assert [r["label"] for r in rows] == ["turn.started", _session.TOO_LARGE, "kontra.undecodable"]
    assert "too large to inline" in rows[1]["summary"]
    assert "x" * 100 not in json.dumps(rows)


def test_stream_poll_outcomes_are_the_completed_polls_alone() -> None:
    def accepted(event_id: int, name: str) -> dict[str, Any]:
        return {
            "eventId": str(event_id),
            "workflowExecutionUpdateAcceptedEventAttributes": {
                "acceptedRequest": {"input": {"name": name}}
            },
        }

    def completed(event_id: int, accepted_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "eventId": str(event_id),
            "workflowExecutionUpdateCompletedEventAttributes": {
                "acceptedEventId": str(accepted_id),
                "outcome": {"success": {"payloads": [payload]}},
            },
        }

    history = {
        "events": [
            accepted(5, "__temporal_workflow_stream_poll"),
            accepted(6, "send_agent_message"),
            completed(7, 5, {"data": "cG9sbA=="}),
            completed(8, 6, {"data": "bm90IGEgcG9sbA=="}),
            accepted(9, "__temporal_workflow_stream_poll"),  # still open: no outcome yet
        ]
    }
    assert _session.stream_poll_outcomes(history) == [(7, {"data": "cG9sbA=="})]
    assert _session.has_update(history, "send_agent_message")
    assert not _session.has_update(history, "tool_approval")


def test_dedupe_is_by_offset() -> None:
    items = [
        (5, _envelope("turn_started")),
        (5, _envelope("model_interaction_started", {"model": "m"})),  # same offset: skipped
    ]
    session = _build(items)
    assert [r["label"] for r in _rows(session)] == ["turn.started"]
    assert session["footer"]["count"] == 1
    assert session["footer"]["last_offset"] == 5


def test_tool_id_links_the_row_to_its_node_and_event_and_marks_a_fallback() -> None:
    items = [
        (0, _envelope("tool_requested", {"tool_id": "probe#0", "tool_name": "probe"})),
        (1, _envelope("tool_requested", {"tool_id": "probe#1", "tool_name": "probe"})),
        (
            2,
            _envelope(
                "tool_end", {"tool_id": "probe#1", "tool_name": "probe", "tool_output": "ok"}
            ),
        ),
        (3, _envelope("tool_requested", {"tool_id": "plan#0", "tool_name": "plan"})),
    ]
    nodes = {
        "probe#0": {"node": "probe", "event_id": 11},
        "probe#1": {"node": "probe#2", "event_id": 12, "fallback": True},
        "plan#0": {"node": "plan", "event_id": 20},
    }
    rows = _rows(_build(items, nodes=nodes))
    # Two parallel same-name tools stay distinct; the result links where its call does.
    assert [(r["node"], r["event_id"]) for r in rows] == [
        ("probe", 11),
        ("probe#2", 12),
        ("probe#2", 12),
        ("plan", 20),
    ]
    assert rows[1]["node_fallback"] is True
    assert "node_fallback" not in rows[0]


def test_summaries_are_capped() -> None:
    big = "x" * 1000
    [row] = _rows(
        _build([(0, _envelope("tool_end", {"tool_id": "t", "tool_name": "n", "tool_output": big}))])
    )
    assert len(row["summary"]) <= _session.SUMMARY_MAX
    assert row["summary"].endswith("…")


def test_the_row_cap_collapses_the_overflow_into_one_explicit_row() -> None:
    items = [(i, _envelope("turn_started")) for i in range(5)]
    session = _build(items, rows_max=3)  # the turn's header is one of them
    assert session["truncated"] is True
    *shown, note = _rows(session)
    assert len(shown) == 2
    assert (note["type"], note["summary"]) == ("truncated", "3 more not shown")
    assert session["footer"]["count"] == 5  # every event seen, not only the rows shown


def _size(value: object) -> int:
    return len(json.dumps(value, separators=(",", ":")))


def test_the_byte_cap_counts_what_is_sent_not_only_the_summaries() -> None:
    items = [
        (i, _envelope("tool_end", {"tool_id": f"t{i}", "tool_name": "n", "tool_output": "y" * 100}))
        for i in range(10)
    ]
    whole = _build(items)
    budget = _size(whole["turns"]) // 2
    session = _build(items, bytes_max=budget)
    *shown, note = _rows(session)
    assert 0 < len(shown) < 10
    assert note["summary"] == f"{10 - len(shown)} more not shown"
    turns = [{**t, "rows": t["rows"][:-1]} for t in session["turns"]]
    assert _size(turns) <= budget + 2 * (len(shown) + 1) + len('"rows":[]')


def _entries(rows: list[dict[str, Any]]) -> int:
    return sum(1 + _entries(r.get("children") or []) for r in rows)


def _drawn(session: dict[str, Any]) -> int:
    """Everything the page draws a line for: each turn, each row, each row nested under one."""
    return sum(1 + _entries(t["rows"]) for t in session["turns"])


def _many_children() -> list[tuple[int, dict[str, Any]]]:
    started = {"subagent_id": "s", "agent_key": "k", "workflow_id": "w"}
    sent = {"subagent_id": "s", "handler": "h"}
    return [(0, _envelope("subagent_started", started))] + [
        (i, _envelope("subagent_message_sent", sent)) for i in range(1, 50_000)
    ]


def _many_turns() -> list[tuple[int, dict[str, Any]]]:
    return [(i, _envelope("turn_end", turn_id=f"turn-{i}")) for i in range(50_000)]


def _many_nested() -> tuple[list[tuple[int, dict[str, Any]]], dict[str, Any]]:
    calls = [
        (i, _envelope("tool_requested", {"tool_id": str(i), "tool_name": f"t{i}"}))
        for i in range(10_000)
    ]
    view = {"rows": _session.nested_rows(_session.build(calls, agent={}))}
    assert len(view["rows"]) > 4000  # one subagent's view: bounded by its own build alone
    started = [
        (k, _envelope("subagent_started", {"subagent_id": f"s{k}", "workflow_id": f"w{k}"}))
        for k in range(16)
    ]
    return started, {f"w{k}": view for k in range(16)}


@pytest.mark.parametrize("case", ["children", "turns", "nested"])
def test_one_budget_bounds_turns_rows_and_nested_rows_alike(case: str) -> None:
    """A handoff's own lines, a turn per ``turn_id`` and a subagent's nested view are each
    charged to the one budget: past :data:`ROWS_MAX`, one row says how much more there is."""
    subagents: dict[str, Any] = {}
    if case == "children":
        items = _many_children()
    elif case == "turns":
        items = _many_turns()
    else:
        items, subagents = _many_nested()
    whole = _build(items, subagents=subagents, rows_max=10**9, bytes_max=10**12)
    session = _build(items, subagents=subagents)
    assert session["truncated"] is True
    assert _drawn(session) == _session.ROWS_MAX + 1  # what fits, and the one row saying so
    note = session["turns"][-1]["rows"][-1]
    assert note["type"] == "truncated"
    assert note["summary"] == f"{_drawn(whole) - _session.ROWS_MAX} more not shown"
    assert _size(session) < _session.BYTES_MAX


def test_every_string_read_from_an_event_is_capped() -> None:
    big = "x" * 50_000
    items = [
        (0, _envelope("message_accepted", {"handler": big, "payload": {big: 1}}, turn_id=big)),
        (1, _envelope(big)),  # an unknown type: its raw type is its label
        (2, _envelope("tool_requested", {"tool_id": "i" * 5000, "tool_name": big})),
        (3, _envelope("tool_end", {"tool_id": "i" * 5000, "tool_name": "n", "tool_output": 1})),
        (
            4,
            _envelope(
                "subagent_started", {"subagent_id": big, "agent_key": "k", "workflow_id": big}
            ),
        ),
        (5, _envelope("subagent_message_sent", {"subagent_id": big, "handler": big})),
        (6, _envelope("subagent_reply_received", {"subagent_id": big, "outcome": big})),
        (7, _envelope("tool_approval_requested", {"tool_id": "a", "tool_name": "n"})),
        (8, _envelope("auto_approval_evaluation_ended", {"tool_id": "a", "verdict": big})),
        (9, _envelope("callback_requested", {"tool_id": "c", "tool_name": "n"})),
        (10, _envelope("callback_resolved", {"tool_id": "c", "outcome": big})),
    ]
    session = _build(items, input_types={big: big})

    ids = {"turn_id", "tool_id", "subagent_id", "workflow_id", "link"}

    def check(value: object, key: str = "") -> None:
        if isinstance(value, str):
            assert len(value) <= (_session.ID_MAX if key in ids else _session.SUMMARY_MAX), key
        elif isinstance(value, dict):
            for k, v in value.items():
                check(k)
                check(v, k)
        elif isinstance(value, list):
            for v in value:
                check(v, key)

    check(session["turns"])
    header = session["turns"][0]  # the message's own turn_id, cut
    assert len(header["input_type"]) <= _session.SUMMARY_MAX
    rows = {r["offset"]: r for r in _rows(session)}
    # A cut id is shown cut and marked (no answer could name it), and still matched whole.
    assert (rows[2]["id_capped"], rows[2]["state"]) == (True, "completed")
    assert "link" not in rows[4]  # no workflow id is that long: nothing to open
    assert rows[4]["children"][0]["label"].endswith("…")


@pytest.mark.parametrize("stamp", ["Infinity", "-Infinity", "NaN", "1e308", "1" * 400])
def test_a_timestamp_that_is_no_finite_time_is_none(stamp: str) -> None:
    """``json.loads`` (the converter's) reads ``Infinity`` and ``NaN``: such a stamp, or one
    past any real time, is no time -- the session still renders, and JSON can carry it."""
    weird = json.loads(f'{{"timestamp": {stamp}}}')["timestamp"]
    items = [
        (0, _envelope("turn_started", timestamp=1.0)),
        (1, _envelope("model_interaction_started", {"model": "m"}, timestamp=weird)),
        (2, _envelope("model_interaction_ended", {"model": "m"}, timestamp=-1e300)),
    ]
    session = _build(items)
    assert [(r["at"], r["rt"]) for r in _rows(session)] == [
        (1.0, "+00:00"),
        (None, None),
        (None, None),
    ]
    json.dumps(session, allow_nan=False)  # what the console sends


def test_rows_carry_offset_and_time_for_client_replay() -> None:
    rows = _rows(_canonical())
    offsets = [r["offset"] for r in rows]
    assert offsets == sorted(offsets)  # offset order, which replay re-reveals
    assert all(isinstance(r["at"], float) for r in rows)
    assert [r["rt"] for r in rows][:3] == ["+00:00", "+00:01", "+00:01"]


# --------------------------------------------------------------------------- the golden


def _canonical() -> dict[str, Any]:
    data = json.loads((FIXTURES / "recon_session.json").read_text())
    sub = _session.build([(o, e) for o, e in data["subagent_events"]], agent={})
    workflow_id = next(iter(data["handoffs"]))
    return _session.build(
        [(off, env) for off, env in data["events"]],
        agent=data["agent"],
        nodes=data["nodes"],
        handoffs=data["handoffs"],
        subagents={workflow_id: {"rows": _session.nested_rows(sub)}},
    )


def test_the_canonical_recon_sequence_reproduces_the_golden() -> None:
    """The wireframe's turn: input line, model call and response, plan, probe with the
    cert-checker handoff nested under it, and the approval pending."""
    golden = json.loads((FIXTURES / "golden_session.json").read_text())
    assert _canonical() == golden


def test_the_footer_states_the_schema_and_the_dedupe_rule() -> None:
    session = _build([(0, _fixture("turn_started"))])
    assert session["schema"] == ae.AGENT_EVENTS_SCHEMA == "temporal-agent-harness/0.6"
    assert session["footer"] == {
        "count": 1,
        "last_offset": 0,
        "dedupe": "offset",
        "decrypted": "this machine's dev key",
    }
