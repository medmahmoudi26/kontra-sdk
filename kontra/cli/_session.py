"""The dev console's agent-session pane, rendered from the ``turn_events`` stream read back
from history (``docs/decisions.md`` I36). Pure, and no payload is decrypted here:
:func:`stream_poll_outcomes` names the history events that carry the stream, and :func:`build`
turns the already-decoded ``AgentEvent`` envelopes into the session the page draws -- turns,
rows, folds and the footer -- so a unit test drives it from fixtures with no server and no codec
(:mod:`kontra.cli._console` holds the codec and the fetches; the split is seam ⑤'s: structure
and short summaries here, verbatim bodies on ``/api/io`` alone).

An agent event never becomes its own history event: the harness publishes it into an
in-memory ``WorkflowStream``, and it reaches history only as an item inside the result of a
stream-poll Update (``__temporal_workflow_stream_poll``). So the one monotonic per-item counter
is the stream **offset**, and the wireframe's "seq" is that offset; dedupe is by offset.

The vocabulary -- every ``AgentEventType`` mapped to a row, a fold or a hidden kind -- lives in
:mod:`kontra.serve._agent_events` (shared with the turn-job wrapper, checked against the harness
in one place). An unknown type renders as a generic row carrying its raw type; nothing is
dropped silently. Every string the session carries is capped (:data:`SUMMARY_MAX` for what is
read, :data:`ID_MAX` for an id), and one budget covers all it emits -- turns, rows and the rows
nested under a handoff, a subagent's included -- by count (:data:`ROWS_MAX`) and by serialized
size (:data:`BYTES_MAX`): past either, one explicit "N more not shown" row says how much.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

from kontra.contract.names import STREAM_POLL_UPDATE
from kontra.serve import _agent_events as ae

__all__ = [
    "BYTES_MAX",
    "ID_MAX",
    "ROWS_MAX",
    "SUMMARY_MAX",
    "TOO_LARGE",
    "VERSION",
    "apply_answers",
    "build",
    "events_in_poll_result",
    "has_update",
    "nested_rows",
    "stream_poll_outcomes",
]

#: The JSON version the page reads.
VERSION = 1
#: A server-derived one-liner never carries a whole body: it is capped here, and so is every
#: other string read from an event (a label, a tool or handler name, an outcome).
SUMMARY_MAX = 120
#: An id read from an event (a turn's, a tool call's, a subagent's and its workflow's): Temporal's
#: own limit for a workflow id, and the longest ``tool_id`` an answer may name.
ID_MAX = 1000
#: At most this many turns and rows (nested ones included) in all; the rest is one "N more not
#: shown" row.
ROWS_MAX = 5000
#: At most this many bytes of them, serialized; the rest is that same row.
BYTES_MAX = 4 * 1024 * 1024
#: An input's field name in a turn's header.
_KEY_MAX = 40
#: The latest timestamp read as one (seconds since the epoch, about the year 5138): a later
#: one, or one that is no finite number, is no time at all.
_AT_MAX = 1e11
#: The type of the event :func:`events_in_poll_result` puts in place of an item too large to
#: show (#52's per-item cap, ``kontra.gate_client.models.STREAM_ITEM_MAX``): a row that says so.
TOO_LARGE = "kontra.too_large"

#: The rows a subagent's nested view leaves out (:func:`nested_rows`).
_FOLDED_WHEN_NESTED = frozenset({"turn.started", "model.response", "tool.result"})
#: A nested subagent row whose label and summary repeat the one before folds into it.
_REPEAT = "×"  # noqa: RUF001 - the multiplication sign is the label's


# --------------------------------------------------------------------------- history → events


def stream_poll_outcomes(history: Mapping[str, Any]) -> list[tuple[int, dict[str, Any]]]:
    """``(event id, payload)`` of every completed stream-poll Update in ``history``: the
    ``PollResult`` the caller decodes with the codec (plain age on an agent child, a
    ``binary/kontra-stream`` envelope on a job root -- one codec path either way). Pure: it
    reads only the history's shape, decrypts nothing.

    A poll Update is ``__temporal_workflow_stream_poll``; its accepted event names it, and the
    completed event that follows carries the result. Anything else is left out."""
    polls = _accepted(history, STREAM_POLL_UPDATE)
    outcomes: list[tuple[int, dict[str, Any]]] = []
    for event_id, completed in _completed(history):
        if _as_int(completed.get("acceptedEventId")) not in polls:
            continue
        payloads = _dig(completed, "outcome", "success", "payloads")
        if isinstance(payloads, list) and payloads and isinstance(payloads[0], dict):
            outcomes.append((event_id, payloads[0]))
    return outcomes


def has_update(history: Mapping[str, Any], name: str) -> bool:
    """Whether ``history`` accepted an Update named ``name`` (a harness agent's front door,
    ``send_agent_message``, says the run is an agent before any poll read its stream)."""
    return bool(_accepted(history, name))


def _accepted(history: Mapping[str, Any], name: str) -> dict[int, dict[str, Any]]:
    found: dict[int, dict[str, Any]] = {}
    for event in _events(history):
        accepted = event.get("workflowExecutionUpdateAcceptedEventAttributes")
        if isinstance(accepted, dict):
            event_id = _as_int(event.get("eventId"))
            if _dig(accepted, "acceptedRequest", "input", "name") == name and event_id is not None:
                found[event_id] = accepted
    return found


def _completed(history: Mapping[str, Any]) -> list[tuple[int, dict[str, Any]]]:
    found: list[tuple[int, dict[str, Any]]] = []
    for event in _events(history):
        completed = event.get("workflowExecutionUpdateCompletedEventAttributes")
        event_id = _as_int(event.get("eventId"))
        if isinstance(completed, dict) and event_id is not None:
            found.append((event_id, completed))
    return found


def _events(history: Mapping[str, Any]) -> list[dict[str, Any]]:
    events = history.get("events")
    if not isinstance(events, list):
        return []
    return [e for e in events if isinstance(e, dict)]


def events_in_poll_result(
    poll_result: Mapping[str, Any],
    inner: Callable[[str], object],
    *,
    topic: str = ae.TURN_EVENTS_TOPIC,
    item_max: int | None = None,
) -> list[tuple[int, dict[str, Any]]]:
    """``(offset, AgentEvent)`` for each item of a decoded ``PollResult`` on ``topic``.
    ``inner(item_data_b64)`` decodes one item's base64 ``Payload`` to the event JSON (the
    caller supplies it; here we only route by topic and offset). An item whose base64 is over
    ``item_max`` bytes is not decoded: a :data:`TOO_LARGE` event takes its place. An item that
    does not decode to an object becomes a generic ``kontra.undecodable`` row (never dropped)."""
    items = poll_result.get("items")
    out: list[tuple[int, dict[str, Any]]] = []
    if not isinstance(items, list):
        return out
    for item in items:
        if not isinstance(item, dict) or item.get("topic") != topic:
            continue
        offset = _as_int(item.get("offset"))
        data = item.get("data")
        if offset is None or not isinstance(data, str):
            continue
        if item_max is not None and len(data) > item_max:
            out.append(
                (offset, {ae.EVENT: {ae.EVENT_TYPE: TOO_LARGE, "bytes": len(data) * 3 // 4}})
            )
            continue
        try:
            event = inner(data)
        except Exception:  # an untrusted item; its bytes are never logged
            event = None
        if not isinstance(event, dict):
            event = {ae.EVENT: {ae.EVENT_TYPE: "kontra.undecodable"}}
        out.append((offset, event))
    return out


# --------------------------------------------------------------------------- events → session


def build(
    items: Sequence[tuple[int, Mapping[str, Any]]],
    *,
    agent: Mapping[str, Any],
    nodes: Mapping[str, Mapping[str, Any]] | None = None,
    handoffs: Mapping[str, Mapping[str, Any]] | None = None,
    subagents: Mapping[str, Mapping[str, Any]] | None = None,
    input_types: Mapping[str, str] | None = None,
    rows_max: int = ROWS_MAX,
    bytes_max: int = BYTES_MAX,
) -> dict[str, Any]:
    """The session from ``items`` -- ``(offset, AgentEvent)`` pairs, in any order, deduped by
    offset. ``nodes`` maps a ``tool_id`` to ``{"node", "event_id", "fallback"}``, the graph
    node (and the activity's scheduled event) its rows link to; ``handoffs`` maps a subagent's
    workflow id to ``{"node", "event_id"}``, its child node and the event that started it;
    ``subagents`` maps that workflow id to ``{"rows": [...]}`` (:func:`nested_rows` of its own
    session, nested under the handoff) or ``{"reason": ...}`` (a link out, and why).
    ``input_types`` names each handler's input type (a turn's header). ``rows_max`` and
    ``bytes_max`` bound all the session emits, turns and nested rows
    included; the overflow becomes one "N more not shown" row."""
    builder = _Session(
        dict(agent), nodes or {}, handoffs or {}, subagents or {}, rows_max, bytes_max
    )
    builder.input_types = input_types or {}
    seen: set[int] = set()
    for offset, event in sorted(items, key=lambda it: it[0]):
        if offset in seen:  # a re-polled offset is skipped (dedupe by offset)
            continue
        seen.add(offset)
        builder.feed(offset, event)
    return builder.result()


def apply_answers(session: Mapping[str, Any], answers: Mapping[str, Mapping[str, Any]]) -> None:
    """Fold into ``session`` the requests history shows answered by an Update (``answers``:
    ``tool_id`` -> ``{"kind": "approval", "approved"}`` or ``{"kind": "callback", "error"}``),
    for a row whose own resolution event the stream has not delivered yet: it is no longer
    pending, and says how it was answered."""
    labels = {"approval": "approval.requested", "callback": "callback.requested"}
    for turn in session.get("turns") or []:
        for row in turn.get("rows") or []:
            answer = answers.get(row.get("tool_id") or "")
            if (
                answer is None
                or not row.get("pending")
                or row.get("label") != labels.get(answer.get("kind") or "")
            ):
                continue
            row["pending"] = False
            if answer["kind"] == "approval":
                row["resolution"] = "allowed" if answer.get("approved") else "denied"
            else:
                row["resolution"] = "error" if answer.get("error") else "ok"
            row["state"] = "completed" if row["resolution"] in ("allowed", "ok") else "failed"


def nested_rows(session: Mapping[str, Any]) -> list[dict[str, Any]]:
    """A subagent's own session as the rows nested under its handoff, as the wireframe nests
    them: its model calls, its tool calls and its response -- its ``turn.started`` is the
    handoff, and a call's response or result folds into the call's state -- with a row whose
    label and summary repeat the previous one's folded into it, counted (``tool.call
    fetch_cert`` twelve times is one row)."""
    out: list[dict[str, Any]] = []
    repeats = 1
    for turn in session.get("turns") or []:
        for row in turn.get("rows") or []:
            if row.get("label") in _FOLDED_WHEN_NESTED:
                continue
            nested = {
                k: row.get(k)
                for k in ("offset", "glyph", "label", "summary", "state", "at", "type")
                if k in row
            }
            nested["nested"] = True
            if row.get("children"):
                nested["children"] = row["children"]
            last = out[-1] if out else None
            if (
                last is not None
                and (last.get("label"), last.get("base"))
                == (nested.get("label"), nested.get("summary"))
                and not nested.get("children")
            ):
                repeats += 1
                last["summary"] = f"{last['base'] or ''} {_REPEAT}{repeats}".strip()
                last["state"] = nested.get("state") or last.get("state")
                continue
            repeats = 1
            nested["base"] = nested.get("summary")
            out.append(nested)
    for row in out:
        row.pop("base", None)
    return out


class _Turn:
    def __init__(self, turn_id: str | None, turn_number: int | None) -> None:
        self.turn_id = turn_id
        self.turn_number = turn_number
        self.handler: str | None = None
        #: The handler's input type, when the caller knows it (``build``'s ``input_types``).
        self.input_type: str | None = None
        self.input_summary: str | None = None
        self.input_keys: list[str] = []
        self.status = "running"
        self.start_at: float | None = None
        self.rows: list[dict[str, Any]] = []
        self.deltas: list[str] = []
        self.thoughts: list[str] = []


class _Session:
    def __init__(
        self,
        agent: dict[str, Any],
        nodes: Mapping[str, Mapping[str, Any]],
        handoffs: Mapping[str, Mapping[str, Any]],
        subagents: Mapping[str, Mapping[str, Any]],
        rows_max: int,
        bytes_max: int,
    ) -> None:
        self._agent = agent
        self._nodes = nodes
        self.handoff_nodes = handoffs
        self.subagent_views = subagents
        self._rows_max = rows_max
        self._bytes_max = bytes_max
        self._turns: list[_Turn] = []
        #: Open rows that later events fold into, by ``tool_id`` / ``subagent_id``: a
        #: resolution may land in the turn after its request's.
        self.tools: dict[str, dict[str, Any]] = {}
        self.approvals: dict[str, dict[str, Any]] = {}
        self.callbacks: dict[str, dict[str, Any]] = {}
        self.handoffs: dict[str, dict[str, Any]] = {}
        self._by_id: dict[str | None, _Turn] = {}
        self.input_types: Mapping[str, str] = {}
        self._count = 0  # every deduped event, whether it opens a row, folds, or is hidden
        self._last_offset: int | None = None

    def _turn(self, event: Mapping[str, Any]) -> _Turn:
        turn_id = _str(event.get(ae.EVENT_TURN_ID))
        turn = self._by_id.get(turn_id)
        if turn is None:
            turn = _Turn(turn_id, _as_int(event.get(ae.EVENT_TURN_NUMBER)))
            self._by_id[turn_id] = turn
            self._turns.append(turn)
        return turn

    def feed(self, offset: int, event: Mapping[str, Any]) -> None:
        self._last_offset = offset if self._last_offset is None else max(self._last_offset, offset)
        self._count += 1
        raw = event.get(ae.EVENT)
        body: Mapping[str, Any] = raw if isinstance(raw, Mapping) else {}
        kind = _str(body.get(ae.EVENT_TYPE))
        turn = self._turn(event)
        at = _as_float(event.get(ae.EVENT_TIMESTAMP))
        if turn.start_at is None and at is not None:
            turn.start_at = at
        spec = ae.VOCAB.get(kind or "")
        if spec is None:  # an unknown event type: a generic row, never dropped
            summary = _too_large(body) if kind == TOO_LARGE else ""
            label = _cap(kind) if kind else "event"
            self.row(turn, offset, at, ae.GENERIC_GLYPH, label, summary=summary, generic=True)
            return
        _DISPATCH[spec.role](self, turn, offset, at, spec, body)

    # -- row plumbing ----------------------------------------------------------

    def _rel(self, turn: _Turn, at: float | None) -> str | None:
        if at is None or turn.start_at is None:
            return None
        delta = max(0, round(at - turn.start_at))  # both are finite and bounded (_as_float)
        return f"+{delta // 60:02d}:{delta % 60:02d}"

    def row(
        self,
        turn: _Turn,
        offset: int,
        at: float | None,
        glyph: str,
        label: str,
        *,
        summary: str = "",
        state: str | None = None,
        tool_id: str | None = None,
        tool_name: str | None = None,
        generic: bool = False,
        extra: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        summary = _cap(summary)
        row: dict[str, Any] = {
            "offset": offset,
            "type": "unknown" if generic else label,
            "glyph": glyph,
            "label": label,
            "at": at,
            "rt": self._rel(turn, at),
            "summary": summary or None,
            "node": None,
            "state": state,
        }
        if tool_id is not None:
            # Shown capped; matched in full (the registries below hold the whole id). A row
            # whose id was cut cannot be answered: no answer could name it.
            row["tool_id"] = _id(tool_id)
            if len(tool_id) > ID_MAX:
                row["id_capped"] = True
            row["tool_name"] = _cap(tool_name) if tool_name is not None else None
            link = self._nodes.get(tool_id)
            if link is not None and isinstance(link.get("node"), str):
                row["node"] = link["node"]
                if isinstance(link.get("event_id"), int):
                    row["event_id"] = link["event_id"]
                if link.get("fallback"):
                    row["node_fallback"] = True
        if generic:
            row["raw_type"] = label
        if extra:
            row.update(extra)
        turn.rows.append(row)
        return row

    def result(self) -> dict[str, Any]:
        turns: list[dict[str, Any]] = [
            {
                "turn_id": _id(t.turn_id),
                "turn_number": t.turn_number,
                "handler": t.handler,
                "input_type": t.input_type,
                "input_keys": t.input_keys,
                "input_summary": t.input_summary,
                "status": t.status,
                "rows": t.rows,
            }
            for t in self._turns
        ]
        fit = _Fit(self._rows_max, self._bytes_max)
        turns = fit.take(turns, "rows")
        if fit.dropped:
            # Never dropped silently: the overflow is one explicit row on the last turn.
            note: dict[str, Any] = {
                "offset": self._last_offset,
                "type": "truncated",
                "glyph": ae.GENERIC_GLYPH,
                "label": "truncated",
                "summary": f"{fit.dropped} more not shown",
                "node": None,
            }
            if not turns:
                turns.append(
                    {"turn_id": None, "turn_number": None, "handler": None, "input_type": None,
                     "input_keys": [], "input_summary": None, "status": "running", "rows": []}
                )  # fmt: skip
            turns[-1]["rows"].append(note)
        return {
            "version": VERSION,
            "schema": ae.AGENT_EVENTS_SCHEMA,
            "agent": self._agent,
            "turns": turns,
            "footer": {
                "count": self._count,
                "last_offset": self._last_offset,
                "dedupe": "offset",
                "decrypted": "this machine's dev key",
            },
            "stream": ae.TURN_EVENTS_TOPIC,
            "truncated": bool(fit.dropped),
        }


class _Fit:
    """One budget over all a session emits, in the order the page draws it: each turn, each
    row and each row nested under it (a subagent's, at any depth) costs one of ``rows`` and
    its serialized size of ``size``. Once one does not fit, it and everything after it are
    left out and counted, so what is shown is a prefix and the count says what follows."""

    def __init__(self, rows: int, size: int) -> None:
        self.rows = rows
        self.size = size
        self.dropped = 0

    def take(self, items: list[dict[str, Any]], inner: str) -> list[dict[str, Any]]:
        kept: list[dict[str, Any]] = []
        for item in items:
            if self.dropped or self.rows < 1:
                self.dropped += _count(item, inner)
                continue
            own = {k: v for k, v in item.items() if k != inner}
            cost = len(json.dumps(own, separators=(",", ":"), default=str))
            if cost > self.size:
                self.dropped += _count(item, inner)
                continue
            self.rows -= 1
            self.size -= cost
            below = item.get(inner)
            if isinstance(below, list) and below:
                own[inner] = self.take(cast("list[dict[str, Any]]", below), "children")
            elif inner in item:
                own[inner] = below
            kept.append(own)
        return kept


def _count(item: Mapping[str, Any], inner: str) -> int:
    """How many entries ``item`` is: itself and everything nested under it."""
    below = item.get(inner)
    if not isinstance(below, list):
        return 1
    return 1 + sum(
        _count(cast("Mapping[str, Any]", c), "children")
        for c in cast("list[object]", below)
        if isinstance(c, Mapping)
    )


# --------------------------------------------------------------------------- the dispatch
#
# One function per Role (_DISPATCH below).

_Handler = Callable[[_Session, _Turn, int, "float | None", ae.EventRow, Mapping[str, Any]], None]


def _turn_header(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
) -> None:
    handler = _str(body.get("handler"))
    turn.handler = _cap(handler) if handler else None
    named = s.input_types.get(handler or "")
    turn.input_type = _cap(named) if named else None
    payload = body.get("payload")
    if isinstance(payload, Mapping):
        turn.input_keys = [_cap(str(k), _KEY_MAX) for k in list(payload)[:4]]
    turn.input_summary = _input(payload) or None


def _turn_close(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
) -> None:
    if turn.status == "running":
        turn.status = "completed"


def _row_ok(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
) -> None:
    _emit_row(s, turn, offset, at, spec, body, failed=False)


def _row_failed(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
) -> None:
    _emit_row(s, turn, offset, at, spec, body, failed=True)


def _fold_model(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
) -> None:
    kind = _str(body.get(ae.EVENT_TYPE))
    if kind == ae.REPLY_DELTA:
        turn.deltas.append(_str(body.get("text")) or "")
    elif kind == ae.THOUGHT_SUMMARY:
        delta = body.get("delta")
        text = _str(delta.get("text")) if isinstance(delta, Mapping) else _str(delta)
        turn.thoughts.append(text or "")
    # text_annotation: a citation to place beside the text, not text; the body has it.


def _fold_tool(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
) -> None:
    row = s.tools.get(_str(body.get("tool_id")) or "")
    if row is not None and _str(body.get(ae.EVENT_TYPE)) == ae.TOOL_START:
        row["state"] = "working"


def _fold_approval(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
) -> None:
    row = s.approvals.get(_str(body.get("tool_id")) or "")
    if row is None:
        return
    kind = _str(body.get(ae.EVENT_TYPE))
    if kind == ae.TOOL_APPROVAL_RESOLVED:
        approved = body.get("approved") is True
        row["pending"] = False
        row["resolution"] = "allowed" if approved else "denied"
        row["state"] = "completed" if approved else "failed"
    elif kind == ae.AUTO_APPROVAL_EVALUATION_ENDED:
        verdict = _str(body.get("verdict"))
        if verdict:
            row["auto"] = _cap(verdict)


def _fold_callback(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
) -> None:
    row = s.callbacks.get(_str(body.get("tool_id")) or "")
    if row is None:
        return
    outcome = _str(body.get("outcome")) or ("error" if body.get("error") else "ok")
    row["pending"] = False
    row["resolution"] = _cap(outcome)
    row["state"] = "completed" if outcome == "ok" else "failed"


def _subagent_nested(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
) -> None:
    row = s.handoffs.get(_str(body.get("subagent_id")) or "")
    if row is None or row.get("fetched"):  # its own history's rows say more; keep them
        return
    children = row.setdefault("children", [])
    # A nested line links where its handoff does: the subagent's child node.
    node = row.get("node")
    if _str(body.get(ae.EVENT_TYPE)) == ae.SUBAGENT_MESSAGE_SENT:
        handler = _cap(_str(body.get("handler")) or "message")
        children.append(
            {
                "offset": offset,
                "glyph": "→",
                "label": handler,
                "summary": None,
                "node": node,
                "nested": True,
            }
        )
    else:  # subagent_reply_received: the parent's own record of the reply
        outcome = _str(body.get("outcome")) or "ok"
        children.append(
            {
                "offset": offset,
                "glyph": spec.glyph,
                "label": spec.label,
                "summary": _cap(outcome),
                "state": "completed" if outcome == "ok" else "failed",
                "node": node,
                "nested": True,
            }
        )


def _fold_subagent(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
) -> None:
    row = s.handoffs.get(_str(body.get("subagent_id")) or "")
    if row is not None and row.get("state") != "failed":
        row["state"] = "completed"


def _note_subagent(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
) -> None:
    row = s.handoffs.get(_str(body.get("subagent_id")) or "")
    if row is not None:
        row["note"] = _cap(_str(body.get("reason")) or "subagent stream unavailable")


def _hidden(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
) -> None:
    return  # its body is on /api/io?event=; never its own line


_DISPATCH: dict[str, _Handler] = {
    ae.Role.TURN_HEADER: _turn_header,
    ae.Role.TURN_CLOSE: _turn_close,
    ae.Role.ROW: _row_ok,
    ae.Role.ROW_FAILED: _row_failed,
    ae.Role.FOLD_MODEL: _fold_model,
    ae.Role.FOLD_TOOL: _fold_tool,
    ae.Role.FOLD_APPROVAL: _fold_approval,
    ae.Role.FOLD_CALLBACK: _fold_callback,
    ae.Role.SUBAGENT_NESTED: _subagent_nested,
    ae.Role.FOLD_SUBAGENT: _fold_subagent,
    ae.Role.NOTE_SUBAGENT: _note_subagent,
    ae.Role.HIDDEN: _hidden,
}


def _emit_row(
    s: _Session,
    turn: _Turn,
    offset: int,
    at: float | None,
    spec: ae.EventRow,
    body: Mapping[str, Any],
    *,
    failed: bool,
) -> None:
    kind = _str(body.get(ae.EVENT_TYPE))
    tool_id = _str(body.get("tool_id"))
    tool_name = _str(body.get("tool_name"))
    state = "failed" if failed else None
    summary = ""
    extra: dict[str, Any] = {}

    if kind == ae.TURN_STARTED:
        summary = f"input: {turn.input_summary}" if turn.input_summary else ""
    elif kind == ae.MODEL_INTERACTION_STARTED:
        turn.deltas, turn.thoughts = [], []
        summary = _str(body.get("model")) or ""
        state = "working"
    elif kind == ae.MODEL_INTERACTION_ENDED:
        text = "".join(turn.deltas).strip()
        summary = f"“{_cap(text, SUMMARY_MAX - 2)}”" if text else ""
        usage = _usage(body.get("usage"))
        if usage:
            extra["usage"] = usage
        if any(turn.thoughts):
            extra["thought"] = _cap("".join(turn.thoughts))
        turn.deltas, turn.thoughts = [], []
        state = "completed"
        for row in reversed(turn.rows):  # the call it answers is done
            if row.get("label") == "model.call" and row.get("state") == "working":
                row["state"] = "completed"
                break
    elif kind == ae.TOOL_REQUESTED:
        summary = tool_name or "tool"
        hint = _summarize(body.get("tool_input"), spaced=True)
        if hint:
            extra["hint"] = hint
        state = "scheduled"
    elif kind in (ae.TOOL_END, ae.TOOL_ERROR):
        if failed:
            summary = f"{tool_name or 'tool'}: {_str(body.get('message')) or 'failed'}"
        else:
            output = _summarize(body.get("tool_output"))
            summary = f"{tool_name} {output}".strip() if tool_name else output
        state = "failed" if failed else "completed"
        call = s.tools.get(tool_id or "")
        if call is not None:
            call["state"] = state
    elif kind in (ae.TOOL_APPROVAL_REQUESTED, ae.CALLBACK_REQUESTED):
        summary = tool_name or "tool"
        hint = _summarize(body.get("tool_input"), spaced=True)
        if hint:
            extra["hint"] = hint
        state = "working"
        extra["pending"] = True
        extra["resolution"] = None
        if kind == ae.CALLBACK_REQUESTED:  # what the answer must be: its tool's result type
            schema = body.get("output_schema")
            named = (
                schema.get("type") or schema.get("title") if isinstance(schema, Mapping) else None
            )
            extra["result_type"] = _cap(named) if isinstance(named, str) else None
    elif kind == ae.SUBAGENT_STARTED:
        key = _str(body.get("agent_key")) or _str(body.get("subagent_id")) or "subagent"
        summary = f"→ {key}"
        extra["subagent_id"] = _id(body.get("subagent_id"))
        extra["workflow_id"] = _id(body.get("workflow_id"))
        state = "working"
    elif kind in (ae.MESSAGE_HANDLER_END, ae.MESSAGE_HANDLER_ERROR):
        if failed:
            summary = _str(body.get(ae.EVENT_MESSAGE)) or "the handler failed"
        else:
            summary = _summarize(body.get(ae.EVENT_OUTPUT), braces=True)
        state = "failed" if failed else "completed"
        if turn.status == "running":
            turn.status = state

    row = s.row(
        turn,
        offset,
        at,
        spec.glyph,
        spec.label,
        summary=summary,
        state=state,
        tool_id=tool_id,
        tool_name=tool_name,
        extra=extra,
    )
    # Register the row (by the whole id) so later events fold into it.
    if kind == ae.TOOL_REQUESTED and tool_id:
        s.tools[tool_id] = row
    elif kind == ae.TOOL_APPROVAL_REQUESTED and tool_id:
        s.approvals[tool_id] = row
    elif kind == ae.CALLBACK_REQUESTED and tool_id:
        s.callbacks[tool_id] = row
    elif kind == ae.SUBAGENT_STARTED:
        _handoff(s, turn, row, body)


def _handoff(s: _Session, turn: _Turn, row: dict[str, Any], body: Mapping[str, Any]) -> None:
    """The handoff row's child node, and its nested rows: the subagent's own history's when
    the console read it, else a link out (by its opaque workflow id, never parsed) saying
    why -- with the parent stream's message/reply summaries nested meanwhile."""
    subagent_id = _str(body.get("subagent_id"))
    workflow_id = _str(body.get("workflow_id"))
    link = s.handoff_nodes.get(workflow_id or "")
    node = link.get("node") if link is not None else None
    if isinstance(node, str):
        row["node"] = node
        event_id = link.get("event_id") if link is not None else None
        if isinstance(event_id, int):
            row["event_id"] = event_id  # the child's StartChildWorkflowExecutionInitiated
    view = s.subagent_views.get(workflow_id or "")
    rows = view.get("rows") if view is not None else None
    if isinstance(rows, list):
        # The subagent's own rows link where the handoff does: its child node (its own
        # activities are in its own history, not drawn).
        row["children"] = [
            {**cast("Mapping[str, Any]", r), "node": row.get("node")}
            for r in cast("list[object]", rows)
            if isinstance(r, Mapping)
        ]
        row["fetched"] = True
    elif workflow_id and len(workflow_id) <= ID_MAX:  # no longer one is a Temporal workflow id
        row["link"] = workflow_id
        reason = view.get("reason") if view is not None else None
        row["link_reason"] = (
            reason if isinstance(reason, str) else "opens the subagent's own session"
        )
    if subagent_id:
        s.handoffs[subagent_id] = row


# --------------------------------------------------------------------------- summaries


def _input(value: object) -> str:
    """A message's input line: a list is its count (``12 domains``), a flag its name when set
    (and nothing when not), anything else its value, short."""
    if not isinstance(value, Mapping):
        return _summarize(value)
    parts: list[str] = []
    for key, item in cast("Mapping[object, object]", value).items():
        if isinstance(item, list):
            parts.append(f"{len(cast('list[object]', item))} {key}")
        elif isinstance(item, bool):
            if item:
                parts.append(str(key))
        elif item is not None:
            parts.append(f"{key}:{_scalar(item)}")
    return _cap(", ".join(parts))


def _too_large(body: Mapping[str, Any]) -> str:
    size = _as_int(body.get("bytes"))
    shown = f"{size // 1024} KiB" if size is not None else "this item"
    return f"{shown}: too large to inline (open it in Temporal UI)"


def _usage(value: object) -> str | None:
    if not isinstance(value, Mapping):
        return None
    got = _as_int(value.get("input_tokens")), _as_int(value.get("output_tokens"))
    if got[0] is None and got[1] is None:
        return None
    return f"{got[0] or 0}→{got[1] or 0}"


def _summarize(value: object, *, spaced: bool = False, braces: bool = False) -> str:
    """A short, length-capped one-liner for a body value -- never the whole body (seam ⑤:
    verbatim content is on ``/api/io`` alone). A mapping of one list is its count
    (``12 domains``); any other mapping its first fields."""
    if value is None:
        return ""
    if isinstance(value, str):
        return _cap(" ".join(value.split()))
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return _cap(str(value))
    if isinstance(value, Mapping):
        fields: list[tuple[object, object]] = list(value.items())
        first = fields[0][1] if len(fields) == 1 else None
        if isinstance(first, list) and not spaced and not braces:
            return _cap(f"{len(cast('list[object]', first))} {fields[0][0]}")
        sep = ": " if spaced else ":"
        text = ", ".join(f"{k}{sep}{_scalar(v)}" for k, v in fields[:4])
        if len(fields) > 4:
            text += ", …"
        return _cap(f"{{{text}}}" if braces else text)
    if isinstance(value, Sequence):
        return _cap(f"{len(value)} items")  # pyright: ignore[reportUnknownArgumentType]
    return _cap(str(value))


def _scalar(value: object) -> str:
    if isinstance(value, str):
        return value if len(value) <= 24 else value[:23] + "…"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Mapping):
        return "{…}"
    if isinstance(value, Sequence):
        return f"[{len(value)}]"  # pyright: ignore[reportUnknownArgumentType]
    return "…"


def _cap(text: str, limit: int = SUMMARY_MAX) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


# --------------------------------------------------------------------------- small helpers


def _dig(value: Any, *path: str) -> Any:
    for key in path:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)  # pyright: ignore[reportUnknownMemberType]
    return value


def _str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _id(value: object) -> str | None:
    """An id read from an event, as the page is shown it: cut at :data:`ID_MAX` (and not
    stripped: an id is matched whole)."""
    if not isinstance(value, str):
        return None
    return value if len(value) <= ID_MAX else value[: ID_MAX - 1] + "…"


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isascii() and value.isdigit() and len(value) <= 19:
        return int(value)
    return None


def _as_float(value: object) -> float | None:
    """A timestamp: a finite number of seconds within :data:`_AT_MAX` of the epoch, else None
    (JSON as Python reads it carries ``Infinity`` and ``NaN``, which are no time)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, int) and abs(value) > _AT_MAX:
        return None  # before float(): a JSON integer may be too large for one
    at = float(value)
    return at if math.isfinite(at) and abs(at) <= _AT_MAX else None
