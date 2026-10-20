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
dropped silently, and the caps (:data:`ROWS_MAX`, :data:`BYTES_MAX`) collapse the overflow into
one explicit "N more not shown" row.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

from kontra.contract.names import STREAM_POLL_UPDATE
from kontra.serve import _agent_events as ae

__all__ = [
    "BYTES_MAX",
    "ROWS_MAX",
    "SUMMARY_MAX",
    "TOO_LARGE",
    "VERSION",
    "build",
    "events_in_poll_result",
    "nested_rows",
    "stream_poll_outcomes",
]

#: The JSON version the page reads.
VERSION = 1
#: A server-derived one-liner never carries a whole body: it is capped here.
SUMMARY_MAX = 120
#: At most this many rows across all turns; the rest is one "N more not shown" row.
ROWS_MAX = 5000
#: At most this many bytes of summaries per snapshot; the rest is that same row.
BYTES_MAX = 4 * 1024 * 1024
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
    handoffs: Mapping[str, str] | None = None,
    subagents: Mapping[str, Mapping[str, Any]] | None = None,
    rows_max: int = ROWS_MAX,
    bytes_max: int = BYTES_MAX,
) -> dict[str, Any]:
    """The session from ``items`` -- ``(offset, AgentEvent)`` pairs, in any order, deduped by
    offset. ``nodes`` maps a ``tool_id`` to ``{"node", "event_id", "fallback"}``, the graph
    node (and the activity's scheduled event) its rows link to; ``handoffs`` maps a subagent's
    workflow id to its child node; ``subagents`` maps that workflow id to ``{"rows": [...]}``
    (:func:`nested_rows` of its own session, nested under the handoff) or ``{"reason": ...}``
    (a link out, and why). ``rows_max`` and ``bytes_max`` bound the pane; the overflow becomes
    one "N more not shown" row."""
    builder = _Session(
        dict(agent), nodes or {}, handoffs or {}, subagents or {}, rows_max, bytes_max
    )
    seen: set[int] = set()
    for offset, event in sorted(items, key=lambda it: it[0]):
        if offset in seen:  # a re-polled offset is skipped (dedupe by offset)
            continue
        seen.add(offset)
        builder.feed(offset, event)
    return builder.result()


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
        handoffs: Mapping[str, str],
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
        self._count = 0  # every deduped event, whether it opens a row, folds, or is hidden
        self._rows = 0
        self._bytes = 0
        self._last_offset: int | None = None
        self._dropped = 0

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
            self.row(
                turn, offset, at, ae.GENERIC_GLYPH, kind or "event", summary=summary, generic=True
            )
            return
        _DISPATCH[spec.role](self, turn, offset, at, spec, body)

    # -- row plumbing ----------------------------------------------------------

    def _rel(self, turn: _Turn, at: float | None) -> str | None:
        if at is None or turn.start_at is None:
            return None
        delta = max(0, round(at - turn.start_at))
        return f"+{delta // 60:02d}:{delta % 60:02d}"

    def _budget(self, summary: str) -> bool:
        """True while a new row still fits; once not, each further row is counted."""
        size = len(summary.encode())
        if self._dropped or self._rows >= self._rows_max or self._bytes + size > self._bytes_max:
            self._dropped += 1
            return False
        self._rows += 1
        self._bytes += size
        return True

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
    ) -> dict[str, Any] | None:
        summary = _cap(summary)
        if not self._budget(summary):
            return None
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
            row["tool_id"] = tool_id
            row["tool_name"] = tool_name
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
                "turn_id": t.turn_id,
                "turn_number": t.turn_number,
                "handler": t.handler,
                "input_keys": t.input_keys,
                "input_summary": t.input_summary,
                "status": t.status,
                "rows": t.rows,
            }
            for t in self._turns
        ]
        if self._dropped:
            # Never dropped silently: the overflow is one explicit row on the last turn.
            note: dict[str, Any] = {
                "offset": self._last_offset,
                "type": "truncated",
                "glyph": ae.GENERIC_GLYPH,
                "label": "truncated",
                "summary": f"{self._dropped} more not shown",
                "node": None,
            }
            self._turns[-1].rows.append(note)
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
            "truncated": bool(self._dropped),
        }


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
    turn.handler = _str(body.get("handler"))
    payload = body.get("payload")
    if isinstance(payload, Mapping):
        turn.input_keys = [str(k) for k in list(payload)[:4]]
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
            row["auto"] = verdict


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
    row["resolution"] = outcome
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
    if _str(body.get(ae.EVENT_TYPE)) == ae.SUBAGENT_MESSAGE_SENT:
        handler = _str(body.get("handler")) or "message"
        children.append(
            {"offset": offset, "glyph": "→", "label": handler, "summary": None, "nested": True}
        )
    else:  # subagent_reply_received: the parent's own record of the reply
        outcome = _str(body.get("outcome")) or "ok"
        children.append(
            {
                "offset": offset,
                "glyph": spec.glyph,
                "label": spec.label,
                "summary": outcome,
                "state": "completed" if outcome == "ok" else "failed",
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
    elif kind == ae.SUBAGENT_STARTED:
        key = _str(body.get("agent_key")) or _str(body.get("subagent_id")) or "subagent"
        summary = f"→ {key}"
        extra["subagent_id"] = _str(body.get("subagent_id"))
        extra["workflow_id"] = _str(body.get("workflow_id"))
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
    if row is None:
        return
    # Register the row so later events fold into it.
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
    node = s.handoff_nodes.get(workflow_id or "")
    if node is not None:
        row["node"] = node
    view = s.subagent_views.get(workflow_id or "")
    rows = view.get("rows") if view is not None else None
    if isinstance(rows, list):
        row["children"] = [dict(r) for r in rows if isinstance(r, Mapping)]
        row["fetched"] = True
    elif workflow_id:
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


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isascii() and value.isdigit() and len(value) <= 19:
        return int(value)
    return None


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None
