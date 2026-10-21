"""The ``temporal-agent-harness`` wire shape and event vocabulary, written out once (I36, OD8).

Kontra never imports the harness at runtime (CLAUDE.md, ``docs/decisions.md`` I29): this module
spells the harness's wire shape out as plain constants, and ``tests/serve/test_agent_events.py``
checks every one against the installed harness -- the golden pattern ``tests/serve/test_turn.py``
already uses for the turn wire. Two modules read from here, and neither imports the harness:

* :mod:`kontra.serve.turn`, the bound-handler job wrapper (``serve(Agent.handler)``), which
  delivers one message and reads the reply off the ``turn_events`` stream;
* :mod:`kontra.cli._session`, the dev console's agent-session pane, which renders the same
  ``turn_events`` stream read back from history (``docs/decisions.md`` I36).

So the wire shape is asserted in exactly one place, and the console and the job agree on it by
construction. Imports only the standard library: it is safe inside Temporal's workflow sandbox
(``turn.py`` imports it at module scope).

The event vocabulary (:data:`VOCAB`) maps every one of the harness's :data:`AGENT_EVENT_TYPES`
to how the console renders it -- the eight row labels and glyphs of the owner's wireframe, or a
fold/hidden role. A harness release that adds a type the table does not map fails the vocabulary
test (so a new type never renders as nothing), and an unknown type at runtime renders as a
generic row carrying its raw type rather than being dropped.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "AGENT_ACCEPTS",
    "AGENT_EVENTS_SCHEMA",
    "AGENT_EVENT_TYPES",
    "AGENT_HANDLERS",
    "AGENT_INIT_DATA",
    "AGENT_INTERFACE_QUERY",
    "APPROVAL_APPROVED",
    "APPROVAL_REASON",
    "APPROVAL_REMEMBER",
    "APPROVAL_RESULT_ACCEPTED",
    "APPROVAL_TOOL_ID",
    "AUTO_APPROVAL_EVALUATION_ENDED",
    "AUTO_APPROVAL_EVALUATION_ERROR",
    "AUTO_APPROVAL_EVALUATION_STARTED",
    "AUTO_APPROVAL_EVALUATION_SUPERSEDED",
    "CALLBACK_ERROR",
    "CALLBACK_REQUESTED",
    "CALLBACK_RESOLVED",
    "CALLBACK_RESULT",
    "CALLBACK_RESULT_ACCEPTED",
    "CALLBACK_TOOL_ID",
    "CLOSE_SIGNAL",
    "EVENT",
    "EVENT_AGENT_ID",
    "EVENT_MESSAGE",
    "EVENT_MESSAGE_ID",
    "EVENT_OUTPUT",
    "EVENT_TIMESTAMP",
    "EVENT_TURN_ID",
    "EVENT_TURN_NUMBER",
    "EVENT_TYPE",
    "GENERIC_GLYPH",
    "HARNESS_PIN",
    "INTERFACE_NAME",
    "INTERFACE_PARAMETERS",
    "MESSAGE_ACCEPTED",
    "MESSAGE_HANDLER_END",
    "MESSAGE_HANDLER_ERROR",
    "MESSAGE_HANDLER_START",
    "MESSAGE_PAYLOAD",
    "MESSAGE_TYPE",
    "MODEL_INTERACTION_ENDED",
    "MODEL_INTERACTION_STARTED",
    "PROVIDE_CALLBACK_RESULT_UPDATE",
    "REPLY_DELTA",
    "REPLY_FIELDS",
    "SEND_AGENT_MESSAGE_UPDATE",
    "STATE_PATCH",
    "STATE_SNAPSHOT",
    "SUBAGENT_MESSAGE_SENT",
    "SUBAGENT_REPLY_RECEIVED",
    "SUBAGENT_STARTED",
    "SUBAGENT_STOPPED",
    "SUBAGENT_STREAM_UNAVAILABLE",
    "TEXT_ANNOTATION",
    "THOUGHT_SUMMARY",
    "TOOL_APPROVAL_REQUESTED",
    "TOOL_APPROVAL_RESOLVED",
    "TOOL_APPROVAL_UPDATE",
    "TOOL_END",
    "TOOL_ERROR",
    "TOOL_PROGRESS_DELTA",
    "TOOL_REQUESTED",
    "TOOL_START",
    "TURN_END",
    "TURN_EVENTS_TOPIC",
    "TURN_STARTED",
    "VOCAB",
    "EventRow",
    "Role",
]

# --------------------------------------------------------------------------- the pin
#
# temporal-agent-harness's protocol, as Kontra relies on it. 0.6.0 and 0.7.0 are identical
# for everything below (turn.py's note); the console pins the schema it understands and a
# test fails if the installed harness drifts from the pin.

#: The harness version pinned in the dev group, and asserted by the vocabulary test.
HARNESS_PIN = "0.6.0"
#: The schema the console advertises it understands (``docs/sellers.md``): the dependency is
#: the event schema, not the harness package, so any agent that publishes it earns the pane.
AGENT_EVENTS_SCHEMA = "temporal-agent-harness/0.6"

# --------------------------------------------------------------------------- updates & signals

#: The update that admits one message: ``{type: <handler name>, payload: <input JSON>}``.
SEND_AGENT_MESSAGE_UPDATE = "send_agent_message"
#: The message envelope's fields (``AgentMessage``).
MESSAGE_TYPE = "type"
MESSAGE_PAYLOAD = "payload"
#: The fields of the message update's reply (``AgentMessageReply``).
REPLY_FIELDS = ("turn_number", "turn_id", "message_id", "disposition", "accepted_offset")
#: The signal that ends the agent's session; its ``run`` returns once in-flight turns drain.
CLOSE_SIGNAL = "close"
#: The workflow-stream topic the agent publishes its events on.
TURN_EVENTS_TOPIC = "turn_events"
#: The query that answers the agent's handlers (``AcceptedFunction``s): each one's name and
#: its input's JSON schema, whose ``title`` is the input type (the console's turn header).
AGENT_INTERFACE_QUERY = "agent_interface"
INTERFACE_NAME = "name"
INTERFACE_PARAMETERS = "parameters"

#: The update that answers a tool-approval request, and the fields of its arg
#: (``ToolApprovalDecision``) and its reply (``ToolApprovalResult``).
TOOL_APPROVAL_UPDATE = "tool_approval"
APPROVAL_TOOL_ID = "tool_id"
APPROVAL_APPROVED = "approved"
APPROVAL_REASON = "reason"
APPROVAL_REMEMBER = "remember"
APPROVAL_RESULT_ACCEPTED = "accepted"

#: The update that answers a callback tool, and the fields of its arg (``CallbackResult``)
#: and its reply (``CallbackResultAck``).
PROVIDE_CALLBACK_RESULT_UPDATE = "provide_callback_result"
CALLBACK_TOOL_ID = "tool_id"
CALLBACK_RESULT = "result"
CALLBACK_ERROR = "error"
CALLBACK_RESULT_ACCEPTED = "accepted"

# --------------------------------------------------------------------------- the event envelope

#: The event envelope's fields (``AgentEvent``). ``EVENT`` holds the typed body.
EVENT_AGENT_ID = "agent_id"
EVENT_TURN_ID = "turn_id"
EVENT_TURN_NUMBER = "turn_number"
EVENT_MESSAGE_ID = "message_id"
EVENT_TIMESTAMP = "timestamp"
EVENT = "event"
#: Within the body: its type, and the fields Kontra reads off specific bodies.
EVENT_TYPE = "type"
EVENT_OUTPUT = "output"
EVENT_MESSAGE = "message"

#: What ``@agent.accepts`` stamps on the handler, and ``@agent.defn`` on the class.
AGENT_ACCEPTS = "__agent_accepts__"
AGENT_HANDLERS = "__agent_handlers__"
AGENT_INIT_DATA = "__agent_init_data__"

# --------------------------------------------------------------------------- the vocabulary

#: The ``AgentEventType`` values, as named constants (the console and the job wrapper name
#: the ones they act on; :data:`VOCAB` maps every one). A test asserts these are exactly the
#: installed harness's, so a rename there fails here, not in a buyer's job or a blank pane.
MESSAGE_ACCEPTED = "message_accepted"
MESSAGE_HANDLER_START = "message_handler_start"
MESSAGE_HANDLER_END = "message_handler_end"
MESSAGE_HANDLER_ERROR = "message_handler_error"
TURN_STARTED = "turn_started"
TURN_END = "turn_end"
MODEL_INTERACTION_STARTED = "model_interaction_started"
MODEL_INTERACTION_ENDED = "model_interaction_ended"
TOOL_REQUESTED = "tool_requested"
TOOL_START = "tool_start"
TOOL_PROGRESS_DELTA = "tool_progress_delta"
TOOL_END = "tool_end"
TOOL_ERROR = "tool_error"
TOOL_APPROVAL_REQUESTED = "tool_approval_requested"
TOOL_APPROVAL_RESOLVED = "tool_approval_resolved"
AUTO_APPROVAL_EVALUATION_STARTED = "auto_approval_evaluation_started"
AUTO_APPROVAL_EVALUATION_ENDED = "auto_approval_evaluation_ended"
AUTO_APPROVAL_EVALUATION_SUPERSEDED = "auto_approval_evaluation_superseded"
AUTO_APPROVAL_EVALUATION_ERROR = "auto_approval_evaluation_error"
CALLBACK_REQUESTED = "callback_requested"
CALLBACK_RESOLVED = "callback_resolved"
SUBAGENT_STARTED = "subagent_started"
SUBAGENT_STOPPED = "subagent_stopped"
SUBAGENT_MESSAGE_SENT = "subagent_message_sent"
SUBAGENT_REPLY_RECEIVED = "subagent_reply_received"
SUBAGENT_STREAM_UNAVAILABLE = "subagent_stream_unavailable"
REPLY_DELTA = "reply_delta"
THOUGHT_SUMMARY = "thought_summary"
TEXT_ANNOTATION = "text_annotation"
STATE_SNAPSHOT = "state_snapshot"
STATE_PATCH = "state_patch"


class Role:
    """How the console renders an event. A role decides whether an event opens its own row,
    folds into an earlier one, nests under a handoff, or is hidden (design §5)."""

    #: The turn's input line: ``message_accepted`` becomes the turn header, not a row.
    TURN_HEADER = "turn_header"
    #: Closes the current turn group (its status): ``turn_end``.
    TURN_CLOSE = "turn_close"
    #: A row of its own (glyph + label from :class:`EventRow`).
    ROW = "row"
    #: A failed row of its own (``message_handler_error``, ``tool_error``).
    ROW_FAILED = "row_failed"
    #: Coalesces into the live ``model.call``/``model.response`` row (streamed deltas).
    FOLD_MODEL = "fold_model"
    #: Updates the ``tool.call`` row with the same ``tool_id`` (start → running, progress).
    FOLD_TOOL = "fold_tool"
    #: Updates the ``approval.requested`` row with the same ``tool_id`` (resolution, auto).
    FOLD_APPROVAL = "fold_approval"
    #: Updates the ``callback.requested`` row with the same ``tool_id`` (outcome).
    FOLD_CALLBACK = "fold_callback"
    #: A compact line nested under its handoff (the parent→child message / the child's reply).
    SUBAGENT_NESTED = "subagent_nested"
    #: Closes the handoff row with the same ``subagent_id`` (its status).
    FOLD_SUBAGENT = "fold_subagent"
    #: A dim note under the handoff (a client-only signal; rare in a history-derived pane).
    NOTE_SUBAGENT = "note_subagent"
    #: Shown only when a row is expanded; never its own line (evaluator churn, agent state).
    HIDDEN = "hidden"


@dataclass(frozen=True, slots=True)
class EventRow:
    """One event type's rendering: its :class:`Role`, and -- for a role that shows text -- the
    leading glyph and the fixed label of the owner's wireframe."""

    role: str
    glyph: str = ""
    label: str = ""


# The eight glyphs of the wireframe: ▸ turn/response, ○ model, ▣ tool, ◆ handoff, ▢ approval.
_TURN = "▸"  # ▸
_MODEL = "○"  # ○
_TOOL = "▣"  # ▣
_HANDOFF = "◆"  # ◆
_GATE = "▢"  # ▢

#: Every ``AgentEventType`` → how the console renders it (design §5). The row glyphs and
#: labels are the wireframe's; the folds and the hidden kinds keep the populated pane exactly
#: the wireframe (``message_accepted`` is the turn header, deltas coalesce, evaluator churn
#: and agent state stay off the timeline until a row is expanded).
VOCAB: dict[str, EventRow] = {
    MESSAGE_ACCEPTED: EventRow(Role.TURN_HEADER),
    MESSAGE_HANDLER_START: EventRow(Role.HIDDEN),
    MESSAGE_HANDLER_END: EventRow(Role.ROW, _TURN, "response"),
    MESSAGE_HANDLER_ERROR: EventRow(Role.ROW_FAILED, _TURN, "response"),
    TURN_STARTED: EventRow(Role.ROW, _TURN, "turn.started"),
    TURN_END: EventRow(Role.TURN_CLOSE),
    MODEL_INTERACTION_STARTED: EventRow(Role.ROW, _MODEL, "model.call"),
    MODEL_INTERACTION_ENDED: EventRow(Role.ROW, _MODEL, "model.response"),
    TOOL_REQUESTED: EventRow(Role.ROW, _TOOL, "tool.call"),
    TOOL_START: EventRow(Role.FOLD_TOOL),
    TOOL_PROGRESS_DELTA: EventRow(Role.FOLD_TOOL),
    TOOL_END: EventRow(Role.ROW, _TOOL, "tool.result"),
    TOOL_ERROR: EventRow(Role.ROW_FAILED, _TOOL, "tool.result"),
    TOOL_APPROVAL_REQUESTED: EventRow(Role.ROW, _GATE, "approval.requested"),
    TOOL_APPROVAL_RESOLVED: EventRow(Role.FOLD_APPROVAL),
    AUTO_APPROVAL_EVALUATION_STARTED: EventRow(Role.HIDDEN),
    AUTO_APPROVAL_EVALUATION_ENDED: EventRow(Role.FOLD_APPROVAL),
    AUTO_APPROVAL_EVALUATION_SUPERSEDED: EventRow(Role.HIDDEN),
    AUTO_APPROVAL_EVALUATION_ERROR: EventRow(Role.HIDDEN),
    CALLBACK_REQUESTED: EventRow(Role.ROW, _GATE, "callback.requested"),
    CALLBACK_RESOLVED: EventRow(Role.FOLD_CALLBACK),
    SUBAGENT_STARTED: EventRow(Role.ROW, _HANDOFF, "handoff"),
    SUBAGENT_STOPPED: EventRow(Role.FOLD_SUBAGENT),
    SUBAGENT_MESSAGE_SENT: EventRow(Role.SUBAGENT_NESTED),
    SUBAGENT_REPLY_RECEIVED: EventRow(Role.SUBAGENT_NESTED, _TURN, "response"),
    SUBAGENT_STREAM_UNAVAILABLE: EventRow(Role.NOTE_SUBAGENT),
    REPLY_DELTA: EventRow(Role.FOLD_MODEL),
    THOUGHT_SUMMARY: EventRow(Role.FOLD_MODEL),
    TEXT_ANNOTATION: EventRow(Role.FOLD_MODEL),
    STATE_SNAPSHOT: EventRow(Role.HIDDEN),
    STATE_PATCH: EventRow(Role.HIDDEN),
}

#: Every ``AgentEventType`` value the console maps: exactly :data:`VOCAB`'s keys. The
#: vocabulary test asserts this is the installed harness's whole enum, so a new type is never
#: silently unrendered (at runtime an unmapped type still renders as a generic row).
AGENT_EVENT_TYPES = frozenset(VOCAB)

#: The glyph a generic row (an unknown event type) carries, with the raw type as its label.
GENERIC_GLYPH = "·"  # ·
