"""Golden: Kontra's copy of the temporal-agent-harness wire shape and event vocabulary
(``kontra/serve/_agent_events.py``) equals the installed harness's. The console's session pane
(``docs/decisions.md`` I36) and the turn-job wrapper both read from that module and never
import the harness; a harness release that renames a constant, adds an event type the
vocabulary does not map, or bumps past the pin fails here, not in a buyer's job or a blank
pane. The sibling ``tests/serve/test_turn.py`` checks the same module through ``turn.py``'s
re-exports."""

from __future__ import annotations

import dataclasses
import importlib.metadata as metadata
from typing import Any

import pytest

from kontra.serve import _agent_events as ae

pytest.importorskip("temporal_agent_harness", reason="the dev group's temporal-agent-harness")


def _fields(cls: Any) -> set[str]:
    """A model's field names, whether it is a pydantic model or a dataclass (the harness
    uses both)."""
    if dataclasses.is_dataclass(cls):
        return {f.name for f in dataclasses.fields(cls)}
    return set(cls.model_fields)


def test_the_pin_matches_the_installed_harness_and_the_schema() -> None:
    assert metadata.version("temporal-agent-harness") == ae.HARNESS_PIN
    major, minor, _ = ae.HARNESS_PIN.split(".")
    assert f"temporal-agent-harness/{major}.{minor}" == ae.AGENT_EVENTS_SCHEMA


def test_every_installed_event_type_is_mapped_and_no_stray_type() -> None:
    """The vocabulary covers the harness's whole ``AgentEventType`` enum exactly, so a new
    type never renders as nothing (at runtime an unmapped one still becomes a generic row)."""
    from temporal_agent_harness.harness.agent_protocol import events

    installed = {kind.value for kind in events.AgentEventType}
    assert installed == ae.AGENT_EVENT_TYPES
    assert set(ae.VOCAB) == installed  # VOCAB maps every one


def test_the_update_signal_and_topic_names_are_the_harnesses() -> None:
    from temporal_agent_harness.harness.agent_protocol import agent_interface as ai
    from temporal_agent_harness.harness.agent_protocol import events

    assert ae.SEND_AGENT_MESSAGE_UPDATE == ai.SEND_AGENT_MESSAGE_UPDATE
    assert ae.TOOL_APPROVAL_UPDATE == ai.TOOL_APPROVAL_UPDATE
    assert ae.PROVIDE_CALLBACK_RESULT_UPDATE == ai.PROVIDE_CALLBACK_RESULT_UPDATE
    assert ae.TURN_EVENTS_TOPIC == events.TURN_EVENTS_TOPIC


def test_the_message_and_reply_fields_are_the_harnesses() -> None:
    from temporal_agent_harness.harness.agent_protocol import agent_interface as ai

    assert {ae.MESSAGE_TYPE, ae.MESSAGE_PAYLOAD} <= _fields(ai.AgentMessage)
    assert set(ae.REPLY_FIELDS) == _fields(ai.AgentMessageReply)


def test_the_approval_and_callback_arg_and_reply_fields_are_the_harnesses() -> None:
    """The console sends these as plain dicts (it imports no harness): the field names must
    be the harness's or the Update is refused by the agent."""
    from temporal_agent_harness.harness.agent_protocol import agent_interface as ai

    approval = {ae.APPROVAL_TOOL_ID, ae.APPROVAL_APPROVED, ae.APPROVAL_REASON, ae.APPROVAL_REMEMBER}
    assert approval == _fields(ai.ToolApprovalDecision)
    assert ae.APPROVAL_RESULT_ACCEPTED in _fields(ai.ToolApprovalResult)
    assert {ae.CALLBACK_TOOL_ID, ae.CALLBACK_RESULT, ae.CALLBACK_ERROR} == _fields(
        ai.CallbackResult
    )
    assert ae.CALLBACK_RESULT_ACCEPTED in _fields(ai.CallbackResultAck)


def test_the_event_envelope_and_the_bodies_the_console_reads() -> None:
    from temporal_agent_harness.harness.agent_protocol import events as e

    envelope = set(e.AgentEvent.model_fields)
    assert {
        ae.EVENT_AGENT_ID,
        ae.EVENT_TURN_ID,
        ae.EVENT_TURN_NUMBER,
        ae.EVENT_MESSAGE_ID,
        ae.EVENT_TIMESTAMP,
        ae.EVENT,
    } <= envelope
    # The body fields the renderer reads for summaries and for tool_id -> node linking.
    assert {"handler", "payload"} <= set(e.MessageAccepted.model_fields)
    assert ae.EVENT_OUTPUT in e.MessageHandlerEnd.model_fields
    assert ae.EVENT_MESSAGE in e.MessageHandlerError.model_fields
    assert "model" in e.ModelInteractionStarted.model_fields
    assert {"model", "usage"} <= set(e.ModelInteractionEnded.model_fields)
    assert {"input_tokens", "output_tokens"} <= set(e.TokenUsage.model_fields)
    for body in (e.ToolRequested, e.ToolStartEvent, e.ToolApprovalRequested, e.CallbackRequested):
        assert {"tool_id", "tool_name", "tool_input"} <= set(body.model_fields)
    assert "tool_output" in e.ToolEndEvent.model_fields
    assert "message" in e.ToolErrorEvent.model_fields
    assert {"approved", "reason"} <= set(e.ToolApprovalResolved.model_fields)
    assert {"outcome", "error"} <= set(e.CallbackResolved.model_fields)
    for body in (e.SubagentStarted, e.SubagentMessageSent, e.SubagentReplyReceived):
        assert {"subagent_id", "agent_key", "workflow_id"} <= set(body.model_fields)
    assert "outcome" in e.SubagentReplyReceived.model_fields
    assert "text" in e.ReplyDelta.model_fields
    assert "delta" in e.ThoughtSummaryDelta.model_fields


def test_the_decorator_markers_are_the_harnesses() -> None:
    from temporal_agent_harness.harness import agent_workflow

    assert ae.AGENT_ACCEPTS == agent_workflow._ACCEPTS_MARKER
    assert ae.AGENT_HANDLERS == agent_workflow._HANDLERS_ATTR
    assert ae.AGENT_INIT_DATA == agent_workflow._INIT_DATA_ATTR
    # The close signal is registered by name in the runner (see test_turn.py's lifecycle).
    import inspect

    runner = inspect.getsource(agent_workflow.AgentWorkflowRunner)
    assert f'set_signal_handler("{ae.CLOSE_SIGNAL}"' in runner
