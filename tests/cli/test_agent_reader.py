"""What the dev console reads of a run's agent (``kontra/cli/_agent.py``, decisions I36), with
no server: the children a history says it started, a batch's progress from its own history
(OD3), a stream's events opened from a sealed poll outcome with a real codec, and a tool row's
link -- exact by the ``tool_id`` in the activity's input, by name and order when that input is
sealed to another key (OD4)."""

from __future__ import annotations

import base64
import json
from typing import Any

from temporalio.api.common.v1 import Payload
from temporalio.converter import PayloadConverter

from kontra import graph
from kontra.cli import _agent, _session
from kontra.codec import KontraCodec
from kontra.codec.age import generate_identity
from tests.graph import synth
from tests.graph.synth import BATCH_FIRST_RUN, BATCH_ID, WORKFLOW_ID

SECRET, PUBLIC = generate_identity()
OTHER_SECRET, OTHER_PUBLIC = generate_identity()


def _codec() -> KontraCodec:
    return KontraCodec(SECRET, [PUBLIC], role="seller")


def _json(payload: Payload) -> dict[str, Any]:
    return {
        "metadata": {k: base64.b64encode(v).decode() for k, v in payload.metadata.items()},
        "data": base64.b64encode(payload.data).decode(),
    }


async def _sealed(value: object, codec: KontraCodec | None = None) -> dict[str, Any]:
    [plain] = PayloadConverter.default.to_payloads([value])
    [sealed] = await (codec or _codec()).encode([plain])
    return _json(sealed)


def _item(offset: int, event: dict[str, Any]) -> dict[str, Any]:
    [inner] = PayloadConverter.default.to_payloads([event])
    return {
        "topic": "turn_events",
        "offset": offset,
        "data": base64.b64encode(inner.SerializeToString()).decode(),
    }


def _event(kind: str, **body: Any) -> dict[str, Any]:
    return {
        "agent_id": "a",
        "turn_id": "t",
        "turn_number": 1,
        "message_id": "m",
        "timestamp": 1.0,
        "event": {"type": kind, **body},
    }


def test_started_children_are_what_the_history_commanded() -> None:
    history = json.loads((synth.FIXTURES_DIR / "child.json").read_text())
    started = _agent.started_children(history)
    assert set(started) == {BATCH_ID, f"{WORKFLOW_ID}/settle"}
    assert started[BATCH_ID][1] == BATCH_FIRST_RUN
    assert all(isinstance(event_id, int) for event_id, _ in started.values())


def test_a_batch_s_progress_is_read_from_its_own_history() -> None:
    history = json.loads((synth.FIXTURES_DIR / "child.json").read_text())
    batch = json.loads((synth.FIXTURES_DIR / "child_batch.json").read_text())
    g = graph.parse(history, children={BATCH_ID: batch})
    _agent.batch_progress(g, {BATCH_ID: batch})
    [node] = [n for n in g["nodes"] if n["name"].startswith("kontra.batch.v1")]
    # Three enrich items done of three, beside its pages= activity (fetch_page, twice).
    assert node["progress"] == {"done": 3, "total": 3, "pages": True}
    assert all("progress" not in n for n in g["nodes"] if n is not node)
    _agent.batch_progress(g, {})  # without its history: as it was
    assert node["progress"] == {"done": 3, "total": 3, "pages": True}


async def test_a_sealed_poll_outcome_opens_to_its_events_and_another_key_s_to_none() -> None:
    poll = {
        "items": [
            _item(0, _event("turn_started")),
            _item(1, _event("reply_delta", text="x" * (_agent._ITEM_B64_MAX))),  # pyright: ignore[reportPrivateUsage]
        ]
    }
    theirs = KontraCodec(OTHER_SECRET, [OTHER_PUBLIC], role="seller")
    history = {
        "events": [
            {
                "eventId": "5",
                "workflowExecutionUpdateAcceptedEventAttributes": {
                    "acceptedRequest": {"input": {"name": "__temporal_workflow_stream_poll"}}
                },
            },
            {
                "eventId": "6",
                "workflowExecutionUpdateCompletedEventAttributes": {
                    "acceptedEventId": "5",
                    "outcome": {"success": {"payloads": [await _sealed(poll)]}},
                },
            },
            {
                "eventId": "7",
                "workflowExecutionUpdateAcceptedEventAttributes": {
                    "acceptedRequest": {"input": {"name": "__temporal_workflow_stream_poll"}}
                },
            },
            {
                "eventId": "8",
                "workflowExecutionUpdateCompletedEventAttributes": {
                    "acceptedEventId": "7",
                    "outcome": {
                        "success": {
                            "payloads": [
                                await _sealed({"items": [_item(9, _event("turn_end"))]}, theirs)
                            ]
                        }
                    },
                },
            },
        ]
    }
    reader = _agent.Reader(_codec)
    items = await reader.items("w", "r", history)
    assert [off for off, _ in items] == [0, 1]  # the other key's outcome: nothing of it
    assert items[0][1] == _event("turn_started")
    assert items[1][1]["event"]["type"] == _session.TOO_LARGE  # over #52's inline cap
    assert await reader.items("w", "r", {"events": []}) == []


def _scheduled(event_id: int, name: str, inputs: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "eventId": str(event_id),
        "activityTaskScheduledEventAttributes": {
            "activityType": {"name": name},
            "input": {"payloads": inputs},
        },
    }


async def test_a_tool_row_links_exactly_by_its_tool_id_else_by_name_and_order() -> None:
    def context(tool_id: str) -> dict[str, Any]:
        return {"tool_id": tool_id, "stream_context": {"turn_id": "t"}}

    theirs = KontraCodec(OTHER_SECRET, [OTHER_PUBLIC], role="seller")
    history = {
        "events": [
            # Two probes in one task, the second's input ours, the first's sealed to another key.
            _scheduled(
                10, "probe", [await _sealed(["a"], theirs), await _sealed(context("p-0"), theirs)]
            ),
            _scheduled(11, "probe", [await _sealed(["b"]), await _sealed(context("p-1"))]),
            _scheduled(
                12, "plan", [_json(PayloadConverter.default.to_payloads([context("plan-0")])[0])]
            ),
        ]
    }
    agent = _agent.AgentRun("job-x/agent", "r", "a", history)
    index = {
        ("job-x/agent", 10): "A/probe",
        ("job-x/agent", 11): "A/probe",
        ("job-x/agent", 12): "A/plan",
    }
    items = [
        (0, _event("tool_requested", tool_id="p-0", tool_name="probe", tool_input={})),
        (1, _event("tool_requested", tool_id="p-1", tool_name="probe", tool_input={})),
        (2, _event("tool_requested", tool_id="plan-0", tool_name="plan", tool_input={})),
        (3, _event("tool_requested", tool_id="lost", tool_name="plan", tool_input={})),
    ]
    links = await _agent.Reader(_codec).tool_links(agent, items, index)
    assert links == {
        "p-1": {"node": "A/probe", "event_id": 11, "fallback": False},
        "p-0": {"node": "A/probe", "event_id": 10, "fallback": True},  # the one left of its name
        "plan-0": {"node": "A/plan", "event_id": 12, "fallback": False},
    }  # and "lost": no activity left to be
