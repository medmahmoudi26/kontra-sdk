"""What the dev console reads of a run's agent (``kontra/cli/_agent.py``, decisions I36), with
no server: the children a history says it started, a batch's progress from its own runs
(OD3), a stream's events opened from a sealed poll outcome with a real codec, a tool row's
link -- exact by the ``tool_id`` in the activity's input, by name and order when that input is
sealed to another key (OD4) -- and a handler's input type from the agent's own interface."""

from __future__ import annotations

import base64
import copy
import json
from datetime import timedelta
from typing import Any

from temporalio.api.common.v1 import Payload
from temporalio.common import RawValue
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


class _Runs:
    """A client that holds histories by run id and answers ``agent_interface`` with
    ``interface`` (None: no worker answers)."""

    def __init__(self, runs: dict[str, dict[str, Any]], interface: Payload | None = None) -> None:
        self.runs, self.interface, self.fetched = runs, interface, list[str | None]()

    def get_workflow_handle(self, workflow_id: str, *, run_id: str | None = None) -> Any:
        outer = self

        class Handle:
            async def fetch_history(self) -> Any:
                outer.fetched.append(run_id)
                value = copy.deepcopy(outer.runs[run_id or ""])

                class History:
                    def to_json_dict(self) -> dict[str, Any]:
                        return value

                return History()

            async def query(self, name: str, *, result_type: Any, rpc_timeout: Any) -> Any:
                assert (name, result_type, rpc_timeout) == (
                    "agent_interface",
                    RawValue,
                    timedelta(seconds=3),
                )
                if outer.interface is None:
                    raise RuntimeError("no worker answered")
                return RawValue(outer.interface)

        return Handle()


def _spec(
    activity: str, pages: str | None = None, items: list[Any] | None = None
) -> dict[str, Any]:
    """``kontra.contrib.parallel._Spec`` as the converter writes it (the fields read here)."""
    return {"activity": activity, "pages": pages, "items": items, "max_concurrent": 4}


def _batch_run(run_id: str, spec: dict[str, Any] | None) -> synth.History:
    given = () if spec is None else (spec, {"next": None, "acc": None})
    return synth.History("kontra.batch.v1", workflow_id=BATCH_ID, run_id=run_id, input=given)


def _continue(h: synth.History, run_id: str) -> dict[str, Any]:
    h.add(
        "WorkflowExecutionContinuedAsNew",
        {"newExecutionRunId": run_id, "workflowType": {"name": "kontra.batch.v1"}},
    )
    return h.json()


async def _progress(first: dict[str, Any], runs: dict[str, dict[str, Any]]) -> tuple[Any, Any]:
    g: Any = {
        "nodes": [
            {
                "id": "b",
                "kind": "child",
                "name": "kontra.batch.v1",
                "ref": {"workflow_id": BATCH_ID},
            }
        ],
        "edges": [],
    }
    watched: list[tuple[str, str, int]] = []
    client: Any = _Runs(runs)
    await _agent.Reader(_codec).batch_progress(client, g, {BATCH_ID: first}, watched)
    return g["nodes"][0].get("progress"), watched


async def test_a_batch_s_progress_is_read_from_its_own_history() -> None:
    history = json.loads((synth.FIXTURES_DIR / "child.json").read_text())
    batch = json.loads((synth.FIXTURES_DIR / "child_batch.json").read_text())
    last = _batch_run("0199f0a1-6f00-7000-8000-000000000002", None)
    last.complete(last.task())
    g = graph.parse(history, children={BATCH_ID: batch})
    client: Any = _Runs({"0199f0a1-6f00-7000-8000-000000000002": last.json()})
    await _agent.Reader(_codec).batch_progress(client, g, {BATCH_ID: batch}, [])
    [node] = [n for n in g["nodes"] if n["name"].startswith("kontra.batch.v1")]
    # Its input names nothing (no spec): enrich, the activity it scheduled most, three done of
    # three, beside fetch_page (its pages=, twice); it continued as new into a run that
    # completed, so three is all there were.
    assert node["progress"] == {"done": 3, "failed": 0, "pages": 2, "total": 3, "bound": False}
    assert all("progress" not in n for n in g["nodes"] if n is not node)


async def test_a_batch_s_total_is_its_own_count_not_what_it_scheduled_so_far() -> None:
    """``items=`` of twelve, four at a time: mid-way it has scheduled eight and finished
    seven, and the count is seven of twelve -- the batch's own, from its spec."""
    h = _batch_run("b-1", _spec("probe_one", items=list(range(12))))
    wtc = h.task()
    scheduled = [h.activity(wtc, "probe_one", i) for i in range(8)]
    for s in scheduled[:7]:
        h.finish(s)
    progress, watched = await _progress(h.json(), {})
    assert progress == {"done": 7, "failed": 0, "pages": None, "total": 12, "bound": False}
    assert watched == [(BATCH_ID, "b-1", len(h.events))]  # open: its growth is watched


async def test_a_paged_batch_is_followed_across_continue_as_new() -> None:
    """``pages=``: the total is the items of the pages fetched so far, at least (``7/≥8``),
    summed over every run the batch continued as new into; a failed item is counted."""
    first = _batch_run("b-1", _spec("enrich", pages="fetch_page"))
    wtc = first.task()
    page = first.activity(wtc, "fetch_page", None)
    first.completed(page, first.started(page), synth.payloads({"items": [0, 1, 2], "next": 3}))
    for s in [first.activity(first.task(), "enrich", i) for i in range(3)]:
        first.finish(s)
    second = _batch_run("b-2", _spec("enrich", pages="fetch_page"))
    page = second.activity(second.task(), "fetch_page", 3)
    second.completed(
        page, second.started(page), synth.payloads({"items": [3, 4, 5, 6, 7], "next": 8})
    )
    wtc = second.task()
    items = [second.activity(wtc, "enrich", i) for i in range(3, 7)]
    for s in items[:3]:
        second.finish(s)
    second.failed(
        items[3], second.started(items[3]), {"message": "x"}, "RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED"
    )
    progress, watched = await _progress(_continue(first, "b-2"), {"b-2": second.json()})
    assert progress == {"done": 6, "failed": 1, "pages": 2, "total": 8, "bound": True}
    assert watched == [(BATCH_ID, "b-2", len(second.events))]


async def test_a_batch_sealed_to_another_key_counts_what_it_scheduled() -> None:
    theirs = KontraCodec(OTHER_SECRET, [OTHER_PUBLIC], role="seller")
    spec = await _sealed(_spec("enrich", items=list(range(12))), theirs)
    h = synth.History(
        "kontra.batch.v1", workflow_id=BATCH_ID, run_id="b-1", extra={"input": {"payloads": [spec]}}
    )
    wtc = h.task()
    for s in [h.activity(wtc, "enrich", i) for i in range(4)][:2]:
        h.finish(s)
    progress, _ = await _progress(h.json(), {})
    assert progress == {"done": 2, "failed": 0, "pages": None, "total": 4, "bound": True}


async def test_a_handler_s_input_type_is_the_agent_s_own_interface() -> None:
    interface = [
        {"name": "sweep", "parameters": {"title": "SweepRequest", "type": "object"}},
        {"name": "other", "parameters": {"type": "object"}},  # no title: nothing said
    ]
    [plain] = PayloadConverter.default.to_payloads([interface])
    [sealed] = await _codec().encode([plain])
    agent = _agent.AgentRun("job-x/agent", "r", "a")
    reader = _agent.Reader(_codec)
    client: Any = _Runs({}, sealed)
    assert await reader.input_types(client, agent) == {"sweep": "SweepRequest"}
    client.interface = None  # asked once per run: the answer is kept
    assert await reader.input_types(client, agent) == {"sweep": "SweepRequest"}
    silent: Any = _Runs({})
    assert await _agent.Reader(_codec).input_types(silent, agent) == {}


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
