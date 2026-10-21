"""Selling one handler of an agent: ``serve(Agent.handler)`` (decisions I29, kontra.serve.turn).

A real agent written with Temporal's agent harness (``tests/serve/harness_agents.py``), whose
handler needs no model, served by ``serve(ReconAgent.sweep, plugins=[AgentHarnessPlugin()])``
on a real dev server with the fake gate: a hire is one turn, its result the handler's reply;
a handler that raises fails the job, non-retryably, with its message; the agent is closed
after the turn; a worker stopped mid-turn costs the job nothing; aliased models travel by
alias both ways. Seam ⑤: the marker-string scan of the job's wrapper and of the agent child's
histories finds neither the buyer's input nor the output in plaintext
(``tests/codec/test_codec_turn.py``, with the seam's other scans). Then what binding
refuses, the reply selection, and Kontra's copy of the harness's wire shape checked against
the harness itself (the golden pattern: Kontra never imports it at runtime)."""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import inspect
import json
import os
import re
import textwrap
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from temporalio.client import NexusOperationFailureError, WorkflowExecutionStatus
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from kontra.cli import cli as kontra_cli
from kontra.cli._project import load_worker
from kontra.codec.stream import open_item
from kontra.contract.names import PROGRESS_TOPIC, agent_workflow_id, job_workflow_id
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.serve import BindingError, offering
from kontra.serve import turn as kturn
from kontra.serve.turn import TURN_ACTIVITY, TURN_WORKFLOW, await_reply
from kontra.testing import Marketplace
from kontra.testing.devserver import DevServer

from .harness_agents import (
    Hop,
    HopAgent,
    Hopped,
    MayHaveData,
    NeedsData,
    ReconAgent,
    Report,
    hop_quote,
    local_handler,
    quote,
)
from .support import MARKER, Buyer, failure_text

HERE = Path(__file__).parent

pytest.importorskip("temporal_agent_harness", reason="the dev group's temporal-agent-harness")


def _offering_yaml(directory: Path, **facts: Any) -> Path:
    spec = {
        "name": "acme/recon",
        "version": f"1.{os.getpid() % 100_000}.{uuid.uuid4().int % 10_000}",
        "description": "Sweeps a host, one turn of an agent",
        "max_duration": "10m",
        "review_window": "1h",
        "max_output_bytes": "1MB",
        "artifact_retention": "1d",
        **facts,
    }
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "offering.yaml"
    path.write_text("".join(f"{k}: {json.dumps(v)}\n" for k, v in spec.items()))
    return path


#: The seller's worker.py: the one file that imports kontra.
_WORKER = """
from temporal_agent_harness.plugin import AgentHarnessPlugin

from kontra import serve
from tests.serve.harness_agents import ReconAgent, quote

serve(ReconAgent.sweep, quote=quote, plugins=[AgentHarnessPlugin()])
"""


def _project(root: Path, worker: str = _WORKER, **facts: Any) -> Path:
    _offering_yaml(root, **facts)
    (root / "worker.py").write_text(textwrap.dedent(worker).lstrip())
    return root


# --------------------------------------------------------------------------- binding


def test_serve_binds_the_handler_and_derives_the_contract_from_its_models(tmp_path: Path) -> None:
    from kontra.serve.turn import TurnWorkflow

    with load_worker(_project(tmp_path / "p")) as worker:
        [bound] = worker.offerings
        # worker.py imports the agent as tests.serve.harness_agents: compared by name, not
        # with this module's own import of it.
        assert (bound.kind, bound.agent_type, bound.handler) == ("handler", "ReconAgent", "sweep")
        assert bound.agent is not None
        assert bound.agent.__qualname__ == "ReconAgent"
        assert bound.workflow is TurnWorkflow  # the job runs Kontra's wrapper
        assert bound.entry_workflow is bound.agent  # the worker registers the agent
        assert (bound.input_type.__name__, bound.output_type.__name__) == ("Sweep", "Report")
        [plugin] = worker.options["plugins"]
        assert plugin.name() == "AgentHarnessPlugin"
        # The contract is the handler's input and output, nothing of the agent's envelope.
        schemas = json.dumps(bound.document())
        assert "Sweep" in schemas
        assert "Report" in schemas
        assert "AgentConfig" not in schemas


def test_an_agent_class_is_refused_with_a_fix_it(tmp_path: Path) -> None:
    with pytest.raises(BindingError, match=r"ReconAgent is an agent: pass the handler.*"):
        offering(_offering_yaml(tmp_path), entry=ReconAgent, quote=quote)


def test_an_agent_that_requires_init_data_is_refused(tmp_path: Path) -> None:
    with pytest.raises(BindingError, match="requires init data"):
        offering(_offering_yaml(tmp_path), entry=NeedsData.sweep, quote=quote)
    # Optional init data is fine: Kontra starts the agent without.
    assert offering(_offering_yaml(tmp_path), entry=MayHaveData.sweep, quote=quote).kind == (
        "handler"
    )


def test_an_agent_defined_in_a_function_is_refused(tmp_path: Path) -> None:
    with pytest.raises(BindingError, match="defined inside a function"):
        offering(_offering_yaml(tmp_path), entry=local_handler(), quote=quote)


def test_a_handler_another_agent_inherits_is_refused(tmp_path: Path) -> None:
    """``ReconV2.sweep is ReconV1.sweep``: the function names ReconV1 (its ``__qualname__``),
    so ``serve(ReconV2.sweep)`` would bind ReconV1 and every job would run the agent the
    seller did not name. Refused for either class, with the fix; a handler a subclass
    overrides is its own, and binds."""
    from tests.serve.inherited_agents import ReconV1, ReconV2
    from tests.serve.inherited_agents import quote as v_quote

    yaml = _offering_yaml(tmp_path)
    for entry in (ReconV2.sweep, ReconV1.sweep):
        with pytest.raises(BindingError) as refused:
            offering(yaml, entry=entry, quote=v_quote)
        said = str(refused.value)
        assert re.search(
            r"ReconV1\.sweep is inherited unchanged by \S*inherited_agents\.ReconV2:", said
        )
        assert "Override sweep in each agent class that sells it" in said
    own = offering(yaml, entry=ReconV2.probe, quote=v_quote)
    assert (own.agent, own.agent_type, own.handler) == (ReconV2, "ReconV2", "probe")
    base = offering(yaml, entry=ReconV1.probe, quote=v_quote)
    assert (base.agent, base.agent_type) == (ReconV1, "ReconV1")


def test_the_message_and_the_reply_are_keyed_by_alias(tmp_path: Path) -> None:
    """The harness validates the message against the handler's model, by alias; it
    publishes the reply by field name, and the job's result is re-keyed by alias -- what a
    workflow entry's result is (temporalio's pydantic converter) and what the buyer reads --
    then checked against the served contract's output (decisions I38)."""
    bound = offering(_offering_yaml(tmp_path), entry=HopAgent.hop, quote=hop_quote)
    job = bound.job_input(Hop.model_validate({"from": "a"}))
    assert job.payload == {"from": "a"}
    Hop.model_validate(job.payload)  # what the harness does with it
    by_name = Hopped.model_validate({"to": "a+1", "via-hops": 1}).model_dump(mode="json")
    assert by_name == {"to_": "a+1", "via_hops": 1}  # what the harness publishes
    contract = bound.validator()
    assert kturn.as_output(Hopped, by_name, contract) == {"to": "a+1", "via-hops": 1}
    wanted = {"to": "a+1", "via-hops": 1}
    assert kturn.as_output(Hopped, wanted, contract) == wanted
    with pytest.raises(
        ApplicationError, match=r"not a Hopped \(output\.via-hops: is missing\)"
    ) as bad:
        kturn.as_output(Hopped, {"to": f"{MARKER}"}, contract)
    assert MARKER not in bad.value.message  # where and what, never the buyer's value
    assert bad.value.non_retryable


def test_a_method_that_is_no_handler_is_refused(tmp_path: Path) -> None:
    async def sweep(req: Any) -> Any: ...

    for entry in (ReconAgent.__init__, sweep, "ReconAgent.sweep"):
        with pytest.raises(BindingError, match=r"@workflow\.defn class.*@agent\.accepts"):
            offering(_offering_yaml(tmp_path), entry=entry, quote=quote)  # type: ignore[arg-type]


def test_kontra_graph_refuses_a_handler_offering(tmp_path: Path) -> None:
    root = _project(tmp_path / "p")
    env = {"KONTRA_HOME": str(tmp_path / "home"), "KONTRA_GATE": None, "KONTRA_API_KEY": None}
    result = CliRunner().invoke(kontra_cli, ["graph", "--dir", str(root), "--runs", "1"], env=env)
    assert result.exit_code == 1, result.output
    assert "an agent's handler has no static graph" in result.stderr
    assert "ReconAgent.sweep" in result.stderr


async def test_a_handler_offering_has_no_static_graph_to_draw(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dev worker serving a handler draws no graph page's static graph (decisions I35),
    and says nothing about it: the page draws what its runs did."""
    from types import SimpleNamespace

    from kontra import KontraPlugin, _graphs
    from kontra.profile import PROFILE_FILE

    started: list[_graphs.Refresh] = []

    def start(self: _graphs.Refresh) -> None:
        started.append(self)

    monkeypatch.setattr(_graphs.Refresh, "start", start)
    home = tmp_path / "home"
    home.mkdir()
    (home / PROFILE_FILE).write_text("{}")
    bound = offering(_offering_yaml(tmp_path), entry=HopAgent.hop, quote=hop_quote)
    plugin = KontraPlugin(bound, project=tmp_path)
    dev = SimpleNamespace(dev=True, home=home)
    drawn = await plugin._draw_graphs(dev, {bound.key: bound.contract_hash()})  # pyright: ignore[reportPrivateUsage, reportArgumentType]
    assert (drawn, started) == (None, [])


# --------------------------------------------------------------------------- one turn, one job


@pytest.fixture
def project(tmp_path: Path) -> Iterator[Any]:
    with load_worker(_project(tmp_path / "p")) as worker:
        yield worker


@pytest.mark.temporal
async def test_a_hire_is_one_turn_of_the_agent(dev_server: DevServer, project: Any) -> None:
    """Hire, the gate starts ``execute``, Kontra's wrapper runs the agent for one turn, and
    the job's result is the handler's reply; the agent is closed (its run completed). The
    seam ⑤ scan of both histories is ``tests/codec/test_codec_turn.py``'s."""
    [bound] = project.offerings
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="100.00")
        async with mp.serve(project, acme):
            entry = await bob.gate.get_offering(bound.key)
            buyer = Buyer(mp, bob)
            value = {"host": f"{MARKER}.example"}
            hired = await buyer.hire(entry, value)
            handle = await buyer.start(hired, value, result_type=Report)
            out = await handle.result()
            assert out == Report(host=f"{MARKER}.example", open=len(f"{MARKER}.example"))

            seller = mp.client(acme.namespace)
            root = job_workflow_id(hired.job_id)
            child = agent_workflow_id(hired.job_id)
            wrapper = await seller.get_workflow_handle(root).describe()
            assert (wrapper.workflow_type, wrapper.status) == (
                TURN_WORKFLOW,
                WorkflowExecutionStatus.COMPLETED,
            )
            agent = await seller.get_workflow_handle(child).describe()
            assert (agent.workflow_type, agent.status) == (
                "ReconAgent",
                WorkflowExecutionStatus.COMPLETED,  # closed after the turn, not terminated
            )
            assert agent.parent_id == root


@pytest.mark.temporal
async def test_a_handler_that_raises_fails_the_job_with_its_message(
    dev_server: DevServer, project: Any
) -> None:
    """The handler's exception ends the turn with ``message_handler_error``: the job fails
    at once (non-retryable, one attempt of the turn activity), its failure carrying the
    handler's message, which travels encrypted like any job failure (seam ⑤:
    ``tests/codec/test_codec_turn.py``)."""
    [bound] = project.offerings
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="100.00")
        async with mp.serve(project, acme):
            entry = await bob.gate.get_offering(bound.key)
            buyer = Buyer(mp, bob)
            value = {"host": f"{MARKER}.broken", "fail": True}
            hired = await buyer.hire(entry, value)
            handle = await buyer.start(hired, value, result_type=Report)
            with pytest.raises(NexusOperationFailureError) as failed:
                await handle.result()
            assert f"cannot sweep {MARKER}.broken" in failure_text(failed.value)
            seller = mp.client(acme.namespace)
            root = job_workflow_id(hired.job_id)
            history = await seller.get_workflow_handle(root).fetch_history()
            attempts = [
                e.activity_task_started_event_attributes.attempt
                for e in history.events
                if e.HasField("activity_task_started_event_attributes")
            ]
            assert attempts == [1]


async def _messages(agent: Any, *, completed: bool = False) -> list[str]:
    """The update ids of the messages the agent admitted (its stream's polls are updates
    too), or of those whose update has completed too."""
    events = (await agent.fetch_history()).events
    admitted = [
        e.workflow_execution_update_accepted_event_attributes.accepted_request.meta.update_id
        for e in events
        if e.HasField("workflow_execution_update_accepted_event_attributes")
        and e.workflow_execution_update_accepted_event_attributes.accepted_request.input.name
        == kturn.SEND_AGENT_MESSAGE_UPDATE
    ]
    if not completed:
        return admitted
    done = {
        e.workflow_execution_update_completed_event_attributes.meta.update_id
        for e in events
        if e.HasField("workflow_execution_update_completed_event_attributes")
    }
    return [u for u in admitted if u in done]


_SLOW_WORKER = """
from temporal_agent_harness.plugin import AgentHarnessPlugin

from kontra import serve
from tests.serve.harness_agents import SlowAgent, quote

serve(SlowAgent.sweep, quote=quote, plugins=[AgentHarnessPlugin()])
"""


async def _until(check: Any, what: str, within: float = 30.0) -> None:
    for _ in range(int(within * 10)):
        if await check():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"not within {within} s: {what}")


@pytest.mark.temporal
async def test_a_worker_stopped_mid_turn_costs_the_job_nothing(
    dev_server: DevServer, tmp_path: Path
) -> None:
    """A deploy: the seller's worker stops gracefully while the handler is still working
    (the turn activity is reading the agent's stream), and a new one starts. temporalio's
    stream client ends its iteration when the shutdown cancels its poll; read as the agent
    having ended, that failed the job for good. Now the activity is retried on the new
    worker, its update id gets the first attempt's admission back (the message is not sent
    twice), and the job delivers the handler's reply."""
    with load_worker(_project(tmp_path / "p", _SLOW_WORKER, streams=["turn_events"])) as worker:
        [bound] = worker.offerings
        async with Marketplace(dev_server=dev_server, relay_interval=0.2) as mp:
            acme = await mp.party("acme")
            bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="100.00")
            seller = mp.client(acme.namespace)
            async with mp.serve(worker, acme):
                entry = await bob.gate.get_offering(bound.key)
                buyer = Buyer(mp, bob)
                value = {"host": "a.example"}
                hired = await buyer.hire(entry, value)
                handle = await buyer.start(hired, value, result_type=Report)
                listener = asyncio.create_task(_turn_events(bob, hired.job_id))
                agent = seller.get_workflow_handle(agent_workflow_id(hired.job_id))

                async def reading() -> bool:
                    try:
                        return bool(await _messages(agent, completed=True))
                    except Exception:
                        return False

                # The message admitted and its update completed: the activity is reading
                # the stream, where a cancelled poll ends the iteration instead of raising.
                await _until(reading, "the message admitted")
                await asyncio.sleep(0.5)
            # Stopped mid-turn: the handler sleeps 6 s on a durable timer.
            async with mp.serve(worker, acme):
                async with asyncio.timeout(60):
                    out = await handle.result()
            assert out == Report(host="a.example", open=len("a.example"))
            root = job_workflow_id(hired.job_id)
            history = await seller.get_workflow_handle(root).fetch_history()
            attempts = [
                e.activity_task_started_event_attributes.attempt
                for e in history.events
                if e.HasField("activity_task_started_event_attributes")
            ]
            assert attempts == [2]  # the first attempt's start is not kept once it retried
            assert len(await _messages(agent)) == 1  # one message, not two
            # The turn's events, mirrored onto the job's stream (decisions I31): the retry
            # mirrored from the first attempt's checkpoint, so none of them twice.
            events = await asyncio.wait_for(listener, 60)
            kinds = [e["event"]["type"] for e in events]
            assert kinds.count("message_accepted") == 1
            assert kinds.count("message_handler_end") == 1
            assert len(kinds) == len({json.dumps(e, sort_keys=True) for e in events})


_HOP_WORKER = """
from temporal_agent_harness.plugin import AgentHarnessPlugin

from kontra import serve
from tests.serve.harness_agents import HopAgent, hop_quote

serve(HopAgent.hop, quote=hop_quote, plugins=[AgentHarnessPlugin()])
"""


@pytest.mark.temporal
async def test_a_handler_with_aliased_models_is_sold_like_a_workflow(
    dev_server: DevServer, tmp_path: Path
) -> None:
    """The buyer sends ``{"from": ...}`` (the contract is by alias); the harness admits the
    message, and the buyer's model reads the result: by alias both ways."""
    with load_worker(_project(tmp_path / "p", _HOP_WORKER)) as worker:
        [bound] = worker.offerings
        async with Marketplace(dev_server=dev_server) as mp:
            acme = await mp.party("acme")
            bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="100.00")
            async with mp.serve(worker, acme):
                entry = await bob.gate.get_offering(bound.key)
                buyer = Buyer(mp, bob)
                value = {"from": "a"}
                hired = await buyer.hire(entry, value)
                handle = await buyer.start(hired, value, result_type=Hopped)
                out = await handle.result()
    assert out == Hopped.model_validate({"to": "a+1", "via-hops": 1})


_STREAMY_WORKER = """
from temporal_agent_harness.plugin import AgentHarnessPlugin

from kontra import serve
from tests.serve.harness_agents import StreamyAgent, quote

serve(StreamyAgent.sweep, quote=quote, plugins=[AgentHarnessPlugin()])
"""


async def _turn_events(party: Any, job_id: str) -> list[dict[str, Any]]:
    """The job's ``turn_events`` as its buyer reads them through the gate, opened."""
    out: list[dict[str, Any]] = []
    async for e in party.gate.stream(job_id, topics=[kturn.TURN_EVENTS_TOPIC]):
        if e.kind is m.StreamEventKind.ITEM:
            ct = base64.b64decode(e.ciphertext_b64)
            p = open_item(ct, [party.identity], topic=e.topic, offset=e.offset, job_id=job_id)
            out.append(json.loads(p.data))
    return out


@pytest.mark.temporal
async def test_listed_topics_are_mirrored_onto_the_jobs_stream_and_nothing_else(
    dev_server: DevServer, tmp_path: Path
) -> None:
    """``streams: [turn_events]``: the agent's events reach the buyer under the same name,
    from the job's own stream (the relay reaches ``job-<id>``, never the agent child); its
    ``notes`` topic is never copied; and the events are in the job's stream before the job
    completes (the turn activity flushes before it returns the reply)."""
    from .harness_agents import NOTES_TOPIC

    project = _project(tmp_path / "p", _STREAMY_WORKER, streams=["turn_events"])
    with load_worker(project) as worker:
        [bound] = worker.offerings
        assert bound.streams == ("turn_events",)
        async with Marketplace(dev_server=dev_server, relay_interval=0.2) as mp:
            acme = await mp.party("acme")
            bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="100.00")
            async with mp.serve(worker, acme):
                entry = await bob.gate.get_offering(bound.key)
                assert entry.streams == ["turn_events"]
                buyer = Buyer(mp, bob)
                value = {"host": f"{MARKER}.example"}
                hired = await buyer.hire(entry, value)
                handle = await buyer.start(hired, value, result_type=Report)
                listener = asyncio.create_task(_turn_events(bob, hired.job_id))
                out = await handle.result()
                events = await asyncio.wait_for(listener, 60)
                with pytest.raises(GateError) as refused:
                    async for _ in bob.gate.stream(hired.job_id, topics=[NOTES_TOPIC]):
                        pass
                assert refused.value.code == "unknown_topic"

                async def stored() -> list[m.StreamEvent]:
                    return list(mp.gate.jobs[hired.job_id].events)

                kept = await mp.gate.run(stored)
                relay = await mp.gate.run(_relay_state(mp, hired.job_id))
    assert out.host == f"{MARKER}.example"
    kinds = [e["event"]["type"] for e in events]
    assert "message_handler_end" in kinds
    reply = next(e for e in events if e["event"]["type"] == "message_handler_end")
    assert reply["event"]["output"] == {"host": f"{MARKER}.example", "open": len(out.host)}
    assert {e.topic for e in kept if e.topic is not None} <= {"turn_events", PROGRESS_TOPIC}
    assert relay.dropped == 0  # nothing unlisted in the job's log for the relay to drop
    assert "private note" not in json.dumps(events)


def _relay_state(mp: Marketplace, job_id: str) -> Any:
    async def get() -> Any:
        return mp.gate.jobs[job_id].relay

    return get


async def test_the_mirror_publishes_only_listed_topics_past_its_checkpoint() -> None:
    """A unit view of the mirror: listed topics only, nothing at or before the checkpoint
    (an earlier attempt's), the checkpoint moved by a flush and carried by every beat."""
    from temporalio.api.common.v1 import Payload

    class Topic:
        def __init__(self, sink: list[tuple[str, Payload]], name: str) -> None:
            self.sink, self.name = sink, name

        def publish(self, value: Payload) -> None:
            self.sink.append((self.name, value))

    class Job:
        def __init__(self) -> None:
            self.published: list[tuple[str, Payload]] = []
            self.flushes = 0

        def topic(self, name: str) -> Topic:
            return Topic(self.published, name)

        async def flush(self) -> None:
            self.flushes += 1

    job = Job()
    mirror = kturn._Mirror(job, frozenset({"turn_events"}), after=4)  # type: ignore[arg-type]  # pyright: ignore[reportPrivateUsage]
    p = Payload(metadata={"encoding": b"json/plain"}, data=b"{}")
    for topic, offset in (("turn_events", 3), ("turn_events", 4), ("notes", 5), ("turn_events", 6)):
        mirror.offer(topic, offset, p)
    assert [(t, v.data) for t, v in job.published] == [("turn_events", b"{}")]
    assert mirror.details() == (4,)
    beats: list[tuple[Any, ...]] = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *d: beats.append(d)
    await env.run(mirror.flush)
    assert (job.flushes, mirror.flushed, beats) == (1, 6, [(6,)])
    await env.run(mirror.flush)
    assert job.flushes == 1  # nothing new: no signal


# --------------------------------------------------------------------------- the reply


async def _events(*events: Any) -> AsyncIterator[object]:
    for e in events:
        yield e


def _event(kind: str, message_id: str | None = "m1", turn_id: str = "t1", **body: Any) -> Any:
    return {
        "agent_id": "a1b2c3",
        "turn_id": turn_id,
        "turn_number": 1,
        "message_id": message_id,
        "timestamp": 0.0,
        "event": {"type": kind, **body},
    }


async def test_the_reply_is_our_messages_handler_end() -> None:
    out = await await_reply(
        _events(
            _event("message_accepted"),
            _event("message_handler_end", message_id="other", output={"not": "ours"}),
            "not an envelope",
            _event("message_handler_start"),
            _event("message_handler_end", output={"host": "h", "open": 1}),
        ),
        "m1",
        "t1",
    )
    assert out == {"host": "h", "open": 1}


@pytest.mark.parametrize(
    ("events", "said"),
    [
        ([_event("message_handler_error", message="boom")], "boom$"),
        (
            [_event("tool_approval_requested")],
            "asked for a tool approval: a Kontra job is one turn",
        ),
        ([_event("callback_requested")], "asked for a callback: .*kontra-sdk#60"),
        ([_event("turn_end", message_id=None)], "turn ended without a reply"),
        ([_event("message_handler_end", output=[1])], "not an object"),
    ],
)
async def test_everything_but_the_reply_fails_the_job_for_good(
    events: list[Any], said: str
) -> None:
    with pytest.raises(ApplicationError) as failed:
        await await_reply(_events(*events), "m1", "t1")
    assert re.search(said, failed.value.message), failed.value.message
    assert failed.value.non_retryable


async def test_the_stream_ending_first_is_not_a_failure_of_its_own() -> None:
    """temporalio's stream client ends its iteration -- it does not raise -- when the agent
    ends, when its poll times out, and when the reading is cancelled: the end alone says
    nothing, and is not the job's failure."""
    with pytest.raises(kturn.StreamEnded):
        await await_reply(_events(_event("message_accepted")), "m1", "t1")


async def _cut_by_a_cancel() -> AsyncIterator[object]:
    """What ``WorkflowStreamClient.subscribe`` does when its poll is cancelled: it returns."""
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        return
    yield  # pragma: no cover


async def _cut_by_shutdown() -> AsyncIterator[object]:
    """The stream ending while the worker stops (its poll timed out, say)."""
    from temporalio import activity

    await activity.wait_for_worker_shutdown()
    return
    yield  # pragma: no cover


async def test_a_cancelled_or_stopping_activity_is_cancelled_not_failed() -> None:
    """In the activity, the stream ending is read against the activity's own state: a
    cancellation (the wrapper's, or a stopping worker's -- which temporalio reports as a
    retryable worker-shutdown failure) stays a cancellation, so the turn is retried on the
    next worker; any other end is a retryable failure. Never the job's final failure: the
    wrapper decides that, by seeing the agent end."""

    async def read(events: AsyncIterator[object]) -> dict[str, Any]:
        return await kturn.read_reply(events, "m1", "t1")

    env = ActivityEnvironment()
    reading = asyncio.ensure_future(env.run(read, _cut_by_a_cancel()))
    await asyncio.sleep(0.05)
    env.cancel()
    with pytest.raises(asyncio.CancelledError):
        await reading

    env = ActivityEnvironment()
    reading = asyncio.ensure_future(env.run(read, _cut_by_shutdown()))
    await asyncio.sleep(0.05)
    env.worker_shutdown()
    with pytest.raises(asyncio.CancelledError):
        await reading

    with pytest.raises(ApplicationError) as ended:
        await ActivityEnvironment().run(read, _events(_event("message_accepted")))
    assert not ended.value.non_retryable
    assert "ended before its reply" in ended.value.message


async def test_another_turns_end_is_not_ours() -> None:
    out = await await_reply(
        _events(
            _event("turn_end", message_id=None, turn_id="t0"),
            _event("message_handler_end", output={"ok": True}),
        ),
        "m1",
        "t1",
    )
    assert out == {"ok": True}


# --------------------------------------------------------------------------- the wire shape


def test_kontras_copy_of_the_wire_shape_is_the_harnesses() -> None:
    """Golden: every name Kontra relies on (kontra/serve/turn.py) equals the harness's own.
    A harness release that renames one fails here, not in a buyer's job."""
    from temporal_agent_harness.harness import agent_workflow
    from temporal_agent_harness.harness.agent_protocol import agent_interface, events

    assert kturn.SEND_AGENT_MESSAGE_UPDATE == agent_interface.SEND_AGENT_MESSAGE_UPDATE
    assert kturn.TURN_EVENTS_TOPIC == events.TURN_EVENTS_TOPIC
    kinds = events.AgentEventType
    assert kinds.MESSAGE_HANDLER_END.value == kturn.MESSAGE_HANDLER_END
    assert kinds.MESSAGE_HANDLER_ERROR.value == kturn.MESSAGE_HANDLER_ERROR
    assert kinds.TURN_END.value == kturn.TURN_END
    assert kinds.TOOL_APPROVAL_REQUESTED.value == kturn.TOOL_APPROVAL_REQUESTED
    assert kinds.CALLBACK_REQUESTED.value == kturn.CALLBACK_REQUESTED
    # The message envelope and the update's reply.
    message = agent_interface.AgentMessage
    assert {kturn.MESSAGE_TYPE, kturn.MESSAGE_PAYLOAD} <= _fields(message)
    assert set(kturn.REPLY_FIELDS) == _fields(agent_interface.AgentMessageReply)
    # The event envelope, and the bodies Kontra reads.
    envelope = events.AgentEvent.model_fields
    assert {kturn.EVENT_MESSAGE_ID, kturn.EVENT_TURN_ID, kturn.EVENT} <= set(envelope)
    assert kturn.EVENT_OUTPUT in events.MessageHandlerEnd.model_fields
    assert kturn.EVENT_MESSAGE in events.MessageHandlerError.model_fields
    # What the decorators stamp.
    assert kturn.AGENT_ACCEPTS == agent_workflow._ACCEPTS_MARKER
    assert kturn.AGENT_HANDLERS == agent_workflow._HANDLERS_ATTR
    assert kturn.AGENT_INIT_DATA == agent_workflow._INIT_DATA_ATTR
    assert getattr(ReconAgent.sweep, kturn.AGENT_ACCEPTS) is True
    assert "sweep" in getattr(ReconAgent, kturn.AGENT_HANDLERS)
    # The close signal is registered by name in the runner (the lifecycle test shows the
    # agent completing once Kontra sends it).
    runner = inspect.getsource(agent_workflow.AgentWorkflowRunner)
    assert f'set_signal_handler("{kturn.CLOSE_SIGNAL}"' in runner


def _fields(cls: Any) -> set[str]:
    if dataclasses.is_dataclass(cls):
        return {f.name for f in dataclasses.fields(cls)}
    return set(cls.model_fields)


def test_kontra_imports_no_harness() -> None:
    """Wire shape only: no module of the kontra package imports the harness."""
    import kontra

    package = Path(kontra.__file__).parent
    for path in package.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "import temporal_agent_harness" not in text, path
        assert "from temporal_agent_harness" not in text, path


def test_the_turn_names_are_kontras() -> None:
    assert TURN_WORKFLOW.startswith("kontra.")
    assert TURN_ACTIVITY.startswith(TURN_WORKFLOW)
