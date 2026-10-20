"""Selling one handler of an agent: ``serve(Agent.handler)`` (decisions I29, kontra.serve.turn).

A real agent written with Temporal's agent harness (``tests/serve/harness_agents.py``), whose
handler needs no model, served by ``serve(ReconAgent.sweep, plugins=[AgentHarnessPlugin()])``
on a real dev server with the fake gate: a hire is one turn, its result the handler's reply;
a handler that raises fails the job, non-retryably, with its message; the agent is closed
after the turn. Seam ⑤: the marker-string scan of the job's wrapper and of the agent child's
histories finds neither the buyer's input nor the output in plaintext
(``tests/codec/test_codec_turn.py``, with the seam's other scans). Then what binding
refuses, the reply selection, and Kontra's copy of the harness's wire shape checked against
the harness itself (the golden pattern: Kontra never imports it at runtime)."""

from __future__ import annotations

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

from kontra.cli import cli as kontra_cli
from kontra.cli._project import load_worker
from kontra.contract.names import agent_workflow_id, job_workflow_id
from kontra.serve import BindingError, offering
from kontra.serve import turn as kturn
from kontra.serve.turn import TURN_ACTIVITY, TURN_WORKFLOW, await_reply
from kontra.testing import Marketplace
from kontra.testing.devserver import DevServer

from .harness_agents import MayHaveData, NeedsData, ReconAgent, Report, local_handler, quote
from .support import MARKER, Buyer, failure_text

HERE = Path(__file__).parent

pytest.importorskip("temporal_agent_harness", reason="the dev group's temporal-agent-harness")


def _offering_yaml(directory: Path) -> Path:
    spec = {
        "name": "acme/recon",
        "version": f"1.{os.getpid() % 100_000}.{uuid.uuid4().int % 10_000}",
        "description": "Sweeps a host, one turn of an agent",
        "max_duration": "10m",
        "review_window": "1h",
        "max_output_bytes": "1MB",
        "artifact_retention": "1d",
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


def _project(root: Path) -> Path:
    _offering_yaml(root)
    (root / "worker.py").write_text(textwrap.dedent(_WORKER).lstrip())
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
    assert "does not draw an agent's handler yet" in result.stderr
    assert "ReconAgent.sweep" in result.stderr


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
        ([], "the agent ended before replying"),
    ],
)
async def test_everything_but_the_reply_fails_the_job_for_good(
    events: list[Any], said: str
) -> None:
    with pytest.raises(ApplicationError) as failed:
        await await_reply(_events(*events), "m1", "t1")
    assert re.search(said, failed.value.message), failed.value.message
    assert failed.value.non_retryable


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
