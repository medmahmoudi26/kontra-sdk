"""``examples/recon-harness`` end to end, on the dev gate (``kontra.testing.Marketplace``, which
runs ``kontra.devgate.DevGate``) with a Temporal dev server: an agent sold as one typed
handler (``ReconAgent.sweep``), its own ``worker.py`` serving as ``acme`` with the seller's
profile, the scripted model answering (no ``LLM_BASE_URL``).

The lifecycle: a buyer quotes and hires it and gets the typed ``SweepReport``; the agent's
events reach the buyer on ``job.stream("turn_events")`` (the topic ``offering.yaml`` lists),
the reply among them; the escrow is released when the buyer accepts; the golden contract is
what the binding derives and equals the harness's schema for the handler, with no difference
(decisions I33); the dev console's ``/api/session`` for the job has its rows (decisions I36);
and the README's graph is this run's -- the turn root, the agent child and its tool activities
(``kontra.graph.mermaid``, the child drawn nested). Then the model behind the worker's switch:
scripted, or an OpenAI-compatible endpoint, which is sent counts and never the domains."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading
from collections.abc import AsyncIterator
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from kontra import graph
from kontra.cli._project import load_worker
from kontra.contract import from_yaml
from kontra.contract import harness as h
from kontra.contract.names import agent_workflow_id, job_workflow_id
from kontra.gate_client import models as m
from kontra.hire import QuoteRejected
from kontra.serve import _agent_events as ae
from tests.cli.session_console import PAGE, consoles, get, row, rows
from tests.graph.test_cli import readme_graph

from .support import EXAMPLES, START, Market, example_module, serving

#: The lifecycle tests run on the package's marketplace, on its event loop.
asynchronous = pytest.mark.asyncio(loop_scope="package")

NAME = "recon-harness"
EXAMPLE = EXAMPLES / NAME
OFFERING = "acme/recon-harness"
#: By ``fetch_cert``'s rule (activities.py: a host's code points summed, mod 7 expired, mod 5
#: weak): www.example.co's certificate has expired, www.example.org's and www.example.edu's
#: are weak, www.example.com's is fine.
DOMAINS = ["example.com", "example.org", "example.co", "example.edu"]
#: The README's block starts with the offering, as every example's graph does.
README_HEADER = "%% acme/recon-harness@0.1.0\n"


async def _collect(items: AsyncIterator[Any]) -> list[Any]:
    return [item async for item in items]


async def _history(market: Market, workflow_id: str) -> dict[str, Any]:
    handle = market.seller_client().get_workflow_handle(workflow_id)
    return (await handle.fetch_history()).to_json_dict()


def run_graph(root: dict[str, Any], root_id: str, agent: dict[str, Any], agent_id: str) -> str:
    """A hired turn's graph as the README draws it: the job's root, ``kontra.turn.v1``, with
    its agent child drawn nested (as the dev console draws it), headed by the offering."""
    g = graph.parse(root, workflow_id=root_id, children={agent_id: agent}, nested={agent_id: None})
    return README_HEADER + graph.mermaid(g, nested=True)


@pytest.mark.temporal
@asynchronous
async def test_a_hire_is_one_turn_of_the_agent(market: Market, tmp_path: Path) -> None:
    report_type = example_module(NAME, "models").SweepReport
    async with serving(market, NAME, tmp_path / f"{NAME}.log"):
        buyer = await market.buyer()
        q = await buyer.k.quote(OFFERING, domains=DOMAINS)
        assert (q.ok, q.offering, q.price, q.currency) == (
            True,
            "acme/recon-harness@0.1.0",
            Decimal("0.04"),  # a cent a domain
            "USD",
        )
        job = await q.hire(max_price=q.price)
        events = asyncio.create_task(_collect(job.stream(ae.TURN_EVENTS_TOPIC, type=dict)))
        result = await job.result(timeout=120, output_type=report_type)
        streamed = await asyncio.wait_for(events, 120)
        view = await job.accept()
        balance = await buyer.balance()
        root = job_workflow_id(job.job_id)
        with consoles() as opened:
            srv = opened.open(market.mp, market.seller)
            page = await asyncio.to_thread(get, srv, f"/api/session?w={root}", **PAGE)
        # The README draws a hire nobody listens to: a job someone listened to also waits, a
        # bounded while, for the relay to have read its stream to the end (a timer).
        quiet = await buyer.k.hire(OFFERING, max_price=q.price, domains=DOMAINS)
        await quiet.result(timeout=120)
        await quiet.accept()
        quiet_root, quiet_agent = job_workflow_id(quiet.job_id), agent_workflow_id(quiet.job_id)
        drawn = run_graph(
            await _history(market, quiet_root),
            quiet_root,
            await _history(market, quiet_agent),
            quiet_agent,
        )

    # The typed report: the scripted model's summary, the tools' counts (the subagent's
    # among them: one expired, two weak, as DOMAINS says).
    assert isinstance(result, report_type)
    assert (result.domains, result.targets, result.expired, result.weak) == (4, 4, 1, 2)
    assert (result.posted, result.confirmed) == (None, None)
    assert result.summary == "done: 4 domains, 4 probed, 3 open, 1 expired, 2 weak"

    # The agent's events, relayed to the buyer: the turn, the model, its tools, the subagent,
    # and last the reply, which is the job's result (the turn mirrors up to it).
    kinds = [e[ae.EVENT][ae.EVENT_TYPE] for e in streamed]
    assert {
        "message_accepted",
        "turn_started",
        "model_interaction_started",
        "reply_delta",
        "tool_requested",
        "tool_start",
        "tool_end",
        "subagent_started",
    } <= set(kinds)
    assert kinds[-1] == ae.MESSAGE_HANDLER_END  # the turn's events up to its reply
    [reply] = [
        e[ae.EVENT] for e in streamed if e[ae.EVENT][ae.EVENT_TYPE] == ae.MESSAGE_HANDLER_END
    ]
    assert reply[ae.EVENT_OUTPUT] == result.model_dump(mode="json")
    tools = [
        (e[ae.EVENT]["tool_name"], e[ae.EVENT]["tool_output"])
        for e in streamed
        if e[ae.EVENT][ae.EVENT_TYPE] == "tool_end"
    ]
    # fetch_cert runs in the subagent, on its own stream.
    assert tools == [("plan", "4 targets"), ("probe", "3/4 open")]

    # The escrow: held at the quoted price, released to the seller on accept.
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.RELEASED)
    assert (balance.available_minor, balance.held_minor) == (START - 4, 0)

    # The dev console: the session pane's rows for the hired turn, read from its agent child.
    assert page.status_code == 200, page.text
    session = page.json()["session"]
    assert (session["agent"]["name"], session["agent"]["case"]) == ("ReconAgent.sweep", "a")
    assert rows(session)
    for tool in ("plan", "probe"):
        assert row(session, "tool.call", tool)["node"] == graph.nested_id("ReconAgent", tool)
    assert row(session, "handoff")["children"]  # the subagent's own rows, nested
    assert row(session, "model.response")["summary"] == "“plan first”"

    # The README's graph is this test's run's.
    if os.environ.get("UPDATE_GOLDEN") == "1":
        readme = EXAMPLE / "README.md"
        text = readme.read_text(encoding="utf-8")
        block = f"```mermaid\n{readme_graph(NAME)}```"
        readme.write_text(text.replace(block, f"```mermaid\n{drawn}```", 1), encoding="utf-8")
    assert drawn == readme_graph(NAME)


@pytest.mark.temporal
@asynchronous
async def test_a_sweep_that_asks_for_a_person_is_refused_at_the_quote(
    market: Market, tmp_path: Path
) -> None:
    """One hire is one turn: an approval or a callback has no one to answer it, so the quote
    refuses them, and nothing is locked."""
    async with serving(market, NAME, tmp_path / f"{NAME}.log"):
        buyer = await market.buyer()
        for asks in ({"want_approval": True}, {"want_callback": True}):
            q = await buyer.k.quote(OFFERING, domains=DOMAINS[:1], **asks)
            assert (q.ok, q.status) == (False, m.QuoteStatus.REJECTED)
            assert q.reason == "one hire is one turn: approvals and callbacks need a session"
            with pytest.raises(QuoteRejected):
                await buyer.k.hire(OFFERING, max_price=1, domains=DOMAINS[:1], **asks)
        assert await buyer.k.jobs() == []
        balance = await buyer.balance()
        assert (balance.available_minor, balance.held_minor) == (START, 0)


def test_the_contract_is_the_harnesses_schema_to_the_last_member() -> None:
    """Decisions I33, for what a seller copies: the golden contract is what the binding
    derives, and the harness's own schema for ``ReconAgent.sweep`` differs from it nowhere
    (both models forbid other keys; every output field is required, without a default)."""
    golden = from_yaml((EXAMPLE / "contract.yaml").read_text(encoding="utf-8"))
    with load_worker(EXAMPLE) as worker:
        [bound] = worker.offerings
        assert (bound.kind, bound.handler, bound.streams) == ("handler", "sweep", ("turn_events",))
        assert bound.document() == golden
        schema = h.harness_schema(h.agent_target(bound.agent), cwd=EXAMPLE)
        assert h.compare_handler(golden, schema, bound.handler) == ()


# --------------------------------------------------------------------------- the model

#: ``think`` run on its own, as the worker runs it: an activity, here in Temporal's test
#: environment, in the example's directory and with the environment the test gives it.
_THINK = """
import asyncio
from temporalio.testing import ActivityEnvironment
from activities import think
from models import Prompt

prompt = Prompt(step="done", notes=["2 domains", "1 expired"])
print(asyncio.run(ActivityEnvironment().run(think, prompt)).model_dump_json())
"""


class StandIn:
    """An OpenAI-compatible chat-completions server on 127.0.0.1 that records each request
    and answers with one sentence."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, dict[str, Any]]] = []
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                stand_in.requests.append((self.path, body))
                answer = json.dumps(
                    {
                        "id": "chatcmpl-stand-in",
                        "object": "chat.completion",
                        "created": 0,
                        "model": body["model"],
                        "choices": [
                            {
                                "index": 0,
                                "message": {"role": "assistant", "content": " Two swept. "},
                                "finish_reason": "stop",
                            }
                        ],
                        "usage": {"prompt_tokens": 31, "completion_tokens": 3, "total_tokens": 34},
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(answer)))
                self.end_headers()
                self.wfile.write(answer)

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}/v1"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def _think(**env: str) -> dict[str, Any]:
    environ = {k: v for k, v in os.environ.items() if not k.startswith(("LLM_", "OPENAI_"))}
    environ.update(env, NO_PROXY="127.0.0.1", no_proxy="127.0.0.1")
    done = subprocess.run(
        [sys.executable, "-c", _THINK],
        cwd=EXAMPLE,
        env=environ,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_without_an_endpoint_the_model_is_scripted() -> None:
    assert _think() == {
        "model": "scripted",
        "text": "done: 2 domains, 1 expired",
        "input_tokens": 1100,
        "output_tokens": 4,
    }


def test_with_an_endpoint_the_model_is_the_workers() -> None:
    """``LLM_BASE_URL`` and ``LLM_MODEL`` in the worker's environment: the model activity asks
    that server, for that model, with the step and its notes (counts, never the domains)."""
    stand_in = StandIn()
    try:
        out = _think(LLM_BASE_URL=stand_in.base_url, LLM_MODEL="recon-test-model")
    finally:
        stand_in.close()
    assert out == {
        "model": "recon-test-model",
        "text": "Two swept.",
        "input_tokens": 31,
        "output_tokens": 3,
    }
    [(path, body)] = stand_in.requests
    assert (path, body["model"]) == ("/v1/chat/completions", "recon-test-model")
    assert body["messages"][-1] == {
        "role": "user",
        "content": "step: done\nnotes: 2 domains; 1 expired",
    }
