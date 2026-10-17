"""``examples/chef-agents`` end to end: an agent as workflow code (OpenAI's Agents SDK on
Temporal's integration), its own ``worker.py`` serving as ``acme`` with ``LLM_BASE_URL``
pointing at an OpenAI-compatible stand-in on 127.0.0.1. The model activity dials the stand-in,
for the model the worker named; the pantry tool runs as an activity between the two model
calls; the dish is delivered; and nothing in the worker reaches for any other host -- not
api.openai.com -- with no OPENAI_API_KEY anywhere.

The stand-in answers as a model that calls the tool once and then cooks with what it got:
what a real model cooks is not tested here (point ``LLM_BASE_URL`` at one for that).

And the other half of kontra-sdk#27: a worker gets the agents plugin, and its lenient payload
converter, exactly when its workflows import ``agents`` -- ``examples/hello`` does not, so it
gets neither and needs no OpenAI key."""

from __future__ import annotations

import importlib.util
import json
import os
import socketserver
import subprocess
import sys
import threading
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from kontra.gate_client import models as m

from .support import EXAMPLES, Market, Seller, seller_env

AGENTS = importlib.util.find_spec("temporalio.openai_agents") is not None
OFFERING = "acme/chef-agents"
#: The model the worker names (``OPENAI_DEFAULT_MODEL``): the workflow names none.
MODEL = "chef-test-model"


class StandIn:
    """An OpenAI-compatible chat-completions server on 127.0.0.1 that records every request:
    the first answer calls ``lookup_pantry`` on the fridge, the next one cooks with what the
    tool said."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, dict[str, Any]]] = []
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                stand_in.requests.append((self.path, body))
                answer = json.dumps(stand_in.answer(body)).encode()
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

    @staticmethod
    def answer(body: dict[str, Any]) -> dict[str, Any]:
        said = [msg["content"] for msg in body["messages"] if msg.get("role") == "tool"]
        if said:
            message: dict[str, Any] = {
                "role": "assistant",
                "content": f"Lemon spaghetti, from the fridge: {said[-1]}",
            }
        else:
            call = {"name": "lookup_pantry", "arguments": json.dumps({"shelf": "fridge"})}
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [{"id": "call_1", "type": "function", "function": call}],
            }
        return {
            "id": "chatcmpl-stand-in",
            "object": "chat.completion",
            "created": 0,
            "model": body["model"],
            "choices": [
                {
                    "index": 0,
                    "message": message,
                    "finish_reason": "stop" if said else "tool_calls",
                }
            ],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


class Tripwire:
    """The worker's HTTP proxy, which no request should reach: everything but 127.0.0.1 and
    localhost (``NO_PROXY``) would come here, be recorded, and go no further."""

    def __init__(self) -> None:
        self.seen: list[bytes] = []
        tripwire = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                tripwire.seen.append(self.rfile.readline(4096))

        self._server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def env(self) -> dict[str, str]:
        proxy = f"http://127.0.0.1:{self._server.server_address[1]}"
        names = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY")
        out = {n: proxy for name in names for n in (name, name.lower())}
        out.update(NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost")
        return out

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


@pytest.mark.temporal
@pytest.mark.asyncio(loop_scope="package")
async def test_the_model_activity_dials_the_workers_endpoint(
    market: Market, tmp_path: Path
) -> None:
    if not AGENTS:
        pytest.skip("the [agents] extra is not installed")
    stand_in, tripwire = StandIn(), Tripwire()
    env = seller_env(market.seller.home, market.sites)
    assert not any(name.startswith("OPENAI_") for name in env)  # no key, no base URL
    env.update(LLM_BASE_URL=stand_in.base_url, OPENAI_DEFAULT_MODEL=MODEL, **tripwire.env())
    seller = Seller("chef-agents", env, tmp_path / "chef-agents.log")
    market.sellers["chef-agents"] = seller  # its log's tail if this fails
    seller.start()
    try:
        await seller.wait_ready(market.seller_client())
        buyer = await market.buyer()
        q = await buyer.k.quote(OFFERING, craving="something quick with eggs")
        assert (q.ok, q.offering, q.price) == (True, "acme/chef-agents@0.1.0", Decimal("0.05"))
        job = await q.hire(max_price="0.05")
        result = await job.result(timeout=120)
        view = await job.accept()
    finally:
        await seller.stop()
        stand_in.close()
        tripwire.close()

    # The dish is the stand-in's, cooked with what the lookup_pantry activity returned.
    assert result.recipe.startswith("Lemon spaghetti, from the fridge: ")
    assert "parmesan" in result.recipe
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.RELEASED)
    # Two model activities dialled the worker's endpoint, for the worker's model: the tool
    # call, then the dish after the tool's answer.
    assert [path for path, _ in stand_in.requests] == ["/v1/chat/completions"] * 2
    assert {body["model"] for _, body in stand_in.requests} == {MODEL}
    first, second = (body["messages"] for _, body in stand_in.requests)
    assert "tool" not in {msg["role"] for msg in first}
    assert second[-1]["role"] == "tool"
    assert "something quick with eggs" in json.dumps(first)
    assert tripwire.seen == []  # no other host: not api.openai.com, not its traces


_PROBE = """
import json, sys
from kontra import _agents
from kontra.cli._project import load_worker
with load_worker(".") as worker:
    plugins = worker.options.get("plugins") or []
print(json.dumps({
    "agents": "agents" in sys.modules,
    "plugins": [type(p).__name__ for p in plugins],
    "converter": _agents.payload_converter_class(plugins).__name__,
}))
"""


@pytest.mark.parametrize(
    ("name", "runs_an_agent"),
    [
        ("hello", False),
        ("enrich", False),
        ("legacy-actor", False),
        ("chef-claude", False),
        ("chef-agents", True),
    ],
)
def test_a_worker_runs_the_agents_plugin_iff_its_workflows_import_agents(
    name: str, runs_an_agent: bool
) -> None:
    """Each example's worker, built as ``kontra serve`` builds it, in a fresh process with no
    ``OPENAI_*`` in its environment. ``examples/hello`` (and every seller but chef-agents):
    no ``agents`` import, no plugin, temporalio's pydantic converter, no key needed.
    ``examples/chef-agents``, which imports ``agents``: the plugin and its converter, and no
    key needed either -- the endpoint is read at the first model call."""
    if runs_an_agent and not AGENTS:
        pytest.skip("the [agents] extra is not installed")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("OPENAI_", "KONTRA_"))}
    result = subprocess.run(
        [sys.executable, "-c", _PROBE],
        cwd=EXAMPLES / name,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    built = json.loads(result.stdout.strip().splitlines()[-1])
    if runs_an_agent:
        assert built == {
            "agents": True,
            "plugins": ["OpenAIAgentsPlugin"],
            "converter": "OpenAIPayloadConverter",
        }
    else:
        assert built == {"agents": False, "plugins": [], "converter": "PydanticPayloadConverter"}
