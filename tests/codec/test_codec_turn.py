"""Seam ⑤ for a bound handler (kontra.serve.turn): a job that is one turn of an agent leaves no
plaintext anywhere a platform can read it.

A real agent (Temporal's agent harness, ``tests/serve/harness_agents.py``) sold with
``serve(ReconAgent.sweep, plugins=[AgentHarnessPlugin()])`` on a real dev server and the fake
gate. The marker-string scan reads both histories the job makes -- Kontra's ``job-<id>``
wrapper (its input and result are encrypted to the buyer and the seller) and the agent child
``job-<id>/agent`` (never job-shaped: the seller's alone; the message update, the stream
polls and their results all live there) -- for the buyer's input, the handler's output and
a handler's failure message. The gate's own record of the job holds no progress event: the
turn activity's heartbeats are Kontra's liveness beats, never forwarded."""

from __future__ import annotations

import base64
import json
import os
import textwrap
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from temporalio.client import Client, NexusOperationFailureError

from kontra.cli._project import load_worker
from kontra.codec import ENCODING
from kontra.contract.names import agent_workflow_id, job_workflow_id
from kontra.gate_client import models as m
from kontra.testing import Marketplace
from kontra.testing.devserver import (
    CLI_ENV,
    DevServer,
    acquire_shared_dev_server,
    release_shared_dev_server,
)

pytest.importorskip("temporal_agent_harness", reason="the dev group's temporal-agent-harness")

pytestmark = [pytest.mark.temporal]

MARKER = "kontra-turn-marker-9c2e"

#: The seller's worker.py: the one file of theirs that imports kontra.
_WORKER = """
from temporal_agent_harness.plugin import AgentHarnessPlugin

from kontra import serve
from tests.serve.harness_agents import ReconAgent, quote

serve(ReconAgent.sweep, quote=quote, plugins=[AgentHarnessPlugin()])
"""


@pytest.fixture(scope="module")
def dev_server() -> Iterator[DevServer]:
    try:
        server = acquire_shared_dev_server()
    except Exception as e:
        if os.environ.get(CLI_ENV):
            raise  # an explicit binary must work
        pytest.skip(f"no Temporal CLI 1.9.1 dev server: {e}")
    try:
        yield server
    finally:
        release_shared_dev_server()


def _project(root: Path) -> Path:
    spec = {
        "name": "acme/recon",
        "version": f"1.{os.getpid() % 100_000}.{uuid.uuid4().int % 10_000}",
        "description": "Sweeps a host, one turn of an agent",
        "max_duration": "10m",
        "review_window": "1h",
        "max_output_bytes": "1MB",
        "artifact_retention": "1d",
    }
    root.mkdir(parents=True)
    (root / "offering.yaml").write_text("".join(f"{k}: {json.dumps(v)}\n" for k, v in spec.items()))
    (root / "worker.py").write_text(textwrap.dedent(_WORKER).lstrip())
    return root


def _payloads(node: Any, path: str = "") -> Iterator[tuple[str, dict[str, Any]]]:
    """Every Payload in a history as JSON (an object with ``metadata`` and ``data``)."""
    if isinstance(node, dict):
        if "metadata" in node and "data" in node and isinstance(node["metadata"], dict):
            yield path, node
            return
        for k, v in node.items():
            yield from _payloads(v, f"{path}.{k}")
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from _payloads(v, f"{path}[{i}]")


async def _scan(client: Client, job_id: str) -> dict[str, int]:
    """The marker scan of the wrapper's and the agent's histories: no marker anywhere, every
    payload Kontra's ciphertext but the plain ``kontra-job`` header (the job id alone).
    Returns how many payloads each history held."""
    encrypted = base64.b64encode(ENCODING).decode()
    counts: dict[str, int] = {}
    for wf_id in (job_workflow_id(job_id), agent_workflow_id(job_id)):
        history = await client.get_workflow_handle(wf_id).fetch_history()
        raw = b"".join(e.SerializeToString() for e in history.events)
        assert MARKER.encode() not in raw, wf_id
        text = history.to_json()
        assert MARKER not in text, wf_id
        counts[wf_id] = 0
        for path, p in _payloads(json.loads(text)):
            if ".header." in path:
                assert set(p["metadata"]) == {"encoding"}, path
                assert json.loads(base64.b64decode(p["data"])) == job_id
                continue
            assert p["metadata"].get("encoding") == encrypted, (wf_id, path, p["metadata"])
            counts[wf_id] += 1
    return counts


async def test_one_turn_leaves_no_plaintext_in_either_history(
    dev_server: DevServer, tmp_path: Path
) -> None:
    from tests.serve.harness_agents import Report
    from tests.serve.support import Buyer

    with load_worker(_project(tmp_path / "p")) as worker:
        [bound] = worker.offerings
        async with Marketplace(dev_server=dev_server) as mp:
            acme = await mp.party("acme")
            bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="100.00")
            async with mp.serve(worker, acme):
                entry = await bob.gate.get_offering(bound.key)
                buyer = Buyer(mp, bob)
                seller = mp.client(acme.namespace)

                value = {"host": f"{MARKER}.example"}
                hired = await buyer.hire(entry, value)
                out = await (await buyer.start(hired, value, result_type=Report)).result()
                assert out.host == f"{MARKER}.example"  # the buyer reads it; nobody else
                counts = await _scan(seller, hired.job_id)
                root, child = job_workflow_id(hired.job_id), agent_workflow_id(hired.job_id)
                assert counts[root] >= 4  # input, the child's input, the activity's, results
                assert counts[child] >= 4  # the message, its admission, the stream polls

                async def events() -> list[Any]:
                    return list(mp.gate.jobs[hired.job_id].events)

                # The gate's own history observations, yes; no heartbeat event: the turn
                # activity's liveness beats never became the buyer's progress.
                sources = {e.source for e in await mp.gate.run(events)}
                assert m.ProgressSource.HEARTBEAT not in sources

                broken = {"host": f"{MARKER}.broken", "fail": True}
                hired = await buyer.hire(entry, broken)
                with pytest.raises(NexusOperationFailureError):
                    await (await buyer.start(hired, broken, result_type=Report)).result()
                await _scan(seller, hired.job_id)  # the failure's message is encrypted too
