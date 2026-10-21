"""Seam ③: the contract does not depend on the kind of entry (decisions I29).

``serve()`` binds a ``@workflow.defn`` class or one ``@agent.accepts`` handler of an agent;
``kontra.serve.offering`` only introspects the entry for its input and output models, and
``kontra.contract`` derives the document from those two models alone. So a handler and a
workflow over the same models, bound to the same ``offering.yaml``, publish the same document
and the same hash: nothing of the agent's envelope (its ``AgentConfig``, its init data, the
harness's injected parameters) reaches what a buyer buys."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from temporalio import workflow

from kontra.contract.canonical import canonical_json
from kontra.serve import offering

pytest.importorskip("temporal_agent_harness", reason="the dev group's temporal-agent-harness")

from tests.serve.harness_agents import ReconAgent, Report, Sweep, quote


@workflow.defn
class SweepFlow:
    """The same sale as ``ReconAgent.sweep``, written as a workflow."""

    @workflow.run
    async def run(self, req: Sweep) -> Report:
        return Report(host=req.host, open=len(req.host))


def _offering_yaml(directory: Path) -> Path:
    spec = {
        "name": "acme/recon",
        "version": "1.0.0",
        "description": "Sweeps a host",
        "max_duration": "10m",
        "review_window": "1h",
        "max_output_bytes": "1MB",
        "artifact_retention": "1d",
    }
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "offering.yaml"
    path.write_text("".join(f"{k}: {json.dumps(v)}\n" for k, v in spec.items()))
    return path


def test_a_handler_and_a_workflow_over_the_same_models_publish_one_contract(
    tmp_path: Path,
) -> None:
    yaml = _offering_yaml(tmp_path)
    by_handler = offering(yaml, entry=ReconAgent.sweep, quote=quote)
    by_workflow = offering(yaml, entry=SweepFlow, quote=quote)
    assert (by_handler.kind, by_workflow.kind) == ("handler", "workflow")
    assert canonical_json(by_handler.document()) == canonical_json(by_workflow.document())
    assert by_handler.contract_hash() == by_workflow.contract_hash()


def test_nothing_of_the_agents_envelope_is_in_the_contract(tmp_path: Path) -> None:
    document = json.dumps(
        offering(_offering_yaml(tmp_path), entry=ReconAgent.sweep, quote=quote).document()
    )
    for envelope in ("AgentConfig", "MessageContext", "approval_policy", "ctx"):
        assert envelope not in document
