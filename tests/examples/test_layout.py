"""The examples are where the PRD puts them (§14: ``examples/{enrich,recon,legacy-actor}``),
next to ``examples/hello``, the smallest offering, two agents: ``examples/chef-agents``
(an agent as workflow code) and ``examples/chef-claude`` (an agent that is a process), and
``examples/sweep`` (a fan-out, a batch and a branch: what the graph page draws), and the agent
story (decisions I37): ``examples/recon-harness``, a seller whose entry is one typed handler
of an agent written with Temporal's agent harness. Each
seller is in the §6.1 project layout, its ``worker.py`` exposing the module-level ``worker``
that ``kontra serve`` runs, binding one ``acme/`` offering."""

from __future__ import annotations

import pytest

from kontra.cli._project import load_worker

from .support import EXAMPLES, SELLERS

#: PRD §6.1, plus the README and the golden contract.
SELLER_FILES = {
    "offering.yaml",
    "quote.py",
    "workflows.py",
    "activities.py",
    "models.py",
    "worker.py",
    "README.md",
    "contract.yaml",
}


def test_the_examples() -> None:
    found = {p.name for p in EXAMPLES.iterdir() if p.is_dir() and p.name[0] not in "._"}
    assert found == {
        "enrich",
        "hello",
        "legacy-actor",
        "recon",
        "chef-agents",
        "chef-claude",
        "sweep",
        "recon-harness",
    }
    assert {"recon.py", "main.py", "README.md"} <= {p.name for p in (EXAMPLES / "recon").iterdir()}


@pytest.mark.parametrize(
    ("name", "offering"),
    [
        ("enrich", "acme/enrich"),
        ("legacy-actor", "acme/probe"),
        ("hello", "acme/hello"),
        ("chef-agents", "acme/chef-agents"),
        ("chef-claude", "acme/chef-claude"),
        ("sweep", "acme/sweep"),
        ("recon-harness", "acme/recon-harness"),
    ],
)
def test_seller_layout(name: str, offering: str) -> None:
    directory = EXAMPLES / name
    assert {p.name for p in directory.iterdir() if p.is_file()} >= SELLER_FILES
    with load_worker(directory) as worker:  # loads offering.yaml: no code names allowed in it
        assert [b.offering_id.full_name for b in worker.offerings] == [offering]
    assert name in SELLERS
