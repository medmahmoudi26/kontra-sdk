"""Seam ③: the examples' contract documents, kept as golden files and reproduced byte for
byte from the code (``worker.py``'s bindings, as ``kontra publish`` loads them).

Spec v2 seals no key, so these are the documents every seller of these examples publishes,
not one seller's: the hash here is the hash anyone gets (decisions I7).

A diff here is a contract change: review it, then regenerate with
``UPDATE_GOLDEN=1 pytest tests/examples/test_golden.py``."""

from __future__ import annotations

import os
from typing import Any

import pytest

from kontra.cli._project import load_worker
from kontra.contract import from_yaml, nexgen_view, to_yaml, validate_document

from .support import EXAMPLES, SELLERS

HEADER = (
    "# GOLDEN FILE (seam ③), checked by tests/examples/test_golden.py: the contract `kontra\n"
    "# publish` derives from models.py and offering.yaml. No key is sealed into a contract\n"
    "# (spec x-kontra.v2.json), so this is the document -- and the contract_hash -- that\n"
    "# anyone publishing this example gets.\n"
    "# Regenerate after a deliberate contract change:\n"
    "#   UPDATE_GOLDEN=1 pytest tests/examples/test_golden.py\n"
)


def derive(name: str) -> dict[str, Any]:
    """The document ``kontra publish`` would send for the example, with the example key."""
    with load_worker(EXAMPLES / name) as worker:
        [bound] = worker.offerings
        return bound.build_document()


def golden_text(doc: dict[str, Any]) -> str:
    return HEADER + to_yaml(doc)


@pytest.mark.parametrize("name", SELLERS)
def test_golden_contract(name: str) -> None:
    path = EXAMPLES / name / "contract.yaml"
    expected = golden_text(derive(name)).encode("utf-8")
    if os.environ.get("UPDATE_GOLDEN") == "1":
        path.write_bytes(expected)
    assert path.is_file(), f"run UPDATE_GOLDEN=1 pytest {__file__}"
    assert path.read_bytes() == expected


@pytest.mark.parametrize(
    ("name", "offering"),
    [
        ("enrich", "acme/enrich@1.2.0"),
        ("legacy-actor", "acme/probe@1.0.0"),
        ("hello", "acme/hello@0.1.0"),
        ("chef-agents", "acme/chef-agents@0.1.0"),
        ("chef-claude", "acme/chef-claude@0.1.0"),
    ],
)
def test_golden_contract_passes_the_catalog(name: str, offering: str) -> None:
    """The golden file is a document the catalog accepts: its own sealed hash and the
    catalog's rules (§11.3); its nexgen view has no ``x-kontra``."""
    doc = from_yaml((EXAMPLES / name / "contract.yaml").read_text(encoding="utf-8"))
    info = validate_document(doc)
    assert info.offering.key == offering
    assert info.facts.party == info.offering.party
    assert "public_key" not in doc["x-kontra"]  # spec v2 seals no key (decisions I7)
    assert info.contract_hash == doc["x-kontra"]["contract_hash"]
    assert "x-kontra" not in nexgen_view(doc)


def test_derivation_is_deterministic() -> None:
    for name in SELLERS:
        assert to_yaml(derive(name)) == to_yaml(derive(name))


def test_no_secret_key_lives_in_the_examples() -> None:
    """Nothing under examples/ carries an age identity. Contracts seal no key at all now
    (decisions I7), so there is not even a public one to keep honest -- but a secret one
    committed by accident is still what this guards against."""
    for path in EXAMPLES.rglob("*"):
        if path.is_file() and "__pycache__" not in path.parts:
            assert b"AGE-SECRET-KEY" not in path.read_bytes(), path
