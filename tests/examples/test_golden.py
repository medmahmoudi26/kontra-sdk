"""Seam ③: the examples' contract documents, kept as golden files and reproduced byte for
byte from the code (``worker.py``'s bindings, as ``kontra publish`` loads them), sealed with
the documented example key in ``examples/example-public-key.txt``.

A diff here is a contract change: review it, then regenerate with
``UPDATE_GOLDEN=1 pytest tests/examples/test_golden.py``."""

from __future__ import annotations

import os
from typing import Any

import pytest

from kontra.cli._project import load_worker
from kontra.contract import from_yaml, nexgen_view, to_yaml, validate_document

from .support import EXAMPLES, SELLERS, example_public_key

KEY = example_public_key()
HEADER = (
    "# GOLDEN FILE (seam ③), checked by tests/examples/test_golden.py: the contract `kontra\n"
    "# publish` derives from models.py and offering.yaml, sealed with the EXAMPLE key in\n"
    "# ../example-public-key.txt. Publishing seals your key, so your contract_hash differs.\n"
    "# Regenerate after a deliberate contract change:\n"
    "#   UPDATE_GOLDEN=1 pytest tests/examples/test_golden.py\n"
)


def derive(name: str) -> dict[str, Any]:
    """The document ``kontra publish`` would send for the example, with the example key."""
    with load_worker(EXAMPLES / name) as worker:
        [bound] = worker.offerings
        return bound.build_document(KEY)


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
    ],
)
def test_golden_contract_passes_the_catalog(name: str, offering: str) -> None:
    """The golden file is a document the catalog accepts: its own sealed hash, the catalog's
    rules (§11.3), the example key; its nexgen view has no ``x-kontra``."""
    doc = from_yaml((EXAMPLES / name / "contract.yaml").read_text(encoding="utf-8"))
    info = validate_document(doc)
    assert info.offering.key == offering
    assert info.facts.public_key == KEY
    assert info.contract_hash == doc["x-kontra"]["contract_hash"]
    assert "x-kontra" not in nexgen_view(doc)


def test_derivation_is_deterministic() -> None:
    for name in SELLERS:
        assert to_yaml(derive(name)) == to_yaml(derive(name))


def test_the_example_key_is_public_only() -> None:
    """Only the public half was committed; no secret key lives anywhere in the examples."""
    assert KEY.startswith("age1")
    for path in EXAMPLES.rglob("*"):
        if path.is_file() and "__pycache__" not in path.parts:
            assert b"AGE-SECRET-KEY" not in path.read_bytes(), path
