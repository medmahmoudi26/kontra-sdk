"""``spec/``: the versioned, language-neutral part of the contract seam.

The reserved definitions must stay field-for-field identical to the Python types they
describe (``kontra.types.Artifact``, ``kontra.contract.KontraQuote``), and the x-kontra
schema to :class:`XKontra`. A change to either side fails here first.
"""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError
from pydantic.json_schema import models_json_schema

from kontra.contract import KontraQuote, OfferingSpec, XKontra
from kontra.contract import spec as spec_mod
from kontra.contract.derive import _Generator, lower_schema  # pyright: ignore[reportPrivateUsage]
from kontra.contract.validate import check_schema_subset
from kontra.types import Artifact

from .conftest import PUBLIC_KEY

ROOT = Path(__file__).resolve().parents[2]


def _without_docs(schema: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in schema.items() if k not in ("description", "title")}


def test_spec_files_are_json_schema_2020_12() -> None:
    for name in (spec_mod.X_KONTRA_FILE, spec_mod.KONTRA_TYPES_FILE):
        data = json.loads((ROOT / "spec" / name).read_text())
        Draft202012Validator.check_schema(data)
        assert data["$schema"] == "https://json-schema.org/draft/2020-12/schema"


def test_the_loader_reads_the_repository_copy() -> None:
    assert spec_mod.spec_dir() == ROOT / "spec"
    assert spec_mod.reserved_names() == {"KontraArtifact", "KontraQuote"}
    a = spec_mod.reserved_def("KontraQuote")
    a["mutated"] = True
    assert "mutated" not in spec_mod.reserved_def("KontraQuote")  # callers get copies
    with pytest.raises(KeyError):
        spec_mod.reserved_def("KontraNothing")


def test_reserved_defs_are_in_nexgens_subset() -> None:
    defs = spec_mod.kontra_types()["$defs"]
    for name, body in defs.items():
        check_schema_subset(body, f"$defs.{name}", defs)
        assert body["type"] == "object"
        assert body["additionalProperties"] is False


def test_kontra_artifact_matches_kontra_types_artifact() -> None:
    """Field for field: the derivation's own lowering of the pydantic model is the spec body."""
    _, generated = models_json_schema([(Artifact, "validation")], schema_generator=_Generator)
    lowered = lower_schema(generated["$defs"]["KontraArtifact"], "$defs.KontraArtifact")
    assert _without_docs(lowered) == _without_docs(spec_mod.reserved_def("KontraArtifact"))
    assert set(Artifact.model_fields) == set(spec_mod.reserved_def("KontraArtifact")["properties"])


def test_kontra_quote_model_matches_the_spec() -> None:
    body = spec_mod.reserved_def("KontraQuote")
    assert set(KontraQuote.model_fields) == set(body["properties"])
    assert [n for n, f in KontraQuote.model_fields.items() if f.is_required()] == body["required"]
    assert body["properties"]["status"]["enum"] == ["ok", "rejected", "busy"]


_SPEC_QUOTE = Draft202012Validator(spec_mod.reserved_def("KontraQuote"))


@pytest.mark.parametrize(
    "data",
    [
        {"status": "ok", "amount_minor": -1, "currency": "USD", "max_duration_seconds": 1},
        {"status": "ok", "amount_minor": 2**53, "currency": "USD", "max_duration_seconds": 1},
        {"status": "ok", "amount_minor": 1, "currency": "usd", "max_duration_seconds": 1},
        {"status": "ok", "amount_minor": 1, "currency": "USD", "max_duration_seconds": 0},
        {"status": "ok", "amount_minor": 1, "currency": "USD", "max_duration_seconds": 2_592_001},
        {"status": "busy", "eta_seconds": 2_592_001},
        {"status": "busy", "eta_seconds": -1},
        {"status": "rejected", "reason": ""},
        {"status": "rejected", "reason": "x" * 1001},
        {"status": "rejected", "reason": "x", "extra": 1},
        {"status": "maybe"},
        {"reason": "x"},
    ],
)
def test_model_and_spec_refuse_the_same_shapes(data: dict[str, Any]) -> None:
    assert not _SPEC_QUOTE.is_valid(data)
    with pytest.raises(ValidationError):
        KontraQuote.model_validate(data)


@pytest.mark.parametrize(
    "quote",
    [
        KontraQuote(status="ok", amount_minor=0, currency="USD", max_duration_seconds=2_592_000),
        KontraQuote(status="rejected", reason="x" * 1000),
        KontraQuote(status="busy", eta_seconds=0, reason="queue"),
    ],
)
def test_model_output_satisfies_the_spec(quote: KontraQuote) -> None:
    assert _SPEC_QUOTE.is_valid(quote.model_dump(mode="json"))


def test_x_kontra_schema_matches_the_model(enrich_spec: OfferingSpec) -> None:
    schema = spec_mod.x_kontra_schema()
    assert set(XKontra.model_fields) == set(schema["properties"])
    required = [n for n, f in XKontra.model_fields.items() if f.is_required()]
    assert sorted(required) == sorted(schema["required"])
    block = XKontra.from_offering(enrich_spec, PUBLIC_KEY, "sha256:" + "0" * 64).to_block()
    Draft202012Validator(schema).validate(block)
    assert XKontra.model_validate(block).to_block() == block


def test_find_spec_dir_prefers_the_wheel_copy(tmp_path: Path) -> None:
    pkg = tmp_path / "site" / "kontra"
    (pkg / "_spec").mkdir(parents=True)
    for name in (spec_mod.X_KONTRA_FILE, spec_mod.KONTRA_TYPES_FILE):
        (pkg / "_spec" / name).write_text("{}")
    assert spec_mod.find_spec_dir(pkg) == pkg / "_spec"


def test_find_spec_dir_falls_back_to_the_checkout(tmp_path: Path) -> None:
    pkg = tmp_path / "repo" / "kontra"
    pkg.mkdir(parents=True)
    (tmp_path / "repo" / "spec").mkdir()
    for name in (spec_mod.X_KONTRA_FILE, spec_mod.KONTRA_TYPES_FILE):
        (tmp_path / "repo" / "spec" / name).write_text("{}")
    assert spec_mod.find_spec_dir(pkg) == tmp_path / "repo" / "spec"


def test_find_spec_dir_fails_loudly(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="incomplete"):
        spec_mod.find_spec_dir(tmp_path / "kontra")


def test_the_distributions_carry_the_spec() -> None:
    """Inside the package (``kontra/_spec``), where :func:`find_spec_dir` looks first: the
    sdist carries it there, and the wheel's build hook maps ``spec/`` there from a checkout
    (``tests/packaging`` checks a real build of both)."""
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())
    targets = config["tool"]["hatch"]["build"]["targets"]
    assert targets["sdist"]["force-include"]["spec"] == "kontra/_spec"
    assert "/spec" not in targets["sdist"]["include"]  # never spec/ at the sdist's top
    assert targets["wheel"]["hooks"]["custom"]["path"] == "hatch_build.py"
    assert '"spec": "kontra/_spec"' in (ROOT / "hatch_build.py").read_text()


def test_x_kontra_hash_has_the_spec_shape(enrich_spec: OfferingSpec) -> None:
    """XKontra is also read on its own (the gate, serve), not only inside
    validate_document, so the model checks what the spec's pattern says."""
    block = XKontra.from_offering(enrich_spec, PUBLIC_KEY).to_block()
    for bad in ["junk", "sha256:" + "0" * 63, "sha256:" + "A" * 64, "sha256:" + "0" * 64 + "\n"]:
        with pytest.raises(ValidationError):
            XKontra.model_validate({**block, "contract_hash": bad})
    assert XKontra.model_validate({**block, "contract_hash": "sha256:" + "0" * 64})
