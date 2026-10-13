"""Types-first derivation (§11.1, decisions I2) and the golden contract (seam ③)."""

from __future__ import annotations

import enum
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any, Literal

import pytest
from pydantic import BaseModel, ConfigDict, Field, RootModel

from kontra.contract import (
    ContractError,
    OfferingSpec,
    derive_document,
    to_yaml,
    validate_document,
)
from kontra.types import Artifact

from .conftest import HERE, PUBLIC_KEY
from .sample_models import EnrichInput, EnrichOutput

GOLDEN = HERE / "golden" / "enrich.contract.yaml"


def derive(spec: OfferingSpec, inp: type[Any], out: type[Any] = EnrichOutput) -> dict[str, Any]:
    return derive_document(spec, inp, out, PUBLIC_KEY)


def refused(spec: OfferingSpec, inp: type[Any], out: type[Any] = EnrichOutput) -> ContractError:
    with pytest.raises(ContractError) as info:
        derive(spec, inp, out)
    return info.value


# --------------------------------------------------------------------------- golden


def test_golden_enrich_contract(enrich_doc: dict[str, Any]) -> None:
    """The derivation reproduces the published document byte for byte. Regenerate with
    UPDATE_GOLDEN=1 -- and review the diff: it is a contract change."""
    text = to_yaml(enrich_doc)
    if os.environ.get("UPDATE_GOLDEN") == "1":
        GOLDEN.write_text(text, encoding="utf-8")
    assert GOLDEN.read_text(encoding="utf-8") == text


def test_derivation_is_deterministic(enrich_spec: OfferingSpec, enrich_doc: dict[str, Any]) -> None:
    again = derive(enrich_spec, EnrichInput)
    assert again == enrich_doc
    assert to_yaml(again) == to_yaml(enrich_doc)


def test_document_layout(enrich_doc: dict[str, Any]) -> None:
    assert list(enrich_doc) == ["nexusrpc", "$schema", "services", "$defs", "x-kontra"]
    assert enrich_doc["nexusrpc"] == "1.0.0"
    assert enrich_doc["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    service = enrich_doc["services"]["AcmeEnrich"]
    assert service["fqn"] == "acme.enrich"
    assert service["description"] == "Resolve a URL list to company records"
    ops = service["operations"]
    assert ops["quote"]["fqn"] == "quote"
    assert ops["execute"]["fqn"] == "execute"
    assert ops["quote"]["input"] == ops["execute"]["input"] == {"$ref": "#/$defs/EnrichInput"}
    assert ops["quote"]["output"] == {"$ref": "#/$defs/KontraQuote"}
    assert ops["execute"]["output"] == {"$ref": "#/$defs/EnrichOutput"}
    assert sorted(enrich_doc["$defs"]) == [
        "EnrichInput",
        "EnrichOutput",
        "KontraArtifact",
        "KontraQuote",
    ]
    urls = enrich_doc["$defs"]["EnrichInput"]["properties"]["urls"]
    assert urls == {
        "type": "array",
        "items": {"type": "string", "format": "uri", "minLength": 1, "maxLength": 2048},
        "minItems": 1,
        "maxItems": 10000,
    }
    out = enrich_doc["$defs"]["EnrichOutput"]
    assert out["properties"]["records"] == {"$ref": "#/$defs/KontraArtifact"}
    assert out["properties"]["summary"] == {
        "type": "object",
        "additionalProperties": {"type": "integer"},
    }
    assert out["additionalProperties"] is False


def test_derived_documents_pass_the_catalog(enrich_doc: dict[str, Any]) -> None:
    info = validate_document(enrich_doc)
    assert info.input_type == "EnrichInput"
    assert info.output_type == "EnrichOutput"


# --------------------------------------------------------------------------- the hash lock


class EnrichOutputFloat(BaseModel):
    """Company records as JSON lines, and a count per resolution status."""

    model_config = ConfigDict(extra="forbid")
    records: Artifact
    summary: dict[str, float]  # was dict[str, int]


class EnrichInputReordered(BaseModel):
    """URLs to resolve to company records."""

    model_config = ConfigDict(extra="forbid", title="EnrichInput")
    urls: list[
        Annotated[str, Field(min_length=1, max_length=2048, json_schema_extra={"format": "uri"})]
    ] = Field(min_length=1, max_length=10_000)


def test_changing_a_field_type_changes_the_hash(
    enrich_spec: OfferingSpec, enrich_doc: dict[str, Any]
) -> None:
    """Acceptance 17's basis: serve recomputes this hash from the loaded code and refuses
    to start when it differs from the catalog's."""
    edited = derive(enrich_spec, EnrichInput, EnrichOutputFloat)
    assert edited["x-kontra"]["contract_hash"] != enrich_doc["x-kontra"]["contract_hash"]


def test_changing_a_limit_changes_the_hash(
    enrich_spec: OfferingSpec, enrich_doc: dict[str, Any]
) -> None:
    other = enrich_spec.model_copy(update={"max_concurrent_jobs": 11})
    assert (
        derive(other, EnrichInput)["x-kontra"]["contract_hash"]
        != enrich_doc["x-kontra"]["contract_hash"]
    )


def test_field_order_does_not_change_the_hash(enrich_spec: OfferingSpec) -> None:
    class A(BaseModel):
        x: int
        y: str

    class B(BaseModel):
        y: str
        x: int

    a = derive(enrich_spec, A, EnrichOutput)
    b = derive(enrich_spec, B, EnrichOutput)
    a_body = a["$defs"]["A"]
    b_body = b["$defs"]["B"]
    assert a_body["required"] == b_body["required"] == ["x", "y"]
    assert a_body == b_body


# --------------------------------------------------------------------------- lowering


class Plain(BaseModel):
    n: int


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Cat(BaseModel):
    kind: Literal["cat"]
    lives: int = Field(ge=0, le=9)


class Dog(BaseModel):
    kind: Literal["dog"]
    good: bool = True


class Rich(BaseModel):
    """Everything the lowering handles."""

    model_config = ConfigDict(extra="allow")  # the contract closes it anyway

    when: datetime
    note: str | None = None
    nested: Cat | None = None
    color: Color = Color.RED
    pet: Annotated[Cat | Dog, Field(discriminator="kind")]
    counts: dict[str, int | None] = {}
    tags: list[str] = Field(default_factory=list)
    titled: int = Field(default=1, title="Explicit title", description="kept")
    doc: Artifact | None = None


def test_lowering(enrich_spec: OfferingSpec) -> None:
    doc = derive(enrich_spec, Rich, Rich)
    defs = doc["$defs"]
    rich = defs["Rich"]
    props = rich["properties"]
    assert rich["additionalProperties"] is False  # extra="allow" does not open the contract
    assert "title" not in rich
    assert "title" not in props["when"]
    assert props["note"] == {
        "oneOf": [{"type": "string"}, {"type": "null"}]
    }  # default: null dropped
    assert props["nested"] == {"oneOf": [{"$ref": "#/$defs/Cat"}, {"type": "null"}]}
    assert props["color"] == {
        "type": "string",
        "enum": ["red", "blue"],
        "default": "red",
    }  # inlined
    assert "Color" not in defs
    assert props["pet"] == {
        "oneOf": [{"$ref": "#/$defs/Cat"}, {"$ref": "#/$defs/Dog"}]
    }  # no discriminator
    assert props["counts"] == {
        "type": "object",
        "additionalProperties": {"oneOf": [{"type": "integer"}, {"type": "null"}]},
    }  # composite default dropped
    assert props["tags"] == {"type": "array", "items": {"type": "string"}}
    assert props["titled"] == {
        "type": "integer",
        "default": 1,
        "title": "Explicit title",
        "description": "kept",
    }
    assert props["doc"] == {"oneOf": [{"$ref": "#/$defs/KontraArtifact"}, {"type": "null"}]}
    assert defs["Cat"]["properties"]["kind"] == {"type": "string", "const": "cat"}
    assert rich["required"] == ["pet", "when"]


def test_artifact_def_only_when_used(enrich_spec: OfferingSpec) -> None:
    doc = derive(enrich_spec, Plain, Plain)
    assert sorted(doc["$defs"]) == ["KontraQuote", "Plain"]


def test_artifact_as_the_whole_output(enrich_spec: OfferingSpec) -> None:
    doc = derive(enrich_spec, EnrichInput, Artifact)
    assert doc["services"]["AcmeEnrich"]["operations"]["execute"]["output"] == {
        "$ref": "#/$defs/KontraArtifact"
    }


# --------------------------------------------------------------------------- refusals


class Unioned(BaseModel):
    x: int | str


class Tupled(BaseModel):
    x: tuple[int, str]


class Untyped(BaseModel):
    x: dict  # type: ignore[type-arg]


class AnyValued(BaseModel):
    x: Any


class BareList(BaseModel):
    x: list  # type: ignore[type-arg]


class Bytes(BaseModel):
    x: bytes


class Money_(BaseModel):
    x: Decimal


class Listy(RootModel[list[str]]):
    pass


class KontraThing(BaseModel):
    x: int


class KontraArtifact(BaseModel):
    """Not kontra.types.Artifact, just the same name."""

    key: str


class UsesFakeArtifact(BaseModel):
    a: KontraArtifact


@pytest.mark.parametrize(
    ("model", "path", "fragment"),
    [
        (Unioned, "$defs.Unioned.properties.x", "union"),
        (Tupled, "$defs.Tupled.properties.x", "tuple"),
        (Untyped, "$defs.Untyped.properties.x", "untyped dict"),
        (AnyValued, "$defs.AnyValued.properties.x", "accepts anything"),
        (BareList, "$defs.BareList.properties.x.items", "accepts anything"),
        (Bytes, "$defs.Bytes.properties.x", "bytes"),
        (Money_, "$defs.Money_.properties.x", "Decimal"),
        (Listy, "execute.input", "must be an object"),
        (KontraThing, "$defs.KontraThing", "reserved"),
    ],
)
def test_refusals_name_the_field(
    enrich_spec: OfferingSpec, model: type[Any], path: str, fragment: str
) -> None:
    err = refused(enrich_spec, model)
    assert err.path == path, err
    assert fragment in err.message


def test_a_seller_type_cannot_take_the_artifact_name(enrich_spec: OfferingSpec) -> None:
    err = refused(enrich_spec, UsesFakeArtifact, Plain)
    assert err.path == "$defs.KontraArtifact"
    assert "reserved for kontra.types.Artifact" in err.message
    # beside the real Artifact, pydantic renames the impostor; still refused
    err = refused(enrich_spec, UsesFakeArtifact, EnrichOutput)
    assert err.path.startswith("$defs.KontraArtifact")
    assert "reserved" in err.message


@pytest.mark.parametrize("tp", [str, int, list[str], dict[str, int]])
def test_operation_types_must_be_models(enrich_spec: OfferingSpec, tp: Any) -> None:
    err = refused(enrich_spec, tp)
    assert err.path == "execute.input"
    assert "must be an object" in err.message
    err = refused(enrich_spec, EnrichInput, tp)
    assert err.path == "execute.output"


def test_invalid_public_key(enrich_spec: OfferingSpec) -> None:
    with pytest.raises(ContractError) as info:
        derive_document(enrich_spec, EnrichInput, EnrichOutput, "age1notakey")
    assert info.value.path == "x-kontra.public_key"
    assert "age public key" in info.value.message


def test_golden_is_checked_in() -> None:
    assert Path(GOLDEN).is_file(), "run UPDATE_GOLDEN=1 pytest tests/contract/test_derive.py"


class Aliased(BaseModel):
    kind: str = Field(alias="@type")


class Spaced(BaseModel):
    state: Literal["in progress", "done"]


class BadDefault(BaseModel):
    code: str = Field(default="bye", pattern=r"^h")


@pytest.mark.parametrize(
    ("model", "path", "fragment"),
    [
        (Aliased, '$defs.Aliased.properties["@type"]', "property name"),
        (Spaced, "$defs.Spaced.properties.state.enum[0]", "generated source code"),
        (BadDefault, "$defs.BadDefault.properties.code.default", "does not match"),
    ],
)
def test_derivation_refuses_what_nexgen_cannot_write_safely(
    enrich_spec: OfferingSpec, model: type[Any], path: str, fragment: str
) -> None:
    """Derivation runs the catalog's rules, so a seller learns at ``kontra publish`` time,
    with the field named, instead of every buyer's ``kontra pull`` failing later."""
    err = refused(enrich_spec, model)
    assert err.path == path, err
    assert fragment in err.message
