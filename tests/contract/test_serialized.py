"""Seam ③: the contract describes what a buyer receives (decisions I38, kontra-sdk#63).

The input is derived as pydantic validates it, the output as pydantic serializes it -- what
temporalio's pydantic converter writes for a workflow entry's result, and what a bound
handler's result is (``as_output``: the harness's ``model_dump(mode="json")``, re-keyed by
alias). For every case of the zoo, both results are the model's ``model_dump(mode="json",
by_alias=True)``, validate against the published contract, and read back through
``kontra.hire``'s decoding."""

from __future__ import annotations

import json
import re
from decimal import Decimal
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict, Field
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.exceptions import ApplicationError

from kontra.contract import (
    ContractError,
    InputValidator,
    OfferingSpec,
    derive_document,
    validate_document,
)
from kontra.contract.serialized import by_alias
from kontra.hire._record import wrap  # pyright: ignore[reportPrivateUsage]
from kontra.serve.turn import as_output

from .zoo import (
    CASES,
    Camel,
    Computed,
    ComputedAlias,
    Defaults,
    Excluded,
    Leaf,
    Nested,
    Optionals,
    Plain,
    Serialized,
    SplitAlias,
    Tagged,
)

MARKER = "kontra-serialized-marker-7c2e"


def derive(spec: OfferingSpec, inp: type[Any], out: type[Any] | None = None) -> dict[str, Any]:
    return derive_document(spec, inp, inp if out is None else out)


def _defs(doc: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in doc["$defs"].items() if k not in ("KontraQuote", "KontraArtifact")}


# --------------------------------------------------------------------------- every case


@pytest.mark.parametrize("case", sorted(CASES))
def test_what_a_buyer_receives_is_what_the_contract_describes(
    enrich_spec: OfferingSpec, case: str
) -> None:
    model, value = CASES[case]
    instance = model.model_validate(value)
    info = validate_document(derive(enrich_spec, model))
    contract = InputValidator(info)
    contract.validate(value)  # the input is what pydantic validates
    wanted = instance.model_dump(mode="json", by_alias=True)

    # a workflow entry's result: what temporalio's pydantic converter writes
    [payload] = PydanticPayloadConverter().to_payloads([instance])
    from_workflow = json.loads(payload.data)
    assert from_workflow == wanted
    contract.validate_output(from_workflow)

    # a bound handler's: the harness publishes model_dump(mode="json"), by field name
    from_handler = as_output(model, instance.model_dump(mode="json"), contract)
    assert from_handler == wanted

    # the buyer reads it: kontra.hire validates a delivery and wraps it by the schema
    assert wrap(wanted, info.output_schema).to_dict() == wanted


# --------------------------------------------------------------------------- the derivation


def test_the_output_is_derived_in_serialization_mode(enrich_spec: OfferingSpec) -> None:
    """Computed fields present and required, excluded fields absent, keyed by serialization
    alias, typed by the serializer's return type; the input of the same model is
    pydantic's validation, as before."""
    computed = _defs(derive(enrich_spec, Computed))
    assert computed["ComputedOutput"]["properties"]["area"] == {"type": "integer"}  # no readOnly
    assert computed["ComputedOutput"]["required"] == ["area", "h", "w"]
    assert "area" not in computed["ComputedInput"]["properties"]

    aliased = _defs(derive(enrich_spec, ComputedAlias))
    assert aliased["ComputedAliasOutput"]["required"] == ["double-w", "w"]

    excluded = _defs(derive(enrich_spec, Excluded))
    assert list(excluded["ExcludedOutput"]["properties"]) == ["keep"]
    assert excluded["ExcludedInput"]["required"] == ["keep", "secret"]

    split = _defs(derive(enrich_spec, SplitAlias))
    assert list(split["SplitAliasInput"]["properties"]) == ["in_a"]
    assert list(split["SplitAliasOutput"]["properties"]) == ["out_a"]

    serialized = _defs(derive(enrich_spec, Serialized))
    assert serialized["SerializedInput"]["properties"]["at"] == {
        "type": "string",
        "format": "date-time",
    }
    assert serialized["SerializedOutput"]["properties"]["at"] == {"type": "integer"}


def test_an_output_member_is_required_and_has_no_default(enrich_spec: OfferingSpec) -> None:
    """The reply always carries a defaulted field, so the output requires it, and writes no
    default (nexgen refuses one on a required member; nothing is ever absent to default)."""
    defs = _defs(derive(enrich_spec, Defaults))
    assert defs["DefaultsInput"]["required"] == ["req"]
    assert defs["DefaultsInput"]["properties"]["n"] == {"type": "integer", "default": 3}
    assert defs["DefaultsOutput"]["required"] == ["b", "f", "n", "req", "s"]
    assert all("default" not in p for p in defs["DefaultsOutput"]["properties"].values())

    optionals = _defs(derive(enrich_spec, Optionals))["OptionalsOutput"]
    assert optionals["required"] == ["later", "must", "note"]
    assert optionals["properties"]["note"] == {"oneOf": [{"type": "string"}, {"type": "null"}]}

    nested = _defs(derive(enrich_spec, Nested))
    assert nested["NestedOutput"]["required"] == ["leaf", "many", "maybe"]
    assert nested["NestedOutput"]["properties"]["leaf"] == {"$ref": "#/$defs/LeafOutput"}
    assert nested["LeafOutput"]["required"] == ["v"]
    assert "required" not in nested["LeafInput"]


def test_a_model_whose_two_modes_differ_is_two_definitions(enrich_spec: OfferingSpec) -> None:
    """``XInput`` and ``XOutput``, as pydantic names them (``X-Input``) but spelled as the
    identifiers a contract's names are; a model both modes describe alike keeps one
    definition under its own name, and so does a model used on one side only."""
    tagged = derive(enrich_spec, Tagged)
    assert sorted(_defs(tagged)) == ["Cat", "DogInput", "DogOutput", "TaggedInput", "TaggedOutput"]
    ops = tagged["services"]["AcmeEnrich"]["operations"]
    assert ops["quote"]["input"] == ops["execute"]["input"] == {"$ref": "#/$defs/TaggedInput"}
    assert ops["execute"]["output"] == {"$ref": "#/$defs/TaggedOutput"}

    assert sorted(_defs(derive(enrich_spec, Plain))) == ["Plain"]
    assert sorted(_defs(derive(enrich_spec, Plain, Defaults))) == ["Defaults", "Plain"]
    assert sorted(_defs(derive(enrich_spec, Defaults, Plain))) == ["Defaults", "Plain"]
    assert derive(enrich_spec, Tagged) == tagged  # deterministic


class DefaultsOutput(BaseModel):
    """A seller type that takes the name Kontra would give the output of ``Defaults``."""

    model_config = ConfigDict(extra="forbid")

    d: Defaults


def test_a_seller_name_never_collides_with_a_split(enrich_spec: OfferingSpec) -> None:
    """pydantic picks the first name only one schema wants, so ``Defaults``'s output and a
    seller's own ``DefaultsOutput`` both fall back to longer, distinct names; the document
    is still one the catalog accepts."""
    doc = derive(enrich_spec, DefaultsOutput, Defaults)
    info = validate_document(doc)
    names = sorted(_defs(doc))
    assert len(names) == 3
    assert all(re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,99}", n) for n in names), names
    assert info.output_type != info.input_type


class Money(BaseModel):
    amount: Decimal


def test_a_decimal_output_is_the_string_it_is_written_as(enrich_spec: OfferingSpec) -> None:
    """pydantic writes a Decimal as a JSON string, so an output may carry one; as an input
    it still accepts a number or a string, which nexgen cannot type, and is refused."""
    doc = derive(enrich_spec, Plain, Money)
    assert doc["$defs"]["Money"]["properties"]["amount"] == {"type": "string"}
    with pytest.raises(ContractError, match="Decimal"):
        derive(enrich_spec, Money, Plain)


# --------------------------------------------------------------------------- re-keying


class Inner(BaseModel):
    model_config = ConfigDict(serialize_by_alias=True)

    x: int = Field(alias="X")


class Outer(BaseModel):
    """Aliases at every depth: a list, a map and an optional of models, and a model whose
    own config dumps it by alias already."""

    model_config = ConfigDict(extra="allow")

    y: int = Field(alias="Y")
    leaves: list[Camel]
    by_key: dict[str, Camel]
    maybe: Camel | None
    inner: Inner
    pet: Tagged


def test_by_alias_rekeys_what_model_dump_writes() -> None:
    value = Outer.model_validate(
        {
            "Y": 1,
            "leaves": [{"firstName": "a"}],
            "by_key": {"k": {"firstName": "b", "lastSeen": 2}},
            "maybe": None,
            "inner": {"X": 3},
            "pet": {"pet": {"kind": "dog"}},
            "extra_key": {"first_name": "kept as it is"},
        }
    )
    dumped = value.model_dump(mode="json")
    assert dumped["y"] == 1  # by name
    assert dumped["inner"] == {"X": 3}  # Inner by alias already, by its own config
    assert by_alias(Outer, dumped) == value.model_dump(mode="json", by_alias=True)
    # a reply already keyed by alias stays as it is
    wanted = value.model_dump(mode="json", by_alias=True)
    assert by_alias(Outer, wanted) == wanted


def test_the_handler_of_an_excluded_field_delivers(enrich_spec: OfferingSpec) -> None:
    """I33: ``as_output`` validated the reply against the model, so a reply without its
    excluded field (which the harness never writes) failed every job."""
    contract = InputValidator(derive(enrich_spec, Excluded))
    assert as_output(Excluded, {"keep": 1}, contract) == {"keep": 1}


def test_a_reply_that_is_not_the_contracts_output_fails_the_job(enrich_spec: OfferingSpec) -> None:
    """Where and what, quoting the contract, never the value: the reply is the buyer's."""
    contract = InputValidator(derive(enrich_spec, Computed))
    with pytest.raises(ApplicationError) as bad:
        as_output(Computed, {"w": MARKER, "extra": MARKER}, contract)
    assert bad.value.message == (
        "the handler's reply is not a Computed (output: has a member the contract does not "
        "declare; output.area: is missing; output.h: is missing; output.w: is not of type "
        "integer)"
    )
    assert bad.value.non_retryable
    assert MARKER not in str(bad.value)
    leaf = InputValidator(derive(enrich_spec, Plain, Leaf))
    assert leaf.output_problems({"v": 1}) == []
