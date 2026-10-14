"""Schema-first: a hand-written Nexus document plus offering.yaml (§11.1)."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from kontra.contract import (
    ContractError,
    OfferingSpec,
    from_yaml,
    merge_document,
    nexgen_view,
    validate_document,
)

HAND_WRITTEN = """
nexusrpc: "1.0.0"
services:
  AcmeEnrich:
    fqn: acme.enrich
    description: Written by a Go team.
    operations:
      quote:
        fqn: quote
        input: {$ref: "#/$defs/EnrichInput"}
        output: {$ref: "#/$defs/KontraQuote"}
      execute:
        fqn: execute
        description: Enrich.
        input: {$ref: "#/$defs/EnrichInput"}
        output: {$ref: "#/$defs/EnrichOutput"}
$defs:
  EnrichInput:
    type: object
    additionalProperties: false
    required: [urls]
    properties:
      urls: {type: array, maxItems: 10000, items: {type: string, format: uri}}
  EnrichOutput:
    type: object
    additionalProperties: false
    required: [records, summary]
    properties:
      records: {$ref: "#/$defs/KontraArtifact"}
      summary: {type: object, additionalProperties: {type: integer}}
"""


def test_merge_adds_facts_and_reserved_defs(enrich_spec: OfferingSpec) -> None:
    hand = from_yaml(HAND_WRITTEN)
    doc = merge_document(enrich_spec, hand)
    info = validate_document(doc)
    assert info.offering.key == "acme/enrich@1.2.0"
    assert "public_key" not in doc["x-kontra"]
    assert doc["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert sorted(doc["$defs"]) == ["EnrichInput", "EnrichOutput", "KontraArtifact", "KontraQuote"]
    # the seller's text is theirs: nothing else changes
    assert doc["services"] == hand["services"]
    assert {k: v for k, v in doc["$defs"].items() if not k.startswith("Kontra")} == hand["$defs"]
    assert hand.get("x-kontra") is None  # the input is not mutated


def test_merging_the_nexgen_view_reproduces_the_derived_document(
    enrich_spec: OfferingSpec, enrich_doc: dict[str, Any]
) -> None:
    """Both sources, one owner each, one result: the hash is IDL-independent (§11.2)."""
    merged = merge_document(enrich_spec, nexgen_view(enrich_doc))
    assert merged == enrich_doc


def test_reserved_defs_present_must_match(
    enrich_spec: OfferingSpec, enrich_doc: dict[str, Any]
) -> None:
    view = nexgen_view(enrich_doc)
    view["$defs"]["KontraQuote"]["description"] = "my own words"
    with pytest.raises(ContractError) as info:
        merge_document(enrich_spec, view)
    assert info.value.path == "$defs.KontraQuote"


def test_unused_artifact_is_not_added(enrich_spec: OfferingSpec) -> None:
    hand = from_yaml(HAND_WRITTEN)
    hand["$defs"]["EnrichOutput"]["properties"]["records"] = {"type": "string"}
    doc = merge_document(enrich_spec, hand)
    assert "KontraArtifact" not in doc["$defs"]


@pytest.mark.parametrize(
    ("mutate", "path", "fragment"),
    [
        (lambda d: d.__setitem__("x-kontra", {}), "x-kontra", "offering.yaml"),
        (lambda d: d.__setitem__("description", "x"), "description", "offering.yaml"),
        (lambda d: d.__setitem__("types", {}), "types", "unknown top-level"),
        (
            lambda d: d["services"]["AcmeEnrich"].__setitem__("fqn", "acme/enrich"),
            "services.AcmeEnrich.fqn",
            "acme.enrich",
        ),
        (
            lambda d: d["$defs"]["EnrichInput"]["properties"].__setitem__(
                "n", {"anyOf": [{"type": "string"}, {"type": "integer"}]}
            ),
            "$defs.EnrichInput.properties.n.anyOf",
            "oneOf",
        ),
        (
            lambda d: d["$defs"]["EnrichInput"].__delitem__("additionalProperties"),
            "$defs.EnrichInput.additionalProperties",
            "unset",
        ),
    ],
)
def test_merge_refuses_with_a_path(
    enrich_spec: OfferingSpec, mutate: Any, path: str, fragment: str
) -> None:
    hand = from_yaml(HAND_WRITTEN)
    mutate(hand)
    with pytest.raises(ContractError) as info:
        merge_document(enrich_spec, hand)
    assert info.value.path == path, info.value
    assert fragment in info.value.message


def test_merge_needs_a_mapping(enrich_spec: OfferingSpec) -> None:
    with pytest.raises(ContractError):
        merge_document(enrich_spec, ["no"])  # type: ignore[arg-type]


def test_merge_offering_mismatch(enrich_spec: OfferingSpec) -> None:
    other = enrich_spec.model_copy(update={"name": "acme/other"})
    with pytest.raises(ContractError) as info:
        merge_document(other, from_yaml(HAND_WRITTEN))
    assert info.value.path == "services"


def test_nexgen_view_drops_only_x_kontra(enrich_doc: dict[str, Any]) -> None:
    view = nexgen_view(enrich_doc)
    assert list(view) == ["nexusrpc", "$schema", "services", "$defs"]
    view["$defs"]["EnrichInput"]["description"] = "changed"
    assert enrich_doc["$defs"]["EnrichInput"]["description"] != "changed"
    assert copy.deepcopy(enrich_doc)["x-kontra"]
