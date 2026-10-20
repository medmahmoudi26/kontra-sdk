"""Seam ③: ``offering.yaml``'s ``streams:`` (decisions I31) -- validated by
``kontra.contract.offering`` and stated in ``spec/offering-streams.v1.json``, the same vectors
through both, and never in the contract hash."""

from __future__ import annotations

from typing import Any

import pytest
from jsonschema import Draft202012Validator
from jsonschema.protocols import Validator

from kontra.contract import OfferingSpec, canonical_json, derive_document, parse_offering
from kontra.contract.errors import OfferingError
from kontra.contract.facts import XKontra
from kontra.contract.formats import ContractValidator
from kontra.contract.names import PROGRESS_TOPIC, RESERVED_TOPIC_PREFIX
from kontra.contract.offering import STREAMS_MAX, TOPIC_PATTERN, check_streams, check_topic
from kontra.contract.spec import offering_streams_schema

from .sample_models import EnrichInput, EnrichOutput

ACCEPTED: list[list[str]] = [
    [],
    ["turn_events"],
    ["a"],
    ["A" + "x" * 63],
    ["events", "private.audit", "x-y_z.1"],
    ["Kontra"],  # no dot: not the reserved prefix
    ["kontraband"],
    ["b", "a", "c"],
    [f"t{i}" for i in range(STREAMS_MAX)],
]

REFUSED: list[Any] = [
    "turn_events",  # a string, not a list
    {"turn_events": True},
    [1],
    [None],
    [""],
    ["1events"],
    ["_events"],
    ["-events"],
    [".events"],
    ["A" + "x" * 64],  # 65 characters
    ["événements"],
    ["turn events"],
    ["turn/events"],
    ["turn_events\n"],
    ["kontra.progress"],
    ["kontra.anything"],
    ["KONTRA.progress"],
    ["Kontra.x"],
    ["a", "a"],
    [f"t{i}" for i in range(STREAMS_MAX + 1)],
]


@pytest.fixture(scope="module")
def schema() -> Validator:
    """The schema read as a gate in any language reads it: JSON Schema 2020-12 with ECMA
    patterns (``$`` is the end of input; Python's ``re`` would accept a trailing newline)."""
    doc = offering_streams_schema()
    Draft202012Validator.check_schema(doc)
    return ContractValidator(doc)


@pytest.mark.parametrize("value", ACCEPTED, ids=repr)
def test_accepted_by_the_code_and_the_spec(value: list[str], schema: Validator) -> None:
    assert check_streams(value) == tuple(sorted(value))
    assert list(schema.iter_errors(value)) == []


@pytest.mark.parametrize("value", REFUSED, ids=repr)
def test_refused_by_the_code_and_the_spec(value: Any, schema: Validator) -> None:
    with pytest.raises(ValueError, match=r"topic|list|at most"):
        check_streams(value)
    assert list(schema.iter_errors(value)) != []


def test_the_spec_states_the_same_rule_as_the_code() -> None:
    doc = offering_streams_schema()
    assert doc["maxItems"] == STREAMS_MAX
    assert doc["uniqueItems"] is True
    assert doc["items"]["pattern"] == f"^{TOPIC_PATTERN}$"
    assert RESERVED_TOPIC_PREFIX == "kontra."
    assert PROGRESS_TOPIC.startswith(RESERVED_TOPIC_PREFIX)
    with pytest.raises(ValueError, match="reserved"):
        check_streams([PROGRESS_TOPIC])


def test_a_topic_is_matched_whole() -> None:
    assert check_topic("turn_events") == "turn_events"
    for bad in ("turn_events\n", "\nturn_events", "turn_events ", "a" * 65, 3):
        with pytest.raises(ValueError, match="must be ASCII letters"):
            check_topic(bad)


def _yaml(**extra: Any) -> dict[str, Any]:
    return {
        "name": "acme/enrich",
        "version": "1.2.0",
        "description": "Enrich companies",
        "max_duration": "10m",
        **extra,
    }


def test_offering_yaml_takes_streams_sorted() -> None:
    spec = parse_offering(_yaml(streams=["turn_events", "audit"]))
    assert spec.streams == ("audit", "turn_events")
    assert parse_offering(_yaml()).streams == ()


def test_offering_yaml_refuses_bad_streams_by_path() -> None:
    with pytest.raises(OfferingError) as refused:
        parse_offering(_yaml(streams=["kontra.progress"]))
    assert refused.value.path == "streams"
    assert "reserved" in refused.value.message


def test_an_unknown_key_names_streams_among_the_keys() -> None:
    with pytest.raises(OfferingError) as refused:
        parse_offering(_yaml(stream=["turn_events"]))
    assert "streams" in refused.value.message


def test_streams_are_not_in_the_contract_hash(enrich_spec: OfferingSpec) -> None:
    """Catalog data, not contract data: the derived document is the same bytes with or
    without streams, and x-kontra cannot carry them."""
    with_streams = enrich_spec.model_copy(update={"streams": ("turn_events",)})
    plain = derive_document(enrich_spec, EnrichInput, EnrichOutput)
    streamed = derive_document(with_streams, EnrichInput, EnrichOutput)
    assert canonical_json(plain) == canonical_json(streamed)
    assert "streams" not in enrich_spec.canonical()
    assert "streams" not in with_streams.canonical()
    assert "streams" not in plain["x-kontra"]
    assert "streams" not in XKontra.model_fields
    with pytest.raises(ValueError, match="streams"):
        XKontra.model_validate({**plain["x-kontra"], "streams": ["turn_events"]})
