"""The A2A Agent Card (D12), checked against a2a-sdk's own AgentCard message.

a2a-sdk 1.x models the card as the A2A 1.0 protobuf ``lf.a2a.v1.AgentCard``; its JSON form
is the proto3 JSON mapping (camelCase). ``ParseDict`` refuses unknown fields, and the
proto's ``field_behavior = REQUIRED`` annotations say which fields a card must carry.
"""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest
from a2a.types import a2a_pb2
from google.api import field_behavior_pb2
from google.protobuf import json_format
from google.protobuf.message import Message

from kontra.contract import ContractError, OfferingSpec, agent_card, derive_document
from kontra.contract.card import CONTRACT_EXTENSION_URI

from .sample_models import EnrichInput, EnrichOutput

URL = "https://gate.kontra.run/a2a/acme/enrich@1.2.0"


def _missing_required(msg: Message, path: str = "card") -> list[str]:
    missing: list[str] = []
    for field in msg.DESCRIPTOR.fields:
        behaviors = list(field.GetOptions().Extensions[field_behavior_pb2.field_behavior])
        value = getattr(msg, field.name)
        repeated = field.is_repeated
        if field_behavior_pb2.REQUIRED in behaviors:
            present = (
                len(value) > 0
                if repeated
                else (
                    msg.HasField(field.name)
                    if field.message_type is not None
                    else value != field.default_value
                )
            )
            if not present:
                missing.append(f"{path}.{field.json_name}")
        if field.message_type is not None and not field.message_type.GetOptions().map_entry:
            items = list(value) if repeated else ([value] if msg.HasField(field.name) else [])
            for i, item in enumerate(items):
                missing += _missing_required(item, f"{path}.{field.json_name}[{i}]")
    return missing


def _parse(card: dict[str, Any]) -> a2a_pb2.AgentCard:
    return json_format.ParseDict(card, a2a_pb2.AgentCard())  # unknown fields raise


def test_card_is_an_a2a_agent_card(enrich_doc: dict[str, Any]) -> None:
    card = agent_card(enrich_doc, url=URL)
    parsed = _parse(card)
    assert _missing_required(parsed) == []
    assert parsed.name == "acme/enrich"
    assert parsed.version == "1.2.0"
    assert parsed.description == "Resolve a URL list to company records"
    assert [i.url for i in parsed.supported_interfaces] == [URL]
    assert parsed.supported_interfaces[0].protocol_version == "1.0"
    assert len(parsed.skills) == 1  # one skill per offering
    skill = parsed.skills[0]
    assert skill.id == "acme/enrich@1.2.0"
    assert list(skill.tags) == ["companies", "enrichment"]
    assert "$0.001 per url" in skill.description
    assert "6h" in skill.description
    assert list(parsed.default_input_modes) == ["application/json"]
    assert "kontra" in parsed.security_schemes
    # the round trip through the proto loses nothing we wrote
    assert json_format.MessageToDict(parsed) == json.loads(json.dumps(card))


def test_card_carries_the_contract_hash_not_the_endpoint(enrich_doc: dict[str, Any]) -> None:
    card = agent_card(enrich_doc, url=URL)
    ext = card["capabilities"]["extensions"][0]
    assert ext["uri"] == CONTRACT_EXTENSION_URI
    assert ext["required"] is True
    params = ext["params"]
    assert params["contractHash"] == enrich_doc["x-kontra"]["contract_hash"]
    assert params["offering"] == "acme/enrich@1.2.0"
    assert "publicKey" not in params  # a contract seals no key (spec v2, decisions I7)
    assert params["maxConcurrentJobs"] == 10
    # endpoints are opaque and assigned after sealing (decisions I1): never in a card
    assert "endpoint" not in json.dumps(card).lower()


def test_card_without_tags_or_hint(enrich_spec: OfferingSpec) -> None:
    spec = enrich_spec.model_copy(
        update={"tags": [], "pricing_hint": None, "max_concurrent_jobs": None}
    )
    card = agent_card(
        derive_document(spec, EnrichInput, EnrichOutput),
        url=URL,
        protocol_binding="HTTP+JSON",
    )
    parsed = _parse(card)
    assert _missing_required(parsed) == []
    assert list(parsed.skills[0].tags) == ["acme"]
    assert parsed.supported_interfaces[0].protocol_binding == "HTTP+JSON"
    params = card["capabilities"]["extensions"][0]["params"]
    assert "pricingHint" not in params
    assert "maxConcurrentJobs" not in params


def test_card_only_from_a_valid_document(enrich_doc: dict[str, Any]) -> None:
    doc = copy.deepcopy(enrich_doc)
    doc["x-kontra"]["description"] = "Now does something else"
    with pytest.raises(ContractError) as info:
        agent_card(doc, url=URL)
    assert info.value.path == "x-kontra.contract_hash"


def test_required_field_check_detects_omissions(enrich_doc: dict[str, Any]) -> None:
    card = agent_card(enrich_doc, url=URL)
    del card["skills"][0]["tags"]
    del card["version"]
    assert _missing_required(_parse(card)) == ["card.version", "card.skills[0].tags"]
