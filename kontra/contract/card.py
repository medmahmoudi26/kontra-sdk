"""The A2A Agent Card of an offering (D12, §11.4 step 3).

Derived from the contract document alone, so the card can never claim something the
contract does not: one card per offering version, one skill per card. The JSON follows
the A2A 1.0 protobuf JSON mapping (``supportedInterfaces``, ``defaultInputModes``, ...),
which ``a2a-sdk`` 1.x parses into its ``AgentCard`` message. Kontra-specific facts (the
contract hash, limits, the seller's key) ride in one capability extension, where A2A
puts protocol extensions.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from kontra.contract.units import format_duration, format_size
from kontra.contract.validate import validate_document

__all__ = ["A2A_PROTOCOL_VERSION", "CONTRACT_EXTENSION_URI", "agent_card"]

A2A_PROTOCOL_VERSION = "1.0"
#: Identifies the Kontra contract extension; a client that does not know it can still
#: read the card, but cannot hire without the catalog document it points at.
CONTRACT_EXTENSION_URI = "https://kontra.run/ext/contract/v1"
_JSON = "application/json"


def agent_card(
    doc: Mapping[str, Any], *, url: str, protocol_binding: str = "JSONRPC"
) -> dict[str, Any]:
    """An A2A Agent Card for the offering in ``doc``, served at ``url``. The document is
    validated first; a card is never derived from a document the catalog would refuse."""
    info = validate_document(doc)
    facts = info.facts
    oid = info.offering
    limits = (
        f"Fixed price per job from a quote; at most {format_duration(facts.max_duration)} per job; "
        f"buyer review window {format_duration(facts.review_window)}."
    )
    if facts.pricing_hint:
        limits = f"Pricing: {facts.pricing_hint}. " + limits
    if facts.uses_hosted_model:
        limits += " Uses a hosted model: the input is sent to a model provider, in plaintext to it."
    params: dict[str, Any] = {
        "offering": oid.key,
        "contractHash": info.contract_hash,
        "service": oid.service_name,
        "maxDuration": format_duration(facts.max_duration),
        "reviewWindow": format_duration(facts.review_window),
        "maxOutputBytes": format_size(facts.max_output_bytes),
        "artifactRetention": format_duration(facts.artifact_retention),
    }
    if facts.pricing_hint:
        params["pricingHint"] = facts.pricing_hint
    if facts.max_concurrent_jobs is not None:
        params["maxConcurrentJobs"] = facts.max_concurrent_jobs
    if facts.uses_hosted_model:
        params["usesHostedModel"] = True
    return {
        "name": oid.full_name,
        "description": facts.description,
        "version": facts.version,
        "supportedInterfaces": [
            {
                "url": url,
                "protocolBinding": protocol_binding,
                "protocolVersion": A2A_PROTOCOL_VERSION,
            }
        ],
        "capabilities": {
            "streaming": False,
            "pushNotifications": False,
            "extensions": [
                {
                    "uri": CONTRACT_EXTENSION_URI,
                    "description": "Kontra contract: quote, escrowed hire, end-to-end encryption. "
                    "Inputs must validate against the catalog document with this hash.",
                    "required": True,
                    "params": params,
                }
            ],
        },
        "securitySchemes": {
            "kontra": {"httpAuthSecurityScheme": {"scheme": "Bearer", "bearerFormat": "JWT"}}
        },
        "securityRequirements": [{"schemes": {"kontra": {}}}],  # no scopes
        "defaultInputModes": [_JSON],
        "defaultOutputModes": [_JSON],
        "skills": [
            {
                "id": oid.key,
                "name": oid.full_name,
                "description": f"{facts.description}\n\n{limits}",
                "tags": list(facts.tags) or [oid.party],
                "inputModes": [_JSON],
                "outputModes": [_JSON],
            }
        ],
    }
