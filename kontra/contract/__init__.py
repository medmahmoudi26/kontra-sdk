"""The shared truth (seam ③): the contract document, its derivation, canonical form,
hash, and validation. ``kontra publish``, ``kontra serve`` and the gate all call this
module; there is no second implementation of the hash (§19.3)."""

from kontra.contract.canonical import canonical_json, contract_hash, seal
from kontra.contract.card import agent_card
from kontra.contract.derive import derive_document, merge_document, nexgen_view
from kontra.contract.errors import ContractError, OfferingError
from kontra.contract.facts import XKontra
from kontra.contract.offering import OfferingSpec, load_offering, parse_offering
from kontra.contract.quote import KontraQuote, Quote, QuoteError
from kontra.contract.validate import ContractInfo, validate_document
from kontra.contract.validator import InputValidator
from kontra.contract.yaml_io import from_yaml, to_yaml

__all__ = [
    "ContractError",
    "ContractInfo",
    "InputValidator",
    "KontraQuote",
    "OfferingError",
    "OfferingSpec",
    "Quote",
    "QuoteError",
    "XKontra",
    "agent_card",
    "canonical_json",
    "contract_hash",
    "derive_document",
    "from_yaml",
    "load_offering",
    "merge_document",
    "nexgen_view",
    "parse_offering",
    "seal",
    "to_yaml",
    "validate_document",
]
