"""Canonical form and the hash lock (PRD §11.2).

``contract_hash = sha256(RFC 8785 canonical JSON of the document minus
x-kontra.contract_hash)``. JCS fixes key order, number spelling and string escaping, so
the hash depends on the document's *value* only: YAML or JSON, any key order, ``1`` or
``1.0``, all hash the same. This module is the only implementation of the hash; the
gate, ``kontra publish`` and ``kontra serve`` all call it (§19.3).
"""

from __future__ import annotations

import copy
import hashlib
import math
from collections.abc import Mapping
from typing import Any

import rfc8785

from kontra.contract.errors import ROOT, ContractError, join_path

__all__ = [
    "HASH_PREFIX",
    "MAX_SAFE_INTEGER",
    "canonical_json",
    "check_json_value",
    "contract_hash",
    "seal",
]

HASH_PREFIX = "sha256:"
#: JSON numbers are binary64 in most of the languages nexgen targets; larger integers do
#: not survive a round trip, so they have no canonical form (RFC 8785 §3.2.2.3).
MAX_SAFE_INTEGER = 2**53 - 1


def check_json_value(value: Any, *, max_depth: int | None = None) -> int:
    """Refuse anything that is not plain JSON (str-keyed dicts, lists, str, int, finite
    float, bool, None), naming the path. Returns the nesting depth (a scalar is 0, ``{}``
    is 1). Iterative, so a hostile document cannot exhaust the Python stack.
    """
    deepest = 0
    stack: list[tuple[Any, str, int]] = [(value, ROOT, 0)]
    while stack:
        node, path, depth = stack.pop()
        if isinstance(node, dict):
            depth += 1
            if max_depth is not None and depth > max_depth:
                raise ContractError(path, f"nested deeper than {max_depth} levels")
            for k, v in node.items():  # pyright: ignore[reportUnknownVariableType]
                if not isinstance(k, str):
                    raise ContractError(path, f"object keys must be strings, got {k!r}")
                _check_text(k, path)
                stack.append((v, join_path(path, k), depth))
        elif isinstance(node, list):
            depth += 1
            if max_depth is not None and depth > max_depth:
                raise ContractError(path, f"nested deeper than {max_depth} levels")
            for i, v in enumerate(node):  # pyright: ignore[reportUnknownVariableType]
                stack.append((v, join_path(path, i), depth))
        elif isinstance(node, str):
            _check_text(node, path)
        elif isinstance(node, bool) or node is None:
            pass
        elif isinstance(node, int):
            if abs(node) > MAX_SAFE_INTEGER:
                raise ContractError(path, f"integer {node} is outside ±(2^53-1)")
        elif isinstance(node, float):
            if not math.isfinite(node):
                raise ContractError(path, f"{node} is not a JSON number")
        else:
            raise ContractError(path, f"{type(node).__name__} is not a JSON value")
        deepest = max(deepest, depth)
    return deepest


def _check_text(value: str, path: str) -> None:
    """A JSON string is Unicode text; a lone surrogate (``"\\ud800"`` from a JSON
    parser) cannot be encoded, so it has no canonical form and no YAML rendering."""
    if not value.isascii():
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            raise ContractError(
                path, "contains a lone surrogate, which is not Unicode text"
            ) from None


def canonical_json(obj: Any) -> bytes:
    """RFC 8785 (JCS) bytes of a JSON value."""
    check_json_value(obj)
    try:
        return rfc8785.dumps(obj)
    except rfc8785.CanonicalizationError as exc:  # pragma: no cover - check_json_value guards
        raise ContractError(ROOT, f"cannot canonicalise: {exc}") from exc


def _without_hash(doc: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(doc)
    facts = out.get("x-kontra")
    if isinstance(facts, Mapping) and "contract_hash" in facts:
        out["x-kontra"] = {k: v for k, v in facts.items() if k != "contract_hash"}  # pyright: ignore[reportUnknownVariableType]
    return out


def contract_hash(doc: Mapping[str, Any]) -> str:
    """``"sha256:<hex>"`` of the canonical document without ``x-kontra.contract_hash``."""
    digest = hashlib.sha256(canonical_json(_without_hash(doc))).hexdigest()
    return HASH_PREFIX + digest


def seal(doc: Mapping[str, Any]) -> dict[str, Any]:
    """A copy of ``doc`` with ``x-kontra.contract_hash`` set (replacing any old value)."""
    facts = doc.get("x-kontra")
    if not isinstance(facts, Mapping):
        raise ContractError("x-kontra", "missing: only a document with x-kontra can be sealed")
    out = copy.deepcopy(dict(doc))
    out["x-kontra"]["contract_hash"] = contract_hash(out)
    return out
