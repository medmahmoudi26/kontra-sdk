from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from kontra.contract import ContractError, canonical_json, contract_hash, from_yaml, seal, to_yaml
from kontra.contract.canonical import check_json_value


def test_jcs_orders_keys_and_spells_numbers_once() -> None:
    assert canonical_json({"b": 2, "a": [1.0, "é", None, True, 1e21]}) == (
        b'{"a":[1,"\xc3\xa9",null,true,1e+21],"b":2}'
    )
    assert canonical_json({"x": 1}) == canonical_json({"x": 1.0})


def test_hash_ignores_only_its_own_slot(enrich_doc: dict[str, Any]) -> None:
    h = contract_hash(enrich_doc)
    assert h == enrich_doc["x-kontra"]["contract_hash"]
    assert h.startswith("sha256:")
    assert len(h) == 7 + 64
    unsealed = copy.deepcopy(enrich_doc)
    del unsealed["x-kontra"]["contract_hash"]
    assert contract_hash(unsealed) == h
    tampered = copy.deepcopy(enrich_doc)
    tampered["x-kontra"]["contract_hash"] = "sha256:" + "0" * 64
    assert contract_hash(tampered) == h


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d["x-kontra"].update(review_window="2d"),
        lambda d: d["x-kontra"].update(public_key="age1qqqq"),
        lambda d: d["$defs"]["EnrichInput"]["properties"]["urls"].update(maxItems=9999),
        lambda d: d["services"]["AcmeEnrich"].update(description="other"),
    ],
)
def test_any_other_change_moves_the_hash(enrich_doc: dict[str, Any], mutate: Any) -> None:
    other = copy.deepcopy(enrich_doc)
    mutate(other)
    assert contract_hash(other) != contract_hash(enrich_doc)


def test_hash_survives_key_order_json_and_yaml(enrich_doc: dict[str, Any]) -> None:
    h = contract_hash(enrich_doc)
    reordered = json.loads(json.dumps(enrich_doc, sort_keys=True))
    reordered = dict(reversed(list(reordered.items())))
    assert contract_hash(reordered) == h
    assert contract_hash(from_yaml(to_yaml(enrich_doc))) == h


def test_seal_copies(enrich_doc: dict[str, Any]) -> None:
    unsealed = copy.deepcopy(enrich_doc)
    del unsealed["x-kontra"]["contract_hash"]
    sealed = seal(unsealed)
    assert "contract_hash" not in unsealed["x-kontra"]
    assert sealed == enrich_doc


def test_seal_needs_x_kontra() -> None:
    with pytest.raises(ContractError) as info:
        seal({"nexusrpc": "1.0.0"})
    assert info.value.path == "x-kontra"


@pytest.mark.parametrize(
    ("value", "path"),
    [
        ({"a": {1: "x"}}, "a"),
        ({"a": [1, float("nan")]}, "a[1]"),
        ({"a": float("inf")}, "a"),
        ({"a": 2**53}, "a"),
        ({"a": (1, 2)}, "a"),
        ({"a": {"b": b"bytes"}}, "a.b"),
        ({"a": {"weird key": {1, 2}}}, 'a["weird key"]'),
    ],
)
def test_only_plain_json_is_canonical(value: Any, path: str) -> None:
    with pytest.raises(ContractError) as info:
        canonical_json(value)
    assert info.value.path == path


def test_depth_is_counted_without_recursion() -> None:
    deep: Any = "leaf"
    for _ in range(5000):
        deep = {"x": deep}
    assert check_json_value(deep) == 5000
    with pytest.raises(ContractError, match="deeper than 32"):
        check_json_value(deep, max_depth=32)
    assert check_json_value([[]]) == 2
    assert check_json_value(3) == 0
