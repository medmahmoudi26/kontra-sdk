from __future__ import annotations

import copy
import json
import random
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from kontra.contract import ContractError, from_yaml, to_yaml


def _shuffled(value: Any, rng: random.Random) -> Any:
    if isinstance(value, dict):
        items = list(value.items())
        rng.shuffle(items)
        return {k: _shuffled(v, rng) for k, v in items}
    if isinstance(value, list):
        return [_shuffled(v, rng) for v in value]
    return value


def test_rendering_ignores_key_order(enrich_doc: dict[str, Any]) -> None:
    text = to_yaml(enrich_doc)
    for seed in range(5):
        assert to_yaml(_shuffled(copy.deepcopy(enrich_doc), random.Random(seed))) == text


def test_round_trip(enrich_doc: dict[str, Any]) -> None:
    assert from_yaml(to_yaml(enrich_doc)) == enrich_doc


def test_structure_reads_top_down(enrich_doc: dict[str, Any]) -> None:
    lines = [ln for ln in to_yaml(enrich_doc).splitlines() if not ln.startswith(" ")]
    assert [ln.split(":")[0] for ln in lines] == [
        "nexusrpc",
        "$schema",
        "services",
        "$defs",
        "x-kontra",
    ]


def test_awkward_strings_survive() -> None:
    doc = {
        "multi": "line one\nline two\n",
        "version": "1.2.0",
        "num": "012",
        "yes": "yes",
        "on": "off",
        "date": "2026-10-11",
        "null": "null",
        "hash": "#/$defs/X",
        "colon": "a: b",
        "unicode": "café",
        "empty": "",
        "tilde": "~",
    }
    text = to_yaml(doc)
    assert from_yaml(text) == doc
    assert 'version: "1.2.0"' in text
    assert "multi: |" in text


def test_no_anchors_for_shared_objects() -> None:
    shared = {"$ref": "#/$defs/X"}
    text = to_yaml({"a": shared, "b": shared})
    assert "&" not in text
    assert "*" not in text


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("on: 1\n", {"on": 1}),
        ("yes: no\n", {"yes": "no"}),
        ("d: 2026-10-11\n", {"d": "2026-10-11"}),
        ("n: 012\n", {"n": 12}),
        ("n: 0x1F\n", {"n": "0x1F"}),
        ("n: 1_000\n", {"n": "1_000"}),
        ("t: 1:30\n", {"t": "1:30"}),
        ("f: 1.5e3\n", {"f": 1500.0}),
        ("b: true\nc: False\nz: ~\n", {"b": True, "c": False, "z": None}),
        ("v: 1.2.0\n", {"v": "1.2.0"}),
        ("x: .inf\n", {"x": ".inf"}),
    ],
)
def test_loader_uses_yaml_1_2_core_scalars(text: str, expected: dict[str, Any]) -> None:
    assert from_yaml(text) == expected


@pytest.mark.parametrize(
    ("text", "fragment"),
    [
        ("a: &x [1]\nb: *x\n", "aliases"),
        ("a: 1\na: 2\n", "duplicate key"),
        ("base: {x: 1}\nother:\n  !!merge <<: {y: 2}\n", "merge keys"),
        ("1: x\n", "keys must be strings"),
        ("- a\n", "must be a mapping"),
        ("a: !!binary aGk=\n", "not a JSON value"),
        ("a: !!python/object:os.system x\n", "not valid YAML"),
        ("a: [1\n", "line"),
    ],
)
def test_loader_refuses_what_json_cannot_say(text: str, fragment: str) -> None:
    with pytest.raises(ContractError, match=fragment):
        from_yaml(text)


def test_to_yaml_refuses_non_json() -> None:
    with pytest.raises(ContractError):
        to_yaml({"a": {1, 2}})


def test_json_and_yaml_agree(enrich_doc: dict[str, Any]) -> None:
    assert from_yaml(json.dumps(enrich_doc)) == enrich_doc  # JSON is YAML 1.2


@pytest.mark.parametrize(
    "value",
    ["-1e5", "+1e5", "+.5", "-.5", "-.5e3", "+0.5e-3", "-5.", "+1", "-0", "TRUE", "Null", "~"],
)
def test_strings_a_yaml_1_2_reader_would_take_for_numbers_stay_strings(value: str) -> None:
    """PyYAML's dumper knows only YAML 1.1 and wrote these plain; our loader (and nexgen's)
    read them back as numbers, so the document changed -- and its hash -- in a round trip."""
    doc = {"k": value, value: "v"}
    assert from_yaml(to_yaml(doc)) == doc


@pytest.mark.parametrize(
    "value", ["\x85", "x\x85y", "a\u2028b", "a\u2029", "\ufeffa", "a\rb", "a\r\nb"]
)
def test_yaml_line_breaks_other_than_newline_survive(value: str) -> None:
    """NEL (U+0085) was written raw inside single quotes and read back as a space."""
    doc = {"k": value, value: "v"}
    assert from_yaml(to_yaml(doc)) == doc


@settings(max_examples=400, deadline=None)
@given(st.dictionaries(st.text(max_size=12), st.text(max_size=12), max_size=4))
def test_any_strings_round_trip(doc: dict[str, str]) -> None:
    assert from_yaml(to_yaml(doc)) == doc


@pytest.mark.parametrize(
    ("text", "fragment"),
    [
        ("a: " + "[" * 5000 + "]" * 5000 + "\n", "too deeply"),
        ("a: " + "{b: " * 3000 + "1" + "}" * 3000 + "\n", "too deeply"),
        ("a: !!float .inf\n", "cannot read"),
        ("a: !!int 0x10\n", "cannot read"),
    ],
)
def test_loader_failures_are_contract_errors(text: str, fragment: str) -> None:
    """These raised RecursionError or ValueError, which a caller handling ContractError
    would not catch."""
    with pytest.raises(ContractError, match=fragment):
        from_yaml(text)


def test_lone_surrogates_are_refused_with_a_path() -> None:
    with pytest.raises(ContractError, match="lone surrogate") as info:
        to_yaml({"a": ["x\ud800"]})
    assert info.value.path == "a[0]"
