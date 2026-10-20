"""The harness check's canonical form and verdicts, rule by rule (decisions I33), on
contracts derived as ``kontra publish`` derives them and harness documents written out by
hand in the shape ``agent_schema`` emits. None of this needs the harness installed:
``test_harness.py`` runs the same comparison against the harness itself."""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Annotated, Any, Literal

import pytest
from pydantic import BaseModel, ConfigDict, Field

from kontra.contract import OfferingSpec, derive_document
from kontra.contract import harness as h
from kontra.contract.harness import ABSENT, Difference, HarnessSchemaError, compare_handler


def _doc(spec: OfferingSpec, inp: type[Any], out: type[Any] | None = None) -> dict[str, Any]:
    return derive_document(spec, inp, out or inp)


def _harness(inp: str, out: str, defs: dict[str, Any], handler: str = "run") -> dict[str, Any]:
    """A document in ``agent_schema``'s shape, with one handler."""
    return {
        "agent": "Agent",
        "source": "agents:Agent",
        "init_data": None,
        "handlers": {
            handler: {
                "description": "Run.",
                "mid_turn": "reject",
                "input": {"$ref": f"#/$defs/{inp}"},
                "output": {"$ref": f"#/$defs/{out}"},
            }
        },
        "states": {},
        "$defs": defs,
    }


def _found(differences: tuple[Difference, ...]) -> set[tuple[str, str, str]]:
    return {(d.side, d.path, d.kind) for d in differences}


class Host(BaseModel):
    """A host to sweep."""

    model_config = ConfigDict(extra="forbid")

    host: str
    port: int = Field(ge=1, le=65535)


def _host(**extra: Any) -> dict[str, Any]:
    """``Host`` as pydantic writes it, titles and all."""
    return {
        "additionalProperties": False,
        "description": "A host to sweep.",
        "properties": {
            "host": {"title": "Host", "type": "string"},
            "port": {"maximum": 65535, "minimum": 1, "title": "Port", "type": "integer"},
        },
        "required": ["port", "host"],
        "title": "Host",
        "type": "object",
        **extra,
    }


# --------------------------------------------------------------------------- representational


def test_names_titles_and_order_are_not_differences(enrich_spec: OfferingSpec) -> None:
    """``$defs`` names (``Host-Input``, ``Host-Output``), titles, descriptions, examples and
    the order of ``required`` do not change which values are valid."""
    defs = {
        "Host-Input": _host(examples=[{"host": "a", "port": 1}], title="Something else"),
        "Host-Output": _host(description="Another text"),
    }
    assert (
        compare_handler(_doc(enrich_spec, Host), _harness("Host-Input", "Host-Output", defs), "run")
        == ()
    )


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Paint(BaseModel):
    """An enum, which the contract writes inline and pydantic names."""

    model_config = ConfigDict(extra="forbid")

    color: Color = Color.RED
    maybe: Color | None


def test_an_enum_written_inline_is_the_named_one(enrich_spec: OfferingSpec) -> None:
    defs = {
        "Color": {"enum": ["red", "blue"], "title": "Color", "type": "string"},
        "Paint": {
            "additionalProperties": False,
            "properties": {
                "color": {"$ref": "#/$defs/Color", "default": "red"},
                "maybe": {"anyOf": [{"$ref": "#/$defs/Color"}, {"type": "null"}]},
            },
            "required": ["maybe"],
            "type": "object",
        },
    }
    assert compare_handler(_doc(enrich_spec, Paint), _harness("Paint", "Paint", defs), "run") == ()


class Note(BaseModel):
    """Optional fields."""

    model_config = ConfigDict(extra="forbid")

    text: str | None = None
    tags: list[str] = []
    size: int = 3


def _note(**size: Any) -> dict[str, Any]:
    return {
        "additionalProperties": False,
        "properties": {
            "text": {"anyOf": [{"type": "string"}, {"type": "null"}], "default": None},
            "tags": {"items": {"type": "string"}, "type": "array", "default": []},
            "size": {"type": "integer", "default": 3, **size},
        },
        "type": "object",
    }


def test_optional_null_and_composite_defaults(enrich_spec: OfferingSpec) -> None:
    """``anyOf: [T, null]`` is the contract's ``oneOf``; ``default: null`` and a list or
    object default are no default (decisions I2)."""
    found = compare_handler(
        _doc(enrich_spec, Note), _harness("Note", "Note", {"Note": _note()}), "run"
    )
    assert found == ()


def test_a_scalar_default_is_compared(enrich_spec: OfferingSpec) -> None:
    """A buyer's generated code fills a scalar default in, so a different one is reported."""
    harness = _harness("Note", "Note", {"Note": _note(default=4)})
    found = compare_handler(_doc(enrich_spec, Note), harness, "run")
    assert [(d.side, d.path, d.contract, d.harness, d.kind) for d in found] == [
        ("input", "$defs.Note.properties.size.default", 3, 4, "drift"),
        ("output", "$defs.Note.properties.size.default", 3, 4, "serialization"),
    ]


def test_anyof_with_a_branch_that_may_be_null_is_not_oneof(enrich_spec: OfferingSpec) -> None:
    """``anyOf`` and ``oneOf`` agree only when the other branch cannot be null."""
    loose = _note()
    loose["properties"]["text"] = {"anyOf": [{"type": ["string", "null"]}, {"type": "null"}]}
    found = compare_handler(
        _doc(enrich_spec, Note), _harness("Note", "Note", {"Note": loose}), "run"
    )
    paths = {d.path for d in found}
    assert "$defs.Note.properties.text.anyOf" in paths
    assert "$defs.Note.properties.text.oneOf" in paths


class Cat(BaseModel):
    """A cat."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["cat"]


class Dog(BaseModel):
    """A dog."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["dog"]


class Pet(BaseModel):
    """A tagged union."""

    model_config = ConfigDict(extra="forbid")

    pet: Annotated[Cat | Dog, Field(discriminator="kind")]


def test_the_discriminator_is_not_a_difference(enrich_spec: OfferingSpec) -> None:
    def branch(kind: str) -> dict[str, Any]:
        return {
            "additionalProperties": False,
            "properties": {"kind": {"const": kind, "type": "string"}},
            "required": ["kind"],
            "type": "object",
        }

    defs = {
        "Cat": branch("cat"),
        "Dog": branch("dog"),
        "Pet": {
            "additionalProperties": False,
            "properties": {
                "pet": {
                    "discriminator": {
                        "mapping": {"cat": "#/$defs/Cat", "dog": "#/$defs/Dog"},
                        "propertyName": "kind",
                    },
                    "oneOf": [{"$ref": "#/$defs/Cat"}, {"$ref": "#/$defs/Dog"}],
                }
            },
            "required": ["pet"],
            "type": "object",
        },
    }
    assert compare_handler(_doc(enrich_spec, Pet), _harness("Pet", "Pet", defs), "run") == ()


class Tree(BaseModel):
    """A recursive model."""

    model_config = ConfigDict(extra="forbid")

    name: str
    children: list[Tree]


def test_a_recursive_model_is_compared_once(enrich_spec: OfferingSpec) -> None:
    def tree(name: str, *, kind: str = "string") -> dict[str, Any]:
        return {
            "additionalProperties": False,
            "properties": {
                "name": {"type": kind},
                "children": {"items": {"$ref": f"#/$defs/{name}"}, "type": "array"},
            },
            "required": ["children", "name"],
            "type": "object",
        }

    defs = {"Tree-Input": tree("Tree-Input"), "Tree-Output": tree("Tree-Output", kind="integer")}
    found = compare_handler(
        _doc(enrich_spec, Tree), _harness("Tree-Input", "Tree-Output", defs), "run"
    )
    assert [(d.side, d.path, d.contract, d.harness) for d in found] == [
        ("output", "$defs.Tree.properties.name.type", "string", "integer")
    ]


class Ranged(BaseModel):
    """Numbers and constants."""

    model_config = ConfigDict(extra="forbid")

    n: float = Field(ge=1)
    on: Literal[True]


def test_values_compare_as_canonical_json(enrich_spec: OfferingSpec) -> None:
    """RFC 8785: ``1`` and ``1.0`` are one number; ``true`` and ``1`` are not one value."""
    body = {
        "additionalProperties": False,
        "properties": {
            "n": {"minimum": 1.0, "type": "number"},
            "on": {"const": 1, "type": "boolean"},
        },
        "required": ["n", "on"],
        "type": "object",
    }
    found = compare_handler(
        _doc(enrich_spec, Ranged), _harness("Ranged", "Ranged", {"Ranged": body}), "run"
    )
    assert {d.path for d in found} == {"$defs.Ranged.properties.on.const"}


# --------------------------------------------------------------------------- semantic


class Open(BaseModel):
    """pydantic's default: other keys ignored, so its schema is open."""

    host: str
    hops: int = 1


def _open(**extra: Any) -> dict[str, Any]:
    return {
        "properties": {"host": {"type": "string"}, "hops": {"type": "integer", "default": 1}},
        "required": ["host"],
        "type": "object",
        **extra,
    }


def test_an_open_model_and_defaults_the_output_requires(enrich_spec: OfferingSpec) -> None:
    """The two semantic differences every harness agent shows: the contract closes every
    model (``open``), and the harness's output schema requires a field with a default
    (``defaults``, its serialization mode). ``additionalProperties: true`` is open too."""
    defs = {
        "Open-Input": _open(additionalProperties=True),
        "Open-Output": _open(required=["hops", "host"]),
    }
    found = compare_handler(
        _doc(enrich_spec, Open), _harness("Open-Input", "Open-Output", defs), "run"
    )
    assert [(d.side, d.path, d.contract, d.harness, d.kind) for d in found] == [
        ("input", "$defs.Open.additionalProperties", False, ABSENT, "open"),
        ("output", "$defs.Open.additionalProperties", False, ABSENT, "open"),
        ("output", "$defs.Open.required", ["host"], ["hops", "host"], "defaults"),
    ]


def test_other_differences_are_serialization_on_the_output_and_drift_on_the_input(
    enrich_spec: OfferingSpec,
) -> None:
    """A field one side lacks: on the output it is how the reply is serialized (a computed
    field here); on the input nothing explains it."""
    body = _host()
    body["properties"]["area"] = {"readOnly": True, "type": "integer"}
    body["required"] = ["area", "host", "port"]
    found = compare_handler(
        _doc(enrich_spec, Host), _harness("Host", "Host", {"Host": body}), "run"
    )
    assert _found(found) == {
        ("input", "$defs.Host.properties.area", "drift"),
        ("input", "$defs.Host.required", "drift"),
        ("output", "$defs.Host.properties.area", "serialization"),
        ("output", "$defs.Host.required", "serialization"),
    }
    [area] = [d for d in found if d.side == "output" and d.path.endswith("area")]
    assert (area.contract, area.harness) == (ABSENT, {"type": "integer"})  # canonical: no readOnly


class Stamped(BaseModel):
    """A datetime."""

    model_config = ConfigDict(extra="forbid")

    at: datetime


def test_a_type_change_is_reported_where_it_is(enrich_spec: OfferingSpec) -> None:
    body = {
        "additionalProperties": False,
        "properties": {"at": {"type": "integer"}},
        "required": ["at"],
        "type": "object",
    }
    found = compare_handler(
        _doc(enrich_spec, Stamped), _harness("Stamped", "Stamped", {"Stamped": body}), "run"
    )
    assert _found(found) == {
        ("input", "$defs.Stamped.properties.at.format", "drift"),
        ("input", "$defs.Stamped.properties.at.type", "drift"),
        ("output", "$defs.Stamped.properties.at.format", "serialization"),
        ("output", "$defs.Stamped.properties.at.type", "serialization"),
    }


def test_only_the_handler_is_compared(enrich_spec: OfferingSpec) -> None:
    """The init data, the agent's other handlers and its states are its envelope: a
    difference there is no difference of the operation."""
    harness = _harness("Host", "Host", {"Host": _host(), "Config": {"type": "object"}})
    harness["init_data"] = {"data": {"$ref": "#/$defs/Config"}, "required": False}
    harness["handlers"]["other"] = {
        "input": {"$ref": "#/$defs/Config"},
        "output": {"$ref": "#/$defs/Config"},
    }
    harness["states"] = {"board": {"$ref": "#/$defs/Config"}}
    assert compare_handler(_doc(enrich_spec, Host), harness, "run") == ()


def test_a_handler_the_harness_does_not_have_is_an_error(enrich_spec: OfferingSpec) -> None:
    doc = _doc(enrich_spec, Host)
    with pytest.raises(HarnessSchemaError, match="no handler 'sweep'"):
        compare_handler(doc, _harness("Host", "Host", {"Host": _host()}), "sweep")
    with pytest.raises(HarnessSchemaError, match="no handler"):
        compare_handler(doc, {"handlers": []}, "run")
    broken = _harness("Host", "Host", {"Host": _host()})
    del broken["handlers"]["run"]["output"]
    with pytest.raises(HarnessSchemaError, match="no output"):
        compare_handler(doc, broken, "run")


# --------------------------------------------------------------------------- what a person reads


def test_describe_says_each_difference_then_what_each_kind_means(enrich_spec: OfferingSpec) -> None:
    defs = {"Open-Input": _open(), "Open-Output": _open(required=["hops", "host"])}
    found = compare_handler(
        _doc(enrich_spec, Open), _harness("Open-Input", "Open-Output", defs), "run"
    )
    lines = h.describe(found)
    assert lines[:3] == [
        "input  $defs.Open.additionalProperties: contract false, harness (absent)",
        "output $defs.Open.additionalProperties: contract false, harness (absent)",
        'output $defs.Open.required: contract ["host"], harness ["hops","host"]',
    ]
    assert lines[3] == f"(open) {h.NOTES['open']}"
    assert lines[4] == f"(defaults) {h.NOTES['defaults']}"
    assert len(lines) == 5
    assert h.describe(()) == []


def test_a_long_value_is_cut_and_control_characters_are_escaped() -> None:
    d = Difference("input", "$defs.A.description", "x" * 500, "a\x1b[31mb", "drift")
    text = str(d)
    assert "\x1b" not in text
    assert "\\u001b" in text
    assert len(text) < 260


# --------------------------------------------------------------------------- running the harness


class _Agent:
    pass


def test_the_target_is_module_and_qualified_name() -> None:
    assert h.agent_target(_Agent) == f"{__name__}:_Agent"
    for bad in ("agents", "agents:", "agents:A b", "-m:A", "a..b:A", "agents:A;rm"):
        with pytest.raises(HarnessSchemaError, match=r"not module\.path:ClassName"):
            h.harness_schema(bad)


def test_a_harness_that_cannot_run_says_why(monkeypatch: pytest.MonkeyPatch) -> None:
    """No such console script in this environment: the child's own error, in one line."""
    monkeypatch.setattr(h, "HARNESS_SCRIPT", "kontra-no-such-script")
    with pytest.raises(HarnessSchemaError) as info:
        h.harness_schema("agents:Agent")
    assert "kontra-no-such-script schema agents:Agent" in str(info.value)
    assert "\n" not in str(info.value)
