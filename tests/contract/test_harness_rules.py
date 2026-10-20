"""The harness check's canonical form and verdicts, rule by rule (decisions I33), on
contracts derived as ``kontra publish`` derives them and harness documents written out by
hand in the shape ``agent_schema`` emits. None of this needs the harness installed:
``test_harness.py`` runs the same comparison against the harness itself."""

from __future__ import annotations

import enum
import tracemalloc
from datetime import datetime
from typing import Annotated, Any, Literal

import pytest
from pydantic import BaseModel, ConfigDict, Field, RootModel, StringConstraints

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


class Code(RootModel[Annotated[str, StringConstraints(max_length=5)]]):
    """A constrained string with a name of its own."""


class Overlap(BaseModel):
    """``max_length`` on the type and where it is used: pydantic enforces both."""

    model_config = ConfigDict(extra="forbid")

    code: Annotated[Code, Field(max_length=10)]


def _overlap(code: dict[str, Any], site: dict[str, Any]) -> dict[str, Any]:
    """``Overlap`` as ``agent_schema`` writes it, with ``Code``'s body and the use site's
    members beside its ``$ref``."""
    return {
        "Code": {"title": "Code", "description": "A constrained string.", **code},
        "Overlap": {
            "additionalProperties": False,
            "properties": {"code": {"$ref": "#/$defs/Code", **site}},
            "required": ["code"],
            "title": "Overlap",
            "type": "object",
        },
    }


def test_a_constraint_on_both_a_type_and_its_use_is_both(enrich_spec: OfferingSpec) -> None:
    """JSON Schema 2020-12 asserts a ``$ref``'s definition and its siblings both, so the
    harness's ``{$ref: Code, maxLength: 10}`` over ``Code``'s ``maxLength: 5`` accepts at most
    5 characters, as the model does; the contract says 10. A use site that writes what its
    definition also writes is compared as the ``allOf`` of the two, and reported (``drift``
    on both sides: it is the emitters, not the serialization)."""
    defs = _overlap({"maxLength": 5, "type": "string"}, {"maxLength": 10})
    found = compare_handler(_doc(enrich_spec, Overlap), _harness("Overlap", "Overlap", defs), "run")
    where = "$defs.Overlap.properties.code"
    both = [{"maxLength": 5, "type": "string"}, {"maxLength": 10}]
    assert [(d.side, d.path, d.contract, d.harness, d.kind) for d in found] == [
        ("input", f"{where}.allOf", ABSENT, both, "drift"),
        ("input", f"{where}.maxLength", 10, ABSENT, "drift"),
        ("input", f"{where}.type", "string", ABSENT, "drift"),
        ("output", f"{where}.allOf", ABSENT, both, "drift"),
        ("output", f"{where}.maxLength", 10, ABSENT, "drift"),
        ("output", f"{where}.type", "string", ABSENT, "drift"),
    ]


class Letter(enum.Enum):
    A = "a"
    B = "b"


class Extra(BaseModel):
    """``json_schema_extra`` writing over an enum's own ``enum``."""

    model_config = ConfigDict(extra="forbid")

    c: Letter = Field(json_schema_extra={"enum": ["a", "z"]})


def test_json_schema_extra_over_a_named_type_is_both(enrich_spec: OfferingSpec) -> None:
    """The contract accepts ``"z"``; the harness's schema, and the model, do not."""
    defs = {
        "Letter": {"enum": ["a", "b"], "title": "Letter", "type": "string"},
        "Extra": {
            "additionalProperties": False,
            "properties": {"c": {"$ref": "#/$defs/Letter", "enum": ["a", "z"]}},
            "required": ["c"],
            "type": "object",
        },
    }
    found = compare_handler(_doc(enrich_spec, Extra), _harness("Extra", "Extra", defs), "run")
    assert _found(found) == {
        (side, f"$defs.Extra.properties.c.{key}", "drift")
        for side in ("input", "output")
        for key in ("allOf", "enum", "type")
    }


@pytest.mark.parametrize(
    ("code", "site"),
    [
        ({"type": "string"}, {"maxLength": 5}),  # a constraint only the use site writes
        ({"maxLength": 5, "type": "string"}, {"maxLength": 5}),  # the same one twice
        # The use site's annotations are its own: a field's description over its type's.
        ({"maxLength": 5, "type": "string"}, {"description": "The code.", "examples": ["ab"]}),
    ],
)
def test_a_use_site_that_changes_nothing_joins_its_definition(
    enrich_spec: OfferingSpec, code: dict[str, Any], site: dict[str, Any]
) -> None:
    class Overlap(BaseModel):
        model_config = ConfigDict(extra="forbid")

        code: str = Field(max_length=5)

    found = compare_handler(
        _doc(enrich_spec, Overlap), _harness("Overlap", "Overlap", _overlap(code, site)), "run"
    )
    assert found == ()


@pytest.mark.parametrize(
    ("body", "site"),
    [
        # additionalProperties reads properties beside it: alone at the use site it refuses
        # every key, beside the definition's properties only the others.
        ({"properties": {"a": {}}, "type": "object"}, {"additionalProperties": False}),
        (
            {"properties": {"a": {}}, "additionalProperties": False},
            {"additionalProperties": False},
        ),
        ({"prefixItems": [{}], "type": "array"}, {"items": False}),
        ({"if": {}, "then": {}}, {"else": False}),
        ({"type": "object"}, {"unevaluatedProperties": False}),
        ({"type": "string"}, {"type": "integer"}),
    ],
)
def test_a_use_site_that_would_change_its_definition_stays_beside_it(
    body: dict[str, Any], site: dict[str, Any]
) -> None:
    """Keywords that read each other are never split between the use site and the
    definition: the node stays the conjunction."""
    resolved = h._resolve({"$ref": "#/$defs/T", "title": "Use", **site}, {"T": body})
    assert resolved == {"title": "Use", "allOf": [body, site]}


def test_null_inside_a_conjunction(enrich_spec: OfferingSpec) -> None:
    """An optional field over such a use site is still ``oneOf`` against ``anyOf``: the
    ``allOf`` cannot be null when one of its branches cannot, so the difference is reported
    inside the branch, not as the union."""

    class Maybe(BaseModel):
        model_config = ConfigDict(extra="forbid")

        maybe: Annotated[Code, Field(max_length=10)] | None

    defs = _overlap({"maxLength": 5, "type": "string"}, {})
    defs["Maybe"] = {
        "additionalProperties": False,
        "properties": {
            "maybe": {
                "anyOf": [{"$ref": "#/$defs/Code", "maxLength": 10}, {"type": "null"}],
            }
        },
        "required": ["maybe"],
        "type": "object",
    }
    found = compare_handler(_doc(enrich_spec, Maybe), _harness("Maybe", "Maybe", defs), "run")
    assert {d.path for d in found} == {
        f"$defs.Maybe.properties.maybe.oneOf[0].{key}" for key in ("allOf", "maxLength", "type")
    }


# --------------------------------------------------------------------------- semantic


class Open(BaseModel):
    """pydantic's default, ``extra="ignore"``: other keys dropped on input and never
    emitted, and its schema says nothing of them."""

    host: str
    hops: int = 1


class Allowed(BaseModel):
    """``extra="allow"``: other keys kept and serialized, so its schema says
    ``additionalProperties: true``."""

    model_config = ConfigDict(extra="allow")

    host: str
    hops: int = 1


class Holder(BaseModel):
    """pydantic's default, around both."""

    allowed: Allowed
    ignored: Open


def _open(**extra: Any) -> dict[str, Any]:
    """``Open`` as ``agent_schema`` writes it, or ``Allowed`` with
    ``additionalProperties=True``."""
    return {
        "properties": {"host": {"type": "string"}, "hops": {"type": "integer", "default": 1}},
        "required": ["host"],
        "type": "object",
        **extra,
    }


def test_the_contract_closes_models_by_policy(enrich_spec: OfferingSpec) -> None:
    """Kontra closes every model (decisions I2, section 11.3) and the harness's schema
    writes no ``additionalProperties`` for pydantic's default ``extra="ignore"``, which
    drops other keys on input and never emits one: no value exchanged differs, so neither
    side reports it. What does differ still is: the harness's output schema requires a
    field with a default (``defaults``, its serialization mode)."""
    defs = {"Open-Input": _open(), "Open-Output": _open(required=["hops", "host"])}
    found = compare_handler(
        _doc(enrich_spec, Open), _harness("Open-Input", "Open-Output", defs), "run"
    )
    assert [(d.side, d.path, d.contract, d.harness, d.kind) for d in found] == [
        ("output", "$defs.Open.required", ["host"], ["hops", "host"], "defaults"),
    ]


@pytest.mark.parametrize("model", [Allowed, Holder])
def test_additional_properties_true_is_open(enrich_spec: OfferingSpec, model: type[Any]) -> None:
    """``extra="allow"``: the harness's ``additionalProperties: true`` is kept, at the root
    as nested, and against the contract's ``false`` is ``open`` on both sides, with both
    values. The ``extra="ignore"`` models around and beside it are not."""
    defs = {
        "Allowed": _open(additionalProperties=True),
        "Open": _open(),
        "Holder": {
            "properties": {
                "allowed": {"$ref": "#/$defs/Allowed"},
                "ignored": {"$ref": "#/$defs/Open"},
            },
            "required": ["allowed", "ignored"],
            "type": "object",
        },
    }
    name = model.__name__
    found = compare_handler(_doc(enrich_spec, model), _harness(name, name, defs), "run")
    assert [(d.side, d.path, d.contract, d.harness, d.kind) for d in found] == [
        ("input", "$defs.Allowed.additionalProperties", False, True, "open"),
        ("output", "$defs.Allowed.additionalProperties", False, True, "open"),
    ]


def _extra(contract: Any, harness: Any) -> list[tuple[Any, Any, str]]:
    """The input differences between two objects that differ only in their
    ``additionalProperties`` (each a value, or :data:`ABSENT` for none)."""

    def node(value: Any) -> dict[str, Any]:
        return {"type": "object"} | ({} if value is ABSENT else {"additionalProperties": value})

    walk = h._Walk("input", {}, {})
    walk.run(node(contract), node(harness), "x")
    return [(d.contract, d.harness, d.kind) for d in walk.found]


@pytest.mark.parametrize(
    ("contract", "harness", "found"),
    [
        # The contract closes models by policy.
        (False, ABSENT, []),
        (False, False, []),
        # true asserts nothing: it is the member's absence, on either side ...
        (True, ABSENT, []),
        (ABSENT, True, []),
        (True, True, []),
        # ... except against the contract's false, where it is kept: extra="allow".
        (False, True, [(False, True, "open")]),
        (True, False, [(True, False, "drift")]),
        (ABSENT, False, [(ABSENT, False, "drift")]),
        # A schema (typed extras) is no true: compared like any other member.
        (False, {"type": "integer"}, [(False, {"type": "integer"}, "drift")]),
        ({"type": "integer"}, {"type": "integer"}, []),
    ],
)
def test_additional_properties_pair_by_pair(contract: Any, harness: Any, found: list[Any]) -> None:
    """Each pair of ``additionalProperties``, the contract's against the harness's: ``true``
    survives the canonical form on both sides, and only the comparison reads it."""
    assert _extra(contract, harness) == found


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
    defs = {
        "Allowed-Input": _open(additionalProperties=True),
        "Allowed-Output": _open(additionalProperties=True, required=["hops", "host"]),
    }
    found = compare_handler(
        _doc(enrich_spec, Allowed), _harness("Allowed-Input", "Allowed-Output", defs), "run"
    )
    lines = h.describe(found)
    assert lines[:3] == [
        "input  $defs.Allowed.additionalProperties: contract false, harness true",
        "output $defs.Allowed.additionalProperties: contract false, harness true",
        'output $defs.Allowed.required: contract ["host"], harness ["hops","host"]',
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


def test_a_harness_that_floods_stderr_is_read_from_the_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The seller's module is imported in the child and may log without bound: only the end
    of its error output is read, for the one line a refusal quotes."""
    flood = 16 * 1024 * 1024
    monkeypatch.setattr(
        h,
        "_RUN_SCRIPT",
        "import sys\n"
        f"sys.stderr.write('noise ' * {flood // 6} + '\\n')\n"
        "sys.stderr.write('the last line\\n')\n"
        "sys.exit(1)\n",
    )
    tracemalloc.start()
    try:
        with pytest.raises(HarnessSchemaError, match=r"the last line$"):
            h.harness_schema("agents:Agent")
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < flood // 8


def test_a_harness_that_does_not_answer_is_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    """The limit holds while the child writes nothing, and while it writes without end."""
    for program in (
        "import time\ntime.sleep(30)\n",
        "import sys\nwhile True: sys.stderr.write('x' * 4096)\n",
    ):
        monkeypatch.setattr(h, "_RUN_SCRIPT", program)
        with pytest.raises(HarnessSchemaError, match=r"did not answer within 0\.5s"):
            h.harness_schema("agents:Agent", timeout=0.5)


def test_a_schema_past_the_limit_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """The document is read up to a limit, never whole whatever its size."""
    monkeypatch.setattr(
        h,
        "_RUN_SCRIPT",
        "import sys\nopen(sys.argv[-1], 'w').write('[' + '0,' * 2048 + '0]')\n",
    )
    monkeypatch.setattr(h, "_SCHEMA_BYTES", 1024)
    with pytest.raises(HarnessSchemaError, match="larger than 1024 bytes"):
        h.harness_schema("agents:Agent")
