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
from pydantic import BaseModel, ConfigDict, Field, StringConstraints
from typing_extensions import TypeAliasType

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


def _found(differences: tuple[Difference, ...]) -> set[tuple[str, str]]:
    """Where each difference is; none of them ``open``."""
    assert not any(d.open for d in differences)
    return {(d.side, d.path) for d in differences}


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
    def paint(*required: str) -> dict[str, Any]:
        return {
            "additionalProperties": False,
            "properties": {
                "color": {"$ref": "#/$defs/Color", "default": "red"},
                "maybe": {"anyOf": [{"$ref": "#/$defs/Color"}, {"type": "null"}]},
            },
            "required": list(required),
            "type": "object",
        }

    defs = {
        "Color": {"enum": ["red", "blue"], "title": "Color", "type": "string"},
        "Paint-Input": paint("maybe"),
        "Paint-Output": paint("color", "maybe"),
    }
    harness = _harness("Paint-Input", "Paint-Output", defs)
    assert compare_handler(_doc(enrich_spec, Paint), harness, "run") == ()


class Note(BaseModel):
    """Optional fields."""

    model_config = ConfigDict(extra="forbid")

    text: str | None = None
    tags: list[str] = []
    size: int = 3


def _note(*, output: bool = False, **size: Any) -> dict[str, Any]:
    """``Note`` as ``agent_schema`` writes it: an input, or an output (serialization mode),
    which requires every field and keeps each default beside it."""
    return {
        "additionalProperties": False,
        "properties": {
            "text": {"anyOf": [{"type": "string"}, {"type": "null"}], "default": None},
            "tags": {"items": {"type": "string"}, "type": "array", "default": []},
            "size": {"type": "integer", "default": 3, **size},
        },
        "type": "object",
        **({"required": ["text", "tags", "size"]} if output else {}),
    }


def _notes(inp: dict[str, Any], out: dict[str, Any]) -> dict[str, Any]:
    return _harness("Note-Input", "Note-Output", {"Note-Input": inp, "Note-Output": out})


def test_optional_null_and_composite_defaults(enrich_spec: OfferingSpec) -> None:
    """``anyOf: [T, null]`` is the contract's ``oneOf``; ``default: null`` and a list or
    object default are no default (decisions I2)."""
    found = compare_handler(_doc(enrich_spec, Note), _notes(_note(), _note(output=True)), "run")
    assert found == ()


def test_a_scalar_default_is_compared(enrich_spec: OfferingSpec) -> None:
    """A buyer's generated code fills a scalar default in, so a different one is reported."""
    harness = _notes(_note(default=4), _note(output=True))
    found = compare_handler(_doc(enrich_spec, Note), harness, "run")
    assert found == (Difference("input", "$defs.NoteInput.properties.size.default", 3, 4),)


def test_a_default_on_a_required_member_is_no_default(enrich_spec: OfferingSpec) -> None:
    """The harness's output schema requires every field the reply carries and keeps each
    one's default beside it (``size: {default: 3}``, and ``4`` here); the contract writes
    none there (decisions I38). A required member is never absent, so its default never
    applies: no difference, whatever its value. Not required, it is compared."""
    doc = _doc(enrich_spec, Note)
    assert compare_handler(doc, _notes(_note(), _note(output=True, default=4)), "run") == ()
    optional = _note(output=True, default=4)
    optional["required"] = ["text", "tags"]
    assert compare_handler(doc, _notes(_note(), optional), "run") == (
        Difference("output", "$defs.NoteOutput.properties.size.default", ABSENT, 4),
        Difference(
            "output", "$defs.NoteOutput.required", ["size", "tags", "text"], ["tags", "text"]
        ),
    )


def test_anyof_with_a_branch_that_may_be_null_is_not_oneof(enrich_spec: OfferingSpec) -> None:
    """``anyOf`` and ``oneOf`` agree only when the other branch cannot be null."""
    loose = _note()
    loose["properties"]["text"] = {"anyOf": [{"type": ["string", "null"]}, {"type": "null"}]}
    found = compare_handler(_doc(enrich_spec, Note), _notes(loose, _note(output=True)), "run")
    assert _found(found) == {
        ("input", "$defs.NoteInput.properties.text.anyOf"),
        ("input", "$defs.NoteInput.properties.text.oneOf"),
    }


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
        ("output", "$defs.TreeOutput.properties.name.type", "string", "integer")
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


# PEP 695 aliases (``type Short = ...`` from Python 3.12), spelled for 3.11. A bound on an
# alias of a constrained alias and again where it is used: pydantic measures the string and
# enforces both, and writes ``{$ref: Documented, maxLength: 10}``. (On ``Short`` itself it
# would replace the alias's bound with the field's, in its schema and its validation alike.)
Short = TypeAliasType("Short", Annotated[str, StringConstraints(max_length=5)])
Documented = TypeAliasType("Documented", Annotated[Short, Field(description="A short text.")])


class Overlap(BaseModel):
    """``max_length`` on the type and where it is used: at most 5 characters."""

    model_config = ConfigDict(extra="forbid")

    code: Annotated[Documented, Field(max_length=10)]


def _overlap(code: dict[str, Any], site: dict[str, Any], *, alias: bool = False) -> dict[str, Any]:
    """``Overlap`` as ``agent_schema`` writes it, with ``Short``'s body and the use site's
    members beside its ``$ref`` (to ``Documented``, an alias of ``Short``, with ``alias``)."""
    used = "Documented" if alias else "Short"
    return {
        "Short": {"title": "Short", "description": "A constrained string.", **code},
        "Documented": {"$ref": "#/$defs/Short", "description": "A short text."},
        "Overlap": {
            "additionalProperties": False,
            "properties": {"code": {"$ref": f"#/$defs/{used}", **site}},
            "required": ["code"],
            "title": "Overlap",
            "type": "object",
        },
    }


def test_a_constraint_on_both_a_type_and_its_use_is_their_conjunction(
    enrich_spec: OfferingSpec,
) -> None:
    """JSON Schema 2020-12 asserts a ``$ref``'s definition and its siblings both, so the
    harness's ``{$ref: Documented, maxLength: 10}`` over ``Short``'s ``maxLength: 5`` accepts
    at most 5 characters, as the model does, and so does the contract (decisions I38): the
    tighter bound, no difference."""
    doc = _doc(enrich_spec, Overlap)
    assert doc["$defs"]["Overlap"]["properties"]["code"] == {
        "description": "A short text.",
        "maxLength": 5,
        "type": "string",
    }
    defs = _overlap({"maxLength": 5, "type": "string"}, {"maxLength": 10}, alias=True)
    assert compare_handler(doc, _harness("Overlap", "Overlap", defs), "run") == ()


def test_a_use_site_no_one_value_conjoins_stays_beside_its_definition(
    enrich_spec: OfferingSpec,
) -> None:
    """Two patterns have no one-value conjunction: the harness's node is the ``allOf`` of
    the two, which the contract never writes (derivation refuses such a field, I38), and it
    is reported as it is, on both sides."""
    defs = _overlap({"pattern": "^a", "type": "string"}, {"pattern": "^b"})
    found = compare_handler(_doc(enrich_spec, Overlap), _harness("Overlap", "Overlap", defs), "run")
    where = "$defs.Overlap.properties.code"
    both = [{"pattern": "^a", "type": "string"}, {"pattern": "^b"}]
    assert found == tuple(
        Difference(side, f"{where}.{key}", contract, harness)
        for side in ("input", "output")
        for key, contract, harness in (
            ("allOf", ABSENT, both),
            ("maxLength", 5, ABSENT),
            ("type", "string", ABSENT),
        )
    )


class Letter(enum.Enum):
    A = "a"
    B = "b"


class Extra(BaseModel):
    """``json_schema_extra`` writing over an enum's own ``enum``."""

    model_config = ConfigDict(extra="forbid")

    c: Letter = Field(json_schema_extra={"enum": ["a", "z"]})


def test_json_schema_extra_over_a_named_type_is_their_conjunction(
    enrich_spec: OfferingSpec,
) -> None:
    """Both sides accept ``"a"`` alone: the values the two ``enum``s share."""
    defs = {
        "Letter": {"enum": ["a", "b"], "title": "Letter", "type": "string"},
        "Extra": {
            "additionalProperties": False,
            "properties": {"c": {"$ref": "#/$defs/Letter", "enum": ["a", "z"]}},
            "required": ["c"],
            "type": "object",
        },
    }
    doc = _doc(enrich_spec, Extra)
    assert doc["$defs"]["Extra"]["properties"]["c"] == {"enum": ["a"], "type": "string"}
    assert compare_handler(doc, _harness("Extra", "Extra", defs), "run") == ()


@pytest.mark.parametrize(
    ("code", "site"),
    [
        ({"type": "string"}, {"maxLength": 5}),  # a constraint only the use site writes
        ({"maxLength": 5, "type": "string"}, {"maxLength": 5}),  # the same one twice
        ({"maxLength": 8, "type": "string"}, {"maxLength": 5}),  # the tighter of two
        ({"maxLength": 5, "type": "string"}, {"maxLength": 8}),
        # The use site's annotations are its own: a field's description over its type's.
        ({"maxLength": 5, "type": "string"}, {"description": "The code.", "examples": ["ab"]}),
    ],
)
def test_a_use_site_joins_its_definition_as_their_conjunction(
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
    ("body", "site", "joined"),
    [
        # The tighter bound of each pair, on both sides of a range.
        (
            {"maximum": 10, "minimum": 0, "type": "integer"},
            {"maximum": 5, "minimum": 1},
            {"maximum": 5, "minimum": 1, "type": "integer"},
        ),
        (
            {"exclusiveMaximum": 1, "exclusiveMinimum": 0, "type": "number"},
            {"exclusiveMaximum": 2, "exclusiveMinimum": -1},
            {"exclusiveMaximum": 1, "exclusiveMinimum": 0, "type": "number"},
        ),
        (
            {"maxItems": 3, "minItems": 1, "type": "array"},
            {"maxItems": 2, "minItems": 2},
            {"maxItems": 2, "minItems": 2, "type": "array"},
        ),
        # integer within number; the least common multiple; uniqueness if either says so.
        ({"type": "number"}, {"type": "integer"}, {"type": "integer"}),
        ({"multipleOf": 4}, {"multipleOf": 6}, {"multipleOf": 12}),
        ({"uniqueItems": False}, {"uniqueItems": True}, {"uniqueItems": True}),
        # The values both enums list, in the definition's order.
        ({"enum": ["c", "b", "a"]}, {"enum": ["a", "c", "z"]}, {"enum": ["c", "a"]}),
        # A group of keywords that read each other joins a definition that writes none of it.
        (
            {"type": "object"},
            {"additionalProperties": False, "properties": {"a": {}}},
            {"additionalProperties": False, "properties": {"a": {}}, "type": "object"},
        ),
    ],
)
def test_a_use_site_s_assertions_are_conjoined(
    body: dict[str, Any], site: dict[str, Any], joined: dict[str, Any]
) -> None:
    resolved = h._resolve({"$ref": "#/$defs/T", "title": "Use", **site}, {"T": body})
    assert resolved == {**joined, "title": "Use"}


def test_an_alias_of_a_named_type_is_followed() -> None:
    """``{$ref: Documented, maxLength: 10}``, ``Documented`` itself ``{$ref: Short,
    description}``: the chain resolves to ``Short``'s body, its annotation and the bound."""
    defs = {
        "Short": {"maxLength": 5, "type": "string"},
        "Documented": {"$ref": "#/$defs/Short", "description": "A short text."},
    }
    resolved = h._resolve({"$ref": "#/$defs/Documented", "maxLength": 10}, defs)
    assert resolved == {"maxLength": 5, "type": "string", "description": "A short text."}
    loop = {"A": {"$ref": "#/$defs/B"}, "B": {"$ref": "#/$defs/A"}}
    assert "allOf" not in h._resolve({"$ref": "#/$defs/A"}, loop)  # bounded, never endless


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
        # No one value says what the two do.
        ({"type": "string"}, {"type": "integer"}),
        ({"pattern": "^a"}, {"pattern": "^b"}),
        ({"format": "date"}, {"format": "uuid"}),
        ({"const": "a"}, {"const": "b"}),
        ({"enum": ["a"]}, {"enum": ["b"]}),
        ({"multipleOf": 0.5}, {"multipleOf": 2}),
    ],
)
def test_a_use_site_that_would_change_its_definition_stays_beside_it(
    body: dict[str, Any], site: dict[str, Any]
) -> None:
    """Keywords that read each other are never split between the use site and the
    definition, and two values no one value conjoins stay apart: the node stays the
    conjunction."""
    resolved = h._resolve({"$ref": "#/$defs/T", "title": "Use", **site}, {"T": body})
    assert resolved == {"title": "Use", "allOf": [body, site]}


def test_null_inside_a_conjunction(enrich_spec: OfferingSpec) -> None:
    """An optional field over such a use site is still ``oneOf`` against ``anyOf``: the
    ``allOf`` cannot be null when one of its branches cannot, so the difference is reported
    inside the branch, not as the union."""

    class Maybe(BaseModel):
        model_config = ConfigDict(extra="forbid")

        maybe: Annotated[str, Field(pattern="^a")] | None

    defs = _overlap({"pattern": "^a", "type": "string"}, {})
    defs["Maybe"] = {
        "additionalProperties": False,
        "properties": {
            "maybe": {
                "anyOf": [{"$ref": "#/$defs/Short", "pattern": "^b"}, {"type": "null"}],
            }
        },
        "required": ["maybe"],
        "type": "object",
    }
    found = compare_handler(_doc(enrich_spec, Maybe), _harness("Maybe", "Maybe", defs), "run")
    assert _found(found) == {
        (side, f"$defs.Maybe.properties.maybe.oneOf[0].{key}")
        for side in ("input", "output")
        for key in ("allOf", "pattern", "type")
    }


# --------------------------------------------------------------------------- what is reported


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


def _open(*, output: bool = False, **extra: Any) -> dict[str, Any]:
    """``Open`` as ``agent_schema`` writes it (an input, or an output, which requires
    ``hops`` beside its default), or ``Allowed`` with ``additionalProperties=True``."""
    return {
        "properties": {"host": {"type": "string"}, "hops": {"type": "integer", "default": 1}},
        "required": ["host", "hops"] if output else ["host"],
        "type": "object",
        **extra,
    }


def test_the_contract_closes_models_by_policy(enrich_spec: OfferingSpec) -> None:
    """Kontra closes every model (decisions I2, section 11.3) and the harness's schema
    writes no ``additionalProperties`` for pydantic's default ``extra="ignore"``, which
    drops other keys on input and never emits one: no value exchanged differs, so neither
    side reports it."""
    defs = {"Open-Input": _open(), "Open-Output": _open(output=True)}
    found = compare_handler(
        _doc(enrich_spec, Open), _harness("Open-Input", "Open-Output", defs), "run"
    )
    assert found == ()


@pytest.mark.parametrize("model", [Allowed, Holder])
def test_additional_properties_true_is_open(enrich_spec: OfferingSpec, model: type[Any]) -> None:
    """``extra="allow"``: the harness's ``additionalProperties: true`` is kept, at the root
    as nested, and against the contract's ``false`` is ``open`` on both sides, with both
    values. The ``extra="ignore"`` models around and beside it are not."""

    def holder(mode: str) -> dict[str, Any]:
        return {
            "properties": {
                "allowed": {"$ref": f"#/$defs/Allowed-{mode}"},
                "ignored": {"$ref": f"#/$defs/Open-{mode}"},
            },
            "required": ["allowed", "ignored"],
            "type": "object",
        }

    defs = {
        **{
            f"Allowed-{m}": _open(output=m == "Output", additionalProperties=True)
            for m in ("Input", "Output")
        },
        **{f"Open-{m}": _open(output=m == "Output") for m in ("Input", "Output")},
        **{f"Holder-{m}": holder(m) for m in ("Input", "Output")},
    }
    name = model.__name__
    found = compare_handler(
        _doc(enrich_spec, model), _harness(f"{name}-Input", f"{name}-Output", defs), "run"
    )
    assert found == (
        Difference("input", "$defs.AllowedInput.additionalProperties", False, True, open=True),
        Difference("output", "$defs.AllowedOutput.additionalProperties", False, True, open=True),
    )


def _extra(contract: Any, harness: Any) -> list[tuple[Any, Any, bool]]:
    """The input differences between two objects that differ only in their
    ``additionalProperties`` (each a value, or :data:`ABSENT` for none)."""

    def node(value: Any) -> dict[str, Any]:
        return {"type": "object"} | ({} if value is ABSENT else {"additionalProperties": value})

    walk = h._Walk("input", {}, {})
    walk.run(node(contract), node(harness), "x")
    return [(d.contract, d.harness, d.open) for d in walk.found]


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
        (False, True, [(False, True, True)]),
        (True, False, [(True, False, False)]),
        (ABSENT, False, [(ABSENT, False, False)]),
        # A schema there is typed extras (__pydantic_extra__: dict[str, int]): extra="allow".
        (False, {"type": "integer"}, [(False, {"type": "integer"}, True)]),
        (False, {}, [(False, {}, True)]),
        # A map's values: compared like any other member.
        ({"type": "integer"}, {"type": "integer"}, []),
        ({"type": "integer"}, {"type": "string"}, [("integer", "string", False)]),
    ],
)
def test_additional_properties_pair_by_pair(contract: Any, harness: Any, found: list[Any]) -> None:
    """Each pair of ``additionalProperties``, the contract's against the harness's: ``true``
    survives the canonical form on both sides, and only the comparison reads it; against
    the contract's ``false`` it is ``open``, and so is a schema (typed extras)."""
    assert _extra(contract, harness) == found


def test_any_other_difference_is_reported_as_it_is(enrich_spec: OfferingSpec) -> None:
    """A field one side lacks (here a computed one the contract does not have), on either
    side: the emitters disagree, and nothing explains it away."""
    body = _host()
    body["properties"]["area"] = {"readOnly": True, "type": "integer"}
    body["required"] = ["area", "host", "port"]
    found = compare_handler(
        _doc(enrich_spec, Host), _harness("Host", "Host", {"Host": body}), "run"
    )
    assert _found(found) == {
        (side, f"$defs.Host.{key}")
        for side in ("input", "output")
        for key in ("properties.area", "required")
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
        (side, f"$defs.Stamped.properties.at.{key}")
        for side in ("input", "output")
        for key in ("format", "type")
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


def test_describe_says_each_difference_then_what_they_mean(enrich_spec: OfferingSpec) -> None:
    defs = {
        "Allowed-Input": _open(additionalProperties=True),
        "Allowed-Output": _open(output=True, additionalProperties=True),
    }
    defs["Allowed-Output"]["properties"]["hops"]["type"] = "string"
    found = compare_handler(
        _doc(enrich_spec, Allowed), _harness("Allowed-Input", "Allowed-Output", defs), "run"
    )
    assert h.describe(found) == [
        "input  $defs.AllowedInput.additionalProperties: contract false, harness true",
        "output $defs.AllowedOutput.additionalProperties: contract false, harness true",
        'output $defs.AllowedOutput.properties.hops.type: contract "integer", harness "string"',
        f"(open input) {h.OPEN_INPUT_NOTE}",
        f"(open output) {h.OPEN_OUTPUT_NOTE}",
        f"the other lines: {h.OTHER_NOTE}",
    ]
    assert h.describe(found[:2]) == [*h.describe(found)[:2], *h.describe(found)[3:5]]
    assert h.describe(found[2:]) == [h.describe(found)[2], h.OTHER_NOTE]
    assert h.describe(()) == []


def test_an_open_model_says_what_it_does_on_its_side(enrich_spec: OfferingSpec) -> None:
    """An open output fails a job whose reply carries another key; an open input fails
    none, since the contract and the gate refuse other keys before the model sees them, and
    only the seller's harness-codegen types are wider. Each side's note is printed only for
    a line of that side."""
    defs = {"Allowed": _open(additionalProperties=True), "Open": _open(output=True)}
    found = compare_handler(
        _doc(enrich_spec, Allowed, Open), _harness("Allowed", "Open", defs), "run"
    )
    assert h.describe(found) == [
        "input  $defs.Allowed.additionalProperties: contract false, harness true",
        f"(open input) {h.OPEN_INPUT_NOTE}",
    ]
    assert "fails the job" in h.OPEN_OUTPUT_NOTE
    assert "no job fails" in h.OPEN_INPUT_NOTE
    defs = {"Open": _open(), "Allowed": _open(output=True, additionalProperties=True)}
    found = compare_handler(
        _doc(enrich_spec, Open, Allowed), _harness("Open", "Allowed", defs), "run"
    )
    assert h.describe(found) == [
        "output $defs.Allowed.additionalProperties: contract false, harness true",
        f"(open output) {h.OPEN_OUTPUT_NOTE}",
    ]


def test_a_long_value_is_cut_and_control_characters_are_escaped() -> None:
    d = Difference("input", "$defs.A.description", "x" * 500, "a\x1b[31mb")
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
