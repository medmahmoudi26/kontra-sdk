"""Seam ③: a ``$ref``'s siblings are conjoined, not overridden (decisions I38, kontra-sdk#64).

An enum or a constrained scalar that pydantic names in ``$defs`` is written out where it is
used. JSON Schema asserts a ``$ref``'s definition and the members beside it both, so the
written-out schema must assert what the two did: the use site's annotations replace the
definition's, a keyword both constrain is conjoined, and one whose conjunction nexgen's
subset cannot write -- or that no value meets -- is refused at derivation, naming the field
and both values.

Where pydantic enforces a use site's constraint itself, the contract is checked against it
value by value (:func:`agree`): a PEP 695 alias of a constrained alias is measured as its
string, both bounds hold, and the conjunction is exact. A ``Field`` constraint on a model,
a ``RootModel``, a plain ``Enum`` or an ``Optional`` alias is applied by pydantic to the
instance (or to null), which it cannot measure: the model refuses every such value, so the
derivation refuses the field instead of publishing a bound that looks sound."""

from __future__ import annotations

import enum
import json
from typing import Annotated, Any

import pytest
from pydantic import BaseModel, Field, RootModel, StringConstraints, create_model
from typing_extensions import TypeAliasType

from kontra.contract import ContractError, InputValidator, OfferingSpec, derive_document

from .zoo import Code, Color, EnumOverlap, Leaf, Overlap, Plain


def field_schema(spec: OfferingSpec, annotation: Any) -> dict[str, Any]:
    """The published schema of one field ``f`` of that type."""
    model = create_model("M", f=(annotation, ...))
    return derive_document(spec, model, model)["$defs"]["M"]["properties"]["f"]


def refusal(spec: OfferingSpec, annotation: Any) -> ContractError:
    model = create_model("M", f=(annotation, ...))
    with pytest.raises(ContractError) as refused:
        derive_document(spec, model, model)
    return refused.value


def extra(**keywords: Any) -> Any:
    """A use site's own JSON Schema keywords, as a seller writes them on a field."""
    return Field(json_schema_extra=keywords)


def agree(contract: InputValidator, model: type[BaseModel], values: list[Any]) -> None:
    """The contract accepts exactly the values the seller's model validates (as JSON, as
    temporalio's converter reads them)."""
    for value in values:
        try:
            model.model_validate_json(json.dumps(value))
            pydantic_says = True
        except (ValueError, TypeError):
            pydantic_says = False
        try:
            contract.validate(value)
            contract_says = True
        except ContractError:
            contract_says = False
        assert contract_says == pydantic_says, value


class Text(RootModel[Annotated[str, StringConstraints(min_length=2, max_length=8)]]):
    """Some text."""


class Pattern(RootModel[Annotated[str, StringConstraints(pattern=r"^a")]]):
    pass


class Count(RootModel[Annotated[int, Field(ge=0, le=100, multiple_of=4)]]):
    pass


class Ratio(RootModel[Annotated[float, Field(gt=0, lt=1)]]):
    pass


class Many(RootModel[Annotated[list[int], Field(min_length=1, max_length=9)]]):
    pass


class Rec(RootModel[list["Rec"]]):
    pass


class Size(enum.Enum):
    S = "s"
    M = "m"
    L = "l"


class Word(enum.StrEnum):
    A = "ab"
    B = "cd"


# PEP 695 aliases (``type Inner = ...`` from Python 3.12), spelled for 3.11
Inner = TypeAliasType("Inner", Annotated[str, StringConstraints(max_length=5)])
Outer = TypeAliasType("Outer", Annotated[Inner, Field(description="outer doc")])
MaybeShort = TypeAliasType("MaybeShort", Annotated[str, StringConstraints(max_length=5)] | None)


# --------------------------------------------------------------------------- I33's cases


class Bounded(BaseModel):
    """A bound on a named type (``Inner``, at most 5) and again where it is used: pydantic
    measures the string and enforces both."""

    loose: Annotated[Outer, Field(max_length=10)]
    tight: Annotated[Outer, Field(max_length=3)]
    low: Annotated[Outer, Field(min_length=2)]


def test_a_bound_on_a_type_and_its_use_both_hold(enrich_spec: OfferingSpec) -> None:
    """The use site's 10 no longer replaces the type's 5 (``"abcdefg"`` is refused); the
    tighter of the two holds on each side, exactly as pydantic validates."""
    doc = derive_document(enrich_spec, Bounded, Bounded)
    props = doc["$defs"]["Bounded"]["properties"]
    text = {"type": "string", "description": "outer doc"}
    assert props == {
        "loose": {**text, "maxLength": 5},
        "tight": {**text, "maxLength": 3},
        "low": {**text, "maxLength": 5, "minLength": 2},
    }
    words = ["", "a", "ab", "abc", "abcd", "abcde", "abcdef", "abcdefg"]
    agree(
        InputValidator(doc),
        Bounded,
        [{"loose": w, "tight": "ab", "low": "ab"} for w in words]
        + [{"loose": "a", "tight": w, "low": "ab"} for w in words]
        + [{"loose": "a", "tight": "a", "low": w} for w in words],
    )


@pytest.mark.parametrize(
    ("annotation", "value", "path", "fragment"),
    [
        (Annotated[Code, Field(max_length=10)], "ab", "", "Field(max_length=10) applies to the Code instance itself"),
        (Annotated[Code, Field(min_length=2)] | None, "abc", ".oneOf[0]", "Field(min_length=2) applies to the Code instance itself"),
        (Annotated[Leaf, Field(max_length=1)], {"v": 0}, "", "constrain Leaf's fields instead"),
        (Annotated[Color, Field(max_length=5)], "red", "", "make Color a str or int Enum"),
        (Annotated[Word, Field(ge=1)], "ab", "", "Field(ge=1) applies to the Word instance itself"),
        (Annotated[MaybeShort, Field(min_length=2)], None, "", "Field(min_length=2) applies to null as well"),
    ],
)  # fmt: skip
def test_a_bound_pydantic_applies_to_the_instance_is_refused(
    enrich_spec: OfferingSpec, annotation: Any, value: Any, path: str, fragment: str
) -> None:
    """I33's ``Annotated[Code, Field(max_length=10)]`` over a ``RootModel`` of at most 5:
    pydantic applies the 10 to the ``Code`` instance, not its string, and refuses every
    value, ``"ab"`` included; so does a bound on a model, a plain ``Enum`` member, a str
    ``Enum`` compared with a number, and null under an ``Optional`` alias. The contract
    would have published a bound the values meet (``maxLength: 5``), so it is refused."""
    model = create_model("M", f=(annotation, ...))
    with pytest.raises(TypeError):  # what pydantic does with every such value
        model.model_validate_json(json.dumps({"f": value}))
    err = refusal(enrich_spec, annotation)
    # Leaf has a default, so M's two modes differ and the input is refused first
    assert err.path in (f"$defs.M.properties.f{path}", f"$defs.MInput.properties.f{path}"), err
    assert fragment in err.message, err.message
    assert "refuses" in err.message


def test_the_i33_overlap_is_refused_where_it_is_used(enrich_spec: OfferingSpec) -> None:
    with pytest.raises(ContractError) as refused:
        derive_document(enrich_spec, Overlap, Overlap)
    assert refused.value.path == "$defs.Overlap.properties.code"
    assert "put the constraint inside Code's own type" in refused.value.message


def test_an_enum_re_constrained_where_it_is_used_keeps_the_common_values(
    enrich_spec: OfferingSpec,
) -> None:
    """``json_schema_extra={"enum": ["red", "green"]}`` over an enum of red and blue: only
    ``red`` is valid for both, and ``green`` (or ``blue``) is refused."""
    doc = derive_document(enrich_spec, EnumOverlap, EnumOverlap)
    assert doc["$defs"]["EnumOverlap"]["properties"]["color"] == {
        "type": "string",
        "enum": ["red"],
    }
    contract = InputValidator(doc)
    contract.validate({"color": "red"})
    for color in ("green", "blue"):
        with pytest.raises(ContractError):
            contract.validate({"color": color})


# --------------------------------------------------------------------------- conjoined


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (Annotated[Text, extra(maxLength=10)], {"maxLength": 8, "minLength": 2}),
        (Annotated[Text, extra(maxLength=4)], {"maxLength": 4, "minLength": 2}),
        (Annotated[Text, extra(minLength=1)], {"maxLength": 8, "minLength": 2}),
        (Annotated[Text, extra(minLength=3)], {"maxLength": 8, "minLength": 3}),
        (Annotated[Count, extra(maximum=50)], {"maximum": 50, "minimum": 0, "multipleOf": 4}),
        (Annotated[Count, extra(minimum=-3)], {"maximum": 100, "minimum": 0, "multipleOf": 4}),
        (Annotated[Count, extra(multipleOf=6)], {"maximum": 100, "minimum": 0, "multipleOf": 12}),
        (Annotated[Count, extra(type="number")], {"maximum": 100, "minimum": 0, "multipleOf": 4}),
        (Annotated[Ratio, extra(exclusiveMaximum=0.5)], {"exclusiveMaximum": 0.5, "exclusiveMinimum": 0}),
        (Annotated[Ratio, extra(exclusiveMinimum=0.25)], {"exclusiveMaximum": 1, "exclusiveMinimum": 0.25}),
        (Annotated[Many, extra(maxItems=20, minItems=3)], {"maxItems": 9, "minItems": 3}),
        (Annotated[Many, extra(uniqueItems=True)], {"maxItems": 9, "minItems": 1, "uniqueItems": True}),
        (Annotated[Pattern, extra(pattern="^a")], {"pattern": "^a"}),
        (Annotated[Size, extra(enum=["l", "s", "x"])], {"enum": ["s", "l"]}),
    ],
)  # fmt: skip
def test_a_keyword_both_constrain_is_conjoined(
    enrich_spec: OfferingSpec, annotation: Any, expected: dict[str, Any]
) -> None:
    """The tighter bound, the common enum values in the type's order, ``integer`` within
    ``number``, the least common multiple; the same value twice is that value."""
    published = field_schema(enrich_spec, annotation)
    assert {k: v for k, v in published.items() if k not in ("type", "items", "description")} == (
        expected
    )


def test_the_use_sites_annotations_replace_the_types(enrich_spec: OfferingSpec) -> None:
    """A field's own description, default, title, examples, ``$comment`` and ``deprecated``
    say what it means where it is used; the type's stand where the field says nothing."""
    own = field_schema(
        enrich_spec,
        Annotated[
            Text,
            Field(description="mine", title="Mine", examples=["ab"], deprecated=True),
            extra(**{"$comment": "here"}),
        ],
    )
    assert own == {
        "type": "string",
        "minLength": 2,
        "maxLength": 8,
        "description": "mine",
        "title": "Mine",
        "examples": ["ab"],
        "deprecated": True,
        "$comment": "here",
    }
    assert field_schema(enrich_spec, Text)["description"] == "Some text."

    class Defaulted(BaseModel):
        size: Size = Size.M

    doc = derive_document(enrich_spec, Defaulted, Plain)
    assert doc["$defs"]["Defaulted"]["properties"]["size"] == {
        "type": "string",
        "enum": ["s", "m", "l"],
        "default": "m",
    }


def test_a_type_over_a_constrained_type_is_conjoined_all_the_way(
    enrich_spec: OfferingSpec,
) -> None:
    class Short(RootModel[Annotated[Text, extra(maxLength=3)]]):
        pass

    assert field_schema(enrich_spec, Annotated[Short, extra(minLength=3)]) == {
        "type": "string",
        "minLength": 3,
        "maxLength": 3,
        "description": "Some text.",
    }


class Pick(RootModel[Color]):
    """A colour the buyer picks."""


Shade = TypeAliasType("Shade", Annotated[Color, Field(description="the shade")])


class Base(RootModel[Annotated[str, StringConstraints(max_length=5)]]):
    pass


class Wrap(RootModel[Base]):
    """A wrapper with a docstring."""


class Documented(BaseModel):
    pick: Pick
    shade: Shade
    wrap: Wrap
    outer: Outer
    tight: Annotated[Outer, Field(max_length=3)]


def test_a_documented_alias_of_a_written_out_type_is_written_out(
    enrich_spec: OfferingSpec,
) -> None:
    """A ``RootModel`` of an enum with a docstring, a PEP 695 alias of one with a
    description, and the same over a constrained type: each is a bare ``$ref`` alias
    (``{$ref, description}``) of a type that is written out, so it is written out too --
    a definition must be an object, a union or an alias -- carrying its own description,
    and a use site's bound beside it is conjoined."""
    doc = derive_document(enrich_spec, Documented, Documented)
    color = {"type": "string", "enum": ["red", "blue"]}
    assert doc["$defs"]["Documented"]["properties"] == {
        "pick": {**color, "description": "A colour the buyer picks."},
        "shade": {**color, "description": "the shade"},
        "wrap": {"type": "string", "maxLength": 5, "description": "A wrapper with a docstring."},
        "outer": {"type": "string", "maxLength": 5, "description": "outer doc"},
        "tight": {"type": "string", "maxLength": 3, "description": "outer doc"},
    }
    assert sorted(doc["$defs"]) == ["Documented", "KontraQuote"]
    ok = {"pick": "red", "shade": "blue", "wrap": "abcde", "outer": "abcde", "tight": "abc"}
    agree(
        InputValidator(doc),
        Documented,
        [ok, {**ok, "pick": "green"}, {**ok, "wrap": "abcdef"}, {**ok, "tight": "abcd"}],
    )


# --------------------------------------------------------------------------- refused


@pytest.mark.parametrize(
    ("annotation", "fragment"),
    [
        (Annotated[Pattern, extra(pattern="^b")], "re-constrains Pattern: Pattern says pattern '^a' and this field says pattern '^b'"),
        (Annotated[Text, extra(type="integer")], "no value satisfies both: Text says type 'string' and this field says type 'integer'"),
        (Annotated[Size, extra(enum=["x", "y"])], "no value satisfies both: Size says enum ['s', 'm', 'l'] and this field says enum ['x', 'y']"),
        (Annotated[Count, extra(multipleOf=0.5)], "re-constrains Count: Count says multipleOf 4 and this field says multipleOf 0.5"),
        (Annotated[Many, extra(items={"type": "string"})], "re-constrains Many: Many says items {'type': 'integer'}"),
    ],
)  # fmt: skip
def test_a_conjunction_nexgen_cannot_write_is_refused(
    enrich_spec: OfferingSpec, annotation: Any, fragment: str
) -> None:
    """Never published looser than the seller's model validates: the refusal names the
    model and the field (its path) and both values, and says where to put the constraint."""
    err = refusal(enrich_spec, annotation)
    assert err.path == "$defs.M.properties.f", err
    assert fragment in err.message, err.message


class Long(RootModel[Annotated[str, StringConstraints(min_length=5)]]):
    pass


class Small(RootModel[Annotated[int, Field(le=10)]]):
    pass


class Big(RootModel[Annotated[int, Field(ge=10)]]):
    pass


class Above9(RootModel[Annotated[int, Field(gt=9)]]):
    pass


class Above9Float(RootModel[Annotated[float, Field(gt=9)]]):
    pass


class Counted(RootModel[Annotated[list[int], extra(contains={"type": "integer"}, minContains=2)]]):
    pass


@pytest.mark.parametrize(
    ("annotation", "fragment"),
    [
        (Annotated[Long, extra(maxLength=3)], "Long says minLength 5 and this field says maxLength 3"),
        (Annotated[Many, extra(maxItems=0)], "Many says minItems 1 and this field says maxItems 0"),
        (Annotated[Counted, extra(maxContains=1)], "Counted says minContains 2 and this field says maxContains 1"),
        (Annotated[Small, extra(minimum=20)], "this field says minimum 20 and Small says maximum 10"),
        (Annotated[Small, extra(exclusiveMinimum=10)], "this field says exclusiveMinimum 10 and Small says maximum 10"),
        (Annotated[Big, extra(exclusiveMaximum=10)], "Big says minimum 10 and this field says exclusiveMaximum 10"),
        (Annotated[Above9, extra(exclusiveMaximum=10)], "Above9 says exclusiveMinimum 9 and this field says exclusiveMaximum 10"),
    ],
)  # fmt: skip
def test_bounds_on_two_keywords_no_value_meets_are_refused(
    enrich_spec: OfferingSpec, annotation: Any, fragment: str
) -> None:
    """The type's lower bound and the field's upper one (or the reverse) on different
    keywords: each is kept, and together they leave no value -- nor any integer between 9
    and 10 exclusive -- so the gate would refuse every hire. Refused like two values of one
    keyword that share nothing."""
    err = refusal(enrich_spec, annotation)
    assert err.path == "$defs.M.properties.f", err
    assert err.message.startswith("no value satisfies both: "), err.message
    assert fragment in err.message, err.message


def test_bounds_some_value_meets_are_kept(enrich_spec: OfferingSpec) -> None:
    """A number between 9 and 10 exclusive exists; a bound that equals the other is met."""
    assert field_schema(enrich_spec, Annotated[Above9Float, extra(exclusiveMaximum=10)]) == {
        "type": "number",
        "exclusiveMinimum": 9,
        "exclusiveMaximum": 10,
    }
    assert field_schema(enrich_spec, Annotated[Big, extra(maximum=10)]) == {
        "type": "integer",
        "minimum": 10,
        "maximum": 10,
    }


def test_a_const_on_both_sides_must_agree(enrich_spec: OfferingSpec) -> None:
    class One(RootModel[Annotated[str, Field(json_schema_extra={"const": "a"})]]):
        pass

    assert field_schema(enrich_spec, Annotated[One, extra(const="a")])["const"] == "a"
    err = refusal(enrich_spec, Annotated[One, extra(const="b")])
    assert "One says const 'a' and this field says const 'b'" in err.message


def test_two_formats_are_refused(enrich_spec: OfferingSpec) -> None:
    class Email(RootModel[Annotated[str, Field(json_schema_extra={"format": "email"})]]):
        pass

    err = refusal(enrich_spec, Annotated[Email, extra(format="uri")])
    assert "Email says format 'email' and this field says format 'uri'" in err.message


def test_a_scalar_type_that_refers_to_itself_is_refused(enrich_spec: OfferingSpec) -> None:
    """A recursive ``RootModel`` of a list has no model to name it; written out where it is
    used, it would never end."""
    err = refusal(enrich_spec, Rec)
    assert "refers to itself" in err.message
    assert "make it a model with a field" in err.message
