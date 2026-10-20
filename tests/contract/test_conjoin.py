"""Seam ③: a ``$ref``'s siblings are conjoined, not overridden (decisions I38, kontra-sdk#64).

An enum or a constrained scalar that pydantic names in ``$defs`` is written out where it is
used. JSON Schema asserts a ``$ref``'s definition and the members beside it both, so the
written-out schema must assert what the two did: the use site's annotations replace the
definition's, a keyword both constrain is conjoined, and one whose conjunction nexgen's
subset cannot write is refused at derivation, naming the field and both values."""

from __future__ import annotations

import enum
from typing import Annotated, Any

import pytest
from pydantic import BaseModel, Field, RootModel, StringConstraints, create_model

from kontra.contract import ContractError, InputValidator, OfferingSpec, derive_document

from .zoo import Code, EnumOverlap, Overlap, Plain


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


# --------------------------------------------------------------------------- I33's cases


def test_a_bound_on_a_type_and_its_use_both_hold(enrich_spec: OfferingSpec) -> None:
    """``Annotated[Code, Field(max_length=10)]`` over a ``RootModel`` of at most 5: the use
    site's 10 no longer replaces the type's 5, and ``"abcdefg"`` is refused."""
    doc = derive_document(enrich_spec, Overlap, Overlap)
    props = doc["$defs"]["Overlap"]["properties"]
    code = {"type": "string", "maxLength": 5, "description": Code.__doc__}
    assert props["code"] == code
    assert props["maybe"] == {"oneOf": [code, {"type": "null"}]}
    contract = InputValidator(doc)
    contract.validate({"code": "abcde", "maybe": None})
    for value, path in (
        ({"code": "abcdefg", "maybe": None}, "input.code"),
        ({"code": "abc", "maybe": "abcdefg"}, "input.maybe"),
    ):
        with pytest.raises(ContractError) as refused:
            contract.validate(value)
        assert refused.value.path == path


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
    class Short(RootModel[Annotated[Text, Field(max_length=3)]]):
        pass

    assert field_schema(enrich_spec, Annotated[Short, extra(minLength=3)]) == {
        "type": "string",
        "minLength": 3,
        "maxLength": 3,
        "description": "Some text.",
    }


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
