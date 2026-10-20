"""The output zoo (decisions I38, from I33's measurements): one model per way a pydantic model
serializes otherwise than it validates, and the ``$ref`` sibling cases. Each case is a model
and an input JSON value for it; ``tests/contract/test_serialized.py`` derives a contract taking
and returning the model and checks that what a buyer receives -- a workflow entry's result
and a bound handler's -- is what the contract describes.

The same cases are what kontra-sdk#53's harness check reports (``harness_zoo.py`` on that
branch); they are written out here so this seam does not depend on it."""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    StringConstraints,
    computed_field,
    field_serializer,
)
from pydantic.alias_generators import to_camel


class Computed(BaseModel):
    """A computed field: in the serialization, not in the validation."""

    w: int
    h: int

    @computed_field
    @property
    def area(self) -> int:
        return self.w * self.h


class ComputedAlias(BaseModel):
    """A computed field written under an alias."""

    model_config = ConfigDict(extra="forbid")

    w: int

    @computed_field(alias="double-w")
    @property
    def double(self) -> int:
        return 2 * self.w


class Excluded(BaseModel):
    """An excluded field: in the validation, not in the serialization."""

    keep: int
    secret: str = Field(exclude=True)


class SplitAlias(BaseModel):
    """A field read under one name and written under another."""

    a: str = Field(validation_alias="in_a", serialization_alias="out_a")


class Camel(BaseModel):
    """An alias generator: read and written by alias, published by alias."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    first_name: str
    last_seen: int = 0


class Serialized(BaseModel):
    """A serializer that changes the field's JSON type."""

    at: datetime

    @field_serializer("at")
    def _at(self, v: datetime) -> int:
        return int(v.timestamp())


class Defaults(BaseModel):
    """Scalar defaults: the reply always carries them."""

    n: int = 3
    s: str = "x"
    b: bool = False
    f: float = 1.5
    req: int


class Factories(BaseModel):
    """Factory and composite defaults."""

    xs: list[int] = Field(default_factory=list[int])
    m: dict[str, int] = {}


class Optionals(BaseModel):
    """Optional fields: defaulted to None, required but nullable, defaulted to a value."""

    note: str | None = None
    must: int | None
    later: int | None = 4


class Leaf(BaseModel):
    """A nested model with a default of its own."""

    v: int = 0


class Nested(BaseModel):
    """Nested defaults: a nested model, an optional one, a list of them."""

    leaf: Leaf = Leaf()
    maybe: Leaf | None = None
    many: list[Leaf] = []


class Cat(BaseModel):
    """One branch of a tagged union, without defaults."""

    kind: Literal["cat"]
    lives: int = Field(ge=0, le=9)


class Dog(BaseModel):
    """The other branch, with a default."""

    kind: Literal["dog"]
    good: bool = True


class Tagged(BaseModel):
    """A tagged union whose branches serialize alike and otherwise."""

    pet: Annotated[Cat | Dog, Field(discriminator="kind")]


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Code(RootModel[Annotated[str, StringConstraints(max_length=5)]]):
    """A constrained string with a name of its own."""


class Overlap(BaseModel):
    """A constraint on a named type and again where it is used (I33): both hold."""

    model_config = ConfigDict(extra="forbid")

    code: Annotated[Code, Field(max_length=10)]
    maybe: Annotated[Code, Field(max_length=10)] | None


class EnumOverlap(BaseModel):
    """An enum re-constrained where it is used (I33): only the common values are valid."""

    color: Annotated[Color, Field(json_schema_extra={"enum": ["red", "green"]})]


class Plain(BaseModel):
    """A model both modes describe alike: one definition, under its own name."""

    model_config = ConfigDict(extra="forbid")

    x: int
    when: datetime


#: name -> (model, an input value for it). Overlap has none: pydantic applies the use site's
#: max_length to the ``Code`` instance and refuses every value (a TypeError inside it).
CASES: dict[str, tuple[type[BaseModel], dict[str, Any]]] = {
    "computed": (Computed, {"w": 2, "h": 3}),
    "computed_alias": (ComputedAlias, {"w": 4}),
    "excluded": (Excluded, {"keep": 1, "secret": "s3cret"}),
    "split_alias": (SplitAlias, {"in_a": "x"}),
    "camel": (Camel, {"firstName": "Ada"}),
    "serialized": (Serialized, {"at": "2026-10-20T12:00:00Z"}),
    "defaults": (Defaults, {"req": 1}),
    "factories": (Factories, {}),
    "optionals": (Optionals, {"must": None}),
    "nested": (Nested, {"many": [{}, {"v": 2}]}),
    "tagged_cat": (Tagged, {"pet": {"kind": "cat", "lives": 7}}),
    "tagged_dog": (Tagged, {"pet": {"kind": "dog"}}),
    "enum_overlap": (EnumOverlap, {"color": "red"}),
    "plain": (Plain, {"x": 1, "when": "2026-10-20T12:00:00Z"}),
}
