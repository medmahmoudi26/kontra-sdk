"""The output zoo (decisions I38, from I33's measurements): one model per way a pydantic model
serializes otherwise than it validates, and the ``$ref`` sibling cases. Each case is a model
and an input JSON value for it; ``tests/contract/test_serialized.py`` derives a contract taking
and returning the model and checks that what a buyer receives -- a workflow entry's result
and a bound handler's -- is what the contract describes.

The same cases are what kontra-sdk#53's harness check reports (``harness_zoo.py`` on that
branch), all 23 of them; they are written out here so this seam does not depend on it.
``Times`` writes its durations as seconds (``ser_json_timedelta="float"``): an ISO duration
output is refused (I38), and ``Overlap`` has no value at all (see :data:`CASES`)."""

from __future__ import annotations

import enum
from datetime import date, datetime, timedelta
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    StringConstraints,
    computed_field,
    field_serializer,
)
from pydantic.alias_generators import to_camel

from kontra.types import Artifact


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


class Level(enum.IntEnum):
    LOW = 1
    HIGH = 2


class Aliases(BaseModel):
    """Aliases, one of them a Python keyword."""

    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    via_hops: int = Field(alias="via-hops")


class Choices(BaseModel):
    """Several names accepted for one field (the schema names the first)."""

    a: str = Field(validation_alias=AliasChoices("a", "alpha"))


class Enums(BaseModel):
    """A string enum with a default, an int enum, an optional enum."""

    color: Color = Color.RED
    level: Level
    maybe: Color | None = None


class Literals(BaseModel):
    """Literals: one value, two with a default, numbers."""

    kind: Literal["one"]
    mode: Literal["a", "b"] = "a"
    num: Literal[1, 2]


class Constrained(BaseModel):
    """Constrained strings, numbers and lists."""

    code: str = Field(min_length=2, max_length=8, pattern=r"^[a-z]+$")
    n: int = Field(ge=0, le=100, multiple_of=5)
    x: float = Field(gt=0, lt=1)
    tags: list[Annotated[str, Field(min_length=1)]] = Field(min_length=1, max_length=5)


class Collections(BaseModel):
    """Lists and maps, with and without defaults."""

    xs: list[int]
    ys: list[str] = Field(default_factory=list[str])
    m: dict[str, int]
    mm: dict[str, list[float]] = {}


class Times(BaseModel):
    """Dates, times, durations and UUIDs; the durations written as seconds, one of them a
    fraction of a second."""

    model_config = ConfigDict(ser_json_timedelta="float")

    at: datetime
    day: date
    took: timedelta
    uid: UUID

    @computed_field
    @property
    def half(self) -> timedelta:
        return self.took / 2


class Files(BaseModel):
    """Artifacts: required, in a list, optional."""

    doc: Artifact
    more: list[Artifact] = []
    opt: Artifact | None = None


class Titled(BaseModel):
    """Explicit titles, descriptions and examples: annotations."""

    model_config = ConfigDict(title="Custom")

    x: int = Field(title="Explicit", description="kept", examples=[1])


class Tree(BaseModel):
    """A recursive model."""

    name: str
    children: list[Tree] = []


class Point(BaseModel):
    """A closed nested model."""

    model_config = ConfigDict(extra="forbid")

    lat: float = Field(ge=-90, le=90)
    color: Color


class Forbid(BaseModel):
    """Closed models without defaults."""

    model_config = ConfigDict(extra="forbid")

    x: int
    when: datetime
    at: Point
    near: list[Point]
    note: str | None


class Allow(BaseModel):
    """Open to other keys (the contract closes it anyway, §11.3)."""

    model_config = ConfigDict(extra="allow")

    x: int


class OuterAllow(BaseModel):
    """pydantic's default (``extra="ignore"``) around ``Allow`` (I33's ``Outer``)."""

    inner: Allow
    many: list[Allow]
    maybe: Allow | None


class Code(RootModel[Annotated[str, StringConstraints(max_length=5)]]):
    """A constrained string with a name of its own."""


class Overlap(BaseModel):
    """A constraint on a named type and again where it is used (I33). pydantic applies the
    use site's ``max_length`` to the ``Code`` instance, not to its string, and refuses every
    value (a TypeError inside it); derivation refuses the model (I38)."""

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


ARTIFACT = {"key": "0" * 32, "size": 3, "sha256": "a" * 64, "content_type": "text/plain"}

#: name -> (model, an input value for it). Every I33 model is here but ``Overlap``, which has
#: no valid value (see its docstring). ``Allow``'s value has no other key: one is a member the
#: closed contract does not declare, refused (§11.3; #53 reports the open output).
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
    "aliases": (Aliases, {"from": "a", "via-hops": 2}),
    "choices": (Choices, {"a": "x"}),
    "enums": (Enums, {"level": 1}),
    "enums_set": (Enums, {"level": 2, "color": "blue", "maybe": "red"}),
    "literals": (Literals, {"kind": "one", "num": 1}),
    "constrained": (Constrained, {"code": "ab", "n": 5, "x": 0.5, "tags": ["t"]}),
    "collections": (Collections, {"xs": [1], "m": {"a": 1}}),
    "times": (
        Times,
        {
            "at": "2026-10-20T12:00:00Z",
            "day": "2026-10-20",
            "took": "PT1S",
            "uid": "12345678-1234-5678-1234-567812345678",
        },
    ),
    "files": (Files, {"doc": ARTIFACT}),
    "files_set": (Files, {"doc": ARTIFACT, "more": [ARTIFACT], "opt": ARTIFACT}),
    "titled": (Titled, {"x": 1}),
    "tree": (Tree, {"name": "r", "children": [{"name": "c", "children": [{"name": "g"}]}]}),
    "forbid": (
        Forbid,
        {
            "x": 1,
            "when": "2026-10-20T12:00:00Z",
            "at": {"lat": 1, "color": "red"},
            "near": [{"lat": -2.5, "color": "blue"}],
            "note": None,
        },
    ),
    "allow": (Allow, {"x": 1}),
    "outer": (OuterAllow, {"inner": {"x": 1}, "many": [{"x": 2}], "maybe": None}),
}
