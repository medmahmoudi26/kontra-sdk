"""The model zoo of the harness check (decisions I33): one handler per kind of field a seller
writes, each taking and returning the same model, so one agent shows how Kontra's derivation
(validation mode, lowered to nexgen's subset) and temporal-agent-harness's ``agent_schema``
(validation mode for an input, serialization mode for an output) describe it.

An importable module, as the harness's ``schema`` command and Temporal's sandbox require:
vanilla temporalio and the harness, and ``kontra.types.Artifact``, as a seller's models.py
may use it. ``Forbid`` is the one model both describe identically: closed, no defaults."""

from __future__ import annotations

import enum
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_serializer,
)
from pydantic.alias_generators import to_camel
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import AgentWorkflowRunner, agent
    from temporal_agent_harness.harness.agent_protocol import AgentConfig, ToolApprovalPolicy
    from temporalio.contrib.workflow_streams import WorkflowStream

    from kontra.serve import Quote
    from kontra.types import Artifact


class Defaults(BaseModel):
    """Scalar defaults."""

    n: int = 3
    s: str = "x"
    b: bool = False
    f: float = 1.5
    req: int


class Optionals(BaseModel):
    """Optional fields: defaulted to None, required but nullable, defaulted to a value."""

    note: str | None = None
    must: int | None
    later: int | None = 4


class Aliases(BaseModel):
    """Aliases, one of them a Python keyword."""

    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    via_hops: int = Field(alias="via-hops")


class SplitAlias(BaseModel):
    """A field read under one name and written under another."""

    a: str = Field(validation_alias="in_a", serialization_alias="out_a")


class Camel(BaseModel):
    """An alias generator."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    first_name: str
    last_seen: int = 0


class Choices(BaseModel):
    """Several names accepted for one field (the schema names the first)."""

    a: str = Field(validation_alias=AliasChoices("a", "alpha"))


class Leaf(BaseModel):
    """A nested model."""

    v: int = 0


class Nested(BaseModel):
    """Nested models: required, optional, in a list."""

    leaf: Leaf
    maybe: Leaf | None = None
    many: list[Leaf] = []


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Level(enum.IntEnum):
    LOW = 1
    HIGH = 2


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
    ys: list[str] = Field(default_factory=list)
    m: dict[str, int]
    mm: dict[str, list[float]] = {}


class Times(BaseModel):
    """Dates, times, durations and UUIDs."""

    at: datetime
    day: date
    took: timedelta
    uid: UUID


class Files(BaseModel):
    """Artifacts: required, in a list, optional."""

    doc: Artifact
    more: list[Artifact] = []
    opt: Artifact | None = None


class Cat(BaseModel):
    """One branch of a tagged union."""

    kind: Literal["cat"]
    lives: int = Field(ge=0, le=9)


class Dog(BaseModel):
    """The other branch, with a default."""

    kind: Literal["dog"]
    good: bool = True


class Tagged(BaseModel):
    """A tagged union of models."""

    pet: Annotated[Cat | Dog, Field(discriminator="kind")]


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
    """Closed models without defaults: the contract and the harness say the same, once
    names, titles, the enum written inline and ``anyOf`` against ``oneOf`` are put aside."""

    model_config = ConfigDict(extra="forbid")

    x: int
    when: datetime
    at: Point
    near: list[Point]
    note: str | None


class Allow(BaseModel):
    """Open to other keys (the contract closes it anyway)."""

    model_config = ConfigDict(extra="allow")

    x: int


class Computed(BaseModel):
    """A computed field: in the serialization, not in the validation."""

    w: int
    h: int

    @computed_field
    @property
    def area(self) -> int:
        return self.w * self.h


class Excluded(BaseModel):
    """An excluded field: in the validation, not in the serialization."""

    keep: int
    secret: str = Field(exclude=True)


class Serialized(BaseModel):
    """A serializer that changes the field's JSON type."""

    at: datetime

    @field_serializer("at")
    def _at(self, v: datetime) -> int:
        return int(v.timestamp())


@agent.defn
class Zoo:
    """Echoes each model back: one handler per case."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )

    @agent.accepts
    async def defaults(self, req: Defaults) -> Defaults:
        """Echo."""
        return req

    @agent.accepts
    async def optionals(self, req: Optionals) -> Optionals:
        """Echo."""
        return req

    @agent.accepts
    async def aliases(self, req: Aliases) -> Aliases:
        """Echo."""
        return req

    @agent.accepts
    async def split_alias(self, req: SplitAlias) -> SplitAlias:
        """Echo."""
        return req

    @agent.accepts
    async def camel(self, req: Camel) -> Camel:
        """Echo."""
        return req

    @agent.accepts
    async def choices(self, req: Choices) -> Choices:
        """Echo."""
        return req

    @agent.accepts
    async def nested(self, req: Nested) -> Nested:
        """Echo."""
        return req

    @agent.accepts
    async def enums(self, req: Enums) -> Enums:
        """Echo."""
        return req

    @agent.accepts
    async def literals(self, req: Literals) -> Literals:
        """Echo."""
        return req

    @agent.accepts
    async def constrained(self, req: Constrained) -> Constrained:
        """Echo."""
        return req

    @agent.accepts
    async def collections(self, req: Collections) -> Collections:
        """Echo."""
        return req

    @agent.accepts
    async def times(self, req: Times) -> Times:
        """Echo."""
        return req

    @agent.accepts
    async def files(self, req: Files) -> Files:
        """Echo."""
        return req

    @agent.accepts
    async def tagged(self, req: Tagged) -> Tagged:
        """Echo."""
        return req

    @agent.accepts
    async def titled(self, req: Titled) -> Titled:
        """Echo."""
        return req

    @agent.accepts
    async def tree(self, req: Tree) -> Tree:
        """Echo."""
        return req

    @agent.accepts
    async def forbid(self, req: Forbid) -> Forbid:
        """Echo."""
        return req

    @agent.accepts
    async def allow(self, req: Allow) -> Allow:
        """Echo."""
        return req

    @agent.accepts
    async def computed(self, req: Computed) -> Computed:
        """Echo."""
        return req

    @agent.accepts
    async def excluded(self, req: Excluded) -> Excluded:
        """Echo."""
        return req

    @agent.accepts
    async def serialized(self, req: Serialized) -> Serialized:
        """Echo."""
        return req


#: Each handler's model (input and output alike).
MODELS: dict[str, type[BaseModel]] = {
    "defaults": Defaults,
    "optionals": Optionals,
    "aliases": Aliases,
    "split_alias": SplitAlias,
    "camel": Camel,
    "choices": Choices,
    "nested": Nested,
    "enums": Enums,
    "literals": Literals,
    "constrained": Constrained,
    "collections": Collections,
    "times": Times,
    "files": Files,
    "tagged": Tagged,
    "titled": Titled,
    "tree": Tree,
    "forbid": Forbid,
    "allow": Allow,
    "computed": Computed,
    "excluded": Excluded,
    "serialized": Serialized,
}


async def forbid_quote(req: Forbid) -> Quote:
    return Quote(price=Decimal("0.01"))
