"""A seller whose output serializes otherwise than it validates (decisions I38), sold twice:
as a workflow entry and as an agent's handler. Vanilla temporalio, the harness and pydantic,
as a seller writes them; an importable module, as Temporal's workflow sandbox requires.

``Delivery`` has every case of ``tests/contract/zoo.py`` a value can carry: a computed field,
an excluded one, a serialization alias, a serializer that changes the JSON type, scalar,
factory, ``= None`` and nested defaults, a tagged union with a defaulted branch, and an enum
re-constrained where it is used (a ``$ref`` and its sibling)."""

from __future__ import annotations

import enum
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, Field, computed_field, field_serializer
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import AgentWorkflowRunner, agent
    from temporal_agent_harness.harness.agent_protocol import AgentConfig, ToolApprovalPolicy
    from temporalio.contrib.workflow_streams import WorkflowStream

    from kontra.serve import Quote


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Order(BaseModel):
    """What the buyer sends."""

    w: int
    h: int
    label: str
    at: datetime


class Leaf(BaseModel):
    v: int = 0


class Cat(BaseModel):
    kind: Literal["cat"]
    lives: int = Field(ge=0, le=9)


class Dog(BaseModel):
    kind: Literal["dog"]
    good: bool = True


class Delivery(BaseModel):
    """What the buyer receives."""

    w: int
    h: int
    secret: str = Field(exclude=True)
    label: str = Field(validation_alias="in_label", serialization_alias="out_label")
    at: datetime
    n: int = 3
    tags: list[str] = Field(default_factory=list[str])
    note: str | None = None
    leaf: Leaf = Leaf()
    pet: Annotated[Cat | Dog, Field(discriminator="kind")]
    color: Annotated[Color, Field(json_schema_extra={"enum": ["red", "green"]})] = Color.RED

    @computed_field
    @property
    def area(self) -> int:
        return self.w * self.h

    @field_serializer("at")
    def _at(self, v: datetime) -> int:
        return int(v.timestamp())


def make_delivery(order: Order) -> Delivery:
    return Delivery.model_validate(
        {
            "w": order.w,
            "h": order.h,
            "secret": f"only the seller sees this ({order.label})",
            "in_label": order.label,
            "at": order.at,
            "pet": {"kind": "dog"},
        }
    )


@workflow.defn
class DeliverFlow:
    """The workflow entry."""

    @workflow.run
    async def run(self, order: Order) -> Delivery:
        return make_delivery(order)


@agent.defn
class DeliverAgent:
    """The same, as an agent's handler: one turn, the same reply."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )

    @agent.accepts
    async def deliver(self, req: Order) -> Delivery:
        """Deliver the order."""
        return make_delivery(req)


async def quote(order: Order) -> Quote:
    return Quote(price=Decimal("0.01"))
