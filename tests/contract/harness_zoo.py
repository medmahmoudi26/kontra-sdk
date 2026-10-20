"""The model zoo of the harness check (decisions I33), as an agent: one handler per model of
the output zoo (``tests/contract/zoo.py``, decisions I38), each taking and returning its
model, so one agent shows how Kontra's derivation and temporal-agent-harness's
``agent_schema`` describe it -- the input in validation mode and the output in serialization
mode, on both sides.

``Overlap`` is not here: derivation refuses it (I38), so there is no contract to compare.
``Bounded`` is this module's own: a bound on a named type (a PEP 695 alias of a constrained
alias, which pydantic measures as its value) and again where it is used, which both emitters
write as a ``$ref`` with siblings and the contract conjoins.

An importable module, as the harness's ``schema`` command and Temporal's sandbox require:
vanilla temporalio and the harness, and the zoo's models, as a seller's models.py."""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints
from temporalio import workflow
from typing_extensions import TypeAliasType

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import AgentWorkflowRunner, agent
    from temporal_agent_harness.harness.agent_protocol import AgentConfig, ToolApprovalPolicy
    from temporalio.contrib.workflow_streams import WorkflowStream

    from kontra.serve import Quote
    from tests.contract.zoo import (
        Aliases,
        Allow,
        Camel,
        Choices,
        Collections,
        Computed,
        ComputedAlias,
        Constrained,
        Defaults,
        EnumOverlap,
        Enums,
        Excluded,
        ExcludedIf,
        Factories,
        Files,
        Forbid,
        Literals,
        Nested,
        Optionals,
        OuterAllow,
        Plain,
        Serialized,
        SplitAlias,
        Tagged,
        Times,
        Titled,
        Tree,
        TypedAllow,
    )

# PEP 695 aliases (``type Short = ...`` from Python 3.12), spelled for 3.11.
Short = TypeAliasType("Short", Annotated[str, StringConstraints(max_length=5)])
Documented = TypeAliasType("Documented", Annotated[Short, Field(description="A short text.")])


class Bounded(BaseModel):
    """A bound on a named type (``Short``, at most 5 characters, through a documented alias)
    and again where it is used: pydantic measures the string and enforces both, the harness
    writes ``{$ref: Documented, maxLength: 10}``, and the contract the tighter of each pair
    (decisions I38)."""

    loose: Annotated[Documented, Field(max_length=10)]
    tight: Annotated[Documented, Field(max_length=3)]
    low: Annotated[Documented, Field(min_length=2)]


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
    async def computed(self, req: Computed) -> Computed:
        """Echo."""
        return req

    @agent.accepts
    async def computed_alias(self, req: ComputedAlias) -> ComputedAlias:
        """Echo."""
        return req

    @agent.accepts
    async def excluded(self, req: Excluded) -> Excluded:
        """Echo."""
        return req

    @agent.accepts
    async def excluded_if(self, req: ExcludedIf) -> ExcludedIf:
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
    async def serialized(self, req: Serialized) -> Serialized:
        """Echo."""
        return req

    @agent.accepts
    async def defaults(self, req: Defaults) -> Defaults:
        """Echo."""
        return req

    @agent.accepts
    async def factories(self, req: Factories) -> Factories:
        """Echo."""
        return req

    @agent.accepts
    async def optionals(self, req: Optionals) -> Optionals:
        """Echo."""
        return req

    @agent.accepts
    async def nested(self, req: Nested) -> Nested:
        """Echo."""
        return req

    @agent.accepts
    async def tagged(self, req: Tagged) -> Tagged:
        """Echo."""
        return req

    @agent.accepts
    async def enum_overlap(self, req: EnumOverlap) -> EnumOverlap:
        """Echo."""
        return req

    @agent.accepts
    async def plain(self, req: Plain) -> Plain:
        """Echo."""
        return req

    @agent.accepts
    async def aliases(self, req: Aliases) -> Aliases:
        """Echo."""
        return req

    @agent.accepts
    async def choices(self, req: Choices) -> Choices:
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
    async def typed_allow(self, req: TypedAllow) -> TypedAllow:
        """Echo."""
        return req

    @agent.accepts
    async def outer(self, req: OuterAllow) -> OuterAllow:
        """Echo."""
        return req

    @agent.accepts
    async def bounded(self, req: Bounded) -> Bounded:
        """Echo."""
        return req


#: Each handler's model (input and output alike).
MODELS: dict[str, type[BaseModel]] = {
    "computed": Computed,
    "computed_alias": ComputedAlias,
    "excluded": Excluded,
    "excluded_if": ExcludedIf,
    "split_alias": SplitAlias,
    "camel": Camel,
    "serialized": Serialized,
    "defaults": Defaults,
    "factories": Factories,
    "optionals": Optionals,
    "nested": Nested,
    "tagged": Tagged,
    "enum_overlap": EnumOverlap,
    "plain": Plain,
    "aliases": Aliases,
    "choices": Choices,
    "enums": Enums,
    "literals": Literals,
    "constrained": Constrained,
    "collections": Collections,
    "times": Times,
    "files": Files,
    "titled": Titled,
    "tree": Tree,
    "forbid": Forbid,
    "allow": Allow,
    "typed_allow": TypedAllow,
    "outer": OuterAllow,
    "bounded": Bounded,
}


async def forbid_quote(req: Forbid) -> Quote:
    return Quote(price=Decimal("0.01"))


async def allow_quote(req: Allow) -> Quote:
    return Quote(price=Decimal("0.01"))
