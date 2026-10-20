"""Agents written with Temporal's agent harness (``temporal-agent-harness``, the seller's
dependency; kontra-sdk's dev group installs it), as a seller writes them: vanilla temporalio
and the harness, nothing from ``kontra``. Their handlers need no model: they compute their
reply, so a test sells one without any network.

An importable module, as Temporal's workflow sandbox requires; ``quote`` is the seller's
``quote.py`` function for :class:`ReconAgent`'s ``sweep``."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import AgentWorkflowRunner, agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        MessageContext,
        ToolApprovalPolicy,
    )
    from temporalio.contrib.workflow_streams import WorkflowStream

    from kontra.serve import Quote


class Sweep(BaseModel):
    """A host to sweep."""

    host: str
    fail: bool = False


class Report(BaseModel):
    """What the sweep found."""

    host: str
    open: int


class Data(BaseModel):
    """What an agent may be started with."""

    depth: int


@agent.defn
class ReconAgent:
    """Sweeps hosts, one per message."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )

    @agent.accepts
    async def sweep(self, req: Sweep, ctx: agent.Injected[MessageContext]) -> Report:
        """Sweep one host and report its open ports."""
        if req.fail:
            raise ValueError(f"cannot sweep {req.host}")
        return Report(host=req.host, open=len(req.host))


@agent.defn
class NeedsData:
    """An agent that cannot start without init data: nothing a Kontra job can pass."""

    @agent.init
    def __init__(self, config: AgentConfig, data: Data) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )

    @agent.accepts
    async def sweep(self, req: Sweep) -> Report:
        """Sweep one host."""
        return Report(host=req.host, open=0)


@agent.defn
class MayHaveData:
    """Init data, optional: Kontra starts it without."""

    @agent.init
    def __init__(self, config: AgentConfig, data: Data | None = None) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )

    @agent.accepts
    async def sweep(self, req: Sweep) -> Report:
        """Sweep one host."""
        return Report(host=req.host, open=1)


@agent.defn
class SlowAgent:
    """Sweeps on a durable timer: its turn outlasts a worker stopped in the middle of it."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )

    @agent.accepts
    async def sweep(self, req: Sweep) -> Report:
        """Sweep one host, slowly."""
        await workflow.sleep(timedelta(seconds=6))
        return Report(host=req.host, open=len(req.host))


class Hop(BaseModel):
    """A message whose field is a Python keyword: it travels by its alias, ``from``."""

    from_: str = Field(alias="from")


class Hopped(BaseModel):
    """A reply with aliases, as a buyer's model reads it: ``{"to": ..., "via-hops": ...}``."""

    to_: str = Field(alias="to")
    via_hops: int = Field(alias="via-hops")


@agent.defn
class HopAgent:
    """One hop, with aliased models both ways."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )

    @agent.accepts
    async def hop(self, req: Hop) -> Hopped:
        """Hop from where the message says."""
        return Hopped.model_validate({"to": f"{req.from_}+1", "via-hops": 1})


async def hop_quote(req: Hop) -> Quote:
    return Quote(price=Decimal("0.01"))


def local_handler() -> Any:
    """A handler of a class defined inside a function (Temporal refuses a local workflow
    class outright, so this one is not an agent yet): not importable by name."""

    class Local:
        @agent.accepts
        async def sweep(self, req: Sweep) -> Report:
            """Sweep one host."""
            return Report(host=req.host, open=0)

    return Local.sweep


async def quote(req: Sweep) -> Quote:
    return Quote(price=Decimal("0.01"))
