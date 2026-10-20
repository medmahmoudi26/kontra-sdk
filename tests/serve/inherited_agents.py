"""Agents that share a handler by inheritance (``tests/serve/test_turn.py``): ``ReconV2``
inherits ``ReconV1.sweep`` unchanged, so ``ReconV2.sweep is ReconV1.sweep`` and a binding of
either cannot tell which agent was meant; it overrides ``probe``, so each class's ``probe`` is
its own. Vanilla temporalio and the harness (and ``quote``, the seller's ``quote.py``); a module
of its own, so the inheritance touches no other test's agent."""

from __future__ import annotations

from decimal import Decimal

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import AgentWorkflowRunner, agent
    from temporal_agent_harness.harness.agent_protocol import AgentConfig, ToolApprovalPolicy
    from temporalio.contrib.workflow_streams import WorkflowStream

    from kontra.serve import Quote
    from tests.serve.harness_agents import Report, Sweep


@agent.defn
class ReconV1:
    """The first version."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )

    @agent.accepts
    async def sweep(self, req: Sweep) -> Report:
        """Sweep one host."""
        return Report(host=req.host, open=1)

    @agent.accepts
    async def probe(self, req: Sweep) -> Report:
        """Probe one host (ReconV2 overrides it)."""
        return Report(host=req.host, open=0)


@agent.defn
class ReconV2(ReconV1):
    """Another agent, which inherits ``sweep`` as it is."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)

    @agent.accepts
    async def probe(self, req: Sweep) -> Report:
        """Its own probe."""
        return Report(host=req.host, open=2)


async def quote(req: Sweep) -> Quote:
    return Quote(price=Decimal("0.01"))
