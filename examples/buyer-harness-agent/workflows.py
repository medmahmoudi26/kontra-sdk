"""A buyer's own agent, written with Temporal's agent harness, that hires a Kontra offering as
one of its tools (tools.py). It runs on the buyer's own worker, in the buyer's namespace, as
any harness agent does; Kontra sees a buyer hiring an offering, nothing else.

**Spend control is the harness's approval policy.** Every hire goes through the `spend` tool,
which is not inherently safe, so the policy decides it. Auto mode is on, with this agent's
evaluator, `under_the_cap`: a price at or under the spending cap (the `spend` criteria's
`spend_cap`, which whoever starts the agent may set in its `AgentConfig`) is approved, one
above it is put to a person -- "agent wants to spend 0.05 USD hiring acme/recon-harness --
approve?" -- who allows or denies it with the harness's `tool_approval` Update (main.py asks in
the terminal). The evaluator only ever approves or escalates: a person is the one who denies.

The model here is scripted (no LLM, as in the seller's example): `audit` asks for the sweep
and words the answer. A real model gets `TOOLS` through the harness's integration of its SDK
(`as_openai_agent_tools`, the Gemini plugin) and calls the same tool."""

from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, Field
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import AgentWorkflowRunner, agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        AutoApprovalContext,
        AutoApprovalCriteria,
        AutoApprovalCriteriaSet,
        AutoApprovalDecision,
        AutoApprovalVerdict,
        ToolApprovalPolicy,
    )
    from temporal_agent_harness.harness.agent_protocol import events as hevents
    from temporal_agent_harness.harness.agent_workflow import ToolApprovalDenied
    from temporalio.contrib.workflow_streams import WorkflowStream
    from tools import OFFERING, SPEND, sweep_domains

#: The spending cap when whoever starts the agent sets none, in the quote's currency.
DEFAULT_CAP = "0.05"


def spending(cap: str = DEFAULT_CAP) -> AutoApprovalCriteria:
    """The criteria auto mode judges a spend by: the `spend` tool's set, and the cap."""
    rules = AutoApprovalCriteriaSet(
        effect="Locks an escrow hold of the buyer's money for the quoted price: paid to the "
        "seller if the job delivers, refunded if it does not.",
        approve_when=("the price is at or under the spending cap",),
        escalate_when=("the price is above the spending cap",),
    )
    return AutoApprovalCriteria(sets={SPEND: rules}, context={"spend_cap": cap})


async def under_the_cap(ctx: AutoApprovalContext) -> AutoApprovalDecision:
    """Approve a spend at or under the cap; put anything else to a person. Never a denial."""
    try:
        price = Decimal(str(ctx.tool_input["price"]))
        cap = Decimal(str(ctx.criteria.context["spend_cap"]))
    except (KeyError, InvalidOperation):
        return AutoApprovalDecision(AutoApprovalVerdict.ESCALATE, "no price or no cap to judge")
    details = {"price": str(price), "cap": str(cap)}
    if price.is_finite() and cap.is_finite() and 0 <= price <= cap:
        return AutoApprovalDecision(
            AutoApprovalVerdict.APPROVE, f"{price} is within the cap", details
        )
    return AutoApprovalDecision(
        AutoApprovalVerdict.ESCALATE, f"{price} is above the cap of {cap}", details
    )


class Audit(BaseModel):
    """The domains to audit."""

    domains: list[str] = Field(min_length=1, max_length=20)


class Findings(BaseModel):
    """What the agent found, from the hired sweep, or why it hired nothing."""

    hired: bool
    targets: int | None = None
    expired: int | None = None
    weak: int | None = None
    answer: str


@agent.defn
class BuyerAgent:
    """Audits domains by hiring a sweep on Kontra, within its spending policy."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy(
                auto_approve_inherently_safe=True, auto_mode_enabled=True
            ),
            auto_mode_evaluator=under_the_cap,
            auto_approval_criteria_default=spending(),
        )

    @agent.accepts
    async def audit(self, req: Audit) -> Findings:
        """Audit the domains: hire a sweep, and say what it found."""
        self._say(f"sweeping {len(req.domains)} domains with {OFFERING}")
        call = "call-sweep_domains-0"
        self._runner.publish(
            hevents.ToolRequested(
                tool_id=call, tool_name="sweep_domains", tool_input={"domains": req.domains}
            )
        )
        try:
            report = await self._runner.run_tool(
                call, sweep_domains, req.domains, injections={"runner": self._runner}
            )
        except ToolApprovalDenied as denied:
            answer = f"not hired: the spend was not approved ({denied.reason or 'no reason given'})"
            self._say(answer)
            return Findings(hired=False, answer=answer)
        answer = f"{report.targets} hosts swept: {report.expired} expired, {report.weak} weak"
        self._say(answer)
        return Findings(
            hired=True,
            targets=report.targets,
            expired=report.expired,
            weak=report.weak,
            answer=answer,
        )

    def _say(self, text: str) -> None:
        """One scripted model interaction, as a model integration publishes it."""
        self._runner.publish(hevents.ModelInteractionStarted(model="scripted"))
        self._runner.publish(hevents.ReplyDelta(text=text))
        usage = hevents.TokenUsage(input_tokens=0, output_tokens=0)
        self._runner.publish(hevents.ModelInteractionEnded(model="scripted", usage=usage))
