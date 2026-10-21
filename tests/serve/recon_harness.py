"""The agent the dev console's session pane is shown with (``docs/decisions.md`` I36), written
as a seller writes one: vanilla temporalio and Temporal's agent harness, nothing from ``kontra``
(but ``Quote``, for its ``quote.py`` function). One turn of :class:`ReconHarnessAgent` has a
scripted model plan, run two activity tools, hand the certificates to a subagent
(:class:`CertChecker`, a child workflow that runs its own tools) and -- when the message asks,
which a one-turn job cannot carry -- wait for a human approval and for a callback.

No LLM: :class:`ScriptedModel` publishes what a model integration would (the harness's own test
model drives the OpenAI SDK's non-streamed path, which publishes no model events). #58 promotes
this module to ``examples/recon-harness``.

Without ``from __future__ import annotations``: the harness reads a tool's annotations from its
own module's globals, so its models must be real classes there, not strings."""

import asyncio
from datetime import timedelta
from decimal import Decimal
from typing import Any

from pydantic import BaseModel
from temporalio import workflow
from temporalio.workflow import ActivityConfig

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import AgentWorkflowRunner, agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        ToolApprovalPolicy,
    )
    from temporal_agent_harness.harness.agent_protocol import events as hevents
    from temporalio.contrib.workflow_streams import WorkflowStream

    from kontra.serve import Quote

_TOOL = ActivityConfig(start_to_close_timeout=timedelta(seconds=30))


class SweepRequest(BaseModel):
    """The domains to sweep, and the interactive steps to take: posting the findings waits
    on a human approval, and ``confirm`` on a value a client sends back."""

    domains: list[str]
    want_approval: bool = False
    want_callback: bool = False


class Targets(BaseModel):
    """What ``plan`` chose to probe."""

    hosts: list[str]

    def __str__(self) -> str:  # the tool's result, as the harness reports it (str(result))
        return f"{len(self.hosts)} targets"


class Probed(BaseModel):
    """What ``probe`` found."""

    probed: int
    open: int

    def __str__(self) -> str:
        return f"{self.probed}/{self.probed}"


class CertRequest(BaseModel):
    """The hosts whose certificates the subagent checks."""

    hosts: list[str]


class Cert(BaseModel):
    """One host's certificate."""

    host: str
    expired: bool
    weak: bool

    def __str__(self) -> str:
        return "expired" if self.expired else "weak" if self.weak else "ok"


class CertReport(BaseModel):
    """What the subagent found."""

    expired: int
    weak: int


class ReconReport(BaseModel):
    """What the sweep found, and what was done with it."""

    domains: int
    targets: int
    expired: int
    weak: int
    posted: str | None = None
    confirmed: str | None = None


@agent.activity_tool_defn(inherently_safe=True, activity_config=_TOOL)
async def plan(domains: list[str]) -> Targets:
    """Choose the hosts to probe: one per domain."""
    return Targets(hosts=[f"www.{d}" for d in domains])


@agent.activity_tool_defn(inherently_safe=True, activity_config=_TOOL)
async def probe(hosts: list[str]) -> Probed:
    """Probe the hosts."""
    return Probed(probed=len(hosts), open=len(hosts) - 1)


@agent.activity_tool_defn(inherently_safe=True, activity_config=_TOOL)
async def fetch_cert(host: str) -> Cert:
    """One host's certificate."""
    n = sum(map(ord, host))
    return Cert(host=host, expired=n % 7 == 0, weak=n % 5 == 0)


@agent.activity_tool_defn(inherently_safe=False, activity_config=_TOOL)
async def post_findings(to: str, expired: int, weak: int) -> str:
    """Post the findings: not inherently safe, so every call waits on a human approval."""
    return f"posted to {to}"


@agent.callback_tool_defn(timeout=timedelta(minutes=10))
async def confirm(prompt: str) -> str:
    """Ask whoever drives the agent to confirm, and wait for their answer."""
    ...


#: The tools a worker registers: ``AgentHarnessPlugin(tools=RECON_TOOLS)``.
RECON_TOOLS = [plan, probe, fetch_cert, post_findings, confirm]


class ScriptedModel:
    """A model with no LLM behind it: each :meth:`say` is one interaction (started, the reply
    in deltas, ended with its usage) and each :meth:`call` one tool it asks for -- the
    events the harness's model integrations publish, published from the workflow."""

    def __init__(self, runner: AgentWorkflowRunner, model: str = "gpt-4o") -> None:
        self._runner = runner
        self._model = model
        self._calls = 0

    def say(self, text: str, *, tokens: tuple[int, int] = (820, 36)) -> None:
        self._runner.publish(hevents.ModelInteractionStarted(model=self._model))
        for word in text.split(" "):
            self._runner.publish(hevents.ReplyDelta(text=word + " "))
        usage = hevents.TokenUsage(input_tokens=tokens[0], output_tokens=tokens[1])
        self._runner.publish(hevents.ModelInteractionEnded(model=self._model, usage=usage))

    def call(self, tool: str, **tool_input: Any) -> str:
        """The id of a call to ``tool``: what the runner's ``run_tool`` is then given."""
        call_id = f"call-{tool}-{self._calls}"
        self._calls += 1
        self._runner.publish(
            hevents.ToolRequested(tool_id=call_id, tool_name=tool, tool_input=tool_input)
        )
        return call_id


@agent.defn
class CertChecker:
    """The subagent: checks each host's certificate, one tool call per host."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )

    @agent.accepts
    async def check(self, req: CertRequest) -> CertReport:
        """Check every host's certificate."""
        model = ScriptedModel(self._runner)
        model.say("fetch every certificate", tokens=(240, 18))
        calls = [(model.call("fetch_cert", host=h), h) for h in req.hosts]
        certs = await asyncio.gather(*(self._runner.run_tool(c, fetch_cert, h) for c, h in calls))
        return CertReport(expired=sum(c.expired for c in certs), weak=sum(c.weak for c in certs))


@agent.defn
class ReconHarnessAgent:
    """Sweeps domains in one turn: plans, probes, hands the certificates to a subagent, and
    -- when asked -- posts the findings once a human approves and confirms through a
    callback."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )

    @agent.accepts
    async def sweep(self, req: SweepRequest) -> ReconReport:
        """Sweep the domains."""
        model = ScriptedModel(self._runner)
        model.say("plan first")
        targets = await self._runner.run_tool(
            model.call("plan", domains=len(req.domains)), plan, req.domains
        )
        model.say("probe them, then check their certificates", tokens=(910, 22))
        probed = await self._runner.run_tool(
            model.call("probe", hosts=len(targets.hosts)), probe, targets.hosts
        )
        checker = await self._runner.start_subagent(
            "cert-checker", "CertChecker", workflow.info().task_queue
        )
        reply = await self._runner.run_subagent_turn(checker, "check", {"hosts": targets.hosts})
        await self._runner.stop_subagent(checker)
        certs = CertReport.model_validate(reply)
        report = ReconReport(
            domains=len(req.domains), targets=probed.probed, expired=certs.expired, weak=certs.weak
        )
        if req.want_approval:
            model.say("post the findings", tokens=(1020, 14))
            call = model.call(
                "post_findings", to="slack #sec", expired=certs.expired, weak=certs.weak
            )
            report.posted = await self._runner.run_tool(
                call, post_findings, "slack #sec", certs.expired, certs.weak
            )
        if req.want_callback:
            call = model.call("confirm", prompt="ship the report?")
            report.confirmed = await self._runner.run_tool(call, confirm, "ship the report?")
        model.say("done", tokens=(1100, 4))
        return report


async def recon_quote(req: SweepRequest) -> Quote:
    return Quote(price=Decimal("0.02"))
