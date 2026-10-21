"""An agent as a workflow whose entry is one typed handler: `ReconAgent.sweep`. Vanilla Temporal
and Temporal's agent harness (`temporal-agent-harness`), nothing from `kontra` (decisions
I29): delete worker.py and this runs on any Temporal worker that has the harness's plugin.

One turn of `ReconAgent` plans, probes, hands the certificates to a subagent (`CertChecker`, a
child workflow that runs its own tools), and -- when the message asks, which a hire cannot
(one hire is one turn: quote.py) -- waits on a person's approval and on their answer to a
callback. Every step goes onto the agent's `WorkflowStream`, the `turn_events` topic, as the
harness's own events: what the dev console's session pane draws and what `offering.yaml`'s
`streams:` relays to the buyer.

The steps are this code's; the model (activities.py: `think`, scripted unless the worker names
an endpoint) says each one and writes the report's summary. `Model` publishes what a model
integration would: the harness's OpenAI Agents and Gemini integrations publish these events
themselves, from the model's own stream."""

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import confirm, fetch_cert, plan, post_findings, probe, think
    from models import CertReport, CertRequest, Prompt, SweepReport, SweepRequest, Thought
    from temporal_agent_harness.harness import AgentWorkflowRunner, agent
    from temporal_agent_harness.harness.agent_protocol import AgentConfig, ToolApprovalPolicy
    from temporal_agent_harness.harness.agent_protocol import events as hevents
    from temporalio.contrib.workflow_streams import WorkflowStream

_THINK = timedelta(seconds=60)


class Model:
    """The model's side of a turn: each :meth:`say` is one interaction -- the ``think``
    activity's words, published as the harness's events (started, the reply in deltas, ended
    with its usage) -- and each :meth:`call` one tool it asks for."""

    def __init__(self, runner: AgentWorkflowRunner) -> None:
        self._runner = runner
        self._calls = 0

    async def say(self, step: str, *notes: str) -> str:
        thought = await workflow.execute_activity(
            think, Prompt(step=step, notes=list(notes)), start_to_close_timeout=_THINK
        )
        self._publish(thought)
        return thought.text

    def _publish(self, thought: Thought) -> None:
        self._runner.publish(hevents.ModelInteractionStarted(model=thought.model))
        for word in thought.text.split(" "):
            self._runner.publish(hevents.ReplyDelta(text=word + " "))
        usage = hevents.TokenUsage(
            input_tokens=thought.input_tokens, output_tokens=thought.output_tokens
        )
        self._runner.publish(hevents.ModelInteractionEnded(model=thought.model, usage=usage))

    def call(self, tool: str, **tool_input: object) -> str:
        """The id of a call to ``tool``: what the runner's ``run_tool`` is then given."""
        call_id = f"call-{tool}-{self._calls}"
        self._calls += 1
        self._runner.publish(
            hevents.ToolRequested(tool_id=call_id, tool_name=tool, tool_input=dict(tool_input))
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
        model = Model(self._runner)
        await model.say("certificates", f"{len(req.hosts)} hosts")
        calls = [(model.call("fetch_cert", host=h), h) for h in req.hosts]
        certs = await asyncio.gather(*(self._runner.run_tool(c, fetch_cert, h) for c, h in calls))
        return CertReport(expired=sum(c.expired for c in certs), weak=sum(c.weak for c in certs))


@agent.defn
class ReconAgent:
    """Sweeps domains in one turn: plans, probes, hands the certificates to a subagent, and --
    when asked -- posts the findings once a person approves, and asks them to confirm."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self.stream = WorkflowStream()
        self._runner = AgentWorkflowRunner(
            config,
            stream=self.stream,
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )

    @agent.accepts
    async def sweep(self, req: SweepRequest) -> SweepReport:
        """Sweep the domains."""
        model = Model(self._runner)
        await model.say("plan", f"{len(req.domains)} domains")
        targets = await self._runner.run_tool(
            model.call("plan", domains=len(req.domains)), plan, req.domains
        )
        await model.say("probe", str(targets))
        probed = await self._runner.run_tool(
            model.call("probe", hosts=len(targets.hosts)), probe, targets.hosts
        )
        checker = await self._runner.start_subagent(
            "cert-checker", "CertChecker", workflow.info().task_queue
        )
        reply = await self._runner.run_subagent_turn(checker, "check", {"hosts": targets.hosts})
        certs = CertReport.model_validate(reply)
        posted = confirmed = None
        if req.want_approval:
            await model.say("post", f"{certs.expired} expired", f"{certs.weak} weak")
            call = model.call(
                "post_findings", to="slack #sec", expired=certs.expired, weak=certs.weak
            )
            posted = await self._runner.run_tool(
                call, post_findings, "slack #sec", certs.expired, certs.weak
            )
        if req.want_callback:
            call = model.call("confirm", prompt="ship the report?")
            confirmed = await self._runner.run_tool(call, confirm, "ship the report?")
        summary = await model.say(
            "done",
            f"{len(req.domains)} domains",
            f"{probed.probed} probed",
            f"{probed.open} open",
            f"{certs.expired} expired",
            f"{certs.weak} weak",
        )
        # The subagent is the turn's: stopped once the turn has nothing more to ask it.
        await self._runner.stop_subagent(checker)
        return SweepReport(
            domains=len(req.domains),
            targets=probed.probed,
            expired=certs.expired,
            weak=certs.weak,
            posted=posted,
            confirmed=confirmed,
            summary=summary,
        )
