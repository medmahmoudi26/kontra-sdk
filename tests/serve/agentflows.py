"""An agent workflow written as a seller writes one: OpenAI's ``agents`` package and
Temporal's OpenAI Agents integration (both the seller's dependencies; kontra-sdk's dev group
installs them), vanilla temporalio, nothing from ``kontra``."""

from __future__ import annotations

from datetime import timedelta

from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from agents import Agent, Runner
    from temporalio.openai_agents.workflow import activity_as_tool


@activity.defn
async def lookup(host: str) -> str:
    """Tell the agent what ``host`` is."""
    return f"{host} is up"


@workflow.defn
class Triage:
    @workflow.run
    async def run(self, host: str) -> str:
        agent = Agent(
            name="triage",
            instructions="Look the host up, then say what you found.",
            tools=[activity_as_tool(lookup, start_to_close_timeout=timedelta(seconds=30))],
        )
        result = await Runner.run(agent, host)
        return str(result.final_output)
