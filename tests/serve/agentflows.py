"""An agent workflow written as a seller writes one: OpenAI's ``agents`` package and Kontra's
façade, nothing from ``temporalio`` (imported only when the ``[agents]`` extra is installed)."""

from __future__ import annotations

from datetime import timedelta

from kontra import activity, workflow

with workflow.unsafe.imports_passed_through():
    from agents import Agent, Runner


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
            tools=[workflow.activity_as_tool(lookup, start_to_close_timeout=timedelta(seconds=30))],
        )
        result = await Runner.run(agent, host)
        return str(result.final_output)
