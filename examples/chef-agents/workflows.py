"""An agent as workflow code: OpenAI's Agents SDK runs its loop here, durably, through
Temporal's integration -- every model call and every tool call is an activity, retried and
recorded like any other. Importing `agents` is all it takes: the worker gives a workflow
that imports it what it needs (decisions I25). Temporal through the kontra façade (D1).

Which model answers, and where it runs, is the worker's (worker.py), never this file's:
the agent names no model, so it runs the one the worker's Agents SDK defaults to."""

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import lookup_pantry
    from agents import Agent, Runner  # OpenAI's Agents SDK: your dependency, allowed
    from models import CookInput, Dish
    from temporalio.openai_agents.workflow import activity_as_tool

INSTRUCTIONS = (
    "You are a chef. Look in the pantry (its shelves are fridge, dry and spices) before you "
    "answer, then propose one dish for the craving that uses only what is there, in a few "
    "short steps."
)


@workflow.defn
class ChefFlow:
    @workflow.run
    async def run(self, inp: CookInput) -> Dish:
        chef = Agent(
            name="chef",
            instructions=INSTRUCTIONS,
            tools=[activity_as_tool(lookup_pantry, start_to_close_timeout=timedelta(seconds=30))],
        )
        result = await Runner.run(chef, inp.craving, max_turns=8)
        return Dish(recipe=str(result.final_output))
