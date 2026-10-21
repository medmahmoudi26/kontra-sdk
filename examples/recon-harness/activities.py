"""The agent's tools and its model, on this worker: vanilla Temporal and Temporal's agent
harness, nothing from `kontra` (decisions I29).

Each tool is an activity the harness runs for the agent (`@agent.activity_tool_defn`), retried
and recorded like any other; `confirm` is a callback tool, answered by whoever drives the
agent. The tools compute their answers from the host names, so the example runs anywhere,
offline: a real offering does its work in their bodies.

`think` is the model: one call per step of the turn. With no `LLM_BASE_URL` in the worker's
environment it is a scripted model, no endpoint and no key; with one, any OpenAI-compatible
server answers (Ollama, vLLM, OpenRouter, Mistral), for the model `LLM_MODEL` names. Which
model answers is this worker's configuration, never the workflow's. The prompt carries what
the turn learnt as counts, never the buyer's domains."""

import os
from datetime import timedelta

from models import Cert, Probed, Prompt, Targets, Thought
from temporal_agent_harness.harness import agent
from temporalio import activity
from temporalio.workflow import ActivityConfig

_TOOL = ActivityConfig(start_to_close_timeout=timedelta(seconds=30))

# --------------------------------------------------------------------------- the tools


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
    """Post the findings: not inherently safe, so every call waits on a person's approval."""
    return f"posted to {to}"


@agent.callback_tool_defn(inherently_safe=True, timeout=timedelta(minutes=10))
async def confirm(prompt: str) -> str:
    """Ask whoever drives the agent to confirm, and wait for their answer: asking is safe (no
    approval first), what comes back is a value, not a yes or no."""
    ...


#: The tools a worker registers: `AgentHarnessPlugin(tools=TOOLS)` (worker.py).
TOOLS = [plan, probe, fetch_cert, post_findings, confirm]

# --------------------------------------------------------------------------- the model

#: What the scripted model says at each step, and the tokens it pretends to spend.
SCRIPT: dict[str, tuple[str, int, int]] = {
    "plan": ("plan first", 820, 36),
    "probe": ("probe them, then check their certificates", 910, 22),
    "certificates": ("fetch every certificate", 240, 18),
    "post": ("post the findings", 1020, 14),
}
INSTRUCTIONS = (
    "You narrate a sweep of web domains, one step at a time. Answer in one short sentence: "
    "what you do next, or, at the step 'done', a summary of what the notes say was found."
)


@activity.defn
async def think(prompt: Prompt) -> Thought:
    """One model call: what the model says at this step of the turn."""
    base_url = os.environ.get("LLM_BASE_URL")
    if not base_url:
        if prompt.step == "done":
            return Thought(
                model="scripted",
                text="done: " + ", ".join(prompt.notes),
                input_tokens=1100,
                output_tokens=4,
            )
        text, tokens_in, tokens_out = SCRIPT.get(prompt.step, (prompt.step, 0, 0))
        return Thought(
            model="scripted", text=text, input_tokens=tokens_in, output_tokens=tokens_out
        )
    from openai import AsyncOpenAI  # the project's dependency, for a real model only

    model = os.environ.get("LLM_MODEL", "qwen2.5")
    client = AsyncOpenAI(
        base_url=base_url,
        api_key=os.environ.get("LLM_API_KEY", "unused"),  # Ollama and vLLM take any
        max_retries=0,  # Temporal retries the activity
    )
    try:
        answer = await client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": INSTRUCTIONS},
                {
                    "role": "user",
                    "content": f"step: {prompt.step}\nnotes: {'; '.join(prompt.notes)}",
                },
            ],
            max_tokens=200,
        )
    finally:
        await client.close()
    usage = answer.usage
    return Thought(
        model=model,
        text=(answer.choices[0].message.content or "").strip()[:2000],
        input_tokens=usage.prompt_tokens if usage else 0,
        output_tokens=usage.completion_tokens if usage else 0,
    )
