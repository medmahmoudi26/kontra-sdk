"""Run the buyer's agent for one audit, in your own namespace, with your profile (`kontra login`
or `kontra dev up`):

    python main.py example.com example.org             # the agent's own cap: 0.05
    python main.py --cap 0.01 example.com example.org  # ask before spending more than a cent

`kontra.hire.worker` is a plain Temporal worker whose client carries your keys and which runs
the hire activities; it also runs the agent, the quote activity and the harness's plugin. The
agent's events stream back here through the harness's own client: when a spend goes past the
cap, the question is asked in this terminal ("agent wants to spend 0.06 USD hiring
acme/recon-harness@0.1.0 -- approve? [y/N]") and the answer goes back as the harness's
`tool_approval` Update. The agent's answer is printed as JSON."""

import argparse
import asyncio
import json
import sys
import uuid
from typing import Any

from temporal_agent_harness.harness.agent_client import (
    AgentClient,
    AgentStreamOutput,
    AgentTurnError,
    AgentTurnTimeout,
)
from temporal_agent_harness.harness.agent_protocol import AgentConfig, AutoApprovalVerdict
from temporal_agent_harness.harness.agent_protocol import events as hevents
from temporal_agent_harness.plugin import AgentHarnessPlugin
from tools import TOOLS, Quotes
from workflows import BuyerAgent, spending

from kontra.hire import Kontra, worker

TASK_QUEUE = "buyer-harness-agent"


async def ask(question: str) -> bool:
    """A yes from the person at the terminal; anything else, or no terminal, is a no."""
    print(question, end="", file=sys.stderr, flush=True)
    answer = await asyncio.to_thread(sys.stdin.readline)
    return answer.strip().lower() in ("y", "yes")


async def main(domains: list[str], cap: str | None = None) -> dict[str, Any]:
    """One audit by the agent; its answer."""
    async with Kontra() as k:  # KONTRA_API_KEY, else your login in ~/.kontra
        w = await worker(
            task_queue=TASK_QUEUE,
            workflows=[BuyerAgent],
            activities=[Quotes(k).quote],
            kontra=k,
            # The harness's activities on this worker (its converter needs nothing more: your
            # client's is pydantic's already, under Kontra's codec). No offloading to local disk.
            plugins=[AgentHarnessPlugin(tools=TOOLS, large_payload_offload=None)],
        )
        async with w:
            agent_id = f"buyer-agent-{uuid.uuid4().hex[:12]}"
            config = (
                AgentConfig() if cap is None else AgentConfig(auto_approval_criteria=spending(cap))
            )
            handle = await w.client.start_workflow(
                "BuyerAgent", config, id=agent_id, task_queue=TASK_QUEUE
            )
            try:
                return await turn(AgentClient(w.client, agent_id), domains)
            finally:
                await handle.signal("close")
                await handle.result()


async def turn(agent: AgentClient, domains: list[str]) -> dict[str, Any]:
    """Send the audit, answer any spend put to a person, and return the agent's reply."""
    asked: dict[str, dict[str, Any]] = {}
    reply: dict[str, Any] = {}

    def item(event: AgentStreamOutput, _offset: int) -> AgentStreamOutput:
        return event

    events = await agent.send_message("audit", {"domains": domains}, on_item=item, timeout=600)
    async for event in events:
        if isinstance(event, AgentTurnError | AgentTurnTimeout):
            raise event
        e = event.event
        if isinstance(e, hevents.ToolApprovalRequested):
            asked[e.tool_id] = e.tool_input
        elif isinstance(
            e, hevents.AutoApprovalEvaluationEnded | hevents.AutoApprovalEvaluationError
        ):
            # Auto mode's judgment of a gated call: a verdict, or a failure (a person decides).
            spend = asked.pop(e.tool_id, None)
            approved = (
                isinstance(e, hevents.AutoApprovalEvaluationEnded)
                and e.verdict is AutoApprovalVerdict.APPROVE
            )
            if spend is not None and not approved:  # escalated, or the evaluator failed
                yes = await ask(
                    f"agent wants to spend {spend['price']} {spend['currency']} hiring "
                    f"{spend['offering']} -- approve? [y/N] "
                )
                reason = "approved at the terminal" if yes else "denied at the terminal"
                await agent.approve_tool(e.tool_id, approved=yes, reason=reason)
        elif isinstance(e, hevents.MessageHandlerEnd):
            reply = e.output
    return reply


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audit domains with the buyer's agent.")
    parser.add_argument("domains", nargs="+")
    parser.add_argument("--cap", help="spend at most this many USD without asking (default 0.05)")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(main(args.domains, args.cap)), indent=2))
