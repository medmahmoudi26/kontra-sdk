"""The glue: a pulled Kontra offering as a tool of your own harness agent. Buyer code, so it
imports `kontra.hire`; nothing of it is in Kontra itself.

`gen/recon_harness` is what `kontra pull acme/recon-harness --lang python -o gen/recon_harness`
wrote: the published contract, verified against its hash, and nexgen's typed client of it --
`SweepRequest`, `SweepReport`, and the `AcmeReconHarness` service. The tool takes the
offering's input as the model would give it and returns its typed output:

1. builds the pulled `SweepRequest` (its own converter writes the contract's JSON);
2. quotes it (`quote_offering`, an activity on your worker: a quote locks nothing);
3. asks to spend the quote: `spend` is a tool of its own, not inherently safe, so the agent's
   approval policy decides -- auto-approved under the spending cap, a person asked above it
   (workflows.py). A denial ends here: nothing is hired, nothing held;
4. hires with `max_price` set to the approved price, so the escrow can never hold more than
   was approved, and waits for the escrowed job durably: `kontra.hire.service` quotes again,
   locks the hold and runs the job as a Nexus operation of this workflow (a later quote above
   the approved price locks nothing and fails the tool);
5. returns the delivery as the pulled `SweepReport`, checked by its own converter."""

import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import BaseModel
from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from gen.recon_harness import SweepReport, SweepRequest
    from temporal_agent_harness.harness import AgentToolContext, AgentWorkflowRunner, agent

    from kontra.hire import service

if TYPE_CHECKING:
    from kontra.hire import Kontra

#: The offering pulled into gen/recon_harness (any version of it the catalog serves).
OFFERING = "acme/recon-harness"
#: The auto-mode criteria set that judges a spend (workflows.py).
SPEND = "spend"
QUOTE_ACTIVITY = "quote_offering"

_T = TypeVar("_T")
#: Temporal's own converter, which writes and reads a pulled type with the converter nexgen
#: generated for it (`DataConverter.encode`/`decode`: pure, no codec, fine in workflow code).
_WIRE = DataConverter.default


async def to_wire(value: object) -> dict[str, Any]:
    """A pulled input as the contract's JSON, written by its own converter."""
    [payload] = await _WIRE.encode([value])
    return json.loads(payload.data)


async def from_wire(value: dict[str, Any], cls: type[_T]) -> _T:
    """A delivery as a pulled output type, read by its own converter (which refuses one that
    is not the contract's)."""
    payload = Payload(metadata={"encoding": b"json/plain"}, data=json.dumps(value).encode())
    [typed] = await _WIRE.decode([payload], [cls])
    return typed


class QuoteAsk(BaseModel):
    offering: str
    input: dict[str, Any]


class Quoted(BaseModel):
    offering: str
    price: str
    currency: str


class Quotes:
    """`quote_offering`, bound to your `Kontra` (main.py registers it on the worker)."""

    def __init__(self, kontra: "Kontra") -> None:
        self.kontra = kontra

    @activity.defn(name=QUOTE_ACTIVITY)
    async def quote(self, ask: QuoteAsk) -> Quoted:
        """What the job would cost; nothing is locked."""
        q = await self.kontra.quote(ask.offering, ask.input)
        if not q.ok:
            raise ApplicationError(
                f"{q.offering} will not take it: {q.reason or q.status.value}", non_retryable=True
            )
        if q.price is None or q.currency is None:  # never for an ok quote; not worth a retry
            raise ApplicationError(f"{q.offering} quoted no price", non_retryable=True)
        return Quoted(offering=q.offering, price=str(q.price), currency=q.currency)


@agent.tool_defn(auto_approval_criteria=SPEND)
async def spend(offering: str, price: str, currency: str) -> str:
    """Spend at most `price` `currency` of the buyer's balance hiring `offering`: an escrow
    hold, paid to the seller if the job delivers, refunded if it does not."""
    return price


@agent.tool_defn(inherently_safe=True)
async def sweep_domains(
    domains: list[str], runner: agent.Injected[AgentWorkflowRunner]
) -> SweepReport:
    """Sweep web domains' hosts and certificates: hires acme/recon-harness on Kontra (after the
    spend is approved) and waits for its report."""
    request = await to_wire(SweepRequest(domains=domains))
    quoted = await workflow.execute_activity(
        QUOTE_ACTIVITY,
        QuoteAsk(offering=OFFERING, input=request),
        result_type=Quoted,
        start_to_close_timeout=timedelta(seconds=60),
    )
    call = f"{AgentToolContext.for_current_tool_id().tool_id}/spend"
    approved = await runner.run_tool(call, spend, quoted.offering, quoted.price, quoted.currency)
    delivered = await service(OFFERING, max_price=approved).execute(request)
    return await from_wire(delivered.to_dict(), SweepReport)


#: The tools a model is given (workflows.py): the hiring tool, never `spend`, which only
#: `sweep_domains` calls, with its own quote -- so no price the model picks is put to approval.
MODEL_TOOLS = [sweep_domains]
#: The agent's tools: `AgentHarnessPlugin(tools=TOOLS)` (main.py).
TOOLS = [*MODEL_TOOLS, spend]
