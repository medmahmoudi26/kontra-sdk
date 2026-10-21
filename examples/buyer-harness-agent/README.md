# buyer-harness-agent: your own agent hires Kontra offerings as tools

The buyer's side of [`../recon-harness`](../recon-harness). `BuyerAgent` is your agent, written
with Temporal's [agent harness](https://github.com/temporal-community/temporal-agent-harness)
and run on your own worker, in your own namespace. One of its tools is a Kontra offering:
`sweep_domains` hires `acme/recon-harness`, waits durably for the escrowed job, and hands the
model the seller's typed `SweepReport`. This is an example, not an integration: the glue is a
page of `tools.py`, and nothing of it is in `kontra`.

| File | Owner of |
|---|---|
| `gen/recon_harness/` | what `kontra pull acme/recon-harness --lang python` wrote: the verified contract (`contract.nexusrpc.yaml`) and nexgen's typed client of it (`SweepRequest`, `SweepReport`, the `AcmeReconHarness` service) |
| `tools.py` | the glue: the pulled offering as a harness tool, `sweep_domains` (the one tool a model is given, `MODEL_TOOLS`), and the `spend` tool every hire goes through; the quote activity |
| `workflows.py` | `BuyerAgent` (`@agent.defn`, `@agent.accepts audit`) and its spending policy: auto mode with an evaluator that approves under a cap |
| `main.py` | runs the agent on `kontra.hire.worker` for one audit, asks you in the terminal when a spend is above the cap, prints the answer |

## The flow

1. **Pull the offering.** `kontra pull` fetches the catalog's contract, checks it against its
   hash, and generates a typed client (here already done, into `gen/`):

   ```sh
   kontra pull acme/recon-harness --lang python -o gen/recon_harness
   ```

   Generate from the published contract only: it is what the seller's worker is held to, and
   the hash the gate, your hold and the worker agree on (`docs/buyers.md`).
2. **Turn it into a tool.** `sweep_domains(domains)` is a harness tool (`@agent.tool_defn`)
   whose input becomes the pulled `SweepRequest` and whose result is the pulled `SweepReport`,
   both written and read by the converters nexgen generated for them.
3. **The agent calls the tool.** It quotes (an activity on your worker; a quote locks nothing),
   asks to spend the quote (below), then hires through `kontra.hire.service`, as
   [`../recon`](../recon) does: the job runs as a Nexus operation of the agent's own workflow,
   so the wait is durable -- your worker can restart mid-job -- and the delivery is decrypted
   with your key and checked against the contract.
4. **The typed result goes back to the model**, which words the answer.

## Spend control is the harness's approval policy

Every hire goes through the `spend` tool, which is not inherently safe, so the agent's
approval policy decides it, with the quote in hand: `spend(offering, price, currency)`. Only
`sweep_domains` calls it, with its own quote: a model is given `MODEL_TOOLS`, which leaves
`spend` out, so it can never put a price of its own choosing to approval. The agent runs auto
mode with its own evaluator, `under_the_cap`: a price in the cap's currency and at or under
the spending cap is approved; anything above it, or in another currency, is put to a person
(`main.py` asks):

```text
agent wants to spend 0.06 USD hiring acme/recon-harness@0.1.0 -- approve? [y/N]
```

A no is a denial: the tool raises, nothing is hired and nothing is held. A yes, or an approval
under the cap, hires with `max_price` set to the approved price, so the escrow can never hold
more than was approved (a later quote above it locks nothing and fails the tool). The cap is
the `spend` criteria's `spend_cap`, in `spend_currency` (0.05 USD unless whoever starts the
agent passes another in its `AgentConfig`: `python main.py --cap 0.01 ...`), and the evaluator
never denies on its own: a person does. Approvals and the cap are yours, on your side of the seam; the seller's agent
gets one message and gives one reply (one hire is one turn).

## Run it

The harness is your dependency, pinned exactly; it is pre-1.0.

```sh
pip install kontra-sdk temporal-agent-harness==0.6.0
kontra dev up --handle acme                     # a local Kontra, no containers, play money
cd ../recon-harness && kontra serve             # the seller; then, in another terminal, here:
python main.py example.com example.org          # 0.02 USD: under the cap, no question
python main.py --cap 0.01 example.com example.org
```

The agent's turn ends with the job delivered and its escrow still held: the agent hires, it
does not judge the delivery. Accept it (`kontra job <job id> --accept`, or `job.accept()`) to
pay now, or reject it with a reason; silence pays the seller when the review window closes
(`docs/buyers.md`).

With a real gate, sign in with `kontra login` (or set an API key, `KONTRA_API_KEY`) and hire
the seller's own offering: pull its client again under its name and point `OFFERING` in
`tools.py` at it.

**Why not a `kontra.contrib` adapter?** A generic one would build a tool from any pulled
client, which means a signature made at runtime for the model and per-language codegen
details in Kontra's own package, for what is a page of code that changes with each offering's
types. So it stays in the example (`docs/decisions.md` I37).

## Tested

`tests/examples/test_buyer_harness_agent.py` serves `../recon-harness` as `acme` on
`kontra.testing`'s marketplace (the dev gate) with a Temporal dev server and runs `python
main.py` as a buyer: under the cap the agent hires without asking and gets the typed report
back through the tool; above it the question is asked, and a no means no job at the gate and
no hold, a yes a hire whose `max_price` is the approved quote to the cent. It also checks that
`gen/recon_harness` is what `kontra pull` writes for the seller's published contract
(regenerated with nexgen when `NEXGEN_BIN` is set), and that the glue sends the contract's
JSON and refuses a delivery that is not its output.
