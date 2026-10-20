# Selling on Kontra

You write a Temporal workflow. Kontra adds a price, a catalog entry, escrow and encryption around
it, and buyers hire it through Nexus. The workflow runs on your machine as a Temporal worker;
Kontra never runs your code. The smallest offering, as a five-minute tutorial from
login to a paid job, is [`examples/hello`](../examples/hello).

- [The project](#the-project)
- [offering.yaml](#offeringyaml)
- [Types and the contract](#types-and-the-contract)
- [The quote](#the-quote)
- [The workflow and activities](#the-workflow-and-activities)
- [Files: `Artifact`](#files-artifact)
- [Progress](#progress)
- [Many activities](#many-activities)
- [Throughput: one heartbeating activity](#throughput-one-heartbeating-activity)
- [Agents](#agents)
- [The worker](#the-worker)
- [Testing without containers](#testing-without-containers)
- [Running locally: `kontra dev`](#running-locally-kontra-dev)
- [Publishing and serving](#publishing-and-serving)
- [Versions](#versions)
- [Getting paid](#getting-paid)
- [Schema-first and other languages](#schema-first-and-other-languages)

## The project

`kontra init <name>` writes it, with a test that runs the whole lifecycle:

```
wordcount/
  offering.yaml           declarative facts: name, version, limits, display text
  models.py               pydantic input and output types
  workflows.py            vanilla Temporal: `from temporalio import workflow`, no kontra
  activities.py           vanilla Temporal: `from temporalio import activity`, no kontra
  quote.py                the price, from the input
  worker.py               serve(Flow): binds workflow and quote to the offering; all worker
                          tuning; the one file that imports kontra
  tests/test_offering.py  the lifecycle against kontra.testing, no containers
  input.json              a sample input, for `kontra quote` and `kontra hire`
  pyproject.toml          one dependency, kontra-sdk; pytest settings
  README.md
```

The offering is `<party>/wordcount`, where the party is your profile's handle (or
`--party`).

Each fact has one owner. The rule that matters most: **`workflows.py` and `activities.py` are
vanilla Temporal** -- `from temporalio import workflow, activity` -- **and import nothing from
`kontra`** (`docs/decisions.md` I29). They are ordinary Temporal code, tested and run like any
other: delete `worker.py` and they run on any Temporal worker, unmodified -- all but
[`Artifact`](#files-artifact) I/O, which needs the Kontra runtime that `worker.py`'s plugin
installs in the process (an offering without `Artifact`, like `examples/hello`, needs
nothing). **`worker.py` is the one file that imports `kontra`**: it binds the code to the offering. Besides it,
`quote.py` takes `Quote` from `kontra.serve`, and `models.py` may use `kontra.types.Artifact`, a
pydantic model of a file reference, which your activities reach through `models.py`. Your
project depends on `kontra-sdk`, which brings `temporalio` in the range it is tested on
(see [Versions](#versions)); anything Temporal ships -- `temporalio.contrib`, its plugins, its
OpenAI Agents integration -- you import as Temporal documents it.

## offering.yaml

```yaml
name: acme/enrich               # <your party handle>/<offering>
version: 1.2.0                  # MAJOR.MINOR.PATCH; quote it if YAML would read a number
description: Resolve a URL list to company records
pricing_hint: "$0.001 per url"  # display only; the price comes from quote()
tags: [enrichment, web]         # optional; up to 16, lowercase
max_duration: 6h                # no delivery by then refunds the buyer
review_window: 24h              # the buyer may reject until then; silence accepts (0 = on delivery)
max_concurrent_jobs: 10         # optional; beyond this many open jobs quotes answer busy
max_output_bytes: 5GB           # the gate refuses artifact uploads beyond this
artifact_retention: 7d          # after accept; the buyer must download by then
uses_hosted_model: false        # optional; true when the input goes to a model provider
```

| Key | Range |
|---|---|
| `max_duration` | 1s to 30d |
| `review_window` | 0 to 30d |
| `artifact_retention` | 1s to 90d |
| `max_output_bytes` | up to 5 TiB (`512MB`, `5GB`, `1GiB`: SI and IEC units both work) |
| `max_concurrent_jobs` | up to 10,000 |

`uses_hosted_model: true` says the job sends the buyer's input to a hosted model (an LLM API),
where it is plaintext to that provider: Kontra's encryption ends at your worker. It is a fact
of the contract, shown on the offering's A2A card (`usesHostedModel`, and a sentence in the
skill's description); `false` is the same as leaving it out, and the document carries the key
only when it is true.

Durations accept `90s`, `30m`, `6h`, `7d` and combinations such as `1h30m`. The catalog stores
one canonical spelling, so two offerings with the same limits hash the same whatever their
authors typed.

Nothing in this file names code. Keys such as `workflow`, `quote` or `task_queue`, and values
that look like an import path (`workflows.EnrichFlow`), are refused: the binding lives in
`worker.py`. The catalog, the console and agents read this file without running any Python.

## Types and the contract

The input and output types of your workflow's `run` method are the contract:

```python
# models.py
from pydantic import BaseModel, ConfigDict, Field
from kontra.types import Artifact


class EnrichInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    urls: list[str] = Field(min_length=1, max_length=10_000)


class EnrichOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    records: Artifact  # a file: a reference, never the bytes
    summary: dict[str, int]  # small, inline
```

`kontra publish` derives a JSON Schema 2020-12 **Nexus document** from them (the format
[nexgen](https://github.com/temporalio/nexgen) consumes) and adds your offering facts under
`x-kontra`. Every object gets `additionalProperties: false`; constructs the document cannot
express portably (`anyOf`, tuples, untyped `dict` or `Any`) are refused at derivation with a
fix-it. Field patterns are matched with RE2 on both sides, so no pattern can stall a handler.

The contract says what each side gets. The input is described as pydantic *validates* it:
what a buyer may send. The output is described as pydantic *serializes* it: what the buyer
receives, `model_dump(mode="json", by_alias=True)`, which is what temporalio writes for your
workflow's result. So in the output a `@computed_field` is a member, a `Field(exclude=True)`
field is not, a `serialization_alias` is the member's name, a serializer's return type is the
member's type, and every field is required, defaulted or not, since the result always
carries it. A model used both ways whose two descriptions differ is published twice, as
`ReportInput` and `ReportOutput` (your own classes keep their names). A bound written on a
named type and again where the type is used holds twice: the contract publishes the tighter
bound and the values both allow, and refuses, naming the field, the two that cannot be
written as one (two different `pattern`s) or that no value meets, rather than publish a
contract looser than your model. A `Field` constraint on a model, a `RootModel` or a plain
`Enum` (`Annotated[Code, Field(max_length=10)]`) is refused: pydantic applies it to the
instance, not its value, and your model refuses every value; put the constraint inside the
type. Two outputs are refused because pydantic's schema would not say what it writes: a
serializer with no return annotation (annotate it), and a `timedelta`, which pydantic writes
as an ISO duration that `format: duration` refuses whenever it has a fraction of a second
(write seconds: `model_config = ConfigDict(ser_json_timedelta="float")`). Whatever payload
converter your client has, your workflow's result is delivered as `model_dump(mode="json",
by_alias=True)` (decisions I38).

See exactly what you would publish:

```sh
kontra contract                 # YAML; --json for JSON
kontra contract --nexgen        # the nexgen view (without x-kontra)
```

The document is sealed with a `contract_hash` (sha256 of its canonical JSON). That hash
locks the catalog, your worker and every buyer's hold token together; see
[Versions](#versions).

## The quote

```python
# quote.py
from kontra.serve import Quote
from models import EnrichInput


async def quote(inp: EnrichInput) -> Quote:
    if len(inp.urls) > 10_000:
        return Quote.reject("max 10,000 urls per job")
    return Quote(price=max(len(inp.urls) * 0.001, 0.01), currency="USD")
```

- `Quote(price, currency="USD", max_duration=None)`: a fixed price, rounded half-up to the
  currency's minor unit. `max_duration` can shorten this job's deadline, never lengthen it.
- `Quote.reject(reason)`: refuse this input, with a reason the buyer can act on.
- `Quote.busy(eta, reason=None)`: come back after `eta` (a `timedelta` or seconds). The gate
  also answers busy by itself when `max_concurrent_jobs` jobs are open.

Keep it cheap and deterministic. Your handler runs it **again** on the input it actually
receives at `execute` and refuses the job if the price exceeds what the buyer's hold covers:
a buyer cannot quote a small input and then send a large one. A quote's answer travels in
plaintext (the gate must read the price to lock it); its input is encrypted.

## The workflow and activities

Plain Temporal: activities, retries, timers, child workflows, signals, updates, continue-as-new.
`temporalio.workflow` and `temporalio.activity` are the whole API, `RetryPolicy` comes from
`temporalio.common`, and `ApplicationError` from `temporalio.exceptions` (raise it with
`non_retryable=True` to stop retries). Kontra adds nothing to the namespace you import:
`kontra.workflow`, `kontra.activity` and the `RetryPolicy`, `ApplicationError` and `Page`
re-exports are gone. Code written for them fails at import with Python's own error, which
names no replacement: `from kontra import workflow` (or `RetryPolicy`, ...) is an
`ImportError`, "cannot import name 'workflow' from 'kontra'", and `import kontra.workflow` or
`from kontra.workflow import defn` a `ModuleNotFoundError`. Only `kontra.workflow` read as an
attribute says what to write. The replacements: `from temporalio import workflow, activity`,
`from temporalio.common import RetryPolicy`, `from temporalio.exceptions import
ApplicationError`, and `from kontra.contrib.parallel import Page` (or a model of your own).

```python
# workflows.py
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import fetch_batch, write_records
    from models import Batch, EnrichInput, EnrichOutput


@workflow.defn
class EnrichFlow:
    @workflow.run
    async def run(self, inp: EnrichInput) -> EnrichOutput:
        records = await workflow.execute_activity(
            fetch_batch,
            Batch(urls=inp.urls, done=0, total=len(inp.urls)),
            start_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(minutes=1),
        )
        artifact = await workflow.execute_activity(
            write_records, records, start_to_close_timeout=timedelta(minutes=10)
        )
        return EnrichOutput(records=artifact, summary={"urls": len(inp.urls)})
```

(The real [`examples/enrich`](../examples/enrich/workflows.py) fetches in batches of 50.)

Your handler starts this workflow as `job-<job id>` in your namespace, with the job's
`max_duration` as its execution timeout and at most one execution per hold. The workflow's
result is the delivery: it is encrypted to the buyer and recorded by hash. A failed workflow
is a failed job, and the hold refunds.

Keep workflow classes in an importable module (not `__main__`): Temporal's sandbox
validates them by import.

Activities need no list: `serve()` registers every function `@activity.defn` decorates in your
project's modules (see [The worker](#the-worker)).

## Files: `Artifact`

Temporal payloads are capped at a few MB. Anything larger is an `Artifact`: a reference to a
file in Kontra's bucket, encrypted with age to the job's parties.

```python
# activities.py -- Artifact comes from models.py
from models import Artifact, Record
from temporalio import activity


@activity.defn
async def write_records(records: list[Record]) -> Artifact:
    path = ...  # write JSONL to a local file
    return await Artifact.from_path(path, content_type="application/x-ndjson")
```

`Artifact.from_path` encrypts while it streams (constant memory, multipart for large files),
tells the gate the exact size first so an upload above `max_output_bytes` is refused before a
byte is stored, and returns `Artifact{key, size, sha256, content_type}` (size and sha256 of the
plaintext). Inline values above 256 KiB of ciphertext are claim-checked into the bucket
automatically; use `Artifact` for anything that is a file.

## Progress

Nothing to write. Kontra derives progress from what Temporal already records (state, the
activities running, how many completed) and forwards your activities' heartbeats, encrypted
to the buyer:

```python
activity.heartbeat({"percent": 40.0, "message": "400 of 1,000 urls"})
```

Details that are a mapping with `percent` or `message` render as such; anything else renders
as "alive, in `<activity>`, at `<time>`". Heartbeats are forwarded at most once a second per
job, latest wins (`KONTRA_PROGRESS_INTERVAL` changes the interval). Forwarding is best effort:
it never raises into, or blocks, your activity.

## Many activities

One activity per item, each with its own retries and timeouts, is plain Temporal: start one
`workflow.execute_activity` per item, and bound how many are open with an `asyncio.Semaphore`
([`examples/sweep`](../examples/sweep) does this for up to 200 URLs):

```python
# workflows.py
import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import probe
    from models import ScanInput, ScanOutput


@workflow.defn
class Scan:
    @workflow.run
    async def run(self, inp: ScanInput) -> ScanOutput:
        slots = asyncio.Semaphore(50)  # at most 50 probes open at once

        async def one(target: str) -> bool:
            async with slots:
                return await workflow.execute_activity(
                    probe, target, start_to_close_timeout=timedelta(seconds=30)
                )

        hits = await asyncio.gather(*(one(t) for t in inp.targets))
        return ScanOutput(hits=sum(hits))
```

Every item is then an activity of your workflow, so its input and result are in the job's
history: about six events per item (the activity's three, and the workflow task its result
wakes). Temporal allows one history 51,200 events and 50 MB (and warns from 10,240 events and
10 MB), so this shape suits hundreds to a few thousand items. Past that, keep the history
bounded yourself: run the items in child workflows, one per slice, or continue as new
(`workflow.continue_as_new`, when `workflow.info().is_continue_as_new_suggested()`) carrying
only a cursor and what you have folded so far. Continuing as new is yours to use: nothing in
Kontra refuses it.

One workflow runs a few hundred activities a second. Past that, partition the items and run
one child workflow per partition -- Temporal's own composition, in your code:

```python
import asyncio

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from scanpart import ScanPart  # a workflow over part p of the items


@workflow.defn
class Scan:
    @workflow.run
    async def run(self, parts: int) -> int:
        runs = [workflow.execute_child_workflow(ScanPart.run, p) for p in range(parts)]
        return sum(await asyncio.gather(*runs))
```

Pass the children to `serve()` as `workflows=[ScanPart]` (see [The worker](#the-worker)).

### The optional helper: `kontra.contrib.parallel`

`execute_activities_in_parallel` does the bounded fan-out and the continue-as-new for you, over
a paged source of any size, in a child workflow, `kontra.batch.v1`. It is an optional helper,
not part of Kontra's core (`docs/decisions.md` I29), and it costs the rule above: the workflow
that calls it imports `kontra.contrib.parallel`, so it runs only on a worker that has
kontra-sdk installed and the helper's plugin -- any Temporal worker with
`plugins=[ParallelPlugin()]`, but no longer one without.

```python
# workflows.py
from datetime import timedelta

from temporalio import activity, workflow

from kontra.contrib.parallel import Page, execute_activities_in_parallel


@activity.defn
async def next_page(cursor: str | None) -> Page[str]:
    keys, token = ..., ...  # one page of your source: a bucket listing, an API, a table
    return Page(items=keys, next=token)  # next=None: that was the last page


@activity.defn
async def probe(target: str) -> bool: ...


def count_hits(acc: int, hit: bool) -> int:
    return acc + hit


@workflow.defn
class Scan:
    @workflow.run
    async def run(self, scope: str) -> int:
        return await execute_activities_in_parallel(
            probe,
            pages=next_page,
            reduce=count_hits,
            init=0,
            max_concurrent=200,
            start_to_close_timeout=timedelta(seconds=30),
        )
```

```python
# worker.py
from workflows import Scan

from kontra import serve
from kontra.contrib.parallel import ParallelPlugin

serve(Scan, plugins=[ParallelPlugin()])  # registers kontra.batch.v1, passes the helper through
```

- **`pages=`** is the source: an activity `(cursor) -> page`, called first with `None` and then
  with each page's `next` until one is `None`. A page is anything with a list `items` and a
  `next`: `Page`, or a pydantic model of your own, so `activities.py` needs nothing from
  `kontra`. The cursor is anything serializable: an offset, a continuation token. Pages are
  fetched by activity, so no item list ever enters your workflow's history.
- **`items=[...]`** instead is one page held in memory. The list travels in the batch's input,
  so it must fit one Temporal payload (a few MB); anything larger is `pages=`.
- **`reduce=`** folds each result into an accumulator, starting from `init=`, in the order
  results complete. It runs in the workflow, so it must be pure and deterministic, and it is a
  module-level function (so are the activities): it crosses into the batch by name. The
  accumulator is the only result state kept, so it stays as small as you make it. Without
  `reduce=` the results come back as a list -- right for a small batch, a payload-size failure
  for a large one: count, sum or keep the top N instead, or write results out from the
  activity (an `Artifact`).
- **`max_concurrent`** (default 100) caps the item activities open at once; the next page is
  fetched only when there is room, so scheduled-but-unstarted work never piles up.
- Every other keyword is a Temporal activity option (`start_to_close_timeout` or
  `schedule_to_close_timeout` required, `retry_policy`, `heartbeat_timeout`, ...), applied to
  every item and page activity. The task queue is your offering's, never an option.

The child runs on your worker's task queue (`ParallelPlugin` registers it; without the plugin
the call fails, as any unregistered workflow does). It continues as new when Temporal suggests
it, after letting what is in flight finish, and carries only the cursor, the accumulator and
the current page's unstarted items into the next run: history is bounded by
`max_concurrent`, never by the number of items, and your workflow just awaits one call. An item
that fails after its retries fails the batch -- what is in flight is cancelled first -- and the
call raises Temporal's `ChildWorkflowError`, with the activity's own error below it.

## Throughput: one heartbeating activity

When per-item durability is noise -- "probe 500,000 hosts", where a lost probe is simply
probed again -- the fastest shape is not one activity per item but one activity doing its own
concurrency and heartbeating its cursor:

```python
# activities.py
import asyncio

from temporalio import activity


async def probe_host(host: str) -> bool: ...


@activity.defn
async def probe_all(hosts: list[str]) -> int:
    details = activity.info().heartbeat_details
    start, hits = details[0] if details else (0, 0)  # where the last attempt got to
    slots = asyncio.Semaphore(1000)

    async def one(host: str) -> bool:
        async with slots:
            return await probe_host(host)

    for at in range(start, len(hosts), 1000):
        hits += sum(await asyncio.gather(*(one(h) for h in hosts[at : at + 1000])))
        activity.heartbeat((at + 1000, hits))  # also the buyer's progress
    return hits
```

Three history events in all, whatever the count; a retry resumes at the last heartbeat (give
the activity a `heartbeat_timeout`); scale by running several of them, one per slice. Nothing
here is Kontra's: `activity.heartbeat` and `activity.info` are Temporal's. A real one reads its hosts
from a page source rather than its input, which, like any input, must fit a payload.

## Agents

A workflow can run an agent, and Kontra switches nothing on: the agent's SDK is your
dependency, and its Temporal plugin is your worker's config, passed in `serve(plugins=[...])`
as on any Temporal worker (`docs/decisions.md` I29). There are three shapes.

**An agent as workflow code**, [`examples/chef-agents`](../examples/chef-agents). OpenAI's
[Agents SDK](https://openai.github.io/openai-agents-python/) runs its loop in the workflow,
durably, through Temporal's integration (`pip install temporalio-openai-agents`): every model
call and every tool call is an activity, retried and recorded like any other.

```python
# workflows.py
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import lookup_pantry
    from agents import Agent, Runner  # OpenAI's Agents SDK: your dependency
    from models import CookInput, Dish
    from temporalio.openai_agents.workflow import activity_as_tool


@workflow.defn
class ChefFlow:
    @workflow.run
    async def run(self, inp: CookInput) -> Dish:
        chef = Agent(
            name="chef",
            instructions="Look in the pantry, then propose one dish from what is there.",
            tools=[activity_as_tool(lookup_pantry, start_to_close_timeout=timedelta(seconds=30))],
        )
        result = await Runner.run(chef, inp.craving)
        return Dish(recipe=str(result.final_output))
```

```python
# worker.py
import os

from agents import (
    OpenAIProvider,
    set_default_openai_api,
    set_default_openai_client,
    set_tracing_disabled,
)
from openai import AsyncOpenAI
from temporalio.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin
from workflows import ChefFlow

from kontra import serve

set_default_openai_client(
    AsyncOpenAI(base_url=os.environ["LLM_BASE_URL"], api_key="unused", max_retries=0),
    use_for_tracing=False,
)
set_default_openai_api("chat_completions")
set_tracing_disabled(True)

serve(
    ChefFlow,
    plugins=[
        OpenAIAgentsPlugin(model_params=ModelActivityParameters(), model_provider=OpenAIProvider())
    ],
)
```

| Runs as | What |
|---|---|
| workflow code | the agent loop, handoffs, and `@function_tool` tools: pure computation only (no I/O, no clock, no randomness) |
| an activity | every model call, every `activity_as_tool(...)` tool, every MCP operation |

- **The plugin is yours.** `serve()` puts `OpenAIAgentsPlugin` on the worker's Temporal
  client, ahead of Kontra's own plugin, which comes last (see [The worker](#the-worker)). The
  client's payload converter is then the plugin's `OpenAIPayloadConverter`, which validates
  more leniently inside your workflows and activities (a buyer's input is still checked
  against your contract before the job starts), and Kontra's codec encrypts every payload
  under it: the codec is a payload *codec*, so it stacks under any converter. A worker whose
  `worker.py` passes no plugin runs none, whatever its workflows import.
- **The model endpoint is worker config**, never workflow code: the agent names no model and
  no server. `worker.py` sets the Agents SDK's own default client and API before `serve()`,
  and the plugin's model activity dials them, so any OpenAI-compatible server answers --
  Ollama, vLLM, OpenRouter, Mistral. That takes `model_provider=OpenAIProvider()`: without it,
  the plugin builds its own `AsyncOpenAI()` from the environment when the worker is built,
  and never asks the default client. The model is the SDK's `OPENAI_DEFAULT_MODEL`, in the
  worker's environment. The API is read when the worker is built, the client at the first
  model call, so the worker starts without an `OPENAI_API_KEY`; with no default client set,
  it is the SDK's own, from `OPENAI_API_KEY` (and `OPENAI_BASE_URL`).
- **MCP servers** are the plugin's too: `OpenAIAgentsPlugin(..., mcp_servers={"name": factory})`
  serves `temporal_mcp_server("name")` (from `temporalio.openai_agents.workflow`) in the
  workflow (install `temporalio-openai-agents[mcp]`). A factory runs on the worker, so it holds
  the processes and credentials an MCP server needs, and a name no workflow asks for is unused.
  Its `factory_argument=` is an activity input, so it is recorded in history: never put a
  secret there; resolve credentials in the factory.
- An activity tool gets a **copy** of the run context: what it changes does not come back.
- A tool that wants scale runs [one activity per item](#many-activities) itself (make it a
  workflow the agent's activity starts, or a heartbeating activity), rather than having the
  model issue a thousand tool calls.
- Set `uses_hosted_model: true` in `offering.yaml`: the buyer's input, decrypted on your
  worker, goes to the model provider in plaintext to it. Model calls are activity payloads,
  encrypted in Temporal like the rest of the job.

**One handler of an agent**, written with Temporal's agent harness
(`pip install temporal-agent-harness`). `serve()` takes the handler itself, and one hire is
one turn of the agent:

```python
# recon.py -- vanilla Temporal and the harness, nothing from kontra
from pydantic import BaseModel
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import AgentWorkflowRunner, agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        MessageContext,
        ToolApprovalPolicy,
    )
    from temporalio.contrib.workflow_streams import WorkflowStream


class Sweep(BaseModel):
    """A host to sweep."""

    host: str


class Report(BaseModel):
    """What the sweep found."""

    host: str
    open: int


@agent.defn
class ReconAgent:
    """Sweeps hosts, one per message."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )

    @agent.accepts
    async def sweep(self, req: Sweep, ctx: agent.Injected[MessageContext]) -> Report:
        """Sweep one host and report its open ports."""
        ...
```

```python
# worker.py
from recon import ReconAgent
from temporal_agent_harness.plugin import AgentHarnessPlugin

from kontra import serve

serve(ReconAgent.sweep, plugins=[AgentHarnessPlugin()])
```

- **The contract is the handler's**: its one pydantic message (`Sweep`) is the input, its
  pydantic return (`Report`) the output; parameters the harness injects
  (`agent.Injected[...]`) are not part of it. Pass the handler, `ReconAgent.sweep`, never the
  class: `serve(ReconAgent)` is refused, naming the fix. The class must be module-level and
  importable, and its init data, if it takes any, optional: a job starts the agent with none.
  A handler an agent inherits unchanged from another agent class is refused too
  (`ReconV2.sweep` *is* `ReconV1.sweep`, so it cannot say which agent you mean): override it
  in each agent class that sells it. Pydantic aliases work as on a workflow: the message is
  the input's JSON by alias, and the job's result is the reply's serialization by alias,
  exactly a workflow's result. The harness publishes the reply keyed by field name; Kontra
  re-keys it, checks it against the contract and delivers it, so computed fields, excluded
  fields and serializers come out as the contract describes them.
- **Two schemas, one contract.** The harness emits its own schema of the same models
  (`temporal-agent-harness schema recon:ReconAgent`), and its codegen turns that into
  TypeScript: use it for your own UI, which is generated from your code. Buyers never see it:
  `kontra pull` generates a buyer's client from the published contract, after checking the
  catalog's hash -- the only schema a buyer should trust, since your code is exactly what they
  cannot see. So `kontra publish` and `kontra contract` compare the two for the handler you
  sell. Both describe the input as validated and the output as your reply is serialized
  (decisions I38), so once both are in one canonical form they are equal, and nothing is
  printed: names, titles, descriptions, key order, `anyOf` against `oneOf` for an optional
  field, a default beside an output field that is always there, a constraint on a type and
  again where it is used written as one, and the other differences that change no valid
  value are set aside, as is the contract closing a model that keeps pydantic's default
  `extra="ignore"`, which changes no value exchanged (decisions I33). What remains is
  printed on stderr, and the command goes on either way; the contract is what is published.
  The models above agree; with `model_config = ConfigDict(extra="allow")` on `Report`:

  ```text
  warning: acme/recon@1.0.0 (ReconAgent.sweep): the contract differs from temporal-agent-harness's schema for this handler in 1 place. Buyers get the contract (`kontra pull`); your harness-codegen types follow the harness's:
    output $defs.Report.additionalProperties: contract false, harness true
    (open) the contract closes every model (no keys but its fields) and the harness's schema leaves this one open, as extra="allow" does: the model keeps other keys and serializes them into its reply, which the contract refuses, so a reply that carries one fails the job; pydantic's default (extra="ignore", which drops them) or extra="forbid" on the model makes the two agree
  ```

  Each line is one place, in the contract's terms:

  | Line | What differs | What to do |
  |---|---|---|
  | `open` | the harness's schema leaves a model open to other keys (`additionalProperties: true`, from `extra="allow"`: the model keeps them, and serializes them into your reply, which then fails the job), and the contract closes every model to its fields | drop `extra="allow"`: keep pydantic's default, or set `model_config = ConfigDict(extra="forbid")`, with which your agent also refuses unknown keys from any caller, as the gate does |
  | any other | the two emitters describe the same model differently, which neither should: a bug in Kontra's derivation or in the harness's | report it, with the model, at the address the warning gives. Buyers still get the contract |

  Nothing is compared, and nothing said, for a workflow entry or when the harness is not
  installed. The harness's schema comes from its own `schema` command, run by the same Python
  as `kontra publish`; if it cannot answer (your agent's module fails to import there), the
  warning says why and publishing goes on. The seam ③ tests hold the two equal for the test
  agents and a zoo of every kind of field (`tests/contract/test_harness.py`), so a harness
  release, or a change to Kontra's derivation, that walks models differently fails CI.
- **A hire is one turn.** The job is Kontra's wrapper workflow, `kontra.turn.v1`, started as
  `job-<id>` like any job. It starts your agent as a child (its whole history encrypted to you
  alone), sends the buyer's input as one message to the handler, waits for the handler's
  reply on the agent's event stream, closes the agent, and delivers the reply, encrypted to
  the buyer. A handler that raises fails the job with its message, without retrying. A
  worker that stops mid-turn (a deploy, a crash) loses nothing: the turn's activity is retried
  on the next worker and picks the same message up where it was.
- **No approvals, no callbacks.** A job has no buyer to ask: a handler that requests a tool
  approval or a callback fails the job at once. Run tools that need no approval
  (`ToolApprovalPolicy.allow_inherently_safe()`, or your own policy): a call the policy
  approves raises no request at all. That includes the tools an auto-mode evaluator would
  approve: the harness publishes the approval request before it asks the evaluator, and
  nothing tells, at that moment, whether a person will be needed, so the job fails there too;
  allow-list those tools in the policy instead. Session jobs, with buyer approvals, are
  kontra-sdk#60.
- `kontra graph` does not draw a handler offering yet (kontra-sdk#56).

**An agent that is a process**, [`examples/chef-claude`](../examples/chef-claude): no SDK at
all. One activity spawns the binary -- here `claude -p ... --output-format stream-json
--verbose --allowedTools ... --max-turns ...` -- with `asyncio.create_subprocess_exec`,
heartbeats once per JSON line it prints and returns on its `result` event; the workflow is one
`execute_activity` with a `heartbeat_timeout` and a `RetryPolicy`. A crashed worker is noticed
within the heartbeat timeout and the activity is retried, and the retry does not start over:
attempt 1 names the binary's session (`--session-id`, a `uuid5` of the workflow id) and a
retry passes `--resume` with the same id; when the binary says this machine does not hold
the session, or already does, the attempt runs the other. That is the shape for any agent
whose loop is a process rather than workflow code. The buyer writes part of its prompt, so
give the binary only the tools a stranger's words may reach, none of your own setup, and the
prompt on stdin rather than on a command line every local user can read.

## The worker

```python
# worker.py
from datetime import timedelta

from workflows import EnrichFlow

from kontra import serve

serve(
    EnrichFlow,
    # every temporalio.worker.Worker keyword, passed through verbatim:
    max_concurrent_activities=40,
    max_task_queue_activities_per_second=50,
    graceful_shutdown_timeout=timedelta(seconds=60),
)
```

`serve(Flow)` binds `offering.yaml` and the `quote` of `quote.py`, both beside `worker.py`, to
`Flow` -- a `@workflow.defn` class, or [one handler of an agent](#agents) -- and registers
every function `@activity.defn` decorated in your project: the modules loaded from under
`worker.py`'s directory by then (dot-directories, `tests`, `test`, `venv`, `build`, `dist`,
`node_modules` and `site-packages` left out), each activity once, in its own module. An
activity a factory of your project makes (`fetch = make_activity("fetch")` in a module of the
project, `make_activity` in another) is found where it is bound. An activity nothing imports
cannot be referenced by a workflow either, so there is no list to keep -- except for the
activities the scan cannot see: a method (it needs its instance), and one defined or made
outside the project (a library's), which you pass in `activities=`. Two activities of one
Temporal name are refused, naming both: rename one (`@activity.defn(name=...)`). Every other keyword is `temporalio.worker.Worker`'s, passed
through verbatim: `workflows=[Child]` registers further workflows, unbound, for the children
`Flow` starts; `activities=[...]` adds to what was found (methods decorated as activities need
their instance: `activities=[obj.method]`); `plugins=[...]` adds Temporal plugins. `quote=`
passes another quote function.

**Plugins.** A plugin that is also a client plugin -- any `temporalio.plugin.SimplePlugin`:
Temporal's `OpenAIAgentsPlugin`, the agent harness's `AgentHarnessPlugin`,
`kontra.contrib.parallel.ParallelPlugin` -- goes on the worker's Temporal client, in your
order, and Kontra's own plugin, `KontraPlugin`, comes last: a payload converter yours chose
stays, and the connection, the namespace and the codec are Kontra's. A worker-only plugin goes
to the worker, where it runs after Kontra's configuration: one that changes the task queue,
removes Kontra's Nexus service, puts another interceptor before Kontra's, swaps the entry
class for another of the same name, or builds the worker on a client without Kontra's codec is
refused before the worker polls. Kontra also refuses a client plugin that sets another payload
codec or failure converter (Kontra's encrypt), or external storage that would offload a job's
payload at or below Kontra's claim-check threshold, where the buyer could not read it -- and
checks the client again when it builds and starts a worker, so a client plugin placed after
Kontra (in a worker main of your own) that sets any of them back, or encodes headers, is
refused there.

`kontra serve` and `python worker.py` run it; `kontra publish` imports `worker.py` and reads
the offering from it without starting anything (imported, `serve()` only builds the worker
and returns it). The worker is `kontra.serve.Worker`, which you may also write out yourself
(several offerings on one worker, explicit lists):

```python
# worker.py
from activities import fetch_batch, write_records
from quote import quote
from workflows import EnrichFlow

from kontra.serve import Worker, offering

worker = Worker(
    offerings=[offering("offering.yaml", entry=EnrichFlow, quote=quote)],
    activities=[fetch_batch, write_records],
    max_concurrent_activities=40,
)

if __name__ == "__main__":
    worker.run()
```

Written out, a `Worker` registers exactly the activities you list (nothing is discovered).
`kontra.serve.Worker` is Temporal's client and worker with `KontraPlugin` on them, and the
plugin adds exactly these: the connection from your profile (the Temporal address, TLS, the
namespace, a worker token kept fresh), a generated Nexus service per bound offering (`quote`
as a synchronous operation, `execute` as a workflow-run operation), the hold-token check
before `execute` starts anything, the encryption codec, the task queue, derived from
`name@version` (passing `task_queue` is a `TypeError`), and at start the hash lock (see
[Versions](#versions)). It passes `kontra` through the workflow sandbox: Kontra's own turn
wrapper runs in it, your models may use `kontra.types`, and a workflow that hires imports
`kontra.hire`. `workflows`, `nexus_service_handlers` and `interceptors` are merged with
Kontra's, and a `workflow_runner` that is Temporal's `SandboxedWorkflowRunner` reaches the
inner worker as a copy with `kontra` added to its passthrough modules (any other runner is
untouched); every other keyword reaches the inner worker unchanged. Written out, `worker.py`
exposes a module-level `worker`: `kontra serve` runs it and `kontra publish` imports it to
find the offerings.

### Your own worker main: `KontraPlugin`

A process of your own, with its own Temporal client and workers, needs no `kontra.serve`: put
`KontraPlugin` last on the client and build Temporal's own worker. Whatever you pass for the
target host and the task queue is replaced by the profile's and the offering's:

```python
# main.py -- offering.yaml and quote.py beside it
import asyncio

from activities import fetch_batch, write_records
from temporalio.client import Client
from temporalio.worker import Worker
from workflows import EnrichFlow

from kontra import KontraPlugin


async def main() -> None:
    kontra = KontraPlugin.from_profile(EnrichFlow)  # binds offering.yaml beside this file
    client = await Client.connect("kontra", plugins=[kontra])  # your client plugins first
    worker = Worker(
        client, task_queue="kontra", workflows=[EnrichFlow], activities=[fetch_batch, write_records]
    )
    try:
        await worker.run()
    finally:
        await kontra.aclose()


asyncio.run(main())
```

`KontraPlugin.from_profile(entry)` binds `offering.yaml` (and `quote.py`, unless `quote=`) beside
the calling file, as `serve()` does; the entry is required, because the contract, the task
queue and the Nexus service come from it, and the yaml names no code. It discovers no
activities: list them on your worker. Any other worker of the same client is left alone; the
plugin claims a worker by the entry class in its `workflows`. `KontraPlugin(*offerings)` takes
offerings bound with `offering(...)` instead, for several on one client. A client built from a
service client, `Client(service_client, plugins=[...])`, skips the connection hook: `await
kontra.prepare()` before it. `kontra publish` still reads the offering from `worker.py`, so
keep one beside `main.py` (`worker = serve(EnrichFlow)`, never run by `main.py`); a `kontra
dev` profile publishes at start without it.

Before `execute` starts your workflow, the handler checks the hold token offline (signature
against the gate's keys, audience, expiry, your namespace, the offering, the contract hash),
validates the input against the contract, and re-runs your quote. No gate call is on that path.

## Testing without containers

`kontra.testing` has a fake for every seam. `Marketplace` wires a Temporal dev server
(downloaded once), the fake gate (catalog, quote relay, escrow by the platform's own rules,
play money) and logged-in parties into one process. The test serves the worker `worker.py`
builds, so name it there -- `worker = serve(Flow, ...)`, as `kontra init` writes it:

```python
# tests/test_offering.py -- `pytest` from the project directory
import asyncio

from kontra.testing import Marketplace


def test_lifecycle() -> None:
    asyncio.run(lifecycle())


async def lifecycle() -> None:
    from worker import worker  # worker = serve(...): what `kontra serve` runs

    async with Marketplace() as mp:
        seller = await mp.party("acme", balance=0)  # offering.yaml: name: acme/wordcount
        buyer = await mp.party("buyer", balance="10.00")  # play money
        async with mp.serve(worker, seller):  # what `kontra serve` does, publish included
            k = buyer.kontra()
            q = await k.quote("acme/wordcount", text="the quick brown fox")
            assert q.ok, q.reason
            job = await q.hire(max_price="1.00")
            result = await job.result(timeout=60)
            assert result.words == 4
            await job.accept()
```

The test `kontra init` writes does exactly this; read it for a full lifecycle, including a
seller that never delivers and the refund at the deadline. Install with
`pip install "kontra-sdk[testing]" pytest`.

## The graph: `kontra graph`

Temporal keeps no definition of a workflow, only what each run did. `kontra graph` draws
yours before anyone pays for a run, from mock runs:

```sh
kontra graph                     # from the directory with worker.py: one line per edge
kontra graph --mermaid           # a Mermaid flowchart, for a `mermaid` block GitHub renders
kontra graph --json              # [{"offering", "runs", "graph", "never_scheduled"}]
kontra graph --runs 16 --seed 7  # more runs, other random values
kontra graph --input in.json     # every run starts with this input
```

For each offering `worker.py` binds (`--offering NAME@VERSION` picks one, `--dir` names
another directory), the workflow runs `--runs` times (default 8) in Temporal's time-skipping
test server, with what `serve()` registers -- the bound workflow and the further ones it
registers (the children it starts) -- and `kontra.contrib.parallel`'s batch child, under
Kontra's sandbox and payload converter, but none of the worker's other options or plugins
(but one, below), and no codec. Every activity is
answered by a stub with a value of the type it declares, so no activity body runs, nothing
is fetched and no key is needed; timers are skipped. The input is drawn the same way from the
workflow's input type, unless `--input` gives one (checked against the type first). Run 0
draws the smallest values (false, 0, empty lists, no optional fields), run 1 the largest
(true, 3 items, every field), run 2 large lists near the top of the value (201 items, so a
loop that batches its input by 50 or 100 goes round), and the others random ones from
`--seed`: an `if` on a boolean an activity returned shows both branches from two runs on,
however deep in the value it sits (only a recursive model is cut short, where it recurses),
and the same command prints the same graph. The runs' graphs are then united: a loop is one
node with an edge back, the activities one workflow task starts at once share a parallel
group (an `if`'s branches, which no run starts together, never do), and the optional
helper's `execute_activities_in_parallel` is one node, `execute_activities_in_parallel(<activity>)`
(a `pages=` batch names its page activity too, the two sorted), however many items it ran,
none included, and however many times it continued as new.
[`examples/sweep`](../examples/sweep) shows a fan-out, a branch, and one activity per URL as
one node (with an edge back from the run whose 201 URLs outnumber the 50 it keeps open), and
every example's README shows its graph.

An agent workflow (one whose `worker.py` passes Temporal's `OpenAIAgentsPlugin` in
`plugins=`) runs with that integration and a scripted model instead of yours: it calls every
tool the agent was given once, then answers with a value of the agent's output type, so the
graph shows model, tools, model. The project's own `temporalio-openai-agents` must be installed
to import it at all. An offering bound to an agent's handler is not drawn yet
(kontra-sdk#56).

What the runs could not show goes to stderr, and the exit status stays 0 (it is advice):

- activities the worker registers that no run of any offering scheduled, once for the worker
  (`never scheduled in 8 mock runs: ship — branches on timers, signals or inputs the runs did
  not reach`): a branch only a signal, a particular input or more runs reach. Activities a
  batch child, a child's child or a local activity ran count as scheduled, and so do another
  offering's; with `--offering`, the offerings left out ran nothing.
- runs that waited for what no timer brings (a signal, a `wait_condition` without a timeout):
  the test server skips to the run's timeout and times it out; the graph holds what it did
  until then. A run still open after `--timeout` seconds of wall-clock time (default 5), or
  whose workflow task fails on the mock values every time, is terminated, with the same
  note. The stub answers an activity the same way every time within a run, so a loop that
  polls one until it says done (`while not (await status()).done`) ends only in the runs
  that drew `done`; a loop over a large drawn count (1000 in run 1) can take that long too.
- a return type no valid value could be drawn for (a pattern with a lookaround, a model
  validator nothing satisfies): the stub returned null, and the workflow may have failed on
  it.

The test server is a binary temporalio downloads once on first use into
`~/.cache/kontra/temporal-test-server` (under `$XDG_CACHE_HOME` when that is set), a
directory made 0700; never the shared temp directory, where whoever creates the file name
first chooses what runs. Anything in that directory someone else owns or can write is
refused. Offline, before the download, `kontra graph` says it cannot start it. The server
cannot be bound to the loopback: while the command runs it listens on every interface, on a
random port, unauthenticated, so anyone who can reach the machine can read the runs there --
an `--input` you give included -- and start workflows the mock worker runs. Ctrl-C, SIGTERM
and SIGHUP stop the command and the server with it. With a profile in
`$KONTRA_HOME`, each offering's graph is also saved under `$KONTRA_HOME/graphs/`, for
`kontra dev`'s graph page to paint a live run on (`--no-save` skips it); it is never sent
anywhere: `kontra publish` does not carry it.

## Running locally: `kontra dev`

```sh
kontra dev                    # = kontra dev up
kontra dev up --ui            # + Temporal's UI, reading your payloads, and the graph page (below)
kontra dev up --observability # + Prometheus, Grafana, and Loki and Tempo for `kontra serve --otlp` (below)
kontra dev status             # containers, the gate's health, the codec server, the profile
kontra dev logs [gate|temporal|postgres|postgres-relay|s3|migrate|ui|prometheus|prometheus-agent|grafana|otelcol|loki|tempo] [-f] [--tail N]
kontra dev down [--volumes]   # --volumes also deletes the stack's data
```

`kontra dev` runs the same `compose.yaml` that CI and production run: Kontra's Temporal server
(with the Authorizer), the gate, Postgres and an S3 bucket. It writes random credentials to
`$KONTRA_HOME/dev.env` on first use, waits for the gate, logs you in as a dev party with play
money, and waits until Temporal accepts your worker token. It refuses to replace a real login
in the same `KONTRA_HOME`; use another home (`KONTRA_HOME=~/.kontra-dev kontra dev`) or run
`kontra dev up --replace-profile`.

Everything is published on 127.0.0.1 only: the gate on 8080, Temporal on 7233, the bucket on
8333, Postgres on 5432. When a port is taken, set `KONTRA_DEV_GATE_PORT`,
`KONTRA_DEV_TEMPORAL_PORT`, `KONTRA_DEV_S3_PORT` or `KONTRA_DEV_PG_PORT` (the header of
[`compose.yaml`](../compose.yaml) lists every variable). `kontra dev up` remembers these four
ports in `$KONTRA_HOME/dev.env`, so `kontra dev status`, `logs`, `down` and the next `up` use
them without being told again; set one in the environment to change it, or set it empty for
one `up` (`KONTRA_DEV_GATE_PORT= kontra dev up`) to go back to the default. The bind addresses
(`KONTRA_DEV_BIND`, `KONTRA_DEV_PG_BIND`) are never remembered: a stack published beyond
loopback (dev login, play money) is one you ask for on that `up`.
The images are
`ghcr.io/medmahmoudi26/kontra-temporal:dev` and `ghcr.io/medmahmoudi26/kontra-gate:dev`;
`KONTRA_TEMPORAL_IMAGE` and `KONTRA_GATE_IMAGE` name others, and `--env-file` adds variables
from a file. The published Temporal image is `linux/amd64` only; on an arm64 machine, build a
native one in a checkout (`make temporal`, then `KONTRA_TEMPORAL_IMAGE=kontra-temporal:dev`).
Temporal's internal frontend, which has no authentication, is never published, and Temporal
shares a network only with the gate and `postgres-relay`, which carries its database
connections (see [architecture](architecture.md#one-environment-definition)).

On a dev profile, `kontra serve` publishes an unpublished version itself, so
`kontra dev && kontra serve` is a hireable offering.

### The UI and the dashboards

`--ui` and `--observability` are the two compose profiles of the same `compose.yaml` (`ui`,
`observability`); production never enables them. `kontra dev up` runs exactly the profiles
its flags name: alone it is the stack above and nothing more, and it stops what an earlier
`--ui` or `--observability` started. `kontra dev status`, `logs` and `down` cover every
profile. Each tool is published on 127.0.0.1 only, whatever `KONTRA_DEV_BIND` says:

| Flag | Service | Address | Port setting |
|---|---|---|---|
| `--ui` | Temporal's UI (`temporalio/ui`) | http://localhost:8233 | `KONTRA_DEV_UI_PORT` |
| `--ui` | the codec server, a process of `kontra dev` on the host, and the [graph page](#the-graph-page) it serves | http://127.0.0.1:8081 | `KONTRA_DEV_CODEC_PORT` |
| `--observability` | Prometheus | http://localhost:9090 | `KONTRA_DEV_PROMETHEUS_PORT` |
| `--observability` | Grafana: Temporal's dashboards; Prometheus, Loki and Tempo to explore; no sign-in | http://localhost:3000 | `KONTRA_DEV_GRAFANA_PORT` |
| `--observability` | an OpenTelemetry Collector, OTLP over gRPC, for `kontra serve --otlp` | http://127.0.0.1:4317 | `KONTRA_DEV_OTLP_PORT` |

The five ports are remembered in `$KONTRA_HOME/dev.env` like the stack's. Loki and Tempo
publish nothing: the collector writes to them and Grafana reads them.

`--ui` is Temporal's own UI, not a Kontra one; after the login, `kontra dev up --ui` prints
the link to your namespace's workflows. Payloads in Kontra's Temporal are age ciphertext, so
`kontra dev up --ui` also starts a codec server in the background: Temporal's codec-server
protocol (`POST /decode` and `/encode`) answered with this machine's key and the job keys
under `$KONTRA_HOME`. The UI is configured to call it (`TEMPORAL_CODEC_ENDPOINT`), so you read
your own inputs, results and failures in the UI; a payload encrypted to another party, or one
claim-checked to the bucket, is shown as stored. `kontra dev down` stops the codec server; it
writes to `$KONTRA_HOME/codec.log`. It starts only when `$KONTRA_HOME` holds a dev profile
(the one `kontra dev up` logs in to): with a real login there (`--no-login`, or
`--replace-profile` against a gate that is not a dev gate) the UI shows payloads as
ciphertext, and `up` says why. The UI talks to Temporal's internal frontend, which has no
authentication: it sees and controls every namespace of the local stack, and so does anyone
who reaches its port, as anyone who reaches the codec server's can have your key decrypt what
was encrypted to it. Both answer on 127.0.0.1 alone, and the codec server to no web origin but
the UI's; run them on a machine that is yours. Loopback does not keep out a web page whose
name rebinds to 127.0.0.1: such a page is same-origin with the UI, Prometheus and Grafana,
which (as upstream) check no `Host` and ask no sign-in, so it can use the UI's actions --
start, signal, terminate a workflow in any namespace -- read the metrics, and read your
workers' logs and traces and edit dashboards through Grafana. The codec server checks `Host`
and refuses it, so your payloads stay sealed. `kontra dev up` alone stops
the profiles when you are not using them.

`--observability` runs Prometheus, Prometheus's agent and Grafana. Prometheus scrapes Kontra's
Temporal server (`kontra dev up --observability` turns its metrics listener on,
`KONTRA_TEMPORAL_METRICS_ADDR`), and receives the metrics of every `kontra serve --metrics` on
this machine on ports 9464 to 9473. Grafana opens on Temporal's SDK dashboard, with its server
dashboard beside it (see [Dashboards](#dashboards)). Serve the metrics on loopback, one port
per worker:

```sh
kontra serve --metrics 127.0.0.1:9464   # the next worker on 127.0.0.1:9465, and so on
```

On Linux the agent (`prometheus-agent`) scrapes them: it is the one service in the host's
network namespace, so it reaches the host's loopback, and it writes what it scrapes to
Prometheus. A container on Docker's own networks reaches the host only through Docker's
gateway, and a host firewall that drops incoming traffic (ufw's default policy) drops that
too. On Docker Desktop, Prometheus scrapes the same ports itself at `host.docker.internal`,
Docker Desktop's name for this machine. Either way, the query `up{job="kontra-serve"}` at
http://localhost:9090 says per port whether a worker answered: `instance` `127.0.0.1:PORT`
through the agent, `host.docker.internal:PORT` directly (Prometheus's targets page lists only
the direct ones).

`--observability` also runs an OpenTelemetry Collector (`otelcol`), Loki and Tempo, for what
`kontra serve --otlp` exports ([Scaling and observability](#scaling-and-observability)): the
collector takes OTLP over gRPC on 127.0.0.1:4317 and writes the spans to Tempo and the logs to
Loki, and Grafana has both as data sources (uids `loki` and `tempo`) beside Prometheus.
`kontra dev up --observability` prints the URL to give `--otlp`:

```sh
kontra dev up --observability   # ...  otlp      http://127.0.0.1:4317  (kontra serve --otlp)
kontra serve --otlp http://127.0.0.1:4317
```

In Grafana's Explore, Loki finds a worker's lines by the Temporal context each was logged in,
for example one activity of one run:

```text
{service_name="kontra-worker"} | workflow_id="job-..." | run_id="..." | activity_type="fetch"
```

A line's `trace_id` links to its trace in Tempo, and a span to its trace's lines in Loki.
`kontra-worker` is the service name when `OTEL_SERVICE_NAME` sets none. Loki keeps 7 days of
logs and Tempo 14 days of traces, on volumes of their own (`kontra dev down --volumes`
deletes them with the rest). From Loki 3.7, Loki refuses every line while the disk its data
is on is fuller than a threshold; it is 98% here, not Loki's 90%, at which many a laptop's disk
already is. Grafana's anonymous user is an Editor here, because Explore needs one in Grafana
13: an Editor also changes dashboards (the vendored ones are read-only), but adds no data
source or plugin, and alerting and external snapshots are off, so it cannot make Grafana send
anything out.

### The graph page

`kontra dev up --ui` also prints the graph page, which the codec server serves:

```text
  workflows http://localhost:8233/namespaces/acme/workflows
  graph     http://127.0.0.1:8081/graph
```

It lists your namespace's latest 50 runs (the batches' children left out), and opens one as
its graph: the steps of the workflow as [`kontra graph`](#the-graph-kontra-graph) draws them,
with what this run did painted on. Each step shows its status (scheduled, running,
completed, failed, timed out, canceled) and duration, `×3` when it is on its third attempt,
`∥50` when it ran 50 at once; the activities one workflow task started together are boxed as
one parallel group, an `execute_activities_in_parallel` batch is one double-bordered node, a
timer is rounded, and a step this run did not take (or has not reached yet) is dashed and
muted. While the run is open the page repaints by itself: the codec server asks Temporal
about once a second whether the run changed, its running activities and their attempts
included, so a retry shows while it runs rather than once history records it (a step
waiting for its next attempt says how long it has been backing off since the last one failed,
and its pane shows that failure's message), and the page stops once the run has closed. Click a step (or focus it and press Enter) for its attempts,
timing, task queue and failure, its input, result and failure message decoded with this
machine's key, and its links:

- **Temporal UI**: the step's own event in the run's history (a child workflow's own run);
- **Logs**: Grafana's Explore on Loki, the step's lines over its time range (a second either
  side), `{service_name="kontra-worker"} | workflow_id="job-..." | run_id="..." |
  activity_type="..."`;
- **Trace**: Explore on Tempo, the job's trace.

The header has the same three for the whole run. Logs and Trace need `--observability` and a
worker serving with `kontra serve --otlp http://127.0.0.1:4317`, which also writes the trace
context the Trace link reads into the run's history. The Logs link selects `kontra-worker`,
or `OTEL_SERVICE_NAME` as `kontra dev up` saw it: give the worker the same one.

The whole workflow, branches this run never takes included, comes from `kontra graph`: run
`kontra graph --dir <project>` with the dev profile's `KONTRA_HOME` once the workflow changes,
and the page paints each run on the graph saved for its task queue (the one `kontra serve`
serves, exactly). Without one, it draws what the run did and says how to get the rest.

The run's graph carries names, statuses and times, never an input, result, failure message
or heartbeat (it is read from history by `kontra.graph`, which emits no payload). A step's
payloads come from one route of the codec server, decoded there, for the page's own requests
only (a header a page on another origin cannot send without a preflight, which is refused):
what is encrypted to keys this machine does not hold is shown as such, and a payload
claim-checked to the bucket as stored there, never their bytes. The page runs
nothing but its own script and loads nothing from elsewhere, and its data routes answer its
own origin alone. The codec server reads Temporal as you, with your dev login's worker token,
from the first time the page asks (so it starts, and decodes for the UI, without Temporal);
like the decoding, it runs for a dev profile only.

## Publishing and serving

```sh
kontra login --email you@example.com   # sign-in link by mail (`kontra dev`: kontra login --dev)
kontra publish                         # from the offering directory, or --dir
kontra serve                           # on the machine that does the work
```

`kontra login` talks to `https://api.kontra.run` unless `--gate` or `KONTRA_GATE` says
otherwise; with `--email` the gate mails you a link whose page asks for the code the command
printed (sign-ups are by invitation; see [buyers.md](buyers.md#setup)).

`kontra publish`:

1. loads `worker.py` and each bound offering's `offering.yaml`;
2. derives the contract document and seals it with its hash (the party is sealed; no key goes
   in, decisions I7); for one handler of a harness agent, it then compares the contract with
   the harness's schema for that handler and warns where they differ ([Agents](#agents));
3. sends it to the gate, which requires your login, **confirmed within the hour** (a worker
   token never qualifies), validates it, assigns the version an opaque Nexus endpoint name and creates the
   endpoint itself, pointed at your namespace and the version's task queue;
4. prints the contract hash, the endpoint and the task queue.

The gate refuses a document with an external `$ref`, an object without
`additionalProperties`, or a reserved `Kontra*` type that differs from the spec, naming the
path at fault. Signing in counts as a confirmation for an hour; after that, `kontra publish`
asks you to confirm it's you in place: it prints a code, the gate mails the address you sign
in with a link, and once you type the code on the link's page the command publishes. Nothing
is published before. Without a terminal (CI) it fails instead and says to run `kontra
confirm` first; a `kontra dev` login confirms at once (decisions I23). An API key never
publishes, however new (403 `fresh_login_required`): publishing is a person's act.

`kontra serve` connects to Temporal with a 15-minute worker token, refreshed in the
background. That token can do one thing: run workers in your namespace. It cannot publish,
create or change an endpoint, or reach another namespace (see [security.md](security.md)).
Ctrl-C stops the worker gracefully.

`kontra status --workers` says whether your workers keep up with each version `worker.py`
binds (or the versions you name, `kontra status acme/enrich@1.2.0`): per task-queue type
(workflow, activity, Nexus), the backlog and the age of its oldest task, the rates tasks are
added and dispatched at, and the workers polling, as Temporal's `DescribeTaskQueue` reports
them, with a one-line verdict such as `activity backlog growing for 5 min, 2 pollers -- scale
up` or `idle, 1 poller`. A backlog dispatched from only as fast as tasks arrive is `not
shrinking -- scale up`: those workers keep pace and never catch up. Each worker is listed with
the build id it polls with and the SDK its heartbeat names (`temporal-python 1.34.0`; a worker
heartbeats once a minute, so a new one shows its SDK within a minute). It needs nothing on
your side: the gate reads these numbers from your namespace (task-queue stats are metadata,
see [security.md](security.md)) and shows only your own versions. Temporal reports zeros and
no pollers for a task queue it unloaded after about five idle minutes, and the command says
so when that is all there is. The console's Workers page and the `workers` tool of `kontra
mcp` show the same answer.

A server that runs `kontra serve` unattended should use an API key rather than your login:
`kontra keys create --label worker-1` on your machine, then `KONTRA_API_KEY` on the server.
The worker serves exactly as with a login. On a `kontra dev` profile a worker publishes
unpublished versions itself; with a key it cannot, so publish from your login first (see
[buyers.md](buyers.md#programs-api-keys)).

### Keys for a fleet

Buyers encrypt an input to **every** recipient your party has registered, so any worker
holding any of those keys can decrypt it. That is what makes a fleet work:

```sh
# Once, on your machine: make the fleet's key and register it.
age-keygen -o fleet-prod.key                       # or: kontra recipients add   (this machine)
kontra recipients add --label fleet-prod --public-key age1...
kontra recipients list
```

Mount that one key into every worker and point `KONTRA_KEY_FILE` at it:

```yaml
env:
  - name: KONTRA_KEY_FILE
    value: /etc/kontra/key
volumeMounts:
  - { name: kontra-key, mountPath: /etc/kontra, readOnly: true }
volumes:
  - { name: kontra-key, secret: { secretName: kontra-fleet-prod, defaultMode: 0400 } }
```

**One key per fleet, never one per pod.** Pod churn then never changes the recipient set, so
an input encrypted a minute ago is still readable by whichever worker picks the task up. It
is the same operational shape as a TLS certificate. `kontra serve` refuses to start if the
key it holds is not one of your registered recipients — otherwise it would poll happily and
fail to read every input it was given.

Adding never replaces, so a second machine or a second fleet is one more recipient; there is
no key to copy around and no `key_mismatch` to resolve. `kontra recipients revoke age1...`
stops future deliveries reaching a lost host. It cannot reach backwards: what was already
encrypted to that key stays encrypted to it (see [security.md](security.md#known-limits)).
Keep the set small — at most 16 — since every recipient adds a stanza to every payload.

## Scaling and observability

Two flags of `kontra serve` turn on what Temporal's Python SDK already measures. Both are off
unless given, and neither adds a metric name or an exporter of Kontra's own.

```sh
kontra serve --metrics 0.0.0.0:9464          # Prometheus text at http://<this host>:9464/metrics
kontra serve --otlp http://collector:4317    # traces and logs over OTLP/gRPC
```

`--metrics IP:PORT` serves the SDK's own metrics: the worker's client connects with a
`temporalio.runtime.Runtime` whose telemetry is `PrometheusConfig(bind_address=IP:PORT)`. The
names are Temporal's ([SDK metrics](https://docs.temporal.io/references/sdk-metrics)), labelled
with your namespace, the version's task queue and the workflow and activity types; counters
have no `_total` suffix and durations are in milliseconds, which is what Temporal's dashboard
reads. What answers the usual questions:

| Question | Metric |
|---|---|
| do I need more workers? | `temporal_activity_schedule_to_start_latency` and `temporal_workflow_task_schedule_to_start_latency` (how long a task waited for a worker) climbing, with `temporal_worker_task_slots_available` at 0 |
| is a slot limit the bottleneck? | `temporal_worker_task_slots_used` against `max_concurrent_activities` |
| are activities failing? | `temporal_activity_execution_failed`, by `activity_type` |
| how long does a job take? | `temporal_workflow_endtoend_latency`, `temporal_activity_execution_latency` |

The endpoint has no authentication: bind it where your Prometheus reaches it and nothing
else does, and scrape each worker like any other target:

```yaml
scrape_configs:
  - job_name: kontra-serve
    static_configs:
      - targets: ["worker-1:9464", "worker-2:9464"]
```

`--otlp URL` puts Temporal's `TracingInterceptor` (`temporalio.contrib.opentelemetry`) on the
worker's client and exports its spans over OTLP/gRPC to the collector at `URL` (`http://` in
plaintext, `https://` over TLS): the Nexus `quote` and `execute` handlers, `StartWorkflow`,
`RunWorkflow`, `StartActivity`, `RunActivity`. Every span the worker starts carries
`temporalTaskQueue`, its task queue. A workflow's spans and `RunActivity` also carry
`temporalWorkflowID` (`job-<id>` for a job's workflow) and `temporalRunID`, `RunActivity` its
`temporalActivityID` too; `StartWorkflow` carries the workflow ID alone, and a Nexus handler's
span the task queue alone. `OTEL_SERVICE_NAME` names the service (`kontra-worker` when it
names none). It needs the `opentelemetry` extra, `pip install 'kontra-sdk[opentelemetry]'`;
without it `kontra serve --otlp` says so and does not start. The interceptor's tracer provider
is its own: spans you make with OpenTelemetry's API go wherever your own setup sends them.

`--otlp` also exports the worker's Python logs to the same collector, at INFO and above, from
a handler on the root logger and a log provider of its own (never OpenTelemetry's global one),
with the spans' service name. Each record carries the Temporal context it was logged in as
attributes -- `workflow_id`, `run_id`, `workflow_type`, `attempt` and `task_queue`, and in an
activity `activity_id` and `activity_type` -- whether it came through `workflow.logger`,
`activity.logger` or a logger of your own inside an activity, and, inside a span, that span's
trace and span IDs. An exception keeps its type, message and stack trace
(`exception.type`, `exception.message`, `exception.stacktrace`).

Neither the metrics nor the spans carry what a buyer sent or received. Metrics are counts and
latencies labelled with names. Spans carry names and IDs. A buyer's Nexus call may carry a trace context of its own
(`traceparent`, `tracestate`, `baggage`): the worker removes it before Temporal's interceptor
reads it, so a handler's span starts a trace of yours, recorded whatever the buyer's sampling
flag says, and nothing of the buyer's reaches a span or the history of the workflow it
starts. The one thing Temporal's interceptor takes from a job, an exception's message (a
failure message, encrypted everywhere else), is removed before export, so a failed span keeps
its status and its exception's type only. The codec's leak test runs jobs whose input,
result, failure, heartbeats and trace context carry a marker through a worker with both flags
on, and finds it in neither the metrics text nor the exported spans.

Logs are different: they are your own output, going to your own collector, and they leave as
your code wrote them -- not scrubbed and not checked for a job's data, exception messages
included. What your workflow or activity logs of a job's input, result or failure is in your
logs, as it is on the worker's console. Log what you would keep; the buyer's data is yours to
handle under the same terms as everywhere else in your code. Kontra's own serving code logs no
payload as such, but what it logs about a refused input (the paths of its invalid fields, where
a map key is the buyer's) or a call that failed (the exception, as raised) can carry pieces of
one, and those lines are exported like yours.

### Dashboards

Temporal publishes Grafana dashboards for its SDK and server metrics. Two are vendored here,
unchanged, in [`docs/dashboards`](dashboards) (MIT, Temporal Technologies):
`temporal-core-sdks-otel.json` reads `kontra serve --metrics` (Python's SDK is built on
Temporal's Core), `server-general.json` a Temporal server's. Import the SDK dashboard
into your Grafana with the Prometheus that scrapes your workers as its data source, or run both
locally with `kontra dev up --observability` (see [Running locally](#the-ui-and-the-dashboards)).

### Scaling

One `kontra serve` is one worker. The metrics say a queue is waiting for workers; the backlog
of the version's task queue (the one `kontra publish` printed) says how far behind it is, and
is what an autoscaler reads. Your worker token may call `DescribeTaskQueue` in your own
namespace, so Temporal's own CLI works unchanged:

```sh
# A worker token (15 minutes), the namespace and the frontend address, from the gate.
read -r token namespace address <<<"$(
  curl -fsS https://api.kontra.run/v1/auth/temporal-token \
       -H "Authorization: Bearer $KONTRA_API_KEY" \
       -H 'Content-Type: application/json' -d '{"role":"worker"}' |
  python -c 'import json,sys; d=json.load(sys.stdin); print(d["token"], d["namespace"], d["temporal_address"])'
)"

temporal task-queue describe \
  --address "$address" --namespace "$namespace" --api-key "$token" \
  --task-queue 'acme/enrich@1.2.0' --report-stats
```

`--report-stats` is the part that matters: `ApproximateBacklogCount` and
`ApproximateBacklogAge` say how far behind you are, and `TasksAddRate` against
`TasksDispatchRate` says whether the gap is growing or closing. Read them per task-queue
type — a workflow backlog and an activity backlog scale differently. The numbers are
Temporal's own and are approximate by design; an idle queue is unloaded after about five
minutes, and then reports zero rather than "no data".

Nothing else about your queue is readable with that token: it cannot describe a queue in
any other namespace, and the stats carry no payload (see [security.md](security.md)).

### KEDA

Because the RPC is the plain one, [KEDA](https://keda.sh)'s Temporal scaler works against
Kontra's frontend with no shim. Put a worker token in a secret and point the scaler at the
version's task queue:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kontra-worker-token
type: Opaque
stringData:
  apiKey: <the token field of POST /v1/auth/temporal-token>
---
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: keda-trigger-auth-kontra
spec:
  secretTargetRef:
    - parameter: apiKey          # `apiKey` is an auth parameter, never trigger metadata
      name: kontra-worker-token
      key: apiKey
---
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: enrich-workers
spec:
  scaleTargetRef:
    name: enrich-worker          # the Deployment running `kontra serve`
  minReplicaCount: 1
  maxReplicaCount: 20
  triggers:
    - type: temporal
      metadata:
        endpoint: temporal.kontra.run:7233
        namespace: acme
        taskQueue: acme/enrich@1.2.0
        queueTypes: activity     # the scaler's default is workflow only
        targetQueueSize: "5"     # scale up while the backlog is above this
      authenticationRef:
        name: keda-trigger-auth-kontra
```

A worker token lives 15 minutes, so whatever refreshes that secret is yours to run — the
same loop `kontra serve` runs internally, as a small CronJob calling the route above. Pick
`queueTypes` to match where your work is: the activity queue if it is in activities, the
workflow queue if it is in workflow tasks. Scaling on both against one `targetQueueSize`
oscillates, because the two backlogs drain at different rates.

## Versions

A published version is fixed. At start, `kontra serve` recomputes each offering's contract hash
from your code and refuses to start if it differs from the catalog's: change a type or
`offering.yaml`, then bump `version` and publish again. The hold token a buyer pays under
carries the hash too, and the handler refuses a token for any other hash, so a buyer who paid
for contract X is never served by code running contract Y.

A new version is a new document, endpoint and task queue. Jobs hired on the old version
finish on a worker that still serves it.

kontra-sdk declares the `temporalio` range it runs on, `temporalio>=1.34,<1.38` (its suite
runs on the minor its lock holds: 1.34, today the only one released in the range), and your
project gets `temporalio` through it: you import it directly, so compatibility is a
dependency range, not an import path. Outside that range -- read as pip reads it, so
`1.34.0rc1`, which comes before `1.34`, is outside -- a Kontra worker refuses to start
(Kontra's codec relies on experimental temporalio APIs a minor release may change), and
`kontra publish` and `kontra serve` say so first, before they import `worker.py` (`publish`
on stderr, `serve` in its log):

```text
warning: temporalio 1.38.0 is outside the range kontra-sdk is tested on (temporalio>=1.34,<1.38); install one in it, e.g. `pip install 'temporalio>=1.34,<1.38'`
```

If your own dependencies pull in another `temporalio`, pin it into the range.

## Getting paid

| Event | Money |
|---|---|
| Delivered, buyer accepts | released to you |
| Delivered, buyer silent through `review_window` | released to you |
| Delivered, buyer rejects | disputed; Kontra resolves it on the hashes and whatever each side decrypts for the resolver |
| Your workflow fails | refunded to the buyer |
| No delivery by the deadline (you were offline, or too slow) | refunded to the buyer |
| Buyer cancels before delivery | refunded to the buyer; your workflow is cancelled |

Kontra takes its platform fee at release. The facts behind each row are observed by the
gate from Temporal and the clock, never reported by either party. `kontra jobs --as seller`
lists the jobs you sell; `kontra balance` shows what you hold. Under `kontra dev` it is play
money.

Orchestration in your namespace (actions, history) is your cost of goods; price for it.

## Schema-first and other languages

Write the Nexus document yourself and bind it:

```python
offering("offering.yaml", entry=EnrichFlow, quote=quote, contract="contract.yaml")
```

`kontra publish` validates it, merges `offering.yaml` into `x-kontra` and otherwise sends it
unchanged. The two files keep different owners (contract vs offering facts) and are never
merged by hand.

The examples: [`examples/hello`](../examples/hello) (the smallest offering, one field in and
one out, as a five-minute tutorial), [`examples/enrich`](../examples/enrich) (types-first, with
an artifact), [`examples/legacy-actor`](../examples/legacy-actor) (an older job turned into
an offering), [`examples/sweep`](../examples/sweep) (a fan-out, one activity per URL with at
most 50 open, and a branch, and the graph `kontra graph` draws of them), and two agents:
[`examples/chef-agents`](../examples/chef-agents) (as workflow code) and
[`examples/chef-claude`](../examples/chef-claude) (as a process). Each keeps its derived
`contract.yaml` as a golden file, reproduced byte for byte by the tests, and its README its
graph, which the tests compare with `kontra graph --mermaid`.
