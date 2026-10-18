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
- [Many activities: `execute_activities_in_parallel`](#many-activities-execute_activities_in_parallel)
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
  workflows.py            Temporal, through `from kontra import workflow`
  activities.py           Temporal, through `from kontra import activity`
  quote.py                the price, from the input
  worker.py               serve(Flow): binds workflow and quote to the offering; all worker tuning
  tests/test_offering.py  the lifecycle against kontra.testing, no containers
  input.json              a sample input, for `kontra quote` and `kontra hire`
  pyproject.toml          one dependency, kontra-sdk; pytest settings
  README.md
```

The offering is `<party>/wordcount`, where the party is your profile's handle (or
`--party`).

Each fact has one owner. The rule that matters most: **`workflows.py` and `activities.py`
reach Temporal only through `kontra`** -- `from kontra import workflow, activity, RetryPolicy,
ApplicationError` -- **and never import `temporalio`** (D1). The façade is Temporal's own
objects re-exported, not wrappers: `kontra.workflow.defn` *is* `temporalio.workflow.defn`, so
the code is ordinary Temporal code, tested and run like any other, and with
`from temporalio import workflow` in place of `from kontra import workflow` it runs on any
Temporal. Your project depends on `kontra-sdk` alone, which pins the Temporal SDK it was
built against. The only other Kontra type your job code may touch is `kontra.types.Artifact`,
a pydantic model of a file reference, and it reaches it through `models.py`. `kontra publish`
refuses an offering whose code imports `temporalio` or `nexusrpc`.

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

Temporal, through the façade: activities, retries, timers, child workflows, signals, updates.
`kontra.workflow` re-exports Temporal's workflow API (`defn`, `run`, `signal`, `query`,
`update`, `execute_activity`, `execute_child_workflow`, `sleep`, `wait_condition`, `now`,
`uuid4`, `unsafe`, ...) and adds [`execute_activities_in_parallel`](#many-activities-execute_activities_in_parallel);
`kontra.activity` is Temporal's activity API (`defn`, `heartbeat`, `info`, `logger`, ...);
`kontra` itself has `RetryPolicy` and `ApplicationError` (raise it with `non_retryable=True`
to stop retries, as with Temporal).

```python
# workflows.py
from datetime import timedelta

from kontra import workflow

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

## Files: `Artifact`

Temporal payloads are capped at a few MB. Anything larger is an `Artifact`: a reference to a
file in Kontra's bucket, encrypted with age to the job's parties.

```python
# activities.py -- Artifact comes from models.py
from kontra import activity
from models import Artifact, Record


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

## Many activities: `execute_activities_in_parallel`

One activity per item, thousands or millions of items, with per-item retries and timeouts,
and never a `continue_as_new` in your code:

```python
# workflows.py
from datetime import timedelta

from kontra import Page, activity, workflow


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
        return await workflow.execute_activities_in_parallel(
            probe,
            pages=next_page,
            reduce=count_hits,
            init=0,
            max_concurrent=200,
            start_to_close_timeout=timedelta(seconds=30),
        )
```

- **`pages=`** is the source: an activity `(cursor) -> Page(items, next)`, called first with
  `None` and then with each page's `next` until one is `None`. The cursor is anything
  serializable: an offset, a continuation token. Pages are fetched by activity, so no item
  list ever enters your workflow's history.
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

It runs as a child workflow, `kontra.batch.v1`, on your worker (`serve()` registers it). That
child continues as new when Temporal suggests it, after letting what is in flight finish, and
carries only the cursor, the accumulator and the current page's unstarted items into the next
run: history is bounded by `max_concurrent`, never by the number of items, and your workflow
just awaits one call. That is why your code never needs `continue_as_new` (the façade does not
offer it): the iteration that would have needed it is this call. An item that fails after its
retries fails the batch -- what is in flight is cancelled first -- and the call raises
Temporal's `ChildWorkflowError`, with the activity's own error below it.

One workflow runs a few hundred activities a second. Past that, partition the source and run
one batch per partition as child workflows -- Temporal's own composition, in your code:

```python
import asyncio

from kontra import workflow

with workflow.unsafe.imports_passed_through():
    from scanpart import ScanPart  # a workflow running execute_activities_in_parallel over part p


@workflow.defn
class Scan:
    @workflow.run
    async def run(self, parts: int) -> int:
        runs = [workflow.execute_child_workflow(ScanPart.run, p) for p in range(parts)]
        return sum(await asyncio.gather(*runs))
```

## Throughput: one heartbeating activity

When per-item durability is noise -- "probe 500,000 hosts", where a lost probe is simply
probed again -- the fastest shape is not one activity per item but one activity doing its own
concurrency and heartbeating its cursor:

```python
# activities.py
import asyncio

from kontra import activity


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
here needs Kontra beyond `activity.heartbeat` and `activity.info`. A real one reads its hosts
from a page source rather than its input, which, like any input, must fit a payload.

## Agents

A workflow can run an agent, and nothing switches that on: the workflow does what it wants,
and no `serve()` or `Worker` keyword changes what workflow code may do (`docs/decisions.md`
I25). There are two shapes, with an example each.

**An agent as workflow code**, [`examples/chef-agents`](../examples/chef-agents). OpenAI's
[Agents SDK](https://openai.github.io/openai-agents-python/) runs its loop in the workflow,
durably, through Temporal's integration (the extra: `pip install 'kontra-sdk[agents]'`): every
model call and every tool call is an activity, retried and recorded like any other.

```python
# workflows.py
from datetime import timedelta

from kontra import workflow

with workflow.unsafe.imports_passed_through():
    from activities import lookup_pantry
    from agents import Agent, Runner  # OpenAI's Agents SDK: your dependency, allowed
    from models import CookInput, Dish


@workflow.defn
class ChefFlow:
    @workflow.run
    async def run(self, inp: CookInput) -> Dish:
        chef = Agent(
            name="chef",
            instructions="Look in the pantry, then propose one dish from what is there.",
            tools=[
                workflow.activity_as_tool(
                    lookup_pantry, start_to_close_timeout=timedelta(seconds=30)
                )
            ],
        )
        result = await Runner.run(chef, inp.craving)
        return Dish(recipe=str(result.final_output))
```

```python
# worker.py
import os

from agents import set_default_openai_api, set_default_openai_client, set_tracing_disabled
from openai import AsyncOpenAI
from workflows import ChefFlow

from kontra import serve

set_default_openai_client(
    AsyncOpenAI(base_url=os.environ["LLM_BASE_URL"], api_key="unused", max_retries=0),
    use_for_tracing=False,
)
set_default_openai_api("chat_completions")
set_tracing_disabled(True)

serve(ChefFlow)
```

| Runs as | What |
|---|---|
| workflow code | the agent loop, handoffs, and `@function_tool` tools: pure computation only (no I/O, no clock, no randomness) |
| an activity | every model call, every `workflow.activity_as_tool(...)` tool, every MCP operation |

- **The import is the opt-in.** A workflow that imports `agents` runs an agent: the worker
  sees the import and puts Temporal's `OpenAIAgentsPlugin` on itself, with the payload
  converter its activities decode with, which validates more leniently inside your workflows
  and activities (a buyer's input is still checked against your contract before the job
  starts). A workflow that does not import `agents` gets neither, whatever is installed.
  `agents` imported without the extra is a `TypeError` that names it. A `Worker(...)` written
  out does the same as `serve()`: `kontra publish` lets your code import no `temporalio`, so
  you could not pass the plugin yourself.
- **The import is read when `worker.py` imports your workflows**, so make it at the top of
  `workflows.py`, as above, or of a module it passes through. An `import agents` inside `run`
  comes too late: the worker was built without the plugin, and the workflow task fails. A
  module the workflow passes through counts, `activities.py` included: an activity that runs
  an agent in-process, outside Temporal's loop, imports `agents` inside its function, or the
  worker gets the plugin and its converter for it. Only OpenAI's `agents` counts: a module of
  your own named `agents.py` runs no agent (and hides the SDK; rename it).
- **The model endpoint is worker config**, never workflow code: the agent names no model and
  no server. `worker.py` sets the Agents SDK's own default client and API before `serve()`,
  and the plugin's model activity dials them, so any OpenAI-compatible server answers --
  Ollama, vLLM, OpenRouter, Mistral. The model is the SDK's `OPENAI_DEFAULT_MODEL`, in the
  worker's environment. The API is read when the worker is built, the client at the first
  model call, so the worker starts without an `OPENAI_API_KEY`; with no default client set,
  it is the SDK's own, from `OPENAI_API_KEY` (and `OPENAI_BASE_URL`).
- `serve(ChefFlow, mcp_servers={"name": factory})` serves `workflow.mcp_server("name")`. It is
  worker config, like `max_concurrent_activities`: a factory runs on the worker, so it holds
  the processes and credentials an MCP server needs, and a name no workflow asks for is unused.
- An activity tool gets a **copy** of the run context: what it changes does not come back.
- `workflow.mcp_server(name, factory_argument=...)`: the argument is an activity input, so it
  is recorded in history. Never put a secret there; resolve credentials in the factory.
- A tool that wants scale calls `execute_activities_in_parallel` itself (make it a workflow
  the agent's activity starts, or a heartbeating activity), rather than having the model issue
  a thousand tool calls.
- Set `uses_hosted_model: true` in `offering.yaml`: the buyer's input, decrypted on your
  worker, goes to the model provider in plaintext to it. Model calls are activity payloads,
  encrypted in Temporal like the rest of the job.

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
`Flow`, and registers every activity decorated with `kontra.activity.defn` that the process
imported -- an activity nothing imports cannot be referenced by a workflow either, so there is
no list to keep. Further workflows (`serve(Flow, Child)`) are registered unbound, for the
children `Flow` starts. `quote=` passes another quote function; methods decorated as
activities need their instance (`activities=[obj.method]`).

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
    offerings=[offering("offering.yaml", workflow=EnrichFlow, quote=quote)],
    activities=[fetch_batch, write_records],
    max_concurrent_activities=40,
)

if __name__ == "__main__":
    worker.run()
```

`kontra.serve.Worker` is `temporalio.worker.Worker` plus exactly five things: a generated
Nexus service per bound offering (`quote` as a synchronous operation, `execute` as a
workflow-run operation), the hold-token check before `execute` starts anything, the
encryption codec, the task queue, derived from `name@version` (passing `task_queue` is a
`TypeError`), and the `kontra.batch.v1` workflow behind `execute_activities_in_parallel`.
It passes `kontra` through the workflow sandbox, so the façade loads once per process.
`workflows`, `nexus_service_handlers` and `interceptors` are merged with Kontra's; every other
keyword reaches the inner worker unchanged. Written out, `worker.py` exposes a module-level
`worker`: `kontra serve` runs it and `kontra publish` imports it to find the offerings.

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
test server, with what `serve()` registers -- the bound workflow, the further ones it
registers (the children it starts) and the batch child, under Kontra's sandbox and payload
converter -- but none of the worker's other options, and no codec. Every activity is
answered by a stub with a value of the type it declares, so no activity body runs, nothing
is fetched and no key is needed; timers are skipped. The input is drawn the same way from the
workflow's input type, unless `--input` gives one (checked against the type first). Run 0
draws the smallest values (false, 0, empty lists, no optional fields), run 1 the largest
(true, 3 items, every field), run 2 large lists near the top of the value (201 items, so a
loop that batches its input by 50 or 100 goes round), and the others random ones from
`--seed`: an `if` on a boolean an activity returned shows both branches from two runs on,
and the same command prints the same graph. The runs' graphs are then united: a loop is one
node with an edge back, the activities one workflow task starts at once share a parallel
group, and `execute_activities_in_parallel` is one node,
`execute_activities_in_parallel(<activity>)`, however many items it ran and however many
times it continued as new. [`examples/sweep`](../examples/sweep) shows the three, and every
example's README shows its graph.

An agent workflow (one that imports `agents`) runs with a scripted model instead of yours:
it calls every tool the agent was given once, then answers with a value of the agent's output
type, so the graph shows model, tools, model. Its `[agents]` extra must be installed to
import the project at all.

What the runs could not show goes to stderr, and the exit status stays 0 (it is advice):

- activities the worker registers that no run scheduled (`never scheduled in 8 mock runs:
  ship — branches on timers, signals or inputs the runs did not reach`): a branch only a
  signal, a particular input or more runs reach. Activities a batch child or a local activity
  ran count as scheduled.
- runs that waited for what no timer brings (a signal, a `wait_condition` without a timeout):
  the test server skips to the run's timeout and times it out; the graph holds what it did
  until then. A run still open after `--timeout` seconds of wall-clock time (default 5: a loop
  that never sleeps), or whose workflow task fails on the mock values every time, is
  terminated, with the same note.
- a return type no valid value could be drawn for (a pattern with a lookaround, a model
  validator nothing satisfies): the stub returned null, and the workflow may have failed on
  it.

The test server is a binary temporalio downloads once into the temp directory on first use;
offline, before that, `kontra graph` says it cannot start it. With a profile in
`$KONTRA_HOME`, each offering's graph is also saved under `$KONTRA_HOME/graphs/`, for
`kontra dev`'s graph page to paint a live run on (`--no-save` skips it); it is never sent
anywhere: `kontra publish` does not carry it.

## Running locally: `kontra dev`

```sh
kontra dev                    # = kontra dev up
kontra dev up --ui            # + Temporal's UI, reading your payloads (below)
kontra dev up --observability # + Prometheus and Grafana with Temporal's dashboards (below)
kontra dev status             # containers, the gate's health, the codec server, the profile
kontra dev logs [gate|temporal|postgres|postgres-relay|s3|migrate|ui|prometheus|prometheus-agent|grafana] [-f] [--tail N]
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
| `--ui` | the codec server, a process of `kontra dev` on the host | http://127.0.0.1:8081 | `KONTRA_DEV_CODEC_PORT` |
| `--observability` | Prometheus | http://localhost:9090 | `KONTRA_DEV_PROMETHEUS_PORT` |
| `--observability` | Grafana, Temporal's dashboards, no sign-in | http://localhost:3000 | `KONTRA_DEV_GRAFANA_PORT` |

The four ports are remembered in `$KONTRA_HOME/dev.env` like the stack's.

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
start, signal, terminate a workflow in any namespace -- and read the metrics. The codec
server checks `Host` and refuses it, so your payloads stay sealed. `kontra dev up` alone stops
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
2. derives the contract document and seals it with your public key and its hash;
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
kontra serve --otlp http://collector:4317    # traces over OTLP/gRPC
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
span the task queue alone. `OTEL_SERVICE_NAME` names the service. It needs the `opentelemetry` extra, `pip install 'kontra-sdk[opentelemetry]'`; without
it `kontra serve --otlp` says so and does not start. The interceptor's tracer provider is its
own: spans you make with OpenTelemetry's API go wherever your own setup sends them.

Neither carries what a buyer sent or received. Metrics are counts and latencies labelled with
names. Spans carry names and IDs. A buyer's Nexus call may carry a trace context of its own
(`traceparent`, `tracestate`, `baggage`): the worker removes it before Temporal's interceptor
reads it, so a handler's span starts a trace of yours, recorded whatever the buyer's sampling
flag says, and nothing of the buyer's reaches a span or the history of the workflow it
starts. The one thing Temporal's interceptor takes from a job, an exception's message (a
failure message, encrypted everywhere else), is removed before export, so a failed span keeps
its status and its exception's type only. The codec's leak test runs jobs whose input,
result, failure, heartbeats and trace context carry a marker through a worker with both flags
on, and finds it in neither the metrics text nor the exported spans.

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
offering("offering.yaml", workflow=EnrichFlow, quote=quote, contract="contract.yaml")
```

`kontra publish` validates it, merges `offering.yaml` into `x-kontra` and otherwise sends it
unchanged. The two files keep different owners (contract vs offering facts) and are never
merged by hand.

The examples: [`examples/hello`](../examples/hello) (the smallest offering, one field in and
one out, as a five-minute tutorial), [`examples/enrich`](../examples/enrich) (types-first, with
an artifact), [`examples/legacy-actor`](../examples/legacy-actor) (an older job turned into
an offering), [`examples/sweep`](../examples/sweep) (a fan-out, a batch of one activity per
URL and a branch, and the graph `kontra graph` draws of them), and two agents:
[`examples/chef-agents`](../examples/chef-agents) (as workflow code) and
[`examples/chef-claude`](../examples/chef-claude) (as a process). Each keeps its derived
`contract.yaml` as a golden file, reproduced byte for byte by the tests, and its README its
graph, which the tests compare with `kontra graph --mermaid`.
