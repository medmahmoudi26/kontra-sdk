# Selling on Kontra

You write a Temporal workflow. Kontra adds a price, a catalog entry, escrow and encryption around
it, and buyers hire it through Nexus. The workflow runs on your machine as a plain Temporal
worker; Kontra never runs your code. The smallest offering, as a five-minute tutorial from
login to a paid job, is [`examples/hello`](../examples/hello).

- [The project](#the-project)
- [offering.yaml](#offeringyaml)
- [Types and the contract](#types-and-the-contract)
- [The quote](#the-quote)
- [The workflow and activities](#the-workflow-and-activities)
- [Files: `Artifact`](#files-artifact)
- [Progress](#progress)
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
  workflows.py            plain Temporal -- no kontra import
  activities.py           plain Temporal -- no kontra import
  quote.py                the price, from the input
  worker.py               binds workflow and quote to the offering; all worker tuning
  tests/test_offering.py  the lifecycle against kontra.testing, no containers
  input.json              a sample input, for `kontra quote` and `kontra hire`
  pyproject.toml          pytest settings only
  README.md
```

The offering is `<party>/wordcount`, where the party is your profile's handle (or
`--party`).

Each fact has one owner. The rule that matters most: **`workflows.py` and `activities.py`
import nothing from `kontra`.** They are ordinary Temporal code, tested and run like any
other. The only Kontra type your job code may touch is `kontra.types.Artifact`, a pydantic
model of a file reference, and it reaches it through `models.py`.

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
```

| Key | Range |
|---|---|
| `max_duration` | 1s to 30d |
| `review_window` | 0 to 30d |
| `artifact_retention` | 1s to 90d |
| `max_output_bytes` | up to 5 TiB (`512MB`, `5GB`, `1GiB`: SI and IEC units both work) |
| `max_concurrent_jobs` | up to 10,000 |

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

Plain Temporal: activities, retries, timers, child workflows, `continue_as_new`, signals.

```python
# workflows.py -- no kontra import
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

## Files: `Artifact`

Temporal payloads are capped at a few MB. Anything larger is an `Artifact`: a reference to a
file in Kontra's bucket, encrypted with age to the job's parties.

```python
# activities.py -- still no kontra import: Artifact comes from models.py
from temporalio import activity
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

## The worker

```python
# worker.py
from datetime import timedelta
from kontra.serve import Worker, offering
from activities import fetch_batch, write_records
from quote import quote
from workflows import EnrichFlow

worker = Worker(
    offerings=[offering("offering.yaml", workflow=EnrichFlow, quote=quote)],
    activities=[fetch_batch, write_records],
    # every temporalio.worker.Worker keyword, passed through verbatim:
    max_concurrent_activities=40,
    max_task_queue_activities_per_second=50,
    graceful_shutdown_timeout=timedelta(seconds=60),
)

if __name__ == "__main__":
    worker.run()
```

`kontra.serve.Worker` is `temporalio.worker.Worker` plus exactly four things: a generated
Nexus service per bound offering (`quote` as a synchronous operation, `execute` as a
workflow-run operation), the hold-token check before `execute` starts anything, the
encryption codec, and the task queue, derived from `name@version` (passing `task_queue` is a
`TypeError`). `workflows`, `nexus_service_handlers` and `interceptors` are merged with
Kontra's; every other keyword reaches the inner worker unchanged. One worker can bind several
offerings. `worker.py` must expose a module-level `worker`: `kontra serve` runs it and
`kontra publish` imports it to find the offerings.

Before `execute` starts your workflow, the handler checks the hold token offline (signature
against the gate's keys, audience, expiry, your namespace, the offering, the contract hash),
validates the input against the contract, and re-runs your quote. No gate call is on that path.

## Testing without containers

`kontra.testing` has a fake for every seam. `Marketplace` wires a Temporal dev server
(downloaded once), the fake gate (catalog, quote relay, escrow by the platform's own rules,
play money) and logged-in parties into one process:

```python
# tests/test_offering.py -- `pytest` from the project directory
import asyncio

from kontra.testing import Marketplace


def test_lifecycle() -> None:
    asyncio.run(lifecycle())


async def lifecycle() -> None:
    from worker import worker  # the module-level worker: what `kontra serve` runs

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

## Running locally: `kontra dev`

```sh
kontra dev                    # = kontra dev up
kontra dev status             # containers, the gate's health, the profile
kontra dev logs [gate|temporal|postgres|postgres-relay|s3|migrate] [-f] [--tail N]
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
3. sends it to the gate, which requires a **fresh human login** (a worker token never
   qualifies), validates it, assigns the version an opaque Nexus endpoint name and creates the
   endpoint itself, pointed at your namespace and the version's task queue;
4. prints the contract hash, the endpoint and the task queue.

The gate refuses a document with an external `$ref`, an object without
`additionalProperties`, or a reserved `Kontra*` type that differs from the spec, naming the
path at fault. A login older than the gate's freshness window is refused: run `kontra login`
and publish again. An API key never publishes, however new (403 `fresh_login_required`):
publishing is a person's act.

`kontra serve` connects to Temporal with a 15-minute worker token, refreshed in the
background. That token can do one thing: run workers in your namespace. It cannot publish,
create or change an endpoint, or reach another namespace (see [security.md](security.md)).
Ctrl-C stops the worker gracefully.

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

## Versions

A published version is fixed. At start, `kontra serve` recomputes each offering's contract hash
from your code and refuses to start if it differs from the catalog's: change a type or
`offering.yaml`, then bump `version` and publish again. The hold token a buyer pays under
carries the hash too, and the handler refuses a token for any other hash, so a buyer who paid
for contract X is never served by code running contract Y.

A new version is a new document, endpoint and task queue. Jobs hired on the old version
finish on a worker that still serves it.

## Scaling

One `kontra serve` is one worker. To decide whether you need more, read the backlog of the
version's task queue — the one `kontra publish` printed. Your worker token may call
`DescribeTaskQueue` in your own namespace, so Temporal's own CLI works unchanged:

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
an artifact) and [`examples/legacy-actor`](../examples/legacy-actor) (an older job turned into
an offering). Each keeps its derived `contract.yaml` as a golden file, reproduced byte for byte
by the tests.
