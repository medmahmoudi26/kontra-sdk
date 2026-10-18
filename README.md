# Kontra

**A marketplace of durable, quoted jobs.** A seller writes a plain [Temporal](https://temporal.io)
workflow, declares an offering in one YAML file and a `quote` function, and runs the worker on
their own hardware. A buyer hires it from a script, from inside their own Temporal workflow, or
from an AI agent over MCP. Calls travel through [Nexus](https://docs.temporal.io/nexus); Kontra
provides the identity, the catalog, escrow and the Temporal frontend, and nothing else. Payloads
are end-to-end encrypted between buyer and seller, so Kontra stores ciphertext and hashes, never
your inputs or results, and money moves only on facts Kontra can observe without reading a
payload.

This repository is the public half: the `kontra` Python package (SDK, CLI, MCP server, test
fakes), the Temporal Authorizer that every party's worker is subject to, the examples, and the
`kontra dev` stack. The gate (catalog, escrow, auth) is a separate, private service.

> Status: alpha (`0.2.3`). Python 3.11+.

## Quickstart

```sh
pip install kontra-sdk
kontra login --email you@example.com   # a sign-in link by mail; the page asks for the code
                                       # this command prints (sign-ups are by invitation)
kontra balance                         # you are in; `kontra quote` and `kontra hire` next
```

`kontra login` talks to `https://api.kontra.run` unless `--gate` or `KONTRA_GATE` says
otherwise, and writes your profile and your encryption key to `~/.kontra`. Plain
`kontra login` approves the code in your browser instead, on a gate whose page offers a way
to (GitHub, or `kontra dev`).

**Programs use an API key**, not your login: a key hires, reads and runs `kontra serve`, and
can never publish, change your encryption key or make other keys.

```sh
kontra keys create --label ci          # prints the key once; the gate keeps only its hash
export KONTRA_API_KEY=kontra_sk_...    # Kontra(), kontra serve and kontra mcp pick it up
kontra keys list
kontra keys revoke key_0123456789abcdef
```

A program also needs your key file (`~/.kontra/key`) to decrypt what it buys: copy it to the
machine, or point `KONTRA_HOME` at a directory holding it. Nothing else is read or written.

## Install

```sh
pip install kontra-sdk                 # the SDK and the `kontra` command
pip install "kontra-sdk[mcp]"          # + `kontra mcp`, for agents
pip install "kontra-sdk[testing]"      # + the in-process marketplace for your tests
```

> **The distribution is `kontra-sdk`.** `kontra` on PyPI is an unrelated project. Never write a
> bare `kontra` dependency in a requirements file or a `pyproject.toml`: it installs someone
> else's code. The import name is `kontra`.

`kontra dev` also needs Docker with the compose plugin. It runs the images
`ghcr.io/medmahmoudi26/kontra-temporal:dev` and `ghcr.io/medmahmoudi26/kontra-gate:dev`;
`KONTRA_TEMPORAL_IMAGE` and `KONTRA_GATE_IMAGE` name others (in a checkout, `make temporal`
builds `kontra-temporal:dev` from [`temporal/authorizer`](temporal/authorizer)). The published
Temporal image is `linux/amd64` only: on an arm64 machine Docker has to emulate it, or build a
native one with `make temporal`.

## Sell a job

```sh
kontra dev                    # a local Kontra in Docker (Temporal, gate, Postgres, bucket);
                              # logs you in as a dev party with play money
kontra init wordcount         # ./wordcount: offering.yaml, models, workflow, quote, worker, a test
cd wordcount
pytest                        # the whole lifecycle -- quote, hire, deliver, accept, refund --
                              # in a unit test, no containers (pip install "kontra-sdk[testing]" pytest)
kontra serve                  # run the worker; on a dev profile it publishes the offering first
```

`kontra init` writes the project layout every offering has:

| File | Owns |
|---|---|
| `offering.yaml` | name, version, limits, display text. Declarative; names no code |
| `models.py` | input and output types; the published contract is derived from them |
| `workflows.py`, `activities.py` | the job, in Temporal through the façade: `from kontra import workflow, activity`, **never `temporalio`** |
| `quote.py` | the price, computed from the input |
| `worker.py` | binds workflow and quote to the offering; every `temporalio.worker.Worker` option |

Against the real marketplace:

```sh
kontra login --email you@example.com   # or plain `kontra login` (browser); makes this machine's key
kontra publish                         # your login, never a key; past its first hour it asks you to confirm it's you
kontra serve                           # on the machine that does the work (a key will do here)
kontra status --workers                # do the workers keep up? Temporal's backlog and pollers
```

A published version is fixed: change a type or `offering.yaml` and bump the version, or
`kontra serve` refuses to start ("republish or bump the version"). See
[docs/sellers.md](docs/sellers.md).

[`examples/hello`](examples/hello) is the smallest offering (a name in, `Hi <name>` out, a
cent a job) and the five-minute tutorial from login to a paid job; a live copy runs as
`demo/hello` on [app.kontra.run](https://app.kontra.run).

## Hire a job

From the shell:

```sh
kontra quote acme/enrich -f in.json
kontra hire acme/enrich -f in.json --max-price 1 --wait --accept
kontra jobs
kontra job <job id> --result out/      # writes result.json and every artifact
```

From a script:

```python
import asyncio

from kontra.hire import Kontra


async def main() -> None:
    # Your profile and key (~/.kontra). The input is checked against the published
    # contract, then encrypted to the seller and to you.
    async with Kontra() as k:
        q = await k.quote("acme/enrich", urls=["https://example.com/"])
        print(q.price, q.currency, q.max_duration)

        job = await q.hire(max_price=1)  # locks the price in escrow; nothing above max_price
        async for p in job.progress():  # the seller's heartbeats, decrypted
            print(p)

        result = await job.result()  # decrypted with your key, checked against the contract
        await result.records.download("records.jsonl")  # an artifact: streamed, verified
        await job.accept()  # or job.reject("why"); silence accepts when the window ends


asyncio.run(main())
```

From inside your own Temporal workflow (composition):

```python
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from kontra.hire import service


@workflow.defn
class Recon:
    @workflow.run
    async def run(self, domain: str) -> dict:
        records = await service("acme/enrich", max_price=5).execute(urls=[f"https://{domain}/"])
        return records.to_dict()
```

Run that workflow on `await kontra.hire.worker(task_queue="recon", workflows=[Recon])`: a
plain Temporal worker whose client carries your keys and which runs the quote-and-hold
activities. Cancelling the workflow cancels the job and refunds the hold.

From an agent: `kontra mcp` serves eight tools (`search`, `describe`, `quote`, `hire`,
`status`, `result`, `accept`, `reject`), and `workers` for a seller's own versions, over
stdio, on the machine that holds your key:

```json
{ "mcpServers": { "kontra": { "command": "kontra", "args": ["mcp"] } } }
```

See [docs/buyers.md](docs/buyers.md).

## The security model, briefly

- **End-to-end encryption.** Every Temporal payload passes through a codec that encrypts it with
  [age](https://age-encryption.org) to the job's parties: inputs to the seller (and the buyer),
  results to the buyer (and the seller). Large values and files go to the bucket as ciphertext.
  Kontra holds ciphertext, hashes, sizes, timestamps and workflow metadata. The one payload it
  reads is a quote's answer (a price, or a refusal), because it must lock the price. Each job
  has its own key, made where it is hired and copied to each of your devices (browsers,
  `kontra login` machines) encrypted to that device alone, so a job hired in the console reads
  in the CLI with nothing to export.
- **Escrow from observable facts.** Money moves by pure rules (`kontra/escrow.py`) over three
  kinds of fact: the state of the seller's handler workflow as Temporal records it, the clock,
  and the buyer's accept, reject and cancel calls. Neither party's word moves money. A job not
  delivered by its deadline refunds the buyer; a delivery the buyer does not reject within the
  review window pays the seller.
- **The Authorizer.** Kontra runs its own Temporal server. Every token on the public frontend
  is one weak role, `<namespace>:worker`, confined to its own namespace by an allowlist; no
  token can reach an Operator or Admin API, so a compromised seller machine cannot repoint an
  offering. The policy is in [`temporal/authorizer`](temporal/authorizer), public so you can
  read the rules you run under.
- **Logins and keys.** A person signs in through a device code that only their own screen
  shows, approved in the browser or through a mailed link whose page asks for that code. A
  change asks them to confirm it's them once an hour: by a mailed link, or in the console with
  one tap of a passkey, which confirms a sign-in and never signs anyone in. An API key is for
  programs: it never publishes, whatever its age.

What Kontra can and cannot see, the threat model, and the limits: [docs/security.md](docs/security.md).

## Documentation

| | |
|---|---|
| [docs/sellers.md](docs/sellers.md) | writing, testing, publishing and serving an offering |
| [docs/buyers.md](docs/buyers.md) | hiring from the CLI, a script, a workflow, or an agent |
| [docs/security.md](docs/security.md) | threat model, encryption and the Authorizer |
| [docs/architecture.md](docs/architecture.md) | the five seams, the one-way rule, how names are built |
| [docs/decisions.md](docs/decisions.md) | implementation decisions, checked against Temporal and nexgen |
| [spec/](spec/README.md) | the contract document's `x-kontra` extension and reserved types |
| [temporal/authorizer](temporal/authorizer/README.md) | the Temporal server, its policy and its configuration |
| [examples/](examples) | `hello` (the smallest seller, a five-minute tutorial), `enrich` (seller), `legacy-actor` (seller), `sweep` (seller: a fan-out, a batch and a branch), `chef-agents` and `chef-claude` (sellers: agents), `recon` (buyer workflow) |

## Contributing

`make sync && make check` runs what CI runs. Work on a topic branch and open the pull request
against `dev`. See [CONTRIBUTING.md](CONTRIBUTING.md); report vulnerabilities privately as
described in [SECURITY.md](SECURITY.md).

## License

[Apache-2.0](LICENSE).
