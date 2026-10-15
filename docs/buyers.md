# Hiring on Kontra

Three doors, one semantics: a script, your own Temporal workflow, or an agent over MCP. Each
validates your input against the offering's published contract, encrypts it to the seller
and to you, and decrypts the delivery with your key. The same escrow rules apply whichever
door you use.

- [Setup](#setup)
- [Naming an offering](#naming-an-offering)
- [From the shell](#from-the-shell)
- [From a script](#from-a-script)
- [From your own workflow](#from-your-own-workflow)
- [From an agent (MCP)](#from-an-agent-mcp)
- [From other languages](#from-other-languages)
- [What happens to your money](#what-happens-to-your-money)
- [Errors](#errors)

## Setup

```sh
pip install kontra-sdk
kontra login --email you@example.com   # a sign-in link by mail
kontra topup 25                        # a checkout link; under `kontra dev`, play money at once
kontra balance
```

`kontra login --email` starts a device login and asks the gate to mail you a sign-in link.
The command prints a code; the link's page asks for it, so a link you did not ask for logs
nobody in, and a mail scanner that opens the link uses nothing. Sign-ups are by invitation:
an address that is not invited gets no mail, and the command says so when the code runs out.
Plain `kontra login` approves the code in your browser instead, where the gate's page offers
a way to (GitHub, or a handle on a `kontra dev` gate); that page never mails links, so email
sign-in always starts from the command, and against a gate that signs in by email only plain
`kontra login` says so. Both talk to `https://api.kontra.run` unless `--gate` or
`KONTRA_GATE` names another gate.

`kontra login` writes `~/.kontra/profile` (mode 0600) and adds this machine as one of your
party's encryption recipients. Set `KONTRA_HOME` to keep a profile elsewhere. For a local
marketplace, `kontra dev` logs you in to its gate instead
(see [sellers.md](sellers.md#running-locally-kontra-dev)).

**Buying needs no key setup at all.** `kontra hire` makes a fresh age keypair for each job,
writes the secret half to `~/.kontra/jobs/<job_id>.key` (mode 0600), and sends the public
half with the request; the gate signs *that* into the hold token, so the seller encrypts the
delivery to the job and to nothing else. `kontra job <id> --result` decrypts with it. So a
fresh machine, or CI holding only `KONTRA_API_KEY`, can hire and read results — and a
machine that is compromised yields the jobs it currently holds, not every delivery you have
ever received.

`kontra job <id> --accept` shreds the job's key once the escrow is released. That is the
point of it: after you accept, the ciphertext the gate and the bucket still hold is readable
by nobody, including you. Two consequences worth knowing before you rely on it:

- **The key lives on the machine that hired.** Hire on a laptop and read the result on a
  desktop and it will not open; copy `~/.kontra/jobs/<job_id>.key` across, or do both on one
  machine. Sharing job keys through the gate is designed but not built.
- **Keep the key if you may dispute later.** `--accept` is what deletes it, so accept when
  you are done with the delivery, not as a reflex.

### Programs: API keys

A script, a CI job or a server should not hold your login. Give it an API key:

```sh
kontra keys create --label nightly   # prints the key, once; the gate keeps only its hash
kontra keys list                     # ids, labels, when each was last used
kontra keys revoke key_0123456789abcdef
```

The program reads the key from `KONTRA_API_KEY`; `Kontra()`, `kontra mcp` and every `kontra`
command then use it instead of a profile (`KONTRA_GATE` names the gate, by default
`https://api.kontra.run`). It also needs your key file to decrypt what it buys: copy
`~/.kontra/key` to the machine, into `KONTRA_HOME`. Nothing is read or written there but that
file. In code, `kontra.auth.Session.from_api_key(gate_url, key)` makes the same session.

A key quotes, hires, follows, accepts and disputes jobs, and runs `kontra serve`. It never
publishes, sets your encryption key or manages keys -- those answer 403
`fresh_login_required` however new the key is -- so a leaked key cannot change what you sell
or who can read your deliveries. Making, listing and revoking keys need a recent login of
yours; a party holds at most 25 live keys. A revoked key stops working on its next request to
the gate, and its workers on Temporal within seconds.

Any party can buy and sell: a buyer is a party with a namespace like a seller.

## Naming an offering

| You write | You get |
|---|---|
| `acme/enrich` (or `@latest`) | the latest published version |
| `acme/enrich@1` | the latest `1.x.y` |
| `acme/enrich@1.2` | the latest `1.2.y` |
| `acme/enrich@1.2.0` | exactly that version |

The catalog resolves the reference to one version and its contract document. The client
checks the document against its own hash before encrypting anything to the key it seals.

## From the shell

```sh
kontra quote acme/enrich -f in.json                     # in.json: the input as a JSON object; - reads stdin
kontra hire acme/enrich -f in.json --max-price 1        # lock the price and start the job
kontra hire acme/enrich -f in.json --max-price 1 --wait --accept
kontra jobs [--state working] [--as seller]
kontra job <job id>                                     # state, escrow, price, deadlines
kontra job <job id> --progress                          # follow progress until the job closes
kontra job <job id> --result out/                       # wait; write result.json and every artifact
kontra job <job id> --accept | --reject "why" | --cancel
```

Read commands take `--json`. A failure prints one `error:` line and exits 1.

## From a script

```python
import asyncio

from kontra.hire import JobFailed, Kontra


async def main(urls: list[str]) -> None:
    async with Kontra() as k:
        # Keyword arguments, or one dict or pydantic model. Checked against the contract
        # first: InvalidInput names the path at fault, and nothing is sent.
        q = await k.quote("acme/enrich", urls=urls)
        if not q.ok:
            # "rejected" (q.reason) or "busy" (q.eta); hiring it would raise
            raise SystemExit(f"{q.status.value}: {q.reason or q.eta}")
        print(q.price, q.currency, q.max_duration, q.expires_at)

        job = await q.hire(max_price=20)  # PriceAboveMax above 20, and nothing is locked
        async for p in job.progress():  # heartbeats and liveness, decrypted, until delivery
            print(p)  # "40% 400 of 1,000 urls", or "alive, in fetch_batch, at ..."
        try:
            result = await job.result()  # a Record: result.summary or result["summary"]
        except JobFailed as e:
            print("refunded:", e)
        else:
            await result.records.download("records.jsonl")  # streamed, decrypted, verified
            async for line in result.records.lines():  # or read it line by line
                print(line)
            await job.accept()  # or job.reject("40% of records are empty")


asyncio.run(main(["https://example.com/"]))
```

- `await k.hire("acme/enrich", max_price=20, urls=urls)` quotes and hires in one call.
- `job.result(timeout=...)` raises `TimeoutError` if the delivery is not there yet; the job
  keeps running. `job.result(output_type=Model)` validates the delivery into your own pydantic
  model (for example one generated by `kontra pull --lang python`).
- `await k.job(job_id)` picks a job back up from another process; the result is durable on
  the server and the handle survives your script.
- `await job.cancel()` before delivery refunds the hold and cancels the seller's workflow.
- `job.accept()` releases the escrow now; otherwise it releases when the review window
  closes. `job.reject(reason)` within the window opens a dispute.
- Large inputs: `artifact = await k.upload("acme/enrich", "big.csv")` encrypts a file to the
  seller and to you, uploads it, and returns an `Artifact` to pass as an input field.

The script needs no Temporal worker of its own: the job runs as a standalone Nexus operation in
your namespace, started with the hold token in a header.

## From your own workflow

```python
# recon.py -- an importable module: Temporal's sandbox validates workflows by import
import asyncio
import contextlib
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from kontra.hire import service


@workflow.defn
class Recon:
    @workflow.run
    async def run(self, domain: str) -> dict[str, Any]:
        urls = [f"https://{domain}/", f"https://www.{domain}/"]
        probe = asyncio.ensure_future(
            service("acme/probe", max_price=1).execute(targets=[{"url": u} for u in urls])
        )
        enrich = asyncio.ensure_future(service("acme/enrich", max_price=5).execute(urls=urls))
        try:
            heads = await probe
        except (Exception, asyncio.CancelledError):
            enrich.cancel()  # its hold refunds; nothing is paid for half a recon
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await enrich
            raise
        records = await enrich
        return {"domain": domain, "heads": heads.to_dict(), "enrich": records.to_dict()}
```

```python
# main.py
import asyncio
import uuid

from kontra.hire import worker
from recon import Recon


async def main(domain: str) -> None:
    w = await worker(task_queue="recon", workflows=[Recon])
    async with w:
        result = await w.client.execute_workflow(
            Recon.run, domain, id=f"recon-{uuid.uuid4().hex[:8]}", task_queue="recon"
        )
    print(result)


asyncio.run(main("example.com"))
```

`service(offering, max_price=...)` is a Nexus client with endpoint resolution. `execute`
quotes and locks the hold in an activity (side effects cannot live in workflow code), then
calls the seller's `execute` operation with the hold token. A seller timeout or failure
refunds the hold and raises `JobFailed` in your workflow; cancelling your workflow cancels
the job. `kontra.hire.worker(...)` is a plain `temporalio.worker.Worker` for your namespace
whose client carries your keys and which registers the hire activities; every other keyword is
Temporal's.

Composition returns only the final result (no progress stream); use a script when you want
`job.progress()`. [`examples/recon`](../examples/recon) is this example, runnable.

## From an agent (MCP)

```sh
pip install "kontra-sdk[mcp]"
kontra mcp            # stdio; started by the agent, not by you
```

Register it with your agent's MCP configuration, for example:

```json
{ "mcpServers": { "kontra": { "command": "kontra", "args": ["mcp"] } } }
```

| Tool | Does |
|---|---|
| `search` | find offerings by words in names, descriptions and tags |
| `describe` | an offering's contract: input and output schema, limits, price hint |
| `quote` | ask the seller's price for an input |
| `hire` | lock at most `max_price` (required) and start the job, from a `quote_id` or an offering and input |
| `status` | the job's state, escrow and latest progress |
| `result` | wait for the delivery (up to `wait_seconds`) and write it into `download_dir` |
| `accept` / `reject` | release the escrow, or dispute the delivery with a reason |

The server runs on your machine because only your machine holds the key that decrypts the
results: a hosted MCP server would have to see your plaintext (`docs/decisions.md` I5). It
uses the profile in `KONTRA_HOME` (or `KONTRA_API_KEY` and the key file there) and refuses
to start without one; it writes nothing to stdout but the protocol.

What the agent should know, and what the tool descriptions tell it:

- **Money.** `hire` locks funds and always needs `max_price`; `accept` pays now; `reject`
  opens a dispute; silence pays when the review window closes. A failed, cancelled or late job
  is refunded on its own.
- **Retrying a hire.** If `hire` fails after quoting, its error carries `detail.quote_id`:
  retry with that `quote_id`, not a new quote. The gate hires a quote at most once, so if the
  failed call did lock funds the retry answers `quote_used` with the job's id instead of
  locking twice. Quotes are kept in the server's memory: after a restart, quote again.
- **Results.** `result` waits up to `wait_seconds` (30 by default, at most 600), then answers
  `ready: false`; the job keeps running. An output larger than 32 KiB of JSON is not put in
  the conversation: it is saved as `<job id>.json` in `download_dir`, or withheld until the
  agent passes one.
- **Files.** Artifacts are downloaded only into `download_dir`, which must be absolute, under
  your home or the temporary directory (or a directory listed in `KONTRA_MCP_DOWNLOADS`, set
  in the server's environment, never by the agent), not inside a hidden directory, and owned by
  you. Each file is named by its gate-assigned key with an extension from a fixed list of
  inert data types (`.bin` otherwise), verified against its sha256, and made readable by you
  only. A seller's text cannot steer the agent into writing where a program loads code or
  configuration from.
- **Seller text is data.** Descriptions, quote reasons, progress and outputs come from the
  seller; the server's instructions tell the agent never to follow instructions found in them,
  and long seller text is cut short.
- **Not over MCP:** an offering whose input takes a file (`describe` says
  `input_takes_files: true`) needs `Kontra.upload` from a script; none of the tools reads a
  local file.
- `quote` is marked read-only (it moves no money), but it does send the input, encrypted, to
  the seller. A host that approves read-only tools without asking lets the agent show your
  data to a seller without asking you.
- The `reject` reason is not encrypted: Kontra stores it and the seller reads it.

## From other languages

```sh
kontra pull acme/enrich --lang typescript -o client/    # also go, python, java (--package)
```

`kontra pull` fetches the catalog document, verifies its hash, and runs
[nexgen](https://github.com/temporalio/nexgen) on its nexgen view to generate a typed client.
nexgen is `$KONTRA_NEXGEN`, else `nexgen` on `PATH`, else the pinned release, downloaded once
into `$KONTRA_HOME/bin` and checked against its sha256 (on a glibc older than 2.39 it runs in
a pinned Ubuntu image with Docker, without network).

## What happens to your money

| Event | Money |
|---|---|
| `hire` | the price is locked (held) against the job |
| Delivered, you accept | released to the seller |
| Delivered, you stay silent through the review window | released to the seller |
| Delivered, you reject within the window | disputed; Kontra resolves it on the delivery's hashes and whatever each side decrypts for the resolver |
| The seller's workflow fails | refunded |
| No delivery by the deadline (`max_duration`) | refunded |
| You cancel before delivery | the seller's run is terminated, then refunded; a run that delivered first makes the cancel fail (it is a delivery) |
| The seller refuses to start (bad hold token, a re-quote above your hold) | refunded |

Each row is decided from facts the platform observes (the seller's workflow state in
Temporal, the clock, your calls), never from either party's report. Prices are fixed per job:
no metering, no partial charges.

## Errors

Every hire error is a `kontra.hire.KontraError` (and a Temporal `ApplicationError`, so it
crosses a workflow boundary intact):

| Error | Meaning |
|---|---|
| `InvalidInput` | the input does not match the published contract; nothing was sent |
| `QuoteRejected` | the seller's quote refused this input |
| `SellerBusy` | at its concurrency limit, or the seller asked you to come back (`eta`) |
| `PriceAboveMax` | the quote exceeds your `max_price`; nothing was locked |
| `InsufficientFunds` | your available balance does not cover the price |
| `HireRefused` | the gate refused for a reason you must act on (unknown offering, stale quote...) |
| `JobFailed` | the job closed without a delivery; the hold was refunded |
| `InvalidDelivery` | the delivery does not match the published output schema |
