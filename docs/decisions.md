# Implementation decisions

The PRD (`kontra-cloud/PRD.md`) is the design. This file records where implementing it
required a choice the PRD does not make, or a correction to something the PRD assumed
about Temporal or nexgen. Every entry was checked against the real thing (Temporal server
v1.32, temporalio 1.34, nexgen 0.2.5) before it was written down.

## I0. Repository

The public package lives in its own repository, `medmahmoudi26/kontra-sdk`, started from an empty
tree: nothing from the old platform's code or history. This replaces the PRD's D14 (a `v2` branch
of `medmahmoudi26/kontra`). The one-way dependency is unchanged: `kontra-cloud` depends on this
package and this package never imports the platform.

## I1. Names: constructed, never parsed; endpoints are opaque

`acme/enrich@1.2.0` cannot be used verbatim everywhere in Temporal, and an encoding that is later
parsed back is a classic source of confusion bugs: two strings that decode alike, a prefix check
that a crafted name satisfies, a regex that accepts a trailing newline. So:

| Identifier | Value | How it is made |
|---|---|---|
| Nexus endpoint | `k3f9c...` (`k` + 32 random hex) | assigned by the catalog at first publish; **means nothing** |
| Nexus service (wire) | `acme.enrich` | constructed; a `/` in a service name makes Temporal's Nexus HTTP route return NOT_FOUND (measured) |
| nexgen service key | `AcmeEnrich` + `fqn: acme.enrich` | constructed; nexgen requires `^[A-Z][a-zA-Z\d]+$` |
| Task queue | `acme/enrich@1.2.0` | constructed (Temporal accepts any string) |
| Seller namespace | `acme` | a party's namespace is its handle |
| Handler workflow id | `job-<32 hex>` | constructed from the job id |

Rules (`kontra/contract/names.py`):

* Constructed identifiers are one-way. No code splits a task queue, service name or endpoint to
  recover an offering, and no authorization decision reads one.
* The catalog is the only map between an endpoint and an offering, in both directions
  (`CatalogEntry.endpoint`, `GET /v1/endpoints/{endpoint}`).
* What people type (`acme/enrich@1.2`, a version, a handle) is validated with anchored,
  ASCII-only `fullmatch` patterns and length limits; nothing is stripped or normalised first.
* The one inverse that exists, workflow id to job id, is construct-and-compare: a candidate is
  accepted only if rebuilding the workflow id from it reproduces the input exactly.

## I2. The contract document and nexgen

nexgen 0.2.5 rejects any unknown keyword at the document root and on a service, so a
document carrying `x-kontra` is not nexgen input. The catalog document (the thing that is
hashed, §11.2) is the PRD's: the Nexus document plus `x-kontra` at the root. The *nexgen
view* is the same document without `x-kontra`; `kontra contract --nexgen` and
`kontra pull` emit it after verifying the hash. Acceptance criterion 16 is tested on the
nexgen view.

nexgen also refuses `$id` and non-local `$ref`, so the PRD's `$ref: "kontra:Artifact"` is
written as a local reference to a reserved definition: `#/$defs/KontraArtifact` and
`#/$defs/KontraQuote`. Their bodies are fixed by `spec/` and the catalog rejects a document
whose `Kontra*` definitions differ from the spec by a single byte. Seller types may not
start with `Kontra`.

The derivation from pydantic lowers to nexgen's strict subset: `Optional[T]` becomes
`oneOf: [T, {type: "null"}]` (pydantic emits `anyOf`), `default: null` is dropped, and
constructs nexgen cannot lower (`anyOf`, tuples, untyped `dict`/`Any`) are refused at
derivation with a fix-it. Every object carries `additionalProperties`: `false` for
models, a schema for typed maps (`dict[str, T]`); never absent and never `true`. This is
how the PRD's own example `summary: dict` is expressed (`dict[str, int]`).

## I3. Roles and Temporal permissions

There is exactly one Temporal role a token can carry: `worker`, as `permissions: ["acme:worker"]`
in Temporal's default JWT claim-mapper format (`common/authorization/default_jwt_claim_mapper.go`).
A buyer is a worker of its own namespace too: any party can sell and buy (§4), and a buyer's own
workflows need a worker.

| Caller | Temporal sees | Authorizer allows |
|---|---|---|
| `worker` in `acme` (seller or buyer) | JWT, `acme:worker` | an explicit allowlist of WorkflowService calls in `acme` only, including standalone Nexus operations and `DescribeTaskQueue` on `acme`'s own queues |
| any JWT | — | **no** OperatorService or AdminService call, ever; no Nexus HTTP dispatch on the public listener |
| control plane | the internal frontend's own principal (`AuthType "temporal"`, `Subject "internal"`) | everything; it is reachable only on the internal listener |

The PRD's `publisher` role (a token allowed to create endpoints whose name starts with its
namespace) is gone, and with it the only rule that had to inspect a request and a name. Publishing
is a gate call (`POST /v1/catalog`) that requires a human session younger than the freshness window
— a worker token never qualifies — and the gate then creates the Nexus endpoint itself through the
internal frontend: target = the caller's namespace and the offering's task queue, name = a fresh
random `k...` string. A compromised seller box therefore still cannot repoint any offering, and the
Authorizer has no name-parsing rule left to get wrong.

## I4. Who calls Nexus

* **Script and MCP**: the buyer SDK starts a *standalone Nexus operation* (Temporal ≥ 1.32,
  `nexusoperation.enableStandalone`) in the buyer's namespace, with the hold token in the
  `kontra-hold` header and `schedule_to_close_timeout = max_duration`. No buyer worker is
  needed; the result is durable on the server and the handle survives the script.
  This is exactly §8.2 step 3 ("in the buyer's namespace").
* **Workflow** (§7.2): the buyer's workflow is the Nexus caller.
* **Quote**: the gate relays it (§8.2 step 1) as a synchronous Nexus call on the internal
  frontend's HTTP API, carrying the buyer's encrypted payload byte for byte
  (`Content-Type: application/x-temporal-payload`). The quote *result* is plaintext: it is
  a price, and the gate must read it to lock it.

The gate never starts `execute`. It learns everything else by watching the seller's
handler workflow `job-<id>` through the internal frontend (I6).

## I5. MCP runs where the buyer's key is

§13 places the MCP server in the gate, but a hosted MCP server would receive the agent's
plaintext input and would have to decrypt the result for it, which breaks §10.1 ("Kontra
must be unable to read job payloads"). The MCP server is therefore `kontra.mcp` in the
public package, started as `kontra mcp` (stdio) next to the agent, and implemented on
`kontra.hire` with the local profile. It has the eight tools of §7.3 and calls the same
gate with the same rules.

## I6. Escrow is driven by observation, not by reports

The rules are pure functions in `kontra/escrow.py`. The gate feeds them three kinds of
fact: the state of `job-<id>` in the seller's namespace (polled through the internal
frontend), the clock, and the buyer's accept / reject / cancel calls. Neither party's
self-report moves money. The job row records the *intended* escrow state; the ledger
records the actual one; every sweep re-issues owed ledger calls (idempotent) until they
agree. Timers live in Postgres and a sweep loop, not in Temporal, so the gate is
testable without Temporal (§19.3).

The handler starts `job-<id>` with `execution_timeout = max_duration` and
`id_reuse_policy = REJECT_DUPLICATE`, so a hold token can start at most one execution.

## I7. Encryption

* Format: [age](https://age-encryption.org/v1) with X25519 recipients (`pyrage`), so a
  party can decrypt a delivery with the `age` CLI during a dispute. Public keys are
  `age1...`, as in the PRD's example. age encrypts to N recipients natively, one ~100-byte
  stanza each, which is what makes everything below cheap.

**A key does one job: encryption.** It is not an identity, not a login, and not a property
of a party. The first version welded all three together — one key per party, created inside
`kontra login`, on one machine — and every complaint fell out of that single edge: a second
machine answered `key_mismatch`; `--replace-key` bricked the first one and every in-flight
delivery to it; a lost laptop lost every past delivery and every open job; CI could not hire
without a human device-flow login on that box; a seller copied the key to each worker host by
hand; and a compromised host could not be revoked without rotating the party. Keys are now
split by role, because buyers and sellers have different key lifetimes:

```
buyer   : key per JOB     -- ephemeral, next to the job record, dies with it
seller  : keys per FLEET  -- a set; one per deployment, registered like authorized_keys
login   : auth only       -- it adds a recipient; it never decides one
```

* **Buyer, per job.** `kontra hire` generates an x25519 keypair, writes the secret half to
  `~/.kontra/jobs/<job_id>.key` (0600) and sends the public half as `buyer_key` in the hire
  request; the gate signs **that** into the hold token and never consults the buyer party's
  own keys. So hiring needs no key setup at all — a fresh machine, CI with `KONTRA_API_KEY`,
  `kontra mcp` — and a compromise reaches open jobs, not years of deliveries. `kontra job <id>
  --result` decrypts with the job key; `kontra job <id> --accept` shreds it, which makes the ciphertext the gate
  and the bucket still hold unreadable by anyone. The key is written *before* the hire call,
  under the quote id, because a lost answer can still have locked a hold: a job whose key was
  never written would be a delivery nobody can read. The seller side is unchanged — it
  encrypts to whatever `buyer_key` the signed token names, and caches it per job.
* **Seller, per fleet.** A party holds a *set* of recipients, each with a label and the time
  it was added (`kontra recipients add`, `list` and `revoke`; `POST`/`DELETE /v1/me/recipients`). Both
  need a fresh human login: a recipient can read every delivery made after it. Adding never
  replaces, so `kontra login` on a second machine adds that machine — `key_mismatch` and
  `--replace-key` no longer exist. Revoking stops future deliveries reaching that key and
  keeps the row: an in-flight job encrypted to it is still the seller's to decrypt, and a
  dispute may need the public key on record. The set is capped (16) so header overhead stays
  bounded.
* **One key per fleet, never one per pod.** A k8s secret mounted into every worker
  (`KONTRA_KEY_FILE`). Pod churn then never changes the set, so an input encrypted a minute
  ago is still decryptable by whichever worker picks the task up. Same ops shape as a TLS
  certificate. `kontra serve` refuses to start if the key it holds is not one of the party's
  recipients — otherwise it would poll happily and fail to read every input.
* **The contract seals no key** (spec `x-kontra.v2.json`). It already seals the party, as the
  first component of `name`; sealing a key as well would make "mount the key on one more
  worker host" a republish, and a republish is a new version, a new endpoint and a new task
  queue. Recipients are catalog data: the buyer reads the seller's current set where it read
  the single key before (the quote and hire answers, `GET /v1/endpoints/{e}`) and encrypts
  the Nexus input to all of them.
* `KontraCodec` encrypts every payload. Recipients come from the serialization context
  (temporalio 1.34): payloads of the root handler workflow `job-<id>` (input, result,
  failures) go to the seller's recipients *and* the job's buyer key; everything else on the
  seller side (activities, children) goes to the seller's recipients only. On the buyer side,
  Nexus inputs go to the seller's set (from the hire answer, or the catalog's
  `GET /v1/endpoints/{endpoint}` — never derived from the name) and to the job's own key. The
  quote operation's *output* passes through in plaintext (I4).
* A worker caches the job's buyer key per job; on a miss (another process, a restart) it asks
  the gate (`GET /v1/jobs/{id}/seller`).
* Failure messages and stack traces are encoded too (`encode_common_attributes`).
* Above 256 KiB of ciphertext a payload is claim-checked: uploaded to the bucket,
  content-addressed by the ciphertext's sha256, and replaced by a reference.
* Artifacts use the same age format, streamed in 64 KiB chunks, uploaded in parts, so
  memory stays flat regardless of size. `Artifact.sha256` is of the plaintext; the gate
  records the ciphertext's.
* **What this does not fix.** Kontra is still the directory for seller recipients, so a gate
  that substituted a key could read a seller's inputs; the contract no longer binds the key,
  so that caveat is no longer narrowed by the sealed document (`docs/security.md`, "Known
  limits"). Closing it needs signed recipient sets — the first seller key as an ed25519
  identity, each `recipients add` signed by an existing key — which the set makes a clean
  upgrade rather than a redesign.

## I8. Distribution name

PyPI `kontra` belongs to an unrelated project. The distribution is `kontra-sdk`; the
import name stays `kontra`. A seller's dependency must say `kontra-sdk`, never `kontra`.

## I9. Ledger accounting

The PRD's `ledger_events` row gains `party_id` and `account` (`available` | `held`):
every event is a set of postings that sum to zero, so `sum(postings) == sum(balances)`
(§9.3) holds by construction and is checked anyway. Money enters by `deposit` (top-up)
and leaves by `withdraw`, both against an external `kontra-world` account. The platform
fee (default 5%, `fee_bps`) is posted to the `kontra` account at release and on the
seller's share of a resolved dispute.

## I10. Hold token

ES256 JWT, `aud: kontra:hold`, claims in `kontra.gate_client.models.HoldClaims`. `exp` is
the hold's lock time plus `max_duration`: a start after that is refused, which refunds
the job by the deadline rule. The handler checks signature, audience, expiry, seller
namespace, offering, and contract hash, then re-runs the seller's own `quote` on the
input it received and refuses if the price exceeds the held amount, so a buyer cannot
quote a small input and execute a large one. No gate call is on this path; the JWKS is
fetched when the worker starts and again only for an unknown `kid`.

## I11. Concurrency limit

`max_concurrent_jobs` is enforced at the gate: a quote for an offering with that many
open jobs returns `busy` with an ETA (the earliest open job's deadline).

## I12. Per-namespace limits

Temporal's per-namespace limits (`frontend.namespaceRPS`, history size and count limits,
`limit.maxIDLength`, …) are dynamic config. The Temporal image ships defaults that apply
to each namespace separately; the gate can write per-namespace overrides to the dynamic
config file the server polls (`KONTRA_TEMPORAL_DYNAMIC_CONFIG`). Workflow duration is
bounded per job by I6.

## I13. CI gates

§19.4's "every PR" checks are five required jobs, each one `make` target, and `make check` is
their union, so local and CI cannot disagree: `lint`, `typecheck`, `test`, `seams` (② to ⑤)
and `deny-matrix` (seam ①: the Go deny matrix and `tests/temporal` over the wire, one job so
the Temporal server compiles once). Tests that would *skip* when their dependency is missing
are made to *fail* in CI instead: the Makefile hands the dev-server tests a checksum-verified
Temporal CLI 1.9.1 through `KONTRA_TEMPORAL_CLI` and the wire suite the freshly built binary
through `KONTRA_TEMPORAL_BIN`, and both conftests raise rather than skip when told a binary.

The end-to-end job needs the gate, which is private (`kontra-cloud`) and reaches this
repository only as an image. A `gate-image` job checks the image can be read first; when it
cannot, the run carries a warning and a summary line and `e2e` shows as skipped, never as a
silent pass. `e2e` is therefore not a required check until the gate image is readable from
this repository's CI.

The Temporal image is pushed only from `dev` (full-SHA tag, plus `:dev`), built without a
layer cache, for `linux/amd64`, with a BuildKit SBOM and provenance. A release is a tag that
names `pyproject.toml`'s version and points at a commit on `dev` or `main`; it publishes by
trusted publishing alone and attaches an SPDX SBOM. Every third-party action is pinned by
commit SHA; Trivy's action is pinned to a release made after its March 2026 tag hijack
(GHSA-69fq-xp46-6x23), with the Trivy version explicit.

## I14. Fork code never runs next to a private gate image

The gate is private, and its image is Python: whoever can pull it can read it. CI's `e2e` job
pulls it and then runs the pull request's own code on the same runner (`make sync`, `kontra
dev up`, `tests/e2e`), which could `docker save` it, or read the registry login. So while the
image is private (CI first tries to read it anonymously), `e2e` runs for pushes to `dev` and
for pull requests from branches of this repository only; a pull request from a fork is skipped
with a warning that says so, as an unreadable gate image is (I13). Once the image is public,
which `kontra dev` needs anyway, there is nothing to protect and forks run `e2e` too. The job
logs out of the registry as soon as the image is pulled, so no later step can read the
credential.

## I15. Only the buyer's own call delivers

I6 watched the *latest* run of `job-<id>` and paid for a completed one. A seller can start
`job-<id>` itself (it learns the id from the quote and the hold), with any input, or stop the
run the buyer's call started and complete a later one; either way it was paid unless the buyer
noticed and disputed in time. Now a run counts only if the buyer's own `execute` call started
its chain, and `Control.observe` establishes that by lookup and exact equality on structured
fields, never by decoding an operation token, a callback URL or any other string:

1. In the seller's namespace, from what only the server writes: the chain's first run (the
   run's `first_run_id`, which the server sets on a continue-as-new, retry or cron successor
   and no start request can carry, read in one call however long the chain), the request id
   that run was *started* with (`request_id_infos`, the entry for its
   `WorkflowExecutionStarted`; a request id attached to a running run does not count), and the
   links of its Nexus completion callbacks (at most two are followed; a genuine run has one).
   A seller chooses all three when it starts a workflow itself, so they only say where to look.
2. In the buyer's namespace (the job row's, never the link's), each such link is resolved: a
   standalone operation by operation id *and* run id (`DescribeNexusOperationExecution`), a
   buyer workflow's `NexusOperationScheduled` event by workflow, run and event id (a link "by
   request id" is refused: no caller makes one, and following it meant scanning a buyer's
   history for a string the seller chose; a link whose ids the frontend refuses as malformed
   names nothing, since a lookup that failed on it would fail every observation and a gate
   that cannot observe refuses the buyer's cancel). The call must be on the job's endpoint and
   service, operation `execute`, with the run's start request id, and carry a `kontra-hold`
   header (any ASCII spelling: the handler sees names lowercased) holding one of the hold
   tokens the gate issued for *this* job. The hold binds a call to a job because it is what
   the seller's handler checked; the operation id is the buyer's choice, so a buyer could call
   under any id and have escrow refuse a run the handler accepted.
3. The call's own record of the run it started (the handler's response link: the operation's
   `links`, the `NexusOperationStarted` event's) must not name another run of `job-<id>`.

How a hold matches (`hold_digest`). Escrow must count every call the handler accepted, and
the handler accepts more strings than the gate issued: ES256 verifies `(r, n - s)` as well as
`(r, s)`, the verifier's base64url decoder ignores padding, and HTTP drops spaces and tabs
around a header value before the handler sees it (server 1.32, measured: the call records
`" <token>\t"`, the handler is handed `"<token>"`). Matched byte for byte, a buyer could call
with such a spelling, have the job done, and see it refunded at the deadline as "not the
buyer's call". So a hold is matched by the sha256 of the bytes its signature covers -- the
JWS signing input, everything before the last `.` -- after dropping those spaces and tabs.
That part is the gate's own signed statement; nothing is decoded and no claim is read. The
gate digests each token it issues with the same function (the one in `start` re-signs the same
claims, so it has the same digest).

The first verified observation pins the run (`JobRecord.run_id`). From then on `observe`
reports that chain only: the latest run of the id when its `first_run_id` is the pinned run,
else (the chain closed and the seller started another run of the id) the chain's own last run,
walked forward through each successor whose start names the run before it. That walk reads at
most `CHAIN_READS` runs per call and then raises `ObservationIncomplete`, which a gate treats
as "cannot say" (the sweeper retries, a buyer's cancel is refused): reporting a long chain as
still running would let a buyer that already had the result cancel and be refunded. Escrow
ignores any other run of the id. A run that is not verified moves nothing (the job neither
starts working nor closes); at the deadline the job fails and refunds, "delivery not started
by the buyer's call" when that run completed. An `Observation` is `unverified` unless the
observer says otherwise, so an observer that does not check pays nobody.

A gate applies each observation to the job row as it is *when it saves* (the fake gate's
`_transition`; a compare-and-set in a database): an observation made before another replica
pinned the job then names another run and is ignored, instead of moving the pin.

What server 1.32.0 records (measured): the handler run's started event has no `links` of its
own; the caller's link is inside `completion_callbacks[].links` (`Link.NexusOperation` with a
run id for a standalone operation, `Link.WorkflowEvent` with an `event_ref` to the
`NexusOperationScheduled` event for a workflow). The operation, and the scheduled event,
record `request_id`, endpoint, service, operation and the Nexus header; the operation's
`links` and the `NexusOperationStarted` event hold the response link to the handler run. A
standalone operation's `request_id` is generated by the server, not taken from the starter's
request, and the `links` a starter puts on `StartNexusOperationExecution` are neither stored
on the operation nor sent to the handler, so a buyer can neither choose the request id nor
plant a response link that disowns the run.

Residual risk. The handler is the seller's code, so it decides which run a call starts: it can
start one, stop it and start another with the call's request id, callbacks and links (the
server de-duplicates a request id only while its run is the latest, so one run in between is
needed), answering the call with no response link or a false one. If the gate's first look
sees only that later run, it verifies. That is no more than a handler that started the later
run in the first place could do, the review window and dispute stand, and once the gate has
seen a run the choice is final. Checking the buyer operation's *outcome* would narrow it, but
the buyer can change that (it may terminate its own operation), which would let a buyer take a
delivery and a refund, so the rule reads only fields that are immutable once written. Each
run the seller starts makes the gate resolve its (at most two) links once, reading a buyer
workflow's history up to the event a link names: bounded by Temporal's history limit, and paid
again for every new run of the id a seller starts between sweeps. Finally, the lookup now reads
the buyer's namespace, which the buyer can load: if per-namespace rate limits a buyer can
exhaust also apply to the gate's internal frontend, a buyer could keep the lookup failing past
the deadline, and a gate that refunds when it has not been able to observe for a while (the
fake gate's `observe_give_up`) would refund a delivered job. The internal frontend must not
share those limits with party tokens.

Retention (`DEFAULT_RETENTION`) is the contract's max_duration cap plus 5 days, 35 days: a
hold token can start `job-<id>` until it expires, and only the first run's record refuses a
second start. `ensure_namespace` raises a shorter retention of an existing namespace to it and
never lowers one.

## I16. A job's start and end, closed to the parties' tricks

Five ways money or blame still moved wrongly with I10 and I15 in place. Each is closed with
facts the server or the gate records, compared for equality; nothing is parsed.

1. **Cancel, then start.** A hold token stays valid until it expires and the handler checks
   it offline (I10), so a buyer that canceled through the gate (refunded) could still run
   `execute` with it, and a job that finished before the gate's next look was delivered for
   free. A cancel of a job with no run now first *occupies* `job-<id>`
   (`Control.occupy_job`): through the internal frontend it starts a placeholder under that
   id (type and task queue `kontra.occupied`, the reason in its memo) with
   `REJECT_DUPLICATE` and `FAIL`, and terminates it at once, by run id. The handler starts
   the job with the same policies, so its start is refused for as long as the namespace
   retains the placeholder (`DEFAULT_RETENTION`, longer than any hold). The placeholder's
   first workflow task is delayed past its own one-minute execution timeout, so no worker
   ever runs it and it closes by itself if the termination never lands (server 1.32,
   measured: the execution timeout runs from the start, delay included). A run found
   instead (`Occupation.STARTED`: a call got there first) is observed again and stopped as
   a running job; an id that cannot be taken refuses the cancel (503), as an unobservable
   job does. The placeholder is recognised by every field it sets and reported as
   `Provenance.OCCUPIED`: never a delivery, never pinned, never "the seller's run
   terminated"; an open job that shows only it refunds at the deadline with its own reason.
   A seller can start a look-alike, which only refuses its own job (it could anyway), so
   the label moves no money; for the same reason that refund counts against the seller
   like any refund at the deadline, or a look-alike would excuse a seller that never
   started the job. The late-start watch stays, for runs that began before the cancel.
2. **Late start.** The hold's `max_duration` counts from the hire, but the run's timeout
   was a full `max_duration` from the handler's start. The handler now ends the run by the
   gate's refund deadline, which the hold carries (`deadline`, Unix seconds rounded down:
   the gate's grace is its own setting, and `kontra.testing`'s is one second; a token
   without it is taken to mean the expiry plus `DEFAULT_GRACE`), and refuses a start when
   the re-quoted duration is longer than the time left before the expiry plus
   `DEFAULT_GRACE` (or the deadline, if later): the seller is not blamed for a deadline it
   never had, and no run delivers after the buyer was refunded. The refusal keeps
   `DEFAULT_GRACE` even under a shorter gate grace because a job quoted for its whole
   `max_duration` always starts a moment after the hire; such a run is cut to the deadline.
   The cut uses the seller's clock against the server's, so a seller whose clock is behind
   gives its run that much more (its own loss). Like `input_size`, the claim is left out
   when unset, so tokens re-sign to the same bytes.
3. **A slow handler looks absent.** A handler that has not answered within one Nexus
   attempt (about 10 s) is recorded as `UPSTREAM_TIMEOUT`, exactly like a seller with no
   worker. The hold now carries `input_size`, the quoted input's size as the seller
   receives it (`kontra._control.input_size`: a claim-check reference's `size`, else the
   serialized payload), which the gate records only after the seller's own quote fetched
   and checked that input. The handler refuses an `execute` input larger than that (plus
   4 KiB for encodings) before fetching any of it, non-retryable and naming the bound, so
   what it fetches and validates is what the seller already did within the gate's quote
   timeout; a fetch still running 2 s before the request's deadline (6 s at most: the
   deadline is the server's clock, read on the seller's) is abandoned with a refusal. The claim is optional, and omitted from the claims when unset, so a token from
   a gate that does not set it re-signs to the same bytes (I15's digests). The offline
   checks of acceptance criteria 7 and 18 are unchanged.
4. **A buyer cancelling its own call** ended the job "canceled", counted against the seller.
   For a verified run that ended canceled, `Observation.buyer_canceled` now says whether
   the buyer's own call recorded a cancel request: the standalone operation's
   `cancellation_info` (described by operation id and run id, as for provenance), or a
   `NexusOperationCancelRequested` event naming the linked scheduled event in the buyer's
   workflow. Only the buyer and the server write either. Escrow then gives the reason a
   cancel through the gate gives (`BUYER_CANCEL_REASON`). The answer is remembered once
   known -- read from the call itself -- because a cancel request that caused the cancel
   is recorded before the handler is asked, so a later request cannot move the blame. A
   look that cannot find the call is "cannot say" (`TemporalError`, asked again), never
   "the buyer did not ask": escrow closes the job on the first answer it gets. Measured on server 1.32: nothing else the buyer does to its call (terminating
   the operation or its workflow, the workflow ending or being cancelled without
   cancelling the call, the operation timing out) cancels the seller's run.
5. **Blind refund.** A failed read of the buyer's namespace (provenance, cancellation)
   raises `BuyerLookupError`, a `TemporalError`. The buyer controls that namespace's load,
   so a gate must not give up into its deadline refund on it: the fake gate keeps asking,
   and the buyer's cancel stays refused meanwhile. A failure on the seller's side, or of
   the frontend, is still a plain `TemporalError`.

Measured on server 1.32: an operation's `cancellation_info` is set once
`RequestCancelNexusOperationExecution` is accepted (its `requested_time` stays empty, so
only its presence is read), and a cancel request on a closed operation is refused.

## I17. A cancel refunds only what Temporal shows was stopped

A buyer's cancel of a job with an observed run used to refund at once and only *ask* the
seller's workflow to cancel. A run that ignored the request and completed still delivered
to the buyer's own Nexus call: the buyer kept the work and the refund.

The cancel is now two steps, in the pure rules (`kontra.escrow`) so both gates share it:

1. The cancel records the request and asks the platform to **terminate** the pinned run
   (`EffectKind.TERMINATE_RUN`), through the control plane, by workflow id and the pinned
   chain's first run id (`Control.terminate_workflow(..., first_run_id=...)`), so a
   cancel can never stop a run outside the job's chain. A job with no run yet is occupied
   instead (I16).
2. The money then follows what Temporal shows of the pinned chain, never the request:
   terminated (or canceled, failed, timed out) is the buyer's cancel, refunded; completed
   first is a delivery, and the cancel is refused as delivered (409). A terminate that did
   not land, or a chain that cannot be observed, answers 503: the cancel stands, the
   sweeper keeps terminating and observing, and asking again is safe.

A closed run's status never changes, so two replicas cannot reach different answers. The
seller's run ends `TERMINATED` (not `CANCELED`) after a buyer's cancel; the reason the job
records is still "canceled by buyer".

## I18. A buyer gets a job's outputs only once it delivered

An output artifact was downloadable by the job's buyer while the job was still open. A
seller that let an artifact key out early (in a progress message, say) let the buyer take
the work and then cancel for a full refund.

Both gates now give a job's buyer its outputs only once the job's state is `completed`
(delivered: in review, accepted, disputed, resolved -- escrow never pairs `completed` with a
refund). Before that, and for failed or canceled jobs, the buyer gets 409 `not_delivered`.
The seller may always download its outputs; an input keeps its rule (the key is the
capability, the bytes are ciphertext to the job's parties).

The delivery reaches the buyer from Temporal as soon as the seller's run completes, while the
gate marks the job delivered on its next sweep. So `kontra.artifacts` treats `not_delivered`
as "not yet": the download retries with backoff for up to 60 seconds -- the same limit
`kontra.hire` uses for `accept` right after `result` -- and every other error fails at once.

## I19. Sign-in by email, and API keys for programs

The production gate signs people in with an email address (the PRD's D21) and gives programs
API keys (D22). Seam ② gains `POST /v1/auth/magic`, the `api_key` grant of
`POST /v1/auth/token` and `/v1/keys`; the fake gate implements all of it (an allowlist,
`FakeGate.allow_email`, and an in-memory outbox instead of mail). Three rules decide what the
new credentials can do, and each closes an attack the obvious design leaves open. An
adversarial review of the first build found four places where they did not hold (F1-F5
below), and each has its rule here.

1. **An API key never satisfies freshness.** A key owns one session whose `auth_time` is the
   key's `created_at`, so by the clock alone a key made a minute ago would pass the gate's
   freshness window. The gate therefore marks the session (`via = 'api_key'`) and every
   `fresh` route answers it 403 `fresh_login_required`, whatever its age; an old human login
   keeps its own 403 `stale_session`. A key hires, reads, accepts, disputes and runs `kontra
   serve` (every `party` and `party|worker` route, `/v1/auth/temporal-token` included), and
   never publishes, sets the party's encryption key, or makes, lists or revokes keys. A
   stolen key thus cannot repoint what a party sells, swap the key its deliveries are
   encrypted to, or mint itself a successor; revoking it (or its session) stops its tokens on
   their next request, because the gate checks the session id per request. The `resolver`
   rows (list disputes, split an escrow) need a fresh human session as well, checked after
   the resolver list: a key a resolver party made for its CI once split escrow like its
   owner (F4). The key is
   `kontra_sk_` plus 32 random bytes, shown once and stored as its sha256; its id (`key_` plus
   16 random hex) is never derived from it, and an unknown key and a revoked one get the same
   400 `invalid_grant`. The SDK keeps nothing on disk for a key (`Session.from_api_key`,
   `KONTRA_API_KEY`), refreshes by exchanging it again, sends it only over TLS or to this
   machine, and never shows it in an error, even a malformed one (a key with a stray newline
   is still the secret).

   **Revocation reaches Temporal.** Temporal verifies a worker token offline (signature,
   expiry, audience), so a revoked key's worker token kept polling the party's task queues
   and starting workflows for up to its 15 minutes (F3). Every token the gate mints names its
   session (`sid`); the gate publishes the sessions revoked in the last 20 minutes as sha256
   digests (`GET /v1/auth/revocations`, public: a digest is worth nothing without the token),
   and Kontra's Temporal server polls it every 5 s (`temporal/authorizer/revocation`) and
   refuses, after Temporal's own checks, a token whose session is listed or that names none.
   If the list cannot be fetched the last one stays in force: revocations happen at the gate,
   and stopping every worker whenever the gate is unreachable would turn a gate outage into
   a Temporal one. Taking an address off the allowlist (`gate.admin disallow`) revokes the
   sessions and keys of its party by default (F5): keys never expire on their own.
2. **A magic link only approves a device authorization whose code the human types.** Every
   sign-in starts an RFC 8628 device authorization (the CLI's, or the console's), and the
   link's page asks for that authorization's user code, which only the requester's screen
   shows: the mail carries the link and never the code. Without that, an attacker starts a
   device login, asks the gate to mail the victim a link for it, and the victim -- who did
   ask to sign in somewhere, perhaps, or simply clicks -- logs the attacker in. With it, the
   victim's mail is a genuine link that approves nothing, since no screen of theirs shows the
   attacker's code; three wrong codes burn the link. That holds only while **no page of the
   gate shows a code it did not issue to that screen**: the first gate's `/device` page
   offered "Email me a sign-in link" for the code in its URL, so an attacker who sent a victim
   his own `verification_uri_complete` had the gate mail her a real link and tell her, twice,
   the code to type -- his -- and her sign-in logged his client in as her, fresh enough to
   mint keys (F1). Email sign-in therefore starts at the client only (`kontra login --email`,
   the console); the device page asks for no address, and plain `kontra login` against a
   gate whose device page approves nothing (`GET /v1/info`: no GitHub, not a dev gate) says
   to use `--email` instead of opening it. A person also sees their sign-ins
   (`GET /v1/sessions`, the console's account page: how each proved itself, when) and ends
   one they do not recognise (`DELETE /v1/sessions/{id}`), which reaches Temporal like any
   revocation. The request answers the same 202 whether or not
   the address is invited, has an account, or the code is pending, and its rate limits count
   every request, so neither the answer nor a 429 says who is on the allowlist. They count
   **per requesting client**: a per-address budget anyone could spend let five requests from
   a stranger lock an address out of sign-in for an hour, and five mails caused with his own
   pending codes fill its quota on every replica (F2); a higher cap over all clients is the
   only one strangers share.
3. **A GET never consumes.** Mail scanners and link previews fetch links before people do. If
   opening `/auth/magic?t=` used the token, a scanner would burn every link (or, with a
   lenient page, sign someone in). The page only shows the form; the POST that carries the
   typed code consumes the link, atomically, once: a second POST, also one racing the first,
   finds it used.
4. **The browser that types the code lands signed in.** Seen in production: the link's page
   approved the device and then said "go back to where you started", and people expected
   the opposite -- the console, signed in, in the tab they were looking at. A console
   session lives only in the tab that made it (its tokens are in that tab's
   `sessionStorage`), so the page now hands this browser its own: having approved, it sends
   the browser to the console with a one-time **handoff** (32 random bytes) in the URL
   fragment, which never reaches a server; the console removes it from the address bar and
   trades it once, within `HANDOFF_TTL_SECONDS` (two minutes), for a session of its own
   (`grant_type=handoff` on `POST /v1/auth/token`; stored as its sha256, unknown, used and
   expired ones all `400 invalid_grant`). The session is as fresh as the approval, so a new
   account can set its key at once. Nothing about rule 2 moves: the handoff exists only
   after the right code was typed, so it goes to whoever typed it -- the person who also
   holds the screen that asked -- and the device that asked still gets its own session.

## I20. Seller code imports Temporal only through `kontra` (D1, amended)

The PRD's D1 said seller `workflows.py` and `activities.py` import nothing from `kontra`, so that
deleting Kontra leaves code that runs on any Temporal. It now says the reverse of the letter and
keeps the intent: they import Temporal **only** through the façade -- `from kontra import
workflow, activity, RetryPolicy, ApplicationError, Page` -- and never `temporalio` or `nexusrpc`.

- **Why.** A seller's dependency closure is then `kontra-sdk` alone, and `kontra-sdk` decides
  which Temporal SDK runs under it; the publish gate can refuse `import temporalio` anywhere in
  an offering instead of guessing which Temporal features a seller relies on; and Kontra's
  additions (`execute_activities_in_parallel`, the agents integration, `serve()` registering
  every `kontra.activity.defn`) sit in the namespace the seller already imports.
- **Why it still runs on any Temporal.** The façade aliases, never wraps: `kontra.workflow` and
  `kontra.activity` re-export Temporal's own objects (`kontra.activity.defn` returns Temporal's
  stamped function after noting it). Replacing `from kontra import workflow` with
  `from temporalio import workflow` gives the same program, less the additions.
- **What stays out.** Workflow and activity code never loads the serving stack (`kontra.serve`,
  the gate client, the codec, auth), never writes `import kontra` (the acceptance grep
  `grep -r "import kontra" examples/*/workflows.py examples/*/activities.py` still prints
  nothing), and never calls `continue_as_new`: the façade does not offer it, and the iteration
  that needs it is `execute_activities_in_parallel`'s child workflow. `models.py` may still use
  `kontra.types.Artifact` and nothing else from `kontra`.
- **Checked by** `tests/examples/test_imports.py` (statements, dynamic imports and module names
  in strings; what loading the code loads) over the examples and the `kontra init` scaffold.
