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
is a gate call (`POST /v1/catalog`) that requires a person's session confirmed within the hour (I23)
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
gate with the same rules; a ninth, `workers`, is a seller's own Workers view (I21).

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
  and the bucket still hold unreadable by anyone. *Amended by I25:* the job key is also
  copied to each of the account's devices, encrypted to that device alone, so a job key
  reaches one job but a device key reaches every job wrapped to it, and `--accept` shreds
  this machine's key file only: every device of the account, this machine included, keeps its
  copy at the gate, so the job stays readable by the account's devices while it exists. The key is written *before* the hire call,
  under the quote id, because a lost answer can still have locked a hold: a job whose key was
  never written would be a delivery nobody can read. The seller side is unchanged — it
  encrypts to whatever `buyer_key` the signed token names, and caches it per job.
* **Seller, per fleet.** A party holds a *set* of recipients, each with a label and the time
  it was added (`kontra recipients add`, `list` and `revoke`; `POST`/`DELETE /v1/me/recipients`). Both
  are changes a person confirms (I23): a recipient can read every delivery made after it. Adding never
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
   `fresh` route answers it 403 `fresh_login_required`, whatever its age; an unconfirmed human
   login gets its own 403, `confirmation_required` (amended by I23: `fresh` means confirmed
   within the hour, and the reads of keys and sign-ins are `human`, which a key never is). A
   key hires, reads, accepts, disputes and runs `kontra
   serve` (every `party` and `party|worker` route, `/v1/auth/temporal-token` included), and
   never publishes, sets the party's encryption key, or makes, lists or revokes keys. A
   stolen key thus cannot repoint what a party sells, swap the key its deliveries are
   encrypted to, or mint itself a successor; revoking it (or its session) stops its tokens on
   their next request, because the gate checks the session id per request. The `resolver`
   rows (list disputes, split an escrow) need a person's session as well, checked after
   the resolver list, and splitting escrow a confirmed one (I23): a key a resolver party
   made for its CI once split escrow like its owner (F4). The key is
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

## I25. The workflow does what it wants (D1; kontra-sdk#27)

Kept next to I20 because it is the same rule from the worker's side: the façade aliases
Temporal and never wraps it, so a workflow that is a legal Temporal workflow without Kontra
is one with Kontra. **No `serve()` or `Worker` keyword may change what workflow code can do.**
Worker-side facts are config -- concurrency caps, timeouts, which MCP processes to spawn,
which model endpoint to dial -- and "may this workflow run an agent" is not one of them.

- **Removed: the `agents` keyword of `serve()`** (7ee0fd1, #20), and the same keyword it
  threaded through `serve_from`, `seller_data_converter` and `payload_converter_class`. It is
  not a `temporalio.worker.Worker` argument, so passing it to `serve()` is now the
  `TypeError` any stranger keyword gets. Its two reasons -- the plugin builds an OpenAI client when the
  worker starts, and swaps in `OpenAIPayloadConverter`, which validates more leniently --
  are consequences of the workflow running an agent, not of a flag: a worker whose workflow
  imports the Agents SDK needs both, one whose workflow does not must get neither.
- **Derived instead.** `serve()` runs after `worker.py` imported its workflows, so
  `"agents" in sys.modules` (OpenAI's Agents SDK, imported by the workflow module or anything
  it passed through) puts `OpenAIAgentsPlugin` on the worker, and the worker's converter
  follows its plugins (`kontra._agents.uses_agents`). `agents` imported without the extra is
  a `TypeError` naming `pip install 'kontra-sdk[agents]'`; the extra installed and not
  imported changes nothing. An `OpenAIAgentsPlugin` already in the seller's own `plugins=` is
  the worker's, and serve() adds none (two would register the same activities).
  For the import to stay the signal, `kontra.workflow` imports `activity_as_tool` and
  `mcp_server` when a workflow first names them, never with the façade, and leaves them out
  of `__all__` (a star import would name them).
- **Where the issue and the code part ways.** The issue keeps the explicit `Worker(plugins=...)`
  path for the seller to pass the plugin; `kontra publish` refuses `temporalio` imports in
  seller code, so a published offering could not. `kontra.serve.Worker` therefore derives the
  plugin the same way when it is made (its workflows are imported by then), and serve()'s
  worker is one of those. And the signal is OpenAI's `agents`, the `openai-agents`
  distribution's, not the name: a seller's own `agents.py` imported by the workflow module
  runs no agent and gets nothing (it was a `TypeError` without the extra, and an
  `ImportError` with it).
- **What the import does not tell.** `sys.modules` is the process's: an `activities.py` the
  workflow passes through counts as the issue words it, so an activity that runs an agent
  in-process, outside Temporal's loop, imports `agents` inside its function to keep the
  worker plain. An import inside `run` comes after the worker was built: no plugin, and the
  workflow task fails. `docs/sellers.md` says both.
- **`mcp_servers=` stays**, as worker config: a factory holds processes and credentials, the
  same kind of thing as `max_concurrent_activities`. It implies nothing about the workflow;
  a factory for a name no workflow asks for is unused.
- **The model endpoint is worker config, set the Agents SDK's own way.** Checked against
  temporalio-openai-agents 1.1: the plugin's default model activity builds
  `OpenAIProvider(openai_client=AsyncOpenAI(max_retries=0))` when the worker is built, so it
  never reads the SDK's default client (and refuses to build without `OPENAI_API_KEY`).
  `serve()` therefore gives the plugin `model_provider=OpenAIProvider()`, which reads the
  default API when it is made, with the worker, and takes the default client at the first
  model call: `set_default_openai_client(AsyncOpenAI(base_url=...))` and
  `set_default_openai_api("chat_completions")` in `worker.py`, before `serve()`, point an
  agent workflow at Ollama, vLLM, OpenRouter or Mistral, with no new keyword. Without a default client it is the
  SDK's own default from `OPENAI_API_KEY`/`OPENAI_BASE_URL`; the model, when the agent names
  none, is the SDK's `OPENAI_DEFAULT_MODEL`. The cost: that fallback client keeps the OpenAI
  client's own two retries inside an activity attempt, where the plugin's had none; set a
  default client with `max_retries=0` to leave every retry to Temporal.
- **Checked by** `tests/serve/test_serve_fn.py` (the keyword refused; no import, no plugin,
  with or without the extra; a seller's own `agents.py` is no import; the import installs it
  with the MCP servers, on a `Worker(...)` written out too; the default client is the
  endpoint) and `tests/examples/test_chef_agents.py` (every example's worker built in a fresh
  process; `examples/chef-agents` against an OpenAI-compatible stand-in). The tests start
  each one as a worker process does, with `agents` not imported (`tests/conftest.py`).
  The agent that is a process needs none of this: `examples/chef-claude`, one heartbeating
  activity resuming the binary's session, `tests/examples/test_chef_claude.py`.

## I21. The Workers view (kontra-cloud#6)

A seller sees whether its workers keep up from what Temporal already knows: the gate asks
`DescribeTaskQueue` of an offering version's task queue in the seller's namespace, through
its internal client (no Authorizer change), and answers
`GET /v1/offerings/{party}/{name}/versions/{version}/workers` (`party|worker`) with a
`WorkersView` whose verdict is `kontra.workers`'s. Where the issue and the code part ways:

- **The route.** The issue's `{o}` is an offering, `party/name`: two path segments, as in the
  catalog's other offering paths. `{version}` resolves as a catalog path does (latest, `@1`,
  exact) among every version the caller published, retired and unconfirmed ones too (a
  retired version's workers may still be finishing its jobs). Another party's version is
  404 `offering_not_found`, like a missing one, and Temporal is not asked.
- **No `report_pollers`.** The issue names `DescribeTaskQueue(report_stats=true,
  report_pollers=true)`. `report_pollers` is deprecated in Temporal's proto and belongs to
  the deprecated `ENHANCED` mode; the default mode lists the pollers in every answer, so the
  gate sends `report_stats` alone, once per task-queue type (`TaskQueueType`: workflow,
  activity, nexus).
- **The SDK version comes from `ListWorkers`.** `PollerInfo` names no SDK; a worker's
  heartbeat does (`WorkerHeartbeat.sdk_name`, `sdk_version`, Temporal's names, kept as
  `PollerInfo.sdk_name` and `sdk_version` on the wire). When anything polls, the gate asks
  `ListWorkers` once with `TaskQueue = '<task queue>'` and matches each poller by identity
  (of two heartbeats with one identity, the worker started last). Server 1.32 keeps
  heartbeats by default (`frontend.WorkerHeartbeatsEnabled`; `ListWorkers` is always on) and
  the Python SDK sends one every 60 s, so a new worker shows its SDK within a minute; until
  then, and from a server without `ListWorkers`, both are null. `build_id` and
  `deployment_name` (what the worker polled with) are shown as well.
- **Strings are fitted, never refused.** A heartbeat's strings are the worker's own and
  unbounded, and a deprecated deployment series name is not bounded everywhere: a value
  longer than the wire carries is left out (an identity is cut), so the seller still sees
  its workers counted instead of a failed answer.
- **The verdict reads one answer.** "Growing for 5 min" is the age of the oldest backlogged
  task (`approximate_backlog_age`), not a history of reads. `backlog_increase_rate` is
  `tasks_add_rate - tasks_dispatch_rate`, as Temporal's SDKs derive it; a backlog dispatched
  from only as fast as it grows (zero) is "not shrinking -- scale up", never "draining".
  `stale` is Temporal reporting nothing at all, which is also what it reports for a task
  queue it unloaded after about five idle minutes.
- **The gate paces Temporal.** This is the first authenticated GET that reaches the
  internal frontend on every request (three RPCs, four when anything polls), which the
  sweeper's observations share: one answer per task queue serves every request for it for
  a few seconds, a request that arrives while one is asked waits for it, and a party may
  cause a bounded number of descriptions a minute (429 `rate_limited` past it), per gate
  replica.
- **The MCP server shows it.** The issue names it next to `kontra status --workers`, so
  `kontra mcp` has a ninth tool, `workers`, read-only, for the caller's own versions; the
  eight of §7.3 are unchanged (I5).

## I22. A job's contract stays readable

A job names the exact version it was hired under (`acme/enrich@1.2.0`), and its buyer checks
the delivery against that version's contract (§11.3). That version is a historical fact:
neither retiring it nor a later spec may make the job unreadable. The release of 0.2.0 did
both at once. The platform retired every version whose document spec v1 had sealed (with the
seller's `public_key`, I7), the catalog answered visible versions only, and the SDK checked a
document against spec v2 only, so every job hired before the release failed
`kontra job <id> --result` with "no such offering" (2026-10-16). Nothing was lost; nothing
could be read.

* **An exact version is answered retired too.** Both gates answer
  `GET /v1/catalog/{party}/{name}/{version}` (and its card) for an exact version with
  `retired: true`, once its endpoint was confirmed. A partial version, `latest`, search,
  `GET /v1/endpoints/{e}`, quotes and hires stay visible-only, so nothing points a buyer at a
  retired version again, and the SDK refuses one itself (`HireRefused` `offering_retired`).
* **A retired version is never rewritten.** Publishing another document under it is 409
  `version_retired`: its document is the only copy of the contract its jobs were hired under
  (a job stores the hash).
* **Spec v1 is read-only.** `spec/x-kontra.v1.json` is back, byte for byte.
  `validate_document(doc, read_only=True)` checks a block that carries `public_key` against
  it, and the key's checksum as 0.1.0 did -- not against v2 minus one member -- and applies
  every other rule and the hash check as before; the result says `spec_version=1`. Only a delivery (and a read of a retired version, below) is checked that way
  (`Kontra._job_contract`, with a cache of its own): publish, serve, quote and hire take
  spec v2 only, and the key a v1 document seals is never encrypted to.
* **A retired version is said to be retired, never an error to read.**
  The MCP `describe` of an exact retired version answers `retired: true` from that same
  contract (`_job_contract`), and `kontra pull` of one warns on stderr and still writes the
  client, reading a spec v1 document as a delivery does. Quote and hire still refuse it
  (`offering_retired`, with the exact version in `detail.offering`).
* A delivery made before 0.2.0 is encrypted to the buyer machine's key of then (the
  profile's `key` file), not to a per-job key, and that is the identity the SDK decrypts
  with first, so it needs nothing new.

## I23. Confirm it's you

Every change to who a party is and what it sells took a sign-in younger than ten minutes
(403 `stale_session`), so a person who had signed in an hour earlier was told, mid-click, to
sign in again -- which read like being signed out, and in the console was: its "Sign in
again" card ended the session. Reading keys and sign-ins took the same. The owner's decisions
(2026-10-16): reads need no step-up; a change asks the person to confirm it's them, in place,
once an hour per sign-in; and the confirmation must give nothing to someone who holds a
stolen session token.

* **Reads need a person, changes a confirmation.** Seam ② gains two auth kinds. `human` is a
  person's session of any age: `GET /v1/keys`, `GET /v1/sessions`, and starting and polling
  a confirmation; an API key's session gets 403 `fresh_login_required`, a worker token 401.
  `fresh` now means *confirmed*: the later of the session's sign-in (`auth_time`) and its
  last completed confirmation is less than the gate's `KONTRA_CONFIRM_TTL` (an hour) ago.
  That covers making and revoking an API key or a recipient, ending a sign-in, and
  publishing; `resolver` (splitting escrow) is the resolver list, then `fresh`, and the
  disputes queue is `resolver_human`, so a resolver reads it without confirming. An
  unconfirmed change is 403 `confirmation_required` (it replaces `stale_session`), whose
  detail, `ConfirmationRequired`, says how this session can confirm: `email`, `dev`, or
  `sign_in` (nothing here can; a new sign-in, which counts as a confirmation, will). The
  gate decides the 403 before the route does anything, so a client confirms and sends the
  same request once more, and only for a change (`fresh`, `resolver`), never a read.
* **A confirmation is a mailed link plus the code the asking screen shows** -- I19.2's
  device pattern, so a thief who starts one with a stolen session token gains nothing from
  the mail that lands in the victim's inbox. `POST /v1/auth/confirm` answers a `ConfirmStart`:
  an opaque id (`cfm_` + 32 random hex), the user code (for the asking screen only), when it
  was asked, how long the link lives (ten minutes) and how often to poll. The gate mails the
  party's own sign-in address, never one from the request: a link, never the code, and which
  sign-in it confirms (how it signed in, when, the first 12 hex of its id), so a person who
  did not ask knows which one to end. The link's page asks for the code; a GET consumes
  nothing; the POST consumes the link once; three wrong codes burn it (a submit that is not
  a code at all costs no attempt). `GET /v1/auth/confirm/{id}` answers only the session that
  asked (any other: 404), with the confirmation's status and the *session's*
  `confirmed_until`, so every tab waiting on that session resumes when any of its
  confirmations completes. Several may be pending; none cancels another.
* **A passkey is a confirmation too** (I24): in the console, one tap of a passkey the person
  added confirms the session that asked, exactly as the mailed link does; email stays.
* **Bound to its session, never a sign-in.** Completing a confirmation confirms the session
  that asked and no other, not even another sign-in of the same person. Its page never
  redirects, never hands a browser a session and never opens one: a confirmation is weaker
  than a sign-in and stays so. A copy of the same session rides that session's hour; that is
  what binding to the session means, bounded by refresh-token reuse detection and the
  15-minute access token.
* **A key never confirms.** There is no person behind it: an API key's session is refused
  every `human` and `fresh` route, starting a confirmation included, and the platform's
  `sessions` table cannot hold a confirmed key session (`ck_sessions_key_unconfirmed`).
* **An operator can ask again.** After a suspected compromise (or for a staging check) the
  platform's `python -m gate.admin require-confirmation <party> [--session <id prefix>]`
  (`FakeGate.require_confirmation` here) makes a party's sign-ins, or one, confirm before
  their next change, whatever is left of their hour: a change then needs a confirmation
  *later* than the request (`confirmed_at > confirm_required_at`). It signs nobody out, never
  moves `auth_time` and never touches a key's session. Clients need nothing new: the refusal
  is the same `confirmation_required`, and a poll's `confirmed_until` stays null until then.
* **Sign-in mail's limits.** A confirmation link is a magic link (one table on the platform),
  so its mail counts against every sign-in mail limit -- per address and requesting network,
  per client, and the per-address and global ceilings -- and one sign-in asks at most five
  times an hour. Only a live session can cause one; a stranger cannot.
* **The dev channel.** On a `KONTRA_DEV=1` gate a development sign-in (`dev-login`,
  `device:dev`) confirms at once, with no mail: dev login already signs in as anyone. An email
  sign-in on a dev gate still confirms by mail.
* **Only a person is asked.** The console opens a "Confirm it's you" panel over the page and
  finishes the click once the code is typed elsewhere. The CLI (`kontra publish`, `kontra
  keys create|revoke`, `kontra recipients add|revoke`) prints the code and waits when stderr
  is a terminal, then sends its request again; without one it fails with a hint to run
  `kontra confirm` first, so a cron job never makes the gate mail a person. `kontra confirm`
  asks on purpose, and starts a new hour. In code, `Session.confirm()` and
  `kontra.auth.confirm_session` do the same; a `Session` confirms by itself only on the dev
  channel (`confirm_if_instant`, so `kontra serve` on `kontra dev` publishes past the hour)
  unless it is given another `Confirmer` (`confirm_in_place` asks the person). The CLI keeps
  its hint for `stale_session` while gates without this decision remain.

## I24. Passkeys confirm it's you

Waiting for a mail is the slow part of "Confirm it's you" (I23). The owner's decision
(2026-10-16): a passkey -- Touch ID, Face ID, Windows Hello, a phone, a security key -- is the
step-up, in one tap, and email stays the fallback. Passkeys never sign anyone in: that would be
a second way into an account, and this one only shortens a way that exists.

* **A passkey confirms a session; it never opens one.** Completing a passkey ceremony sets the
  asking session's `confirmed_at`, as typing the mailed code does, and nothing else: the same
  hour, the same `fresh` rule, the same clearing of an operator's mark. A passkey in the wrong
  hands confirms only sessions those hands already hold, and ending them ends it. Sign-in stays
  email (or GitHub, or dev).
* **Seam ② gains a kind and six routes.** `recent` is `fresh` and less than
  `RECENT_CONFIRMATION_SECONDS` (300) since the session's sign-in or last confirmation, with no
  mark since; else the same 403 `confirmation_required` and `ConfirmationRequired`. It guards
  one route, starting to add a passkey (`POST /v1/passkeys/options`): a passkey can confirm
  every change after it, so a session confirmed 50 minutes ago -- perhaps stolen since -- must
  confirm again, now, to add one. The window is about eleven minutes in effect, not five: a
  ceremony started at 4 min 59 s lives six minutes, and finishing it is `fresh` (a finish
  refused for a lapsed window would strand a credential the authenticator already made). A
  client that confirms in place for a `recent` row waits for its own confirmation to complete
  (`confirm_session(extend=True)`), not for the running hour.
  Finishing (`POST /v1/passkeys`) and revoking (`DELETE /v1/passkeys/{id}`) are `fresh`;
  listing (`GET /v1/passkeys`) and confirming with one (`POST /v1/auth/confirm/passkey/options`,
  `POST /v1/auth/confirm/passkey`) are `human`. An API key's session gets 403
  `fresh_login_required` from all six, a worker token 401. No existing response model gained
  a field (they forbid extras, so 0.2.2 keeps parsing every answer); everything is new models.
* **The gate verifies, the console relays.** The browser's own JSON
  (`PublicKeyCredential.toJSON()`) travels as an opaque object of at most 32 KiB: typing its
  fields would refuse what a new browser adds. The RP ID is the console's host per environment
  (`app.kontra.run`, `staging-app.kontra.run`: neither under the other, so a browser never
  offers one's passkey to the other); answers must come from the console's origins on it, and
  never from the console framed by another site's page (`crossOrigin`, `topOrigin`: the console
  is never framed, so no such answer is expected); user verification is required at
  registration and at every assertion (a browser that ignores the option still fails at the
  gate); attestation `none`; EdDSA, ES256 or RS256.
* **Every ceremony is used once.** `wac_` + 32 random hex, bound to the session that asked and
  to its purpose (adding, confirming), six minutes, and used up by the first answer before the
  answer is looked at, whatever comes of it. Another session's, the other purpose's, a used or
  an expired one are one answer: 410 `ceremony_gone`. A refused answer is 400
  `passkey_rejected`, one answer whatever the reason (the reason is a log field).
* **A use count that does not move forward is a copy -- of a passkey on one device.** It is
  judged after the signature verified (`kontra._passkeys.judge_counter`), so a forgery is a
  refusal and a clone is a clone: for a passkey that is not backup eligible, the passkey is
  revoked (`counter`), every sign-in of the party must confirm again, the party is mailed, and
  the answer is 403 `passkey_cloned`. A synced passkey (backup eligible) lives in copies by
  design, every device of a password manager, and a manager that counts per device can answer
  behind its own other copy: that answer is refused (400 `passkey_rejected`, reason `counter`;
  the panel offers email) and nothing is revoked, marked or mailed, so a password manager's
  counting can never cost a person their passkey and their sign-ins' hour. A copy of a synced
  passkey is therefore not detected: the password manager is its boundary. Passkeys that count
  nothing (0 on both sides) carry no signal either way; backup eligibility must stay what it
  was at registration.
* **Revoking marks the other sign-ins.** Whoever held a revoked passkey may have used it to
  confirm one of them within the hour: revocation sets their `confirm_required_at` (I23's
  operator mark, widened), signs nobody out, and their next confirmation clears it. The person
  revoking is not asked again; a clone or an operator's `revoke-passkeys` marks every sign-in.
  A mark is never earlier than a sign-in's own last confirmation, so a confirmation that
  landed while the revocation waited for the passkey's row ends too. Known and left: the I23
  operator mark (`require-confirmation`) is set at the command's own time, so a confirmation
  committing in the same instant (or on a pod whose clock runs ahead) can outlive it; an email
  confirmation needs the person's mailbox and the asking screen's code, so no stolen sign-in
  can use that race.
* **Limits and notices.** Ten live passkeys per party; twenty ceremonies per session and purpose
  an hour; ten refused answers of a session in an hour stop its new ceremonies. A credential id
  is the gate's once, revoked ones included (409 `passkey_exists`). Adding a passkey mails the
  party's address which sign-in added it; every revocation mails it too -- by the person ("A
  passkey was removed", naming the sign-in that removed it, and to sign that one out if it was
  not them), by an operator's `revoke-passkeys` (the same, in the operator's words), or by the
  gate on a copy ("A passkey of yours was revoked"). A label is quoted with its own quotes
  escaped: whoever adds a passkey names it, and a name never speaks as Kontra. No link, nothing
  secret. `KONTRA_PASSKEYS=0` turns every ceremony off at once (stored passkeys stay); a gate
  whose console host cannot be an RP ID (an IP address), or with no console origin on its RP ID,
  has passkeys off.
* **One verifier, the same in both gates.** `kontra._passkeys` holds the ceremony's parameters
  and wraps `webauthn` (duo-labs py_webauthn, BSD-3-Clause); the platform imports it, as it
  imports `kontra._control`. The fake gate verifies real answers with it rather than a
  test-only token: what passkeys get wrong lives in the verification's inputs -- the RP ID, the
  origins, user verification, the challenge's binding, the counter and backup rules -- and a
  shortcut would exercise none of them, the drift seam ② exists to stop.
  `kontra.testing.SoftAuthenticator` makes those answers (real keys, real CBOR) for both
  repositories' tests, and `kontra.testing.passkey_contract` holds the scenarios both gates
  pass. The cost is test-only: `kontra-sdk[testing]` gains `webauthn` and its dependencies
  (pyOpenSSL among them, which caps `cryptography` below 51 there); `import kontra` and the CLI
  never load it.
* **The CLI gets no passkeys.** A terminal has no WebAuthn; its email path is unchanged, and the
  client methods are the console's (and a test's).

## I25. Share to my account

The owner's plan item (decision 8): *"Wrap each job's key to your account's keys so a job hired
in one browser reads in another, or in the CLI. It removes the last key chore."* Folded in:
*"Exact key per job on the job page: the gate returns a job's buyer key."* Per-job keys (I7)
made hiring need no key setup, but left a job readable only where it was hired: the console
offered "Job keys" export, restore and save-for-CLI, and the CLI said to copy the key file.
Nothing here touches Temporal, the hold token, the codec or the seller: the seller still
encrypts to the `buyer_key` signed into the hold. This decides only who else gets a copy of
the job key's secret half, and how.

* **"The account's keys" are a new per-party set, devices -- not the recipients.** The plan's
  phrase allows both readings; the recipients were rejected because they include fleet keys
  mounted on every seller worker pod (a compromised worker host would read everything the
  party ever bought), because a browser key registered as a recipient would receive every
  buyer's input to the party's offerings and add a stanza to each, and because recipients are
  public (`GET /v1/parties/{p}`), where a list of a person's browsers and laptops should not
  be. A device is `browser` (a key per browser profile and party, non-extractable WebCrypto
  X25519 where the browser has it) or `cli` (`kontra login`'s `~/.kontra/key`, also a
  recipient: two rows, two revocations, the same machine). Fleet keys are never devices; an
  API key's session is none either -- it wraps the jobs it hires to the devices and reads
  only those.
* **Seam ② gains six routes and only new models**, so 0.2.0-0.2.3 keep parsing every answer.
  `GET /v1/me/devices[?for=K]` (`party`, so a program's hire wraps to them; `for` marks the
  caller's own device used; an API key's session reads the keys, its labels hidden and no
  sign-in or use named -- a program needs the keys, not the names of a person's browsers),
  `POST /v1/me/devices` (`recent`, `AddDevice` -> `DeviceView`; the same live key again is the
  same view, a removed one 409 `device_revoked`, a bad key 422 `invalid_public_key`, more than
  `DEVICE_ADDS_PER_HOUR` (48) new ones an hour 429 `rate_limited` -- each mails the party),
  `DELETE /v1/me/devices/{key}` (`fresh`), `GET /v1/jobs/{id}/key`
  (`party`, the buyer only: a seller 403 `forbidden`, anyone else 404 `job_not_found`, as for
  `result`), `GET /v1/me/job-keys?lacking=D[&for=K]` (`party`; `D` a live device of the
  caller, else 404 `device_not_found`) and `POST /v1/me/job-keys` (`party`, at most
  `SHARE_ITEMS_MAX` (100) jobs, a body limit of 256 KiB on the platform). Under `/v1/me`,
  because `/v1/jobs/keys` would be read as the job "keys".
* **Registration asks nothing in the common path.** Adding a device is `recent`, the same as
  starting a passkey and for the same reason: a device also receives the keys of past jobs.
  Right after a sign-in that holds, so the console registers its browser and `kontra login`
  this machine without a prompt; the console's call is quiet (a 403 never opens the panel)
  and is retried after the next confirmation the tab completes. `kontra login --no-device` is
  the opt-out (a seller's box); a CLI login from before devices registers at its next
  confirmation (`Session.confirm`, the CLI's in-place confirmations, `kontra confirm`) unless
  it was declined, and `kontra devices add` does it on purpose. The gate mails the account's
  address every device added and removed, naming the sign-in, with no link.
* **A browser device that is gone retires by itself (review H1).** Every sign-in in a private
  window, a profile that deletes site data on close, or a new profile makes a key and adds a
  device -- useful on a borrowed machine, so such a browser registers (the design had it
  stay out; nothing tells such storage apart reliably) -- and nothing a person does removes
  them. So the gate *retires* a browser device nobody used for `DEVICE_IDLE_SECONDS` (30 days:
  every catch-up lists the devices `for=` itself, so a browser in use never idles, and a
  sign-in idle that long has expired anyway), and at `DEVICES_MAX` (32) live devices a new
  one takes the place of the browser used least recently, named in the new device's mail;
  only an account of 32 machines answers 409 `too_many_devices`. Retired is not removed: the
  device is listed nowhere and gets nothing, but keeps its copies, and its browser adds the
  same key again by itself at its next sign-in (quietly; `recent` as any add, no mail -- it
  was a device already, and a loop of comebacks mails nobody). A browser whose device left
  the list asks the gate, which answers which it was: back (retired) or 409
  `device_revoked` (removed by a person: never again). A `kontra login` machine never
  retires; one removed by mistake comes back with a key of its own, `kontra devices add
  --new-key` (`~/.kontra/device.key`), never a new `~/.kontra/key`, which is also a
  recipient.
* **A wrap is a job key encrypted to one device.** Its plaintext is the CLI's key file
  (`# job:`, `# public key:`, the `AGE-SECRET-KEY-1...` line), so `age -d -i ~/.kontra/key`
  opens one by hand; its ciphertext is age with exactly one X25519 stanza, at most `WRAP_MAX`
  (1024) bytes. One file per (job, device), never one with N stanzas: catch-up then adds a row
  and never rewrites anyone's, removal deletes only that device's rows, and a stolen session
  cannot replace the shared file. Rows are first-write-wins (`ON CONFLICT DO NOTHING`), never
  updated. The gate checks the shape only (`kontra._jobkeys.check_wrap_shape`); a reader
  unwraps, requires exactly one identity, and requires its public key to be *this* job's
  `buyer_key` from the gate, so a wrap moved to another job is useless.
* **Only a holder of the job key writes a wrap.** Without that, a stolen API key could post
  junk for a new device first and, first write winning, keep it from ever reading the job.
  `GET /v1/jobs/{id}/key` and every listed item carry a `challenge`: age-encrypted to the
  job's `buyer_key`, an HMAC over the job, the asking session and a ten-minute bucket, under a
  key derived from the gate's signing secret (`KONTRA_SIGNING_KEYS`; nothing stored). A share
  carries the opened `proof`; the gate recomputes it for this session in this bucket and the
  one before. Refused jobs come back by code (`job_not_found`, `not_shareable`, `bad_proof`,
  `invalid_wrap`), whole; a wrap to no live device, or one that exists, is skipped.
* **Exact key per job: `JobView.buyer_key`, as the plan item says -- sent only when asked
  (review M5).** Every released SDK parses answers strictly: `JobView` with a `buyer_key`
  (even `null`) fails `kontra job`, `kontra jobs`, `kontra hire` and every MCP job tool on
  0.2.0-0.2.3 (measured), and I24 made "no existing response model gains a field" a rule for
  exactly this. So the field is there, and `GET /v1/jobs/{id}` and `GET /v1/jobs` add it only
  on `?include=buyer_key` -- absent from the JSON otherwise, never `null` -- which the console
  asks for: the job page looks its key up by that name, with no trial and, when the key is
  kept here, no other call. And from 0.2.4 a client reads every answer ignoring the fields it
  does not know (requests stay strict on both sides), so once 0.2.3 and older need no
  serving, the gate may send it, or any new field, to everyone. `GET /v1/jobs/{id}/key`
  (`JobKey.buyer_key`, with the wraps and the challenge) is what a page without its key asks.
* **Catch-up: every online device serves the devices it has not served.** A device keeps,
  per party, the devices it has shown the person (*seen*) and served (*synced*). For each live
  device it has not served, itself included (its own jobs' keys are then kept for it too), it
  pages `lacking=D`, wraps every job whose key it holds or can unwrap from its own wrap, with
  the proof, and marks D served; a job no key here opens is left to the device that has it. A
  share that failed at hire is retried first. The console runs this at sign-in, when a tab
  becomes visible and every five minutes, in one tab per browser; the CLI in `kontra login`
  and `hire` within five seconds and in `jobs` and `job` within 1.5, each *after* it printed
  what was asked; `kontra mcp` every five minutes; a failure only notes. Where a device's
  listing stopped (the time, the network) is kept per device (`devices.json` `cursors`) and
  the next catch-up resumes there, so a backlog of jobs nobody here can open -- older
  browsers' jobs, a CI's whose keys are gone -- costs one pass in all, not every command
  (review M2). A page with a proof the gate refused (`bad_proof`: a signing key rotated, a
  bucket rolled over mid-page) leaves the device unserved and its cursor before that page:
  the next catch-up sends those jobs again (review L1). Old jobs are shared too: a device's
  first catch-up wraps every job key it holds. A device wraps only to devices it has shown
  the person ("X can now read your jobs"), at hire as in catch-up: the console in a notice,
  the CLI once on stderr, a library through `on_new_device` (by default a logged warning),
  and `kontra mcp`, which has no terminal anyone reads, in its next tool answer -- until it
  has, nothing is copied to that device from there (a hook that returns False; review L9).
  Honest limit: a new device reads an old job only once a device holding that job's key has
  been online since; until then the CLI says so (`JobKeyMissing`, reason `no_wrap`) and the
  job page too (it looks again by itself while it waits). A job that closed without a
  delivery reports its failure (`JobFailed`) wherever no key reached, as where it was hired
  (review M4).
* **In the SDK.** `kontra._jobkeys` (proof, challenge, wrap shape, wrap and unwrap, the
  notices) is shared by both gates and both clients, as `kontra._passkeys` is: the platform
  imports it. `kontra._devices` is the client's half: registration, sharing, reading a job's
  key from this device's wrap (written to `~/.kontra/jobs/<id>.key`, 0600, never over a key
  already there), and catch-up, with state in `~/.kontra/devices.json` and a share still owed
  as an empty `~/.kontra/jobs/<id>.unshared` marker (one file per job, so concurrent commands
  never lose one in a read-modify-write). `Kontra` shares a new job's key in the background,
  so a hire neither waits for nor fails by it, reads a job's key before decrypting its result
  or progress, and `Kontra.sync_job_keys()` is the catch-up. An API key's session shares and
  retries but neither catches up devices nor writes `devices.json`.
* **Removal.** `DELETE /v1/me/devices/{key}` marks the row removed (never added again), and
  deletes its wraps of the party's jobs in the same transaction (a share in flight holds the
  device rows it read, so a removal waits for it and then deletes what it stored: nothing is
  left for a sweeper); from then on no hire, catch-up or listing gives it one. What it already
  opened -- a key it unwrapped, a key it hired with, a wrap copied from a backup taken before
  -- cannot be recalled, and job keys are never rotated: the seller encrypted to them. The
  console, the CLI and the mails say so; removing a device does not end its sign-in.
* **I7's shredding, amended.** `kontra job --accept` shreds this machine's key *file* only.
  "The ciphertext the gate and the bucket hold is readable by nobody after accept" becomes
  "readable by the account's devices while the job exists" -- this machine included, when it
  is one: its copy at the gate stays, and a later `--result` here fetches it again, which the
  CLI and `Job.accept` say (review M3). A job key still reaches one job; a device key now
  reaches every job wrapped to it. A "forget this job's key everywhere" route (`DELETE
  /v1/jobs/{id}/key`) is possible follow-up work.
* **Never shared:** a job whose `buyer_key` is or was one of the buyer's recipients, revoked
  ones included -- every job from before per-job keys was hired with the party's login key,
  which also reads seller input and every older job. Its key route answers `buyer_key` and no
  wraps; listings leave it out; a share is refused `not_shareable`. The account's login key
  and the console's imported older key are never wrapped by the clients either.
* **What this does not fix.** The gate is the directory for devices, as it is for
  recipients: a compromised gate could list a key of its own and receive copies, though each
  browser first shows it to the person, and a session stolen within five minutes of a sign-in
  or confirmation can add a device that past jobs are then copied to (the party is mailed).
  Signed device sets -- a key per device, each addition co-signed by an existing device or a
  passkey assertion -- would close both, and are future work.
