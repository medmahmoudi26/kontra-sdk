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
* *Amended by I30:* the stream relay's Temporal subject, `relay/<namespace>/job-<32 hex>`, is
  constructed too (`relay_subject`), and the Authorizer compares it whole with the one it
  rebuilds from a request's namespace and workflow id; nothing splits it.

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

*Amended by I30:* two more kinds of token mean something on the public frontend, both minted by
the gate alone and carrying no permission: the stream relay of one job (its poll Update and
offset Query on that job only) and the worker controller (ten worker-deployment calls in the
platform namespace `kontra-system` only). The relay's rule reads two typed fields of the
decoded request, the workflow id and the handler name, and compares each whole with a value it
constructs; it parses nothing.

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

**Superseded by I29** (kontra-sdk#51): seller code imports `temporalio` directly and nothing
from `kontra`; `worker.py` is the one file that imports `kontra`. Kept as the record of why
the façade was tried.

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

**Superseded by I29** (kontra-sdk#51) in its derivation: no plugin or converter is derived
from what a workflow imports, and the `[agents]` extra is gone; a seller passes
`OpenAIAgentsPlugin` in `serve(plugins=[...])`. Its principle (no Kontra keyword changes what
workflow code may do) and its endpoint rule (`model_provider=OpenAIProvider()`) stand. Not the
I25 that `docs/security.md` and most "decisions I25" references mean: that one is "Share to
my account", below.

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

## I26. The workflow graph is read from history (kontra-sdk#40, #41)

Temporal keeps no definition of a workflow, only what each run did, so the graph page (#43) and
the mock runs of #42 read the structure back from a run's history. `kontra.graph.parse` turns
history JSON (`WorkflowHistory.to_json_dict()`, `temporal workflow show --output json`, the UI's
API) into `{workflow, nodes, edges}`, with a status, an attempt, a span and references per node;
`union` keeps the structure several runs share, `merge` paints a live run on it, `links` adds
Temporal UI and Grafana URLs and `mermaid` draws it as text. The module imports nothing but the
standard library at import time, so the fake gate, the CLI and a page server read histories
without loading the SDK. Checked against temporalio 1.34, server 1.32 (the dev server and the
time-skipping test server) and Temporal UI 2.55.0. Where the issue and the code part ways:

- **Edges are causal, not "batches chain".** Commands one workflow task made share a
  `workflowTaskCompletedEventId` and are one parallel group, as the issue says. Chaining each
  group to the next, though, draws edges from what a task never waited on: in
  `gather(chain(x), chain(y))` the second `b` would hang off the first, a fire-and-forget
  activity would precede everything after it, and the shape would change between runs with
  timing. A group's predecessors are the closes written between the previous group's first
  command and its own: what arrives while a task is in flight is written after that task's
  commands, so the window holds exactly what this task (and any empty task before it)
  observed. With no close there (a signal woke it), the previous group; for the first, `start`.
  A child's Started event and a timer the workflow canceled are nothing it waited on. The
  terminal event the workflow commanded (completed, failed, canceled, continued as new) leads
  to `end` from what it observed; a run terminated, timed out or still running has no edge to
  `end`.
- **A local activity splits its workflow task, and the end belongs to its task (where this
  departs from #41's spec).** The spec groups commands by task only and treats the end as a
  group of its own, and real histories (both servers, temporalio 1.34) show both rules wrong.
  A local activity runs inside its task, and sdk-core writes its marker when it resolves:
  `x = await local(); await b(x)` is the marker, then `b`, in one task; `gather(local(), b())`
  is `b`, then the marker. Grouped by task alone, `b` looked independent of the local activity
  and the local activity led nowhere, not even to the end the run waited on it for. So a
  group is what one activation commanded: a task's commands split before each command that
  follows a marker, and a marker is a close at its own place (just after itself), seen by the
  next group's window like any other. Two markers in a row stay one group: history cannot
  tell `gather(l1(), l2())` from `await l1(); await l2()`. And `workflow.start_activity(c);
  return x` writes `c` and the completion in one task: as a group of its own the end found no
  close in its window and fell back to `c`, a thing the run never waited on. The terminal
  event joins its task's last group (unless a marker comes right before it) and leads to
  `end` from that group's predecessors.
- **Identity is an occurrence index after folding repeats.** "Same activity and same
  predecessors" never settles on a loop -- the second iteration of `for x in xs: await a(x)`
  has `a` before it and the first has `start`, so it makes `a#2`, `a#3`, ... -- and it splits
  the join after an if/else into `ship` and `ship#2`. Instead each group's set of
  `(kind, name)` is its signature; tandem repeats of signatures fold, innermost first (a block
  of up to 16 groups repeated, and a last iteration cut short); and a node is one
  `(kind, name)` at one position of what is left: `name` at the first position holding it,
  `name#k` at the k-th. A loop is one node with a self or back edge, `a; b; a` is `a`, `b`,
  `a#2`, and a call site of straight code, or of a loop whose body does not branch, gets the
  same id in every run, which is what `union` and `merge` match by. `%`, `#` and `~` are
  percent-escaped in the name part, so `#k` and `~kind` only ever come from the suffixes: an
  activity named `a#2` is `a%232`, never the second `a` of another run. The accepted limits:
  `await a(); await a()` is a loop of two, since history cannot tell two call sites in a row
  from two iterations; a loop that ran once anywhere looks like straight code there; folding
  folds only iterations whose groups repeat exactly, so a loop whose body branches
  (`for x in xs: await a(x); if x.ok: await b(x)`) folds only where its iterations happened
  to match, its ids follow the branch pattern of that run (`a, b, a#2, b#2` for ok, no, ok;
  `a, b` for ok, ok, ok), and `union` and `merge` do not line its runs up; and a name two
  kinds share (a function run as an activity and as a local activity) stays with the activity
  within a run, the other being `name~kind`, while two runs that each hold only one of them
  give both the bare name.
- **A timer is `wait`**, labelled with its duration (`wait 30s`; the latest when a node's
  timers differ), so a polling loop's timer is one node.
- **A batch child is named from its own history, never from its id.** Every child of
  `execute_activities_in_parallel` is a `kontra.batch.v1`, and its id embeds the activity's
  name, but ids are constructed, never parsed (I1). Given the child's history (`children=`, its
  first run is enough), the node is `kontra.batch.v1[<its activities>]`, labelled
  `execute_activities_in_parallel(...)`; without it, `kontra.batch.v1`. Its activities are the
  item activity and the `pages=` activity its input names, when that input is plain JSON (a
  mock run's, which no codec encrypts; an encrypted one is never opened), else the ones it
  scheduled: a run whose batch got no items, or an empty first page, schedules no item
  activity, and would otherwise be a second node for the same call (found by #42's review,
  `kontra graph` on a `pages=` project). Both give the same name for a batch that ran items
  (`[enrich,fetch_page]`), so a live run's batch still matches the static one. A seller's
  own child workflow keeps its type.
- **`workflowTaskCompletedEventId` is compared by value, never looked up.** The dev server
  writes the WorkflowTaskCompleted event's id there; the time-skipping test server, which #42's
  mock runs use, writes the WorkflowTaskStarted event's, one before it. The same workflow gives
  the same graph from both, but for what the test server does not write: it leaves
  `retryState` off a workflow's and a child's failure (`retry_state` is then null, where the
  dev server's run says `retry_policy_not_set`), writes no Started event for an activity
  attempt that timed out, and no `taskId`.
- **One event in Temporal UI is a path.** UI 2.55.0 has no `?eventId=` parameter: a node links
  to `/namespaces/{ns}/workflows/{wf}/{run}/history/events/{id}`, a run to `.../history`. A
  closed child links to its last run (its close event names the last of its continue-as-new
  chain; its Started event names the first), an open one to `/workflows/{child}` with no run,
  which the UI resolves to the latest. Every segment is percent-encoded: a batch child's id
  holds `/`.
- **Trace ids are per command.** `TracingInterceptor` writes its carrier under `_tracer-data`
  (the issue's correction stands): a `json/plain` payload of `{"traceparent": ...}` that
  Temporal stores unencoded, since kontra keeps headers out of the codec. Each activity and
  child start carries its own trace; a run is one trace only when a traced client started it,
  and its own Started event then holds it (the workflow's `trace_id`). Timers and local
  activities carry none. A traceparent is read by an anchored full match, version `00` and the
  flags as written (`03` was seen, not `01`); anything else is no trace.
- **Payloads are a separate step, with a codec (seam ⑤).** The issue's `with_io=True` flag
  would put decoding inside the parser. Here `parse` emits no payload, encrypted or not --
  no input, result, heartbeat, memo, search attribute, termination reason, failure message,
  stack trace or encoded attributes -- and a failure is its kind (application; timeout, with
  its type; canceled; terminated; server; an activity or child failure is its cause's) and its
  retry state, the event's own: the `retryState` inside an `activityFailureInfo` the failure
  wraps is the activity's, and taking it when the event has none made the same run report
  `maximum_attempts_reached` on one server and `retry_policy_not_set` on the other.
  `with_io(graph, history, codec)`, async, decodes inputs, results and failure messages
  through the codec it must be given and Temporal's default converter, a payload the codec
  cannot open being `{"error": "undecodable"}`. With kontra's converter a failure's `message`
  reads `Encoded failure` and the real one is in `encodedAttributes`, which only `with_io`
  opens. The two payloads `parse` reads are records, not a party's data: `_tracer-data`, for
  the trace and span ids, and a local activity marker's sdk-core record, for its type and
  attempt.
- **Running and attempts come from `DescribeWorkflowExecution`.** Temporal writes no event per
  retry, and an activity's Started event (its attempt, its last failure) only when the activity
  closes, so history alone cannot show one running or retrying. `parse(pending=)` takes the
  answer's `pendingActivities`: an open activity whose state is started or cancel-requested is
  `running`, with the attempt, the last failure's kind and, in `t.last_failed`, when that
  attempt ended (`lastAttemptCompleteTime`; the pending `scheduledTime` is the next attempt's,
  and history's is the first). `with_io(pending=)` takes the same answer: a retrying activity's
  last failure, message included, is there alone until the activity closes (found by #43's
  review: the page said it was decoding a message that history did not have).
- **What a closed run left open ended with it** (the spec is silent). No event closes an
  activity, timer or child still open when its run closes, and left `scheduled` with an open
  span, a run closed days ago read as in flight and its log links queried up to now. Temporal
  drops a closed run's activities, timers and Nexus operations (an activity heartbeating after
  gets NotFound, which the SDK turns into a cancel), so they are `canceled`, or `terminated` /
  `timed_out` with a run the server ended so; a child follows its parent close policy
  (terminated by default, canceled on request cancel, still open when abandoned, since it may
  well be running). Their `t.closed` and span end at the run's close.
- **Local activities and Nexus operations are nodes.** A local activity is only a
  `core_local_activity` marker; other markers are no node. It runs inside its workflow task, so
  its marker is a close at its own place in the task, and what the task commanded after it
  is a group that follows it (above). A Nexus operation is `service/operation`, running from
  its Started event, which is no close either.
- **Edges are objects, and union and merge are two functions.** The issue's `merge` did both.
  `union(graphs)` is structure only: every node and edge of the mock runs, and nothing any run
  said (status, counts, times, references, failures). Its `group` is not one run's: a run's
  group is a position in that run, so an `if`'s two branches, taken at the same point by two
  runs, number alike, and would read as one fan-out. In a union two nodes share a group only
  when some run commanded them in one batch (or they are chained by such runs), numbered in
  node order (found by #42's review). `merge(static, live)` paints a live run
  on it: a live node with `reached: true`, a static-only one (a branch not taken, or not yet)
  with `reached: false`, and each edge `{"from", "to", "reached"}`. A live batch not named yet
  (its child has scheduled nothing, and its input is encrypted) is painted on the static
  graph's one batch whose predecessors include all of its, under that batch's name; with two
  such batches, or none, it stays `kontra.batch.v1` (found live by #43's review: the run drew
  an unnamed batch beside the static one, which read "not reached yet", for over a second).
- **Logs and traces are Grafana Explore panes.** A node's `logs` link is Loki,
  `{selector} | workflow_id="..." | run_id="..."` and `| activity_type="..."` for an activity
  (a child's logs are under its own workflow id), over the node's span with a second either
  side, as epoch-millisecond strings: Grafana 13.2 drops a numeric range. `trace` is Tempo by
  the trace id. A link is left out when its base URL or datasource is not configured or the
  graph lacks what it needs; `LinkConfig` takes http(s) base URLs only. A string no server
  writes (a lone surrogate: proto strings are UTF-8) is unreadable, like anything else
  malformed, so no link is built from one. `mermaid` writes `%` as an entity code as well as
  `#`, quotes, `<`, `>`, `&` and backticks: Mermaid applies a `%%{init: ...}%%` directive found
  anywhere in its text, a quoted label included, and an activity's name is a label.
- **Checked by** `tests/graph`: synthesized histories in both servers' shapes (`synth.py`
  builds them; a test holds the checked-in files to it, and another parses every one
  strictly into Temporal's own `History` proto, so no field a server does not write creeps
  in), one test per item of #41's "done when" and per identity case above, every fixture
  truncated at every event and damaged at random (hypothesis, including the `json/plain`
  payloads `parse` decodes) without `parse` raising, two 10,000-wide fan-outs and 50,000
  folded groups in bounded time (edges are built from each source and target once, and
  folding looks only where a block can repeat), and a history with a marker in every payload,
  one sealed result and a pending activity whose heartbeat and last failure carry it, of
  which no graph, union, merge, link or Mermaid text carries anything while `with_io` with
  the test codec shows it all. A codec that gives back more or fewer payloads than it was
  handed is undecodable there, never a null result.

## I27. `kontra graph` (kontra-sdk#42)

**Amended by I29**: an agent workflow's mock runs follow the `OpenAIAgentsPlugin` its worker
passes, not its `agents` import, and there is no `[agents]` extra; `examples/sweep` fans out
in plain Temporal, so its graph has one `check_headers` node instead of a batch.

`kontra graph` prints the static graph of the workflows `worker.py` binds, before anyone has
run them: for each bound offering it runs the workflow `--runs` times (default 8) in
Temporal's time-skipping test server, every activity answered by ONE dynamic stub with a
value of the type the seller's activity of that name declares, parses each run's history
with its children's (I26), and prints `graph.union` of the runs -- one line per edge, Mermaid
(`--mermaid`) or JSON (`--json`, always a list of `{offering, runs, graph,
never_scheduled}`). Activities the worker registers that no run scheduled, runs that did not
close, and return types no valid value was found for are warnings on stderr; the exit status
stays 0. With a profile in `$KONTRA_HOME`, each offering's union is also saved, atomically and
0600 in a 0700 directory, at `$KONTRA_HOME/graphs/<sha256(task queue)>.json` with the task
queue `kontra serve` uses for it (`kontra.contract.names` builds it, nothing here does), for
the graph page of #43 to paint a live run on; `load_static(home, task_queue)` reads it back
only when the stored queue equals the one asked for, and anything unreadable is None. The
values come from `kontra._sample`, an in-house sampler over the pydantic JSON Schema, by run:
0 smallest, 1 largest, 2 large lists near the top (201 items), then seeded random ones; the
same command prints the same graph. Every seller example's README carries its graph, which
`tests/graph/test_cli.py` regenerates and compares (`UPDATE_GOLDEN=1` rewrites them). Checked
against temporalio 1.34 and its test server. Where the issue and the code part ways:

- **The graph is not published (moved to #44).** #42 has `kontra publish` write the graph into
  the offering document and `kontra pull` show it. The contract document is closed and hashed,
  a sibling field is a lockstep change of seam ②, and the catalog is public while the graph
  names the seller's internals (activity names, children, loops). `kontra publish` and `kontra
  pull` do not change; the graph stays on the seller's machine.
- **A shipped example shows the three shapes.** No example had a fan-out, a batch or a branch,
  so #42's "done when" had nothing to point at. `examples/sweep` (a security-headers sweep,
  offline-tested over local sites like enrich) has `plan`, then `asyncio.gather` of two
  different activities (`fetch_robots`, `fetch_security_txt`), then one
  `execute_activities_in_parallel(check_headers, items=..., reduce=tally)`, then an `if` on
  what the batch found (`score` or `all_clear`), then `write_report`. The fan-out is one
  parallel group, the batch ONE node, `kontra.batch.v1[check_headers]`, labelled
  `execute_activities_in_parallel(check_headers)` with no continue-as-new chain, and both
  branches show from two runs; its lifecycle test parses the real job's history into the
  same path. Two things #42's review found in the parser serve these checks (I26): a batch
  is named from what its input asked for, so a run whose batch got no items (min mode draws
  `[]` for a list without `minItems`; a `Page`'s items never have one) is not a second node;
  and in a union only nodes some run commanded together share a group, so an `if`'s two
  branches, taken at the same point by two runs, do not pass for a fan-out. Every value of
  sweep's job is bounded in bytes, not characters, as the payload limit is: its URLs follow
  RFC 3986 (ASCII, the rest percent-encoded) and each kept line of a site's files is cut at
  100 bytes as JSON writes it (a control character is six there); a site sending lines of
  U+0001 had pushed `Findings` past 2 MiB on a Temporal without the codec, whose claim-check
  carries a payload over 256 KiB under `kontra serve`.
- **No hypothesis-jsonschema; an in-house, seeded sampler.** It has had no release since
  2024-02, does not read pydantic's 2020-12 output (`prefixItems` ignored, so invalid tuples;
  `format: uuid` unknown; recursive models raise), is MPL-2.0, and draws at random outside
  `@given`, where "both branches with 4 runs" would be flaky. `kontra/_sample.py` is pure,
  pyright-strict, adds no dependency, and draws by run mode. Recursion is bounded where it
  recurses: once a `$ref` repeats on the way down, the rest is drawn in min mode, and past 32
  path elements nothing is. A first cut drew everything from 4 path elements down in min mode
  (array indexes count), so `report.pages[i].headers.hsts` was false in every run and an `if`
  on it never showed its other branch (#42's review); 16 elements now bound only the size of
  a value nested that deep without recursing. Past the issue's list: a
  `pattern` with no `examples` or `default` is followed by a generator over Python's own
  regex parser (enrich's and legacy-actor's inputs and `Artifact` have patterns, and giving up
  on them would contradict sampling the input), giving up only on a lookaround, a
  backreference or a conditional; a mapping's keys follow `propertyNames` and
  `patternProperties` (`dict[Literal[...], X]`, as sweep's report has; found by `kontra
  graph` on it); a value validation refuses is drawn again, in random mode, only where the
  errors point, for up to 32 rounds (legacy-actor's "exactly one of url or host", across 201
  targets, never passes a whole redraw), in the order of the errors (a set's order follows
  the process's string hashing, and two refused sibling fields came out swapped from one
  process to the next), a refused mapping key redrawing its mapping with fresh keys, and a
  datetime's offset a `NaiveDatetime` refuses taken off in place; a pattern's draw meets its
  `minLength`/`maxLength` (extra repetitions go to its first repeats with room), a class no
  printable character matches (`\s`) draws a space, and each pattern is read once, its
  classes' characters worked out once (a large-mode list of `Artifact`s took 1 s, long
  enough to starve the run's deadline; the stub now also draws off the event loop); `bytes`
  and `SecretStr` are strings of their lengths, base64 the encoding of one, random numbers
  have two decimals (a `Decimal`'s `decimal_places` and `max_digits` are not in its schema);
  anything but `Unsampleable` a draw runs into (an overflowing `multipleOf`, a validator
  raising `TypeError`) is unsampleable or drawn again, never a traceback; and values are
  checked with `validate_json`, as the payload converter decodes them, and the stub returns
  the drawn JSON itself (`dump_python` writes field names, not aliases, and can fail to
  round-trip). In large mode an array inside
  an array stays at 3 items (201 x 201 is no graph). A `Page`'s `next` is `Any`, which
  samples as null, so a `pages=` source always ends after one page.
- **The seller commands' shape: `--dir` and `--offering`, not `<module:Workflow>`.** The
  examples import their siblings by top-level name, and only `load_worker` puts the project
  on `sys.path`; `worker.py` already names the workflow, its activities, the extra workflows
  and the input type. Every bound offering by default, like `publish`.
- **The input is sampled too, or given.** enrich's and legacy-actor's loop counts depend on
  the input's size, so a fixed input would hide the loop. `--input FILE|-` starts every run
  with one value, checked against the input type first (`error: ...`, exit 1).
- **Agent workflows get a scripted model.** A workflow that imports `agents` (I25) keeps the
  agents plugin, with a `model_provider` whose model calls every function tool it was given
  once, in one response, then answers with a value of the agent's output type (or a short
  text): chef-agents' model, `lookup_pantry`, model, with no network and no key. Without the
  `[agents]` extra such a project cannot be imported, and the command says so (exit 1).
  The seller's MCP servers, handoffs (never taken), and the worker's other options, plugins
  and interceptors are not carried into the mock runs; no example needs them.
- **`examples/recon` is buyer code.** It hires offerings from its own workflow and binds
  none, so it has no `worker.py`; `kontra graph --dir examples/recon` fails with the line
  every seller command prints for a directory without one, and the tests say why.
- **Children's histories count.** Each `ChildWorkflowExecutionStarted` event names a child
  (its id from the event, never built or parsed, I1); its first run's history is read, its
  own children's too, and handed to `parse(children=)`: that names a batch node, and the
  activities a child ran (a batch's items) count as scheduled for the never-scheduled
  warning, as do local-activity markers (`tests/graph/projects/nested` holds both).
- **Never scheduled is the worker's, said once.** A worker's activities are every bound
  offering's, and one offering's runs never schedule another's: per offering, each one's
  warning listed the others' activities as unreached branches (#42's review). The warning
  lists what no run of any offering scheduled, once, naming the offerings (and, with
  `--offering`, that the ones left out ran nothing); `--json` gives each offering that list.
- **What does not close is timed out or terminated, and still drawn.** A wait no timer ends
  (a signal, `wait_condition` without a timeout) never reaches the wall-clock deadline: the
  test server skips to the run's execution timeout and times the run out within
  milliseconds, so that is its own warning ("waited for what no timer brings"). A run still
  open after `--timeout` wall-clock seconds (default 5) is terminated; so is one whose
  workflow task fails on the mock values, found by reading its history every 0.1 s (it would
  fail the same way on every retry). Either way its history up to there carries its
  structure, and it goes into the union. The warning says what is known, that the run was
  still open: the stub answers an activity the same way every time in a run, so a poll loop
  that sleeps between calls (`while not (await status()).done`) runs until then in every run
  that drew `done: false`, and a loop over a drawn count runs 1000 times in run 1. The first
  wording blamed "a loop that never waits on a timer" for both (#42's review). Answers are
  not varied call by call: calls of one run race (a batch's items), so which call got which
  value would change from one command to the next.
- **One worker per offering, on its real task queue; the runs in sequence.** One test server
  per command, a plain `temporalio.worker.Worker` per bound offering on the task queue
  `kontra serve` would use, with the workflow, its extra workflows and `kontra.batch.v1`, the
  stub (temporalio's `defn`, so it never lands in kontra's registry), Kontra's sandbox runner
  and the converter `serve()` picks, without the codec. Runs are sequential and the stub
  knows the run in progress, so a batch child's items draw in the parent run's mode.
  temporalio's logging, the sandbox's late-import warning and the core's WARN lines are
  silenced while they go: what matters is in the warnings.
- **The test server is downloaded once, into a directory only the user can write
  (departs from #42's spec).** The spec has it fetched "into the temp dir", temporalio's
  default, and temporalio runs whatever file sits at its predictable name there
  (`/tmp/temporal-test-server-sdk-python-1.34.0`), with no check of owner or checksum: on a
  shared machine, another user who creates that name first (or after a reboot cleans `/tmp`)
  runs code as whoever runs `kontra graph` (#42's review planted one). It goes to
  `$XDG_CACHE_HOME/kontra/temporal-test-server` (`~/.cache` without it), made 0700, and the
  command refuses a directory it does not own or anything in it that is a link, someone
  else's, or writable by others. Offline, before the download, the command says it cannot
  start it, and the tests skip. The server takes a port and nothing else, so it cannot be
  bound to the loopback: it listens on every interface, unauthenticated, while the command
  runs (an `--input` is readable there, and the docs and `--help` say so). SIGTERM and SIGHUP
  take Ctrl-C's way out while the runs go, so the server is shut down with the command;
  before, Python died without leaving `async with env`, and the server stayed, orphaned and
  listening.
- **Found on the way: the batch child imported seller modules inside the sandbox.**
  `execute_activities_in_parallel`'s child resolved its activity and `reduce=` with
  `importlib.import_module` inside `imports_passed_through()`, which goes around the
  sandbox's `__import__`: the module was executed again inside the child's sandbox, and
  sweep's `activities.py`, which reads `SWEEP_ALLOWED_PRIVATE_NETWORKS` from `os.environ` as
  it loads (as enrich's does), failed the child's every workflow task. `kontra serve` runs the
  same sandbox, so a real job would have hung the same way. `_resolve` now imports through
  `__import__` and reads `sys.modules`: passed through, as it always said
  (`tests/parallel`, an activity module that reads `os.environ` at import).

## I28. The dev graph page (kontra-sdk#43)

`kontra dev up --ui` prints `graph http://127.0.0.1:<codec port>/graph`: one page, shipped as
`kontra/cli/_console.html` (no build step), served by the codec server process
(`kontra/cli/_console.py`), that lists the dev party's latest 50 runs and draws one as a
layered SVG graph: `kontra graph`'s static graph for the run's task queue with the live run
painted on it (`graph.links(graph.merge(static, live) or live)`, I26, I27), repainted over
server-sent events while the run is open. A node opens a pane with its attempts, timing, task
queue, failure, its decoded input, result and failure message, and links to its event in
Temporal's UI, its lines in Loki and its trace in Tempo through Grafana's Explore.
`kontra serve --otlp` exports the worker's Python logs beside its spans, each line tagged with
the Temporal context it was logged in, and `--observability` gains a collector, Loki and Tempo
for them. Checked against temporalio 1.34, Kontra's Temporal server (1.32) and the CLI's dev
server, Temporal's UI 2.55.0, Grafana 13.2.3, Loki 3.7.8, Tempo 3.1.0, collector-contrib
0.163.1 and headless Chromium (Playwright 1.58), end to end on a real `kontra dev up --ui
--observability` with `examples/sweep` served and hired. Where the issue and the code part
ways:

- **The codec server serves the page, not "the fake gate".** `kontra dev` runs the real
  kontra-gate image (the fake gate exists only in pytest), and the SDK cannot add routes to
  it. The codec server is already SDK code, loopback-only, dev-profile-only and
  Host-checked, and holds this machine's key; it gains a dev session (`Session.load` of its
  home, `connect_temporal("worker")`, which keeps the token fresh) to read history in the
  party's own namespace. The Authorizer already allows a worker token
  `DescribeWorkflowExecution`, `GetWorkflowExecutionHistory` and `ListWorkflowExecutions`
  there: no seam ① change. One event loop on a daemon thread owns the session and the client,
  connected at the first request that needs them, so the server starts without Temporal and
  `/decode` answers while Temporal is down; request threads hand it coroutines and wait 30 s
  at most; an error is a JSON `{"error": ...}` of one line, in Temporal's or the profile's
  words or the error's type alone, never a traceback nor an exception's text that could hold
  a payload. Its routes take the console's own origins (`http://127.0.0.1:P`,
  `http://localhost:P`) and no other, and a cross-site `Sec-Fetch-Site` on a data route is
  refused; `/decode` and `/encode` keep the UI's origin alone, and a preflight is answered for
  those two only.
- **Payloads only through `/api/io`, and only for the page.** `kontra.graph.parse` emits no
  payload, so `/api/graph`, its event stream, `/api/workflows` and the page carry no input,
  result, failure message or heartbeat, encrypted or not; the page never receives ciphertext.
  `/api/io?w=&r=&node=` decodes the latest occurrence of that one node (`node=` empty: the
  workflow) with `graph.with_io` in this process, through a codec that shows what this
  machine's keys cannot open as `{"encrypted": true}` and a claim-check reference as
  `{"claim_check": true}` (this process holds no gate session to fetch it) -- never their
  bytes. It requires `X-Kontra-Console: 1`, which a page on another origin cannot send without
  a preflight, and the preflight is refused. "Without the codec server reachable, the page shows
  no payload bytes" (the issue's seam ⑤ item) holds trivially: without it there is no page. The
  seam ⑤ scan is `tests/codec/test_codec_console.py`: a seller's worker on a real dev server
  runs a workflow whose input, heartbeat, failure message, result and batch items carry a
  marker; the page, every graph read while it ran, the whole event stream and the runs list
  hold neither the marker in any case nor any payload's ciphertext, as base64, URL-safe base64,
  hex or a 32-character start of each; `/api/io` returns it decoded only with the header and
  the right `Host` (wrong `Host`, the UI's or a foreign `Origin`, a cross-site fetch, another
  header value and a preflight are refused with none of it). `with_io` shows a failure whose
  encoded attributes decode to something other than a message and a stack trace as what they
  decoded to, so an undecryptable failure reads `{"encrypted": true}`, not "no message".
  Every answer is `Cache-Control: no-store` and `nosniff`; the page has a policy with a
  per-response nonce (`default-src 'none'; script-src 'nonce-…'; style-src 'nonce-…';
  connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none';
  frame-ancestors 'none'`), `Referrer-Policy: no-referrer` and `X-Frame-Options: DENY`; it has
  no style or handler attribute, loads nothing from elsewhere, and puts every string from the
  server in with `textContent` or `setAttribute`, never as HTML.
- **No `kontra.yaml`.** The issue's `console:` keys exist nowhere. `kontra dev up` resolves the
  UI's and Grafana's URLs from the compose configuration it ran (`_published()`) and hands them
  to the codec server (`--temporal-ui`, and `--grafana` with `--observability` only); the data
  source uids are the provisioned ones (`loki`, `tempo`); the LogQL selector is
  `{service_name="kontra-worker"}`, or `OTEL_SERVICE_NAME` as `kontra dev up` saw it.
  The production console's per-fleet Grafana (the issue's note on the recipients fleet entry) is
  kontra-cloud's.
- **Logs and traces get backends.** The issue links logs to a Grafana that had nothing to
  show: `kontra serve --otlp` exported spans only and `--observability` had no log or trace
  store. The profile gains an OpenTelemetry Collector (OTLP/gRPC on
  `127.0.0.1:${KONTRA_DEV_OTLP_PORT:-4317}`, traces to Tempo, logs to Loki's OTLP endpoint),
  Loki and Tempo, pinned by digest and held to the stack's walls, on a `telemetry` network
  shared with Grafana alone. Grafana provisions Loki and Tempo beside Prometheus and its
  anonymous user is an Editor: Explore needs `datasources:explore`, which Grafana 13 gives a
  Viewer only through the deprecated `viewers_can_edit` (it warns that a non-Viewer anonymous
  role is deprecated; a later Grafana may take Explore away from these links). Alerting and
  external snapshots are off, so an Editor cannot make it send anything out. Loki is 3.7.8, not
  the 3.6.18 the spec named: 3.7 is the newest stable line, and its write-ahead-log disk guard,
  which refuses every push above 90% disk use (nothing arrived on a 94% full disk), is moved
  to 98%; 3.6.18 has a later Go security bump. The collector's exporters are `otlp_grpc` and
  `otlp_http`, the 0.163 names. `--otlp` exports the worker's logs at INFO and above through a
  handler and a log provider of its own (never the global ones) with the tracer provider's
  resource (`OTEL_SERVICE_NAME`, else `kontra-worker`, now the spans' default too), each record
  carrying `workflow_id`, `run_id`, `workflow_type`, `activity_id`, `activity_type`, `attempt`
  and `task_queue` from temporalio's logger extras or `activity.info()`, and the span's trace
  and span ids. Not OpenTelemetry's `LoggingHandler`: deprecated in opentelemetry-sdk 1.45,
  it exports every logging extra, and temporalio's are dicts, which OpenTelemetry refuses with
  a warning. **Seam ⑤ policy:** logs are the seller's own output to the seller's own stack, so
  they are neither scrubbed nor marker-scanned (spans still are); `docs/sellers.md` says so.
  `examples/sweep` logs a line per step, in counts and statuses, never a URL or a file's text.
- **Live attempts come from `DescribeWorkflowExecution`.** History says nothing of an
  activity's attempts or that it started until it closes, so the stream asks
  `DescribeWorkflowExecution` about once a second and sends the graph again (with its pending
  activities, I26's `pending=`) when the history's length, the status, the close time or a
  pending activity's state, attempt, start or last failure changed -- never for a heartbeat. A
  retry's `×n` and a running activity show before history has them, and the stream ends with
  `event: end` after a closed run's graph; it says `: keepalive` after 15 quiet seconds, sets
  EventSource's `retry` to 3 s, ends (and EventSource reconnects) on an error midway, and at
  most 16 are open at once (503 beyond). A batch node is named from its child's history (its
  input is encrypted, I26), which names nothing until the child schedules an activity and then
  grows while the run's history does not: while one is unnamed the stream reads the graph
  again at every poll and sends it when it differs, and `merge` paints it on the static batch
  after the same steps meanwhile (I26). Closed batch children's histories are kept, 256 at
  most; 64 are read per graph. **Where #43's spec falls short:** it has `/api/io` read
  `graph.with_io` of history alone, and a step backing off between attempts has no failure in
  history until it closes, so its pane said "Decoding the message…" for as long as it
  retried; `/api/io` passes the same `DescribeWorkflowExecution` answer (`with_io(pending=)`),
  and a step still open with no message yet says so. A step backing off counts from when its
  last attempt ended (`t.last_failed`), not from its first schedule.
- **The static graph is `kontra graph`'s, matched by the exact task queue.** Not one cached per
  `name@version` by the page: `load_static(home, task_queue)` with the task queue
  `DescribeWorkflowExecution` reports, which the cache file stores (the file name is only its
  hash, I27). Without one the graph says `static_graph: false` and the page says to run
  `kontra graph --dir <project>`. `merge` now keeps a static-only node's group apart from the
  run's: the run's groups are its positions and the static graph's are `union`'s, and an
  untaken `if` branch numbered like the taken one was drawn live as one parallel group with it.
- **Explore's links take string ranges and its event path.** Explore drops a numeric `range`,
  so `from`/`to` are epoch milliseconds as strings, a second either side of the node's span
  (`now` while it is open); the Logs query is the selector with `workflow_id`, `run_id` and,
  for an activity, `activity_type` (a child's lines are under its own workflow id). The
  Temporal link is the UI's event path, `/namespaces/<ns>/workflows/<id>/<run>/history/events/<event id>`
  (a child: its own run's history, or its id alone while open, which the UI follows); Trace
  is Tempo by the trace id `--otlp` writes into the command's `_tracer-data` header.
- **The page is not in `kontra/templates/`**, which is `kontra init`'s scaffold. Its layout is
  the longest path from `start` over forward edges (back edges found by a DFS from `start` and
  drawn as return arcs), vertical under 640 px or when it does not fit, with parallel groups
  boxed, start and end circles, double-bordered children, rounded timers, status classes and
  `×n`/`∥n` badges, and keyboard-focusable nodes. The preview's page had no doctype (quirks
  mode) and showed placeholders as raw JSON; it now has a doctype, says what a placeholder
  means, and says so when the stream is refused or the codec server goes away.
- **Found on the way: Temporal's UI 2.55 could not reach the codec server.** It posts to
  `/decode?preserveStorageRefs=true`, and the server compared the whole request target with
  `/decode`: every decode was a 404 and the UI said "Codec Server could not connect", showing
  ciphertext. Routes are compared without the query now (the server fetches no claim-check
  reference whatever the query says), and `tests/cli/test_codec_server.py` posts what the UI
  posts.

## I29. Sellers write native Temporal; `worker.py` is the only kontra import (kontra-sdk#50, #51)

Decisions 1, 2, 3, 4 and 12 of kontra-sdk#50. **Supersedes** D1 as I20 amended it, and the
first I25's derivation of the agents plugin from the workflow's imports; **amends** I27's
lines on agent workflows and batches in mock runs. A seller's `workflows.py` and
`activities.py` are vanilla `temporalio`: they import Temporal themselves and nothing from
`kontra`. `worker.py` is the one file of an offering that imports `kontra`; besides it,
`quote.py` takes `Quote` from `kontra.serve` and `models.py` may use `kontra.types`
(`Artifact`). Delete `worker.py` and the rest runs on any Temporal worker, unmodified --
all but `Artifact` I/O (`kontra.types`), which needs the Kontra runtime that `worker.py`'s
plugin installs in the process; an offering without `Artifact` needs nothing.

- **Why.** The façade was never a security boundary. What constrains a seller does not care
  what their code imports: the Authorizer (seam ①: deny by default, a token scoped to the
  seller's namespace), the codec (everything a buyer sees, encrypted to the job's keys), the
  contract hash (a worker serves only code whose contract the catalog holds), `names.py`
  (service, task-queue and workflow-id strings constructed, never parsed back) and escrow
  (I6, I16). All five are untouched. What the façade cost: an import path that is not
  Temporal's, so no Temporal doc, sample or plugin could be pasted in as written; a mirror
  that is never complete (contrib modules, plugins, exceptions); and Kontra's additions mixed
  into Temporal's namespace. I20's own reasons go too: the dependency closure is a tested
  range now, and the publish scan it enabled is dropped (both below).
- **`kontra` exports only what Kontra defines**: `serve`, `Worker` and `KontraPlugin`, each
  loaded on first use (so `import kontra` and `kontra.types` load no Temporal), and
  `__version__`. `kontra.workflow`, `kontra.activity` and the `RetryPolicy`,
  `ApplicationError` and `Page` re-exports are deleted. `kontra.workflow` (and the other four)
  raises an `AttributeError` naming the replacement (`from temporalio import workflow`, ...);
  `from kontra import workflow` gets Python's own `ImportError` ("cannot import name"), which
  drops a module `__getattr__`'s message.
- **Activities are found by a scan of the project.** `temporalio.activity.defn` notes nothing
  anywhere, so `serve()` registers every module-level function it stamped
  (`__temporal_activity_definition`) in a module loaded from under `worker.py`'s directory and
  defined in that module (a function one module imports from another counts once, in its
  own), skipping dot-directories, `tests`, `test`, `venv`, `node_modules`, `__pycache__`,
  `build`, `dist`, `site-packages` and `kontra` itself, in import then definition order; then
  the activities a project module binds that a function of another project module made (a
  factory's, whose own module never binds them); then the `activities=` passed, by identity,
  none twice. A method still needs `activities=[obj.method]`, and so does an activity defined
  or made outside the project. Two activities of one Temporal name are a `BindingError` naming
  both, never the last one silently winning. `KontraPlugin` and a `Worker(...)`
  written out discover nothing: they register what they are given.
- **`serve(entry, /, *, quote=None, **worker_kwargs)`.** `entry` is a `@workflow.defn` class or
  one `@agent.accepts` handler of an agent written with Temporal's agent harness
  (`serve(ReconAgent.sweep)`, below); passing the agent class itself is a `BindingError` with
  that fix-it. Every other keyword is `temporalio.worker.Worker`'s, verbatim: `workflows=` for
  the children the entry starts (`serve(Flow, Child)` is Python's own `TypeError` now),
  `activities=` after the scan, and `plugins=`. `offering(path, entry=..., quote=...)` binds the
  same two kinds (`workflow=` stays, as that keyword for a class). The contract is derived by
  `kontra.contract`, unchanged, from the entry's type hints: for a handler, its one pydantic
  message (parameters the harness injects, `Annotated[...]`, are skipped) and its pydantic
  return, cross-checked with the models the agent declares for it. An agent whose init data
  is required is refused: Kontra has nothing to pass, and init data is no part of the
  contract. A handler of a class defined in a function or in `__main__` is refused, as a
  workflow class is. So is a handler another agent class inherits unchanged: `ReconV2.sweep`
  *is* `ReconV1.sweep`, the class is found from the function's `__qualname__`, and a function
  cannot say which class it was read through, so `serve(ReconV2.sweep)` would serve
  `ReconV1`. Refused rather than guessed, with the fix (override the handler in each agent
  class that sells it); an explicit `entry=Agent, handler="name"` form was not added.
- **`KontraPlugin` (`kontra.plugin`) is everything Kontra adds to a seller's Temporal**, as one
  `temporalio.plugin.SimplePlugin` (client and worker plugin) named `kontra.KontraPlugin`:
  - *connection*: the profile resolved (or `KONTRA_API_KEY`), its key checked against the
    party's recipients, the profile's Temporal address, TLS and `worker` JWT (refreshed in the
    background), the process's Kontra runtime, and the temporalio range. The `target_host`
    passed is replaced: `kontra.plugin.TARGET` (`"kontra"`) is the placeholder, anything else
    is logged.
  - *client*: the profile's namespace, and the codec. Kontra's codec is a payload *codec*, so it
    stacks under any payload *converter*: one an earlier plugin set is kept
    (`OpenAIPayloadConverter`, the harness's), temporalio's default is replaced by pydantic's.
    Refused, with a `ServeError` naming it: another payload codec; a failure converter other
    than temporalio's default (Kontra installs its own, which encrypts failure messages);
    external storage whose threshold is at or below the codec's claim-check threshold plus
    4,096 bytes, which would offload a job's payload where its buyer cannot read it (the
    harness's 1.5 MB default is fine). Headers stay out of the codec: any other
    `header_codec_behavior` is replaced, with a warning, since the job header must be read
    before any payload key is known.
  - *worker*: a worker whose `workflows` hold an offering's entry class (the workflow, or the
    agent of a bound handler) serves that offering: the task queue from its `name@version`
    (the one passed is replaced), the entry first, the generated Nexus service and Kontra's
    interceptor prepended, Temporal's sandbox with `kontra` passed through (a seller's own
    `SandboxedWorkflowRunner` gets the same; any other runner is theirs, untouched), and for a
    handler the turn wrapper and its activity. Any other worker of the client is left alone.
    Several offerings bound to one class (versions of the same code: `tests/hire` serves
    three of `TextFlow` from one worker) are told apart by the task queue the worker names,
    compared with each offering's constructed one; a worker naming none of them is refused.
    Refusing a second offering of a class outright, the first plan, would have broken that.
  - *start*, once per plugin, in `run_worker`: the gate's JWKS for the hold check, the hash
    lock (each contract hash against the catalog's; a `kontra dev` profile publishes an
    unpublished version) and the progress forwarder, all moved there from `Worker`, so a
    hash-lock refusal comes out of the worker's run, not out of `prepare()`. Then the worker's
    final config is checked: a plugin given to the worker alone runs after Kontra's
    `configure_worker`, and a changed task queue, a dropped Nexus service, another interceptor
    first, an entry class no longer registered (a same-named swap included), or a client
    whose converter is no longer what `configure_client` made (Kontra's codec and failure
    converter, no offloading storage, headers unencoded) is a `ServeError`. The worker is
    claimed for that check from the config it was built with as well as its final one, and
    by Kontra's own Nexus service or interceptor on it, so a plugin cannot get past it by
    moving the entry class. The client is checked the same way in `configure_worker`: a
    client plugin placed after Kontra has the last word on the client. A start check that
    fails fails the workers starting with it and is then forgotten: a seller's main that
    runs the worker again checks again. A revoked session is diagnosed as before.

  The constructor does no I/O. `await plugin.prepare()` (idempotent; the connection hook runs
  it) checks the range, opens the session and builds the codec, and `aclose()` releases them.
  A client built as `Client(service_client, plugins=[kontra])` skips the connection hook: its
  `configure_client` without `prepare()` is a `ServeError` saying so.
- **Plugin order: Kontra last.** `kontra.serve.Worker` stays the process runner (the CLI's
  capture, `kontra.testing`'s Marketplace, signals, `wait_ready`), built on the plugin:
  `Client.connect("kontra", plugins=[*client_plugins, kontra])`, then one
  `temporalio.worker.Worker` per offering on that client. A seller plugin that is a
  `temporalio.client.Plugin` (any `SimplePlugin`: `OpenAIAgentsPlugin`, the harness's
  `AgentHarnessPlugin`, `ParallelPlugin`) goes on the client, in the seller's order, before
  Kontra's: the converter it chose stays, and the last word on the connection, the namespace
  and the codec is Kontra's. A worker-only plugin goes to the inner workers, and the start
  check above holds it to what Kontra set.
- **The escape hatch: `KontraPlugin.from_profile(entry)`.** A seller with a worker main of
  their own writes `Client.connect("kontra", plugins=[*theirs, KontraPlugin.from_profile(SweepFlow)])`
  and a plain `temporalio.worker.Worker(client, task_queue="kontra", workflows=[SweepFlow],
  activities=[...])`, and never touches the CLI. **Where the issue and the code part ways:**
  the issue sketches `from_profile()` with no argument; the entry is required, because the
  contract, the task queue and the generated Nexus service all come from it, and finding it
  in the yaml (D11), by convention or by a decorator in workflow code is exactly what the
  issue rejects. `from_profile` binds `offering.yaml` (and `quote.py`, unless `quote=`) beside
  the calling file, as `serve()` does; it discovers no activities.
- **A bound handler: one hire is one turn** (`kontra.serve.turn`). An agent workflow cannot be
  the job: its `run` returns nothing, and only after `close`; a handler's reply is an event on
  the agent's workflow stream, not an update result. So the generated `execute` starts
  Kontra's wrapper workflow, `kontra.turn.v1`, as the job (`job-<id>`, exactly as for a
  workflow entry, so escrow, the codec's buyer rule and the job header are unchanged). The
  wrapper starts the agent as a child (`args=[{}]`; id `names.agent_workflow_id(job_id)`,
  `job-<id>/agent`, which is not job-shaped, so the agent's whole history is encrypted to the
  seller alone; same task queue; `ParentClosePolicy.TERMINATE`; the remaining job time as its
  timeout), runs one activity, `kontra.turn.v1.deliver`, which sends the contract-validated
  input as the handler's message (`send_agent_message`, with an update id stable across
  attempts) and reads the `turn_events` stream from the receipt's offset to the reply to that
  message, then signals `close`, waits for the agent, and returns the reply as the job's
  result. The message is the input's JSON by alias (what the harness validates it against),
  and the reply, which the harness publishes by field name, is validated against the output
  model and returned by alias: a job's result reads the same as a workflow entry's. A handler
  that raises fails the job, non-retryably, with its message; a tool approval or a callback
  for that message fails it at once (a job is one turn, without buyer approvals; session jobs
  are kontra-sdk#60) -- an approval an auto-mode evaluator would have given included, since
  the harness publishes the request before it consults the evaluator and the stream says
  nothing then about whether a person will be needed (a tool the agent's policy allow-lists
  raises no request); the agent ending first fails it. The stream ending is never final in
  the activity: temporalio's stream client ends its iteration, rather than raising, when its
  poll is cancelled or times out, so a worker stopping mid-turn makes the activity's
  cancellation (retried on the next worker, whose update id gets the same admission back)
  and any other end a retryable failure; only the wrapper, seeing the child end, fails the
  job for it. Kontra never
  imports the harness: its wire shape (the update, signal and topic names, the envelope and
  reply fields, the event types) is written out once in `turn.py`, and a golden test holds it
  to the harness. The activity heartbeats for liveness only: the interceptor never forwards
  its heartbeats as buyer progress. `kontra graph` refuses a handler offering for now
  (kontra-sdk#56).
- **`execute_activities_in_parallel` is an optional helper**, `kontra.contrib.parallel`, not a
  primitive: neither `serve()` nor `Worker` registers `kontra.batch.v1` any more. A seller who
  wants it imports it in their workflow and passes `plugins=[ParallelPlugin()]` (a
  `SimplePlugin` adding the child workflow and passing the module through the sandbox) to
  `serve()` or to any Temporal worker. The child takes any page with a list `items` and a
  `next` (`Page` stays, as a convenience). `BATCH_WORKFLOW = "kontra.batch.v1"` and the field
  names of `_Spec`, `_Cursor` and `_Options` do not change: runs in flight, `kontra.graph` and
  the console read them. The helper puts a `kontra` import in the workflow file, so no
  example uses it: `examples/sweep` fans out in plain Temporal (one `execute_activity` per URL
  under an `asyncio.Semaphore(50)`, folded by `tally`), well within one history's limits for
  its 200 URLs (its README has the arithmetic).
- **The `[agents]` extra is gone**, with everything that read `sys.modules["agents"]`:
  `kontra._agents`, the derived plugin and converter, `serve(mcp_servers=)`, and the tests'
  guard that kept `agents` unimported. A seller running OpenAI's Agents SDK depends on
  `temporalio-openai-agents` and passes `OpenAIAgentsPlugin(model_params=ModelActivityParameters(),
  model_provider=OpenAIProvider(), mcp_servers=...)` in `serve(plugins=[...])` themselves, as
  `examples/chef-agents/worker.py` does. The first I25's premise, that a published offering
  could not import `temporalio` to pass the plugin, is what decision 2 removes. Its endpoint
  rule stands: `model_provider=OpenAIProvider()` makes the model activity read the Agents
  SDK's default client, set in `worker.py`; without it the plugin builds its own
  `AsyncOpenAI()` from the environment when the worker is built. Its principle stands too: no
  Kontra keyword changes what workflow code may do; a seller's Temporal plugins are Temporal
  config. kontra-sdk's dev group carries `temporalio-openai-agents[mcp]` (and
  `temporal-agent-harness==0.6.0` for the handler tests), so CI still runs chef-agents and
  type-checks `kontra graph`'s scripted model.
- **The temporalio range.** Compatibility is a dependency problem, not an import path:
  `temporalio>=1.34,<1.38` (and the `opentelemetry` extra's) is the range kontra-sdk is tested
  on. It is one constant pair in `kontra._compat`, never read back from installed metadata (an
  editable install's can be stale), and a test holds it equal to `pyproject.toml`'s.
  `KontraPlugin.prepare()` refuses to start outside it, before anything else (the codec relies
  on temporalio's serialization context and Nexus operation info, experimental APIs a minor
  may change); `kontra serve` (a log warning) and `kontra publish` (a line on stderr) warn
  before they import `worker.py`, which may be what then fails. The lock stays on the minor
  the suite last ran green on (Dependabot ignores temporalio minors), and widening the range
  is a deliberate PR after a green suite on the new minor. Today the range admits only 1.34:
  1.35 to 1.37 are not released. The check orders versions as PEP 440 does: a pre-release of
  a minor inside the range (`1.35.0rc1`) is in it, one of the lowest bound (`1.34.0rc1`,
  `1.34.0.dev0`) sorts before `1.34` and is not, as pip would have it.
- **The publish scan is dropped** (decision 2). `kontra publish` no longer refuses `temporalio`
  imports or `continue_as_new` in seller code (`kontra/cli/_seller_code.py` is deleted; the
  gate's copy is kontra-cloud#46): continuing as new is the seller's business, and an import
  scan constrained nothing the five controls above leave open.
- **The sandbox still passes `kontra` through** by default (`workflow_runner()`): Kontra's turn
  wrapper runs in it, a seller's models may use `kontra.types`, a workflow that hires imports
  `kontra.hire`, and the parallel helper keeps one class identity for its spec.
- **Rejected.** `from kontra.temporal import workflow`: the façade under a longer name -- the
  mirror is never complete, sellers end up with mixed imports, copying from Temporal's docs
  stops working, and to pyright it is one line thick anyway. `entry:` in `offering.yaml`
  (D11: the yaml names no code), a `@kontra.offering` decorator (`kontra` back inside workflow
  files), and finding the entry by convention.
- **Amends I27.** An agent workflow's mock runs follow the plugin the seller passed, not an
  import: `kontra graph` gives the scripted model to a worker whose `plugins=` hold
  `temporalio.openai_agents.OpenAIAgentsPlugin` (an `isinstance`, against that module only
  when it is loaded: an instance existing means it is; `temporalio.contrib.openai_agents` is
  not recognised), and its converter is that plugin's. A project whose `agents`, or Temporal's
  integration, is not installed fails naming `temporalio-openai-agents`, the project's own
  dependency. The mock worker still registers `kontra.batch.v1` itself, whatever the seller's
  plugins, and its stub is temporalio's `defn` (there is no Kontra registry left to stay out
  of). `examples/sweep`'s graph changed with its fan-out: `check_headers` is one activity
  node, with an edge to itself from the run whose `plan` drew 201 URLs (50 start at once, then
  each completion starts the next in a workflow task of its own), where it was the
  `kontra.batch.v1[check_headers]` child.
- **Checked by** `tests/examples/test_imports.py` (workflows and activities import `temporalio`
  and no `kontra`, by statement, dynamic import or module name in a string; models only
  `kontra.types`, quote only `kontra.serve.Quote`; loading them loads at most `kontra` and
  `kontra.types`, and nothing for a seller without `Artifact`; the scaffold the same; and
  `examples/hello` without `worker.py` and `quote.py` runs on a plain Temporal worker in a
  process where importing `kontra` fails), `tests/serve/test_serve_fn.py` (the scan and its
  refusals, the entry kinds, the agents plugin on the client with its converter kept),
  `tests/serve/test_plugin.py` (the converter, codec, failure-converter and storage rules, the
  order, the escape hatch running a whole Marketplace job on a plain temporalio worker, a
  worker-only plugin that undoes Kontra refused), `tests/serve/test_turn.py` and
  `tests/codec/test_codec_turn.py` (a real harness agent hired: one turn, a handler's failure,
  the binding refusals, the wire constants against the harness, the marker scan of both
  histories), `tests/serve/test_compat.py`, `tests/parallel` (the helper and its plugin) and
  `tests/examples/test_chef_agents.py`.

## I30. The authorizer after the narrowing: three token kinds, one rule that reads a request (kontra-sdk#59)

_Q1–Q9 below were decided by the implementer in the owner's place, as kontra-sdk#50 directs (no
step waits on an answer); each is listed on #59 for the owner to confirm or overturn._

kontra-sdk#50 narrows the seller SDK to native Temporal (its decision 1): sellers write
`temporalio` and emit whatever it allows -- updates, queries, workflow streams, signals between
their own workflows, schedules, outward Nexus, Worker Versioning. What constrains them is the
deny matrix in `temporal/authorizer`, so it was audited against the whole frontend, not the
subset the façade curated: all 191 surfaces Temporal server v1.32.1 hands its authorizer (123
WorkflowService, 12 OperatorService and 46 AdminService methods, `Health` `Check`/`List`/`Watch`,
both gRPC reflection versions, five Nexus HTTP routes), each against every principal, with
callers built by Temporal's real default JWT claim mapper and the typed request Temporal hands
the authorizer (`CallTarget.Request`, the decoded message itself, the same object the handler
then receives; `interceptor.go`). Checked against go.temporal.io/server v1.32.1, api v1.63.7,
grpc 1.84.0, temporalio 1.34.0 and temporal-worker-controller v1.13.0. Exactly twelve cells
changed, all deny to allow: two for the stream relay, ten for the worker controller.

* **Three kinds of token, recognised in this order.** The internal principal (unchanged), then
  `PublicAPIs`, then no token; then **a token that carries any permission is a party token**,
  judged by the party rule alone whatever its subject says; then **no permission and
  `sub == ControllerSubject`** (`platform/worker-controller`) is the worker controller; then any
  other token is judged by **the relay rule**. "Any permission" is any namespace key or system
  role, not any role bit: the default mapper creates a namespace's key even for a role it does
  not know (`acme:relay` gives `Namespaces{"acme": 0}`), so a new principal cannot be a role
  string. A permission string the mapper drops is no permission: `temporal-system:` with a role
  it does not know (no role, no namespace key), an entry with no `:`, an entry that is not a
  string, a `permissions` claim that is not a list; a token carrying only those is judged by its
  subject (`TestWhatTheClaimMapperCountsAsAPermission`, and matrix cells for the controller's and
  the relay's subject). So party tokens and the other two are told apart twice: the gate mints a
  party token with a permission and the other two with none, and a party's subject is a handle,
  which never contains the `/` both other subjects contain. The second holds whatever a token's
  permissions say; the first as long as the gate writes no dropped permission into a relay's or
  the controller's token. Neither test is a prefix test: the controller is equality with a
  constant, the relay equality with a construction.
* **The party allowlist is unchanged**: the same 40 WorkflowService calls, in the token's own
  namespace, each now justified in `authorizer.go`. A seller's worker token, a seller's API-key
  session (the gate has one Temporal mint, and I19's limits on keys are gate-side), a buyer and
  the dev party are identical on the wire and get the same cells. **Seller self-access is decided:
  allowed.** A party updates, queries and signals its own workflows (between its own workflows
  too), and cancels or terminates them; its stream subscriptions, the session pane (#54) and
  stock update handles need these. Escrow reads none of what they write: the server-recorded
  status of the pinned chain and its first run id, provenance from the buyer's records (I16),
  the clock and the buyer's gate calls. Stopping its own job is a refund against the seller.
* **The stream relay** (#52): a JWT with `sub = relay/<namespace>/job-<32 hex>`
  (`names.relay_subject`, below), a revocable `sid` and no permissions. For an
  `UpdateWorkflowExecutionRequest` under `UpdateWorkflowExecution` with update name
  `__temporal_workflow_stream_poll`, or a `QueryWorkflowRequest` under `QueryWorkflow` with
  query type `__temporal_workflow_stream_offset`, in a party's namespace (not `""`,
  `temporal-system` or the platform namespace), it is allowed when
  `RelaySubject(request namespace, request workflow id)` equals its subject, compared whole.
  Every other request type, a type under another API's name (Temporal itself hands the
  cross-namespace command check a workflow task's response under `SignalWorkflowExecution`),
  an update inside `ExecuteMultiOperation`, a stream handshake (no request) and the Nexus HTTP
  dispatch are denied. The rule reads those two typed fields and the request's encoded size,
  and nothing else -- no run id, update id, payload, header, callback or link is read, though
  each counts toward the size. **A relay request is at most `RelayRequestMax`, 4 KiB** (the
  #52 review): `proto.Size` of the whole typed request, so identity, header, arguments and
  topics all count, compared with the constant, one integer comparison, after the handler
  name and the subject. The largest request the gate's relay builds is 898 bytes (nine
  topics: an offering's eight streams named with 64 characters and `kontra.progress`; the
  largest offset; a 32-character namespace; the pinned run id; a pid@hostname identity), so
  4 KiB is over four times a genuine poll, and the offset query is about 110 bytes. A
  party's own requests are not capped. **Strict wire shape (Q1):** the relay
  makes only those two calls; it re-sends its Update with the same update id and wait policy
  COMPLETED until the answer carries the outcome (the server finds an update by its id), with an
  empty run id so it follows a continue-as-new; `StreamDraining` is retried after a cooldown with
  a new id, `TruncatedOffset` from offset 0, `AcceptedUpdateCompletedWorkflow` with a new id, and
  it stops when the gate's own observer (I6) says the job closed. No
  `PollWorkflowExecutionUpdate` (it names no update, so it would read any update's outcome by
  id) and no `DescribeWorkflowExecution`: the stock `WorkflowStreamClient.subscribe()` uses both
  and is refused at the first.
* **The worker controller** (KON-15): a JWT with `sub = platform/worker-controller`, a revocable
  `sid` and no permissions: ten calls, each named with the controller code that makes it
  (`ControllerAPIs`: describe, set current, set ramping, set manager, update metadata and delete
  of worker deployments and versions, `DescribeTaskQueue`, `CountWorkflowExecutions`), **in the
  platform namespace only**. **The platform namespace is `kontra-system`, a constant** in the
  public policy (`PlatformNamespace`, `names.PLATFORM_NAMESPACE`; Q2) and a reserved
  handle, so no party can register it. **No `StartWorkflowExecution` or
  `DescribeWorkflowExecution`** there: rollout gate workflows are not supported (Q3; the
  owner may revisit), because a stolen controller credential could otherwise start any platform
  workflow with any input. **Kontra's own workers** in `kontra-system` use ordinary
  `kontra-system:worker` tokens on the public frontend (Q4): the party rule, no change
  here; minting them is kontra-cloud's.
* **The denials escrow depends on**, for every token: `ResetWorkflowExecution` (a reset revives
  a terminated pinned run inside the same chain, its callbacks included -- the reset run's first
  run id comes from the replayed history -- so a seller resetting after a buyer's cancel and
  completing the new run would turn I17's refund into a delivery); `DeleteWorkflowExecution` and
  `DeleteNexusOperationExecution` (a missing buyer record is unverified, never a delivery: a
  buyer deleting its caller would turn a delivery into a refund against the seller; a seller
  deleting I16's placeholder reopens cancel-then-start); `UpdateNamespace` and the other
  namespace administration (retention decides how long those records exist); batch operations
  and schedules (they act as Temporal's internal principal, past this policy and the callback
  guard); the compute-config calls (`CreateWorkerDeploymentVersion`,
  `UpdateWorkerDeploymentVersionComputeConfig`, `ValidateWorkerDeploymentVersionComputeConfig`:
  the server would provision and invoke workers on a cloud provider with its own identity),
  denied to the controller too. Pause, workflow options, activity operations, standalone
  activities and workflow rules stay denied for want of a consumer. **Seller Worker Versioning
  stays denied in v1** (Q5): a seller who turns on `use_worker_versioning` gets no tasks.
  **Seller operations calls** (`ListWorkers`, describe and update of workers, pause and reset of
  activities, `UpdateTaskQueueConfig`) stay denied until a seller tool needs one (Q6).
* **Run ids are left to the gate** (Q7): every run of `job-<id>` is the seller's own, and
  escrow ignores runs outside the pinned chain; once escrow has pinned the chain the gate sets
  `first_execution_run_id` on the relay's Update, and Temporal refuses a look-alike chain
  (NOT_FOUND, measured).
* **The PRD's "only the gate starts or steers a workflow there [prod]"** describes the console
  and buyers, not the authorizer: a seller can always steer its own job, because it runs the
  code (Q8: to confirm on the issue).
* **Names** (Q9): the subjects `relay/<ns>/job-<32 hex>` and
  `platform/worker-controller`; `AllowedAPIs` keeps its name (no rename churn), with a
  justification on every entry.

`kontra/contract/names.py` gains `WORKER_CONTROLLER_SUBJECT`, `PLATFORM_NAMESPACE`,
`STREAM_POLL_UPDATE`, `STREAM_OFFSET_QUERY` and `relay_subject(namespace, job_id)`, which
validates a non-reserved handle and a job id and returns `"relay/" + namespace + "/" +
job_workflow_id(job_id)`. `RelaySubject` is its Go twin. Both languages read one golden file,
`temporal/authorizer/testdata/principals.json`, so the two constructions cannot drift, and
`tests/contract/test_names.py` pins the installed `temporalio.contrib.workflow_streams` handler
names (`_POLL_UPDATE`, `_OFFSET_QUERY`, Public Preview) to it: a rename upstream fails CI
instead of silently denying every relay. Injectivity: the namespace is a handle and the workflow
id `job-<32 hex>`, so the text after `relay/` holds exactly one `/`, and any request that
rebuilds the subject is that one split (`TestRelaySubjectHasOneSplit`).

**Amends** I3 (three token kinds, and "the only rule that had to inspect a request" is now the
relay's two typed fields, compared whole with a construction -- the removed publisher rule was a
prefix test of an attacker-chosen name, the opposite), I1 (`relay_subject` joins the
constructed identifiers), `authorizer.go`'s invariant ("nothing reads a request body" becomes
the sentence above) and `docs/security.md`.

**Rejected:**

1. A role string for the relay or the controller: an unknown role becomes `RoleUndefined` while
   still creating a namespace key, and Temporal's role bitmask is fixed at four bits.
2. The scope in `Claims.Extensions` through a second claim-mapper wrapper: a second JWT parse
   plus a custom claim, and the default mapper would no longer decide alone what a token is
   worth.
3. Per-principal audiences: there is one `AudienceGetter` per server.
4. The relay on the internal frontend: full control of Temporal inside an internet-facing
   process.
5. `PollWorkflowExecutionUpdate` and `DescribeWorkflowExecution` for the relay: they read any
   update by id and the run's metadata, and the strict shape does not need them.
6. A platform namespace in dynamic config: the gate edits that file at runtime (I12), so whoever
   edits rate limits would move the controller's scope. A server setting read once at startup
   stays possible if self-hosters need another name.
7. Opening schedules or Worker Versioning to sellers now: no consumer, and the escrow reasons
   above.

**Consequences.**

- kontra-cloud must mint both credentials before either works (the tracking issue's text, on
  #59): `KeyRing.sign_relay(namespace, job_id, session_id)` and
  `KeyRing.sign_controller(session_id)`, one function per kind with no role parameter (a relay
  or controller token that also carried a permission would be judged a party token, and one
  carrying a permission string the mapper drops would still be judged by its subject), each with a
  `sid` and a 15-minute lifetime; a relay session per job relay, revoked when the job closes or
  the last SSE listener leaves; the relay dials the **public** frontend and makes only the two
  calls above; `kontra-system` registered through the internal frontend (a reserved handle, so
  not through `ensure_party`); Kontra's own workers on `kontra-system:worker` tokens from a
  platform-only mint; the controller's token rotated into the Secret its
  `TemporalConnection.apiKeySecretRef` names. Deploy the kontra-temporal image that carries #59
  first.
- #52 ships the strict poll loop in kontra-sdk, so the FakeGate and the gate relay the same way:
  `kontra dev up`'s in-process server runs no authorizer (#57), and a FakeGate relaying through a
  stock `WorkflowStreamClient` would depend on two calls production denies. A stream must be
  hosted by the job's handler workflow itself, since the relay reaches `job-<id>` and nothing
  else. Each relay poll writes accepted and completed events to the seller's history and counts
  against its rate limits: one subscription per job, fanned out, only while a buyer listens.
  Topic filtering is the gate's (topics sit in the poll's payload, which the authorizer does not
  read), and a poll's result is one ciphertext. **A poll's outcome is stored in the history**
  (`WorkflowExecutionUpdateCompletedEventAttributes.Outcome`), up to the stream's poll response
  cap (`_MAX_POLL_RESPONSE_BYTES`, about 1 MB): a genuine relay doubles the stream's volume in
  history (each item once in its publish signal, again in a poll outcome) and spends one of the
  run's `history.maxTotalUpdates` (2,000, Temporal's default, pinned in
  `dynamicconfig/kontra.yaml`) per poll. #52 polls only from the tail, with topics, and has to
  plan for both.
- Residual risk: a relay that presented another update's id would get that update's outcome
  (the server matches an update by id alone); update ids are random ids the relay never sees,
  and outcomes are ciphertext to the job's parties.
- **The relay's size cap closes what the #52 review found.** A leaked relay token's requests
  could still end its job by their size alone: every workflow task must fit in Temporal's
  4 MiB gRPC message, Temporal terminates a run whose workflow task it cannot deliver, and
  #52's poll guard cannot refuse an oversized poll, because a refused update's request
  travels in the workflow task all the same. With `RelayRequestMax`, what a relay token's
  requests add to one workflow task of its job is at most `history.maxInFlightUpdates` (10:
  Temporal's default, pinned in `dynamicconfig/kontra.yaml` so that the bound does not move
  with Temporal's; `TestUpdateLimitsArePinned`) times 4 KiB, two orders of magnitude below the
  limit. #52's poll guard (polls refused at validation, so not written to history, once the
  run's history is past half of Kontra's pinned limits) bounds what polls add to the job's
  history, and could not bound this; the cap bounds what their requests add to a workflow
  task. With both, the burst the #52 review found no longer ends a job. Neither bounds the
  polls' answers; I31's answer budget does (the residual risk next).
- **Residual risk: a leaked relay token can end its job** (*amended by I31*). Since #52, not by
  the history growth described next: a job root refuses polls at validation once its history is
  past half of the pinned limits, a refused poll is not written to history, and with the size
  cap above that bounds what a leaked relay token can do to its job's history (the guard plus
  the polls in flight; a job someone listens to must keep its own history under the other half).
  Nor, since I31's answer budget, through the polls' answers: the polls one workflow task
  completes answer together in the worker's reply to that task, which must fit the 4 MiB gRPC
  limit, and concurrent polls of the size the cap allows could make it pass that limit and
  terminate the run (measured with both in place); a job root now answers at most 1.5 MiB in
  one workflow task and holds any further poll back for a later one. What remains is the
  relay's share of the history: a leaked token can spend it at once, where a genuine relay
  spends it as the stream grows, so a job someone listens to whose own history needs more than
  the other half of the limits is terminated (I31's history budget). As first found, with only
  its two allowed calls, it polls from offset 0 again and again, each time with a new update id,
  and so appends up to `history.maxTotalUpdates` completed updates of up to about 1 MB each to
  job J's history.
  Past `limit.historySize.error` (20 MiB, pinned) Temporal terminates the run
  (`enforceHistorySizeCheck`, reason "Workflow history size exceeds limit."), and escrow refunds
  the job as "seller workflow terminated", against the seller. Any stream above about 10.5 KB
  (20 MiB / 2,000) is enough, and one of about 1 MB takes about twenty polls (measured over the
  wire). The session is revoked only when the job closes or the last listener leaves, so a
  leaked token stays live while a buyer listens. The Authorizer cannot bound this: its size cap
  holds a relay's requests, the cost is in the answers, and it does not read the payload.
  Mitigations, recorded and **not adopted here** (the owner decides): handler workflows that
  host a stream continue-as-new on `workflow.info().is_continue_as_new_suggested()` (escrow
  follows the pinned chain, and the relay's empty run id follows the current run), which turns
  the termination into churn; #52's relay polls only from the tail, with topics (I31 chose
  offset 0, once per job), which bounds a genuine relay's cost but not a leaked token's, since
  that token picks its own offset; a per-namespace `history.maxTotalUpdates` below the pinned
  2,000, chosen with `limit.historySize.error` in mind, which bounds it only together with
  continue-as-new (20 MiB at 1 MB a poll is about twenty updates, a cap a genuine subscription
  would hit too).
- #51 documents that a seller who turns on Worker Versioning gets no tasks.

Tested: the Go deny matrix, every cell with its typed request, against Kontra's and Temporal's
two authorizers (331 cells, 260 of them denials that Temporal's no-op authorizer allows; 993
checks), the relay's size cap among them (the largest genuine poll and the offset query
allowed; a poll past the cap by its topics, its identity or a header, and an offset query by
its arguments or a header, denied; a party's oversized update and query allowed); a sweep of
all 191 surfaces against 26 kinds of caller, five namespaces and five requests -- a favourable
one, the same past the relay's cap, two hostile ones and none (124,150 decisions, the expected
rule restated in the test); the cap at its edge (`TestRelayRequestMax`: exactly 4 KiB passes,
one byte more does not); Temporal's own authorization interceptor handing the relay rule the
decoded request; and `tests/temporal` over the wire against the real binary (59 tests), the
relay, controller, self-access and escrow cells included, and a relay poll past the cap
refused while the relay's own poll on the same job still answers. What a relay's polls cost a job's history is stated
above and in `docs/security.md`, not pinned by a wire test: that test would be a ready-made
recipe in a public repository.

## I31. Streams cross the boundary through the job's own workflow stream, sealed per item (kontra-sdk#52; decisions 5, 6 of #50)

Nexus does not stream. A seller already streams with Temporal's own API
(`temporalio.contrib.workflow_streams`: a log in workflow state, written by a publish signal,
read by a poll Update and an offset Query), and a buyer holds no Temporal client. So a job's
stream reaches its buyer the way #59's relay credential allows (I30), and it **replaces** the
bespoke progress channel rather than adding a second one. Checked against temporalio 1.34.0
(`contrib/workflow_streams`, `worker/_workflow_instance.py`) and server 1.32 (CLI 1.9.1),
with two prototypes whose results are below.

* **The rule.** A topic the offering lists reaches the buyer; nothing else does.
  `offering.yaml` gains `streams:` -- topic names, data, never code (D11): at most 8 distinct
  names matching `[A-Za-z][A-Za-z0-9_.-]{0,63}`, `kontra.` reserved in any case
  (`kontra.progress` is relayed for every job), stored sorted. `kontra.contract.offering`
  (`check_streams`) is the one validator and `spec/offering-streams.v1.json` states the same
  rule language-neutrally (the same vectors run through both). It is **not an `x-kontra`
  fact**: not in the contract hash, every golden unchanged. It travels as catalog data beside
  the document, as `recipients` does (I7): `PublishRequest.streams`, `CatalogEntry.streams`.
  It is **fixed per version** (owner, OD5): the gate refuses a republish of a version with
  other streams (409 `streams_fixed`) and `KontraPlugin`'s hash lock refuses to start code
  whose list differs from the catalog's. Per-topic item schemas in the contract are later
  (OD11): `job.stream(type=)` takes the buyer's own model.
* **Why the codec seals each item.** The contrib encodes a poll's answer as one payload and
  runs the codec once on it (its own docstrings say so). Encrypted whole, the relay could read
  no topic or offset; encrypted whole to the buyer, a gate that asked for every topic would
  hand the buyer the seller's private ones. The codec runs outside the sandbox and knows only
  the serialization context (one worker serves several offerings, and every job id looks
  alike), so the only channel from the workflow to the codec is the payload's own metadata.
  Kontra's workflow interceptor therefore marks the answer of a poll on a job root -- a
  `RawValue` with `kontra-stream: poll/v1` and the topics the buyer may read (the offering's
  `streams`, by task queue, and `kontra.progress`); `CompositePayloadConverter` passes a
  `RawValue` through untouched -- and `KontraCodec` turns it into the **stream envelope**
  (`kontra/codec/stream.py`): `{"v": 1, "items": [{"topic", "offset", "ct"}...], "next_offset",
  "more_ready"}`, encoding `binary/kontra-stream`. Each item is the age ciphertext of its own
  `Payload` with its topic and offset sealed inside (`kontra-topic`, `kontra-offset`), so a
  buyer checks both against the event and a gate cannot relabel or reorder items. A listed
  topic is sealed to the seller's recipient set and the job's buyer key and named in the clear;
  any other to the seller's set alone, its name `null`. Items above 256 KiB of ciphertext are
  claim-checked (or a stub without a blob store; the relay records a gap); the envelope stops at
  512 KiB with `next_offset`/`more_ready` set exactly as the contrib's own truncation does, so
  every subscriber just polls again. The workflow interceptor cuts the answer the same way
  first, where the sealed envelope could pass 512 KiB (an upper bound per item, for the
  worker's recipient count), so the codec's own trimming never changes an answer the run
  counted as read to its end (the drain below); and an item above 256 KiB is claim-checked
  once per job, not once per poll that re-reads it (age's randomness would make each copy a
  new blob). Offsets and the log's length are in the clear: the
  platform reads them from history sizes anyway. Decoding restores the exact `PollResult`, so
  the seller's stock `WorkflowStreamClient`, its console and replay are unchanged. The offset
  query's answer is marked too and passes in plaintext. A mark anywhere but a seller's job
  workflow is refused (`CodecError`), never encrypted another way. `KontraPlugin` refuses
  external storage at or below the envelope's 512 KiB (plus framing): the relay must read it.
* **Who hosts.** The relay reaches `job-<id>` and nothing else (I30), so the job root hosts the
  stream. **Host if absent:** when a root's run starts and its code built no `WorkflowStream`,
  Kontra builds the contrib's own (`_HostedStream`, a subclass whose `__init__` satisfies the
  contrib's caller-frame check), after the seller's `__init__` ran; publish signals that came
  first are buffered by temporalio. **Early handlers:** in a run's first activation updates are
  applied before the workflow is initialised, and the prototype got `Update handler for
  '__temporal_workflow_stream_poll' expected but not found`; the interceptor's `init` registers
  placeholders (a poll that waits for the stream, an offset of 0), which the stream replaces.
  **Drain:** a relay sleeps between polls, and the prototype lost 5 of 6 items of every short
  job; when a run returns or fails and any poll was seen in the chain, the root waits (5 s at
  most, OD9) for a poll to have answered up to the stream's end, then answers the polls still
  waiting (no "unfinished update handlers" warning on every listened job). 6 of 6 with it.
  **Continue-as-new:** for a stream Kontra hosts, it drains, detaches the pollers and carries
  the stream's end to the next run in a plaintext header (`kontra-stream-base`, rewritten in the
  same dict the command reads when the error reaches the top), so offsets stay monotonic across
  the chain (a 3-run chain relayed offsets 0..5, all six); a stream the seller hosts follows the
  contrib's recipe (`prior_state`), which `docs/sellers.md` states. Every prototype history
  replayed clean.
* **Bound handlers: mirroring, under the same names.** A bound handler's events are published
  by the agent child (`job-<id>/agent`), whose payloads are the seller's alone and which the
  relay cannot reach. The turn activity already reads the child's stream, so it mirrors the
  listed topics onto the job's stream, each item's `Payload` untouched, flushed every half
  second and before it returns the reply, and checkpointed in its heartbeat details so a
  retried attempt mirrors from where the last flush got (a lost worker re-publishes at most
  one flush window; the test counts every event once). Unlisted topics are never copied.
  **Naming rule: identity** (owner, OD3): `streams: [turn_events]` relays the harness's own
  `turn_events`; there is no `agent` alias -- the PRD's and the issue's `streams: [agent]`
  was illustrative. One sentence covers both entry kinds, no alias table couples Kontra to a
  pre-1.0 harness, and #54's renderer reads `turn_events` by one name in dev and prod.
* **The relay** (`kontra/_relay.py`), written once: the FakeGate relays with it and
  kontra-cloud imports it. The strict wire shape of I30 -- only `UpdateWorkflowExecution` of the
  poll (run id empty, `first_execution_run_id` once pinned, wait policy COMPLETED, the same
  update id re-sent until it carries an outcome) and, for a tail start, the offset
  `QueryWorkflow` -- with every failure classified (`StreamDraining`, `TruncatedOffset`,
  `AcceptedUpdateCompletedWorkflow`, `KontraStreamFull`, other rejections, RPC errors),
  `RelayBatch`es whose `next_offset` the caller persists after storing their items, and pacing
  (`MIN_INTERVAL` 2 s, `pace_for` from the job's history size). It imports no
  `temporalio.contrib`; `parse_envelope` refuses any `ct` that is not an age file, so plaintext
  never reaches a gate's storage, and a worker without #52 (whole ciphertext) stops it with
  `protocol`. **Amends the issue's "FakeGate relays from a real `WorkflowStreamClient`":** the
  stock client uses `PollWorkflowExecutionUpdate` and `DescribeWorkflowExecution`, which
  production denies (I30), and `kontra dev up` runs no Authorizer to show it.
* **When the relay runs** (owner, OD2, **changed from the design's tail start**): one relay per
  job, started once escrow has pinned the chain **and** its first listener attaches, **from
  offset 0, once per job**; nothing polls while nobody ever listened. The gate keeps a
  monotonic per-job cursor and stores what it relays (the caps below), so a later listener
  replays from the gate's store with `after` or `Last-Event-ID`, as progress replayed before;
  each item is still polled once. A listener leaving pauses the relay after 30 s, the offset
  kept; a buyer's resume never moves it (a buyer-chosen offset would hand every buyer the
  leaked-token amplification of I30). `kontra._relay` keeps `start="tail"` for a gate that
  wants it, but `start=` has no default: a gate that omits it gets an error, not the tail.
* **Seam ②** (owner, OD4: a hard cut in this 0.x release). `POST` and `GET
  /v1/jobs/{id}/progress`, `ProgressPost` and `ProgressEvent` are gone. `GET
  /v1/jobs/{id}/stream` (the job's buyer or seller) serves SSE of `StreamEvent{cursor, kind:
  item | gap | history, ...}`: `topic` (repeatable; each in the version's `streams` or
  `kontra.progress`, else 422 `unknown_topic`), `history`, `after` or `Last-Event-ID`; it ends
  once the job is closed and its relay finished. History events are the platform's liveness
  summary, unchanged in content. Caps (owner, OD7): 256 KiB per relayed item
  (`gap{too_large}` with its size), 16 MiB per topic and 64 MiB per job stored -- past one,
  a single `gap{topic_budget|job_budget}` (the first item that did not fit), and the gate
  relays no more of that topic (it restarts the relay without it) or of the job (the relay
  stops), so neither the store nor the seller's history grows by an item the gate drops --
  8 listeners per job (429 `too_many_listeners`); no rate drop -- items wait in the
  workflow's log while the relay paces itself.
* **The buyer.** `job.stream(topic, type=T)` -> `JobStream[T]`: decrypt with the job's key,
  check the sealed topic and offset, validate as `T` (`dict` for leniency), dedupe by cursor
  (a replay) and by offset (a gate that re-read), resume through `.cursor`. Not sealed for us,
  or relabelled: logged and skipped. A gap with an offset is logged; one without (the relay
  stopped) raises `StreamInterrupted`; an invalid item `StreamItemInvalid`. `job.progress()`
  keeps its public shape, as sugar over the `kontra.progress` topic (items are
  `kontra.types.ProgressRecord{v, at, activity, details}`) plus history events; the MCP
  `status` tool and `kontra job --progress` keep their interface, but what they show follows
  the delivery semantics below: heartbeats appear only for a job someone listened to before
  it closed (asking for its status while it runs is listening), else history events alone.
  `kontra job <id> --stream TOPIC` prints decrypted items as JSON lines.
* **History budget** (owner, OD1, OD8). Heartbeats now cost the seller's history: one publish
  signal is about four events, a relayed poll about five more (measured: 20 activity publish
  batches cost 95 events, 192 with a relay polling). Against Kontra's pinned
  `limit.historyCount.error` (20,480), yesterday's one-a-second forwarding with a listening
  relay would terminate a job, refunded against the seller, in about 52 minutes. Adopted: **the
  job's poll guard** -- a root refuses a poll at validation (`KontraStreamFull`) once its
  history is past half of the pinned limits (10,240 events or 10 MiB; a test pins `== error //
  2`); a rejected update is not written to history, so no poll, whether a genuine relay's or a
  leaked relay token's, grows the job further, and the relay stops with `gap{history_budget}` --
  and **a budgeted ProgressForwarder**: the latest details at most every 5 s
  (`KONTRA_PROGRESS_INTERVAL` still overrides; 0 publishes every heartbeat, batched), the
  interval doubling every 360 items up to 300 s, at most 1,000 items per job (about 4,000
  events, a fifth of the limit), then one log line and nothing more; an activity's latest
  heartbeat is published when it returns, whatever the interval. The count is per worker process
  (each one that runs the job's activities has its own) and survives the job going quiet: a job
  forgotten after five idle minutes keeps its count for 31 days, past the longest
  `max_duration`. **What the guard bounds, and what it does not.** It bounds what polls add to
  the history: past it, polls are refused and not stored, so their share is the guard plus the
  polls in flight (`history.maxInFlightUpdates`, 10, pinned since #59, of at most 512 KiB each).
  It caps the relay's share near half of the limits, not the job's total: the relay's events
  stay, so a job someone listens to needs its own history under the other half (measured with
  the limits scaled down: a job whose own work reached about 85% of them completed unlistened
  and was terminated with a genuine relay listening, which the guard had stopped at half).
  Capping the relay's own share (counting answered polls and their bytes in the interceptor) is
  the better bound, recorded, not built. **The guard and #59's request cap (I30) together bound
  what a leaked relay token can do to its job's history.** The guard stops its polls at half of
  the limits; the cap, 4 KiB a relay request (the gate's largest poll is 898 bytes), refuses the
  burst this change's review measured -- concurrent polls with oversized requests made the
  worker's reply to the workflow task carrying them pass gRPC's 4 MiB limit, since the SDK
  echoes every update request it accepts or rejects, and Temporal terminated the run
  (`GrpcMessageTooLarge`) whether the job accepted or refused them, and with no poll handler at
  all (the shape before #52 too); no validator can refuse what is echoed when it is refused. The
  residual, as measured: a relay listening still costs a job history, its share up to the guard,
  whoever holds the token, so a job someone listens to must keep its own history under the other
  half (above). **And the answer budget closes I30's residual risk through the answers.** The
  polls one workflow task completes answer together in the worker's reply to that task, up to
  512 KiB each, and with only the guard and the cap, concurrent polls of the size the cap allows
  could make that reply pass gRPC's 4 MiB limit, and Temporal terminated the run
  (`GrpcMessageTooLarge`; measured with the pinned limits, the history far below the guard): the
  cap holds requests and the guard holds history, so neither reached it. Now the job root counts
  what it answers in each workflow task (`kontra.codec.stream.envelope_bound`, the codec's own
  arithmetic for the most keys any item is sealed to, so the same on every worker) and answers
  at most `STREAM_ANSWER_BUDGET`, 1.5 MiB, three full envelopes, in one task: a poll past it is
  answered with no items and `more_ready`, at the offset it would have started from, so its
  poller polls again and is answered in a later task -- not a refusal, which the relay would
  back off from. Only answers that carry items count and the first of a task is always sent, so
  the stream makes progress; an answer held back is never counted as read, so the drain waits
  for the poll that carries it. 1.5 MiB leaves the rest of that reply 2.5 MiB: the update
  requests it echoes (at most `history.maxInFlightUpdates`, 10, of at most `RelayRequestMax`,
  4 KiB: 40 KiB), the answers with no items (a frame each), and the seller's own commands, one
  payload at the pinned `limit.blobSize.error` (2 MiB) included; a test sums them under 4 MiB
  from `kontra.yaml` and `authorizer.go`. A workflow task is told apart by the run's history
  length (`workflow.info().get_current_history_length()`), checked on temporalio 1.34 in the
  sandbox: one value for every answer completed in one task's reply (the activations that
  resolve its local activities included), a larger one for the next, and the same on replay, so
  the cut, and the drain's count of it, replay the same. Measured again on a dev server with the
  pinned limits: the job keeps running, and no workflow task's answers pass 1.5 MiB. The two
  other bounds recorded with it, an envelope small enough that the polls in flight fit one reply
  and a lower pinned `history.maxInFlightUpdates`, are not needed. What I30's risk keeps is the
  relay's share of the history (above): a leaked token can spend it at once, where a genuine
  relay spends it as the stream grows. A poll names at most nine topics (`kontra._relay`: the
  offering's eight streams and `kontra.progress`), so the gate's largest stays far under the
  cap.
* **Delivery semantics.** Live: a buyer who never listens before the job closes gets nothing
  of its stream but history events (a relay attaching after a short job closed reads nothing;
  items published before a continue-as-new of a Kontra-hosted stream nobody polled are not
  carried). At-least-once transport from the gate, deduplicated by the buyer. An activity
  retry re-publishes (the turn mirror is checkpointed; a seller's own activities are theirs).
* **Not in v1.** Client publish signals are not sealed per item (owner, OD6): a publish
  signal, like a continue-as-new input, stays one payload encrypted to the buyer and the
  seller, so an unlisted topic is private in what is *relayed*, not in what the buyer's key
  could open if it ever read the seller's history (it cannot: the buyer has no Temporal access
  to the seller's namespace). Harness-internal activities' heartbeats are forwarded as any
  seller activity's are (owner, OD10, asked on #52). Item schemas in the contract (OD11).
* **The authorizer: no new cell here.** The relay's two calls are #59's, and so is the request
  cap this change's review asked for (I30, merged in); the forwarder's and the turn mirror's
  publish signal and the seller's own subscriptions are the party allowlist on its own job; the
  gate's pacing reads the internal frontend. The relay loop's wire test
  (`tests/temporal/test_relay_loop_wire.py`) belongs to this change, not to #59's branch.

**Rejected.** A Kontra-owned poll update (it duplicates the contrib's long-poll and changes
the authorizer's handler names); one whole ciphertext per poll with the item list in the clear
beside it (the gate would decide what the buyer can open); encrypting items at publish time (a
workflow's publish runs in the sandbox, without keys or I/O, and converters are synchronous);
a buyer-reported relay offset (every buyer gets the leaked-token amplification); a relay subject
for the agent child (it breaks I30's one-split proof, and the child's payloads are the
seller's); an `agent` alias; `streams:` in `x-kontra` (a hashed document with unhashed members).

**Amends** I3 and I30 (the relay's stop, its cost, the residual risk above), I7 (the stream
envelope; a plaintext log length; external storage above the envelope limit), I29 (the turn
activity mirrors; Kontra hosts a stream on every job root), and `docs/security.md`.
**Supersedes** the bespoke progress channel (PRD §8.4's `POST .../progress`).

Tested: the envelope and the codec branches, JSON nested past the stack refused, the
interceptor's cut sealed whole for every size class and recipient count and within the bound
it counts, an answer held back sealed within its frame, and a re-read large item stored once
per job (`tests/codec/test_stream_envelope.py`); the streams rule through the
code and the spec (`tests/contract/test_offering_streams.py`); hosting, early handlers, the
drain (a short job's tail that passes one envelope included), the answer budget (with the budget
scaled down, polls completed in one workflow task answered within it, the rest held back, the
job running on and every poller reading the whole stream; the drain waiting for answers held
back), a continue-as-new chain, the guard and replay (`tests/serve/test_stream_hosting.py`);
the budget and the rest of a workflow task's reply under 4 MiB, from the pinned limits
(`tests/serve/test_interceptors.py`); every relay classification (a `TruncatedOffset` at offset
0 backed off, not spun on), an answer held back polled again at once, the same update id
re-sent, a look-alike chain refused, `start=` required, at most `STREAMS_MAX` + 1 topics and no
contrib import (`tests/serve/test_relay.py`); the
forwarder, its budget across a quiet spell (`tests/serve/test_progress.py`); the mirror and its
checkpoint (`tests/serve/test_turn.py`); the FakeGate's relay, caps (a spent budget one gap),
resume and listener rules (`tests/testing/test_stream_relay.py`); `job.stream()`
(`tests/hire/test_stream.py`); seam ⑤'s marker scans of every run's history, the raw SSE and the
gate's store, with a misbehaving relay, for a workflow entry and a bound handler
(`tests/serve/test_streams.py`); and the loop over the wire with a relay token, under #59's
request cap (`tests/temporal/test_relay_loop_wire.py`). What a leaked token's concurrent polls
did before the answer budget, and do with it, was measured on a dev server with the pinned
limits; no test ends a job, and the budget's tests stay far below gRPC's limit with or without
the budget, for the reason I30 gives: a test that ended a job would be a recipe.
