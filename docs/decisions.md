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
| `worker` in `acme` (seller or buyer) | JWT, `acme:worker` | an explicit allowlist of WorkflowService calls in `acme` only, including standalone Nexus operations |
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
  `age1...`, as in the PRD's example.
* `KontraCodec` encrypts every payload. Recipients come from the serialization context
  (temporalio 1.34): payloads of the root handler workflow `job-<id>` (input, result,
  failures) go to the seller *and* the job's buyer; everything else on the seller side
  (activities, children) goes to the seller only. On the buyer side, Nexus inputs go to
  the seller (the key of the offering behind the endpoint: from the hire response, or the
  catalog's `GET /v1/endpoints/{endpoint}` — never derived from the name) and the buyer. The quote operation's
  *output* passes through in plaintext (I4).
* The buyer's key travels in the signed hold token (`buyer_key`), so a seller cannot be
  tricked into encrypting a delivery to someone else. A worker caches it per job; on a
  miss (another process, a restart) it asks the gate (`GET /v1/jobs/{id}/seller`).
* Failure messages and stack traces are encoded too (`encode_common_attributes`).
* Above 256 KiB of ciphertext a payload is claim-checked: uploaded to the bucket,
  content-addressed by the ciphertext's sha256, and replaced by a reference.
* Artifacts use the same age format, streamed in 64 KiB chunks, uploaded in parts, so
  memory stays flat regardless of size. `Artifact.sha256` is of the plaintext; the gate
  records the ciphertext's.

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
