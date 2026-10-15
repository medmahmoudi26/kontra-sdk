# Security

Kontra brokers jobs between parties who do not trust each other, on infrastructure neither of
them controls. This document states what each party is protected from, what Kontra can and
cannot see, how the Temporal frontend is locked down, and where the protections stop.
To report a vulnerability, see [SECURITY.md](../SECURITY.md).

- [Threat model](#threat-model)
- [What Kontra can and cannot see](#what-kontra-can-and-cannot-see)
- [Identity and transport](#identity-and-transport)
- [The Authorizer](#the-authorizer)
- [The callback guard and the upstream Nexus completion issue](#the-callback-guard-and-the-upstream-nexus-completion-issue)
- [Escrow and the hold token](#escrow-and-the-hold-token)
- [Contracts and input](#contracts-and-input)
- [Known limits](#known-limits)
- [Supply chain](#supply-chain)

## Threat model

From PRD §10.1, unchanged:

- **The seller's worker is untrusted.** It is a process on hardware Kontra does not control,
  holding a credential for Kontra's Temporal frontend.
- **The buyer is untrusted.**
- Each must be unable to harm other sellers, other buyers, or Kontra.
- **Kontra must be unable to read job payloads.**

What that means concretely, and what enforces it:

| Threat | Stopped by |
|---|---|
| A compromised seller machine reaches another party's namespace | the Authorizer: a token acts in its own namespace only, from an allowlist; cross-namespace workflow commands are off |
| A compromised seller machine repoints its offering (or anyone's) at itself | no token reaches an Operator or Admin API; endpoints are created by the gate after a fresh human login, and their names are opaque |
| One seller forges another seller's Nexus result | the callback guard (below) |
| A buyer reads, signals or cancels a seller's handler workflow | the Authorizer: a buyer's token has no call in the seller's namespace, including the job it paid for |
| A buyer forges or alters a hold token (amount, seller, expiry, contract) | ES256 signature checked offline by the seller's handler, with audience, expiry, seller namespace, offering and contract hash |
| A buyer replays a hold token | the handler workflow id is built from the job id and started with reject-duplicate: one execution per hold; a cancel before the job started occupies that id, so the refunded hold cannot start it either |
| A buyer quotes a small input and executes a large one | the handler refuses, before fetching any of it, an input larger than the one the hold was quoted for (the hold carries its size), then validates the input and re-runs the seller's `quote` on it, refusing a price above the hold; a start too late for the quoted duration is refused, and a run never outlives the gate's refund deadline, which the hold carries |
| A buyer cancels its own call, or keeps its own namespace too busy to read, to blame the seller or be refunded blind | who canceled is read from the buyer's own records of the call; a failed read of the buyer's namespace is never followed by a blind refund |
| A seller serves code that differs from the contract the buyer paid for | the hash lock: `kontra serve` refuses to start on a mismatch, and the handler refuses a token for another contract hash |
| A seller takes the money without delivering | escrow moves only on observed facts: no delivery by the deadline refunds |
| A buyer cancels a nearly finished job, to be refunded and still receive the result through its own call | a cancel of a job whose run was observed terminates that run first (a cancel request can be ignored) and refunds only once Temporal shows it terminated or closed without completing; a run that completed first is a delivery, and the cancel is refused |
| An authenticated client sends a body built to make the gate's validation expensive (a megabyte of bad list items, megabytes of stray keys) | every list, dict and string of the wire models has a ceiling; a list is refused at its first item past it, and more than a few unknown keys (or a long one) are one error, so a body is a handful of errors whatever it holds |
| A buyer takes the delivery without paying | the review window closes on its own and releases the payment |
| A malicious contract document (SSRF through `$ref`, an unbounded schema) | the catalog's document rules; patterns run on RE2 |
| A seller's text drives a buyer's terminal or agent | the CLI strips control characters from seller text; `kontra mcp` writes deliveries only where it is safe to (below) |
| Kontra, its database, bucket or Temporal history leaks | payloads are ciphertext end to end; the keys live only on the parties' machines |

## What Kontra can and cannot see

Every payload is encrypted with [age](https://age-encryption.org) (X25519 recipients) by a
Temporal `PayloadCodec` on the parties' own machines, with recipients chosen from the
payload's serialization context (`docs/decisions.md` I7):

- the handler workflow `job-<id>` (its input, result and failures) is encrypted to the seller
  **and** the job's buyer;
- everything else on the seller's side (activities, child workflows) to the seller only;
- the buyer's Nexus input to the seller and the buyer.

Payloads above 256 KiB of ciphertext go to the bucket, content-addressed by the ciphertext's
hash; every `Artifact` is stored there too, encrypted while it streams, under a key the gate
assigns. Ciphertext is standard age, so a party can decrypt what it received with the stock
`age` CLI and its key, which is how a dispute is argued.

| Kontra **cannot** read | Kontra **can** read |
|---|---|
| job inputs, results and failure messages (including stack traces) | a quote's answer: price, currency, a refusal's reason, a busy ETA (it must lock the price) |
| a quote's input (relayed byte for byte) | who hired which offering, when, at what price, and the job's state; the reason a buyer gives for rejecting a delivery (it is a gate call, not a payload) |
| the seller's heartbeat details (progress is encrypted to the buyer) | workflow metadata in Temporal: workflow ids, task queues, activity names, timers, timestamps, states, event counts, payload sizes |
| artifacts and claim-checked payloads, including the plaintext sha256 an `Artifact` reference carries (it travels inside the encrypted delivery) | the size and sha256 of each ciphertext (age's overhead is fixed, so plaintext sizes follow) and each artifact's content type |
| any private key: keys are generated by `kontra login` on your machine and never leave it | public keys, contract documents, offering facts, balances |

Two consequences worth knowing:

- **Metadata is visible.** Activity names and the shape of a workflow's history are what
  progress liveness is built from. Do not encode secrets in workflow ids, activity names or
  Temporal search attributes (Temporal indexes search attributes; they are not payloads and are
  not encrypted).
- **Kontra is the key directory.** A buyer encrypts to the seller key in the catalog (checked
  against the document's own hash, which proves integrity, not who wrote it); a seller encrypts
  a delivery to the buyer key carried in the hold token Kontra signs. Encryption therefore
  protects payloads from anyone who obtains Kontra's stored data -- a database or bucket leak,
  Temporal history, a request for records -- and from anyone on the path. It does not protect
  against an operator that actively substitutes keys at hire time. If that is in your threat
  model, compare the seller's key (`QuoteResult.seller_public_key`, the `public_key` in
  `x-kontra`) with one the seller publishes elsewhere. A key-transparency log is not built.

## Identity and transport

- `kontra login` runs a device-code flow and stores a refresh token and an age identity in
  `$KONTRA_HOME` (default `~/.kontra`, files mode 0600). The public key is registered with the
  gate; the private key never leaves the machine. The device code is approved in the browser
  or through a mailed sign-in link whose page asks for the code only the requester's screen
  shows; opening the link consumes nothing (decisions I19).
- A program uses an **API key** (`KONTRA_API_KEY`), stored by the gate as a hash and exchanged
  for the same 15-minute tokens. Its session never counts as fresh: it cannot publish, set the
  party's key or manage keys. Revoking it ends its tokens on their next request.
- `kontra serve` (and a buyer's client) exchanges the session for a **15-minute** ES256 JWT,
  `permissions: ["<namespace>:worker"]`, audience `kontra:temporal`, refreshed in the
  background and swapped into the running Temporal client. Temporal's default claim mapper
  verifies it against the gate's JWKS. Revoking a session stops the refresh; a token already
  issued stays valid until it expires.
- Publishing is `POST /v1/catalog` with a **fresh human session**: a worker token or an API
  key never qualifies, so a stolen worker credential or key cannot publish or repoint anything.
- In production the public Temporal frontend sits behind a TLS-terminating proxy Kontra
  controls; TLS is always on. The public frontend refuses every Operator and Admin call to
  every token (the Authorizer); only the internal frontend, which is never published, accepts
  them. `kontra dev` is plaintext on loopback.

## The Authorizer

Kontra runs a self-hosted Temporal server (`temporal/authorizer`, public so you can read the
policy your worker runs under). The public frontend enforces this policy (PRD §10.4,
`docs/decisions.md` I3); every decision is exact string equality or set membership, with no
name parsing and no request-body inspection:

| Caller on the public frontend | Allowed | Denied |
|---|---|---|
| no token | health, `GetSystemInfo`, `GetClusterInfo` | everything else |
| malformed token (expired, unknown key, `alg: none`, HS256 with the public key, wrong or missing audience, a gate API token) | -- | everything, at claim mapping |
| `worker` in namespace A (seller or buyer) | an explicit allowlist of WorkflowService calls in A: task polling and responding, heartbeats, start / signal / query / describe / cancel of A's workflows, standalone Nexus operations in A, and `DescribeTaskQueue` on A's own queues (backlog stats for `temporal task-queue describe` and KEDA — metadata, no payloads) | any other namespace; every OperatorService and AdminService call; the Nexus HTTP API; namespace admin, batch, schedules, resets, deletes, deployments |
| any other role (`admin`, `write`, `read`, `temporal-system:*`), even next to `worker` | nothing beyond what `worker` in A grants | everything those roles grant on stock Temporal |
| the control plane | everything | -- (it exists only on the internal listener) |

There is exactly one role and it is deliberately weak: a compromised seller machine yields
that seller's running jobs, not the ability to repoint `acme/enrich` elsewhere. Defence in
depth, pinned in dynamic config and re-asserted by the server at runtime whatever the file
says: cross-namespace commands off, task tokens bound to their namespace, the streaming
authorizer on, and the Nexus headers that carry a callback token, request id or links stripped
from callers (otherwise a buyer could hand a seller's handler values of its choosing).
The server refuses to start if the dynamic config file contradicts any of these.

**Tested as the required check `deny-matrix`** (`make deny-matrix`): the Go deny matrix runs
every cell against Kontra's authorizer *and* against Temporal's own two authorizers, to show
each denied cell is allowed by default and denied only because of this policy; a sweep runs
every frontend API against every kind of caller; then `tests/temporal` proves the same cells
over the wire against the real server binary with real `temporalio` clients, together with the
positive hiring path.

## The callback guard and the upstream Nexus completion issue

A Nexus handler completes an asynchronous operation by starting a workflow whose completion
callback is `temporal://system` plus a callback token Temporal gave it. In Temporal v1.32.1
that token is unsigned, it names the caller's namespace, workflow, run and operation, and the
server compares the completion's request id with the operation's **only when the token
carries one**. A seller hired alongside another seller could therefore complete the other
seller's operation with a result of its own choosing, and the buyer would receive it as the
other seller's delivery. The completion reaches the buyer's namespace as Temporal's internal
principal, so no authorizer can refuse it. The wire suite reproduced this against a buyer
workflow and against a standalone operation before the guard existed.

`temporal/authorizer/callbackguard` runs on both frontends after authorization. For every
caller but Temporal's internal principal, on every API that can carry a completion callback
(found by walking the request's type, not from a list), it accepts a callback only in the one
shape a handler needs: a Nexus callback to exactly `temporal://system`, with exactly one token,
whose request id is set. The request id is a UUID only that operation's caller and handler
know, so a token completes only its own operation. Every other callback URL is refused too:
v1.32.1 does not validate callback URLs on every API (`UpdateWorkflowExecution` skips it), and
a callback is an HTTP request Temporal itself makes.

The root cause is upstream, so the guard stays until Temporal validates completion tokens
itself, and every Temporal upgrade is held for review: the Go tests include a sweep that fails
when a new request type can carry callbacks, and dependabot does not propose Temporal minor
upgrades.

## Escrow and the hold token

Money moves by pure functions (`kontra/escrow.py`), the same in the real gate and the fake
one, over three kinds of fact: the state of the seller's handler workflow `job-<id>` as the
gate reads it from Temporal, the clock, and the buyer's accept, reject and cancel calls. No
party's report moves money. The ledger is double-entry; every event's postings sum to zero,
and the §9.3 invariants are a property test every ledger adapter must pass.

On `hire` the gate locks the price and signs a **hold token**: ES256, audience `kontra:hold`,
carrying the job, the seller's namespace, the offering, the amount, the contract hash, the
buyer's public key, and an expiry of lock time plus `max_duration`. The buyer's client sends
it in the `kontra-hold` Nexus header. The seller's handler verifies it with the gate's JWKS
(fetched at start, re-fetched only for an unknown key id, at a bounded rate), so no gate call
sits on the job-start path. A start after the expiry is refused, which refunds the job by the
deadline rule.

## Contracts and input

- **One implementation.** `kontra.contract` is the only code that derives, canonicalises (RFC
  8785), hashes or validates a contract; `publish`, `serve` and the gate all call it, so the
  hash lock cannot drift between implementations.
- **Closed schemas.** Every object carries `additionalProperties: false` (or a schema, for
  typed maps). The catalog refuses external `$ref` (resolving one is an SSRF), caps depth and
  size, and refuses reserved `Kontra*` definitions that differ from `spec/` by a byte.
- **Both sides validate.** The buyer's client validates input before encrypting it; the
  seller's handler validates it again before starting anything; the buyer validates the
  delivery against the output schema. Contract patterns run on RE2, in linear time.
- **Files are references.** A large input cannot arrive through the Nexus payload path; it is
  an `Artifact`, uploaded encrypted and bounded by `max_output_bytes` before a byte is stored.
- **Names are constructed, never parsed.** Task queues, Nexus service names and workflow ids
  are built from validated parts and never split back; Nexus endpoint names are opaque random
  strings, and the catalog is the only map from an endpoint to an offering. Nothing decides
  anything from the shape of a name. Typed input is validated with anchored `fullmatch` and
  length limits.
- **Seller text is hostile.** Rejection reasons, failure messages and progress text are printed
  with control characters removed. `kontra mcp` writes deliveries only into an absolute
  directory under your home or the temporary directory (or one you list in
  `KONTRA_MCP_DOWNLOADS`), never inside a hidden directory, owned by you all the way down, with
  files named by their gate-assigned key and an inert extension, mode 0600.

## Known limits

- **The internal frontend has no authentication.** Reaching it is full control of every
  namespace; its isolation is the network alone: loopback by default, and in `compose.yaml`
  Temporal's fixed address on the `control` network (a pinned subnet, `KONTRA_CONTROL_NET`),
  shared only with the gate. It does not listen on `temporal-db`, whose only other member is
  the Postgres relay; Postgres and the gate's migration job reach the database through that
  relay and cannot reach Temporal at all; the bucket's network and the published ports never
  carry it. A deployment that adds a container to `control` gives that container the
  internal frontend. The bind is not the whole wall on `temporal-db`: Linux accepts a packet
  for any of its addresses on any interface, so a peer there that routes Temporal's `control`
  address through Temporal's `temporal-db` address reaches it (measured); that takes
  `NET_ADMIN` (or `NET_RAW`), which the relay -- no capabilities, not root -- lacks. The host
  itself reaches every container address, the internal frontend's included, and on Docker
  before 28 a machine on the host's own network that routes the `control` subnet through the
  host may too, unless the host's `FORWARD` policy drops it; the address is the same on every
  install (`KONTRA_CONTROL_NET`.2).
- **Token lifetime.** A worker token's session revoked at the gate is refused by Kontra's
  Temporal server within its revocation poll (5 s), as long as the server can reach the
  gate's list (`GET /v1/auth/revocations`); while it cannot, the token stays valid until its
  `exp` (15 minutes at most). See decisions I19.
- **Kontra is the key directory** (see above).
- **The seller sees your input.** End-to-end means buyer to seller: the seller's worker
  decrypts the input to do the job. Hire sellers you are willing to show it to.
- **The seller's worker runs your input on its network.** An offering that fetches URLs you
  provide fetches them from the seller's machine; sellers must keep such workers away from
  anything private (see `examples/enrich`).
- **Disputes are judged on hashes** and on whatever each side chooses to decrypt for the
  resolver. Kontra cannot inspect a delivery on its own.
- **Rate limits** are per namespace; unauthenticated health calls are bounded only by the
  frontend's global limits and the proxy in front of it.
- **Agents act with your money.** `kontra mcp` gives an agent your balance up to each
  `max_price` it chooses. Its `quote` tool is marked read-only because it moves no money, but
  it sends the input to a seller: an MCP host that approves read-only tools without asking lets
  the agent disclose data to a seller without asking you.

## Supply chain

- The PyPI distribution is **`kontra-sdk`**. `kontra` on PyPI is an unrelated project; a bare
  `kontra` dependency installs someone else's code.
- Releases are published from CI by PyPI trusted publishing (OIDC, no token exists), with
  Sigstore attestations, and each GitHub release carries an SPDX SBOM.
- Every third-party GitHub Action is pinned by commit SHA. CI runs gitleaks on every push,
  pip-audit over the locked dependencies, Trivy on the built Temporal image, and zizmor on the
  workflows; dependabot proposes updates after a seven-day cooldown.
- The Temporal image is a static binary on distroless, running as non-root, pushed with a
  BuildKit SBOM and provenance.
