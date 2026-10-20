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
| A compromised seller machine repoints its offering (or anyone's) at itself | no token reaches an Operator or Admin API; endpoints are created by the gate for a person's confirmed login, and their names are opaque |
| One seller forges another seller's Nexus result | the callback guard (below) |
| A buyer reads, signals or cancels a seller's handler workflow | the Authorizer: a buyer's token has no call in the seller's namespace, including the job it paid for |
| A seller resets or deletes a workflow, or a buyer deletes its call records or shortens their retention, to change what escrow sees | the Authorizer: resets, deletes, namespace updates, batch operations and schedules are denied to every token |
| The gate's stream-relay credential leaks | the Authorizer holds it to one job's stream poll and offset query, until its session is revoked (within 5 s) or it expires (15 minutes): it reads that stream as ciphertext and cannot publish to it. It **can end that job**: every poll's result is written to the job's history, and enough polls push the job past Temporal's history size limit, which terminates it, a refund against the seller ([the stream relay](#the-stream-relay)) |
| The worker controller's credential leaks | rollouts of Kontra's own workers in the platform namespace: no seller namespace, no workflow start, no compute config |
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
| A stolen session adds a passkey of its own, to confirm every change after it | adding one needs a confirmation of the last five minutes, not the hour; the party is mailed which sign-in added it; a passkey confirms only sessions its holder already has, and never signs in |
| A copied passkey, or a page that is not the console, confirms a change | the gate checks the RP ID, the console's origin, user verification and a single-use challenge bound to the session; a use count that does not move forward revokes a passkey on one device and asks every sign-in to confirm again (a synced passkey's such answer is refused) |
| A stolen API key plants junk copies of job keys, or squats a new device's copy so the real one is never stored | a copy (a wrap) is stored only with the proof that its sender holds the job's key: the job's challenge, encrypted to that key and bound to the session asking, opened; copies are first-write-wins |
| A stolen session adds a device of its own, to receive the keys of every job | adding a device needs a confirmation of the last five minutes; the party is mailed which sign-in added it; every other device says it can now read the jobs before it copies it anything |

## What Kontra can and cannot see

Every payload is encrypted with [age](https://age-encryption.org) (X25519 recipients) by a
Temporal `PayloadCodec` on the parties' own machines, with recipients chosen from the
payload's serialization context (`docs/decisions.md` I7):

- the handler workflow `job-<id>` (its input, result and failures) is encrypted to the
  seller's recipients **and** the job's buyer key;
- everything else on the seller's side (activities, child workflows) to the seller's
  recipients only;
- the buyer's Nexus input to the seller's recipients and the job's own key.

A key does one job here, and it is not identity. A **buyer** makes a fresh keypair per job
at hire and sends the public half with the request; the gate signs *that* into the hold
token, so hiring needs no key setup and a job key reaches one job. The job key is then copied
to each of the account's **devices** -- a browser profile's key, a `kontra login` machine's
`~/.kontra/key` -- encrypted to that device alone, so a job hired in one place reads in every
other (decisions I25); the gate keeps those copies and cannot open them. A **seller** holds a set of recipients, one per deployment, registered with
`kontra recipients add` like `authorized_keys`; a fleet mounts one key into every worker
(`KONTRA_KEY_FILE`) so pod churn never changes the set. `kontra login` adds this machine as
one more recipient and decides nothing: adding never replaces, so a second machine cannot
make another machine's deliveries unreadable.

Payloads above 256 KiB of ciphertext go to the bucket, content-addressed by the ciphertext's
hash; every `Artifact` is stored there too, encrypted while it streams, under a key the gate
assigns. Ciphertext is standard age, so a party can decrypt what it received with the stock
`age` CLI and its key, which is how a dispute is argued.

| Kontra **cannot** read | Kontra **can** read |
|---|---|
| job inputs, results and failure messages (including stack traces) | a quote's answer: price, currency, a refusal's reason, a busy ETA (it must lock the price) |
| a quote's input (relayed byte for byte) | who hired which offering, when, at what price, and the job's state; the reason a buyer gives for rejecting a delivery (it is a gate call, not a payload) |
| the seller's heartbeat details (progress is encrypted to the buyer) | workflow metadata in Temporal: workflow ids, task queues, activity names, timers, timestamps, states, event counts, payload sizes; each task queue's backlog, rates and pollers (a worker's identity, `pid@host` by default, build id, and the SDK name and version its heartbeat names), which `kontra status --workers` shows the seller |
| artifacts and claim-checked payloads, including the plaintext sha256 an `Artifact` reference carries (it travels inside the encrypted delivery) | the size and sha256 of each ciphertext (age's overhead is fixed, so plaintext sizes follow) and each artifact's content type |
| any private key: every key is generated on the machine that holds it -- a buyer's per job, a seller's per deployment -- and leaves it only encrypted to one of the account's own devices (a job key's copies, decisions I25) | public keys (the seller's recipient set, the job's buyer key, the account's device keys and labels), contract documents, offering facts, balances |

Two consequences worth knowing:

- **Metadata is visible.** Activity names and the shape of a workflow's history are what
  progress liveness is built from. Do not encode secrets in workflow ids, activity names or
  Temporal search attributes (Temporal indexes search attributes; they are not payloads and are
  not encrypted).
- **Kontra is the key directory.** A buyer encrypts to the seller's recipients as the catalog
  reports them; a seller encrypts a delivery to the buyer key carried in the hold token Kontra
  signs; a device copies a job's key to the devices the gate lists for the account. Encryption therefore protects payloads from anyone who obtains Kontra's stored data --
  a database or bucket leak, Temporal history, a request for records -- and from anyone on the
  path. It does not protect against an operator that actively substitutes keys at hire time.
  Compare `QuoteResult.seller_recipients` with what the seller publishes elsewhere if that is
  in your threat model; a key-transparency log is not built. **This caveat grew with spec v2**:
  a contract used to seal the seller's key, so a substituted key disagreed with the sealed
  document. It no longer does -- recipients are catalog data, because a seller must be able to
  add a fleet key without republishing -- so the only cross-check left inside Kontra is that
  the quote and the hire name the same set.

## Identity and transport

- `kontra login` runs a device-code flow and stores a refresh token and an age identity in
  `$KONTRA_HOME` (default `~/.kontra`, files mode 0600). The public key is registered with the
  gate; the private key never leaves the machine. The device code is approved in the browser
  or through a mailed sign-in link whose page asks for the code only the requester's screen
  shows; opening the link consumes nothing (decisions I19).
- A program uses an **API key** (`KONTRA_API_KEY`), stored by the gate as a hash and exchanged
  for the same 15-minute tokens. Its session is never a person's: it cannot publish, change
  the party's recipients or devices, see or manage keys and sign-ins, or confirm. It is no
  device either: it copies the keys of the jobs it hires to the account's devices, and reads
  only those jobs. Revoking it ends its
  tokens on their next request.
- A person's **changes** (publishing, API keys, recipients, removing a device, ending a
  sign-in, resolving a dispute) need the login confirmed within the hour: a sign-in counts, and so does "Confirm
  it's you" -- a link mailed to the account's address whose page asks for a code only the
  asking screen shows, bound to that one session and never a sign-in itself. A stolen session
  token that asks gets a mail sent to its victim and nothing else (decisions I23).
- In the console, a **passkey** confirms in one tap instead (decisions I24): the gate verifies
  the browser's answer -- this console's origin and RP ID, user verification (biometric or
  PIN), a challenge used once and bound to the session that asked -- and keeps only the public
  key. A passkey confirms a sign-in the person already has and never signs anyone in. Adding
  one needs a confirmation of the last five minutes; revoking one asks the party's other
  sign-ins to confirm again; a use count that goes back (a copy of the device) revokes a
  passkey on one device, and is only refused for a synced one. Every revocation is mailed.
  The CLI has no passkeys; email stays everywhere.
- **Adding a device** (decisions I25) needs a sign-in or confirmation of the last five
  minutes, because a device is then copied the keys of past jobs too. Right after a sign-in
  that holds, so the console and `kontra login` add theirs without asking anything; the party
  is mailed every device added and removed.
- `kontra serve` (and a buyer's client) exchanges the session for a **15-minute** ES256 JWT,
  `permissions: ["<namespace>:worker"]`, audience `kontra:temporal`, refreshed in the
  background and swapped into the running Temporal client. Temporal's default claim mapper
  verifies it against the gate's JWKS. Revoking a session stops the refresh; a token already
  issued stays valid until it expires.
- Publishing is `POST /v1/catalog` with a person's **confirmed session**: a worker token or an
  API key never qualifies, so a stolen worker credential or key cannot publish or repoint anything.
- In production the public Temporal frontend sits behind a TLS-terminating proxy Kontra
  controls; TLS is always on. The public frontend refuses every Operator and Admin call to
  every token (the Authorizer); only the internal frontend, which is never published, accepts
  them. `kontra dev` is plaintext on loopback.

## The Authorizer

Kontra runs a self-hosted Temporal server (`temporal/authorizer`, public so you can read the
policy your worker runs under). The public frontend enforces this policy (PRD §10.4,
`docs/decisions.md` I3, I30). Every decision is exact string equality or set membership, and
no name or subject is ever split or parsed. One rule reads the request: the stream relay's,
which takes two typed fields of the message Temporal has already decoded (the workflow id, and
the update name or query type) and compares each, whole, with a value it constructs. A request
of any other type is denied.

Three kinds of token mean anything on the public frontend, and the Authorizer tells them apart
from the claims Temporal's default claim mapper produces, in this order: a token that carries
**a permission** is a party token, judged by the party rule alone whatever its subject says;
a token with no permission and the subject `platform/worker-controller` is the worker
controller; any other token with no permission is judged as a stream relay, and passes only if
its subject is exactly the one built from the request. A permission is a permission string the
mapper turns into a namespace key (any `<namespace>:<role>`, even an unknown role) or into a
system role it knows (`temporal-system:` with `read`, `write`, `admin` or `worker`). A string
the mapper drops is no permission: `temporal-system:` with a role it does not know, an entry
with no `:`, an entry that is not a string, a `permissions` claim that is not a list. A token
carrying only those is judged by its subject, like one that carries none.

So the kinds are told apart twice. The gate mints a party token with a permission and the other
two with none, each with its own mint and no role parameter; and a party's subject is its
handle, which can never contain `/`, while both other subjects do. The second holds whatever a
token's permissions say; the first only as long as the gate writes no permission the mapper
drops into a relay's or the controller's token.

| Caller on the public frontend | Allowed | Denied |
|---|---|---|
| no token | health, `GetSystemInfo`, `GetClusterInfo` | everything else |
| malformed token (expired, unknown key, `alg: none`, HS256 with the public key, wrong or missing audience, a gate API token, no session, a revoked session) | -- | everything, at claim mapping |
| **party token**: `worker` in namespace A. A seller's worker token, a seller's API-key session, a buyer and the dev party hold the same token; they are identical on the wire | an explicit allowlist of WorkflowService calls in A: task polling and responding, heartbeats, start / signal / signal-with-start / update-with-start / update / query / describe / history / cancel / terminate / list / count of A's own workflows, standalone Nexus operations in A, and `DescribeTaskQueue` on A's own queues (backlog stats for `temporal task-queue describe` and KEDA -- metadata, no payloads) | any other namespace, `temporal-system` and the platform namespace `kontra-system`; every OperatorService and AdminService call; the Nexus HTTP API; namespace admin, batch operations, schedules, resets, deletes, pause, workflow options, worker deployments and versioning rules, compute config |
| **stream relay** of job J in A: `sub = relay/A/job-J`, no permissions, minted by the gate only | `UpdateWorkflowExecution` with update name `__temporal_workflow_stream_poll`, and `QueryWorkflow` with query type `__temporal_workflow_stream_offset`, on workflow `job-J` in A only | every other call, on J or anywhere: the stream's publish signal, any other update or query (`__stack_trace` included), `PollWorkflowExecutionUpdate`, describe, history, start, cancel, terminate, update-with-start; both handlers on any other workflow of A, and on J's id in any other namespace |
| **worker controller**: `sub = platform/worker-controller`, no permissions, minted by the gate only | ten calls, in the platform namespace `kontra-system` only: describe, promote, ramp, manage, annotate and delete worker deployments and their versions, `DescribeTaskQueue`, `CountWorkflowExecutions` | every call in any other namespace; in `kontra-system`, every other call: no task, no workflow start, describe, signal, update or query, no other deployment or versioning call, no compute config |
| any other role (`admin`, `write`, `read`, an unknown role in a namespace, `temporal-system:` with `read`, `write`, `admin` or `worker`), even next to `worker`, and whatever the token's subject (a relay's or the controller's included) | nothing beyond what `worker` in A grants | everything those roles grant on stock Temporal |
| a permission string the mapper drops (`temporal-system:` with an unknown role, no `:`, not a string, a claim that is not a list) | no permission: judged by the token's subject, so the controller's or a relay's subject gets exactly that principal's calls | as for that subject |
| the control plane | everything | -- (it exists only on the internal listener) |

A party has exactly one role and it is deliberately weak: a compromised seller machine yields
that seller's running jobs, not the ability to repoint `acme/enrich` elsewhere. Defence in
depth, pinned in dynamic config and re-asserted by the server at runtime whatever the file
says: cross-namespace commands off, task tokens bound to their namespace, the streaming
authorizer on, and the Nexus headers that carry a callback token, request id or links stripped
from callers (otherwise a buyer could hand a seller's handler values of its choosing).
The server refuses to start if the dynamic config file contradicts any of these.

**Tested as the required check `deny-matrix`** (`make deny-matrix`): the Go deny matrix runs
every cell, each with the typed request Temporal would hand the Authorizer, against Kontra's
authorizer *and* against Temporal's own two authorizers, to show each denied cell is allowed
by default and denied only because of this policy. A sweep runs all 191 surfaces the frontend
hands the Authorizer (every WorkflowService, OperatorService and AdminService method, health,
both reflection versions and the Nexus HTTP routes) against every kind of caller, namespace and
request, favourable and hostile, against the rule restated independently in the test
(`go test -run TestSweep -v` prints its size); the relay subject's construction is checked
against golden vectors shared with `kontra.contract.names`. Then `tests/temporal` proves the
same cells over the wire against the real server binary with real `temporalio` clients: the
positive hiring path, a relay polling its job's stream with only its two calls, the controller
rolling out a versioned platform worker, a seller steering its own jobs, and every escrow
denial.

### What a party's own token can do to its own jobs, and why escrow doesn't care

A seller steers its own workflows: it can update, query and signal them (including signals
between its own workflows, as a workflow stream's publisher does), and cancel or terminate
them. It runs that code; its own stream subscriptions, the dev console's session pane and stock
`temporalio` update handles need exactly these calls. Escrow reads none of what they can
write: a job's facts are the server-recorded status of its pinned run chain and that chain's
first run id (set only by the server), provenance read from the **buyer's** records, the clock,
and the buyer's gate calls. A seller that terminates or cancels its own job is refunded
against; a seller's cancel cannot pass for the buyer's, because who canceled is read from the
buyer's side. Buyers never hold a token for a seller's namespace, so nobody else can steer a
seller's job. (The PRD's "only the gate starts or steers a workflow there" describes the dev
console and buyers, not this policy: a seller can always steer its own job.)

What escrow does depend on is denied to every token, and must stay denied:

- **Reset.** A reset can bring a terminated run back to life inside the same chain, with its
  completion callbacks: its first run id comes from the replayed history. A seller that reset
  its job after a buyer's cancel had terminated it, and completed the new run, would turn the
  refund the cancel earned into a delivery.
- **Deletes and retention.** Provenance is read from the buyer's records, and a missing record
  is unverified, never a delivery: a buyer that deleted its caller workflow or standalone
  operation, or shortened its namespace's retention, would turn a real delivery into a refund
  counted against the seller. In a seller's namespace, deleting the placeholder a cancel leaves
  reopens cancel-then-start, and deleting history erases what a dispute is argued from.
- **Batch operations and schedules.** Both act as Temporal's internal principal, past this
  policy and the callback guard; a batch reset is the reset above, and a schedule outlives a
  revoked session.
- **Compute config** (`CreateWorkerDeploymentVersion`, `UpdateWorkerDeploymentVersionComputeConfig`,
  `ValidateWorkerDeploymentVersionComputeConfig`) makes the server provision and invoke workers
  on a cloud provider with its own identity. No token has it, the worker controller included.

Pause, workflow options and the activity, worker and workflow-rule operations are denied for
want of a consumer. **Worker Versioning stays denied to sellers in v1**: a seller who turns on
`use_worker_versioning` gets no tasks, because it cannot make its version current.

### The stream relay

The gate relays a job's workflow stream to its buyer over SSE. It does not use the internal
frontend for that, where any bug in an internet-facing process would be full control of
Temporal: it dials the public frontend with a token scoped to one job's stream. The token's
subject is `relay/<seller namespace>/job-<job id>` (`kontra.contract.names.relay_subject`), it
carries no permission, and its `sid` names a relay session the gate revokes when the job closes
or the last listener leaves; the server then refuses it within its revocation poll (5 s), and
it expires within 15 minutes in any case.

The Authorizer accepts exactly two requests from it. An `UpdateWorkflowExecutionRequest` under
`UpdateWorkflowExecution`, whose update name is `__temporal_workflow_stream_poll`, and a
`QueryWorkflowRequest` under `QueryWorkflow`, whose query type is
`__temporal_workflow_stream_offset`; and in both, the token's subject must equal, whole,
`relay/` + the request's namespace + `/` + the request's workflow id. Nothing is split: a
namespace is a handle and a job's workflow id is `job-<32 hex>`, neither holds a `/`, so only
the job the subject was built from rebuilds it. Any other request type, a request whose type is
not its API's, an update inside `ExecuteMultiOperation`, and a stream handshake (no request)
are denied. Run ids, update ids, payloads, headers and callbacks are not read; callbacks are
the callback guard's, which applies to the relay as to every other token.

The relay needs nothing else. Re-sending its Update with the same update id returns that
update's outcome, and with an empty run id it targets the current run, so it follows a
continue-as-new; once escrow has pinned the job's chain, the gate sets that run as the
request's first execution run id and Temporal refuses a look-alike chain. It is denied
`PollWorkflowExecutionUpdate`, which names no update and would read any update's outcome by
id, and `DescribeWorkflowExecution`; the stock `WorkflowStreamClient.subscribe()` uses both,
and is refused. One residual risk is Temporal's: an Update that presents another update's id
receives that update's outcome, because the server finds an update by its id alone. Update ids
are random ids the relay never sees, and every outcome is ciphertext to the job's parties. The
relay never sends the publish signal, so it cannot inject events, and is denied the built-in
queries (`__stack_trace`) that would show a workflow's internals. A stream must be hosted by
the job's handler workflow itself: the relay reaches `job-<id>` and nothing else.

**What the relay's polls cost the job.** Every poll the relay completes is written to the job's
history: Temporal stores a completed Update's outcome in the history
(`WorkflowExecutionUpdateCompleted`), and a poll's outcome is the batch of stream items it
returns, up to the stream's response cap of about 1 MB. A genuine relay therefore doubles a
stream's volume in history (each item once in the publish signal that wrote it, and again in
the poll that read it) and spends one of the run's updates per poll (`history.maxTotalUpdates`,
Temporal's default of 2,000; Kontra does not pin it). A leaked relay token can spend them on
purpose. Polling from offset 0 again and again, each time with a new update id, it appends up
to 2,000 completed updates of up to about 1 MB each, and once the history passes
`limit.historySize.error` (20 MiB, pinned in `dynamicconfig/kontra.yaml`) Temporal terminates
the run. Escrow then refunds the job as "seller workflow terminated", against the seller. Any
stream above about 10.5 KB is enough; one of about 1 MB takes about twenty polls
(`tests/temporal`: `test_relay_polls_alone_can_end_their_job`). The token stays live while a
buyer listens, because its session is revoked only when the job closes or the last listener
leaves.

The Authorizer cannot bound this, because it does not read payloads. Three mitigations are
recorded and none is adopted yet (decisions I30). A handler workflow that hosts a stream can
continue-as-new when `workflow.info().is_continue_as_new_suggested()`: that turns the
termination into churn, because escrow follows the pinned chain and the relay's empty run id
follows its current run. The gate's relay (#52) polls only from the tail, with topics: that
keeps a genuine relay's share of history to what its buyer reads, but does nothing against a
leaked token, which picks its own offset. A per-namespace `history.maxTotalUpdates` pin bounds
it only together with continue-as-new: 20 MiB at 1 MB a poll is about twenty updates, which
would also cap a genuine subscription's polls per run.

### The worker controller

Kontra's own workers are rolled out by `temporal-worker-controller` in Kontra's own namespace,
`kontra-system` (`PlatformNamespace`, a constant in the public policy and a reserved handle no
party can take). Its token has the subject `platform/worker-controller` and no permission, is
minted by the gate alone for the Kubernetes Secret the controller reads, lives 15 minutes and
names a session the gate can revoke. It may make the ten calls it needs (describe,
promote, ramp, manage, annotate and delete worker deployments and versions, check their task
queues for pollers, count the pinned workflows still running), in `kontra-system` only. It
cannot start or describe a workflow there (rollout gate workflows are not supported), touches
no seller namespace, and never compute config. Kontra's own workers in `kontra-system` hold
ordinary `kontra-system:worker` party tokens from a platform-only mint: the party rule, with
no exception for them.

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
  the Postgres relay (and, with `kontra dev up --observability`, Prometheus, held to the
  same terms); Postgres and the gate's migration job reach the database through that
  relay and cannot reach Temporal at all; the bucket's network and the published ports never
  carry it. A deployment that adds a container to `control` gives that container the
  internal frontend; `kontra dev up --ui` does so on purpose with Temporal's UI, a dev tool
  published on 127.0.0.1 only that production never runs (as is the codec server it starts,
  which decrypts with the machine's key for whoever reaches its loopback port, and serves the
  graph page, reading the dev party's runs with its worker token). The bind is
  not the whole wall on `temporal-db`: Linux accepts a packet
  for any of its addresses on any interface, so a peer there that routes Temporal's `control`
  address through Temporal's `temporal-db` address reaches it (measured); that takes
  `NET_ADMIN` (or `NET_RAW`), which the relay -- no capabilities, not root -- lacks. The host
  itself reaches every container address, the internal frontend's included, and on Docker
  before 28 a machine on the host's own network that routes the `control` subnet through the
  host may too, unless the host's `FORWARD` policy drops it; the address is the same on every
  install (`KONTRA_CONTROL_NET`.2).
- **A leaked stream-relay token can end its job.** Its two calls are enough: every poll's
  result is written to the job's history, and enough of them push the job past Temporal's
  history size limit, which terminates it, a refund against the seller. The Authorizer
  cannot bound it (it does not read payloads); see [the stream relay](#the-stream-relay) for
  the numbers and the mitigations recorded in decisions I30.
- **Token lifetime.** A worker token's session revoked at the gate is refused by Kontra's
  Temporal server within its revocation poll (5 s), as long as the server can reach the
  gate's list (`GET /v1/auth/revocations`); while it cannot, the token stays valid until its
  `exp` (15 minutes at most). See decisions I19.
- **Kontra is the key directory** (see above), and spec v2 widened that: a contract no longer
  seals a seller key, so nothing outside Kontra contradicts a substituted one.
- **A job's key reaches every device of the account, and stays there** (decisions I25). The
  machine or browser that hires copies the job's key to each device, encrypted to it alone,
  with the proof that it holds the key; a device added later is copied the keys of past jobs
  by whichever device holding them is online next -- until then it cannot read them. `kontra
  job <id> --accept` shreds *this machine's key file* only: the ciphertext the gate and the
  bucket hold stays readable by the account's devices -- this machine too, from its copy at
  the gate -- while the job exists, where before sharing it was readable by nobody. Removing a device stops new copies and deletes the copies the gate
  kept for it; **keys it already opened cannot be taken back** (job keys are never rotated:
  the seller encrypted to them). Jobs whose key is or was one of the party's recipients --
  every job from before per-job keys -- are never copied.
- **The gate is the directory for devices too.** A device copies a job's key to the device
  keys the gate lists for the account. A gate that listed a key of its own would get copies;
  every browser shows a device it has not shown before ("can now read your jobs") before it
  copies it anything, the CLI says so on stderr, `kontra mcp` in its next answer (copying
  nothing to it until then), and a library through its `on_new_device` hook, by default only
  a logged warning; but nothing signs the device set (the
  same limit as recipients; signed device sets -- a key per device, each addition co-signed by
  an existing device or a passkey -- are future work). A session stolen within five minutes of
  a sign-in or a confirmation can add a device; the party is mailed which sign-in added it,
  and removing it deletes its copies, but not what it fetched meanwhile.
- **Revoking a recipient does not reach backwards.** Deliveries already encrypted to a lost
  machine's key stay encrypted to it. `kontra recipients revoke` closes that key's future, and
  nothing more; what closes its past is the open jobs being accepted and their keys shredded.
- **A session stolen within five minutes of a confirmation can add a passkey** -- about
  eleven minutes in effect: a ceremony started at the end of those five lives six more, and
  finishing it needs only the hour. It still confirms only sessions the thief holds, the party
  is mailed which sign-in added it, and revoking the passkey or ending that sign-in ends it;
  but until then the thief confirms past the hour. A copy of a synced passkey (a password
  manager's, backup eligible) is not detected: its answers behind the count are refused, never
  treated as a clone, and its holder's own password manager is the boundary (decisions I24).
- **Nothing signs a recipient set.** Any confirmed session of the party can add a recipient,
  so a stolen session, inside an hour its person confirmed, can add a key it controls and read
  everything delivered afterwards.
  Making the first seller key an ed25519 identity and requiring each addition to be signed by
  an existing one would close this; the set makes that a clean upgrade rather than a redesign.
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
