# Kontra's Temporal server and Authorizer

This is the Temporal frontend of the Kontra marketplace: a self-hosted Temporal server
(every service in one process) whose public frontend enforces the Kontra authorization
policy (PRD §10.4, `docs/decisions.md` I3). It is the blocking Phase 0 isolation spike
(PRD §15): if a cell of the deny matrix could not be enforced, this package would say so.
Every cell can be enforced.

- `authorizer.go` — the policy, a `temporal.authorization.Authorizer`. Every allow justified
  in a comment, no string parsing; decisions are exact string equality and set membership
  only. One rule reads the request: the stream relay's, which takes two typed fields of the
  message Temporal already decoded and compares each, whole, with a value it constructs, then
  compares the whole message's encoded size with `RelayRequestMax` (4 KiB), one integer
  comparison, then its payloads' count and metadata with the one shape the gate's relay sends
  (nothing decoded), and holds the relay subject to `RelayRate` requests a second, the policy's
  only state (decisions I30).
- `callbackguard/` — the one check the Authorizer cannot make: a Nexus completion callback
  token may only complete the operation it was issued for (see below). Separate on
  purpose: it validates what a request carries, the policy decides who may make a call.
- `revocation/` — refuses the worker tokens of sessions the gate revoked (see below): a
  wrapper around Temporal's default claim mapper that only ever refuses more.
- `cmd/kontra-temporal` — the server binary (`serve`, `setup-schema`, `register-namespace`).
- `dynamicconfig/kontra.yaml` — Nexus features on, cross-namespace commands off, per-namespace
  limits.
- `authorizer_test.go` — the deny matrix as a Go table, run against Kontra's authorizer and
  against Temporal's own two authorizers to show each denied cell fails by default.
- `../../tests/temporal` — the same policy proven over the wire with real `temporalio`
  clients, plus the positive hiring path.

## The enforced policy

Two listeners. The **internal frontend** is never published; Temporal maps every caller
there to its own internal principal (the gate, and Temporal itself), which may do anything.
The **public frontend** is the only one reachable from outside (behind Kontra's TLS proxy),
and every caller there presents a short-lived JWT verified by Temporal's default claim
mapper against Kontra's JWKS. Three kinds of token mean anything there (`docs/decisions.md`
I30), told apart in this order:

1. a **party token** carries a permission -- a permission string the default mapper turns
   into a namespace key (any `"<namespace>:<role>"`, even an unknown role) or into a system
   role it knows (`temporal-system:` with `read`, `write`, `admin` or `worker`) -- and the
   one that grants anything is `"<namespace>:worker"`. A seller's worker token, a seller's
   API-key session, a buyer and the dev party all hold this token; they are identical on the
   wire. A token with a permission is judged by this rule alone, whatever its subject says;
2. the **worker controller** (KON-15) carries no permission and the subject
   `platform/worker-controller` (`ControllerSubject`);
3. the gate's **stream relay** carries no permission and the subject
   `relay/<namespace>/job-<32 hex>` (`RelaySubject`, the twin of
   `kontra.contract.names.relay_subject`).

A permission string the mapper drops is no permission: `temporal-system:` with a role it
does not know (no role, and no namespace key), an entry with no `:`, an entry that is not a
string, a `permissions` claim that is not a list. A token carrying only those is judged by
its subject, like one that carries none.

So the kinds are told apart twice. The gate mints a party token with a permission and the
other two with none (each principal has its own mint, with no role parameter); and a party's
subject is its handle, which never contains `/`, while both other subjects do. The second
holds whatever the token's permissions say; the first only as long as the gate writes no
permission the mapper drops.

| Caller (on the public frontend) | Allowed | Denied |
|---|---|---|
| **No token** | `Health/Check`, `GetSystemInfo`, `GetClusterInfo` | everything else |
| **Malformed token** (expired, unknown `kid`, `alg:none`, HS256-with-public-key, wrong or missing audience, a gate API token) | — | everything (rejected at claim-mapping, before the authorizer) |
| **`worker` in A** (a party token: seller, buyer, API-key session, dev party) | the WorkflowService allowlist below, in namespace **A** only, including updates, queries, signals, cancel and terminate of A's own workflows | any namespace B, `temporal-system` and the platform namespace `kontra-system`; every OperatorService / AdminService call; the Nexus HTTP API (`DispatchNexusTask`, completions); namespace admin, batch, schedules, resets, deletes, pause, workflow options, worker deployments and versioning, compute config |
| **stream relay** for job J of A (`sub = relay/A/job-J`, no permissions) | `UpdateWorkflowExecution` with update name `__temporal_workflow_stream_poll`, and `QueryWorkflow` with query type `__temporal_workflow_stream_offset`, on workflow `job-J` in namespace **A** only, each request at most `RelayRequestMax` (4 KiB) encoded, shaped as the gate's relay builds it (a poll's one `json/plain` argument, no header; the query none), at most `RelayRate` (10) a second | every other API (`PollWorkflowExecutionUpdate`, describe, history, signal — the stream's publish signal included — start, `ExecuteMultiOperation`); any other update or query (`__stack_trace` included); any other workflow of A; job J's id in any other namespace; a poll or offset query over 4 KiB, whatever field (topics, identity, header, arguments) makes it so; any other argument shape or a header; past its rate |
| **worker controller** (`sub = platform/worker-controller`, no permissions) | the 10 worker-deployment, task-queue and count calls in `ControllerAPIs`, in the platform namespace `kontra-system` only | every call in any other namespace; in `kontra-system`: polls, start, describe, signal, update, query, terminate, reset, delete, batch, schedules, `ListWorkerDeployments`, `CreateWorkerDeployment`, the three compute-config calls, versioning rules |
| **any other role** (`admin`, `write`, `read`, an unknown role in a namespace, `temporal-system:read`/`write`/`admin`/`worker`), even alongside `worker`, whatever the token's subject (a relay's or the controller's included) | nothing beyond what `worker` in A grants | everything those roles would grant on a stock Temporal — including all Operator/Admin APIs |
| **a permission the mapper drops** (`temporal-system:` with an unknown role, no `:`, not a string, a claim that is not a list) | no permission: the token is judged by its subject, so the controller's or a relay's subject gets exactly that principal's calls | as for that subject |
| **buyer `worker` in C** | start/describe its own workflows and standalone Nexus operations in C; call any endpoint via Nexus | describe / query / list / signal / cancel / poll **anything** in a seller's namespace A, including the handler workflow its own job started |
| **control plane** (internal frontend only) | everything: create namespaces and Nexus endpoints, watch jobs, history's Nexus dispatch and completions | — (only reachable on the private network) |

The WorkflowService allowlist (`AllowedAPIs` in `authorizer.go`) is exactly what the Python
SDK's workers and clients use: task polling and responding (workflow, activity, Nexus,
queries), activity heartbeats, worker heartbeat/shutdown, start / signal / signal-with-start
/ update / query / describe / get-history / cancel / terminate / list / count of workflows,
`ExecuteMultiOperation`, `DescribeNamespace`, and the standalone Nexus operation APIs
(start / describe / poll / cancel / terminate / list / count). Nothing administrative is on
it.

`DescribeTaskQueue` is on it too, for a different reason: it is how a seller reads the
backlog of its *own* task queue. With `report_stats` it returns `ApproximateBacklogCount`,
`ApproximateBacklogAge`, `TasksAddRate` and `TasksDispatchRate` per task-queue type —
metadata, never a payload — and it is scoped to the token's namespace like everything else
here. Without it `temporal task-queue describe` fails for sellers and KEDA's Temporal
scaler, which calls exactly this RPC, cannot scale workers against Kontra's frontend. It
sits in the same class as `DescribeNamespace` and `CountWorkflowExecutions`.

A party steers its own workflows: updates, queries and signals (between its own workflows
too), cancel and terminate. Escrow reads nothing these can write: a job's facts are the
server-recorded status of its pinned run chain, provenance from the buyer's own records, the
clock and the buyer's gate calls, and a seller that stops its own job is refunded against.
What escrow does depend on is denied to every token: a reset (it can revive a terminated
pinned run, completion callbacks included), deletes of workflows and Nexus operations and
namespace updates (a buyer's missing record turns a delivery into a refund), batch
operations and schedules (they act as Temporal's internal principal, past this policy and
the callback guard), and compute config (the server would provision and invoke workers on a
cloud provider itself).

### The stream relay and the worker controller

The relay's rule is the only one that reads a request. For an `UpdateWorkflowExecutionRequest`
under `UpdateWorkflowExecution`, or a `QueryWorkflowRequest` under `QueryWorkflow` — any
other request type, none (a stream), or a type under another API's name is denied — it takes
the workflow id and the update name or query type from the typed message Temporal already
decoded, requires the handler name to equal its constant, and compares the token's subject,
whole, with `RelaySubject(request namespace, request workflow id)`. Nothing is split or
unescaped: a namespace is a handle and a job's workflow id is `job-<32 hex>`, neither holds
a `/`, so only the job the subject was built from rebuilds it. Run ids, update ids, the data
of payloads and callbacks are not read, only counted: next, a request whose encoded size
(`proto.Size` of the whole message) is over `RelayRequestMax`, 4 KiB, is denied. The largest
the gate's relay builds is 898 bytes (`TestRelayRequestMax`); the cap keeps what a relay token's requests can
add to one workflow task of its job far below Temporal's 4 MiB gRPC message limit, and does
not apply to a party's own requests. Callbacks are the callback guard's, which applies to the
relay and the controller as to every other token. A relay needs nothing else: re-sending its
Update with the same update id returns the outcome, and with an empty run id it follows a
continue-as-new. `PollWorkflowExecutionUpdate` is denied because it names no update and
would read any update's outcome by id. Every completed poll's outcome is written to the job's
history, so a leaked relay token's polls spend it; the size cap holds requests, not answers,
and the authorizer never sees an answer, so it cannot bound that: the job's side does
(`docs/security.md`, decisions I30).

Then the request's shape: a poll's one argument with metadata exactly `{encoding: json/plain}`
and no header, the offset query neither (`relayShaped`; the count and metadata are compared,
a payload's data is never read). Every payload of a request is decoded by the seller's worker
(Kontra's codec, temporalio's external storage) before the job's validator can refuse it, and
one it cannot decode — a Kontra ciphertext, a claim-check reference it would fetch — fails the
job's workflow task on every attempt, so only the shape the gate's relay sends may reach it
(deploy the rule before, or with, the first relay token a gate mints). Last, the rate: each
relay subject is held to `RelayRate` (10) requests a second (`RelayBurst` 20, per frontend
process; Kontra runs one), refused past it with `RelayRateReason`, on which a relay waits
instead of stopping. Only a request every other check allowed spends the rate, and the
authorization interceptor runs before Temporal's namespace rate limiter, so a relay token
spends at most 5% of its namespace's 200 requests a second. The rate slows what a leaked relay
token's polls add to its job's history; it does not bound it. Both were decided in the owner's
place and are listed on #59 (decisions I30, Q10 and Q11).

The controller is `temporal-worker-controller` rolling out Kontra's own workers. It may make
the 10 calls in `ControllerAPIs` (describe, promote, ramp, manage, annotate and retire worker
deployments and versions, check their queues for pollers, count pinned workflows still
running), in the platform namespace `PlatformNamespace` (`kontra-system`, a reserved handle
no party can take) only. It cannot start or describe a workflow (rollout gate workflows are
not supported), and no token may touch compute config. Kontra's own workers in that
namespace hold ordinary `kontra-system:worker` party tokens.

### Why there is no publisher role

The PRD's `publisher` role — a token allowed to create a Nexus endpoint whose name started
with its namespace — is gone (`docs/decisions.md` I3). It was a prefix test of a name an
attacker chooses, and endpoint names are now opaque random strings that mean nothing (I1).
(The relay's rule is the opposite: whole-string equality with a string it builds.) Publishing is a gate call with a fresh human session; the gate then creates the
endpoint itself through the internal frontend. So **no JWT ever reaches an Operator or Admin
API**, with no exception, and the authorizer has no name-parsing rule to get wrong.

### Completion callbacks (`callbackguard`)

A Nexus handler completes an asynchronous operation by starting a workflow whose completion
callback is `temporal://system` plus the callback token Temporal handed it. The token is
unsigned and names the caller's namespace, workflow, run and operation; the completion then
reaches the caller's namespace as Temporal's internal principal, which the Authorizer has to
allow. Temporal v1.32.1 compares the token's request id with the operation's **only when the
token has one**. Without the guard, any seller hired by the same buyer workflow as acme
could rewrite its own token to name acme's operation (event ids are small integers), drop
the request id, and deliver any result as acme's — a wire probe did exactly that before
the guard existed (the working probe is kept private, outside this repository). The same
works against a standalone operation whose id the forger knows.

`callbackguard.Intercept` runs on both frontends after authorization. For every caller but
the internal principal it accepts a completion callback, on any API that can carry one
(StartWorkflowExecution, ExecuteMultiOperation, UpdateWorkflowExecution,
StartActivityExecution, StartNexusOperationExecution — found by walking the request's type,
not by a list), only in the
shape a handler needs: a Nexus callback to exactly `temporal://system`, with exactly one
token, whose request id is set. A token then completes only its own operation: the request
id is a UUID that only that operation's caller and handler know. Every other URL is refused
too, because a callback is an HTTP request Temporal itself makes and v1.32.1 does not
validate the URL on every API (UpdateWorkflowExecution skips it).

### Revoked sessions (`revocation`)

Temporal's default claim mapper checks a token's signature, expiry and audience, and cannot
know that the gate revoked the session the token was minted from: a revoked API key's worker
token kept polling the party's task queues and starting workflows for up to its 15 minutes.
Every token the gate mints names its session (`sid`), and the gate publishes the sessions it
revoked in the last 20 minutes as sha256 digests (`GET /v1/auth/revocations`). `revocation.List`
polls that list (`KONTRA_REVOCATIONS_REFRESH`) and `revocation.Check` wraps the default mapper
on the public frontend: after the mapper accepts a token, a token whose `sid` is listed, or
that names none, is refused `PERMISSION_DENIED`. If the list cannot be fetched, the last one
stays in force and the failure is logged: revocations happen at the gate, and stopping every
worker whenever the gate is unreachable would turn a gate outage into a Temporal one. A gate
that answers 404 publishes no list (it predates it): nothing is refused, and that is logged.

### Defence in depth (dynamic config, pinned: checked at startup, enforced while running)

- `system.enableCrossNamespaceCommands: false` — a workflow task cannot start a child,
  signal or cancel in another namespace. A worker of C therefore cannot reach A from
  `RespondWorkflowTaskCompleted`. (The Python SDK can't even express a cross-namespace
  command; this closes the gRPC path regardless.)
- `frontend.enableTokenNamespaceEnforcement: true` — a task token minted in B is refused in a
  request naming A.
- `system.disableStreamingAuthorizer: false` — streaming RPCs go through the authorizer too.
- `component.nexusoperations.disallowedHeaders` / `nexusoperation.disallowedHeaders` —
  Temporal's defaults plus `nexus-callback-temporal-callback-token`, `nexus-request-id` and
  `nexus-link`: Temporal sets a caller's Nexus headers after its own, so a buyer could
  otherwise hand a seller's handler a callback token, request id or links of its choosing.

`kontra-temporal` refuses to start if the dynamic config file sets any of these to anything
else, uses an unknown key, or gives a value of the wrong type. While it runs it serves these
settings from its own code whatever the file says: Temporal re-reads the file every poll and
would apply a change, and the gate edits that file (I12).

The JWT audience is always checked (`kontra:temporal` by default). The gate signs API, hold
and browser tokens with the same keys as worker tokens, and Temporal's claim mapper skips
the audience check when none is configured.

## Build

```sh
# Local binary (reuses a warm Go build cache; links the host libc). For tests/temporal and
# the local Docker image.
GOFLAGS=-p=1 ./build.sh          # -> bin/kontra-temporal

# Go tests: the deny matrix and the config checks.
GOFLAGS=-p=1 go test ./...

# The fast local image (needs bin/kontra-temporal from build.sh; base is Ubuntu 24.04).
docker build -f Dockerfile.local -t kontra-temporal:dev .

# The shipped image (static, -trimpath, distroless; build from this directory).
docker build -f Dockerfile -t kontra-temporal:<tag> .
```

`GOFLAGS=-p=1` keeps the server compile inside a small machine's memory. A cold build of
`go.temporal.io/server` is large; detach any build over ~10 minutes.

## Run

Configured entirely by environment variables; sane defaults run SQLite in memory on
loopback.

```sh
# In-memory SQLite, loopback only:
bin/kontra-temporal serve

# Register namespaces through the internal frontend (tests/dev; the gate does this in prod):
bin/kontra-temporal register-namespace acme buyer

# Postgres, creating/upgrading the schema first:
KONTRA_DB=postgres KONTRA_DB_HOST=... KONTRA_DB_PASSWORD=... KONTRA_AUTO_SETUP=1 \
  bin/kontra-temporal serve
# or set the schema up and exit:
KONTRA_DB=postgres ... bin/kontra-temporal setup-schema
```

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `KONTRA_PORT_BASE` | `7233` | base gRPC port; frontend = base, internal frontend = base+3 |
| `KONTRA_FRONTEND_PORT` / `KONTRA_INTERNAL_FRONTEND_PORT` | base+0 / base+3 | override a service's gRPC port |
| `KONTRA_HISTORY_PORT` / `KONTRA_MATCHING_PORT` / `KONTRA_WORKER_PORT` | base+1 / +2 / +6 | internal service gRPC ports (loopback) |
| `KONTRA_FRONTEND_BIND_IP` | `127.0.0.1` | bind address (an IP address) of the **public** frontend |
| `KONTRA_INTERNAL_FRONTEND_BIND_IP` | `127.0.0.1` (images too) | bind address (an IP address) of the **internal** frontend, which has no authentication. Set it only to a private interface the gate shares and nothing else does: a VPC address, or in compose Temporal's fixed address on the `control` network (`KONTRA_CONTROL_NET`.2, on a pinned subnet), so the internal frontend does not answer on Temporal's other networks; never publish its ports. The server dials it at this address too (see below) |
| `KONTRA_FRONTEND_HTTP_PORT` | `0` (off) | public HTTP API port; left off because the SDKs use gRPC and every HTTP route is denied to tokens |
| `KONTRA_INTERNAL_FRONTEND_HTTP_PORT` | base+13 | internal HTTP port — required (history delivers Nexus tasks and completions here; the gate relays quotes here) |
| `KONTRA_DB` | `sqlite` | `sqlite` or `postgres` |
| `KONTRA_SQLITE_FILE` | *(empty = in memory)* | SQLite database file |
| `KONTRA_DB_HOST` / `KONTRA_DB_PORT` / `KONTRA_DB_USER` / `KONTRA_DB_PASSWORD` | `127.0.0.1` / `5432` / `temporal` / *(empty)* | Postgres connection |
| `KONTRA_DB_NAME` / `KONTRA_DB_VISIBILITY_NAME` | `temporal` / `temporal_visibility` | Postgres database names |
| `KONTRA_DB_TLS`, `KONTRA_DB_TLS_CA` | off | Postgres TLS |
| `KONTRA_AUTO_SETUP` | off | create/upgrade the Postgres schema at startup |
| `KONTRA_HISTORY_SHARDS` | `1` (sqlite) / `4` (postgres) | history shards (fixed for the life of a database) |
| `KONTRA_JWKS_URLS` | *(empty)* | comma-separated JWKS URIs (`https://…` or `file://…`); empty rejects every token |
| `KONTRA_JWKS_REFRESH` | `1m` | JWKS refresh interval |
| `KONTRA_JWT_AUDIENCE` | `kontra:temporal` | required JWT `aud`; always checked (empty means the default) |
| `KONTRA_REVOCATIONS_URL` | the gate of the first JWKS URL: `http(s)://host/.well-known/jwks.json` gives `http(s)://host/v1/auth/revocations`; none for any other JWKS URL | the gate's list of revoked sessions; `off` runs without one (a revoked session's worker tokens then work until they expire) |
| `KONTRA_REVOCATIONS_REFRESH` | `5s` | how often the list is fetched |
| `KONTRA_TEMPORAL_DYNAMIC_CONFIG` | *(built-in default)* | dynamic config file to poll (the gate writes per-namespace overrides here) |
| `KONTRA_DYNAMIC_CONFIG_POLL` | `10s` | how often the server re-reads the dynamic config file |
| `KONTRA_LOG_LEVEL` / `KONTRA_LOG_FORMAT` | `info` / `json` | logging |
| `KONTRA_METRICS_ADDR` | *(off)* | Prometheus listen address (`/metrics`); its labels name every party's namespaces and task queues, so keep it private |

Internal services (history, matching, the system worker) always bind loopback and have no
variable to change that: they run in this one process and nothing outside it should reach
them.

The server reaches each of its own services at exactly the address that service binds: the
system worker, history's Nexus dispatch and completions and `register-namespace` all go
through the internal frontend, at `KONTRA_INTERNAL_FRONTEND_BIND_IP` exactly as given and its
ports. So a private address works as is (the server needs no route to it but its own
interface), and so does `0.0.0.0`, which Go dials as the local system. Both bind addresses
must be IP addresses: a host name is refused at start, as Temporal would refuse to bind it.
`0.0.0.0` listens on every interface the server has: on a host, the public one included; in a
container, every network it joins, whatever else is on them (compose's Postgres relay was, on
`temporal-db`). Prefer the one private address the gate uses: compose pins the `control`
network's subnet so Temporal's address there is known before it starts. If the server's environment sets
`HTTP_PROXY` or `HTTPS_PROXY` (either case), put that address in `NO_PROXY` too: Go exempts
only loopback from the proxy, and these connections carry Nexus tasks and completion tokens
(with `0.0.0.0` and a proxy, the system worker's gRPC goes to the proxy and the server does not
start). `compose.yaml` sets all four proxy variables empty for Temporal, because compose copies
a Docker client's proxy settings into every container.

## Tests

- `GOFLAGS=-p=1 go test ./...` — the deny matrix (every cell against Kontra's authorizer,
  Temporal's no-op authorizer and Temporal's default authorizer, each with the typed request
  Temporal would hand it), a full sweep of every frontend API (both reflection versions and
  the Nexus HTTP routes included) against every kind of caller, namespace and a favourable,
  an oversized and hostile requests, the relay's size cap at its edge, the relay's request
  shape and its rate (`TestRelayRate`: a bucket per subject, refilled on a fake clock, safe
  under concurrent calls; full buckets swept), a check that every
  listed API is a real unary gRPC method and that no
  allowlist holds a call escrow depends on, the relay subject's golden vectors shared with
  the Python twin (`testdata/principals.json`) and its injectivity, Temporal's own
  interceptor handing the authorizer the decoded request, the callback guard (every carrier, every header spelling, every URL), the config /
  dynamic-config checks (including the pins holding at runtime), and one real server start
  with the internal frontend bound to `127.0.0.2` (skipped under `-short`, or where that
  address cannot be bound).
- `KONTRA_TEMPORAL_BIN=$PWD/bin/kontra-temporal ../../.venv/bin/pytest -m temporal ../../tests/temporal`
  (from this directory) — the policy over the wire against the real binary, plus the
  positive seller/buyer Nexus hiring path, the forged-completion probes and the callback
  and Nexus-header checks. Without `KONTRA_TEMPORAL_BIN` it uses `bin/kontra-temporal`, and
  skips if that is missing.

## Known limits

- The internal frontend has no authentication: reaching it is full control of every
  namespace. Its isolation is the network alone (loopback by default; one address on a
  private network shared only with the gate otherwise). Internode mTLS would add a second
  lock.
- A revoked session's worker token is refused within one revocation poll (5 s) while the
  gate's list can be read; when it cannot, the last list fetched stays in force and the
  failure is logged (`revocation list: cannot refresh it`), so a session revoked meanwhile
  keeps its worker tokens until their `exp` (15 minutes). Temporal's claim mapper also
  accepts a token with no `exp`; the gate never signs one (`KeyRing._sign` refuses), and
  nothing here checks it.
- Rate limits are per namespace; unauthenticated `GetSystemInfo` / health calls are limited
  only by the frontend's global limits and the TLS proxy in front of it.
