# Acceptance criteria (PRD §16)

What proves each of the nineteen acceptance criteria, how to run it, and what the last full
run gave. A criterion is proved one of two ways, often both:

* **End to end** (`tests/e2e`, run by `make e2e`): against the real stack `kontra dev` brings
  up -- Kontra's Temporal server with the Authorizer, the gate image, Postgres and SeaweedFS
  -- with sellers as `kontra serve` processes (their own `KONTRA_HOME` each, and the
  settings a seller ships with: no `KONTRA_*` of the test run reaches them), buyers as
  `kontra.hire.Kontra`, the `kontra` CLI or `kontra mcp`, and every party logged in with the
  dev login. One test per criterion that needs the stack, named after it
  (`test_ac04_...`). Durations are the ones the PRD allows to be short: a 2-minute
  `max_duration` for 4, a 30-second `review_window` for 5, a 5-second `artifact_retention`
  for 14, written into a temporary copy of the offering, never into the examples.
* **Unit and integration suites**: the criteria that are properties of one component (the
  Authorizer's deny matrix, the ledger, the contract, the worker's keyword passthrough), and
  the per-cell versions of the end-to-end ones against `kontra.testing`'s fake gate and a
  Temporal dev server, here and in kontra-cloud (the gate's repository, private).

## Running them

The stack's images, built from the current trees:

```sh
make temporal                # kontra-temporal:dev (temporal/authorizer)
# in kontra-cloud: make image  -- kontra-gate:dev, from that tree and the ../kontra-sdk checkout
```

The end-to-end suite. `make e2e` runs `kontra dev up` (timed: acceptance 1 counts the
stack's start, handed to the session as `KONTRA_E2E_UP_SECONDS`), then the tests; the
session's fixture runs `kontra dev down --volumes` at the end, so a session starts from an
empty catalog and ledger. That is the machine's one dev stack (compose project
`kontra-dev`, whatever the home): run it where no dev stack holds data you want, with a
`KONTRA_HOME` that holds no real login (the stack's credentials go into its `dev.env`) and
host ports that are free:

```sh
export KONTRA_HOME=/tmp/kontra-e2e
export KONTRA_GATE_IMAGE=kontra-gate:dev KONTRA_TEMPORAL_IMAGE=kontra-temporal:dev
export KONTRA_DEV_GATE_PORT=18080 KONTRA_DEV_TEMPORAL_PORT=17233
export KONTRA_DEV_S3_PORT=18333 KONTRA_DEV_PG_PORT=15432
kontra dev down --volumes                         # from nothing
make e2e PYTEST_FLAGS="-q -ra -s --durations=0"   # -s prints each test's [acceptance N] line
KONTRA_LEDGER=fake make e2e PYTEST_FLAGS="-q -ra -s --durations=0"   # acceptance 9
```

Without Docker or either image the tests are skipped, saying which. Acceptance 14 needs about
9 GiB of free disk (a 3 GiB file, its ciphertext in the bucket, the download) and skips
without it.

The unit and integration proofs named in the table, in this repository:

```sh
make deny-matrix                                    # acceptance 2: Go deny matrix, then tests/temporal
.venv/bin/pytest -q tests/examples/test_imports.py  # acceptance 3 (needs a Temporal dev server)
NEXGEN_BIN=/path/to/nexgen .venv/bin/pytest -q -m slow \
    tests/contract/test_nexgen.py tests/cli/test_pull_ts.py tests/cli/test_publish_nexgen.py
```

and in kontra-cloud (its own `.venv`; its fixtures start a Postgres and a SeaweedFS container
of their own and remove them):

```text
make ledger     # with that Makefile's interleavings variable at 10000 (its nightly CI run): acceptance 6
.venv/bin/pytest -q <the kontra-cloud node ids in the table>
```

## The criteria

`e2e` is `tests/e2e`; `cloud:` marks a test of kontra-cloud; every other path is in this
repository. The result column is the run recorded below.

| # | Criterion | Proved by | Result |
|---|---|---|---|
| 1 | `kontra init && kontra dev && kontra serve` yields a hireable offering in under an hour; `workflows.py`/`activities.py` are vanilla Temporal: they import `temporalio` and nothing from `kontra` (decisions I29); no Python symbol in `offering.yaml` | e2e `test_seller.py::test_ac01_init_dev_serve_yields_a_hireable_offering`; `tests/cli/test_cli.py::test_init_scaffolds_the_project_layout`; `tests/cli/test_scaffold.py::test_the_scaffold_passes_its_own_test` | passed; 55.0 s from nothing to an accepted delivery: the stack's start 30.0 s (`kontra dev up`, no stack before), then `kontra dev` 6.5 s, `kontra init` 0.7 s, the scaffold's own `pytest` 12.8 s, `kontra serve` 1.5 s, `kontra hire --accept` 3.1 s |
| 2 | Every §10.4 deny-matrix cell has a failing-by-default test that passes with the Authorizer | `temporal/authorizer`: `TestDenyMatrix`, `TestDenyCellsFailByDefault`, `TestSweep` (`go test ./...`); `tests/temporal/test_isolation_wire.py`, `tests/temporal/test_streams_and_platform_wire.py` (the cells over the wire, against the built server) | passed (2026-10-20, kontra-sdk#59, decisions I30): 957 checks (319 matrix cells, 253 of them denials, against 3 authorizers), a sweep of 99,320 decisions over 191 APIs, 58 wire tests |
| 3 | `grep -rE '^\s*(from\|import)\s+kontra\b' examples/*/workflows.py examples/*/activities.py` prints nothing (decisions I29: any import of `kontra`, where the PRD's grep saw only `import kontra`) | `tests/examples/test_imports.py` (`::test_the_acceptance_grep_prints_nothing`; the checks that seller code imports `temporalio` and no `kontra` by any spelling, `models.py` only `kontra.types`, `quote.py` only `kontra.serve.Quote`, and that loading it loads at most `kontra.types`; and `::test_without_worker_py_the_code_runs_on_any_temporal`, `examples/hello` without `worker.py` on a plain Temporal worker with `kontra` unimportable) | passed |
| 4 | An offline seller's job refunds within `max_duration` + 1 minute, no human action | e2e `test_escrow.py::test_ac04_an_offline_seller_refunds_within_max_duration_plus_a_minute`; `tests/hire/test_buyer.py::test_an_offline_seller_refunds_by_the_deadline`; cloud: `tests/jobs/test_sweeper.py::test_offline_seller_refunds_by_the_deadline` | passed; refunded 151.1 s after the hire (bound: 180 s, and not before 120 s), never started |
| 5 | A delivered job with a silent buyer releases at `review_window` expiry | e2e `test_escrow.py::test_ac05_a_silent_buyer_releases_at_review_window_expiry`; `tests/hire/test_script.py::test_a_silent_buyer_pays_when_the_review_window_closes`; cloud: `tests/jobs/test_sweeper.py::test_completion_delivers_then_the_review_window_releases` | passed; released 2.7 s after the window closed (bound: 10 s, and not before), the seller credited 0.19 of 0.20 |
| 6 | The §9.3 property test runs 10,000 random interleavings without violating the balance invariant | cloud: `tests/ledger/test_ledger_pg_contract.py::test_postgres_ledger_passes_the_contract` at 10,000 (Postgres); `tests/ledger/test_ledger_contract.py::test_fake_ledger_passes_10000_interleavings` (the in-memory ledger) | passed: 10,000 interleavings against Postgres in 1,611 s (kontra-cloud `b999a73`); the in-memory ledger at 10,000 too |
| 7 | A tampered hold token (amount, seller, expiry) is refused by the handler without contacting the gate | `tests/serve/test_serve.py::test_tampered_tokens_are_refused_offline`; e2e smoke `test_seller.py::test_ac07_tampered_hold_tokens_are_refused_without_the_gate` | passed; amount, seller and expiry each refused (the signature), the seller's worker sent the gate 0 bytes during each refusal |
| 8 | Kontra's bucket and Temporal history hold no plaintext of a job with a 100 MB input and output (marker scan) | e2e `test_privacy.py::test_ac08_no_plaintext_of_a_100mb_job_in_the_bucket_or_temporal`; `tests/codec/test_codec_temporal.py`, `tests/codec/test_codec_nexus.py` | passed; 0 hits in the bucket's 10 objects (196.5 MiB, S3 API), in 295 Postgres columns (SQL, decompressed; 63 Temporal `history_node` rows among them) and in the files on both disks (Postgres's data directory with its WAL, SeaweedFS's volumes); the seller's job history held 5 payloads, all claim-check references but the activity header carrying the job id; a control marker was found by each scan |
| 9 | Switching the ledger adapter from `postgres` to `fake` changes no SDK test outcome | `make e2e` twice, with `KONTRA_LEDGER=postgres` (the default) and `KONTRA_LEDGER=fake` (the gate's own switch); `tests/hire/test_script.py` (every test on two ledger adapters behind the fake gate); cloud: `tests/jobs/test_market_pg.py`, `tests/test_core_settings.py` | passed; the same 14 outcomes with the gate on either adapter (every balance the tests assert included: the seller's 0.19, the buyers' 99.99 and 100.00); the Postgres ledger's tables stayed empty in the `fake` run |
| 10 | Every `temporalio.worker.Worker` kwarg reaches the inner worker unchanged (decisions I29, two exceptions: `plugins` routed -- a client plugin goes on the client before Kontra's and temporalio carries it onto the inner worker, a worker-only one goes to the inner worker; either way it is among the inner worker's effective plugins --, and a `workflow_runner` that is Temporal's `SandboxedWorkflowRunner` arrives as a copy with `kontra` added to its passthrough modules; any other runner is untouched) | `tests/serve/test_worker.py::test_every_temporalio_keyword_has_a_sentinel`, `::test_every_keyword_reaches_the_inner_worker_unchanged` | passed |
| 11 | `recon` composes two offerings in one buyer workflow and refunds the second when the first fails | e2e `test_escrow.py::test_ac11_recon_refunds_the_second_offering_when_the_first_fails`; `tests/examples/test_recon.py::test_recon_refunds_the_enrichment_when_the_probe_fails`; `tests/hire/test_workflow.py::test_recon_refunds_the_second_job_when_the_first_fails` | passed; probe failed, enrichment cancelled (its run terminated), both refunded |
| 12 | MCP search, quote, hire, result, accept completes against `kontra dev` | e2e `test_agents.py::test_ac12_mcp_search_quote_hire_result_accept_against_kontra_dev` (`kontra mcp` over stdio); `tests/mcp/test_mcp_server.py::test_search_quote_hire_result_accept_completes_and_moves_money`, `::test_kontra_mcp_over_stdio` | passed; released, 0.01 paid |
| 13 | 100 heartbeats `{percent, message}` arrive in order via `job.progress()`, no Kontra import in `activities.py`, no plaintext at the gate; a job without heartbeat details still shows liveness and activity names | e2e `test_privacy.py::test_ac13_heartbeats_reach_the_buyer_in_order_and_the_gate_keeps_ciphertext`; `tests/hire/test_buyer.py::test_progress_shows_100_heartbeats_in_order_decrypted`; `tests/serve/test_serve.py::test_heartbeats_reach_the_gate_in_order_and_only_as_ciphertext`; cloud: `tests/progress/test_progress_post.py::test_heartbeats_are_kept_in_order_as_ciphertext`, `::test_malformed_or_plaintext_details_are_refused` | passed; 100 of 100 in order with the seller's default forwarding (one event a second per job, latest wins) and a heartbeat every 1.5 s; the quiet job showed `alive, in quiet`; 0 hits (SQL, bucket, Postgres's files) |
| 14 | A 3 GB `Artifact` output uploads with worker RSS under 256 MB above baseline, downloads with matching sha256, and is gone after `artifact_retention` | e2e `test_privacy.py::test_ac14_a_3gb_artifact_in_flat_memory_verified_and_gone_after_retention` (also marked `slow`); `tests/artifacts/test_artifact_memory.py` (64 MiB by default); cloud: `tests/artifacts/test_artifacts_retention.py` | passed; 3 GiB: the seller's worker +19.2 MiB, the buyer's download +22.6 MiB, sha256 equal, deleted 71 s after accept |
| 15 | An upload over `max_output_bytes` is refused by the gate before any bytes are stored | e2e `test_privacy.py::test_ac15_an_upload_beyond_max_output_bytes_is_refused_before_storing` and `::test_ac15_a_body_beyond_its_declared_size_is_refused_before_storing`; `tests/testing/test_artifacts.py::test_outputs_above_max_output_bytes_are_refused_before_storing`; cloud: `tests/artifacts/test_artifacts.py::test_an_output_above_max_output_bytes_is_refused_before_anything_is_stored`, `tests/artifacts/test_artifacts_s3.py::test_an_output_over_budget_leaves_the_bucket_empty` | passed; a declared 2 MB over 1 MB: 413 `too_large` naming `max_output_bytes`, bucket and gate unchanged; 1,000 bytes declared and 2,000,000 sent: the bucket answered 403 `SignatureDoesNotMatch` and stored nothing (the same URL then took the 1,000 bytes) |
| 16 | `kontra publish` on `enrich` emits a document nexgen accepts for all four targets; `kontra pull --lang ts` type-checks against the example's output | `tests/cli/test_publish_nexgen.py` (the example published with `kontra publish`, pulled with `kontra pull` in go, python, ts and java); `tests/cli/test_pull_ts.py::test_pull_ts_type_checks_against_the_enrich_output`; `tests/contract/test_nexgen.py` (what derivation can emit, on sample models) -- all marked `slow` (nexgen, Docker, npm) | passed: 14 |
| 17 | Editing a type after publish makes `kontra serve` refuse that version, naming the hash mismatch | e2e `test_seller.py::test_ac17_a_type_edited_after_publish_makes_serve_refuse`; `tests/serve/test_serve.py::test_an_edited_type_after_publish_refuses_to_start`; `tests/cli/test_marketplace.py::test_serve_refuses_code_that_drifted_from_the_catalog`; `tests/serve/test_worker.py::test_the_hash_lock_names_both_hashes` | passed; refused, both hashes named |
| 18 | A hold minted for contract hash X is refused by a handler serving hash Y, before any workflow starts | e2e `test_seller.py::test_ac18_a_hold_for_one_contract_is_refused_by_a_handler_serving_another`; `tests/serve/test_serve.py::test_validly_signed_tokens_for_something_else_are_refused` | passed; refused, no seller run of `job-<id>` (only the gate's placeholder), refunded |
| 19 | A document with an external `$ref` or `additionalProperties` unset is refused by the catalog, naming the path | `tests/contract/test_validate.py` (`::test_rule[external $ref in a field]`, `[additionalProperties unset]` and the rest); cloud: `tests/jobs/test_catalog.py::test_invalid_document_names_the_path`; e2e smoke `test_seller.py::test_ac19_the_catalog_refuses_external_refs_and_open_objects` | passed; both refused (422) with their paths |

### What the end-to-end tests look at

* **1** starts from a new home (only the stack's `dev.env`, so `kontra dev` finds the running
  stack instead of starting a second one), times `kontra dev`, `kontra init`, the scaffold's
  own `pytest`, `kontra serve` (a dev profile publishes) and a buyer's
  `kontra hire --accept` through the CLI, adds the session's `kontra dev up` (the stack's
  start, which `kontra dev` does on a fresh machine), and checks the two files and
  `offering.yaml`. Pulling the images is not counted.
* **4** quotes while the seller serves, stops the seller, hires, and polls the job (a read:
  `GET /v1/jobs/{id}` changes nothing, the gate's sweeper alone moves the escrow): refunded,
  never started, no earlier than `max_duration`, no later than `max_duration` + 60 s.
* **5** lets the delivery sit: released no earlier than the recorded review deadline and
  within 10 s of it (the gate sweeps every 2 s), the seller paid the price less the 5% fee.
* **7** points the seller's profile at a recording TCP relay in front of the gate: for each
  tampered claim the seller's process sends the gate nothing between the hire and the
  refusal. **18** runs the seller's worker with edited types and the hash lock taken out (a
  modified or stale worker). In both, the seller's namespace shows no run of the seller's
  workflow under `job-<id>`: only the gate's placeholder, which occupies the id when it
  cancels the refused job so the token can never start one later.
* **8** and **13** scan the stack as its operator could: every object in the bucket (S3 API)
  and every text-like and binary column of every table in every Postgres database (the
  gate's, Temporal's persistence and visibility), for each marker raw, hex and base64 at
  every alignment; and the files on the disks byte for byte (`grep` inside the containers:
  Postgres's data directory with its write-ahead log, where a row written then deleted
  stays; for 8 also SeaweedFS's volumes, where a deleted object's bytes stay until a
  vacuum). A control marker planted in the bucket and in a scratch database (once large and
  compressible, so TOASTed, once short and stored as is) is found by the same scans, then
  removed. 8's 100 MB are random bytes with the marker every MiB, so no store would compress
  them out of the file scans' sight. 8 also reads the seller's history of the job as
  Temporal stores it (no codec): every payload is age ciphertext or a claim-check reference,
  but the activity header `kontra-job`, which carries the job id (control plane, like the
  workflow id).
* **11** runs the examples unchanged: `acme` serves `examples/legacy-actor` and
  `examples/enrich`, the buyer runs `examples/recon` on `kontra.hire.worker`; both jobs reach
  a site that stalls, then the probe's seller terminates the probe's workflow.
* **13** runs the seller with no `KONTRA_PROGRESS_INTERVAL`: the forwarder throttles as it
  ships (§8.4: one event a second per job, latest wins), and the activity heartbeats every
  1.5 s, so each heartbeat is an event of its own and all 100 must arrive.
* **14** samples the seller's worker process every 5 ms (after one small warm-up job) and the
  buyer's while it downloads, and watches the bucket until the object is gone.
* **15** compares the bucket's objects and open multipart uploads before and after, and the
  gate's `artifacts` table for the job. Its second test is the dishonest seller: it asks the
  gate for an upload of 1,000 bytes for a running job (its own credentials, as modified code
  could) and sends 2,000,000 to the presigned URL.

### How we know the tests bite

Each of these deliberate breaks, made one at a time and reverted, made the named test fail
(for the end-to-end rows the stack's images were unchanged and the break was in the host's
`kontra` package, which every seller and buyer process there runs):

| Break | Test | How it failed |
|---|---|---|
| `kontra/serve/hold.py`: the signature not verified | e2e 7 | the amount tamper refused only by the price check (`prices above the held 0.00 USD`), not as a forgery |
| `kontra/serve/hold.py`: the gate's JWKS fetched on every token | e2e 7 | the relay recorded `GET /.well-known/jwks.json` during a refusal |
| `kontra/serve/handler.py`: the contract hash not compared | e2e 18 | the hire succeeded (`DID NOT RAISE JobFailed`) |
| `kontra/codec`: a payload's first 4 KiB copied into its claim-check reference's metadata | e2e 8 | the marker found in `kontra.deliveries.payload`, `temporal.history_node.data`, `temporal.chasm_node_maps.data` |
| `kontra/artifacts.py`: the whole file read into memory before uploading | e2e 14, 600 MiB | worker RSS +616.6 MiB |
| `kontra/testing/ledger.py`: one minor unit too many to the seller on 1 release in 97 | the §9.3 suite | violation at interleaving 10 |
| `temporal/authorizer`: the worker-role check skipped | `TestDenyMatrix` | 38 deny cells failed |
| `temporal/authorizer`: the stream relay's handler-name check skipped (2026-10-20) | `TestDenyMatrix`, `TestSweep` | 7 deny cells failed, and the sweep |
| `temporal/authorizer`: the stream relay's subject comparison skipped (2026-10-20) | `TestDenyMatrix`, `TestSweep`, `TestTemporalHandsTheDecodedRequest` | 11 deny cells failed, and the sweep |
| `temporal/authorizer`: the worker controller allowed outside the platform namespace (2026-10-20) | `TestDenyMatrix`, `TestSweep` | 13 deny cells failed, and the sweep |
| `temporal/authorizer`: an unknown role not counted as a permission (2026-10-20) | `TestDenyMatrix`, `TestSweep` | 2 deny cells failed: the relay's subject with `acme:relay` was judged a relay |
| nexgen failing (`NEXGEN_BIN=/bin/false`) | `tests/cli/test_publish_nexgen.py` | `kontra pull --lang go` failed |

## The run recorded here

2026-10-12, on a 2-vCPU, 8 GB machine shared with other workloads (load average 2 to 4).

* kontra-sdk `400e002` plus this change (`tests/e2e`, `tests/cli/test_publish_nexgen.py`,
  this file, the `make e2e` timing, the CLI fix under "Notes"), uncommitted at the time.
* kontra-cloud `bd610bf`, a clean tree. (`a0171fd` and `b999a73` landed during the runs: CI
  workflows, `docs/operations.md` and the console's `session.js`, none on any criterion's
  path; the image was not rebuilt for them.)
* Images: `kontra-gate:dev` `ce43fd990b4f`, built from that tree and this checkout right
  before the run (twice, 40 minutes apart: the same image); `kontra-temporal:dev`
  `19364f9bdcf8`, from `temporal/authorizer` at `400e002` (`make temporal`: its server
  binary is the one built from those sources).

| What | How | Result | Wall time |
|---|---|---|---|
| `tests/e2e`, Postgres ledger | `kontra dev down --volumes`, then `make e2e` with the settings above, 20:31 to 20:48 CEST | 14 passed | 1,007 s (the stack from nothing 30 s, 974 s of session, down) |
| `tests/e2e`, fake ledger | the same with `KONTRA_LEDGER=fake` | 14 passed | 953 s (the stack from nothing 46 s, 903 s of session, down) |
| The node ids above, here | `.venv/bin/pytest -rs` on them (the wire suite through `make deny-matrix`) | 232 passed, none skipped | 145 s |
| Acceptance 2 | `make deny-matrix`, and the two matrix tests again uncached | Go packages ok (396 matrix cells), 26 wire tests passed | 25 s + 5 s |
| Acceptance 16 | the `slow` command above, nexgen 0.2.5 | 14 passed | 22 s |
| The kontra-cloud node ids above | `.venv/bin/pytest -rs` on them | 46 passed | 64 s |
| Acceptance 6, Postgres | the kontra-cloud ledger node at 10,000 interleavings, kontra-cloud `b999a73`, 21:04 to 21:31 | passed | 1,626 s (1,611 s of interleavings, at `nice 5`) |

Acceptance 6 ran after the end-to-end runs, at `b999a73` (its ledger code is that of
`bd610bf`: `gate/ledger`, `kontra/ledger.py` and the §9.3 suite are unchanged between them).

Per end-to-end test (call time, Postgres ledger): 14 288.4 s, 13 227.6 s, 4 153.2 s,
8 101.7 s, 5 36.7 s, 15 (declared too little) 33.8 s, 1 25.0 s, 11 11.1 s, 12 8.3 s,
15 6.9 s, 7 6.6 s, 18 5.3 s, 17 4.7 s, 19 1.9 s.

### Against the images production runs (2026-10-16)

`tests/e2e` was run again after the release of items 1-5 (kontra-sdk 0.2.0), against the exact
images then running in staging and production, by digest. The suite needs `kontra dev`'s
stack (its dev login, its own Postgres and bucket), which production neither has nor should
have. So "against the deployed gate" means against the deployed **images**.

The same images were then checked live:

| Check | Staging | Production |
|---|---|---|
| `/healthz` | `ok` (database, Temporal) | `ok` |
| Migrations at head | `0012` | `0012` (`party_keys` backfilled) |
| Recipient and Workers routes | 401 without a session | 401 without a session |
| Removed `PUT /v1/me/key` | 404 | 404 |
| Gate's traces in Grafana Cloud | arriving | arriving |

The run:

* **When and where:** 2026-10-15 23:47 to 2026-10-16 00:05 UTC, on a 2-vCPU, 8 GB machine
  shared with other workloads.
* **kontra-sdk:** `b5c12e4` (`dev`). Its package is that of `530c28b`, the 0.2.0 release
  commit; the tag `v0.2.0` is `b5c12e4`.
* **kontra-cloud:** `d1d7397` (`dev`).
* **Images**, the digests staging and production ran from 2026-10-15 23:46 and 2026-10-16
  00:06 UTC (`.github/scripts/deploy-stack`):
  * `KONTRA_GATE_IMAGE=ghcr.io/medmahmoudi26/kontra-gate@sha256:c46825214fdc404d70f8db8664d5574c00a32a1e59905723f9b4e689c6571b75`:
    `kontra-gate:d1d7397`, built by kontra-cloud's deploy workflow.
  * `KONTRA_TEMPORAL_IMAGE=ghcr.io/medmahmoudi26/kontra-temporal@sha256:235d32fe669a76fc19c6a03981c277d1ac5a33b95a01af9c6d81c79716fcfd51`:
    `kontra-temporal:530c28b`, published by this repository's `temporal-image` workflow.
* **The stack:** the gate and Temporal containers were checked to run exactly those digests.

| What | How | Result | Wall time |
|---|---|---|---|
| `tests/e2e`, Postgres ledger | `kontra dev down --volumes`, then `make e2e` with the settings above | 14 passed | 1,011 s of session, plus `kontra dev up` 46 s |

Per end-to-end test (call time): 14 286.3 s, 13 216.8 s, 4 158.1 s, 8 93.7 s, 1 38.3 s,
5 36.8 s, 15 (declared too little) 36.3 s, 12 16.3 s, 17 14.7 s, 11 14.2 s, 18 13.0 s,
7 8.7 s, 19 6.4 s, 15 6.3 s.

What the criteria printed:

* **4:** refunded 152.9 s after the hire (bound: 180 s).
* **5:** released 1.6 s after the review window closed.
* **1:** 38.2 s from `kontra dev` to an accepted delivery, 84.2 s with the stack's start.
* **13:** 100 heartbeats in order.
* **14:** a 3 GiB artifact.
* **8:** six encodings of the marker scanned, none found in clear.

Not run again here: the fake-ledger session (acceptance 9), nexgen (16) and the kontra-cloud
nodes. The deny matrix (2) ran in every pull request's CI (`deny-matrix`), the matrix of the
authorizer in these images.

## Notes and open items

* **Rows 1, 3 and 10 were reworded after the recorded run**, for decisions I29 (kontra-sdk#51:
  seller code imports `temporalio` directly, `worker.py` alone imports `kontra`). The unit
  proofs of 3 and 10 were run again then and pass; 1's end-to-end test now checks the
  inverted rule and runs in CI's `e2e`, which this record predates.
* **Fix: a transport failure with no message.** A seller died with
  `error: cannot reach http://127.0.0.1:.../v1/catalog: ` and nothing after the colon:
  httpx's timeouts carry no text, and `describe` meant to fall back to the exception's
  name but tested the exception (always true) instead of its text. It now prints
  `... /v1/catalog: ReadTimeout` (`kontra/cli/_common.py`; test
  `tests/cli/test_cli.py::test_describe_names_a_transport_failure_that_has_no_message`).
* **A starved host fails the suite.** An earlier full run, beside another full gate suite
  (load average 100), failed 7 and 19 on Temporal's lost shard lock and the gate's 503
  `namespace_unavailable`; nothing retries that. 13 now needs each progress post to land
  within 1.5 s, and 5 a release within 10 s of the window.
* **Artifact retention runs on the janitor's clock** (kontra-cloud, product note). The gate
  deletes an output on the first janitor pass after `artifact_retention` has run from the
  pass that first saw the job's escrow closed; passes are a minute apart and not
  configurable. So an output goes between one and two minutes after its retention (71 s to
  122 s observed for 5 s): gone after `artifact_retention`, as 14 asks, but never at it.
* **8's Temporal history holds references.** Every payload of the 100 MB job is above the
  codec's 256 KiB threshold, so its ciphertext is in the bucket and the history holds
  claim-check references; age ciphertext inline in history is what 13's scan covers (its
  marker travels in the job's small input and in each heartbeat's details). 8's file scans
  see bytes as stored: a value Postgres compressed or an object SeaweedFS gzipped (it does,
  for a compressible body) is seen by the SQL and S3 scans only.
* **7 and 18 need parties the platform would not produce.** The tampered tokens of 7 are
  made by the buyer from a real hold. For 18 the handler must serve a contract the catalog
  does not hold, which `kontra serve` refuses (17), so the test starts the same worker with
  the hash lock patched out. "Without contacting the gate" (7) is observed for the seller's
  worker process; the test's own later lookups in the seller's namespace use the same relay.
* **CI.** `e2e` is not a required check (decisions I13) and has a 60-minute limit: the suite
  takes about 17 minutes here, and 14 skips on a runner without 9 GiB of free disk.
