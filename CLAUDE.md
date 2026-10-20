# kontra-sdk (public)

The package sellers and buyers install (`pip install kontra-sdk`, `import kontra`), the Temporal
Authorizer, the examples, and the `kontra dev` environment. The platform (the gate) lives in the
private `kontra-cloud` repository. This repository started from an empty tree; nothing comes from
the old `medmahmoudi26/kontra` platform (`docs/decisions.md` I0).

## Branches

**Work on a topic branch and open the PR against `dev`. Never commit to `main`.** `dev` is the
default branch and the base for every pull request; `main` moves only by merging `dev`,
deliberately.

## The one-way rule

`kontra-cloud` depends on this package (a pinned revision). This package **never** imports the
platform: it speaks HTTP to the gate (`kontra.gate_client`) and nothing else. Nothing Stripe-shaped,
payout-shaped or PII-shaped belongs here. The console is a gate client with no privileged path.

## Seams

A seam is a boundary where one side cannot know the other's code, only its contract. A change that
crosses a seam updates that seam's test **in the same PR**.

| # | Seam | Contract | Test |
|---|---|---|---|
| ① | Temporal frontend | Temporal's protocol + the §10.4 policy in `temporal/authorizer` | `temporal/authorizer` Go deny matrix + `tests/temporal` over the wire (required) |
| ② | Gate HTTP API | `kontra/gate_client/models.py` + `routes.py` | `tests/gate_client/test_routes.py`; the gate's OpenAPI snapshot in `kontra-cloud` |
| ③ | Contract document | `kontra/contract` + `spec/` | golden `examples/*/contract.yaml` reproduced byte for byte |
| ④ | Ledger | `kontra/ledger.py` Protocol | `kontra.testing.ledger_contract` against every adapter |
| ⑤ | Codec | `kontra/codec` | round trips + the marker-string scan of bucket, history, a worker's metrics endpoint and its exported spans, and the dev graph page's routes |

## Rules that keep the design honest

- `kontra.contract` is the only code that derives, canonicalises, hashes or validates a contract.
  `publish`, `serve` and the gate all call it.
- `kontra/contract/names.py` is the only code that builds service, task-queue or workflow-id strings,
  and they are **constructed, never parsed back**. Nexus endpoint names are opaque random strings
  assigned by the catalog; nothing derives an offering from one or authorizes by its shape
  (`docs/decisions.md` I1, I3). Validate typed input with anchored `fullmatch`, never `re.match`.
- `kontra/escrow.py` holds the job rules as pure functions; both gates (real and fake) use it.
- Seller `workflows.py` and `activities.py` are vanilla Temporal (`docs/decisions.md` I29): they import
  `temporalio` directly and nothing from `kontra`, so deleting `worker.py` leaves code that runs on any
  Temporal worker, unmodified. `worker.py` is the one file of an offering that imports `kontra`
  (`serve`, or `KontraPlugin` on the seller's own client); besides it, `quote.py` takes only
  `kontra.serve.Quote` and `models.py` only `kontra.types` (`Artifact`). No `kontra.temporal` re-export
  namespace (rejected in I29). `grep -rE '^\s*(from|import)\s+kontra\b' examples/*/workflows.py
  examples/*/activities.py` must print nothing; `tests/examples/test_imports.py` checks the rest (and
  runs `examples/hello` without `worker.py` on a plain Temporal worker).
- Kontra never imports `temporal_agent_harness` at runtime: `kontra/serve/turn.py` holds the harness's
  wire shape as constants, checked against the harness in `tests/serve/test_turn.py`, and
  `kontra/contract/harness.py` reads its schema through its CLI (`tests/contract/test_harness.py`).
- Every seam has a fake in `kontra.testing`, so a seller tests a full lifecycle without containers.
- The PyPI distribution is `kontra-sdk`; `kontra` on PyPI is someone else's package. Never add a bare
  `kontra` dependency anywhere.

## Commands

`make check` runs exactly what CI runs: ruff, pyright (strict), the unit tests, the seam tests, and the
Authorizer's Go tests. `make e2e` brings up `kontra dev` (compose) and runs `tests/e2e`.

## Issue tracker

Issues and plans live as markdown under `.scratch/<feature-slug>/`.
