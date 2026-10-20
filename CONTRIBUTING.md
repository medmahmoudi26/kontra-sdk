# Contributing

Thanks for helping. This file is the short version of how work lands here; the design is in
[docs/architecture.md](docs/architecture.md) and [docs/decisions.md](docs/decisions.md).

## Branches

**Work on a topic branch and open the pull request against `dev`. Never commit to `main`.**

- `dev` is the default branch and the base of every pull request.
- `main` moves only by merging `dev`, deliberately.
- A release is a tag `v<version>` on `dev` or `main` (`.github/workflows/release.yml`).

Security problems do not go in pull requests or issues: see [SECURITY.md](SECURITY.md).

## Set up

You need [uv](https://docs.astral.sh/uv/), Go (the version in `temporal/authorizer/go.mod`;
`GOTOOLCHAIN=auto` fetches it), and Docker for `kontra dev` and the images.

```sh
make sync      # .venv from uv.lock: Python 3.12, every extra, the dev group (which carries
               # temporalio-openai-agents and temporal-agent-harness for the agent tests)
make check     # what CI runs
```

`make help` lists every target.

## `make check` is CI

Every required CI job runs one `make` target, and `make check` runs all of them in the same
order, so a green `make check` is a green pull request:

| CI job (required) | Target | Runs |
|---|---|---|
| `lint` | `make lint` | `ruff check`, `ruff format --check`, and `.github/scripts/check_docs.py` |
| `typecheck` | `make typecheck` | `pyright`, strict, over `kontra/` |
| `test` | `make test` | the unit tests (pytest), with a real Temporal dev server |
| `seams` | `make seams` | the seam tests ② to ⑤: gate API, contract and golden files, ledger, codec |
| `deny-matrix` | `make deny-matrix` | seam ①: `go vet` and `go test` in `temporal/authorizer`, then `tests/temporal` over the wire against the locally built server |

Notes:

- `ruff format --check` also checks the Python code blocks in Markdown (`README.md`,
  `docs/`): run `.venv/bin/ruff format` after editing an example there.
- `.github/scripts/check_docs.py` holds the README, this file, `SECURITY.md`, `docs/`, the
  other READMEs and the one `kontra init` writes to the code: every Python block must run as a
  file and import only names `kontra` has, every `kontra` command line must use real
  subcommands and options, every `make` target (in the docs and the workflows) must exist, and
  every `KONTRA_*` variable must be read somewhere. Fix the doc, or the code, not the check.
- The tests download Temporal CLI 1.9.1 once (checksum-verified, into `~/.cache/kontra-sdk`)
  and pass it as `KONTRA_TEMPORAL_CLI`; with it set, a dev server that fails to start fails
  the run instead of skipping tests. Point `KONTRA_TEMPORAL_CLI` at your own binary to skip the
  download.
- `make deny-matrix` compiles the Temporal server. `GOFLAGS=-p=1` (the default here) keeps it
  inside a small machine's memory; `GOFLAGS=-p=4 make deny-matrix` is faster on a big one.
- Not in `make check`: `make slow` (needs nexgen, Docker and npm; skips without them) and
  `make e2e` (below). CI also runs `e2e` after the required jobs, and `security.yml`:
  gitleaks, pip-audit, Trivy, an SBOM and zizmor.

## End to end

```sh
make temporal          # bin/kontra-temporal and the image kontra-temporal:dev (off Linux,
                       # the image compiles its own server: the multi-stage Dockerfile)
make e2e               # `kontra dev up` with that image, then tests/e2e
kontra dev down        # when you are done (--volumes also drops the data)
```

`make e2e` uses `KONTRA_TEMPORAL_IMAGE` (default `kontra-temporal:dev`) and
`KONTRA_GATE_IMAGE` (default: compose's, `ghcr.io/medmahmoudi26/kontra-gate:dev`). If your
`KONTRA_HOME` holds a real login, `kontra dev` refuses to replace it: run with
`KONTRA_HOME=~/.kontra-dev`. The suite ends with `kontra dev down --volumes`, which deletes
the data of this machine's one dev stack (the compose project is `kontra-dev` whatever the
home): do not run it next to a dev stack whose data you want to keep.
[docs/acceptance.md](docs/acceptance.md) maps each PRD §16 criterion to the tests that prove
it, end to end and below, with the last full run.

## Rules every change keeps

- **Seams.** A change that crosses one of the five seams updates that seam's test in the same
  pull request (table in [docs/architecture.md](docs/architecture.md#the-five-seams)). A golden
  contract that changes is a contract change: regenerate it with
  `UPDATE_GOLDEN=1 pytest tests/examples/test_golden.py` and explain the diff in the PR.
- **The one-way rule.** This package never imports the platform; it speaks HTTP to the gate
  through `kontra.gate_client`. Nothing Stripe-, payout- or PII-shaped belongs here.
- **One implementation.** Contract derivation, canonicalisation, hashing and validation live
  only in `kontra.contract`; escrow rules only in `kontra/escrow.py`; Temporal identifiers are
  built only in `kontra/contract/names.py`, **constructed and never parsed back**. Nexus
  endpoint names are opaque; nothing derives an offering from one or authorizes by its shape.
- **Validate what people type** with anchored `re.fullmatch` and length limits. Never
  `re.match` with `$` (it accepts a trailing newline), and never strip or normalise before
  validating.
- **Sellers write native Temporal** (D1 as the PRD wrote it; decisions I29). Seller
  `workflows.py` and `activities.py` import `temporalio` directly and nothing from `kontra`;
  `worker.py` is the one file that imports `kontra` (`quote.py` takes `kontra.serve.Quote`,
  `models.py` may use `kontra.types`). `grep -rE '^\s*(from|import)\s+kontra\b'
  examples/*/workflows.py examples/*/activities.py` prints nothing. Never add a Temporal
  re-export to `kontra`. Kontra never imports `temporal_agent_harness` at runtime.
- **Dependencies.** The distribution is `kontra-sdk`; never add a bare `kontra` dependency
  anywhere (PyPI `kontra` is an unrelated project). Add a dependency in `pyproject.toml`, run
  `uv lock`, and commit both files.
- **Temporal upgrades** (`temporalio`, `go.temporal.io/server`) are deliberate: re-run the
  callback guard's carrier sweep and the wire suite, and read the upstream changes to Nexus
  completions ([docs/security.md](docs/security.md#the-callback-guard-and-the-upstream-nexus-completion-issue)).
  `temporalio` is a declared range (`>=1.34,<1.38`, in `pyproject.toml` and `kontra/_compat.py`,
  which a test keeps equal). The lock stays on the minor the suite last passed on (1.34, today
  the only release in the range, so 1.35 to 1.37 are admitted untested): when a new minor
  ships, a PR moves the lock to it with a green suite, and the range widens past 1.38 only
  that way.
- **Tests for behaviour.** Every behaviour a change adds has a test; no stubs, `TODO`s or
  `NotImplementedError` left behind. pyright strict stays clean on `kontra/`.
- **Docstrings say why**, briefly. The code already says what.
- **No secrets** in the tree, tests included. The gitleaks configuration in
  `.github/gitleaks.toml` allows only one fake fixture value and bare `KONTRA_*` variable
  names; add to it only with a reason.

## CI workflow changes

Pin every third-party action by full commit SHA with its version in a comment, give each job
the least permissions it needs, never interpolate `${{ }}` expressions into `run:` scripts
(pass them through `env:`), and keep `zizmor` and `actionlint` clean:

```sh
actionlint
zizmor .github
```

## Issues

Plans and issues live as Markdown under `.scratch/` ([.scratch/README.md](.scratch/README.md)).

## License

By contributing you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE).
