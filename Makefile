# kontra-sdk developer entry points.
#
# `make check` runs exactly what CI runs: every job in .github/workflows/ci.yml calls one of
# the targets below, and `check` is their union (PRD §19.3). If CI and `make check` ever
# disagree, the fix belongs here, not in the workflow.
#
#   make sync        install the locked environment into .venv (run once, and after a lock change)
#   make check       lint + typecheck + test + seams + deny-matrix
#   make temporal    build the Temporal server binary and the kontra-temporal:dev image
#   make e2e         tests/e2e against `kontra dev` (Docker, the Temporal image, the gate image)
#
# `make help` lists every target.

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
.DEFAULT_GOAL := help
MAKEFLAGS += --no-builtin-rules

# --- Python ---------------------------------------------------------------------------

PYTHON ?= 3.12
UV ?= uv
BIN := .venv/bin
# -ra: name every skip, so a test that did not run is visible in CI's log.
PYTEST_FLAGS ?= -q -ra

# The seam tests, ② to ⑥ (CLAUDE.md, PRD §19.1). They run as their own CI job so that a
# seam break is named as one; `test` runs everything else, so nothing runs twice.
#   ② gate HTTP API   tests/gate_client/test_routes.py
#   ③ contract        tests/contract (derivation, canonical form, hash, validation)
#                     tests/examples/test_golden.py (examples/*/contract.yaml byte for byte)
#   ④ ledger          tests/ledger (the §9.3 suite, and its teeth)
#   ⑤ codec           tests/codec, tests/artifacts (round trips, marker scans of history, bucket,
#                     a worker's metrics and spans, and the dev graph page's routes)
#   ⑥ engine          tests/engine (the engine contract and its teeth against FakeEngine, and
#                     against TemporalEngine on a dev server; the Protocol's pins)
# Seam ① is `deny-matrix` below (it also runs seam ⑥'s isolation over the wire).
SEAM_TESTS := \
	tests/gate_client/test_routes.py \
	tests/contract \
	tests/examples/test_golden.py \
	tests/ledger \
	tests/codec \
	tests/artifacts \
	tests/engine
WIRE_TESTS := tests/temporal
E2E_TESTS := tests/e2e

# slow: needs the outside world (nexgen, npm); e2e: needs `kontra dev`. Both have targets.
UNIT_MARKERS := not e2e and not slow
UNIT_IGNORES := $(addprefix --ignore=,$(SEAM_TESTS) $(WIRE_TESTS) $(E2E_TESTS))

# --- Temporal -------------------------------------------------------------------------

# The dev-server tests need Temporal CLI 1.9.1 (server 1.32, standalone Nexus operations).
# It is downloaded once, checked against the sha256 GitHub publishes for the release, and
# handed to the tests as KONTRA_TEMPORAL_CLI: with that set, a server that fails to start
# fails the tests instead of skipping them, so CI cannot pass by not running them.
TEMPORAL_CLI_VERSION := 1.9.1
TEMPORAL_CLI_SHA256_linux_amd64 := 09a0326a51db84d02735e53542b9ebd8c4758daf47482a9ab0abce15844e60d5
TEMPORAL_CLI_SHA256_linux_arm64 := 6c57c352d52fc3df34412376fd9ba6f74b7e3ace8e426e6cba8600156d36a145
TEMPORAL_CLI_SHA256_darwin_amd64 := 48cdd6c84c56e27ae8e4d9a47052b89289a9c552dee029b49dd312c6fb72873f
TEMPORAL_CLI_SHA256_darwin_arm64 := 41e0425378fcb4fb5766340b97435e20fe47bbff2d7bf644ec2d51f7662b7c56

HOST_OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
HOST_ARCH := $(patsubst aarch64,arm64,$(patsubst x86_64,amd64,$(shell uname -m)))
CACHE_DIR ?= $(or $(XDG_CACHE_HOME),$(HOME)/.cache)/kontra-sdk
TEMPORAL_CLI := $(CACHE_DIR)/temporal-cli-$(TEMPORAL_CLI_VERSION)/temporal
# Set but empty counts as unset: `?=` would keep an exported empty value, and the tests,
# told no binary, would skip instead of failing.
ifeq ($(strip $(KONTRA_TEMPORAL_CLI)),)
override KONTRA_TEMPORAL_CLI := $(TEMPORAL_CLI)
endif
export KONTRA_TEMPORAL_CLI

# The Authorizer and Kontra's Temporal server (temporal/authorizer).
AUTHORIZER := temporal/authorizer
TEMPORAL_BIN := $(AUTHORIZER)/bin/kontra-temporal
TEMPORAL_IMAGE ?= kontra-temporal:dev
# The image is Linux. On a Linux host it packages the binary built here (Dockerfile.local,
# seconds); anywhere else that binary is for the wrong OS (a Mach-O file in a Linux image
# only fails at `kontra dev up`), so the image compiles its own (the multi-stage Dockerfile
# CI ships, slower).
ifeq ($(HOST_OS),linux)
TEMPORAL_DOCKERFILE := $(AUTHORIZER)/Dockerfile.local
else
TEMPORAL_DOCKERFILE := $(AUTHORIZER)/Dockerfile
endif
AUTHORIZER_SOURCES := $(shell find $(AUTHORIZER) -type f \( -name '*.go' -o -name '*.yaml' -o -name 'go.mod' -o -name 'go.sum' \) -not -path '*/bin/*')
# One package at a time keeps the Temporal server compile inside a small machine's memory;
# CI, on bigger runners, overrides it.
GOFLAGS ?= -p=1
export GOFLAGS

# `make e2e` runs against the stack `kontra dev` brings up. By default that stack uses the
# image `make temporal` builds; KONTRA_GATE_IMAGE keeps compose's default (the published gate).
ifeq ($(strip $(KONTRA_TEMPORAL_IMAGE)),)
override KONTRA_TEMPORAL_IMAGE := $(TEMPORAL_IMAGE)
endif
export KONTRA_TEMPORAL_IMAGE

# --------------------------------------------------------------------------------------

.PHONY: help
help: ## List the targets
	@grep -hE '^[a-z][a-z0-9-]*:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

.PHONY: sync
sync: ## Install the locked environment into .venv (every extra and the dev group; PYTHON=3.12)
	$(UV) sync --locked --all-extras --python $(PYTHON)

.PHONY: venv-present
venv-present:
	@test -x $(BIN)/pytest || { echo "no .venv here: run \`make sync\` first" >&2; exit 1; }

.PHONY: check
check: lint typecheck test seams deny-matrix ## Everything CI runs on a pull request, in the same order

.PHONY: lint
lint: venv-present ## ruff check, ruff format --check, and the docs' examples against the package
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .
	$(BIN)/python .github/scripts/check_docs.py

.PHONY: typecheck
typecheck: venv-present ## pyright, strict, over kontra/ (pyproject.toml [tool.pyright])
	$(BIN)/pyright

.PHONY: test
test: venv-present $(KONTRA_TEMPORAL_CLI) ## The unit tests: everything but the seams, the wire suite, slow and e2e
	$(BIN)/pytest -m "$(UNIT_MARKERS)" $(UNIT_IGNORES) $(PYTEST_FLAGS) tests

.PHONY: seams
seams: venv-present $(KONTRA_TEMPORAL_CLI) ## The seam tests ② to ⑥ (gate API, contract, ledger, codec, engine)
	$(BIN)/pytest -m "$(UNIT_MARKERS)" $(PYTEST_FLAGS) $(SEAM_TESTS)

.PHONY: deny-matrix
deny-matrix: go wire ## Seam ①: the Go deny matrix, then the same policy over the wire

.PHONY: go
go: ## go vet and go test in temporal/authorizer (deny matrix, callback guard, config pins)
	cd $(AUTHORIZER) && go vet ./... && go test ./...

.PHONY: wire
wire: venv-present $(TEMPORAL_BIN) ## The wire suite (tests/temporal) against the locally built server
	KONTRA_TEMPORAL_BIN="$(abspath $(TEMPORAL_BIN))" $(BIN)/pytest -m "$(UNIT_MARKERS)" $(PYTEST_FLAGS) $(WIRE_TESTS)

.PHONY: slow
slow: venv-present $(KONTRA_TEMPORAL_CLI) ## Tests that need nexgen, Docker or npm (acceptance 16); they skip without them
	$(BIN)/pytest -m "slow and not e2e" $(PYTEST_FLAGS) tests

.PHONY: temporal-bin
temporal-bin: $(TEMPORAL_BIN) ## Build temporal/authorizer/bin/kontra-temporal (host build, reuses the Go cache)

$(TEMPORAL_BIN): $(AUTHORIZER_SOURCES)
	$(AUTHORIZER)/build.sh

.PHONY: temporal
temporal: $(TEMPORAL_BIN) ## Build the server binary and the local image kontra-temporal:dev
	docker build -f $(TEMPORAL_DOCKERFILE) -t $(TEMPORAL_IMAGE) $(AUTHORIZER)
	@# The image's binary must start in it (OS, architecture, libc): a server that cannot
	@# exec dies before it logs anything, so say so here rather than at `kontra dev up`.
	docker run --rm $(TEMPORAL_IMAGE) help >/dev/null
	@echo "built $(TEMPORAL_IMAGE) from $(TEMPORAL_DOCKERFILE)"

.PHONY: temporal-cli
temporal-cli: $(KONTRA_TEMPORAL_CLI) ## Download the Temporal CLI the dev-server tests use (checksum-verified)

$(TEMPORAL_CLI):
	@sha='$(TEMPORAL_CLI_SHA256_$(HOST_OS)_$(HOST_ARCH))'; \
	if [ -z "$$sha" ]; then echo "no pinned Temporal CLI for $(HOST_OS)/$(HOST_ARCH); set KONTRA_TEMPORAL_CLI" >&2; exit 1; fi; \
	tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	url='https://github.com/temporalio/cli/releases/download/v$(TEMPORAL_CLI_VERSION)/temporal_cli_$(TEMPORAL_CLI_VERSION)_$(HOST_OS)_$(HOST_ARCH).tar.gz'; \
	echo "downloading $$url"; \
	curl -fsSL --proto '=https' --tlsv1.2 --retry 3 -o "$$tmp/cli.tar.gz" "$$url"; \
	if command -v sha256sum >/dev/null; then got="$$(sha256sum "$$tmp/cli.tar.gz")"; else got="$$(shasum -a 256 "$$tmp/cli.tar.gz")"; fi; \
	if [ "$${got%% *}" != "$$sha" ]; then echo "Temporal CLI checksum mismatch: got $${got%% *}, want $$sha" >&2; exit 1; fi; \
	tar --no-same-owner -xzf "$$tmp/cli.tar.gz" -C "$$tmp" temporal; \
	mkdir -p "$(@D)"; mv "$$tmp/temporal" "$@"; chmod 0755 "$@"; \
	echo "Temporal CLI $(TEMPORAL_CLI_VERSION) at $@"

.PHONY: e2e
# $(KONTRA_TEMPORAL_CLI) because acceptance 1 scaffolds a project and runs ITS tests, and
# `kontra.testing.Marketplace` starts a Temporal dev server from that CLI. Every other
# suite declares it; e2e did not, and nothing noticed while the job was skipped -- on a
# machine that had run `make test` once the cache was already warm, and on a fresh runner
# the scaffolded project failed with "ephemeral server executable does not exist".
e2e: venv-present $(KONTRA_TEMPORAL_CLI) ## tests/e2e against `kontra dev` (brings the stack up; `kontra dev down` stops it)
	@test -d $(E2E_TESTS) || { echo "$(E2E_TESTS) does not exist: there are no end-to-end tests to run" >&2; exit 1; }
	@docker image inspect "$$KONTRA_TEMPORAL_IMAGE" >/dev/null 2>&1 || [[ "$$KONTRA_TEMPORAL_IMAGE" == */* ]] || \
		{ echo "image $$KONTRA_TEMPORAL_IMAGE not found: run \`make temporal\`, or set KONTRA_TEMPORAL_IMAGE" >&2; exit 1; }
	@# Acceptance 1 counts the stack's start (`kontra dev` on a fresh machine starts it):
	@# KONTRA_E2E_UP_SECONDS hands this `up`'s wall time to the session.
	start=$$(date +%s); $(BIN)/kontra dev up; \
	KONTRA_E2E_UP_SECONDS=$$(( $$(date +%s) - start )) $(BIN)/pytest -m e2e $(PYTEST_FLAGS) $(E2E_TESTS)

.PHONY: clean
clean: ## Remove build outputs (the server binary, dist/); leaves .venv and the CLI cache
	rm -rf $(AUTHORIZER)/bin dist build
