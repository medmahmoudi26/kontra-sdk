#!/bin/sh
# Builds bin/kontra-temporal on this machine, for tests/temporal and Dockerfile.local.
#
# Deliberately a plain `go build` (no -trimpath, cgo as the host has it) so that it reuses
# the build cache of an earlier Temporal server build: a cold build of the server takes a
# long time on a small machine. The result links the host's glibc, which is why
# Dockerfile.local uses a base image at least as new. The CI image is built by the
# multi-stage Dockerfile instead (static, -trimpath).
#
# GOFLAGS=-p=1 (the default here) keeps the build inside a small machine's memory;
# override GOFLAGS to build faster on a bigger one.
set -eu
cd "$(dirname "$0")"
mkdir -p bin
GOFLAGS="${GOFLAGS:--p=1}" go build -ldflags "-s -w" -o bin/kontra-temporal ./cmd/kontra-temporal
echo "built $(pwd)/bin/kontra-temporal"
