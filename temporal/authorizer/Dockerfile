# Kontra's Temporal server image for CI and production (PRD §13).
#
# Multi-stage: a full Go toolchain compiles a static, -trimpath binary, copied into a slim
# runtime with only CA certificates and the default dynamic config. Runs as a non-root
# user. Only the public frontend's gRPC port is meant to be exposed (behind Kontra's TLS
# proxy); the internal frontend stays on the container/VPC network (PRD §10.2).
#
# Build from the repository root so the module and its dynamicconfig/ are in context:
#   docker build -f temporal/authorizer/Dockerfile -t kontra-temporal:TAG temporal/authorizer
#
# For a fast local image that reuses the host's warm build cache, use Dockerfile.local.

# Base images are pinned by digest (the multi-platform index, so arm64 builds too), with the
# tag beside it: a tag moves, and the next build would silently compile with, or ship on, an
# image nobody reviewed. Dependabot (docker, this directory) proposes digest bumps.
#
# The builder's Go is the standard library compiled into the server: it must be at least
# go.mod's `toolchain` (the image sets GOTOOLCHAIN=local, so an older Go would build with
# its own, vulnerable, standard library rather than fetch the pinned one).

# --- builder ------------------------------------------------------------------------
# golang:1.26
FROM golang:1.26@sha256:d7722066f0b60ceccb6c0643cbed1f5f9e15506ac237146c95333504d7805d89 AS builder

WORKDIR /src

# Modules first, so the dependency layer caches across source changes.
COPY go.mod go.sum ./
# Refuse a builder older than go.mod's `toolchain` (see above) instead of shipping its
# standard library.
RUN want="$(awk '$1 == "toolchain" { print $2 }' go.mod)"; \
    have="$(go env GOVERSION)"; \
    if [ -n "$want" ] && [ "$(printf '%s\n%s\n' "$want" "$have" | sort -V | head -n 1)" != "$want" ]; then \
        echo "the builder has Go '$have'; go.mod requires toolchain $want: bump the golang digest" >&2; \
        exit 1; \
    fi
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .
# CGO off: SQLite here is pure Go, so the binary is static and needs no libc at runtime.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" \
    -o /out/kontra-temporal ./cmd/kontra-temporal

# --- runtime ------------------------------------------------------------------------
# gcr.io/distroless/static-debian12:nonroot
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS runtime

COPY --from=builder /out/kontra-temporal /usr/local/bin/kontra-temporal
COPY --from=builder /src/dynamicconfig/kontra.yaml /etc/kontra/dynamicconfig.yaml

# The public frontend binds every interface (it sits behind Kontra's TLS proxy). The
# internal frontend has no authentication at all -- whoever reaches it is Temporal's own
# principal -- so it stays on loopback unless a deployment opts in: compose or the VPC
# sets KONTRA_INTERNAL_FRONTEND_BIND_IP to the private interface the gate uses, and never
# publishes 7236/7246 (PRD §10.2). With loopback by default, a mistaken -p or
# --network host does not expose it.
ENV KONTRA_TEMPORAL_DYNAMIC_CONFIG=/etc/kontra/dynamicconfig.yaml \
    KONTRA_FRONTEND_BIND_IP=0.0.0.0

# Public frontend gRPC only.
EXPOSE 7233

USER nonroot
ENTRYPOINT ["kontra-temporal"]
CMD ["serve"]
