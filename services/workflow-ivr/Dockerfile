# Build context is the monorepo ROOT, not this directory, e.g.:
#   docker build -f services/workflow-ivr/Dockerfile .
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/workflow-ivr ./services/workflow-ivr/cmd

# grpc_health_probe: a small static binary the exec-based liveness/readiness
# probes in deploy/k8s/*/deployment.yaml invoke to check the gRPC health
# service registered by pkg/health. Pinned version, checksum unverified here
# (single-arch dev image) -- bump deliberately, not via a floating tag.
FROM golang:1.27 AS health-probe
RUN mkdir -p /out && \
    GRPC_HEALTH_PROBE_VERSION=v0.4.57 && \
    wget -qO/out/grpc_health_probe \
      https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /out/grpc_health_probe

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/workflow-ivr /usr/local/bin/workflow-ivr
COPY --from=health-probe /out/grpc_health_probe /usr/local/bin/grpc_health_probe
ENTRYPOINT ["/usr/local/bin/workflow-ivr"]
