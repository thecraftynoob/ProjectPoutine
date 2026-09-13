# Build context is the monorepo ROOT, not this directory, e.g.:
#   docker build -f services/background-worker-pool/Dockerfile .
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/background-worker-pool ./services/background-worker-pool/cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/background-worker-pool /usr/local/bin/background-worker-pool
ENTRYPOINT ["/usr/local/bin/background-worker-pool"]
