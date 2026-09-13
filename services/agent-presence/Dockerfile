# Build context is the monorepo ROOT, not this directory, e.g.:
#   docker build -f services/agent-presence/Dockerfile .
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/agent-presence ./services/agent-presence/cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/agent-presence /usr/local/bin/agent-presence
ENTRYPOINT ["/usr/local/bin/agent-presence"]
