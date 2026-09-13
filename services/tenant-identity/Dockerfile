# Build context is the monorepo ROOT, not this directory, e.g.:
#   docker build -f services/tenant-identity/Dockerfile .
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/tenant-identity ./services/tenant-identity/cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/tenant-identity /usr/local/bin/tenant-identity
ENTRYPOINT ["/usr/local/bin/tenant-identity"]
