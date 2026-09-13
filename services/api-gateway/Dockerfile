# Build context is the monorepo ROOT, not this directory, e.g.:
#   docker build -f services/api-gateway/Dockerfile .
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/api-gateway ./services/api-gateway/cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/api-gateway /usr/local/bin/api-gateway
ENTRYPOINT ["/usr/local/bin/api-gateway"]
