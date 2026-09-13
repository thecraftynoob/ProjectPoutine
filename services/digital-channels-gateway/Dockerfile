# Build context is the monorepo ROOT, not this directory, e.g.:
#   docker build -f services/digital-channels-gateway/Dockerfile .
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/digital-channels-gateway ./services/digital-channels-gateway/cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/digital-channels-gateway /usr/local/bin/digital-channels-gateway
ENTRYPOINT ["/usr/local/bin/digital-channels-gateway"]
