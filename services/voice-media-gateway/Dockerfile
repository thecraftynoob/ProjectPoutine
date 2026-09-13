# Build context is the monorepo ROOT, not this directory, e.g.:
#   docker build -f services/voice-media-gateway/Dockerfile .
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/voice-media-gateway ./services/voice-media-gateway/cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/voice-media-gateway /usr/local/bin/voice-media-gateway
ENTRYPOINT ["/usr/local/bin/voice-media-gateway"]
