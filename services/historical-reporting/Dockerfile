# Build context is the monorepo ROOT, not this directory, e.g.:
#   docker build -f services/historical-reporting/Dockerfile .
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/historical-reporting ./services/historical-reporting/cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/historical-reporting /usr/local/bin/historical-reporting
ENTRYPOINT ["/usr/local/bin/historical-reporting"]
