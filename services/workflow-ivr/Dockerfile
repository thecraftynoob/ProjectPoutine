# Build context is the monorepo ROOT, not this directory, e.g.:
#   docker build -f services/workflow-ivr/Dockerfile .
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/workflow-ivr ./services/workflow-ivr/cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/workflow-ivr /usr/local/bin/workflow-ivr
ENTRYPOINT ["/usr/local/bin/workflow-ivr"]
