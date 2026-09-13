# Build context is the monorepo ROOT, not this directory, e.g.:
#   docker build -f services/task-router/Dockerfile .
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/task-router ./services/task-router/cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/task-router /usr/local/bin/task-router
ENTRYPOINT ["/usr/local/bin/task-router"]
