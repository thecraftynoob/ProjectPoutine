# ProjectPoutine — CCaaS Platform (monorepo)

Multi-tenant Contact Center as a Service platform. Go microservices, gRPC
contracts, NATS JetStream event bus, PostgreSQL, Redis.

**Design source of truth:** read these two documents before changing
anything structural.

- [`CCAAS_ENTERPRISE_ARCHITECTURE.md`](./CCAAS_ENTERPRISE_ARCHITECTURE.md) — service topology, multi-tenancy model, data/IPC architecture, naming conventions.
- [`TASK_ROUTER_SPECIFICATION.md`](./TASK_ROUTER_SPECIFICATION.md) — the Task Router's full domain spec (routing algorithm, state machines, event catalog). Not yet implemented as of this scaffold.

## Status

Repo layout, shared libraries, and proto contracts for `presence.v1` and
`taskrouter.v1` are in place. **Task Router** has a full production domain
implementation per `TASK_ROUTER_SPECIFICATION.md` sections 1-6 (Redis-backed
hot-path matching, Postgres-backed config registries, NATS event
publishing). The other 7 services are still skeletons that build, start,
and respond to gRPC health checks, with no domain logic yet.

All 9 services build into container images and deploy to the `ccaas-dev`
namespace on Docker Desktop's local Kubernetes — see "Running on
Kubernetes" below.

## Layout

```
/proto           buf-managed gRPC contracts (one per service, most empty)
/pkg              shared Go libraries (tenant context, event bus, pg helpers, health, config)
  /genproto       generated Go code from /proto (buf generate output)
/services         one directory per deployable service
  /{name}/cmd     main.go entrypoint
  /{name}/internal domain logic (empty scaffold today)
/deploy/k8s       one manifest set per service, namespace ccaas-dev
docker-compose.yml  infra only: Postgres 16, Redis 7, NATS (JetStream)
```

Services (per architecture doc Section 2.2): `tenant-identity`,
`voice-media-gateway`, `digital-channels-gateway`, `task-router`,
`agent-presence`, `workflow-ivr`, `historical-reporting`,
`background-worker-pool`, `api-gateway`.

## Running infra locally

```powershell
docker compose up -d
docker compose down
```

Copy `.env.local.example` to `.env.local` and adjust as needed — it documents
every `POSTGRES_*` / `REDIS_*` / `NATS_*` variable the compose file and
services read. `.env.local` is git-ignored.

## Running a service locally

```powershell
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
go run ./services/task-router/cmd
```

Each service reads its config from environment variables (see
`/pkg/config` and that service's `cmd/main.go`).

## Working with proto contracts

```powershell
cd proto
buf generate
```

Generated code lands in `/pkg/genproto/{package}`. Requires
`protoc-gen-go` and `protoc-gen-go-grpc` on `PATH` (`go install` targets —
see `Makefile`).

## Makefile targets

`proto-gen`, `build`, `test`, `tidy`, `compose-up`, `compose-down`. Run
`make <target>` from repo root (or invoke the underlying commands directly
on Windows without `make` — see the Makefile for the exact commands).

## Running on Kubernetes

Requires Docker Desktop's Kubernetes enabled (Settings → Kubernetes →
Enable Kubernetes). Infra (Postgres/Redis/NATS) still runs via
`docker compose up -d` on the host per architecture doc Section 1.4 — only
the services themselves run in the cluster, reaching that infra through
`host.docker.internal`.

```powershell
# 1. Build all 9 service images into Docker Desktop's local daemon
docker build -f services/task-router/Dockerfile -t ccaas/task-router:local .
# ...repeat per service, or see deploy/k8s/README.md for the full list

# 2. Namespace + shared infra connection config
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/infra-config.yaml

# 3. Postgres password secret (copy the example, do not commit your copy)
copy deploy\k8s\infra-secret.example.yaml deploy\k8s\infra-secret.local.yaml
kubectl apply -f deploy/k8s/infra-secret.local.yaml

# 4. Deploy every service
kubectl apply -f deploy/k8s/tenant-identity/
kubectl apply -f deploy/k8s/voice-media-gateway/
kubectl apply -f deploy/k8s/digital-channels-gateway/
kubectl apply -f deploy/k8s/task-router/
kubectl apply -f deploy/k8s/agent-presence/
kubectl apply -f deploy/k8s/workflow-ivr/
kubectl apply -f deploy/k8s/historical-reporting/
kubectl apply -f deploy/k8s/background-worker-pool/
kubectl apply -f deploy/k8s/api-gateway/

kubectl get pods -n ccaas-dev
```

See `deploy/k8s/README.md` for manifest conventions, the health-probe
setup, and how the infra ConfigMap/Secret are consumed.
