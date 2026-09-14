# Project Rules & Instructions

Persistent guardrails for working in this repository. These apply to every
session, every service, every change — not just the one you're currently
making. If a change you're about to make would violate one of these rules,
stop and flag it rather than proceeding silently.

---

## 1. Living Data Flow Mapping (Mandatory)

**Rule:** Maintain a continuous, accurate mapping of "what talks to what."

**Action:** Whenever you create, modify, or delete a microservice, API
endpoint, WebSocket gateway, or Pub/Sub event, immediately update
`ARCHITECTURE_FLOW.md` (create it if it doesn't exist yet — it did not
exist as of this rule's adoption, 2026-09-13, and was created in the same
commit as this file).

**Format:** `ARCHITECTURE_FLOW.md` must clearly document service-to-service
communication, e.g.:
- `Task Router calls Agent & Presence Service via gRPC PresenceService (proto/presence/v1)`
- `Task Router publishes tenant.{tenant_id}.reservation.created to NATS JetStream; Agent & Presence Service subscribes`
- `Agent Desktop connects to Agent & Presence Service via WebSocket (GET /ws)`

Do not let this drift from reality. A stale `ARCHITECTURE_FLOW.md` is worse
than none — if you're not sure a section is still accurate, verify against
the actual code before trusting or extending it.

---

## 2. Kubernetes-First Networking

**Rule:** No hardcoded `127.0.0.1` or `localhost` for service-to-service
communication.

**Action:** Always use environment variables for hostnames and ports, so
the same code runs unmodified against Docker Desktop K8s locally and any
production-conformant cluster later, resolved via CoreDNS / Kubernetes
Service DNS names.

`localhost`/`127.0.0.1` may appear **only** as an `envDefault` fallback for
running a single service standalone via `go run` outside the cluster
(e.g. `REDIS_ADDR string \`env:"REDIS_ADDR" envDefault:"localhost:6379"\``)
— and only when every Kubernetes deployment manifest supplies a real
override (a ConfigMap-sourced value or a Service DNS name). If a manifest
doesn't override it, that's a bug: fix the manifest, don't rely on the
default reaching production topology by accident.

---

## 3. Strict Domain Boundaries

**Rule:** Maintain strict separation of concerns. The Task Router (logic)
must never directly handle media (SIP/WebRTC), and Media Gateways must
never execute routing logic.

**Action:** All inter-domain communication happens via the event bus
(NATS JetStream — see `pkg/eventbus`) or synchronous gRPC contracts
(defined in `/proto`), never by one service reading or writing another
service's database tables directly.

**Database sharing — the actual, current rule (not a hypothetical):**
Services in this repo already share one Postgres instance (see
`docker-compose.yml`, `deploy/k8s/infra-config.yaml`) — that is
intentional and will not change. What's forbidden is **cross-service
table access**: every table belongs to exactly one service's own
migrations (e.g. `services/task-router/internal/pgconfig/migrations/`,
`services/tenant-identity/internal/pgstore/migrations/`), and no other
service's code may query or write those tables directly. Tenant isolation
*within* a service's own tables is enforced by Postgres Row-Level Security
per `pkg/pgtenant`, not by separate database instances — see
`CCAAS_ENTERPRISE_ARCHITECTURE.md` §1.1 and §3.1 for the full rationale.
If a new feature seems to need Service A to read Service B's data, the
answer is an API call to B or a subscription to an event B publishes —
never a direct query against B's tables.

Redis is **not** the event bus in this repo — it backs Task Router's
hot-path routing state and Agent & Presence Service's cross-replica
WebSocket fan-out, two unrelated per-service uses. The domain event bus
(the mechanism this rule's Pub/Sub language refers to) is **NATS
JetStream**, tenant-scoped subjects (`tenant.{tenant_id}.{domain}.{event}`).

---

## Reference documents

Read these before making structural changes — they are the design source
of truth this file's rules are derived from:

- [`CCAAS_ENTERPRISE_ARCHITECTURE.md`](./CCAAS_ENTERPRISE_ARCHITECTURE.md) — service topology, multi-tenancy, data/IPC architecture.
- [`TASK_ROUTER_SPECIFICATION.md`](./TASK_ROUTER_SPECIFICATION.md) — Task Router's full domain spec.
- [`PROGRESS.md`](./PROGRESS.md) — current build status and to-do list. Read this first each session.
- [`ARCHITECTURE_FLOW.md`](./ARCHITECTURE_FLOW.md) — the living data-flow map this file's Rule 1 requires (create on first use per Rule 1).
