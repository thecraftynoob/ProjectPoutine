# Architecture Flow — Service-to-Service Communication

**This file is mandatory and living — see `CLAUDE.md` Rule 1.** Whenever a
service, API endpoint, WebSocket gateway, or Pub/Sub event is created,
changed, or removed, update this file in the same change. Do not let it
drift from the code.

**Last updated:** 2026-09-14 (initial creation, backfilled against current `main`)

---

## 1. Synchronous gRPC contracts

| Caller | Callee | Contract | Notes |
|---|---|---|---|
| *(none yet)* | **Tenant & Identity Management** | `IdentityService` / `TenantService` (`proto/tenant-identity/v1`) | No service currently dials this in steady-state application code. `pkg/svcauth.TokenSource` exists as the client-side helper for any future caller to obtain a service token via `IssueServiceToken`, but nothing calls it yet — Task Router and Agent Presence were wired with the env vars/Secret to do so (`TENANT_IDENTITY_GRPC_ADDR`, `SERVICE_SHARED_SECRET`) as forward-compatible plumbing, not because they call it today. |
| *(planned, not built)* | **Agent & Presence Service** | `PresenceService` (`proto/presence/v1`) | The service definition is intentionally empty today (`service PresenceService {}`) — its original `GetAvailableAgents` RPC was removed because Task Router already owns all agent routing state and never needed to call it. Kept as a stable package for a future RPC (e.g. "is agent X currently connected"). |
| *(external test clients only)* | **Task Router** | `TaskRouterService` + `TaskRouterAdminService` (`proto/task-router/v1`) | Agent CRUD/presence/capacity/queues, Task lifecycle, Reservation handshake, Queue/Status/Attribute registries, dashboard view. No other service in this repo calls it today — it's exercised by end-user/operator clients (or, in dev, throwaway smoke-test clients) via the API Gateway (once built) or directly. |

**All gRPC calls (once any exist) require `authorization: Bearer <jwt>`
metadata**, verified by `pkg/tenantctx`'s interceptor against Tenant &
Identity's signing public key (distributed via the `tenant-identity-public-key`
K8s ConfigMap). `x-tenant-id` metadata is attached for logging only and is
never trusted for authorization. See `CLAUDE.md` Rule 2/3 and
`CCAAS_ENTERPRISE_ARCHITECTURE.md` §1.1.

---

## 2. Event bus (NATS JetStream) — publishers and subscribers

**Stream:** `TASK_ROUTER_EVENTS` (created by Task Router via
`pkg/eventbus.EnsureStream`). **Subject convention:**
`tenant.{tenant_id}.{domain}.{event_type}` (architecture doc §5).

### Published by Task Router (`services/task-router/internal/events/events.go`)

| Domain | Event (subject suffix) | Trigger |
|---|---|---|
| `task` | `task.enqueued` | A new task is created |
| `task` | `task.accepted` | A reservation for the task is accepted |
| `task` | `task.completed` | The task-completion capability is invoked |
| `agent` | `agent.created` | A new agent is provisioned |
| `agent` | `agent.status.changed` | Master status updated |
| `agent` | `agent.capacity_config.updated` | Capacity map replaced or one channel's ready flag toggled |
| `agent` | `agent.queues.updated` | Queue memberships replaced |
| `agent` | `agent.deleted` | Agent removed |
| `reservation` | `reservation.created` | Matching algorithm commits a match |
| `reservation` | `reservation.accepted` | Accept capability invoked |
| `reservation` | `reservation.rejected` | Manual reject, automatic expiry, or agent-deletion-triggered rejection |

Full catalog and exact trigger semantics: `TASK_ROUTER_SPECIFICATION.md` §6.

### Subscribed by Agent & Presence Service (`services/agent-presence/internal/relay/`)

Consumes **only** the filtered subset TASK_ROUTER_SPECIFICATION.md §6.3
designates for real-time client delivery — every other event above is
published but has no consumer today:

- `tenant.*.reservation.created`
- `tenant.*.reservation.rejected`
- `tenant.*.agent.status.changed`
- `tenant.*.agent.deleted`

Each uses its own **ephemeral** JetStream consumer per replica
(`pkg/eventbus.SubscribeEphemeral`) — not a shared durable name — so every
replica of Agent Presence independently receives every event (required for
the cross-replica Redis fan-out below to work correctly; a shared durable
consumer name would make replicas compete for messages instead of each
seeing every one).

**Historical Reporting** (`services/historical-reporting`) is the
documented future consumer of the *full* event catalog for BI/audit
purposes (architecture doc §2.2) — not yet implemented (still a stub).

---

## 3. Redis — not the event bus, two unrelated per-service uses

| Service | Use | Detail |
|---|---|---|
| **Task Router** | Hot-path routing state | Agent/Task/Reservation live state as Redis hashes/sets, one Lua script per atomic mutation. System of record for in-flight routing data — see `TASK_ROUTER_SPECIFICATION.md` §5.4 and `services/task-router/internal/redisdomain/`. Also used for TTL + keyspace-notification-driven reservation expiry. |
| **Agent & Presence Service** | Cross-replica WebSocket delivery fan-out | Every replica publishes a relayed event to `agent-presence:tenant:{tenant_id}:events`; every replica (including the publisher) subscribes and delivers only if it locally owns the target connection. Pure pub/sub transport — nothing persisted. See `services/agent-presence/internal/relay/fanout.go`. |

Do not confuse either use with the domain event bus (NATS JetStream, §2
above) when adding new inter-service communication.

---

## 4. Client-facing transports (not service-to-service, but part of the flow)

| Client | Service | Transport | Auth |
|---|---|---|---|
| Agent Desktop (future; smoke-tested with a throwaway client today) | **Agent & Presence Service** | `GET /ws` WebSocket upgrade | `Authorization: Bearer <jwt>` HTTP header on the upgrade request. `tenant_id` ← JWT `tid` claim, `agent_id` ← JWT `sub` claim (a documented pragmatic stand-in — see `services/agent-presence/internal/wsserver/wsserver.go`'s doc comment for the Task-Router-Agent-vs-Tenant-Identity-User reconciliation gap). Delivers a JSON envelope per forwarded event: `{"type": "...", "agentId": "...", "payload": {...}}`. |
| Any external caller | **Tenant & Identity Management** | gRPC | `CreateTenant`, `Login`, `IssueServiceToken` (and a few others — see the proto's exempt-method doc comment) are exempt from `pkg/tenantctx`'s bearer-token requirement, since they're how a caller first obtains a tenant/token context. Every other RPC on every service requires a valid bearer JWT. |

**API Gateway** (`services/api-gateway`) is the architecturally intended
single external entry point (REST + WebSocket-upgrade proxying fronting
the internal gRPC services above) but is still an unimplemented stub — see
`PROGRESS.md`.

---

## 5. Database instance vs. table ownership

One shared Postgres instance (`docker-compose.yml` / `deploy/k8s/infra-config.yaml`),
by design — see `CLAUDE.md` Rule 3. Table ownership, not instance count, is
the enforced boundary:

| Service | Owns (via its own migrations) |
|---|---|
| **Task Router** | `task_router_queues`, `task_router_statuses`, `task_router_attributes` (`services/task-router/internal/pgconfig/migrations/`) |
| **Tenant & Identity Management** | `tenants`, `tenant_identity_users`, the signing-keypair table (`services/tenant-identity/internal/pgstore/migrations/`) |
| **Background Worker Pool** | `background_jobs` (`pkg/pgqueue` — shared schema pattern, not yet consumed by a real job type) |

No service queries another service's tables directly. Cross-service data
needs go through that service's gRPC API or a published event — never a
shared-table shortcut.

---

*Update this file in the same change that adds/removes a service,
endpoint, event, or gateway. If you're reading this and a section looks
stale, verify against the actual code before extending it — see
`CLAUDE.md` Rule 1.*
