# Architecture Flow — Service-to-Service Communication

**This file is mandatory and living — see `CLAUDE.md` Rule 1.** Whenever a
service, API endpoint, WebSocket gateway, or Pub/Sub event is created,
changed, or removed, update this file in the same change. Do not let it
drift from the code.

**Last updated:** 2026-09-13 (API Gateway built: REST routing via
grpc-gateway fronting Task Router + Tenant & Identity, end-user JWT
validation, WebSocket ticket + proxying for Agent Desktop)

---

## 1. Synchronous gRPC contracts

| Caller | Callee | Contract | Notes |
|---|---|---|---|
| **API Gateway** | **Task Router** | `TaskRouterService` + `TaskRouterAdminService` (`proto/task-router/v1`) | Every REST request grpc-gateway routes to one of these RPCs (see §1.1 below for the full route table) is translated to a real gRPC call against `task-router-svc:50054`, with the caller's bearer token forwarded as `authorization` metadata. |
| **API Gateway** | **Tenant & Identity Management** | `IdentityService` / `TenantService` (`proto/tenant-identity/v1`) | Same pattern, against `tenant-identity-svc:50051`. `IssueServiceToken` is deliberately NOT exposed via REST (no `google.api.http` annotation on that RPC) and is never called by API Gateway — it is service-to-service only. |
| *(external test clients only, direct)* | **Task Router** / **Tenant & Identity** | same as above | Both services remain directly gRPC-dialable in this dev topology (no mTLS/network policy blocking it yet) for throwaway smoke-test clients — API Gateway is the intended production entry point, not the only way to reach them today. |
| *(planned, not built)* | **Agent & Presence Service** | `PresenceService` (`proto/presence/v1`) | The service definition is intentionally empty today (`service PresenceService {}`) — its original `GetAvailableAgents` RPC was removed because Task Router already owns all agent routing state and never needed to call it. Kept as a stable package for a future RPC (e.g. "is agent X currently connected"). |

**All gRPC calls require `authorization: Bearer <jwt>` metadata**,
verified by `pkg/tenantctx`'s interceptor against Tenant & Identity's
signing public key (distributed via the `tenant-identity-public-key` K8s
ConfigMap). `x-tenant-id` metadata is attached for logging only and is
never trusted for authorization. See `CLAUDE.md` Rule 2/3 and
`CCAAS_ENTERPRISE_ARCHITECTURE.md` §1.1.

### 1.1 REST → gRPC route table (API Gateway, via grpc-gateway)

`google.api.http` annotations were added directly to the RPCs in
`proto/task-router/v1/task_router.proto`, `task_router_admin.proto`, and
`proto/tenant-identity/v1/tenant_identity.proto` (one source of truth per
RPC). `protoc-gen-grpc-gateway` (wired into `proto/buf.gen.yaml`, using
`google/api/http.proto`/`annotations.proto` vendored locally under
`proto/google/api/`) generates a `*.pb.gw.go` per proto file, registering
a `runtime.ServeMux` API Gateway builds into its HTTP server
(`services/api-gateway/internal/httpapi`).

| Method | Path | Backend RPC | Auth |
|---|---|---|---|
| POST | `/v1/tenants` | `TenantService.CreateTenant` | exempt (bootstrapping) |
| GET | `/v1/tenants` | `TenantService.ListTenants` | exempt (registry read) |
| GET | `/v1/tenants/{tenant_id}` | `TenantService.GetTenant` | exempt (registry read) |
| POST | `/v1/tenants/{tenant_id}/users` | `IdentityService.CreateUser` | exempt (bootstrapping) |
| POST | `/v1/auth/login` | `IdentityService.Login` | exempt (bootstrapping) |
| GET | `/v1/users/{user_id}` | `IdentityService.GetUser` | required |
| GET | `/v1/users` | `IdentityService.ListUsers` | required |
| — | *(no REST route)* | `IdentityService.IssueServiceToken` | n/a — service-to-service only, deliberately no `google.api.http` annotation |
| POST | `/v1/agents` | `TaskRouterService.CreateAgent` | required |
| GET | `/v1/agents` | `TaskRouterService.ListAgents` | required |
| GET | `/v1/agents/{agent_id}` | `TaskRouterService.GetAgent` | required |
| DELETE | `/v1/agents/{agent_id}` | `TaskRouterService.DeleteAgent` | required |
| POST | `/v1/agents/{agent_id}/status` | `TaskRouterService.SetAgentStatus` | required |
| POST | `/v1/agents/{agent_id}/capacity` | `TaskRouterService.ReplaceAgentCapacity` | required |
| POST | `/v1/agents/{agent_id}/capacity/{channel}/toggle` | `TaskRouterService.ToggleChannelReady` | required |
| POST | `/v1/agents/{agent_id}/queues` | `TaskRouterService.ReplaceAgentQueues` | required |
| POST | `/v1/agents/{agent_id}/attributes` | `TaskRouterService.ReplaceAgentAttributes` | required |
| GET | `/v1/agents/{agent_id}/offers` | `TaskRouterService.ListAgentPendingOffers` | required |
| POST | `/v1/tasks` | `TaskRouterService.EnqueueTask` | required |
| GET | `/v1/tasks` | `TaskRouterService.ListTasks` | required |
| GET | `/v1/tasks/{task_id}` | `TaskRouterService.GetTask` | required |
| POST | `/v1/tasks/{task_id}/complete` | `TaskRouterService.CompleteTask` | required |
| POST | `/v1/reservations/{reservation_id}/accept` | `TaskRouterService.AcceptReservation` | required |
| POST | `/v1/reservations/{reservation_id}/reject` | `TaskRouterService.RejectReservation` | required |
| GET | `/v1/dashboard` | `TaskRouterService.GetDashboard` | required |
| POST | `/v1/admin/queues` | `TaskRouterAdminService.RegisterQueue` | required |
| GET | `/v1/admin/queues` | `TaskRouterAdminService.ListQueues` | required |
| GET | `/v1/admin/queues/{queue_id}` | `TaskRouterAdminService.GetQueue` | required |
| DELETE | `/v1/admin/queues/{queue_id}` | `TaskRouterAdminService.RemoveQueue` | required |
| POST | `/v1/admin/statuses` | `TaskRouterAdminService.RegisterStatus` | required |
| GET | `/v1/admin/statuses` | `TaskRouterAdminService.ListStatuses` | required |
| DELETE | `/v1/admin/statuses/{status}` | `TaskRouterAdminService.RemoveStatus` | required |
| POST | `/v1/admin/attributes` | `TaskRouterAdminService.RegisterAttribute` | required |
| GET | `/v1/admin/attributes` | `TaskRouterAdminService.ListAttributes` | required |
| GET | `/v1/admin/attributes/{name}` | `TaskRouterAdminService.GetAttribute` | required |
| DELETE | `/v1/admin/attributes/{name}` | `TaskRouterAdminService.RemoveAttribute` | required |
| POST | `/v1/ws-ticket` | *(gateway-local — see §4 below, not a backend RPC)* | required (normal session bearer token) |
| GET | `/ws?ticket=<jwt>` | *(gateway-local proxy to Agent Presence — see §4.1 below)* | exempt from `gwauth.Middleware` — authenticates itself via `?ticket=`, verified by `internal/wsproxy` before ever dialing upstream |

"Required" bearer tokens are validated by
`services/api-gateway/internal/gwauth.Middleware` against Tenant &
Identity's session-signing public key, then forwarded UNMODIFIED to the
backend as outgoing gRPC `authorization` metadata (grpc-gateway forwards
the `Authorization` header automatically — see that package's doc
comment) so the backend's own `pkg/tenantctx` interceptor re-verifies it
independently (Layer 2 defense in depth, architecture doc §1.1).
"Exempt" routes mirror `tenant_identity.proto`'s TENANTCTX EXEMPTION list
exactly (Login, CreateTenant, CreateUser) plus GetTenant/ListTenants
(platform-registry reads, not end-user tenant-scoped data).

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
| Any REST client | **API Gateway** | HTTP/JSON (`/v1/...`, see §1.1's route table) | `Authorization: Bearer <jwt>` on every non-exempt route, validated by `internal/gwauth` against Tenant & Identity's session public key. |
| Agent Desktop, native/non-browser client | **Agent & Presence Service** | `GET /ws` WebSocket upgrade, direct | `Authorization: Bearer <jwt>` HTTP header on the upgrade request (original path, unchanged). `tenant_id` ← JWT `tid` claim, `agent_id` ← JWT `sub` claim (a documented pragmatic stand-in — see `services/agent-presence/internal/wsserver/wsserver.go`'s doc comment for the Task-Router-Agent-vs-Tenant-Identity-User reconciliation gap). Delivers a JSON envelope per forwarded event: `{"type": "...", "agentId": "...", "payload": {...}}`. |
| Agent Desktop, browser client | **API Gateway** → proxied to **Agent & Presence Service** | `GET /ws?ticket=<jwt>` WebSocket upgrade, proxied through API Gateway (`internal/wsproxy`) | Short-lived (default 45s) single-purpose ws-ticket, obtained from `POST /v1/ws-ticket` (itself requiring a normal session bearer token) and signed by API Gateway's OWN dedicated ECDSA key (`internal/wsticket`) — deliberately separate from Tenant & Identity's session key, since API Gateway never holds that private key. See §4.1 below for the full flow and design rationale. |
| Any external caller | **Tenant & Identity Management** | gRPC (direct) or REST (via API Gateway, §1.1) | `CreateTenant`, `Login`, `IssueServiceToken` (and a few others — see the proto's exempt-method doc comment) are exempt from `pkg/tenantctx`'s bearer-token requirement, since they're how a caller first obtains a tenant/token context. Every other RPC on every service requires a valid bearer JWT. |

### 4.1 WebSocket ticket flow (browser Agent Desktop clients)

A browser's native `WebSocket` API cannot set custom headers on the
upgrade request, so it cannot present `Authorization: Bearer <jwt>` the
way a native/server-side client can. The flow this milestone builds:

1. Client already holds a normal session JWT (from `POST /v1/auth/login`).
2. Client calls `POST /v1/ws-ticket` with that JWT as a normal bearer
   token (NOT exempt — this is not a bootstrapping operation, an
   already-authenticated caller is required).
3. API Gateway verifies the session JWT, then mints a new, short-lived
   (default 45s) ticket carrying the SAME `tid`/`sub`/`roles` claims,
   signed with API Gateway's own dedicated ws-ticket ECDSA keypair
   (generated fresh in memory at every API Gateway startup, never
   persisted — see `services/api-gateway/internal/wsticket`'s package doc
   comment).
4. Client opens `wss://<gateway>/ws?ticket=<ticket>`.
5. API Gateway (`internal/wsproxy`) validates the ticket, then **proxies**
   the WebSocket connection to Agent & Presence Service's own `/ws`
   endpoint, forwarding the same ticket as `?ticket=` there too.
6. Agent Presence independently re-verifies the ticket against API
   Gateway's ws-ticket public key (distributed via the
   `api-gateway-ws-ticket-public-key` K8s ConfigMap, mounted the same
   manual-copy way as `tenant-identity-public-key`) — Layer 2 defense in
   depth, the same principle as REST bearer-token forwarding in §1.1.

**Design choice — proxy, not redirect:** API Gateway proxies the
WebSocket (holds one open upstream connection per active client and pumps
frames both ways) rather than telling the client to connect directly to
Agent Presence. This matches architecture doc §2.2's literal description
of this layer's job ("WebSocket upgrade proxying for Agent Desktop") and
keeps API Gateway as the genuinely single external entry point (§2.1's
service map draws `APS <--> WebSocket <--> GW` as the edge, not Agent
Presence reachable directly from outside the cluster) — the honestly-noted
tradeoff is one extra network hop and an open connection held per client
for its lifetime, an accepted standard reverse-proxy cost at this
system's scale. See `services/api-gateway/internal/wsproxy`'s package doc
comment for the full discussion.

**Scoping — short TTL, not true single-use:** a ws-ticket is not
replay-protected; there is no server-side used-ticket registry on either
side. The short TTL alone is judged sufficient for this milestone (an
intercepted ticket has a window well under a minute); a database-backed
single-use registry is explicitly out of scope, not silently skipped —
see `internal/wsticket`'s package doc comment.

Agent Presence's `/ws` endpoint still accepts the original
`Authorization: Bearer <jwt>` header path completely unchanged, for any
client that CAN set custom headers (native/server-side clients, test
harnesses) — the two paths coexist, sharing one claims-resolution helper
(`internal/wsserver.ResolveClaims`) so neither duplicates the
tenant/agent-identity mapping logic.

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
