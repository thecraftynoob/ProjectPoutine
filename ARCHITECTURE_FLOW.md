# Architecture Flow — Service-to-Service Communication

**This file is mandatory and living — see `CLAUDE.md` Rule 1.** Whenever a
service, API endpoint, WebSocket gateway, or Pub/Sub event is created,
changed, or removed, update this file in the same change. Do not let it
drift from the code.

**Last updated:** 2026-09-14 (Postgres connection-identity fix: every
service that touches Postgres now runs its ongoing, steady-state queries
as a new non-superuser role instead of the superuser every service
previously connected as unconditionally, which had silently made every
Row-Level Security policy in this repo a no-op against real traffic —
this was a genuine bug, not a scope decision; see §5's rewritten Postgres
section below and `GAPS.md`'s "Closed gaps" section for the full
writeup). Previously, same day: Background Worker Pool's first real
milestone built: a durable NATS JetStream consumer subscribing ONLY to
`tenant.*.task.completed`, enqueuing a `wrapup_sync` row into
`background_jobs` (Database-as-a-Queue), which `pkg/pgqueue.Poller`
claims and a handler POSTs as JSON to a tenant-configured URL — see §2's
new subsection below and §5's new table-ownership row. Previously, same
day: Historical Reporting's first real milestone built: a durable NATS
JetStream consumer materializing Task Router's full event catalog into
one generic Postgres table, `historical_events` — see §2's other new
subsection below. Ingestion only, no read/query API this pass.
Previously, same day: Digital Channels Gateway's first real milestone
built: inbound `POST /webhooks/chat/{tenant_id}` webhook, normalizing one
generic chat message shape into a Task Router `EnqueueTask` call via
`pkg/svcauth`. Previously: 2026-09-13, API Gateway built — REST routing
via grpc-gateway fronting Task Router + Tenant & Identity, end-user JWT
validation, WebSocket ticket + proxying for Agent Desktop)

---

## 1. Synchronous gRPC contracts

| Caller | Callee | Contract | Notes |
|---|---|---|---|
| **API Gateway** | **Task Router** | `TaskRouterService` + `TaskRouterAdminService` (`proto/task-router/v1`) | Every REST request grpc-gateway routes to one of these RPCs (see §1.1 below for the full route table) is translated to a real gRPC call against `task-router-svc:50054`, with the caller's bearer token forwarded as `authorization` metadata. |
| **API Gateway** | **Tenant & Identity Management** | `IdentityService` / `TenantService` (`proto/tenant-identity/v1`) | Same pattern, against `tenant-identity-svc:50051`. `IssueServiceToken` is deliberately NOT exposed via REST (no `google.api.http` annotation on that RPC) and is never called by API Gateway — it is service-to-service only. |
| **Digital Channels Gateway** | **Task Router** | `TaskRouterService.EnqueueTask` (`proto/task-router/v1`) | Triggered by an inbound webhook request (see §4.2 below) — normalizes the webhook body into an `EnqueueTaskRequest` (fixed `task_type: "chat"`, `queue_id` mapped straight through from the webhook body, `required_attributes` mapped from the webhook's optional `attributes` map) and calls Task Router at `task-router-svc:50054`. Authenticated via `pkg/svcauth`: Digital Channels Gateway mints a service JWT scoped to the webhook path's `tenant_id` by calling Tenant & Identity's `IssueServiceToken` (`tenant-identity-svc:50051`, using `SERVICE_SHARED_SECRET` — same shared-credential mechanism task-router and agent-presence already use, see `deploy/k8s/service-credential.example.yaml`), then attaches it as `authorization: Bearer <token>` metadata. Unlike task-router's/agent-presence's existing `svcauth` usage (one fixed tenant baked in at process start), this is genuinely per-request tenant scoping — see `services/digital-channels-gateway/internal/webhookapi/taskrouterclient.go`'s `TenantScopedTaskRouterClient` doc comment for the resulting per-tenant `TokenSource` cache and its explicitly-flagged unbounded-map tradeoff. |
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
| POST | `/v1/tasks` | `TaskRouterService.EnqueueTask` | required — optional `wrap_up_timeout_seconds` body field (Wrap Up / Disposition lifecycle, 2026-09-14); 0 (default) means no wrap-up timer |
| GET | `/v1/tasks` | `TaskRouterService.ListTasks` | required — optional `?status=` query param (`Pending`\|`Reserved`\|`Active`\|`WrapUp`\|`Completed`) filters to one status; an unrecognized value is rejected with `InvalidArgument` rather than silently returning an empty list (2026-09-14) |
| GET | `/v1/tasks/{task_id}` | `TaskRouterService.GetTask` | required |
| POST | `/v1/tasks/{task_id}/complete` | `TaskRouterService.CompleteTask` | required — now valid from `Active` OR `WrapUp` (2026-09-14); resets the assigned agent's status to `Available` |
| POST | `/v1/tasks/{task_id}/end` | `TaskRouterService.EndTask` | required — new (2026-09-14). Wrap Up / Disposition lifecycle step 1: stops the communication channel, moves the task `Active`→`WrapUp` and the assigned agent's status to `WrapUp`; starts the wrap-up timer if `wrap_up_timeout_seconds` > 0 |
| POST | `/v1/tasks/{task_id}/disposition` | `TaskRouterService.SetTaskDisposition` | required — new (2026-09-14). Tags a `WrapUp` task with a registered Disposition; rejected unless the task is currently `WrapUp` |
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
| POST | `/v1/admin/dispositions` | `TaskRouterAdminService.RegisterDisposition` | required — new (2026-09-14) |
| GET | `/v1/admin/dispositions` | `TaskRouterAdminService.ListDispositions` | required — new (2026-09-14) |
| DELETE | `/v1/admin/dispositions/{disposition_id}` | `TaskRouterAdminService.RemoveDisposition` | required — new (2026-09-14) |
| POST | `/v1/admin/queues/{queue_id}/dispositions` | `TaskRouterAdminService.AssociateQueueDispositions` | required — new (2026-09-14). Full replace, all-or-nothing |
| GET | `/v1/admin/queues/{queue_id}/dispositions` | `TaskRouterAdminService.ListQueueDispositions` | required — new (2026-09-14) |
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
| `task` | `task.completed` | The task-completion capability is invoked. Payload now includes `dispositionId`/`dispositionName` (nullable) — Wrap Up / Disposition lifecycle, 2026-09-14 |
| `task` | `task.ended` | **New (2026-09-14).** `EndTask` invoked — step 1 of the Wrap Up / Disposition lifecycle |
| `task` | `task.disposition.set` | **New (2026-09-14).** `SetTaskDisposition` invoked |
| `agent` | `agent.created` | A new agent is provisioned |
| `agent` | `agent.status.changed` | Master status updated |
| `agent` | `agent.capacity.config.updated` | Capacity map replaced or one channel's ready flag toggled |
| `agent` | `agent.queues.updated` | Queue memberships replaced |
| `agent` | `agent.deleted` | Agent removed |
| `agent` | `agent.wrapup.timed_out` | **New (2026-09-14).** A task's wrap-up timer reached 0 while the task was still `WrapUp`, resetting the agent's status to `Available` |
| `reservation` | `reservation.created` | Matching algorithm commits a match |
| `reservation` | `reservation.accepted` | Accept capability invoked |
| `reservation` | `reservation.rejected` | Manual reject, automatic expiry, or agent-deletion-triggered rejection |

Full catalog and exact trigger semantics: `TASK_ROUTER_SPECIFICATION.md` §6.

**Wrap Up / Disposition two-step completion lifecycle (2026-09-14):** a new
`Task` status, `WrapUp`, sits between `Active` and `Completed`. `EndTask`
(step 1 — stopping the communication channel) moves `Active`→`WrapUp` and
sets the assigned agent's status to the new system-assigned value
`WrapUp`; an optional per-task `wrap_up_timeout_seconds` (set at
`EnqueueTask` time) drives a Redis TTL + keyspace-notification timer
(`services/task-router/internal/redisdomain/expiry.go`'s
`SubscribeWrapUpTimeout`, mirroring reservation-expiry's mechanism
exactly) — if it reaches 0 while the task is still `WrapUp`, the agent's
status is reset to `Available`, but the task itself stays `WrapUp` (no
auto-complete). `SetTaskDisposition` (step 2's data element) tags a
`WrapUp` task with a tenant-registered Disposition (new Postgres registry,
`task_router_dispositions` + `task_router_queue_dispositions` join table,
same RLS-protected pattern as Queue/Status/Attribute) any time before the
timer fires. `CompleteTask` (step 2 — formal completion) is now valid from
either `Active` (skipping WrapUp entirely, the original unmodified path)
or `WrapUp`, and — new — always resets the assigned agent's status to
`Available`. The disposition, if set, rides along in the `task.completed`
event payload, so Historical Reporting's existing generic JSONB ingestion
(`services/historical-reporting/internal/eventconsumer`) captures it on
the historical record with **no historical-reporting-side schema
change** — see that service's package doc comment for why its
`historical_events` table is deliberately schema-agnostic per event type.

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

### Subscribed by Historical Reporting (`services/historical-reporting/internal/eventconsumer`)

Unlike Agent & Presence Service's filtered subset above, Historical
Reporting consumes **the full event catalog, all three domains** — every
row in the table at the top of this section, task/agent/reservation
alike — for BI/SLA/compliance reporting (architecture doc §2.2). This is
by design: Agent Presence exists to relay a narrow, real-time-client-
relevant slice; Historical Reporting exists to durably materialize
everything, since a future reporting/analytics read path can't know in
advance which historical event types it'll need to query.

Mechanically the opposite of Agent Presence's approach in every way that
matters:

- **One durable, named JetStream consumer** (`pkg/eventbus.Client.Subscribe`,
  NOT `SubscribeEphemeral`) — durable consumer name
  `historical-reporting-ingest`, fixed and stable across restarts, since
  JetStream tracks delivery position server-side keyed by that name. This
  is the correct choice (not ephemeral) because Historical Reporting is a
  single-replica, stateful-ingestion service that must resume from
  exactly where it left off after a restart and never silently drop an
  event — the opposite requirement from Agent Presence's multi-replica
  fan-out, which needs every replica to independently see every event
  rather than compete for a shared position.
- **One subscribe call covers all three domains** via a single wildcard
  `FilterSubject` of `tenant.*.>` on the `TASK_ROUTER_EVENTS` stream,
  rather than three separate per-domain `Subscribe` calls — confirmed
  working (JetStream consumer `FilterSubject` supports the same wildcard
  syntax as stream subjects) by
  `services/historical-reporting/internal/eventconsumer/consumer_test.go`'s
  `TestConsumer_ReceivesAllThreeDomainsOnOneSubscribeCall`, which publishes
  one event per domain against a real in-process JetStream server and
  asserts a single `Consumer.Start` call ingests all three.
- **Defensive `EnsureStream`:** Historical Reporting calls
  `EnsureStream(TASK_ROUTER_EVENTS, ...)` itself at startup, using its own
  local copy of Task Router's subject list (see below), rather than
  assuming Task Router has already run first — `CreateOrUpdateStream` is
  idempotent, so this is safe to call redundantly from multiple services
  and removes any startup-order dependency between the two services.
- **No cross-service Go import.** `services/historical-reporting/internal/eventconsumer`
  does NOT import `services/task-router/internal/events` — both because
  Go's internal-package visibility forbids it (a different service's
  `internal/` tree) and because CLAUDE.md Rule 3 forbids the
  implementation coupling that would represent even if Go allowed it.
  Instead, `eventconsumer` defines its own local copies of the
  `TASK_ROUTER_EVENTS` stream name and the three domain subject patterns,
  documented as reproducing Task Router's public NATS contract (the
  subject-naming convention itself, not Task Router's Go types) — see
  that package's doc comment for the full reasoning.
- **Every event materializes into ONE generic table**,
  `historical_events` (`tenant_id`, `domain`, `event_type`, `subject`,
  `payload` JSONB, `received_at`, plus a fresh server-generated
  `event_id`) — no per-event-type schema, no read/query API yet (this
  milestone is ingestion-only; verification is direct SQL, not an RPC).
  See `services/historical-reporting/internal/pgstore`'s package doc
  comment and §5 below for table ownership.

**Known, documented gap — at-least-once delivery, no idempotency key:**
`event_id` is generated fresh at ingestion time, NOT derived from the
inbound NATS message, because Task Router's published `structpb.Struct`
payloads carry no stable application-level event ID of their own (see
`services/task-router/internal/events/events.go` — none of its 11
publisher methods include one). JetStream's `AckExplicitPolicy` guarantees
at-least-once delivery, not exactly-once: a crash after this service
inserts a row but before it acks the message will cause JetStream to
redeliver that message on reconnect, which — with no idempotency key to
dedupe against — inserts a second, distinct `event_id` row for what was
really one event. This is an accepted, explicitly documented milestone-
scope gap, not a silently swallowed one — the same tone/rigor as
`services/api-gateway/internal/wsticket`'s short-TTL-not-single-use
tradeoff. See `internal/eventconsumer`'s package doc comment for the full
write-up and what a future fix would need (either a stable event ID added
on the publish side, or a dedupe key derived from something already on
the wire, e.g. the JetStream message sequence number).

### Subscribed by Background Worker Pool (`services/background-worker-pool/internal/wrapupsync`)

Unlike both services above, Background Worker Pool consumes a **single,
narrow filter** — only `tenant.*.task.completed`, not Historical
Reporting's full-catalog wildcard and not Agent Presence's filtered
multi-event subset. This service only cares about one guarantee: "every
completed task must eventually get a wrap-up job."

- **One durable, named JetStream consumer**
  (`pkg/eventbus.Client.Subscribe`, NOT `SubscribeEphemeral`) — durable
  consumer name `background-worker-pool-wrapup`, fixed and stable across
  restarts. Durable (not ephemeral) for the same reason as Historical
  Reporting's consumer: this is a real guarantee worth having, not
  best-effort — confirmed with the user this milestone.
- **`FilterSubject` = `tenant.*.task.completed`** — deliberately narrower
  than Historical Reporting's `tenant.*.>`, since this service has no use
  for agent/reservation events or any other task event.
- **Defensive `EnsureStream`:** calls `EnsureStream(TASK_ROUTER_EVENTS, ...)`
  itself at startup with its own local copy of Task Router's subject
  list, for the same startup-ordering reason as Historical Reporting's
  consumer (`CreateOrUpdateStream` is idempotent, safe to call
  redundantly).
- **No cross-service Go import** — same two reasons as Historical
  Reporting's consumer (Go internal-package visibility; CLAUDE.md Rule 3).
  `internal/wrapupsync` defines its own local copies of the stream name
  and this one subject filter.
- **On receipt:** parses the subject for `tenant_id`, decodes the
  `structpb` payload for `taskId`/`agentId` (nullable), and calls
  `pkg/pgqueue.Enqueue` with `job_type = "wrapup_sync"` and payload
  `{taskId, agentId, tenantId}`. Acks on successful enqueue, Naks
  (leaving it for redelivery) on any parse/enqueue failure.
- **Then the Database-as-a-Queue pipeline takes over:** a
  `pkg/pgqueue.Poller` (run in `cmd/main.go`) claims pending `wrapup_sync`
  jobs via `FOR UPDATE SKIP LOCKED` and dispatches to
  `internal/wrapupsync.Handler`, which looks up the tenant's configured
  wrap-up target URL (`background_worker_pool_wrapup_targets` — see §5
  below and GAPS.md) and does a real `net/http` POST of the job payload as
  JSON, with a 5-second timeout. A 2xx response marks the job `'done'`
  (via the new `pkg/pgqueue.MarkDone` helper); anything else — non-2xx,
  timeout, connection failure — schedules a retry (`pkg/pgqueue.MarkFailed`
  with `retry=true`) with `run_after = now + attempts*30s`, capped at 5
  minutes, up to `maxAttempts = 5`, after which the job is marked
  permanently `'failed'`. A tenant with no configured target URL fails the
  job immediately (not retried — no amount of retrying fixes a missing
  config row). See GAPS.md for this backoff formula and max-attempts
  cutoff as documented scope decisions, not oversights.
- **`pkg/pgqueue.MarkDone`/`MarkFailed`** are new, genuinely generic
  helpers added to `pkg/pgqueue` itself (not job-type-specific) — the
  terminal-status transition Poller's own doc comment explicitly leaves to
  the caller/handler.

**Delivery guarantee / known gap:** the same at-least-once/no-dedupe
tradeoff as Historical Reporting's consumer (see above) — a crash between
`pgqueue.Enqueue` succeeding and the message being acked causes JetStream
to redeliver, producing a second `wrapup_sync` job for the same
`task.completed` event. Same root cause (no stable event ID on Task
Router's published payloads), not a new gap this service invents — see
PROGRESS.md To-Do #9b.

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
| Agent Desktop, native/non-browser client | **Agent & Presence Service** | `GET /ws` WebSocket upgrade, direct | `Authorization: Bearer <jwt>` HTTP header on the upgrade request (original path, unchanged). `tenant_id` ← JWT `tid` claim, `agent_id` ← JWT `sub` claim (a documented pragmatic stand-in — see `services/agent-presence/internal/wsserver/wsserver.go`'s doc comment for the Task-Router-Agent-vs-Tenant-Identity-User reconciliation gap). Delivers a JSON envelope per forwarded event: `{"type": "...", "agentId": "...", "payload": {...}}`. Task Router's `Agent` entity now has an optional `user_id` field (`proto/task-router/v1/task_router.proto`) recording which User operates it — purely informational today, not consulted by this mapping; see that proto message's doc comment. |
| Agent Desktop, browser client | **API Gateway** → proxied to **Agent & Presence Service** | `GET /ws?ticket=<jwt>` WebSocket upgrade, proxied through API Gateway (`internal/wsproxy`) | Short-lived (default 45s) single-purpose ws-ticket, obtained from `POST /v1/ws-ticket` (itself requiring a normal session bearer token) and signed by API Gateway's OWN dedicated ECDSA key (`internal/wsticket`) — deliberately separate from Tenant & Identity's session key, since API Gateway never holds that private key. See §4.1 below for the full flow and design rationale. |
| Any external caller | **Tenant & Identity Management** | gRPC (direct) or REST (via API Gateway, §1.1) | `CreateTenant`, `Login`, `IssueServiceToken` (and a few others — see the proto's exempt-method doc comment) are exempt from `pkg/tenantctx`'s bearer-token requirement, since they're how a caller first obtains a tenant/token context. Every other RPC on every service requires a valid bearer JWT. |
| Channel provider (e.g. a chat/SMS platform's webhook caller) | **Digital Channels Gateway** | `POST /webhooks/chat/{tenant_id}` (HTTP/JSON, direct — NOT via API Gateway) | **No signature/secret verification, no bearer token** — `tenant_id` comes solely from the URL path. A deliberate, explicitly-scoped tradeoff for this milestone, not an oversight — see §4.2 below and `services/digital-channels-gateway/internal/webhookapi`'s `ServeHTTP` doc comment. Different trust boundary than every other row in this table: webhooks are unauthenticated by nature (no provider account exists yet to hold a shared secret), so this endpoint is reached directly, not fronted by API Gateway's JWT-gated REST surface. |

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

### 4.2 Inbound chat webhook (Digital Channels Gateway)

**Scope (deliberately narrow — this milestone's build):** inbound only,
one generic "chat" channel shape, ending at a successfully enqueued Task
Router `Task`. No outbound/agent-reply delivery back to the channel, no
Postgres persistence of the message/session (architecture doc Section 2.2
explicitly defers message/thread persistence to future async workers, not
inline), no real per-provider (Twilio, etc.) integration.

1. A channel provider (or, today, a test caller standing in for one)
   `POST`s to `/webhooks/chat/{tenant_id}` — `tenant_id` is a URL path
   segment, not derived from a JWT, because a webhook has no bearer token
   to derive it from. This mirrors how real multi-tenant webhook
   provisioning works in practice (e.g. Twilio configured with a
   per-tenant callback URL) — each tenant gets its own webhook URL.
2. Body shape: `{"session_id", "from", "text", "queue_id",
   "attributes"}` — see `services/digital-channels-gateway/internal/webhookapi.InboundChatMessage`.
   `session_id`/`from`/`text`/`queue_id` are required (400 with a JSON
   error body if any is missing); `queue_id` must reference a queue that
   already exists in the target tenant's Task Router Queue registry (Task
   Router's `EnqueueTask` rejects it otherwise, mapped to 400 — see
   below). `attributes` is optional and maps into Task Router's
   `AttributeValue` oneof.
3. **No signature/secret verification on this endpoint** — an explicit,
   documented scope decision for this milestone (no real provider account
   exists yet to hold a shared secret against), not an oversight. See
   `internal/webhookapi`'s `ServeHTTP` doc comment for the full tradeoff
   write-up (mirroring how `services/api-gateway/internal/wsticket`
   documents its own short-TTL-not-single-use tradeoff instead of
   silently shipping it). **A production build MUST add per-tenant
   webhook signature verification before this endpoint faces a real
   untrusted provider** — today, anyone who discovers or guesses a
   tenant_id can enqueue tasks into that tenant's queues.
4. Digital Channels Gateway normalizes the body into an `EnqueueTaskRequest`
   (fixed `task_type: "chat"`) and calls Task Router's `EnqueueTask` as a
   service via `pkg/svcauth` — see §1's new Digital Channels Gateway →
   Task Router row above for the full auth mechanism.
5. On success: `201` with the created Task (`task_id`, `queue_id`,
   `task_type`, `status`) as JSON. On failure: Task Router's gRPC error is
   mapped to `400` (InvalidArgument/AlreadyExists — e.g. queue doesn't
   exist) or `502` (anything else, e.g. Internal/Unavailable) with a
   generic client-safe message; the real gRPC error is always logged
   server-side via `slog`, never leaked verbatim to the webhook caller.

---

## 5. Database instance vs. table ownership

One shared Postgres instance (`docker-compose.yml` / `deploy/k8s/infra-config.yaml`),
by design — see `CLAUDE.md` Rule 3. Table ownership, not instance count, is
the enforced boundary:

### 5.0 Two Postgres connection identities (fixed 2026-09-14)

Every service that touches Postgres (Task Router, Tenant & Identity,
Historical Reporting, Background Worker Pool) now opens **two** separate
`*pgxpool.Pool`/`pgtenant.Pool` connections, authenticated as two
different roles, instead of one:

- **Superuser (`ccaas`, `POSTGRES_DSN`)** — used ONLY to run that
  service's own `Migrate()` at startup: applying schema migrations, and
  (new) idempotently provisioning the runtime role below and `GRANT`ing
  it privileges. `CREATE ROLE`/`GRANT` require superuser or table-owner
  privilege, so this step legitimately needs the superuser connection.
  Never used for a service's ongoing, steady-state queries.
- **Runtime role (`ccaas_app`, `RUNTIME_POSTGRES_DSN`)** — a shared,
  non-superuser, `NOSUPERUSER NOBYPASSRLS` role every service's REAL,
  ongoing queries connect as (the pool passed to `pgconfig.NewRegistry`,
  `pgstore.NewUserStore`, `pgstore.NewStore`, `pgqueue.Poller`, etc.).
  Idempotently created and granted by each service's own `Migrate()` (see
  `services/*/internal/pg{store,config}/runtime_role.go`) — no manual
  operator step, no new shared `pkg`, safe under `go run`, docker-compose,
  and Kubernetes alike.

**Why this exists (a real bug, not a design choice):** docker-compose's
and Kubernetes' `postgres:16` bootstrap always creates `POSTGRES_USER`
("ccaas") as a Postgres **superuser** — that is simply how the official
image's bootstrap works, not something this repo's config asked for.
Postgres superusers **unconditionally bypass Row-Level Security**, and
`ALTER TABLE ... FORCE ROW LEVEL SECURITY` does not change that — FORCE
only binds the table *owner*, never a superuser. Every service in this
repo connected as `ccaas` for ALL queries, including steady-state ones,
since the beginning — which meant every RLS policy this repo ever wrote
(`tenant_identity_users`, `task_router_queues`/`statuses`/`attributes`)
was silently a no-op against real traffic, despite being correctly
written and correctly using `pkg/pgtenant.Pool.WithTenant` to set
`app.current_tenant` per-transaction. Confirmed live before the fix:
`IdentityService.ListUsers` returned users from every tenant in the
table, not just the caller's own. `pkg/pgtenant` itself, and every RLS
policy's SQL, were always correct — this was purely a connection-
privilege bug. See `GAPS.md`'s "Closed gaps" section for the full
writeup and the live cross-tenant-isolation proof that confirmed the fix.

**Scope:** ALL Postgres-touching services switch uniformly, not just the
two with RLS tables today (Task Router, Tenant & Identity) — Historical
Reporting's and Background Worker Pool's tables carry no RLS policy today
but switch too, so a future RLS-protected table never silently inherits
this same bug again. Services with no Postgres connection at all today
(Agent & Presence, API Gateway, Voice/SIP Media Gateway, Workflow &
IVR Orchestrator) needed no change. Digital Channels Gateway declares a
`POSTGRES_DSN` config field for future parity but never actually
connects it to anything yet (no `internal/pgstore` package exists for
this service per its own milestone scope) — also needed no change.

**Password provisioning:** one fixed, shared `ccaas_app` password,
provisioned via a new `POSTGRES_RUNTIME_PASSWORD` key in the SAME
`ccaas-infra-secret` K8s Secret that already carries the superuser's
`POSTGRES_PASSWORD` (see `deploy/k8s/infra-secret.example.yaml`) — every
service's `runtime_role.go` reads it from the same env var to provision
the role with a matching password, and every `deployment.yaml` composes
`RUNTIME_POSTGRES_DSN` from it the same `$(VAR)`-interpolation way
`POSTGRES_DSN` is already composed. This is a deliberate, documented
dev/POC-scale simplification, consistent with this repo's existing
`service-credential.example.yaml` precedent for `SERVICE_SHARED_SECRET`
— see `GAPS.md`'s "Security & auth" section for the corresponding
not-yet-closed gap entry.

Table ownership itself is unchanged by this fix — same map as before,
now with each table's runtime-role grant noted:

| Service | Owns (via its own migrations) |
|---|---|
| **Task Router** | `task_router_queues`, `task_router_statuses`, `task_router_attributes`, `task_router_dispositions`, `task_router_queue_dispositions` (the last two added 2026-09-14 for the Wrap Up / Disposition lifecycle's Disposition registry) (`services/task-router/internal/pgconfig/migrations/`), plus its own `task_router_schema_migrations` tracking table. All RLS-protected (`tenant_isolation` policy, `FORCE ROW LEVEL SECURITY`). `ccaas_app` (the runtime role) granted `SELECT, INSERT, UPDATE, DELETE` on all six by `internal/pgconfig/runtime_role.go`, run from `Migrate()`. |
| **Tenant & Identity Management** | `tenants`, `tenant_identity_users`, the signing-keypair table (`services/tenant-identity/internal/pgstore/migrations/`), plus its own `tenant_identity_schema_migrations` tracking table. Only `tenant_identity_users` is RLS-protected — `tenants`/the signing-keypair table have no `tenant_id` column to scope by. `ccaas_app` granted `SELECT, INSERT, UPDATE, DELETE` on all four by `internal/pgstore/runtime_role.go`, run from `Migrate()` (same grant regardless of RLS, for connection-identity consistency — see §5.0). |
| **Background Worker Pool** | `background_jobs` and `background_worker_pool_wrapup_targets` (`services/background-worker-pool/internal/pgstore/migrations/`) — see below for the migration-ownership decision and the new table. Tracking table `background_worker_pool_schema_migrations` (own migration runner, `pg_advisory_xact_lock`-guarded like Historical Reporting's, same unique-per-service-name pattern). No RLS on either table — same system-level-table reasoning as `historical_events` below. `ccaas_app` granted `SELECT, INSERT, UPDATE, DELETE` on all three tables by `internal/pgstore/runtime_role.go`, plus `USAGE, SELECT` on `background_jobs_id_seq` (its `BIGSERIAL` primary key's implicit sequence — INSERT relying on the column default needs sequence privilege independently of the table grant). |
| **Historical Reporting** | `historical_events` (`services/historical-reporting/internal/pgstore/migrations/`) — the single generic ingestion table for the full NATS event catalog (see §2's "Subscribed by Historical Reporting" above). Tracking table `historical_reporting_schema_migrations` (own migration runner, same unique-per-service-name pattern as every other service's). No RLS on `historical_events` — like `background_jobs` above, it's a system-level ingestion table with no in-service tenant-scoped query path yet, not a per-tenant CRUD resource; see `internal/pgstore`'s package doc comment. `ccaas_app` granted `SELECT, INSERT, UPDATE, DELETE` on both tables by `internal/pgstore/runtime_role.go`. |

No service queries another service's tables directly. Cross-service data
needs go through that service's gRPC API or a published event — never a
shared-table shortcut.

**Migration-ownership decision (Background Worker Pool, 2026-09-14):**
`pkg/pgqueue/migrations/001_background_jobs.sql` existed before this
milestone but nothing embedded or ran it — no service owned the table it
describes. Go's `go:embed` cannot reach across a package boundary (a
service's `migrate.go` cannot embed a file living under
`pkg/pgqueue/migrations`), and this repo's established convention (every
other service embeds and runs its own `migrations/*.sql`, per CLAUDE.md
Rule 3) rules out a shared embedded FS anyway. Background Worker Pool
therefore owns its own copy at
`services/background-worker-pool/internal/pgstore/migrations/001_background_jobs.sql`
(same columns/index, `IF NOT EXISTS`-guarded — see that file's own doc
comment for why: `pkg/pgqueue`'s own package-level unit tests also
provision this same real table name for pkg-level Enqueue/ClaimJobs/
MarkDone/MarkFailed tests, so both this migration and that test file
guard with `IF NOT EXISTS` to stay idempotent against each other
regardless of `go test ./...`'s run order against the shared
docker-compose Postgres instance). `pkg/pgqueue/migrations/001_background_jobs.sql`
itself is left in place as reference documentation of the shape the
`pkg/pgqueue` Go code assumes — nothing embeds or runs it directly
anymore; see that file's own updated doc comment.

---

*Update this file in the same change that adds/removes a service,
endpoint, event, or gateway. If you're reading this and a section looks
stale, verify against the actual code before extending it — see
`CLAUDE.md` Rule 1.*
