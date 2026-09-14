# ProjectPoutine — Progress & To-Do

**Purpose:** a running, cross-session status doc for this build. Update it at
the end of each work session (or ask Claude to). This is the source of truth
for "what's done, what's next" — more durable than chat history.

**Last updated:** 2026-09-14 (Workflow/IVR Engine's build explicitly
discussed and deferred — see To-Do #11 and §3's Recommended next step;
no code changed. Previous entries, same day: Background Worker Pool's
first real milestone built — see "Background Worker Pool" row below and
`ARCHITECTURE_FLOW.md` §2's "Subscribed by Background Worker Pool"
subsection. This closes out the "3 stub services" count to 2 — see
"Where things stand" below. Earlier, same day: Historical Reporting's
first real milestone built — see "Historical Reporting" row below and
`ARCHITECTURE_FLOW.md` §2's "Subscribed by Historical Reporting"
subsection. Earlier, same day: Digital Channels Gateway's first real
milestone built — see `ARCHITECTURE_FLOW.md` §4.2. Earlier:
full-repo review + cleanup pass — see "Cleanup pass" note below; Task
Router's `Agent` gained an optional `user_id` field; API Gateway built —
REST routing, JWT validation, WebSocket ticket + proxying)

**Cleanup pass (2026-09-13):** A full independent audit of every service,
`/pkg`, K8s manifests, and all four living docs found no functional bugs
and reconfirmed all 17 previously-fixed gotchas are still fixed. Fixed
one real, if latent, Rule 3 violation: `task-router` and `tenant-identity`
were both creating and sharing a single unnamespaced `schema_migrations`
bookkeeping table in the shared Postgres instance — harmless only because
their migration filenames hadn't yet collided. Split into
`task_router_schema_migrations` / `tenant_identity_schema_migrations`
(each service's own migrate.go). Also: fixed a doc-drift event name in
this file's own event catalog (`agent.capacity_config.updated` →
`agent.capacity.config.updated`, matching `events.go`), corrected
README.md's stale "3 of 9 services have real logic" status and its
K8s walkthrough (it omitted the now-required tenant-identity-public-key
and service-credential apply steps), fixed a stale date in CLAUDE.md, a
stale Lua-script filename reference, a stale test-file comment, and a
minor redundant JSON re-marshal in Agent Presence's fanout delivery path.
No behavior changes to any RPC, event, or auth flow.

---

## 1. Where things stand

### Services with real domain logic (7 of 9)

| Service | Status | Notes |
|---|---|---|
| **Task Router** | Done, deployed | Full domain per `TASK_ROUTER_SPECIFICATION.md` §1-6, extended (2026-09-14, beyond what that frozen spec snapshot describes) with a Wrap Up / Disposition two-step task-completion lifecycle: a new `WrapUp` task status between `Active` and `Completed`, `EndTask`/`SetTaskDisposition` RPCs, an optional per-task wrap-up timer (Redis TTL + keyspace notifications, same mechanism as reservation expiry), and a new Postgres-backed Disposition registry (+ queue association) alongside Queue/Status/Attribute. Redis hot path (Lua scripts, atomic commits), Postgres-backed admin registries with RLS, full event catalog to NATS JetStream (now including `task.ended`, `task.disposition.set`, `agent.wrapup.timed_out`). §7 gaps (transfers, skill matching, force-routing, bullseye, queue timeouts) deliberately out of scope. Requires a real bearer JWT on every RPC. Now also fronted by API Gateway's REST surface (see below) — RPCs unchanged, only `google.api.http` annotations added. See `ARCHITECTURE_FLOW.md` §2's lifecycle writeup and `GAPS.md` for this addition's own scope decisions. |
| **Agent & Presence Service** | Done, deployed | WebSocket connection registry + relay of Task Router's 4 client-facing events (reservation.created/rejected, agent.status.changed, agent.deleted). Redis pub/sub cross-replica fan-out. WebSocket upgrade accepts EITHER a real `Authorization: Bearer <jwt>` header (original path) OR a `?ticket=` query parameter (new — short-lived ticket minted by API Gateway, for browser clients that can't set custom WebSocket headers). Both paths share one claims-resolution helper. |
| **Tenant & Identity Management** | Done, deployed | Tenant CRUD, bcrypt login, RBAC roles, ES256 JWT issuance, plus a new `IssueServiceToken` RPC for service-to-service auth. `pkg/jwtauth` is now wired into every service. Keypair persisted in Postgres, generated once. Now also fronted by API Gateway's REST surface. |
| **API Gateway** | Done, deployed | Single external entry point: REST routing (via `grpc-gateway`, `google.api.http` annotations added directly to Task Router's and Tenant & Identity's existing RPCs) fronting both services, end-user JWT validation (Layer 1, forwarded to backends for Layer 2 re-verification), and WebSocket upgrade proxying to Agent Presence for browser Agent Desktop clients via a short-lived ws-ticket mechanism (API Gateway holds its own dedicated, in-memory-only signing keypair for tickets — separate from Tenant & Identity's session key). Full REST route table: `ARCHITECTURE_FLOW.md` §1.1. TLS termination, rate limiting, quota enforcement, feature-flagging, and `ingress-nginx` external exposure remain explicitly deferred (architecture doc §1.1/§1.4) — this pass is JWT validation + routing + WS proxying only. |
| **Digital Channels Gateway** | First real milestone done | Inbound-only: `POST /webhooks/chat/{tenant_id}` (direct, NOT via API Gateway — a different trust boundary, see `ARCHITECTURE_FLOW.md` §4.2) accepts one generic chat message shape and calls Task Router's `EnqueueTask` as a service via `pkg/svcauth`, ending at a created Task. No outbound/agent-reply delivery, no Postgres persistence of messages (explicitly deferred to future async workers per architecture doc §2.2), no webhook signature verification (explicitly deferred, documented tradeoff — see `internal/webhookapi`'s `ServeHTTP` doc comment), no real per-provider integration. The scaffold's gRPC health server is unchanged and still runs alongside. |
| **Historical Reporting** | First real milestone done | Ingestion only: one durable, fixed-name JetStream consumer (`historical-reporting-ingest`, `internal/eventconsumer`) subscribes to Task Router's FULL event catalog (all three domains — task/agent/reservation) via a single wildcard `FilterSubject` (`tenant.*.>`), and materializes every event into one generic Postgres table, `historical_events` (`internal/pgstore`) — `event_id`, `tenant_id`, `domain`, `event_type`, `subject`, `payload` JSONB, `received_at`. No read/query API, no new RPC, no REST route this pass — verification is direct SQL, proven via a live smoke test including a stop/publish-while-down/restart cycle confirming the durable consumer resumes and catches up on missed events rather than dropping them. Known, documented gap: `event_id` is generated fresh at ingestion (Task Router's published payloads carry no stable app-level event ID to reuse), so at-least-once JetStream delivery + a crash between insert and ack can double-insert a redelivered message — accepted milestone-scope gap, not silently swallowed. See `ARCHITECTURE_FLOW.md` §2's "Subscribed by Historical Reporting" subsection for the full design writeup. |
| **Background Worker Pool** | First real milestone done | End-to-end pipeline: a durable, fixed-name JetStream consumer (`background-worker-pool-wrapup`, `internal/wrapupsync.Consumer`) subscribes ONLY to `tenant.*.task.completed` (not Historical Reporting's full-catalog wildcard), enqueuing a `wrapup_sync` row into `background_jobs` (the Database-as-a-Queue table from `pkg/pgqueue`, now actually run/owned for the first time — see migration-ownership note below). A `pkg/pgqueue.Poller` claims pending jobs and dispatches to `internal/wrapupsync.Handler`, which looks up a tenant's configured wrap-up target URL (new `background_worker_pool_wrapup_targets` table — GAPS.md's pre-existing "Instance: Background Worker Pool's wrap-up sync target URL" entry) and does a real `net/http` POST of `{taskId, agentId, tenantId}` as JSON, marking the job `'done'` on 2xx or scheduling a backed-off retry (`run_after = now + attempts*30s`, capped at 5 min, up to `maxAttempts = 5` before permanent `'failed'` — both documented in GAPS.md as real scope decisions). New generic `pkg/pgqueue.MarkDone`/`MarkFailed` helpers added (Poller's own doc comment explicitly left the terminal-status transition to the caller). **Migration-ownership decision:** `pkg/pgqueue/migrations/001_background_jobs.sql` existed but nothing ran it — Go's `go:embed` can't cross a package boundary, so Background Worker Pool now owns its own copy under `services/background-worker-pool/internal/pgstore/migrations/`, matching every other service's own-migrations convention (CLAUDE.md Rule 3); the `pkg/pgqueue` copy is left as reference documentation only. Verified via a full live smoke test (real tenant/agent/task through `EnqueueTask`→match→`AcceptReservation`→`CompleteTask`, confirming a real HTTP POST lands on a local test server and the job reaches `'done'` in Postgres) covering both the success path and a genuinely-unreachable-URL failure path (confirmed retry-with-backoff engages). See `ARCHITECTURE_FLOW.md` §2's "Subscribed by Background Worker Pool" subsection and §5's updated table-ownership row for the full design writeup. |

### Stub services (2 of 9) — build/health-check only, no domain logic

Voice/SIP Media Gateway, Workflow/IVR.

### Shared platform plumbing (`/pkg`)

`tenantctx` (gRPC interceptor — now verifies a real bearer JWT and derives
`tenant_id` from its `tid` claim; `x-tenant-id` metadata is logging-only,
never trusted), `eventbus` (NATS JetStream wrapper, incl.
`SubscribeEphemeral` for per-replica fan-out), `pgtenant` (Postgres RLS
helper), `pgqueue` (database-as-a-queue), `health`, `config`, `jwtauth`
(Signer/Verifier + client-side token injection/caching, now in active use),
`svcauth` (new — wraps the mint/cache/refresh/attach cycle for a service
calling another service's gRPC API on its own behalf).

### Infrastructure

- Docker Compose: Postgres 16, Redis 7 (keyspace notifications on), NATS
  JetStream — all healthy, running locally.
- Kubernetes: Docker Desktop K8s, `ccaas-dev` namespace, all 9 services
  deployed with working gRPC health probes (`grpc_health_probe` baked into
  every image). All 9 pods healthy, zero restarts, as of last check.
- **Auth is now real, not a placeholder**, end-to-end: every gRPC call
  requires a valid bearer JWT (verified against Tenant & Identity's
  signing public key, distributed via a K8s ConfigMap every service
  mounts); Agent Presence's WebSocket requires the same. Service-to-service
  calls authenticate via `IssueServiceToken` + a shared credential — see
  `deploy/k8s/service-credential.example.yaml` for the explicit scoping
  (a narrow stand-in for real per-service identity, not mTLS/zero-trust).
- **Known gap (narrowed, not closed):** Task Router's `Agent` entity now
  carries an optional `user_id` field linking it to a Tenant & Identity
  `User`, but it's purely informational — Agent Presence's WebSocket
  still maps a connection to `agent_id` via the JWT's `sub` claim as a
  pragmatic stand-in (see
  `services/agent-presence/internal/wsserver/wsserver.go`'s doc comment),
  meaning an operator must still provision an Agent and a User with the
  matching ID by convention. No automatic reconciliation/validation
  exists yet — see To-Do #1.

---

## 2. To-Do (ordered, most actionable first)

### Auth follow-ups (small, but real)

1. **Formal Agent ↔ User reconciliation — partially addressed.** Task
   Router's `Agent` (and `CreateAgentRequest`) now carries an optional
   `user_id` field (`proto/task-router/v1/task_router.proto`,
   2026-09-13) recording which Tenant & Identity `User` operates it. This
   is purely additive and purely informational today: it is not
   validated against Tenant & Identity at creation time (no cross-service
   call), and Agent Presence's WebSocket mapping still uses the JWT
   `sub` claim directly as `agent_id` rather than looking this field up
   (see `services/agent-presence/internal/wsserver/wsserver.go`'s doc
   comment) — an operator must still provision matching IDs by
   convention for WebSocket delivery to reach the right connection.
   What's still open: (a) validating `user_id` against Tenant & Identity
   on `CreateAgent`, (b) switching Agent Presence's mapping to a real
   lookup instead of the `sub`-as-`agent_id` shortcut, (c) deciding what
   happens when the two disagree. Worth finishing before building a real
   Agent Desktop client against this.
2. **Per-service identity for service-to-service auth.** `IssueServiceToken`
   today uses one shared secret for every calling service — it proves "some
   service in this cluster," not "specifically task-router." Fine for now
   (explicitly scoped as such), but a real per-service credential or mTLS
   scheme is real future work if this goes beyond home-lab scale.
3. **JWT key rotation** — no rotation scheme exists; rotating Tenant &
   Identity's signing key today would invalidate every token every service
   still expects, with no multi-key/`kid` support to roll it forward safely.

### Digital Channels Gateway follow-ups (small, but real)

4. **Webhook signature/secret verification.** `POST
   /webhooks/chat/{tenant_id}` currently has none — an explicit,
   documented scope decision for the first milestone (no real provider
   account exists yet to verify against), not an oversight. See
   `internal/webhookapi`'s `ServeHTTP` doc comment and
   `ARCHITECTURE_FLOW.md` §4.2. Needed before this endpoint faces a real,
   untrusted, public-internet channel provider — today anyone who
   discovers/guesses a `tenant_id` can enqueue tasks into that tenant's
   queues.
5. **Outbound/agent-reply delivery.** This milestone is inbound-only;
   nothing yet delivers an agent's reply back to the originating channel.
6. **Message/thread persistence.** Explicitly deferred to a future async-
   worker milestone per architecture doc §2.2 — nothing in this service
   writes to Postgres today, `session_id` travels through unused beyond
   request validation.
7. **Real per-provider integrations** (Twilio, etc.) instead of the one
   generic "chat" webhook shape this milestone defines.
8. **Bound the per-tenant token-source cache.** `TenantScopedTaskRouterClient`
   (`services/digital-channels-gateway/internal/webhookapi/taskrouterclient.go`)
   caches one `jwtauth.TokenSource` per tenant_id ever seen, with no
   eviction — flagged as acceptable for this milestone's small,
   operator-provisioned tenant set, but worth an LRU/TTL policy before
   fielding many tenants.

### Historical Reporting follow-ups (small, but real)

9a. **No read/query API yet.** This milestone is ingestion-only by
    explicit scope — `historical_events` has no RPC, no REST route; the
    only way to read it today is direct SQL. A future milestone should
    add a query surface (likely a new gRPC service + REST routes via API
    Gateway) once real reporting requirements (dashboards, SLA rollups,
    compliance exports) are clearer.
9b. **No idempotency/deduplication.** `event_id` is generated fresh at
    ingestion time; a crash between insert and ack can double-insert a
    redelivered message. See `internal/eventconsumer`'s package doc
    comment and `ARCHITECTURE_FLOW.md` §2 for the full tradeoff. Needs
    either a stable event ID added on Task Router's publish side (a
    cross-service change, out of scope for this milestone) or a dedupe
    key derived from the JetStream message sequence number.
9c. **No partitioning, no ClickHouse.** `historical_events` is a single
    unpartitioned table — correct scope for this first milestone per the
    architecture doc, but won't scale indefinitely; partitioned tables
    and/or a ClickHouse migration are the documented future upgrade path
    (architecture doc §2.2/§3.1).

### Background Worker Pool follow-ups (small, but real)

10a. **`wrapup_sync` is the only job_type implemented.** `pkg/pgqueue`'s
     `background_jobs` table and `internal/wrapupsync.Handler`'s
     dispatch-by-`job_type` structure both support more (e.g.
     `webhook_delivery`, `billing_rollup` — see `background_jobs`'
     column comment), but only `wrapup_sync` has a real handler this
     milestone; any other `job_type` is logged and marked `'failed'`
     rather than crashing the poller.
10b. **No idempotency/deduplication on the NATS side.** Same root cause
     as Historical Reporting's #9b above (no stable event ID on Task
     Router's published payloads) — a crash between `pgqueue.Enqueue`
     succeeding and the message being acked can double-enqueue a
     `wrapup_sync` job. See GAPS.md.
10c. **Fixed linear backoff + hard max-attempts cutoff, not exponential-
     with-jitter or unlimited retries.** `run_after = now +
     attempts*30s`, capped at 5 minutes, `maxAttempts = 5` before
     permanent `'failed'` — real, documented scope decisions (GAPS.md),
     not unstated defaults. No alerting/dead-letter handling exists for
     a permanently-failed job today.
10d. **No real tenant-settings API for `background_worker_pool_wrapup_targets`.**
     Direct SQL insert only — see GAPS.md's pre-existing entry for this
     table.

### Next service to build (pick one — see recommendation below)

9. **Voice/SIP Media Gateway** — the other channel-ingestion path Digital
   Channels Gateway's build didn't cover. See "Voice merge" below for its
   own open prerequisites.
11. **Workflow/IVR Engine — deliberately NOT started (2026-09-14).**
    Discussed and explicitly deferred, not forgotten: the architecture
    doc frames this service primarily around voice/DTMF call flows
    (`Workflow / IVR Engine` row, architecture doc §2.2) — "plays
    prompts, collects DTMF or NLU intents... before a Task is hard-queued
    to the Router" — and its stated dependency is Voice/SIP Media
    Gateway's playback/DTMF control gRPC, which doesn't exist yet (blocked
    on the Voice merge, see below). A channel-agnostic first slice (run
    the state-machine core against Digital Channels Gateway's chat path
    instead of voice) was considered and rejected — it would produce
    something needing substantial rework once Voice/SIP Media Gateway
    lands, rather than something that extends cleanly. Correct order:
    resolve the Voice merge's open prerequisites first, then build
    Voice/SIP Media Gateway, then Workflow/IVR against a real
    playback/DTMF contract.

### API Gateway follow-ups (small, but real — see `ARCHITECTURE_FLOW.md` §4.1 for the flow these refer to)

12. **True single-use ws-ticket protection**, if a future milestone judges
    the current short-TTL-only scoping insufficient (e.g. once WebSocket
    traffic carries something more sensitive than task/presence
    notifications) — would need a shared "used tickets" registry (Redis,
    likely, given Agent Presence already depends on it) rather than the
    current pure-JWT-TTL approach.
13. **ws-ticket key durability across API Gateway restarts.** The
    ticket-signing keypair is regenerated fresh (never persisted) on every
    API Gateway startup, which means the `api-gateway-ws-ticket-public-key`
    ConfigMap goes stale on every restart and needs a manual re-`kubectl
    apply` — fine for a single-replica dev deployment, but worth revisiting
    (e.g. Postgres-backed persistence like Tenant & Identity's own signing
    key) before a multi-replica or production deployment.
14. **External exposure via `ingress-nginx`.** API Gateway's `Service` is
    `ClusterIP` today (like every other service) — real external
    reachability needs the `ingress-nginx` + `mkcert` TLS setup
    architecture doc §1.4 already flags as deferred, manual/interactive
    local-cluster tooling.

### Voice merge (friend's ESXi/k3s PoC → `voice-media-gateway`)

**Status: investigation only, blocked on answers from friend — not started.**

Friend has a working FreeSWITCH-based voice/STT PoC running on a separate
k3s cluster (3-node, ESXi-hosted: `poutine-poc-master`/`poc-1`/`poc-2`,
documented in [`poutine-poc-architecture.md`](./poutine-poc-architecture.md)).
Goal: merge his voice code
into this repo as the real implementation of the already-scaffolded
`voice-media-gateway` service, sharing one Git repo with two different
local dev setups (this repo's Docker Desktop K8s vs. his ESXi k3s).

**Compatible:**
- Both are Kubernetes — manifests are portable in principle.
- FreeSWITCH is already the architecture doc's chosen voice technology
  (§2.2) — no technology mismatch on the voice/SIP layer itself.
- Postgres + Redis present on both sides (minor version drift: his
  `postgres:17-alpine` vs. this repo's `postgres:16` — trivial to align).

**Incompatible / needs resolving before merge, not after:**
1. **Event bus: his PoC uses Kafka (KRaft mode); this repo uses NATS
   JetStream.** Everything here — tenant-scoped subjects, the event
   catalog, `pkg/eventbus`, Agent Presence's relay, the planned Historical
   Reporting consumer — is built on JetStream semantics. Recommended
   direction: standardize on NATS JetStream and have his side drop Kafka,
   since his own doc states Kafka was chosen only as "the intended
   end-state technology," not because anything downstream is already
   built against its wire protocol. **Open question for friend: is he
   actually willing to swap Kafka → NATS?**
2. **No tenant_id concept in his pipeline.** Every table and every event
   subject in this system is tenant-scoped (architecture doc §1.1). His
   voice/STT code needs tenant_id threaded through before it can plug into
   the rest of the platform — this can't be bridged around, it has to be
   retrofitted into his code.
3. **His language/framework is unconfirmed** (not Go, per last check —
   likely Python for an STT pipeline, but not verified). If non-Go, it
   joins the monorepo as its own non-Go module (its own Dockerfile/build,
   not `go.mod`) and can't use this repo's Go `/pkg` helpers
   (`tenantctx`, `eventbus`, `pgtenant`) directly — needs either Python
   equivalents or a thinner contract-only boundary (gRPC + NATS pub/sub,
   no shared library code).

**Next action:** confirm with friend (a) Kafka→NATS flexibility, (b)
actual language/framework, (c) feasibility of adding tenant_id to his
existing schema/pipeline — before any code merge work starts.

### Known deferred items (by design, not oversights)

- Digital Channels Gateway: webhook signature/secret verification,
  outbound/agent-reply delivery, message/thread Postgres persistence, and
  real per-provider integrations — all explicitly out of scope for the
  first milestone (architecture doc §2.2 defers persistence specifically
  to future async workers). See To-Do #4-#7 above.
- Task Router §7: transfers, attribute/skill-based matching, force-routing,
  bullseye routing, queue timeouts — all explicitly out of scope until a v2
  design pass (see `TASK_ROUTER_SPECIFICATION.md` §7 for the design
  questions already written up).
- Agent Presence: durable Redis presence keys with TTL heartbeats (only the
  pub/sub fan-out half of architecture doc §2.2's dependency line was
  built — no durable "who's online" registry yet).
- Tenant & Identity: full OAuth2/OIDC authorization server, MFA, password
  reset, tenant business-hours/feature-flag config.
- Real mTLS/zero-trust service mesh (per-service identity, transport
  encryption) — today's service-to-service auth is a shared-credential
  stand-in; see To-Do #2. No gRPC connection in this repo uses transport
  TLS yet (`insecure.NewCredentials()` throughout) — the bearer token is
  the only security boundary so far, not the transport itself.
- `ingress-nginx` + `mkcert` TLS setup for local K8s (architecture doc
  §1.4) — noted as manual/interactive setup, never scaffolded.

---

## 3. Recommended next step

**Resolve the Voice merge's open prerequisites, or harden Digital
Channels Gateway's webhook (signature verification, outbound delivery)
in the meantime.**

Reasoning: Digital Channels Gateway's, Historical Reporting's, and
Background Worker Pool's first milestones are all done now — Task Router
receives real inbound traffic (one generic chat webhook shape) instead of
only synthetic, test-driven tasks, every event it publishes is durably
materialized for future reporting, and every completed task now
genuinely triggers a real outbound wrap-up sync via the
Database-as-a-Queue pipeline. Only Voice/SIP Media Gateway and
Workflow/IVR remain stub services, and **both are now effectively gated
on the same thing**: Workflow/IVR's build was explicitly discussed and
deferred (2026-09-14, To-Do #11) because its real spec depends on
Voice/SIP Media Gateway's playback/DTMF contract, which doesn't exist —
and Voice/SIP Media Gateway itself is blocked on the Voice merge's three
open questions (Kafka→NATS, tenant_id retrofit, language/framework
confirmation — see "Voice merge" below). So the actual highest-leverage
next step is getting those three answers from the friend, not more
platform code. In the meantime, closing Digital Channels Gateway's own
explicitly-deferred gaps (webhook signature verification is the
highest-priority one — see To-Do #4 — since the endpoint is genuinely
open/unauthenticated today) is real, unblocked work.

---

## 4. How to use this doc

- At the start of a session, read this file first for full context.
- At the end of a session (or when a milestone lands), update "Where things
  stand" and re-order/prune the To-Do list — ask Claude to do this
  explicitly ("update PROGRESS.md").
- Treat §2 as the backlog; §3 as "what I'd do next if you said go."
