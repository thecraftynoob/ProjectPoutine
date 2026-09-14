# ProjectPoutine — Progress & To-Do

**Purpose:** a running, cross-session status doc for this build. Update it at
the end of each work session (or ask Claude to). This is the source of truth
for "what's done, what's next" — more durable than chat history.

**Last updated:** 2026-09-14 (JWT auth wired platform-wide)

---

## 1. Where things stand

### Services with real domain logic (3 of 9)

| Service | Status | Notes |
|---|---|---|
| **Task Router** | Done, deployed | Full domain per `TASK_ROUTER_SPECIFICATION.md` §1-6. Redis hot path (Lua scripts, atomic commits), Postgres-backed Queue/Status/Attribute registries with RLS, full event catalog to NATS JetStream. §7 gaps (transfers, skill matching, force-routing, bullseye, queue timeouts) deliberately out of scope. Now requires a real bearer JWT on every RPC. |
| **Agent & Presence Service** | Done, deployed | WebSocket connection registry + relay of Task Router's 4 client-facing events (reservation.created/rejected, agent.status.changed, agent.deleted). Redis pub/sub cross-replica fan-out. WebSocket upgrade now requires a real `Authorization: Bearer <jwt>` header (placeholder query-param auth retired). |
| **Tenant & Identity Management** | Done, deployed | Tenant CRUD, bcrypt login, RBAC roles, ES256 JWT issuance, plus a new `IssueServiceToken` RPC for service-to-service auth. `pkg/jwtauth` is now wired into every service. Keypair persisted in Postgres, generated once. |

### Stub services (6 of 9) — build/health-check only, no domain logic

Voice/SIP Media Gateway, Digital Channels Gateway, Workflow/IVR, Historical
Reporting, Background Worker Pool, API Gateway.

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
- **Known gap:** Task Router's `Agent` entity and Tenant & Identity's
  `User` entity are unrelated today — Agent Presence's WebSocket maps a
  connection to `agent_id` via the JWT's `sub` claim as a pragmatic stand-in
  (see `services/agent-presence/internal/wsserver/wsserver.go`'s doc
  comment), meaning an operator must provision an Agent and a User with the
  matching ID by convention. No automatic reconciliation exists yet.

---

## 2. To-Do (ordered, most actionable first)

### Auth follow-ups (small, but real)

1. **Formal Agent ↔ User reconciliation.** Right now Agent Presence maps a
   WebSocket connection's `agent_id` to the authenticated JWT's `sub`
   claim, which only works if an operator provisions a Task Router `Agent`
   and a Tenant & Identity `User` with the same ID by convention (see the
   "known gap" note above). A real fix likely means Task Router's `Agent`
   gaining a `user_id` reference, or an explicit link table — worth doing
   before building a real Agent Desktop client against this.
2. **Per-service identity for service-to-service auth.** `IssueServiceToken`
   today uses one shared secret for every calling service — it proves "some
   service in this cluster," not "specifically task-router." Fine for now
   (explicitly scoped as such), but a real per-service credential or mTLS
   scheme is real future work if this goes beyond home-lab scale.
3. **JWT key rotation** — no rotation scheme exists; rotating Tenant &
   Identity's signing key today would invalidate every token every service
   still expects, with no multi-key/`kid` support to roll it forward safely.

### Next service to build (pick one — see recommendation below)

4. **API Gateway** — BFF: end-user JWT validation, REST routing to internal
   gRPC services, WebSocket upgrade proxying for Agent Desktop. This is the
   first service a real external client would ever hit, and the natural
   place to originate tokens for browser-based clients that can't easily
   attach custom WebSocket headers themselves.
5. **Digital Channels Gateway or Voice/SIP Media Gateway** — channel
   ingestion, normalizing inbound work into Task Router's `Task`
   abstraction. Gives Task Router real inbound traffic instead of only
   synthetic test-driven tasks. See "Voice merge" below for the
   Voice/SIP Media Gateway option specifically.
6. **Historical Reporting** — durable NATS JetStream consumer materializing
   the full event catalog into query-optimized Postgres storage.
7. **Background Worker Pool** — first real job type + `pgqueue.Poller`
   wiring (e.g. post-call wrap-up sync, webhook delivery).

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

**Build API Gateway next.**

Reasoning: every service now genuinely enforces tenant isolation via real
JWTs — that story is done. API Gateway is the one piece standing between
this system and an actual external client being able to do anything at
all: today, every call into the platform has to originate from a
throwaway internal test client with a hand-obtained token. API Gateway is
also the natural place to solve the practical end of the auth story (an
end-user login flow, issuing/refreshing tokens for a browser client) since
it's the edge that's supposed to originate tenant context in the first
place per architecture doc §1.1 Layer 1.

Runner-up: **Voice/SIP Media Gateway**, if the friend's-PoC merge (see
below) becomes actionable before then — it would let the API Gateway
build and the voice merge proceed somewhat in parallel, since they touch
different services.

---

## 4. How to use this doc

- At the start of a session, read this file first for full context.
- At the end of a session (or when a milestone lands), update "Where things
  stand" and re-order/prune the To-Do list — ask Claude to do this
  explicitly ("update PROGRESS.md").
- Treat §2 as the backlog; §3 as "what I'd do next if you said go."
