# ProjectPoutine — Progress & To-Do

**Purpose:** a running, cross-session status doc for this build. Update it at
the end of each work session (or ask Claude to). This is the source of truth
for "what's done, what's next" — more durable than chat history.

**Last updated:** 2026-09-14

---

## 1. Where things stand

### Services with real domain logic (3 of 9)

| Service | Status | Notes |
|---|---|---|
| **Task Router** | Done, deployed | Full domain per `TASK_ROUTER_SPECIFICATION.md` §1-6. Redis hot path (Lua scripts, atomic commits), Postgres-backed Queue/Status/Attribute registries with RLS, full event catalog to NATS JetStream. §7 gaps (transfers, skill matching, force-routing, bullseye, queue timeouts) deliberately out of scope. |
| **Agent & Presence Service** | Done, deployed | WebSocket connection registry + relay of Task Router's 4 client-facing events (reservation.created/rejected, agent.status.changed, agent.deleted). Redis pub/sub cross-replica fan-out. **Auth is a placeholder** (tenant_id/agent_id query params — insecure, marked loudly). |
| **Tenant & Identity Management** | Done, **not yet redeployed** | Tenant CRUD, bcrypt login, RBAC roles, ES256 JWT issuance. New shared `pkg/jwtauth` (Signer/Verifier) — built but **not wired into anything yet**. Keypair persisted in Postgres, generated once. |

### Stub services (6 of 9) — build/health-check only, no domain logic

Voice/SIP Media Gateway, Digital Channels Gateway, Workflow/IVR, Historical
Reporting, Background Worker Pool, API Gateway.

### Shared platform plumbing (`/pkg`)

`tenantctx` (gRPC tenant interceptor, now with exemptable methods),
`eventbus` (NATS JetStream wrapper, incl. `SubscribeEphemeral` for
per-replica fan-out), `pgtenant` (Postgres RLS helper), `pgqueue`
(database-as-a-queue), `health`, `config`, `jwtauth` (new, unused so far).

### Infrastructure

- Docker Compose: Postgres 16, Redis 7 (keyspace notifications on), NATS
  JetStream — all healthy, running locally.
- Kubernetes: Docker Desktop K8s, `ccaas-dev` namespace, all 9 services
  deployed with working gRPC health probes (`grpc_health_probe` baked into
  every image).
- **Known gap:** `tenant-identity`'s running pod is still the OLD stub
  image — the real implementation was built and committed but never
  rebuilt/redeployed. See To-Do #1.

---

## 2. To-Do (ordered, most actionable first)

### Immediate / housekeeping

1. **Redeploy `tenant-identity`** — rebuild its Docker image and roll the
   K8s deployment so the cluster runs the real implementation, not the old
   stub. (Same step already done once for `agent-presence`.)
2. **Distribute Tenant & Identity's public key** — currently a manual step
   (copy PEM from startup logs into `deploy/k8s/tenant-identity-public-key.local.yaml`,
   `kubectl apply`). Do this once tenant-identity is redeployed, so the key
   is actually available in-cluster for step 4 below.

### Real auth wiring (retires the placeholder auth debt)

3. **Wire `pkg/jwtauth` into `pkg/tenantctx`** — today the gRPC interceptor
   trusts the `x-tenant-id` metadata header with no signature check. Extend
   it to validate a real JWT (Authorization header) and derive `tenant_id`
   from the verified `tid` claim instead, per architecture doc §1.1 Layer 1/2.
4. **Retire Agent Presence's placeholder WebSocket auth** — replace the
   `?tenant_id=&agent_id=` query-param scheme with real JWT validation
   (Authorization header or a signed query param) once #3 exists.

### Next service to build (pick one — see recommendation below)

5. **API Gateway** — BFF: JWT validation (consumes #3), REST routing to
   internal gRPC services, WebSocket upgrade proxying for Agent Desktop.
   This is the first service a real external client would ever hit.
6. **Digital Channels Gateway or Voice/SIP Media Gateway** — channel
   ingestion, normalizing inbound work into Task Router's `Task`
   abstraction. Gives Task Router real inbound traffic instead of only
   synthetic test-driven tasks.
7. **Historical Reporting** — durable NATS JetStream consumer materializing
   the full event catalog into query-optimized Postgres storage. Low
   external dependencies (doesn't block on #3/#4), good candidate to build
   in parallel with auth wiring if desired.
8. **Background Worker Pool** — first real job type + `pgqueue.Poller`
   wiring (e.g. post-call wrap-up sync, webhook delivery). Also low-
   dependency, could go in parallel.

### Known deferred items (by design, not oversights)

- Task Router §7: transfers, attribute/skill-based matching, force-routing,
  bullseye routing, queue timeouts — all explicitly out of scope until a v2
  design pass (see `TASK_ROUTER_SPECIFICATION.md` §7 for the design
  questions already written up).
- Agent Presence: durable Redis presence keys with TTL heartbeats (only the
  pub/sub fan-out half of architecture doc §2.2's dependency line was
  built — no durable "who's online" registry yet).
- Tenant & Identity: full OAuth2/OIDC authorization server, MFA, password
  reset, tenant business-hours/feature-flag config, JWT key rotation.
- `ingress-nginx` + `mkcert` TLS setup for local K8s (architecture doc
  §1.4) — noted as manual/interactive setup, never scaffolded.

---

## 3. Recommended next step

**Wire real JWT auth (To-Do #3/#4) before starting a new service.**

Reasoning: three services now carry real, load-bearing placeholder-auth debt
(Agent Presence's WebSocket, and implicitly every service's blind trust of
`x-tenant-id`). Every new service built on top of `pkg/tenantctx` inherits
that same gap, and the longer it's deferred, the more call sites eventually
need to be revisited. Tenant & Identity — the service that makes this fix
possible — is already done. This is a contained, well-scoped change (one
shared package, two call sites) versus the more open-ended scope of a new
service, and it's the last piece standing between this system and an
honest "yes, tenant isolation is actually enforced end-to-end" story.

Runner-up: **Historical Reporting**, if you'd rather see a visible new
capability (a real BI/reporting consumer) before circling back to auth — it
has no dependency on the auth wiring and can be built in parallel by a
second background agent without contention.

---

## 4. How to use this doc

- At the start of a session, read this file first for full context.
- At the end of a session (or when a milestone lands), update "Where things
  stand" and re-order/prune the To-Do list — ask Claude to do this
  explicitly ("update PROGRESS.md").
- Treat §2 as the backlog; §3 as "what I'd do next if you said go."
