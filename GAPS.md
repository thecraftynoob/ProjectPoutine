# ProjectPoutine — Known Gaps & POC-Stage Stand-Ins

**Purpose:** a single place that names every deliberate shortcut, stand-in,
or simplification taken to keep this a buildable proof-of-concept, together
with what a real (beyond-POC) version would need instead. This is NOT a
bug list — everything here is a conscious, documented scope decision made
at the time, usually to avoid building infrastructure or a subsystem
(a settings UI, a real provider integration, a key-rotation scheme) that
nothing yet depends on. `PROGRESS.md` tracks *what's built and what's
next*; this file tracks *what's fake, narrow, or simplified in what's
already built*, so that decision is visible and revisitable rather than
buried in a doc comment nobody re-reads.

**How to use this doc:**
- Before starting a new milestone that touches an area listed here, check
  whether this milestone is the right moment to also close that gap
  (sometimes yes — e.g. building Digital Channels Gateway's outbound path
  is the natural moment to also add webhook signature verification).
- When a new milestone introduces a fresh stand-in/simplification
  (a fake secret, a single-tenant hardcoded value, a missing validation,
  an infrastructure piece deliberately not built), add it here in the same
  change — mirrors `CLAUDE.md` Rule 1's "update the living doc in the same
  change" discipline for `ARCHITECTURE_FLOW.md`.
- When a gap is actually closed, move its entry to "Closed gaps" at the
  bottom with the date and a one-line pointer to what replaced it, rather
  than deleting it — the history of what used to be a stand-in is useful
  context for why something is shaped the way it is.

---

## Security & auth

- **Service-to-service auth is one shared secret, not per-service
  identity.** `IssueServiceToken` (Tenant & Identity) proves "some
  service in this cluster," not "specifically task-router" — any service
  (or anyone holding `SERVICE_SHARED_SECRET`) can mint a token claiming to
  be any `callerService`. See `deploy/k8s/service-credential.example.yaml`
  and `pkg/svcauth`'s doc comment. **Real version needs:** per-service
  credentials or mTLS/zero-trust mesh identity. Tracked: `PROGRESS.md`
  To-Do #2.
- **No transport-level TLS anywhere.** Every gRPC connection in this repo
  uses `insecure.NewCredentials()` — the bearer token is the only security
  boundary, not the transport itself. **Real version needs:** mTLS or at
  minimum TLS termination between services, not just at the edge.
- **No JWT key rotation.** Tenant & Identity's signing keypair is
  generated once and persisted; there's no multi-key/`kid` support, so
  rotating it today would invalidate every outstanding token every
  service expects. **Real version needs:** `kid`-based key rotation with
  an overlap window. Tracked: `PROGRESS.md` To-Do #3.
- **Digital Channels Gateway's webhook has no signature/secret
  verification.** `POST /webhooks/chat/{tenant_id}` trusts the URL's
  `tenant_id` alone — anyone who discovers or guesses one can enqueue
  tasks into that tenant's queues. See
  `services/digital-channels-gateway/internal/webhookapi`'s `ServeHTTP`
  doc comment. **Real version needs:** per-tenant HMAC secret (or
  provider-native signature scheme, e.g. Twilio's) checked before
  processing. Tracked: `PROGRESS.md` To-Do #4.
- **API Gateway's ws-ticket is short-TTL, not single-use.** A leaked
  ticket is repayable within its ~45s window; no server-side used-ticket
  registry exists. See `services/api-gateway/internal/wsticket`'s package
  doc comment. **Real version needs:** a Redis-backed used-ticket set if
  WebSocket traffic ever carries something more sensitive than
  task/presence notifications. Tracked: `PROGRESS.md` To-Do #12.
- **ws-ticket signing key is not persisted.** Regenerated fresh every API
  Gateway process start; the distribution ConfigMap goes stale on every
  restart and needs a manual re-`kubectl apply`. **Real version needs:**
  Postgres-backed persistence, mirroring Tenant & Identity's own signing
  key. Tracked: `PROGRESS.md` To-Do #13.
- **The Postgres runtime role (`ccaas_app`) uses one fixed, shared
  password across every service**, provisioned the same way
  `POSTGRES_PASSWORD` (the superuser's) already is — a new
  `POSTGRES_RUNTIME_PASSWORD` key in the same `ccaas-infra-secret` K8s
  Secret, read identically by every service's
  `internal/pg{store,config}/runtime_role.go` (see
  `ARCHITECTURE_FLOW.md` §5.0). This is the same class of tradeoff as
  `SERVICE_SHARED_SECRET` above: "one service in this cluster" identity,
  not "specifically task-router," and no rotation story. Unlike
  `SERVICE_SHARED_SECRET` (which authenticates a service AS a caller of
  one specific RPC), a leaked `ccaas_app` password grants direct
  `SELECT/INSERT/UPDATE/DELETE` on every RLS-protected table's rows the
  role's grants cover, gated only by whatever `app.current_tenant` the
  holder's own queries choose to set — i.e. RLS still applies (this role
  is genuinely `NOBYPASSRLS`, unlike the superuser it replaces), but
  there is no per-service boundary preventing, say, a compromised
  Historical Reporting process from opening a connection as `ccaas_app`
  and querying Tenant & Identity's tables directly (nothing in Postgres
  itself stops it — CLAUDE.md Rule 3's per-service table boundary is
  enforced by convention/code review today, not by distinct Postgres
  roles per service). **Real version needs:** either a distinct Postgres
  role per service (each granted only its own tables, closing that gap
  at the database layer) or a real per-service credential/identity
  mechanism (mirroring whatever eventually replaces
  `SERVICE_SHARED_SECRET`, e.g. SPIFFE/SPIRE-issued short-lived Postgres
  credentials) rather than one long-lived shared password.

## Identity & data model

- **Task Router's `Agent` and Tenant & Identity's `User` are only
  informally linked.** `Agent.user_id` (added 2026-09-13) is purely
  informational — nothing validates it, and Agent Presence's WebSocket
  still maps a connection to `agent_id` via the JWT's `sub` claim as a
  convention-based shortcut, not a real lookup. See
  `services/agent-presence/internal/wsserver/wsserver.go`'s doc comment.
  **Real version needs:** validate `user_id` against Tenant & Identity at
  `CreateAgent` time, and switch the WebSocket mapping to a real lookup.
  Tracked: `PROGRESS.md` To-Do #1.
- **No idempotency key on published domain events.** Task Router's
  `structpb` event payloads carry no stable application-level event ID,
  so Historical Reporting's at-least-once JetStream consumer can
  double-insert a row if it crashes between insert and ack. See
  `services/historical-reporting/internal/eventconsumer`'s package doc
  comment. **Real version needs:** either a stable event ID added on the
  publish side, or a dedupe key derived from the JetStream message
  sequence number. Tracked: `PROGRESS.md` To-Do #9b. Same underlying gap
  now also affects Background Worker Pool's `internal/wrapupsync.Consumer`
  (2026-09-14): a crash between `pgqueue.Enqueue` succeeding and the
  JetStream message being acked can enqueue a second, duplicate
  `wrapup_sync` job for one `task.completed` event — not a new gap this
  consumer invents, the same root cause surfacing in a second durable
  consumer.

## Infrastructure stand-ins

- **No real per-tenant settings/config system exists anywhere.** Every
  place a milestone has needed a tenant-configurable value (a webhook
  target URL, a CRM sync endpoint) has had to invent a narrow, ad-hoc
  stand-in rather than reading from a real settings subsystem, because no
  such subsystem has been built yet. Each instance is listed individually
  below as it's introduced; this entry exists so the *pattern* (not just
  each individual instance) is visible. **Real version needs:** a genuine
  tenant configuration store (architecture doc's own Tenant & Identity row
  already scopes "CRUD for tenant configuration... business hours,
  feature flags" as that service's job — this hasn't been built yet).
  - **Instance: Background Worker Pool's wrap-up sync target URL**
    (2026-09-14). A minimal `tenant_id -> wrapup_url` table, owned by
    Background Worker Pool itself, stands in for real tenant-configurable
    CRM-sync settings. No admin UI/API to manage it exists — populated
    directly for testing. **Real version needs:** this config living in
    an actual tenant-settings subsystem (likely Tenant & Identity, per
    the architecture doc's own scoping), reachable via a real
    CRUD API, not a service-owned table nobody else can write to.
- **Background Worker Pool's retry backoff is a fixed linear formula
  with a hard max-attempts cutoff, not exponential-with-jitter or
  unlimited retries** (2026-09-14). `internal/wrapupsync.Handler`
  schedules a failed `wrapup_sync` job's retry at
  `run_after = now + attempts*30s`, capped at 5 minutes, and gives up
  (marks the job permanently `'failed'`) after `maxAttempts = 5` —
  both are real, deliberate scope decisions for this milestone, not
  unstated defaults or oversights. A tenant with no configured wrap-up
  target URL, or a job whose payload fails to parse, is marked `'failed'`
  immediately without retrying (retrying can't fix either condition).
  **Real version needs:** exponential backoff with jitter (avoids
  synchronized retry storms across many jobs failing at once), and a
  considered answer for what "permanently failed" should trigger
  downstream (today: nothing — a `'failed'` row just sits there; no
  alerting, no dead-letter handling, no operator notification).
- **`ingress-nginx` + TLS external exposure never scaffolded.** Every
  service's K8s `Service` is `ClusterIP` — nothing is reachable from
  outside the Docker Desktop K8s cluster today except via `kubectl
  port-forward`. Architecture doc §1.4 flags this as manual/interactive
  local-cluster tooling, deliberately not YAML-scaffolded. Tracked:
  `PROGRESS.md` To-Do #14.
- **No table partitioning, no ClickHouse.** `historical_events` is a
  single unpartitioned table — correct scope for its first milestone, but
  won't scale indefinitely. Architecture doc's own documented future
  upgrade path. Tracked: `PROGRESS.md` To-Do #9c.

## Per-provider / per-channel integration

- **Digital Channels Gateway supports exactly one generic "chat" webhook
  shape**, not real per-provider integrations (Twilio, SMTP, social
  platform APIs). **Real version needs:** provider-specific adapters that
  normalize into the same `InboundChatMessage` shape. Tracked:
  `PROGRESS.md` To-Do #7.
- **No outbound/agent-reply delivery** back to the originating channel —
  Digital Channels Gateway is inbound-only so far. Tracked: `PROGRESS.md`
  To-Do #5.
- **Voice/SIP Media Gateway is unbuilt**, blocked on external
  confirmation from a collaborator's separate PoC (Kafka→NATS
  flexibility, language/framework, tenant_id retrofit feasibility) — see
  `PROGRESS.md`'s "Voice merge" section.

---

## Closed gaps

- **Every service connected to Postgres as a superuser, which silently
  made every Row-Level Security policy in this repo a no-op against real
  traffic (closed 2026-09-14).** This was a genuine, unintentional
  security bug, not a deliberate scope decision — unlike every other
  entry in this file. `POSTGRES_USER` ("ccaas") is always created as a
  Postgres **superuser** by the official `postgres:16` bootstrap image
  (both docker-compose's and Kubernetes' Postgres, per
  `deploy/k8s/infra-config.yaml`'s `POSTGRES_USER` ConfigMap key), and
  Postgres superusers unconditionally bypass Row-Level Security —
  `ALTER TABLE ... FORCE ROW LEVEL SECURITY` does not change this, FORCE
  only binds the table *owner*, never a superuser. Every service in this
  repo connected as `ccaas` for ALL queries (not just migrations) since
  its inception, so `tenant_identity_users`' and
  `task_router_queues`/`statuses`/`attributes`' `tenant_isolation` RLS
  policies — correctly written, correctly using
  `pkg/pgtenant.Pool.WithTenant` to set `app.current_tenant` per
  transaction from a JWT-derived tenant ID never taken from client input
  — were silently never actually enforced. Confirmed live before the
  fix: `IdentityService.ListUsers` returned users from every tenant in
  the table (62 distinct tenants observed), not just the caller's own,
  despite the policy and the Go code both being correct. This repo had
  already fixed the identical issue once before, but only for its own
  test suite (`services/tenant-identity/internal/pgstore/pgstore_test.go`'s
  `rlsTestRole`) — that fix never touched the actual running services.
  **Fix:** every Postgres-touching service (Task Router, Tenant &
  Identity, Historical Reporting, Background Worker Pool) now opens a
  SECOND connection pool, authenticated as a new, non-superuser,
  `NOSUPERUSER NOBYPASSRLS` role (`ccaas_app`), for all of its ongoing,
  steady-state queries — the superuser connection is now used ONLY to
  run that service's own `Migrate()` at startup (schema migrations, plus
  idempotently provisioning `ccaas_app` and `GRANT`ing it privileges on
  that service's own tables). Applied platform-wide, not just to the two
  services with RLS tables today, so a future RLS-protected table can
  never silently inherit this same bug again. See
  `ARCHITECTURE_FLOW.md` §5.0 for the full design and
  `services/*/internal/pg{store,config}/runtime_role.go` for the
  implementation. Verified live end-to-end post-fix: two real tenants
  created via real RPCs, `ListUsers`/`ListQueues` authenticated as tenant
  A now return ONLY tenant A's rows, and
  `SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname =
  'ccaas_app'` returns `f, f`.
