# tenant-identity

**Responsibility:** Tenant CRUD (platform tenant registry), user/agent
identity with bcrypt-hashed password login, simple string-tag RBAC roles,
and short-lived JWT issuance consumed (in a later milestone) by every other
service. Intentionally the most upstream service — no dependency on any
other domain service (architecture doc Section 2.2).

**Scope for this milestone:** core identity + JWT issuance only.
Explicitly OUT of scope for now: tenant business-hours/feature-flag
config, full OAuth2/OIDC authorization-code flow, MFA, password reset,
external IdP federation, and JWT key rotation. Also explicitly deferred:
wiring real JWT validation into `pkg/tenantctx`'s interceptor or into
Agent Presence's WebSocket auth — this service issues and can verify its
own tokens end-to-end (see `pkg/jwtauth`), but nothing else in the
topology consumes them yet.

**State:** Stateless (compute) / backed by stateful PostgreSQL.

**Status:** Postgres-backed and feature-complete for this milestone's
scope. gRPC server with health check and tenant-context interceptors
wired; `TenantService` (CreateTenant/GetTenant/ListTenants) and
`IdentityService` (CreateUser/Login/GetUser/ListUsers) are implemented
over `internal/pgstore` and `internal/authn`. See
`proto/tenant-identity/v1/tenant_identity.proto` for the full contract and
its file-level note on why a handful of bootstrapping RPCs take an
explicit `tenant_id` field and are exempted from `pkg/tenantctx`'s
transport-level enforcement.

## JWT signing key

On first-ever startup this service generates an ECDSA P-256 keypair,
persists the private key in Postgres
(`internal/pgstore/migrations/003_signing_keypair.sql`), and reuses that
same key on every subsequent restart — tokens issued before a restart
remain verifiable after it. The public key is logged prominently at
startup for manual distribution to other services via a ConfigMap; see
`deploy/k8s/tenant-identity-public-key.example.yaml` for the mechanism and
its current manual-operator-step tradeoff. `pkg/jwtauth` implements the
actual Signer/Verifier and has zero dependency on this service's
infrastructure, so any service can verify a token given only that public
key in PEM form.

## Local development

Requires a reachable Postgres (`POSTGRES_DSN`, required) — see the repo
root's `docker-compose.yml`. Migrations run automatically on startup
(`internal/pgstore/migrate.go`, mirroring `services/task-router`'s
runner). `internal/pgstore`'s tests are integration-style and skip (not
fail) when Postgres is unreachable, overridable via `TEST_POSTGRES_DSN`.
