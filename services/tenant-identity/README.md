# tenant-identity

**Responsibility:** CRUD for tenant configuration (routing policies, business
hours, feature flags); user/agent identity, roles (RBAC), and OAuth2/OIDC-based
authentication; issues short-lived JWTs consumed by every other service.
Intentionally the most upstream service — no dependency on any other domain
service (architecture doc Section 2.2).

**State:** Stateless (compute) / backed by stateful PostgreSQL.

**Status:** scaffold only. gRPC server with health check and tenant-context
interceptors wired; no domain RPCs, Postgres, or Redis connections yet.
