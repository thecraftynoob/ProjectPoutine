-- Tenant registry (architecture doc Section 2.2 / Section 3.1 Tier 2:
-- "Tenant configuration ... system of record"). This table is deliberately
-- NOT tenant-scoped by a tenant_id column and carries NO Row-Level
-- Security policy: it IS the tenant registry itself, the thing every other
-- tenant-scoped table's tenant_id column refers to. RLS Pattern A
-- (pkg/pgtenant/migrations/000_example_rls_pattern.sql) exists to stop one
-- tenant's connection from seeing ANOTHER tenant's rows in a shared table
-- -- that concept doesn't apply to the registry of tenants itself, which
-- is inherently platform-level, cross-tenant-by-definition data (the same
-- way task_router's Queue/Status/Attribute registries are tenant-scoped
-- but this repo's platform migration-tracking table schema_migrations is
-- not tenant data at all).
CREATE TABLE tenants (
    tenant_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
