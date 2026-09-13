-- REFERENCE PATTERN ONLY — not a real domain migration.
--
-- This file documents the Row-Level Security pattern every tenant-scoped
-- table in every service must follow, per
-- CCAAS_ENTERPRISE_ARCHITECTURE.md Section 1.1, Pattern A
-- ("Shared Schema + Row-Level Security"). No domain service has real
-- tables yet — this migration is not applied to any schema and exists
-- purely as a copy-pasteable example for future service migrations.

-- 1. Every tenant-scoped table carries a NOT NULL tenant_id column.
CREATE TABLE example_widgets (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   UUID NOT NULL,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_example_widgets_tenant ON example_widgets (tenant_id);

-- 2. Row-Level Security is enabled on the table...
ALTER TABLE example_widgets ENABLE ROW LEVEL SECURITY;

-- ...and forced even for the table owner, so an application connecting as
-- the owning role still can't accidentally bypass RLS.
ALTER TABLE example_widgets FORCE ROW LEVEL SECURITY;

-- 3. A policy filters every query transparently against the tenant id set
-- for the current transaction by pkg/pgtenant.Pool.WithTenant via
-- `SELECT set_config('app.current_tenant', $1, true)`.
CREATE POLICY tenant_isolation ON example_widgets
    USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);

-- Even a buggy "SELECT * FROM example_widgets" run through a connection
-- that went through WithTenant can never return another tenant's rows,
-- because the policy above is applied by Postgres itself, not by
-- application-level WHERE clauses.
