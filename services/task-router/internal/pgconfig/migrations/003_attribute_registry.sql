-- Attribute registry (spec Section 2.6): a registered {name, type}
-- definition used to validate the shape of arbitrary key/value data
-- stored on Agents and Tasks. No default vocabulary is seeded -- a fresh
-- tenant starts with zero registered attributes.
--
-- type is constrained to exactly the two values the spec defines
-- ("numeric" | "boolean") via a CHECK constraint rather than a Postgres
-- ENUM type, so future values don't require an ALTER TYPE migration --
-- consistent with keeping this registry simple, matching the Status
-- registry's plain-TEXT approach.
--
-- Follows the RLS pattern from pkg/pgtenant/migrations/000_example_rls_pattern.sql.

CREATE TABLE task_router_attributes (
    tenant_id   UUID NOT NULL,
    name        TEXT NOT NULL,
    type        TEXT NOT NULL CHECK (type IN ('numeric', 'boolean')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, name)
);

CREATE INDEX idx_task_router_attributes_tenant ON task_router_attributes (tenant_id);

ALTER TABLE task_router_attributes ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_router_attributes FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON task_router_attributes
    USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
