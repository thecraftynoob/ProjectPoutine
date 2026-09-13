-- Status registry (spec Section 2.5): an open, admin-extensible allow-list
-- of valid Agent.status strings. NOT seeded here at migration time --
-- seeding the four defaults (Available, Break, Offline, Not Responding)
-- only makes sense once a tenant exists, so it is done at the
-- application layer on first-use/tenant-provision (see
-- pgconfig.EnsureDefaultStatuses), not baked into this schema migration.
--
-- Follows the RLS pattern from pkg/pgtenant/migrations/000_example_rls_pattern.sql.

CREATE TABLE task_router_statuses (
    tenant_id   UUID NOT NULL,
    status      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, status)
);

CREATE INDEX idx_task_router_statuses_tenant ON task_router_statuses (tenant_id);

ALTER TABLE task_router_statuses ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_router_statuses FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON task_router_statuses
    USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
