-- User/agent identity records (architecture doc Section 2.2: "user/agent
-- identity, roles (RBAC)"). Genuinely tenant-scoped data -- one tenant's
-- users must never be visible to another tenant's connection -- so this
-- follows the RLS pattern from
-- pkg/pgtenant/migrations/000_example_rls_pattern.sql exactly, same as
-- task_router_queues etc. in services/task-router/internal/pgconfig/migrations.
--
-- Roles are stored as a simple TEXT[] rather than a joined roles table:
-- per this milestone's scope, a role is just a named string tag on a
-- user, not a policy engine with its own permission graph -- a join table
-- would be pure overhead for that shape of data today.
CREATE TABLE tenant_identity_users (
    user_id        UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id      UUID NOT NULL,
    username       TEXT NOT NULL,
    password_hash  TEXT NOT NULL,
    roles          TEXT[] NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, user_id),
    -- Username unique per tenant (spec: "username unique per tenant"), not
    -- globally unique -- two different tenants may each have a "jdoe".
    UNIQUE (tenant_id, username)
);

CREATE INDEX idx_tenant_identity_users_tenant ON tenant_identity_users (tenant_id);

ALTER TABLE tenant_identity_users ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_identity_users FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant_identity_users
    USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
