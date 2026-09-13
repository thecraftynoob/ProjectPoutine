-- Queue registry (spec Section 2.4): a named work bucket that Agents join
-- and Tasks reference. No other configuration exists on a Queue -- no
-- priority, no SLA target, no routing strategy.
--
-- Follows the RLS pattern from pkg/pgtenant/migrations/000_example_rls_pattern.sql
-- per architecture doc Section 1.1, Pattern A.

CREATE TABLE task_router_queues (
    tenant_id   UUID NOT NULL,
    queue_id    TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, queue_id)
);

CREATE INDEX idx_task_router_queues_tenant ON task_router_queues (tenant_id);

ALTER TABLE task_router_queues ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_router_queues FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON task_router_queues
    USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
