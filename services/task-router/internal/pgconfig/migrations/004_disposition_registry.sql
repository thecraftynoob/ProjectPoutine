-- Disposition registry: a tenant-defined tag (e.g. "Ticket Created",
-- "Escalated Ticket", "Call Complete") an agent selects during a task's
-- WrapUp status, for historical reporting -- new concept introduced
-- alongside the Wrap Up / Disposition two-step task completion lifecycle
-- (TaskRouterService.EndTask / SetTaskDisposition / CompleteTask). No
-- default vocabulary is seeded -- a fresh tenant starts with zero
-- registered dispositions, same as the Attribute registry.
--
-- Follows the RLS pattern from pkg/pgtenant/migrations/000_example_rls_pattern.sql.

CREATE TABLE task_router_dispositions (
    tenant_id       UUID NOT NULL,
    disposition_id  TEXT NOT NULL,
    name            TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, disposition_id)
);

CREATE INDEX idx_task_router_dispositions_tenant ON task_router_dispositions (tenant_id);

ALTER TABLE task_router_dispositions ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_router_dispositions FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON task_router_dispositions
    USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);

-- Queue <-> Disposition association (spec: "an API to associate
-- dispositions to a Queue"). A queue's agents only see dispositions
-- relevant to that queue's work (TaskRouterAdminService.
-- AssociateQueueDispositions/ListQueueDispositions). No FK to
-- task_router_queues/task_router_dispositions -- this repo's RLS-scoped
-- tenant tables don't use cross-table FKs elsewhere in this schema either
-- (see e.g. task_router_queues itself, which spec Section 3.1 documents as
-- allowing orphaned references rather than blocking deletes on them); the
-- same orphaned-reference-is-fine convention applies here: removing a
-- queue or a disposition does not cascade-clean this join table, mirroring
-- RemoveQueue's existing documented behavior.
CREATE TABLE task_router_queue_dispositions (
    tenant_id       UUID NOT NULL,
    queue_id        TEXT NOT NULL,
    disposition_id  TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, queue_id, disposition_id)
);

CREATE INDEX idx_task_router_queue_dispositions_tenant_queue ON task_router_queue_dispositions (tenant_id, queue_id);

ALTER TABLE task_router_queue_dispositions ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_router_queue_dispositions FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON task_router_queue_dispositions
    USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
