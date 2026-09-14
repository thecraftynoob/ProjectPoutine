-- Minimal, service-owned stand-in for real per-tenant settings (no such
-- subsystem exists yet -- see GAPS.md's "Infrastructure stand-ins" ->
-- "Instance: Background Worker Pool's wrap-up sync target URL"). Maps a
-- tenant to the URL its wrapup_sync jobs POST to. No admin UI/API to
-- manage this table exists this milestone -- rows are inserted directly
-- via SQL for testing.

CREATE TABLE background_worker_pool_wrapup_targets (
    tenant_id  UUID PRIMARY KEY,
    target_url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
