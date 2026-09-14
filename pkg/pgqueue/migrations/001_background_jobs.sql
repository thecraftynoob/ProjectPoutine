-- Database-as-a-Queue table, per CCAAS_ENTERPRISE_ARCHITECTURE.md
-- Section 3.3, columns/index reproduced exactly as specified there.
--
-- REFERENCE ONLY -- nothing embeds or runs this file directly. Go's
-- go:embed cannot reach across a package boundary (a service's
-- migrate.go cannot embed a file living under pkg/pgqueue/migrations),
-- and this repo's established convention is every service embeds and
-- runs its OWN copy of any migration it depends on (see
-- services/task-router/internal/pgconfig/migrate.go et al., and
-- CLAUDE.md Rule 3's per-service-table-ownership requirement). Background
-- Worker Pool -- the first and, per this doc's own Section 3.3, likely
-- only real consumer of this table shape -- owns and runs its own copy
-- at services/background-worker-pool/internal/pgstore/migrations/001_background_jobs.sql.
-- Keep that copy's columns/index in sync with this file if the shape
-- here ever changes; this file stays as the canonical description the
-- pkg/pgqueue Go code (Job struct, Enqueue, ClaimJobs) assumes.

CREATE TABLE background_jobs (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    job_type TEXT NOT NULL,          -- e.g. 'webhook_delivery', 'wrapup_sync', 'billing_rollup'
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending | processing | done | failed
    attempts INT NOT NULL DEFAULT 0,
    run_after TIMESTAMPTZ NOT NULL DEFAULT now(),  -- enables scheduling/backoff
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_jobs_claimable ON background_jobs (run_after)
    WHERE status = 'pending';
