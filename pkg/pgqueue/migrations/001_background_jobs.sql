-- Database-as-a-Queue table, per CCAAS_ENTERPRISE_ARCHITECTURE.md
-- Section 3.3, columns/index reproduced exactly as specified there.

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
