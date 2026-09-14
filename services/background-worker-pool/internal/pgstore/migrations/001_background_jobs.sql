-- Background Worker Pool's own copy of the Database-as-a-Queue table
-- defined generically in pkg/pgqueue/migrations/001_background_jobs.sql
-- (CCAAS_ENTERPRISE_ARCHITECTURE.md Section 3.3), reproduced here
-- verbatim (same columns/index) so THIS service owns and runs the
-- migration that actually creates the table it reads/writes via
-- pkg/pgqueue's Enqueue/ClaimJobs helpers.
--
-- Why a copy instead of embedding pkg/pgqueue's file directly: Go's
-- go:embed only embeds files within the embedding package's own
-- directory tree (or subdirectories of it) -- it cannot reach across a
-- package boundary to embed a file that lives in a different package's
-- directory (pkg/pgqueue/migrations is pkg/pgqueue's own //go:embed
-- target, not something this package can point a second //go:embed
-- directive at). This repo's established convention (see
-- services/task-router/internal/pgconfig/migrate.go,
-- services/tenant-identity/internal/pgstore/migrate.go,
-- services/historical-reporting/internal/pgstore/migrate.go) is every
-- service embeds and runs ITS OWN migrations/*.sql via its own
-- go:embed, tracked in its own uniquely-named *_schema_migrations table
-- (CLAUDE.md Rule 3) -- never a shared embedded FS reused across
-- service/package boundaries. Background Worker Pool is the first (and,
-- per architecture doc Section 3.3, likely only) real consumer of
-- pkg/pgqueue's table shape, so this migration is this service's own,
-- matching that convention exactly.
--
-- pkg/pgqueue/migrations/001_background_jobs.sql itself is left in place
-- as reference documentation of the generic table shape the pkg/pgqueue
-- Go code (Job struct, Enqueue, ClaimJobs) assumes -- nothing embeds or
-- runs it directly; see that file's own doc comment.
--
-- IF NOT EXISTS (on the table and the index): pkg/pgqueue's own package
-- tests (pkg/pgqueue/pgqueue_test.go) also provision a real
-- background_jobs table with this identical shape, directly against the
-- same shared docker-compose Postgres instance, so that pkg-level unit
-- tests can exercise Enqueue/ClaimJobs/MarkDone/MarkFailed without
-- depending on any services/ package (a layering inversion). `go test
-- ./...` runs different packages' tests concurrently by default, so
-- either ordering can create the table first -- IF NOT EXISTS makes this
-- migration idempotent against that, exactly like every CREATE TABLE IF
-- NOT EXISTS already used for this repo's *_schema_migrations tracking
-- tables (see migrate.go), just applied one layer down to the data table
-- itself because this is the one table two independent packages both
-- legitimately provision in tests.

CREATE TABLE IF NOT EXISTS background_jobs (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    job_type TEXT NOT NULL,          -- e.g. 'webhook_delivery', 'wrapup_sync', 'billing_rollup'
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending | processing | done | failed
    attempts INT NOT NULL DEFAULT 0,
    run_after TIMESTAMPTZ NOT NULL DEFAULT now(),  -- enables scheduling/backoff
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_claimable ON background_jobs (run_after)
    WHERE status = 'pending';
