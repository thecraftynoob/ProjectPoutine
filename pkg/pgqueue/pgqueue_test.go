package pgqueue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testDSN returns the docker-compose Postgres DSN used across this repo's
// local dev, overridable via TEST_POSTGRES_DSN, mirroring every other
// service's own testDSN helper (e.g.
// services/historical-reporting/internal/pgstore).
func testDSN() string {
	if dsn := os.Getenv("TEST_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://ccaas:ccaas_dev_password@localhost:5432/ccaas?sslmode=disable"
}

// connectForTest opens a plain pgxpool.Pool and ensures a background_jobs
// table exists (this package has no Migrate of its own -- see
// migrations/001_background_jobs.sql's doc comment for why: each real
// consumer, e.g. Background Worker Pool, owns and runs its own copy of
// this table's migration). Purely so this package's own unit tests have
// somewhere to exercise Enqueue/ClaimJobs/MarkDone/MarkFailed without
// depending on any service package (which would be a layering inversion
// -- pkg/pgqueue must not import a services/ package). Skips (not fails)
// if Postgres is unreachable.
//
// Uses CREATE TABLE IF NOT EXISTS and deliberately does NOT drop the
// table afterward: services/background-worker-pool/internal/pgstore's own
// Migrate creates this SAME real table name (background_jobs) via a plain
// CREATE TABLE, against the same shared docker-compose Postgres instance
// `go test ./...` may run concurrently with this package's tests --
// dropping it here on cleanup raced Background Worker Pool's migration
// test in practice (one FAIL with `relation "background_jobs" already
// exists`, the mirror image of the classic schema_migrations race this
// repo already hit once — see
// services/historical-reporting/internal/pgstore/migrate.go's doc
// comment). Leaving the table in place once created, guarded by
// IF NOT EXISTS on both sides, makes both packages' tests idempotent
// against each other regardless of run order.
func connectForTest(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, testDSN())
	if err != nil {
		t.Skipf("skipping: cannot construct postgres pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skipping: postgres unreachable: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS background_jobs (
			id BIGSERIAL PRIMARY KEY,
			tenant_id UUID NOT NULL,
			job_type TEXT NOT NULL,
			payload JSONB NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			attempts INT NOT NULL DEFAULT 0,
			run_after TIMESTAMPTZ NOT NULL DEFAULT now(),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		pool.Close()
		t.Fatalf("create background_jobs for test: %v", err)
	}

	t.Cleanup(pool.Close)
	return pool
}

func TestEnqueueClaimMarkDone(t *testing.T) {
	pool := connectForTest(t)
	ctx := context.Background()
	tenantID := uuid.New()

	if err := Enqueue(ctx, pool, tenantID, "test_job", []byte(`{"x":1}`), time.Now()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	jobs, err := ClaimJobs(ctx, tx, 10)
	if err != nil {
		t.Fatalf("ClaimJobs: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var found *Job
	for i := range jobs {
		if jobs[i].TenantID == tenantID {
			found = &jobs[i]
		}
	}
	if found == nil {
		t.Fatalf("expected to claim the enqueued job for tenant %s", tenantID)
	}
	if found.Status != "processing" {
		t.Errorf("status after claim: want processing, got %q", found.Status)
	}
	if found.Attempts != 1 {
		t.Errorf("attempts after claim: want 1, got %d", found.Attempts)
	}

	if err := MarkDone(ctx, pool, found.ID); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM background_jobs WHERE id = $1`, found.ID).Scan(&status); err != nil {
		t.Fatalf("query status: %v", err)
	}
	if status != "done" {
		t.Errorf("status after MarkDone: want done, got %q", status)
	}
}

func TestMarkFailedRetryReschedules(t *testing.T) {
	pool := connectForTest(t)
	ctx := context.Background()
	tenantID := uuid.New()

	if err := Enqueue(ctx, pool, tenantID, "test_job", []byte(`{}`), time.Now()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	jobs, err := ClaimJobs(ctx, tx, 10)
	if err != nil {
		t.Fatalf("ClaimJobs: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	var jobID int64
	for _, j := range jobs {
		if j.TenantID == tenantID {
			jobID = j.ID
		}
	}
	if jobID == 0 {
		t.Fatalf("expected to claim the enqueued job")
	}

	retryAt := time.Now().Add(1 * time.Hour)
	if err := MarkFailed(ctx, pool, jobID, true, retryAt); err != nil {
		t.Fatalf("MarkFailed (retry): %v", err)
	}

	var status string
	var runAfter time.Time
	if err := pool.QueryRow(ctx, `SELECT status, run_after FROM background_jobs WHERE id = $1`, jobID).Scan(&status, &runAfter); err != nil {
		t.Fatalf("query: %v", err)
	}
	if status != "pending" {
		t.Errorf("status after retry MarkFailed: want pending, got %q", status)
	}
	if runAfter.Before(time.Now().Add(30 * time.Minute)) {
		t.Errorf("expected run_after to be pushed out to retryAt, got %v", runAfter)
	}
}

func TestMarkFailedPermanent(t *testing.T) {
	pool := connectForTest(t)
	ctx := context.Background()
	tenantID := uuid.New()

	if err := Enqueue(ctx, pool, tenantID, "test_job", []byte(`{}`), time.Now()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	jobs, err := ClaimJobs(ctx, tx, 10)
	if err != nil {
		t.Fatalf("ClaimJobs: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	var jobID int64
	for _, j := range jobs {
		if j.TenantID == tenantID {
			jobID = j.ID
		}
	}
	if jobID == 0 {
		t.Fatalf("expected to claim the enqueued job")
	}

	if err := MarkFailed(ctx, pool, jobID, false, time.Time{}); err != nil {
		t.Fatalf("MarkFailed (permanent): %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM background_jobs WHERE id = $1`, jobID).Scan(&status); err != nil {
		t.Fatalf("query: %v", err)
	}
	if status != "failed" {
		t.Errorf("status after permanent MarkFailed: want failed, got %q", status)
	}
}
