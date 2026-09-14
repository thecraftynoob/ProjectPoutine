package wrapupsync

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thecraftynoob/ProjectPoutine/pkg/pgqueue"
	"github.com/thecraftynoob/ProjectPoutine/services/background-worker-pool/internal/pgstore"
)

// testPostgresPool opens a plain pgxpool.Pool against the docker-compose
// Postgres (overridable via TEST_POSTGRES_DSN, matching
// internal/pgstore's own testDSN convention) and runs Migrate, skipping
// the test if Postgres is unreachable.
func testPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://ccaas:ccaas_dev_password@localhost:5432/ccaas?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping: cannot construct postgres pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skipping: postgres unreachable: %v", err)
	}
	if err := pgstore.Migrate(context.Background(), pool); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// enqueueTestJob inserts a wrapup_sync job directly (bypassing the
// consumer) and returns its claimed pgqueue.Job -- claiming it the same
// way the real Poller would (FOR UPDATE SKIP LOCKED, attempts
// incremented), so this test exercises the handler exactly as the poller
// invokes it in production.
//
// Claims a generous batch and picks out the row matching tenantID, rather
// than assuming ClaimJobs' single-job return is necessarily the one this
// call just enqueued: background_jobs is a real shared table in the
// docker-compose Postgres instance, and earlier test runs (this package's
// own, or a manual smoke test) can leave old pending/failed rows behind
// since nothing truncates the table between runs. Each test uses a fresh
// uuid.New() tenantID, so filtering on it is a reliable, collision-free
// selector regardless of what else is sitting in the table.
func enqueueTestJob(t *testing.T, pool *pgxpool.Pool, tenantID uuid.UUID, payload WrapupSyncPayload) pgqueue.Job {
	t.Helper()
	ctx := context.Background()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := pgqueue.Enqueue(ctx, pool, tenantID, JobType, body, time.Now()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	return claimJobForTenant(t, pool, tenantID)
}

// claimJobForTenant claims a batch of pending jobs and returns the one
// belonging to tenantID -- see enqueueTestJob's doc comment for why a
// tenant-scoped selection, not "the first job claimed", is required
// against this shared table.
func claimJobForTenant(t *testing.T, pool *pgxpool.Pool, tenantID uuid.UUID) pgqueue.Job {
	t.Helper()
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	jobs, err := pgqueue.ClaimJobs(ctx, tx, 100)
	if err != nil {
		t.Fatalf("ClaimJobs: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit claim: %v", err)
	}

	for _, j := range jobs {
		if j.TenantID == tenantID {
			return j
		}
	}
	t.Fatalf("expected to claim a job for tenant %s, but none of the %d claimed jobs matched", tenantID, len(jobs))
	return pgqueue.Job{}
}

func jobStatus(t *testing.T, pool *pgxpool.Pool, jobID int64) (status string, attempts int, runAfter time.Time) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT status, attempts, run_after FROM background_jobs WHERE id = $1`, jobID,
	).Scan(&status, &attempts, &runAfter)
	if err != nil {
		t.Fatalf("query job status: %v", err)
	}
	return status, attempts, runAfter
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestHandleJob_Success proves the highest-value path end to end: a real
// httptest.Server stands in for the tenant's CRM, the handler POSTs the
// exact wrapup_sync payload as JSON, and on a 2xx response the job's
// status flips to 'done' in Postgres.
func TestHandleJob_Success(t *testing.T) {
	pool := testPostgresPool(t)
	targets := pgstore.NewWrapupTargetStore(pool)
	tenantID := uuid.New()

	var mu sync.Mutex
	var gotBody []byte
	var gotContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotContentType = r.Header.Get("Content-Type")
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		gotBody = body
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err := targets.SetWrapupTargetURL(context.Background(), tenantID, srv.URL); err != nil {
		t.Fatalf("SetWrapupTargetURL: %v", err)
	}

	payload := WrapupSyncPayload{TaskID: "task-abc", AgentID: "agent-xyz", TenantID: tenantID}
	job := enqueueTestJob(t, pool, tenantID, payload)

	handler := NewHandler(pool, targets, testLogger())
	if err := handler.HandleJob(job); err != nil {
		t.Fatalf("HandleJob: %v", err)
	}

	status, attempts, _ := jobStatus(t, pool, job.ID)
	if status != "done" {
		t.Errorf("job status: want done, got %q", status)
	}
	if attempts != 1 {
		t.Errorf("job attempts: want 1, got %d", attempts)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotContentType != "application/json" {
		t.Errorf("Content-Type: want application/json, got %q", gotContentType)
	}
	var gotPayload WrapupSyncPayload
	if err := json.Unmarshal(gotBody, &gotPayload); err != nil {
		t.Fatalf("unmarshal received body: %v (body=%s)", err, gotBody)
	}
	if gotPayload.TaskID != payload.TaskID {
		t.Errorf("taskId: want %q, got %q", payload.TaskID, gotPayload.TaskID)
	}
	if gotPayload.AgentID != payload.AgentID {
		t.Errorf("agentId: want %q, got %q", payload.AgentID, gotPayload.AgentID)
	}
	if gotPayload.TenantID != tenantID {
		t.Errorf("tenantId: want %v, got %v", tenantID, gotPayload.TenantID)
	}
}

// TestHandleJob_NonTerminalHTTPStatus_SchedulesRetry proves the failure
// path: a non-2xx response causes the job to go back to 'pending' with
// run_after pushed into the future (backoff), not immediately 'failed',
// while attempts stays under maxAttempts.
func TestHandleJob_NonTerminalHTTPStatus_SchedulesRetry(t *testing.T) {
	pool := testPostgresPool(t)
	targets := pgstore.NewWrapupTargetStore(pool)
	tenantID := uuid.New()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	if err := targets.SetWrapupTargetURL(context.Background(), tenantID, srv.URL); err != nil {
		t.Fatalf("SetWrapupTargetURL: %v", err)
	}

	payload := WrapupSyncPayload{TaskID: "task-fail", TenantID: tenantID}
	job := enqueueTestJob(t, pool, tenantID, payload)
	before := time.Now()

	handler := NewHandler(pool, targets, testLogger())
	if err := handler.HandleJob(job); err != nil {
		t.Fatalf("HandleJob: %v", err)
	}

	status, attempts, runAfter := jobStatus(t, pool, job.ID)
	if status != "pending" {
		t.Errorf("job status: want pending (scheduled retry), got %q", status)
	}
	if attempts != 1 {
		t.Errorf("job attempts: want 1, got %d", attempts)
	}
	if !runAfter.After(before) {
		t.Errorf("expected run_after (%v) to be pushed into the future relative to %v", runAfter, before)
	}
}

// TestHandleJob_UnreachableURL_EventuallyFailsAfterMaxAttempts proves the
// full retry-exhaustion path against an unreachable URL: simulate
// maxAttempts consecutive claims/handles and assert the job ends up
// permanently 'failed', not stuck retrying forever.
func TestHandleJob_UnreachableURL_EventuallyFailsAfterMaxAttempts(t *testing.T) {
	pool := testPostgresPool(t)
	targets := pgstore.NewWrapupTargetStore(pool)
	tenantID := uuid.New()

	// Deliberately unreachable: a closed httptest server's URL.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	unreachableURL := srv.URL
	srv.Close()

	if err := targets.SetWrapupTargetURL(context.Background(), tenantID, unreachableURL); err != nil {
		t.Fatalf("SetWrapupTargetURL: %v", err)
	}

	payload := WrapupSyncPayload{TaskID: "task-unreachable", TenantID: tenantID}
	job := enqueueTestJob(t, pool, tenantID, payload)

	handler := NewHandler(pool, targets, testLogger())

	var lastStatus string
	var lastAttempts int
	for i := 0; i < maxAttempts+1; i++ {
		if err := handler.HandleJob(job); err != nil {
			t.Fatalf("HandleJob (iteration %d): %v", i, err)
		}
		lastStatus, lastAttempts, _ = jobStatus(t, pool, job.ID)
		if lastStatus == "failed" {
			break
		}
		// Force run_after into the past so the next claim is immediately
		// eligible, and re-claim exactly like the real poller would
		// (incrementing attempts) rather than reusing the same Job value.
		// Uses claimJobForTenant (not a bare limit=1 claim) since
		// background_jobs is a shared table that can have other pending
		// rows left over from earlier runs -- see enqueueTestJob's doc
		// comment.
		if _, err := pool.Exec(context.Background(), `UPDATE background_jobs SET run_after = now() - interval '1 minute' WHERE id = $1`, job.ID); err != nil {
			t.Fatalf("force run_after: %v", err)
		}
		job = claimJobForTenant(t, pool, tenantID)
	}

	if lastStatus != "failed" {
		t.Fatalf("expected job to be permanently 'failed' after exceeding maxAttempts, got status=%q attempts=%d", lastStatus, lastAttempts)
	}
}

// TestHandleJob_NoTargetConfigured proves a tenant with no wrapup target
// row fails the job immediately (not a crash, not a silent drop) with a
// clear reason logged.
func TestHandleJob_NoTargetConfigured(t *testing.T) {
	pool := testPostgresPool(t)
	targets := pgstore.NewWrapupTargetStore(pool)
	tenantID := uuid.New() // deliberately never configured

	payload := WrapupSyncPayload{TaskID: "task-no-target", TenantID: tenantID}
	job := enqueueTestJob(t, pool, tenantID, payload)

	handler := NewHandler(pool, targets, testLogger())
	if err := handler.HandleJob(job); err != nil {
		t.Fatalf("HandleJob: %v", err)
	}

	status, _, _ := jobStatus(t, pool, job.ID)
	if status != "failed" {
		t.Errorf("job status: want failed (no target configured), got %q", status)
	}
}

// TestHandleJob_UnrecognizedJobType proves the dispatcher's default case
// marks an unknown job_type 'failed' rather than panicking the poller.
func TestHandleJob_UnrecognizedJobType(t *testing.T) {
	pool := testPostgresPool(t)
	targets := pgstore.NewWrapupTargetStore(pool)
	tenantID := uuid.New()
	ctx := context.Background()

	if err := pgqueue.Enqueue(ctx, pool, tenantID, "some_other_job_type", []byte(`{}`), time.Now()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job := claimJobForTenant(t, pool, tenantID)

	handler := NewHandler(pool, targets, testLogger())
	if err := handler.HandleJob(job); err != nil {
		t.Fatalf("HandleJob: %v", err)
	}

	status, _, _ := jobStatus(t, pool, job.ID)
	if status != "failed" {
		t.Errorf("job status: want failed (unrecognized job_type), got %q", status)
	}
}
