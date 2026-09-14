// Package pgqueue implements the PostgreSQL Database-as-a-Queue pattern
// from CCAAS_ENTERPRISE_ARCHITECTURE.md Section 3.3: a durable,
// transactionally-safe job queue built on `background_jobs` plus
// `FOR UPDATE SKIP LOCKED`, avoiding a fourth piece of queue-specific
// infrastructure for delay-tolerant transactional work (webhook delivery,
// wrap-up sync, billing rollups, scheduled reports).
package pgqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job mirrors the background_jobs table columns (see
// migrations/001_background_jobs.sql).
type Job struct {
	ID        int64
	TenantID  uuid.UUID
	JobType   string
	Payload   []byte
	Status    string
	Attempts  int
	RunAfter  time.Time
	CreatedAt time.Time
}

// Enqueue inserts a new pending job, runnable at or after runAfter.
func Enqueue(ctx context.Context, pool *pgxpool.Pool, tenantID uuid.UUID, jobType string, payload []byte, runAfter time.Time) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO background_jobs (tenant_id, job_type, payload, run_after)
		VALUES ($1, $2, $3, $4)
	`, tenantID, jobType, payload, runAfter)
	if err != nil {
		return fmt.Errorf("pgqueue: enqueue: %w", err)
	}
	return nil
}

// ClaimJobs atomically claims up to `limit` pending, due jobs using
// FOR UPDATE SKIP LOCKED, exactly per architecture doc Section 3.3 — so N
// worker replicas calling this concurrently never race on the same row;
// each row is claimed by exactly one caller. Claimed jobs are marked
// 'processing' with attempts incremented as part of the same statement.
//
// Callers must supply an already-open transaction (tx) and commit it
// promptly after claiming so the row locks are released quickly — see
// Poller for a reusable loop that does this correctly.
func ClaimJobs(ctx context.Context, tx pgx.Tx, limit int) ([]Job, error) {
	rows, err := tx.Query(ctx, `
		WITH claimed AS (
		    SELECT id FROM background_jobs
		    WHERE status = 'pending' AND run_after <= now()
		    ORDER BY run_after
		    FOR UPDATE SKIP LOCKED
		    LIMIT $1
		)
		UPDATE background_jobs
		SET status = 'processing', attempts = attempts + 1
		WHERE id IN (SELECT id FROM claimed)
		RETURNING id, tenant_id, job_type, payload, status, attempts, run_after, created_at
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("pgqueue: claim jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.TenantID, &j.JobType, &j.Payload, &j.Status, &j.Attempts, &j.RunAfter, &j.CreatedAt); err != nil {
			return nil, fmt.Errorf("pgqueue: scan claimed job: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgqueue: iterate claimed jobs: %w", err)
	}
	return jobs, nil
}

// MarkDone transitions a claimed job to the terminal 'done' status. Generic
// across every job_type -- see Poller's doc comment: the generic Run/
// ClaimJobs loop deliberately leaves the terminal-status transition to the
// caller/handler, since a real handler still needs to decide what counts
// as success vs. failure for its own job_type. This helper (and
// MarkFailed below) exist so every handler doesn't hand-roll the same two
// UPDATE statements -- they are intentionally job-type-agnostic (no
// payload inspection, no job_type-specific branching), which is what
// keeps them fit for pkg/pgqueue rather than living in one service's
// handler code.
func MarkDone(ctx context.Context, pool *pgxpool.Pool, jobID int64) error {
	_, err := pool.Exec(ctx, `UPDATE background_jobs SET status = 'done' WHERE id = $1`, jobID)
	if err != nil {
		return fmt.Errorf("pgqueue: mark job %d done: %w", jobID, err)
	}
	return nil
}

// MarkFailed transitions a claimed job either back to 'pending' (with
// run_after pushed out to retryAt, for the caller's own backoff policy) or
// to the terminal 'failed' status, depending on retry. Callers decide
// retry themselves (e.g. based on the job's Attempts count against a
// max-attempts cutoff) -- this helper only performs the resulting
// Postgres update, it has no opinion on backoff formula or attempt
// limits, keeping it generic across job types the same way MarkDone is.
func MarkFailed(ctx context.Context, pool *pgxpool.Pool, jobID int64, retry bool, retryAt time.Time) error {
	if retry {
		_, err := pool.Exec(ctx, `
			UPDATE background_jobs SET status = 'pending', run_after = $2 WHERE id = $1
		`, jobID, retryAt)
		if err != nil {
			return fmt.Errorf("pgqueue: mark job %d pending for retry: %w", jobID, err)
		}
		return nil
	}
	_, err := pool.Exec(ctx, `UPDATE background_jobs SET status = 'failed' WHERE id = $1`, jobID)
	if err != nil {
		return fmt.Errorf("pgqueue: mark job %d failed: %w", jobID, err)
	}
	return nil
}
