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
