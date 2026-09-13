package pgconfig

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/thecraftynoob/ProjectPoutine/pkg/pgtenant"
)

// ErrAlreadyExists is returned by Register* calls on a duplicate ID/name.
var ErrAlreadyExists = errors.New("pgconfig: already exists")

// ErrNotFound is returned by Get*/Remove* calls when the row doesn't
// exist.
var ErrNotFound = errors.New("pgconfig: not found")

// Queue mirrors spec Section 2.4.
type Queue struct {
	QueueID   string
	CreatedAt time.Time
}

// Registry wraps a pgtenant.Pool with the Task Router's admin-config
// repository methods. Every method funnels through pgtenant.WithTenant
// per the architecture doc's implementation rule (Section 1.1): no
// repository method here operates without a tenant-scoped transaction.
type Registry struct {
	pool *pgtenant.Pool
}

// NewRegistry constructs a Registry.
func NewRegistry(pool *pgtenant.Pool) *Registry {
	return &Registry{pool: pool}
}

// RegisterQueue creates a new queue. Returns ErrAlreadyExists on a
// duplicate queue_id (spec Section 3.1).
func (r *Registry) RegisterQueue(ctx context.Context, tenantID uuid.UUID, queueID string) (Queue, error) {
	var out Queue
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO task_router_queues (tenant_id, queue_id)
			VALUES ($1, $2)
			ON CONFLICT (tenant_id, queue_id) DO NOTHING
			RETURNING queue_id, created_at
		`, tenantID, queueID)
		return row.Scan(&out.QueueID, &out.CreatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Queue{}, ErrAlreadyExists
	}
	if err != nil {
		return Queue{}, err
	}
	return out, nil
}

// ListQueues enumerates all queues for the tenant.
func (r *Registry) ListQueues(ctx context.Context, tenantID uuid.UUID) ([]Queue, error) {
	var out []Queue
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT queue_id, created_at FROM task_router_queues ORDER BY queue_id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var q Queue
			if err := rows.Scan(&q.QueueID, &q.CreatedAt); err != nil {
				return err
			}
			out = append(out, q)
		}
		return rows.Err()
	})
	return out, err
}

// GetQueue retrieves one queue. Returns ErrNotFound if absent.
func (r *Registry) GetQueue(ctx context.Context, tenantID uuid.UUID, queueID string) (Queue, error) {
	var out Queue
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `SELECT queue_id, created_at FROM task_router_queues WHERE queue_id = $1`, queueID)
		return row.Scan(&out.QueueID, &out.CreatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Queue{}, ErrNotFound
	}
	return out, err
}

// QueueExists is a lightweight existence check used by validation flows
// (agent queue-membership replacement, task enqueue).
func (r *Registry) QueueExists(ctx context.Context, tenantID uuid.UUID, queueID string) (bool, error) {
	var exists bool
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM task_router_queues WHERE queue_id = $1)`, queueID).Scan(&exists)
	})
	return exists, err
}

// QueuesExist checks existence of multiple queue IDs at once (used by
// "Replace Agent Queue Memberships", spec Section 3.2: "rejected
// entirely, no partial application, if any queue ID doesn't exist").
// Returns the subset of queueIDs that do NOT exist.
func (r *Registry) QueuesExist(ctx context.Context, tenantID uuid.UUID, queueIDs []string) ([]string, error) {
	if len(queueIDs) == 0 {
		return nil, nil
	}
	var missing []string
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT q FROM unnest($1::text[]) AS q
			WHERE NOT EXISTS (SELECT 1 FROM task_router_queues WHERE queue_id = q)
		`, queueIDs)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var q string
			if err := rows.Scan(&q); err != nil {
				return err
			}
			missing = append(missing, q)
		}
		return rows.Err()
	})
	return missing, err
}

// RemoveQueue deletes a queue definition (spec Section 3.1: orphaned
// references on Agents/Tasks are not blocked or updated).
func (r *Registry) RemoveQueue(ctx context.Context, tenantID uuid.UUID, queueID string) error {
	return r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `DELETE FROM task_router_queues WHERE queue_id = $1`, queueID)
		return err
	})
}
