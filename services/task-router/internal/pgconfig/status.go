package pgconfig

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// StatusEntry mirrors spec Section 2.5.
type StatusEntry struct {
	Status    string
	CreatedAt time.Time
}

// DefaultStatuses is the seed vocabulary from spec Section 2.5: "Seeded
// on first run with four defaults." Seeding happens per-tenant at the
// application layer (EnsureDefaultStatuses), not in a schema migration,
// since a migration runs once at deploy time before any tenant
// necessarily exists -- see migrations/002_status_registry.sql's header
// comment for the reasoning.
var DefaultStatuses = []string{"Available", "Break", "Offline", "Not Responding"}

// EnsureDefaultStatuses idempotently seeds the four default statuses for
// a tenant if its Status registry is currently empty. Called once at
// service startup for now (the simplest reasonable interpretation of "on
// tenant provision, seed default statuses" in an environment that does
// not yet have a dedicated tenant-provisioning lifecycle hook/event to
// attach to -- see README for the fuller design note); safe to call
// repeatedly since each insert is ON CONFLICT DO NOTHING.
func (r *Registry) EnsureDefaultStatuses(ctx context.Context, tenantID uuid.UUID) error {
	return r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		for _, s := range DefaultStatuses {
			if _, err := tx.Exec(ctx, `
				INSERT INTO task_router_statuses (tenant_id, status)
				VALUES ($1, $2)
				ON CONFLICT (tenant_id, status) DO NOTHING
			`, tenantID, s); err != nil {
				return err
			}
		}
		return nil
	})
}

// RegisterStatus adds a new status to the allow-list. Returns
// ErrAlreadyExists on a duplicate (spec Section 3.6).
func (r *Registry) RegisterStatus(ctx context.Context, tenantID uuid.UUID, status string) (StatusEntry, error) {
	var out StatusEntry
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO task_router_statuses (tenant_id, status)
			VALUES ($1, $2)
			ON CONFLICT (tenant_id, status) DO NOTHING
			RETURNING status, created_at
		`, tenantID, status)
		return row.Scan(&out.Status, &out.CreatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return StatusEntry{}, ErrAlreadyExists
	}
	return out, err
}

// ListStatuses enumerates the registry.
func (r *Registry) ListStatuses(ctx context.Context, tenantID uuid.UUID) ([]StatusEntry, error) {
	var out []StatusEntry
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT status, created_at FROM task_router_statuses ORDER BY status`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var s StatusEntry
			if err := rows.Scan(&s.Status, &s.CreatedAt); err != nil {
				return err
			}
			out = append(out, s)
		}
		return rows.Err()
	})
	return out, err
}

// StatusExists checks whether a status value is currently registered
// (used to validate "Set Agent Master Status" per spec Section 3.2).
func (r *Registry) StatusExists(ctx context.Context, tenantID uuid.UUID, status string) (bool, error) {
	var exists bool
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM task_router_statuses WHERE status = $1)`, status).Scan(&exists)
	})
	return exists, err
}

// RemoveStatus removes a status from the allow-list (spec Section 3.6:
// agents currently holding this status are unaffected).
func (r *Registry) RemoveStatus(ctx context.Context, tenantID uuid.UUID, status string) error {
	return r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `DELETE FROM task_router_statuses WHERE status = $1`, status)
		return err
	})
}
