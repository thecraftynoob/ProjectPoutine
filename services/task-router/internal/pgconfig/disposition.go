package pgconfig

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Disposition is a tenant-defined tag an agent selects during a task's
// WrapUp status, for historical reporting (Wrap Up / Disposition two-step
// completion lifecycle).
type Disposition struct {
	DispositionID string
	Name          string
	CreatedAt     time.Time
}

// RegisterDisposition defines a new disposition. If dispositionID is
// empty, a random one is server-generated (mirroring the Queue registry's
// "Queue ID" being caller-required but this registry choosing to make its
// ID optional, since a UI selecting from a disposition dropdown has no
// natural human-meaningful ID the way a queue name does). Returns
// ErrAlreadyExists on a duplicate dispositionID.
func (r *Registry) RegisterDisposition(ctx context.Context, tenantID uuid.UUID, dispositionID, name string) (Disposition, error) {
	if dispositionID == "" {
		var err error
		dispositionID, err = randomDispositionID()
		if err != nil {
			return Disposition{}, fmt.Errorf("pgconfig: generate disposition id: %w", err)
		}
	}
	var out Disposition
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO task_router_dispositions (tenant_id, disposition_id, name)
			VALUES ($1, $2, $3)
			ON CONFLICT (tenant_id, disposition_id) DO NOTHING
			RETURNING disposition_id, name, created_at
		`, tenantID, dispositionID, name)
		return row.Scan(&out.DispositionID, &out.Name, &out.CreatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Disposition{}, ErrAlreadyExists
	}
	if err != nil {
		return Disposition{}, err
	}
	return out, nil
}

// ListDispositions enumerates every disposition registered for the
// tenant.
func (r *Registry) ListDispositions(ctx context.Context, tenantID uuid.UUID) ([]Disposition, error) {
	var out []Disposition
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT disposition_id, name, created_at FROM task_router_dispositions ORDER BY name`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var d Disposition
			if err := rows.Scan(&d.DispositionID, &d.Name, &d.CreatedAt); err != nil {
				return err
			}
			out = append(out, d)
		}
		return rows.Err()
	})
	return out, err
}

// GetDisposition retrieves one disposition. Returns ErrNotFound if absent.
func (r *Registry) GetDisposition(ctx context.Context, tenantID uuid.UUID, dispositionID string) (Disposition, error) {
	var out Disposition
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `SELECT disposition_id, name, created_at FROM task_router_dispositions WHERE disposition_id = $1`, dispositionID)
		return row.Scan(&out.DispositionID, &out.Name, &out.CreatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Disposition{}, ErrNotFound
	}
	return out, err
}

// RemoveDisposition retires a disposition (existing task references are
// unaffected -- see TaskRouterAdminService.RemoveDisposition's doc
// comment). Also removes it from every queue association.
func (r *Registry) RemoveDisposition(ctx context.Context, tenantID uuid.UUID, dispositionID string) error {
	return r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM task_router_queue_dispositions WHERE disposition_id = $1`, dispositionID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM task_router_dispositions WHERE disposition_id = $1`, dispositionID)
		return err
	})
}

// DispositionsExist checks existence of multiple disposition IDs at once
// (used by AssociateQueueDispositions' all-or-nothing validation, mirrors
// QueuesExist). Returns the subset of dispositionIDs that do NOT exist.
func (r *Registry) DispositionsExist(ctx context.Context, tenantID uuid.UUID, dispositionIDs []string) ([]string, error) {
	if len(dispositionIDs) == 0 {
		return nil, nil
	}
	var missing []string
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT d FROM unnest($1::text[]) AS d
			WHERE NOT EXISTS (SELECT 1 FROM task_router_dispositions WHERE disposition_id = d)
		`, dispositionIDs)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var d string
			if err := rows.Scan(&d); err != nil {
				return err
			}
			missing = append(missing, d)
		}
		return rows.Err()
	})
	return missing, err
}

// AssociateQueueDispositions fully replaces the set of dispositions
// available to agents working queueID. All-or-nothing: the caller
// (grpcapi) is expected to have already validated every dispositionID via
// DispositionsExist before calling this.
func (r *Registry) AssociateQueueDispositions(ctx context.Context, tenantID uuid.UUID, queueID string, dispositionIDs []string) error {
	return r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM task_router_queue_dispositions WHERE queue_id = $1`, queueID); err != nil {
			return err
		}
		for _, dispositionID := range dispositionIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO task_router_queue_dispositions (tenant_id, queue_id, disposition_id)
				VALUES ($1, $2, $3)
			`, tenantID, queueID, dispositionID); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListQueueDispositions returns the dispositions currently associated to
// queueID.
func (r *Registry) ListQueueDispositions(ctx context.Context, tenantID uuid.UUID, queueID string) ([]Disposition, error) {
	var out []Disposition
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT d.disposition_id, d.name, d.created_at
			FROM task_router_queue_dispositions qd
			JOIN task_router_dispositions d ON d.tenant_id = qd.tenant_id AND d.disposition_id = qd.disposition_id
			WHERE qd.queue_id = $1
			ORDER BY d.name
		`, queueID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var d Disposition
			if err := rows.Scan(&d.DispositionID, &d.Name, &d.CreatedAt); err != nil {
				return err
			}
			out = append(out, d)
		}
		return rows.Err()
	})
	return out, err
}

// randomDispositionID generates a short random hex ID for
// RegisterDisposition when the caller doesn't supply one.
func randomDispositionID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "disp-" + hex.EncodeToString(b), nil
}
