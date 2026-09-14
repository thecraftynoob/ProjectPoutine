package pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrWrapupTargetNotFound is returned by GetWrapupTargetURL when no row
// exists for the given tenant -- the wrapup_sync handler treats this as a
// clear, logged reason to fail the job rather than a crash (see
// internal/wrapupsync's job handler).
var ErrWrapupTargetNotFound = errors.New("pgstore: no wrapup target configured for tenant")

// WrapupTargetStore wraps background_worker_pool_wrapup_targets reads (and
// a test/operator-only insert helper -- see package doc comment: no
// admin API manages this table this milestone).
type WrapupTargetStore struct {
	pool *pgxpool.Pool
}

// NewWrapupTargetStore constructs a WrapupTargetStore.
func NewWrapupTargetStore(pool *pgxpool.Pool) *WrapupTargetStore {
	return &WrapupTargetStore{pool: pool}
}

// GetWrapupTargetURL looks up the configured wrap-up sync URL for a
// tenant, returning ErrWrapupTargetNotFound if no row exists.
func (s *WrapupTargetStore) GetWrapupTargetURL(ctx context.Context, tenantID uuid.UUID) (string, error) {
	var url string
	err := s.pool.QueryRow(ctx, `
		SELECT target_url FROM background_worker_pool_wrapup_targets WHERE tenant_id = $1
	`, tenantID).Scan(&url)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrWrapupTargetNotFound
	}
	if err != nil {
		return "", fmt.Errorf("pgstore: get wrapup target url: %w", err)
	}
	return url, nil
}

// SetWrapupTargetURL upserts a tenant's wrap-up sync target URL. Not
// exposed via any RPC/REST route this milestone (see GAPS.md) -- used by
// this package's own tests and by the live smoke-test setup to populate a
// row directly, standing in for the admin API a real tenant-settings
// subsystem would provide.
func (s *WrapupTargetStore) SetWrapupTargetURL(ctx context.Context, tenantID uuid.UUID, targetURL string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO background_worker_pool_wrapup_targets (tenant_id, target_url)
		VALUES ($1, $2)
		ON CONFLICT (tenant_id) DO UPDATE SET target_url = EXCLUDED.target_url, updated_at = now()
	`, tenantID, targetURL)
	if err != nil {
		return fmt.Errorf("pgstore: set wrapup target url: %w", err)
	}
	return nil
}
