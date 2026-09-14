package pgstore

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrAlreadyExists is returned by Create-style calls on a duplicate
// unique key.
var ErrAlreadyExists = errors.New("pgstore: already exists")

// ErrNotFound is returned by Get-style calls when the row doesn't exist.
var ErrNotFound = errors.New("pgstore: not found")

// Tenant mirrors the tenants registry row.
type Tenant struct {
	TenantID  uuid.UUID
	Name      string
	CreatedAt time.Time
}

// TenantStore is the tenant-registry repository. Unlike every other
// repository in this package (and every repository in the whole repo's
// tenant-scoped services), TenantStore deliberately holds a raw
// *pgxpool.Pool rather than a *pgtenant.Pool: the tenants table is not
// tenant-scoped data -- it IS the tenant registry -- so there is no
// tenant_id to funnel queries through pgtenant.WithTenant with (see
// migrations/001_tenants.sql).
type TenantStore struct {
	pool *pgxpool.Pool
}

// NewTenantStore constructs a TenantStore over an unscoped connection
// pool.
func NewTenantStore(pool *pgxpool.Pool) *TenantStore {
	return &TenantStore{pool: pool}
}

// CreateTenant registers a new tenant, returning its generated ID.
func (s *TenantStore) CreateTenant(ctx context.Context, name string) (Tenant, error) {
	var out Tenant
	row := s.pool.QueryRow(ctx, `
		INSERT INTO tenants (name)
		VALUES ($1)
		RETURNING tenant_id, name, created_at
	`, name)
	if err := row.Scan(&out.TenantID, &out.Name, &out.CreatedAt); err != nil {
		return Tenant{}, err
	}
	return out, nil
}

// GetTenant retrieves one tenant by ID. Returns ErrNotFound if absent.
func (s *TenantStore) GetTenant(ctx context.Context, tenantID uuid.UUID) (Tenant, error) {
	var out Tenant
	row := s.pool.QueryRow(ctx, `SELECT tenant_id, name, created_at FROM tenants WHERE tenant_id = $1`, tenantID)
	if err := row.Scan(&out.TenantID, &out.Name, &out.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Tenant{}, ErrNotFound
		}
		return Tenant{}, err
	}
	return out, nil
}

// ListTenants enumerates every registered tenant.
func (s *TenantStore) ListTenants(ctx context.Context) ([]Tenant, error) {
	rows, err := s.pool.Query(ctx, `SELECT tenant_id, name, created_at FROM tenants ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Tenant
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.TenantID, &t.Name, &t.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// TenantExists is a lightweight existence check.
func (s *TenantStore) TenantExists(ctx context.Context, tenantID uuid.UUID) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)`, tenantID).Scan(&exists)
	return exists, err
}
