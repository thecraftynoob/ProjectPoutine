// Package pgtenant implements the Postgres Row-Level-Security helper for
// Pattern A multi-tenancy described in CCAAS_ENTERPRISE_ARCHITECTURE.md
// Section 1.1 ("Shared Schema + Row-Level Security") and Section 3.1 Tier 2.
//
// WithTenant is THE method every repository-layer function in every
// service must funnel through to touch tenant-scoped data. Per the
// architecture doc's implementation rule: "no repository method compiles
// without a tenant_id parameter or an already-scoped *sql.Tx" — RLS is the
// last line of defense, not the first. A repository function should accept
// either a tenant ID (and call WithTenant itself) or a pgx.Tx that a
// caller already produced via WithTenant; it should never acquire its own
// unscoped connection/transaction for tenant-owned tables.
package pgtenant

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool wraps a pgxpool.Pool.
type Pool struct {
	pool *pgxpool.Pool
}

// Connect creates a connection pool against connString.
func Connect(ctx context.Context, connString string) (*Pool, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("pgtenant: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgtenant: ping: %w", err)
	}
	return &Pool{pool: pool}, nil
}

// Close closes the underlying pool.
func (p *Pool) Close() {
	p.pool.Close()
}

// WithTenant acquires a connection, begins a transaction, sets
// app.current_tenant for the lifetime of that transaction (via
// set_config(..., true) so it's parameterized rather than string-
// concatenated — safe even though tenantID is already a typed uuid.UUID),
// runs fn, and commits on success or rolls back on any error (including a
// panic, which is re-raised after rollback).
//
// Every RLS policy created per the architecture doc's Section 1.1 pattern
// reads current_setting('app.current_tenant') — so any query run inside
// fn via tx is transparently scoped to tenantID, even a buggy `SELECT *`.
func (p *Pool) WithTenant(ctx context.Context, tenantID uuid.UUID, fn func(tx pgx.Tx) error) (err error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgtenant: begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return
		}
		err = tx.Commit(ctx)
	}()

	// set_config's third argument (is_local=true) scopes the setting to
	// this transaction only, equivalent to SET LOCAL but safely
	// parameterized instead of building a "SET LOCAL app.current_tenant =
	// '<value>'" string.
	if _, err = tx.Exec(ctx, `SELECT set_config('app.current_tenant', $1, true)`, tenantID.String()); err != nil {
		return fmt.Errorf("pgtenant: set tenant context: %w", err)
	}

	if err = fn(tx); err != nil {
		return err
	}
	return nil
}
