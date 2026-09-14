// Package pgstore is Historical Reporting's Postgres persistence layer:
// one generic ingestion table, historical_events, that materializes Task
// Router's full NATS JetStream event catalog (see
// ../eventconsumer) for BI/SLA/compliance reporting (architecture doc
// Section 2.2). This milestone is ingestion-only -- no read/query API,
// no RPC, no REST route; verification is direct SQL (see this service's
// live smoke test).
//
// Deliberately a single, unpartitioned table with no ClickHouse and no
// per-tenant Row-Level Security: unlike task-router's/tenant-identity's
// RLS-protected tables (pkg/pgtenant Pattern A), historical_events has no
// in-service query path at all today -- every read this milestone
// performs is an operator's direct psql query for verification, not a
// tenant-scoped application code path, so there is nothing yet for RLS to
// protect against. This mirrors pkg/pgqueue's background_jobs table (also
// a plain tenant_id column, no RLS) rather than task-router's/
// tenant-identity's tenant-scoped registries -- both are system-level
// ingestion/queue tables, not per-tenant CRUD resources. RLS should be
// added here if/when a real tenant-scoped read API is built on top of
// this table.
package pgstore

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies every embedded migration file in filename order, inside
// a single transaction per file, tracked via a
// historical_reporting_schema_migrations table so re-running on an
// already-migrated database is a safe no-op. Mirrors
// services/task-router/internal/pgconfig/migrate.go's and
// services/tenant-identity/internal/pgstore/migrate.go's pattern exactly
// -- a minimal hand-rolled runner, no external migration framework
// dependency.
//
// The tracking table is named historical_reporting_schema_migrations, NOT
// a bare schema_migrations -- see those sibling files' doc comments for
// why: this service shares one physical Postgres instance with every
// other service in this repo (CLAUDE.md Rule 3), and a bare name would
// have every service's independent migration runner silently reading and
// writing the SAME table, which is exactly the cross-service table access
// Rule 3 forbids (this repo already fixed one real instance of this
// mistake -- task-router and tenant-identity briefly shared one
// unnamespaced schema_migrations table before being split apart). Each
// service owning its own uniquely-named tracking table keeps that
// boundary real, not just nominal.
//
// Serializes concurrent callers via pg_advisory_xact_lock before touching
// the tracking table. Unlike task-router's/tenant-identity's sibling
// Migrate (each called from exactly one test package in this repo), this
// package's own tests and internal/eventconsumer's integration test both
// independently connect and call Migrate against the same live Postgres
// instance, and `go test ./...` runs different packages' tests
// concurrently by default -- without serialization, two concurrent first-
// time CREATE TABLE IF NOT EXISTS calls can race (observed in practice as
// Postgres error 23505 duplicate key on pg_type's catalog index, since
// EXISTS-then-CREATE isn't atomic against another session doing the same
// DDL). The advisory lock is released automatically at transaction end
// (pg_advisory_xact_lock, not the session-scoped pg_advisory_lock), so it
// never leaks past one Migrate call.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	// Arbitrary fixed int64 key, unique to this service within this
	// shared Postgres instance's advisory-lock keyspace -- any distinct
	// constant works, the value itself carries no meaning.
	const advisoryLockKey = 0x48495354 // "HIST" in hex, mnemonic only

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgstore: begin migration lock tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(advisoryLockKey)); err != nil {
		return fmt.Errorf("pgstore: acquire migration advisory lock: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS historical_reporting_schema_migrations (
			filename   TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("pgstore: create historical_reporting_schema_migrations: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgstore: commit migration lock tx: %w", err)
	}

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("pgstore: read migrations dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		var alreadyApplied bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM historical_reporting_schema_migrations WHERE filename = $1)`, name,
		).Scan(&alreadyApplied); err != nil {
			return fmt.Errorf("pgstore: check migration %s: %w", name, err)
		}
		if alreadyApplied {
			continue
		}

		data, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("pgstore: read migration %s: %w", name, err)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("pgstore: begin tx for %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(data)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("pgstore: apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO historical_reporting_schema_migrations (filename) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("pgstore: record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pgstore: commit migration %s: %w", name, err)
		}
	}

	return nil
}
