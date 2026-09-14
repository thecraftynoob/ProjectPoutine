// Package pgstore is Background Worker Pool's Postgres persistence layer:
// this service's own copy of pkg/pgqueue's background_jobs table (see
// migrations/001_background_jobs.sql's doc comment for why it's a copy,
// not a cross-package embed), plus a new, minimal
// background_worker_pool_wrapup_targets table mapping tenant_id -> the
// URL wrapup_sync jobs POST to.
//
// background_worker_pool_wrapup_targets is a deliberate, documented
// stand-in for a real per-tenant settings subsystem, which doesn't exist
// anywhere in this repo yet -- see GAPS.md's "Infrastructure stand-ins"
// section, "Instance: Background Worker Pool's wrap-up sync target URL".
// No admin UI/API manages it this milestone; rows are inserted directly
// via SQL for testing.
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
// background_worker_pool_schema_migrations table so re-running on an
// already-migrated database is a safe no-op. Mirrors
// services/historical-reporting/internal/pgstore/migrate.go's pattern
// exactly -- a minimal hand-rolled runner, no external migration
// framework dependency.
//
// The tracking table is named background_worker_pool_schema_migrations,
// NOT a bare schema_migrations -- see the sibling services' migrate.go
// doc comments for why: this service shares one physical Postgres
// instance with every other service in this repo (CLAUDE.md Rule 3), and
// a bare name would have every service's independent migration runner
// silently reading and writing the SAME table, which is exactly the
// cross-service table access Rule 3 forbids (this repo already fixed one
// real instance of this mistake between task-router and tenant-identity).
// Each service owning its own uniquely-named tracking table keeps that
// boundary real, not just nominal.
//
// Serializes concurrent callers via pg_advisory_xact_lock before touching
// the tracking table, preemptively applying the same fix
// historical-reporting's Migrate needed: this package's own migration
// test and internal/wrapupsync's integration test both independently
// connect and call Migrate against the same live Postgres instance, and
// `go test ./...` runs different packages' tests concurrently by
// default -- without serialization, two concurrent first-time
// CREATE TABLE IF NOT EXISTS calls can race (Postgres error 23505
// duplicate key on pg_type's catalog index, since EXISTS-then-CREATE
// isn't atomic against another session doing the same DDL). The advisory
// lock is released automatically at transaction end
// (pg_advisory_xact_lock, not the session-scoped pg_advisory_lock), so it
// never leaks past one Migrate call.
//
// Also idempotently provisions the shared, non-superuser runtime role
// (see ensureRuntimeRole's doc comment) and grants it exactly the
// privileges this service's own tables need -- see
// grantRuntimeRolePrivileges. Both run inside the SAME advisory-lock-
// guarded transaction as the schema_migrations table creation below (not
// because role creation itself needs the lock -- CREATE ROLE is safe
// under concurrent execution on its own, see ensureRuntimeRole's doc
// comment -- but because it's simplest to piggyback on a transaction this
// function already opens and controls the lifetime of). This must run
// BEFORE this service's real Postgres-touching pool
// (services/background-worker-pool/cmd/main.go) is opened as that role,
// since a role with no GRANTs yet would fail every query the moment the
// service started using it.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	// Arbitrary fixed int64 key, unique to this service within this
	// shared Postgres instance's advisory-lock keyspace -- any distinct
	// constant works, the value itself carries no meaning.
	const advisoryLockKey = 0x4257504c // "BWPL" in hex, mnemonic only

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgstore: begin migration lock tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(advisoryLockKey)); err != nil {
		return fmt.Errorf("pgstore: acquire migration advisory lock: %w", err)
	}

	if err := ensureRuntimeRole(ctx, tx); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS background_worker_pool_schema_migrations (
			filename   TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("pgstore: create background_worker_pool_schema_migrations: %w", err)
	}

	if err := grantRuntimeRolePrivileges(ctx, tx); err != nil {
		return err
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
			`SELECT EXISTS(SELECT 1 FROM background_worker_pool_schema_migrations WHERE filename = $1)`, name,
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
		if _, err := tx.Exec(ctx, `INSERT INTO background_worker_pool_schema_migrations (filename) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("pgstore: record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pgstore: commit migration %s: %w", name, err)
		}
	}

	return nil
}
