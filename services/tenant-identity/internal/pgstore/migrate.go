// Package pgstore is Tenant & Identity Management's Postgres persistence
// layer (architecture doc Section 2.2 / Section 3.1 Tier 2): the tenant
// registry, tenant-scoped user/agent identity records (RLS-protected per
// pkg/pgtenant's Pattern A), and the platform-level JWT signing keypair
// (no RLS -- see migrations/003_signing_keypair.sql for why).
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
// tenant_identity_schema_migrations table so re-running on an
// already-migrated database is a safe no-op. Mirrors
// services/task-router/internal/pgconfig/migrate.go's pattern exactly --
// a minimal hand-rolled runner, no external migration framework
// dependency.
//
// The tracking table is named tenant_identity_schema_migrations, NOT a
// bare schema_migrations -- see that sibling file's doc comment for why:
// this service shares one physical Postgres instance with Task Router
// (CLAUDE.md Rule 3), and a bare name would have both services'
// independent migration runners silently sharing one table, which is
// exactly the cross-service table access Rule 3 forbids.
//
// Also idempotently provisions the shared, non-superuser runtime role
// (see ensureRuntimeRole's doc comment) and grants it exactly the
// privileges this service's own tables need -- see
// grantRuntimeRolePrivileges. This must run BEFORE this service's real
// gRPC-serving pools are opened as that role
// (services/tenant-identity/cmd/main.go), since a role with no GRANTs yet
// would fail every query the moment the service started using it.
//
// The role-creation/grant step runs inside its own
// pg_advisory_xact_lock-guarded transaction, unlike the rest of this
// function's plain pool.Exec calls: this package's own pgstore_test.go
// AND internal/grpcapi's test package both independently call Migrate
// against the same live Postgres instance, and `go test ./...` runs
// different packages' tests concurrently by default. CREATE ROLE ... IF
// NOT EXISTS is safe under that (see ensureRuntimeRole's doc comment),
// but two concurrent GRANT statements on the SAME table are not -- both
// need to update that table's ACL entry in pg_class, and observed in
// practice (this repo's own `go test ./...` run) as Postgres error
// XX000 "tuple concurrently updated" when two sessions' GRANTs raced.
// The advisory lock serializes ensureRuntimeRole+grantRuntimeRolePrivileges
// across concurrent Migrate callers, mirroring the exact fix
// services/historical-reporting/internal/pgstore/migrate.go and
// services/background-worker-pool/internal/pgstore/migrate.go already
// apply for their schema_migrations table creation (this repo has hit
// this shape of race more than once). Committed and released before the
// migration-file loop below runs (which has its own, separate
// EXISTS-then-INSERT idempotency via tenant_identity_schema_migrations,
// unaffected by this).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	roleTx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgstore: begin runtime role lock tx: %w", err)
	}
	if _, err := roleTx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(runtimeRoleAdvisoryLockKey)); err != nil {
		_ = roleTx.Rollback(ctx)
		return fmt.Errorf("pgstore: acquire runtime role advisory lock: %w", err)
	}
	if err := ensureRuntimeRole(ctx, roleTx); err != nil {
		_ = roleTx.Rollback(ctx)
		return err
	}
	if _, err := roleTx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS tenant_identity_schema_migrations (
			filename   TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		_ = roleTx.Rollback(ctx)
		return fmt.Errorf("pgstore: create tenant_identity_schema_migrations: %w", err)
	}
	if err := grantRuntimeRolePrivileges(ctx, roleTx); err != nil {
		_ = roleTx.Rollback(ctx)
		return err
	}
	if err := roleTx.Commit(ctx); err != nil {
		return fmt.Errorf("pgstore: commit runtime role lock tx: %w", err)
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
			`SELECT EXISTS(SELECT 1 FROM tenant_identity_schema_migrations WHERE filename = $1)`, name,
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
		if _, err := tx.Exec(ctx, `INSERT INTO tenant_identity_schema_migrations (filename) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("pgstore: record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pgstore: commit migration %s: %w", name, err)
		}
	}

	return nil
}
