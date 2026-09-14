// Package pgconfig implements the Postgres-backed, RLS-protected
// low-change admin configuration registries for Task Router: Queue (spec
// Section 2.4/3.1), Status (spec Section 2.5/3.6), and Attribute (spec
// Section 2.6/3.6) -- architecture doc Section 3.1 Tier 2 territory
// ("long-term, transactionally queried, rarely mutated"), as opposed to
// the Redis-backed hot path in ../redisdomain.
package pgconfig

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
// a single transaction, tracked via a task_router_schema_migrations table
// so re-running on an already-migrated database is a safe no-op. This is a
// minimal hand-rolled runner (no external migration framework dependency)
// consistent with this repo's other pkg helpers being small and
// dependency-light.
//
// The tracking table is named task_router_schema_migrations, NOT a bare
// schema_migrations, deliberately: this service shares one physical
// Postgres instance with Tenant & Identity Management (CLAUDE.md Rule 3
// -- "services in this repo already share one Postgres instance... that
// is intentional"), and a bare schema_migrations name would have both
// services' independent migration runners silently reading and writing
// the SAME table, which is exactly the cross-service table access Rule 3
// forbids ("every table belongs to exactly one service's own
// migrations"). Each service owning its own uniquely-named tracking
// table keeps that boundary real, not just nominal.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS task_router_schema_migrations (
			filename   TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("pgconfig: create task_router_schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("pgconfig: read migrations dir: %w", err)
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
			`SELECT EXISTS(SELECT 1 FROM task_router_schema_migrations WHERE filename = $1)`, name,
		).Scan(&alreadyApplied); err != nil {
			return fmt.Errorf("pgconfig: check migration %s: %w", name, err)
		}
		if alreadyApplied {
			continue
		}

		data, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("pgconfig: read migration %s: %w", name, err)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("pgconfig: begin tx for %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(data)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("pgconfig: apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO task_router_schema_migrations (filename) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("pgconfig: record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pgconfig: commit migration %s: %w", name, err)
		}
	}

	return nil
}
