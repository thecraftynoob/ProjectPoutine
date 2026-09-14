package pgstore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testDSN returns the docker-compose Postgres DSN used across this repo's
// local dev (.env.local.example), overridable via TEST_POSTGRES_DSN --
// mirrors services/historical-reporting/internal/pgstore's testDSN
// helper.
func testDSN() string {
	if dsn := os.Getenv("TEST_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://ccaas:ccaas_dev_password@localhost:5432/ccaas?sslmode=disable"
}

// connectForTest opens a plain pgxpool.Pool, runs Migrate, and registers
// cleanup, skipping the test entirely if Postgres isn't reachable.
func connectForTest(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, testDSN())
	if err != nil {
		t.Skipf("skipping: cannot construct postgres pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skipping: postgres unreachable: %v", err)
	}

	if err := Migrate(context.Background(), pool); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}

	t.Cleanup(pool.Close)
	return pool
}

// TestMigrateCreatesExpectedTables asserts background_jobs,
// background_worker_pool_wrapup_targets, and
// background_worker_pool_schema_migrations all exist after Migrate runs,
// and that re-running Migrate on an already-migrated database is a safe
// no-op -- mirrors services/historical-reporting/internal/pgstore's
// migration test pattern.
func TestMigrateCreatesExpectedTables(t *testing.T) {
	pool := connectForTest(t)
	ctx := context.Background()

	for _, table := range []string{
		"background_jobs",
		"background_worker_pool_wrapup_targets",
		"background_worker_pool_schema_migrations",
	} {
		var exists bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM information_schema.tables WHERE table_name = $1
		)`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("check table %s exists: %v", table, err)
		}
		if !exists {
			t.Errorf("expected table %s to exist after Migrate", table)
		}
	}

	// Re-running must be a safe no-op.
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("second Migrate call: %v", err)
	}
}

// TestWrapupTargetRoundTrip exercises SetWrapupTargetURL/
// GetWrapupTargetURL, including the not-found path and the upsert-on-
// conflict path.
func TestWrapupTargetRoundTrip(t *testing.T) {
	pool := connectForTest(t)
	store := NewWrapupTargetStore(pool)
	ctx := context.Background()

	tenantID := uuid.New()

	if _, err := store.GetWrapupTargetURL(ctx, tenantID); err != ErrWrapupTargetNotFound {
		t.Fatalf("expected ErrWrapupTargetNotFound for unconfigured tenant, got %v", err)
	}

	if err := store.SetWrapupTargetURL(ctx, tenantID, "http://example.invalid/wrapup"); err != nil {
		t.Fatalf("SetWrapupTargetURL: %v", err)
	}
	got, err := store.GetWrapupTargetURL(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetWrapupTargetURL: %v", err)
	}
	if got != "http://example.invalid/wrapup" {
		t.Errorf("target_url: want %q, got %q", "http://example.invalid/wrapup", got)
	}

	// Upsert should replace, not duplicate/conflict.
	if err := store.SetWrapupTargetURL(ctx, tenantID, "http://example.invalid/wrapup-v2"); err != nil {
		t.Fatalf("SetWrapupTargetURL (update): %v", err)
	}
	got, err = store.GetWrapupTargetURL(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetWrapupTargetURL (after update): %v", err)
	}
	if got != "http://example.invalid/wrapup-v2" {
		t.Errorf("target_url after update: want %q, got %q", "http://example.invalid/wrapup-v2", got)
	}
}
