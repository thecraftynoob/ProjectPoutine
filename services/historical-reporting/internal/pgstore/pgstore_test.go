package pgstore

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testDSN returns the docker-compose Postgres DSN used across this repo's
// local dev (.env.local.example), overridable via TEST_POSTGRES_DSN.
// Tests using it are integration-style and skip (not fail) if Postgres is
// unreachable, mirroring services/tenant-identity/internal/pgstore's and
// services/task-router/internal/pgconfig's own testDSN helpers.
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

// TestMigrateCreatesExpectedTables asserts both historical_events and
// historical_reporting_schema_migrations exist after Migrate runs, and
// that re-running Migrate on an already-migrated database is a safe
// no-op -- mirroring services/tenant-identity/internal/pgstore's and
// services/task-router/internal/pgconfig's migration test pattern.
func TestMigrateCreatesExpectedTables(t *testing.T) {
	pool := connectForTest(t)
	ctx := context.Background()

	for _, table := range []string{"historical_events", "historical_reporting_schema_migrations"} {
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

// TestInsertAndGetEventRoundTrip inserts a fabricated event row with a
// nested/multi-field JSONB payload and reads it back, asserting every
// column -- including the payload -- round-trips correctly.
func TestInsertAndGetEventRoundTrip(t *testing.T) {
	pool := connectForTest(t)
	store := NewStore(pool)
	ctx := context.Background()

	tenantID := uuid.New()
	payload := json.RawMessage(`{
		"reservationId": "res-123",
		"taskId": "task-456",
		"agentId": "agent-789",
		"expiresAt": "2026-09-14T00:00:30Z",
		"nested": {"queues": ["sales", "support"], "priority": 3, "urgent": true}
	}`)

	eventID, err := store.InsertEvent(ctx, tenantID, "reservation", "created", "tenant."+tenantID.String()+".reservation.created", payload)
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if eventID == uuid.Nil {
		t.Fatal("expected a generated non-nil event_id")
	}

	got, err := store.GetEvent(ctx, eventID)
	if err != nil {
		t.Fatalf("GetEvent: %v", err)
	}

	if got.EventID != eventID {
		t.Errorf("event_id: want %v, got %v", eventID, got.EventID)
	}
	if got.TenantID != tenantID {
		t.Errorf("tenant_id: want %v, got %v", tenantID, got.TenantID)
	}
	if got.Domain != "reservation" {
		t.Errorf("domain: want reservation, got %q", got.Domain)
	}
	if got.EventType != "created" {
		t.Errorf("event_type: want created, got %q", got.EventType)
	}
	if got.Subject != "tenant."+tenantID.String()+".reservation.created" {
		t.Errorf("subject mismatch: got %q", got.Subject)
	}
	if got.ReceivedAt.IsZero() {
		t.Error("expected received_at to be populated")
	}

	var wantMap, gotMap map[string]any
	if err := json.Unmarshal(payload, &wantMap); err != nil {
		t.Fatalf("unmarshal want payload: %v", err)
	}
	if err := json.Unmarshal(got.Payload, &gotMap); err != nil {
		t.Fatalf("unmarshal got payload: %v", err)
	}
	if wantMap["reservationId"] != gotMap["reservationId"] {
		t.Errorf("payload.reservationId: want %v, got %v", wantMap["reservationId"], gotMap["reservationId"])
	}
	gotNested, ok := gotMap["nested"].(map[string]any)
	if !ok {
		t.Fatalf("expected payload.nested to round-trip as an object, got %T", gotMap["nested"])
	}
	if gotNested["priority"] != float64(3) {
		t.Errorf("payload.nested.priority: want 3, got %v", gotNested["priority"])
	}
	if gotNested["urgent"] != true {
		t.Errorf("payload.nested.urgent: want true, got %v", gotNested["urgent"])
	}
	gotQueues, ok := gotNested["queues"].([]any)
	if !ok || len(gotQueues) != 2 {
		t.Fatalf("expected payload.nested.queues to round-trip as a 2-element array, got %v", gotNested["queues"])
	}
}
