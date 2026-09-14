package eventconsumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats-server/v2/server"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/thecraftynoob/ProjectPoutine/pkg/eventbus"
	"github.com/thecraftynoob/ProjectPoutine/services/historical-reporting/internal/pgstore"
)

// startTestNATS starts an in-process NATS server with JetStream enabled
// on a random free port, mirroring pkg/eventbus/eventbus_integration_test.go's
// helper of the same name/shape exactly, so this proves the wildcard
// FilterSubject assumption against a real NATS+JetStream server rather
// than against documented-but-unverified consumer semantics.
func startTestNATS(t *testing.T) *server.Server {
	t.Helper()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random free port
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("server.NewServer: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatalf("test NATS server did not become ready in time")
	}
	t.Cleanup(s.Shutdown)
	return s
}

// testPostgresPool opens a plain pgxpool.Pool against the docker-compose
// Postgres (overridable via TEST_POSTGRES_DSN, matching
// internal/pgstore's own testDSN convention) and runs Migrate, skipping
// the test if Postgres is unreachable.
func testPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://ccaas:ccaas_dev_password@localhost:5432/ccaas?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping: cannot construct postgres pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skipping: postgres unreachable: %v", err)
	}
	if err := pgstore.Migrate(context.Background(), pool); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestConsumer_ReceivesAllThreeDomainsOnOneSubscribeCall is this
// milestone's highest-value test: it proves the whole pipeline end to
// end -- publish via eventbus to a real (in-process) JetStream stream,
// one Consumer.Start call with subjectFilter="tenant.*.>", and every
// published domain (task/agent/reservation) lands as a row in
// historical_events with the correct tenant_id/domain/event_type/payload.
//
// This also settles the real technical unknown this milestone's spec
// flagged: whether a single durable consumer's FilterSubject can use a
// wildcard broad enough to cover all three of Task Router's domains in
// one Subscribe call, rather than needing three separate Subscribe calls
// (and three consumer names). It can -- "tenant.*.>" matches
// "tenant.{id}.task.enqueued", "tenant.{id}.agent.status.changed", and
// "tenant.{id}.reservation.created" all in the same consumer, as this
// test demonstrates by publishing one of each and asserting all three are
// ingested by ONE Consumer.Start call.
func TestConsumer_ReceivesAllThreeDomainsOnOneSubscribeCall(t *testing.T) {
	srv := startTestNATS(t)
	url := srv.ClientURL()
	pool := testPostgresPool(t)
	store := pgstore.NewStore(pool)

	pub, err := eventbus.Connect(url)
	if err != nil {
		t.Fatalf("Connect (publisher): %v", err)
	}
	defer pub.Close()

	sub, err := eventbus.Connect(url)
	if err != nil {
		t.Fatalf("Connect (subscriber): %v", err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Mirrors Task Router's real stream subjects -- see
	// services/task-router/internal/events.StreamSubjects.
	if err := pub.EnsureStream(ctx, streamName, []string{
		"tenant.*.task.>",
		"tenant.*.agent.>",
		"tenant.*.reservation.>",
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	consumer := NewConsumer(sub, store, logger)

	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Start(subCtx); err != nil {
			t.Errorf("Consumer.Start: %v", err)
		}
	}()

	// Give the durable consumer a moment to be fully established before
	// publishing, matching eventbus_integration_test.go's own timing.
	time.Sleep(200 * time.Millisecond)

	tenantID := uuid.New()

	type published struct {
		domain    string
		eventType string
		fields    map[string]any
	}
	events := []published{
		{"task", "enqueued", map[string]any{"taskId": "task-1", "queueId": "queue-1", "taskType": "chat"}},
		{"agent", "capacity.config.updated", map[string]any{"agentId": "agent-1"}},
		{"reservation", "created", map[string]any{"reservationId": "res-1", "taskId": "task-1", "agentId": "agent-1", "expiresAt": "2026-09-14T00:00:30Z"}},
	}
	for _, e := range events {
		payload, err := structpb.NewStruct(e.fields)
		if err != nil {
			t.Fatalf("structpb.NewStruct: %v", err)
		}
		if err := pub.PublishEvent(ctx, tenantID, e.domain, e.eventType, payload); err != nil {
			t.Fatalf("PublishEvent(%s.%s): %v", e.domain, e.eventType, err)
		}
	}

	// Poll historical_events for all three rows to land.
	deadline := time.Now().Add(8 * time.Second)
	var rows []pgstore.Event
	for time.Now().Before(deadline) {
		rows = queryEventsForTenant(t, pool, tenantID)
		if len(rows) == 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if len(rows) != 3 {
		t.Fatalf("expected 3 ingested rows for tenant %s, got %d", tenantID, len(rows))
	}

	seen := map[string]pgstore.Event{}
	for _, r := range rows {
		seen[r.Domain+"."+r.EventType] = r
	}

	for _, e := range events {
		key := e.domain + "." + e.eventType
		row, ok := seen[key]
		if !ok {
			t.Errorf("missing ingested row for %s", key)
			continue
		}
		if row.TenantID != tenantID {
			t.Errorf("%s: tenant_id mismatch: want %v, got %v", key, tenantID, row.TenantID)
		}
		var payloadMap map[string]any
		if err := json.Unmarshal(row.Payload, &payloadMap); err != nil {
			t.Errorf("%s: payload did not unmarshal as JSON: %v", key, err)
			continue
		}
		for field, want := range e.fields {
			if got := payloadMap[field]; got != want {
				t.Errorf("%s: payload[%q]: want %v, got %v", key, field, want, got)
			}
		}
	}
}

func queryEventsForTenant(t *testing.T, pool *pgxpool.Pool, tenantID uuid.UUID) []pgstore.Event {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT event_id, tenant_id, domain, event_type, subject, payload, received_at
		FROM historical_events
		WHERE tenant_id = $1
	`, tenantID)
	if err != nil {
		t.Fatalf("query historical_events: %v", err)
	}
	defer rows.Close()

	var out []pgstore.Event
	for rows.Next() {
		var e pgstore.Event
		if err := rows.Scan(&e.EventID, &e.TenantID, &e.Domain, &e.EventType, &e.Subject, &e.Payload, &e.ReceivedAt); err != nil {
			t.Fatalf("scan historical_events row: %v", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate historical_events rows: %v", err)
	}
	return out
}
