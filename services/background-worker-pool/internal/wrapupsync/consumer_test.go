package wrapupsync

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
	"github.com/thecraftynoob/ProjectPoutine/services/background-worker-pool/internal/pgstore"
)

// startTestNATS starts an in-process NATS server with JetStream enabled
// on a random free port, mirroring
// services/historical-reporting/internal/eventconsumer/consumer_test.go's
// helper of the same name/shape exactly.
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

func consumerTestPostgresPool(t *testing.T) *pgxpool.Pool {
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

// TestConsumer_TaskCompletedEnqueuesWrapupJob is this milestone's
// highest-value integration proof: publish a real task.completed event
// (plus a non-matching event this consumer must NOT react to) via
// eventbus to a real (in-process) JetStream stream, run Consumer.Start
// with subjectFilter="tenant.*.task.completed", and assert exactly one
// wrapup_sync row lands in background_jobs with the right payload.
func TestConsumer_TaskCompletedEnqueuesWrapupJob(t *testing.T) {
	srv := startTestNATS(t)
	url := srv.ClientURL()
	pool := consumerTestPostgresPool(t)

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
	consumer := NewConsumer(sub, pool, logger)

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
	// publishing, matching historical-reporting's consumer_test.go timing.
	time.Sleep(200 * time.Millisecond)

	tenantID := uuid.New()

	// task.enqueued must NOT produce a wrapup_sync job -- this consumer's
	// subjectFilter is scoped to task.completed only, unlike Historical
	// Reporting's full-catalog wildcard.
	enqueuedPayload, err := structpb.NewStruct(map[string]any{
		"taskId": "task-should-be-ignored", "queueId": "queue-1", "taskType": "chat",
	})
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	if err := pub.PublishEvent(ctx, tenantID, "task", "enqueued", enqueuedPayload); err != nil {
		t.Fatalf("PublishEvent(task.enqueued): %v", err)
	}

	completedPayload, err := structpb.NewStruct(map[string]any{
		"taskId": "task-completed-1", "agentId": "agent-completed-1",
	})
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	if err := pub.PublishEvent(ctx, tenantID, "task", "completed", completedPayload); err != nil {
		t.Fatalf("PublishEvent(task.completed): %v", err)
	}

	// Poll background_jobs for the wrapup_sync row to land.
	deadline := time.Now().Add(8 * time.Second)
	var rows []jobRow
	for time.Now().Before(deadline) {
		rows = queryJobsForTenant(t, pool, tenantID)
		if len(rows) >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 background_jobs row for tenant %s (task.completed only), got %d", tenantID, len(rows))
	}

	row := rows[0]
	if row.jobType != JobType {
		t.Errorf("job_type: want %q, got %q", JobType, row.jobType)
	}
	if row.status != "pending" {
		t.Errorf("status: want pending, got %q", row.status)
	}

	var payload WrapupSyncPayload
	if err := json.Unmarshal(row.payload, &payload); err != nil {
		t.Fatalf("unmarshal job payload: %v", err)
	}
	if payload.TaskID != "task-completed-1" {
		t.Errorf("payload.taskId: want %q, got %q", "task-completed-1", payload.TaskID)
	}
	if payload.AgentID != "agent-completed-1" {
		t.Errorf("payload.agentId: want %q, got %q", "agent-completed-1", payload.AgentID)
	}
	if payload.TenantID != tenantID {
		t.Errorf("payload.tenantId: want %v, got %v", tenantID, payload.TenantID)
	}

	// Give a bit more time to make sure the ignored task.enqueued event
	// really never produces a second row (guards against a slow false
	// negative rather than a real pass).
	time.Sleep(300 * time.Millisecond)
	rows = queryJobsForTenant(t, pool, tenantID)
	if len(rows) != 1 {
		t.Errorf("expected task.enqueued to be ignored (still exactly 1 row), got %d", len(rows))
	}
}

type jobRow struct {
	jobType string
	status  string
	payload []byte
}

func queryJobsForTenant(t *testing.T, pool *pgxpool.Pool, tenantID uuid.UUID) []jobRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT job_type, status, payload FROM background_jobs WHERE tenant_id = $1
	`, tenantID)
	if err != nil {
		t.Fatalf("query background_jobs: %v", err)
	}
	defer rows.Close()

	var out []jobRow
	for rows.Next() {
		var r jobRow
		if err := rows.Scan(&r.jobType, &r.status, &r.payload); err != nil {
			t.Fatalf("scan background_jobs row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate background_jobs rows: %v", err)
	}
	return out
}
