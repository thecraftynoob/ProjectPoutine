package eventbus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/types/known/structpb"
)

// startTestNATS starts an in-process NATS server with JetStream enabled
// on a random free port, using a temp directory for its JetStream store.
// This proves the fix against a real NATS+JetStream server, not just
// against the assumption that documented consumer-group semantics hold.
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

// TestSubscribeEphemeral_MultipleReplicasEachReceiveEveryEvent is the
// direct regression test for the cross-replica fan-out bug: two
// independent Consumer/Client instances (simulating two replicas of
// agent-presence) must each independently receive their OWN copy of
// every published event on a shared stream, proving they are NOT
// competing consumers within one shared group.
//
// This is the crux of the fix -- see
// services/agent-presence/internal/relay/consumer.go's Start doc comment
// for the full incident description: the previous implementation used
// Subscribe with a deterministic, cross-replica-identical durable
// consumer name, which JetStream treats as ONE shared consumer group, so
// only one replica would ever receive any given message.
func TestSubscribeEphemeral_MultipleReplicasEachReceiveEveryEvent(t *testing.T) {
	srv := startTestNATS(t)
	url := srv.ClientURL()

	const streamName = "TEST_MULTI_REPLICA_STREAM"
	const subjectFilter = "tenant.*.reservation.created"
	tenantID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	// Publisher client: creates the stream and publishes one event.
	pub, err := Connect(url)
	if err != nil {
		t.Fatalf("Connect (publisher): %v", err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pub.EnsureStream(ctx, streamName, []string{"tenant.*.reservation.>"}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	// Two independent "replica" clients, each with its own connection --
	// closer to the real multi-process scenario than sharing one Client.
	replicaA, err := Connect(url)
	if err != nil {
		t.Fatalf("Connect (replica A): %v", err)
	}
	defer replicaA.Close()

	replicaB, err := Connect(url)
	if err != nil {
		t.Fatalf("Connect (replica B): %v", err)
	}
	defer replicaB.Close()

	var (
		mu        sync.Mutex
		receivedA [][]byte
		receivedB [][]byte
	)

	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	if err := replicaA.SubscribeEphemeral(subCtx, streamName, subjectFilter, time.Minute, func(msg jetstream.Msg) error {
		mu.Lock()
		receivedA = append(receivedA, msg.Data())
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("SubscribeEphemeral (replica A): %v", err)
	}

	if err := replicaB.SubscribeEphemeral(subCtx, streamName, subjectFilter, time.Minute, func(msg jetstream.Msg) error {
		mu.Lock()
		receivedB = append(receivedB, msg.Data())
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("SubscribeEphemeral (replica B): %v", err)
	}

	// Give both ephemeral consumers a moment to be fully established
	// before publishing, so this isn't racing consumer creation.
	time.Sleep(200 * time.Millisecond)

	payload, err := structpb.NewStruct(map[string]any{
		"reservationId": "res-1",
		"taskId":        "task-1",
		"agentId":       "agent-1",
		"expiresAt":     "2026-09-13T00:00:30Z",
	})
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	if err := pub.PublishEvent(ctx, tenantID, "reservation", "created", payload); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	// Both replicas must independently receive the SAME single published
	// event -- this is the crux of the fix. With the old buggy behavior
	// (a shared durable consumer name), only one of the two would ever
	// receive it.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		gotA := len(receivedA)
		gotB := len(receivedB)
		mu.Unlock()
		if gotA >= 1 && gotB >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(receivedA) != 1 {
		t.Errorf("replica A: got %d messages, want exactly 1 (each replica must receive its own independent copy)", len(receivedA))
	}
	if len(receivedB) != 1 {
		t.Errorf("replica B: got %d messages, want exactly 1 (each replica must receive its own independent copy)", len(receivedB))
	}
}

// TestSubscribeEphemeral_DoesNotCollideWithDurableSubscribe guards
// against a regression where SubscribeEphemeral and Subscribe might
// accidentally be wired to share consumer state: a durable Subscribe
// consumer (a competing-consumer group by design, e.g. for horizontally
// scaled worker pools) and any number of SubscribeEphemeral calls on the
// same stream/filter must each receive their own full copy, independent
// of each other.
func TestSubscribeEphemeral_DoesNotCollideWithDurableSubscribe(t *testing.T) {
	srv := startTestNATS(t)
	url := srv.ClientURL()

	const streamName = "TEST_MIXED_CONSUMER_STREAM"
	const subjectFilter = "tenant.*.agent.deleted"
	tenantID := uuid.MustParse("44444444-4444-4444-4444-444444444444")

	pub, err := Connect(url)
	if err != nil {
		t.Fatalf("Connect (publisher): %v", err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pub.EnsureStream(ctx, streamName, []string{"tenant.*.agent.>"}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	durableClient, err := Connect(url)
	if err != nil {
		t.Fatalf("Connect (durable): %v", err)
	}
	defer durableClient.Close()

	ephemeralClient, err := Connect(url)
	if err != nil {
		t.Fatalf("Connect (ephemeral): %v", err)
	}
	defer ephemeralClient.Close()

	var mu sync.Mutex
	var durableCount, ephemeralCount int

	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	if err := durableClient.Subscribe(subCtx, streamName, "test-durable-consumer", subjectFilter, func(msg jetstream.Msg) error {
		mu.Lock()
		durableCount++
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := ephemeralClient.SubscribeEphemeral(subCtx, streamName, subjectFilter, time.Minute, func(msg jetstream.Msg) error {
		mu.Lock()
		ephemeralCount++
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("SubscribeEphemeral: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	payload, err := structpb.NewStruct(map[string]any{"agentId": "agent-9"})
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	if err := pub.PublishEvent(ctx, tenantID, "agent", "deleted", payload); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		d, e := durableCount, ephemeralCount
		mu.Unlock()
		if d >= 1 && e >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if durableCount != 1 {
		t.Errorf("durable consumer: got %d messages, want 1", durableCount)
	}
	if ephemeralCount != 1 {
		t.Errorf("ephemeral consumer: got %d messages, want 1", ephemeralCount)
	}
}
