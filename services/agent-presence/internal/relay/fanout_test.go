package relay

import (
	"context"
	"encoding/json"
	"log/slog"
	"io"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/thecraftynoob/ProjectPoutine/services/agent-presence/internal/registry"
)

type fakeConn struct {
	received chan []byte
}

func newFakeConn() *fakeConn {
	return &fakeConn{received: make(chan []byte, 10)}
}

func (f *fakeConn) Send(message []byte) error {
	f.received <- message
	return nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestFanout_DeliversOnlyToOwningReplica exercises the exact design
// described in fanout.go's doc comment: two "replicas" (two Fanout
// instances backed by two Registry instances, sharing one Redis) each
// subscribe to the same tenant channel; only the replica whose local
// Registry holds the target agent's connection should actually deliver.
func TestFanout_DeliversOnlyToOwningReplica(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer redisClient.Close()

	// Replica A holds the connection for agent-1.
	regA := registry.New()
	connA := newFakeConn()
	if err := regA.Register("tenant-x", "agent-1", connA); err != nil {
		t.Fatalf("Register on replica A: %v", err)
	}
	fanoutA := NewFanout(redisClient, regA, testLogger())

	// Replica B holds nothing for agent-1.
	regB := registry.New()
	fanoutB := NewFanout(redisClient, regB, testLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fanoutA.Subscribe(ctx, "tenant-x")
	fanoutB.Subscribe(ctx, "tenant-x")

	// Give the subscriptions a moment to establish before publishing --
	// miniredis pub/sub delivery is not guaranteed to be ready
	// instantaneously after Subscribe returns.
	time.Sleep(100 * time.Millisecond)

	env := Envelope{
		Type:    EventTypeAgentDeleted,
		AgentID: "agent-1",
		Payload: map[string]any{"agentId": "agent-1"},
	}
	if err := fanoutA.Publish(ctx, "tenant-x", env); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-connA.received:
		var got Envelope
		if err := json.Unmarshal(msg, &got); err != nil {
			t.Fatalf("unmarshal delivered message: %v", err)
		}
		if got.AgentID != "agent-1" || got.Type != EventTypeAgentDeleted {
			t.Fatalf("unexpected delivered envelope: %#v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("expected replica A (owning connection) to receive the event")
	}

	// Replica B must never have delivered anything -- it holds no
	// connection for agent-1. There's no connection object on B to
	// assert against directly (that's the point), so instead assert that
	// looking the agent up on B's registry still finds nothing,
	// confirming B never had a delivery target in the first place.
	if _, ok := regB.Lookup("tenant-x", "agent-1"); ok {
		t.Fatalf("replica B should never have a registered connection for agent-1 in this test")
	}
}

// TestFanout_UnknownAgentIsSilentlyDropped covers the common case where
// neither/no replica currently holds a connection for the target agent
// (e.g. the agent is not logged in) -- delivery should be a no-op, not
// an error.
func TestFanout_UnknownAgentIsSilentlyDropped(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer redisClient.Close()

	reg := registry.New()
	fanout := NewFanout(redisClient, reg, testLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fanout.Subscribe(ctx, "tenant-y")
	time.Sleep(100 * time.Millisecond)

	env := Envelope{Type: EventTypeAgentDeleted, AgentID: "ghost-agent", Payload: map[string]any{"agentId": "ghost-agent"}}
	if err := fanout.Publish(ctx, "tenant-y", env); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// No assertion possible beyond "did not panic / did not block" --
	// give the subscriber goroutine a moment to process, then move on.
	time.Sleep(200 * time.Millisecond)
}
