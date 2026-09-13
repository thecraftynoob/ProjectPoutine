package registry

import (
	"fmt"
	"sync"
	"testing"
)

// fakeConn is a minimal Connection implementation for tests: it records
// every message sent to it and can be marked closed.
type fakeConn struct {
	mu     sync.Mutex
	sent   [][]byte
	closed bool
}

func (f *fakeConn) Send(message []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return fmt.Errorf("fakeConn: closed")
	}
	f.sent = append(f.sent, message)
	return nil
}

func TestRegisterLookupUnregister(t *testing.T) {
	r := New()
	c := &fakeConn{}

	if _, ok := r.Lookup("tenant-a", "agent-1"); ok {
		t.Fatalf("expected no connection before Register")
	}

	if err := r.Register("tenant-a", "agent-1", c); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, ok := r.Lookup("tenant-a", "agent-1")
	if !ok {
		t.Fatalf("expected connection after Register")
	}
	if got != Connection(c) {
		t.Fatalf("Lookup returned a different connection than registered")
	}

	// Different tenant with the same agentId must not collide.
	if _, ok := r.Lookup("tenant-b", "agent-1"); ok {
		t.Fatalf("expected tenant isolation: tenant-b should not see tenant-a's agent-1")
	}

	r.Unregister("tenant-a", "agent-1")
	if _, ok := r.Lookup("tenant-a", "agent-1"); ok {
		t.Fatalf("expected no connection after Unregister")
	}
}

func TestRegisterValidation(t *testing.T) {
	r := New()
	c := &fakeConn{}

	if err := r.Register("", "agent-1", c); err == nil {
		t.Fatalf("expected error for empty tenantID")
	}
	if err := r.Register("tenant-a", "", c); err == nil {
		t.Fatalf("expected error for empty agentID")
	}
	if err := r.Register("tenant-a", "agent-1", nil); err == nil {
		t.Fatalf("expected error for nil connection")
	}
}

func TestRegisterReplacesExisting(t *testing.T) {
	r := New()
	first := &fakeConn{}
	second := &fakeConn{}

	if err := r.Register("tenant-a", "agent-1", first); err != nil {
		t.Fatalf("Register first: %v", err)
	}
	if err := r.Register("tenant-a", "agent-1", second); err != nil {
		t.Fatalf("Register second: %v", err)
	}

	got, ok := r.Lookup("tenant-a", "agent-1")
	if !ok || got != Connection(second) {
		t.Fatalf("expected second connection to occupy the slot after replacement")
	}
	if r.Len() != 1 {
		t.Fatalf("expected exactly one entry after replacement, got %d", r.Len())
	}
}

// TestUnregisterIfCurrentDoesNotEvictNewerConnection covers the exact
// race Register's doc comment describes: an old connection's disconnect
// handler firing after a new connection has already taken its slot must
// not evict the new one.
func TestUnregisterIfCurrentDoesNotEvictNewerConnection(t *testing.T) {
	r := New()
	oldConn := &fakeConn{}
	newConn := &fakeConn{}

	if err := r.Register("tenant-a", "agent-1", oldConn); err != nil {
		t.Fatalf("Register old: %v", err)
	}
	if err := r.Register("tenant-a", "agent-1", newConn); err != nil {
		t.Fatalf("Register new: %v", err)
	}

	// Simulate the old connection's read loop noticing its own close and
	// trying to unregister itself.
	r.UnregisterIfCurrent("tenant-a", "agent-1", oldConn)

	got, ok := r.Lookup("tenant-a", "agent-1")
	if !ok {
		t.Fatalf("expected the newer connection to remain registered")
	}
	if got != Connection(newConn) {
		t.Fatalf("expected newConn to still occupy the slot")
	}

	// Now the new connection's own disconnect should successfully clear it.
	r.UnregisterIfCurrent("tenant-a", "agent-1", newConn)
	if _, ok := r.Lookup("tenant-a", "agent-1"); ok {
		t.Fatalf("expected slot to be empty after the current connection unregisters itself")
	}
}

// TestConcurrentAccess exercises Register/Lookup/Unregister/UnregisterIfCurrent
// from many goroutines across many agent keys simultaneously. Run with
// -race to catch any data race in the Registry's internal map access.
func TestConcurrentAccess(t *testing.T) {
	r := New()
	const numAgents = 50
	const numIterations = 200

	var wg sync.WaitGroup
	for a := 0; a < numAgents; a++ {
		agentID := fmt.Sprintf("agent-%d", a)
		wg.Add(1)
		go func(agentID string) {
			defer wg.Done()
			for i := 0; i < numIterations; i++ {
				c := &fakeConn{}
				if err := r.Register("tenant-a", agentID, c); err != nil {
					t.Errorf("Register: %v", err)
					return
				}
				if got, ok := r.Lookup("tenant-a", agentID); ok {
					_ = got.Send([]byte("hello"))
				}
				r.UnregisterIfCurrent("tenant-a", agentID, c)
			}
		}(agentID)
	}
	wg.Wait()
}
