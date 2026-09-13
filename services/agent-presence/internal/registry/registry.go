// Package registry implements Agent & Presence Service's core
// responsibility (architecture doc Section 2.2): "holds the live
// WebSocket connection to every logged-in Agent Desktop". It is
// deliberately narrow in scope -- this is an in-memory map of
// (tenantID, agentID) -> the currently-active *Connection* on THIS
// process/replica, nothing more. It has no opinion on agent routing
// eligibility, status, capacity, or queues -- Task Router remains the
// sole owner of that state (see the package doc comment in
// services/agent-presence/internal/doc.go for the full boundary
// rationale).
//
// Multi-replica fan-out: this Registry only ever knows about connections
// held by its own process. Delivering an event to an agent whose
// WebSocket happens to be held by a *different* replica is handled one
// layer up, by internal/relay's Redis pub/sub fan-out -- every replica
// subscribes to the same per-tenant channel and each one independently
// checks its own local Registry via Lookup, delivering only if it
// actually owns the target connection. That design keeps this package
// simple: it is pure in-memory bookkeeping, safe for concurrent use, and
// has no Redis dependency of its own.
package registry

import (
	"fmt"
	"sync"
)

// Connection is the minimal surface the registry needs from a live
// WebSocket connection: a way to push a message to it and a way to know
// it's gone. wsserver's connection wrapper implements this; tests can
// supply a fake.
type Connection interface {
	// Send delivers a single message (already-encoded, e.g. JSON bytes)
	// to the connection. Implementations should be safe to call from any
	// goroutine and should return a non-nil error if the connection can
	// no longer accept writes (closed/broken).
	Send(message []byte) error
}

// key identifies one agent's connection slot, scoped by tenant so two
// tenants can never collide on the same agentId string.
type key struct {
	tenantID string
	agentID  string
}

// Registry is the in-memory, per-replica connection table. Safe for
// concurrent use by multiple goroutines (the WebSocket accept loop
// registering/unregistering connections, and the relay consumer looking
// connections up to deliver events, run concurrently).
type Registry struct {
	mu    sync.RWMutex
	conns map[key]Connection
}

// New constructs an empty Registry.
func New() *Registry {
	return &Registry{conns: make(map[key]Connection)}
}

// Register records conn as the active connection for (tenantID, agentID)
// on this replica. If a connection is already registered for the same
// agent, it is replaced -- this deliberately matches the "no
// reconnection/session state preserved" philosophy carried over from
// Task Router spec Section 5.6: a second connection for the same agent
// (e.g. a reconnect racing a slow-to-close old socket) simply wins the
// registry slot; the old connection is left to fail on its next write
// and get cleaned up by its own read-loop's Unregister call, which will
// be a harmless no-op against the map at that point (see Unregister).
//
// Returns an error only if tenantID or agentID is empty, which would
// indicate a caller bug (the wsserver upgrade handler is expected to
// have already validated both per the placeholder-auth contract).
func (r *Registry) Register(tenantID, agentID string, conn Connection) error {
	if tenantID == "" || agentID == "" {
		return fmt.Errorf("registry: tenantID and agentID must be non-empty")
	}
	if conn == nil {
		return fmt.Errorf("registry: conn must not be nil")
	}
	k := key{tenantID: tenantID, agentID: agentID}
	r.mu.Lock()
	r.conns[k] = conn
	r.mu.Unlock()
	return nil
}

// Unregister removes the connection recorded for (tenantID, agentID), if
// any. It is safe to call this with a stale/already-replaced connection
// in mind (see Register's doc comment) because Unregister only removes
// the map entry -- it does not attempt to identify "was this call's
// caller the one currently registered," since the registry does not track
// per-connection identity beyond "the current occupant of this slot."
// Callers that need exactly-this-connection semantics should guard with
// UnregisterIfCurrent instead.
func (r *Registry) Unregister(tenantID, agentID string) {
	k := key{tenantID: tenantID, agentID: agentID}
	r.mu.Lock()
	delete(r.conns, k)
	r.mu.Unlock()
}

// UnregisterIfCurrent removes the (tenantID, agentID) entry only if the
// connection currently registered is the exact same one passed in
// (compared by identity). This is what wsserver's read loop should call
// on disconnect, so that an old, already-superseded connection's
// disconnect handler can never accidentally evict a newer connection
// that has since taken its slot (see Register's doc comment on
// replacement semantics).
func (r *Registry) UnregisterIfCurrent(tenantID, agentID string, conn Connection) {
	k := key{tenantID: tenantID, agentID: agentID}
	r.mu.Lock()
	if current, ok := r.conns[k]; ok && current == conn {
		delete(r.conns, k)
	}
	r.mu.Unlock()
}

// Lookup returns the connection currently registered for (tenantID,
// agentID) on this replica, and whether one was found.
func (r *Registry) Lookup(tenantID, agentID string) (Connection, bool) {
	k := key{tenantID: tenantID, agentID: agentID}
	r.mu.RLock()
	conn, ok := r.conns[k]
	r.mu.RUnlock()
	return conn, ok
}

// Len returns the number of connections currently registered on this
// replica. Exposed mainly for tests/observability.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.conns)
}
