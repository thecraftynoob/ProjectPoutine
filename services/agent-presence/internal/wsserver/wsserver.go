// Package wsserver implements Agent & Presence Service's client-facing
// transport: the WebSocket upgrade endpoint an Agent Desktop connects to
// in order to receive real-time, filtered Task Router events
// (architecture doc Section 2.2 / Section 3.2's "Live agent-facing push"
// IPC row; TASK_ROUTER_SPECIFICATION.md Section 3.5/6.3 for the delivery
// contract this service relays).
//
// ============================================================================
// AUTH IS A PLACEHOLDER. THIS IS NOT REAL AUTHENTICATION.
// ============================================================================
// The WebSocket upgrade endpoint in this package identifies the caller
// using two UNSIGNED, UNVERIFIED query parameters:
//
//	GET /ws?tenant_id=<uuid>&agent_id=<string>
//
// tenant_id must parse as a UUID and agent_id must be non-empty, or the
// upgrade is rejected with HTTP 400 -- but neither value is authenticated
// in any way. Any caller who can reach this endpoint can claim to be any
// agent in any tenant simply by setting these query parameters. This is
// acceptable ONLY because Tenant & Identity Management (architecture doc
// Section 2.2) does not exist yet in this platform, so there is no JWT
// issuer to validate against.
//
// This MUST be replaced, before any real deployment, with real
// authentication: an Authorization header bearing a JWT with a `tid`
// claim (tenant ID) and an agent/subject claim, validated the way
// architecture doc Section 1.1 describes for the API Gateway (Layer 1
// enforcement) -- and this service, as an internal service, should also
// apply Layer 2 re-validation per that same section rather than trusting
// the Gateway blindly. Do not treat the current query-param scheme as
// "auth that will be hardened later" -- it provides no security
// whatsoever today.
// ============================================================================
package wsserver

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/thecraftynoob/ProjectPoutine/services/agent-presence/internal/registry"
)

// Server is an http.Handler serving the WebSocket upgrade endpoint.
type Server struct {
	registry *registry.Registry
	logger   *slog.Logger

	mu   sync.Mutex
	conn map[*wsConnection]struct{}
}

// New constructs a Server backed by reg (the process's connection
// Registry, shared with the NATS relay so delivered events reach
// connections registered here).
func New(reg *registry.Registry, logger *slog.Logger) *Server {
	return &Server{
		registry: reg,
		logger:   logger,
		conn:     make(map[*wsConnection]struct{}),
	}
}

// ServeHTTP implements http.Handler. Mount this at "/ws".
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/ws" {
		http.NotFound(w, r)
		return
	}

	tenantIDRaw := r.URL.Query().Get("tenant_id")
	agentID := r.URL.Query().Get("agent_id")

	tenantID, err := uuid.Parse(tenantIDRaw)
	if err != nil {
		http.Error(w, "wsserver: tenant_id query parameter must be a valid UUID", http.StatusBadRequest)
		return
	}
	if agentID == "" {
		http.Error(w, "wsserver: agent_id query parameter must be non-empty", http.StatusBadRequest)
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		// websocket.Accept has already written an appropriate error
		// response if the upgrade itself failed.
		s.logger.Warn("wsserver: upgrade failed", slog.Any("error", err))
		return
	}

	wc := &wsConnection{
		conn:     c,
		tenantID: tenantID.String(),
		agentID:  agentID,
	}

	if err := s.registry.Register(wc.tenantID, wc.agentID, wc); err != nil {
		s.logger.Error("wsserver: failed to register connection", slog.Any("error", err))
		_ = c.Close(websocket.StatusInternalError, "registration failed")
		return
	}

	s.trackConn(wc, true)
	s.logger.Info("wsserver: agent connected",
		slog.String("tenant_id", wc.tenantID),
		slog.String("agent_id", wc.agentID))

	defer func() {
		s.registry.UnregisterIfCurrent(wc.tenantID, wc.agentID, wc)
		s.trackConn(wc, false)
		s.logger.Info("wsserver: agent disconnected",
			slog.String("tenant_id", wc.tenantID),
			slog.String("agent_id", wc.agentID))
	}()

	// Read loop: this service has no client->server message contract
	// (spec Section 3.5: "A persistent connection; no client->server
	// message contract") -- the only purpose of reading is to detect
	// disconnects (close frame, network error) promptly so the
	// connection can be unregistered. Any inbound data frame is
	// discarded.
	readLoop(r.Context(), c)
}

// trackConn adds/removes wc from the server's bookkeeping set, used only
// by Shutdown to close every live connection.
func (s *Server) trackConn(wc *wsConnection, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if add {
		s.conn[wc] = struct{}{}
	} else {
		delete(s.conn, wc)
	}
}

// Shutdown closes every currently-open WebSocket connection with a
// "going away" close frame, so graceful shutdown (SIGINT/SIGTERM)
// drains connections cleanly rather than dropping them silently. It does
// not wait for client acknowledgement beyond what websocket.Close itself
// does.
func (s *Server) Shutdown(_ context.Context) {
	s.mu.Lock()
	conns := make([]*wsConnection, 0, len(s.conn))
	for c := range s.conn {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, c := range conns {
		_ = c.conn.Close(websocket.StatusServiceRestart, "server shutting down")
	}
}

// readLoop blocks reading (and discarding) inbound messages until the
// connection closes or ctx is done, purely to detect disconnects.
func readLoop(ctx context.Context, c *websocket.Conn) {
	for {
		_, _, err := c.Read(ctx)
		if err != nil {
			// Any error (client close, network failure, context
			// cancellation) means the connection is done. Best-effort
			// close in case it wasn't already; ignore the result.
			_ = c.Close(websocket.StatusNormalClosure, "")
			return
		}
	}
}
