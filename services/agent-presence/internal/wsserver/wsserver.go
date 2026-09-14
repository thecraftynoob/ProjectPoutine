// Package wsserver implements Agent & Presence Service's client-facing
// transport: the WebSocket upgrade endpoint an Agent Desktop connects to
// in order to receive real-time, filtered Task Router events
// (architecture doc Section 2.2 / Section 3.2's "Live agent-facing push"
// IPC row; TASK_ROUTER_SPECIFICATION.md Section 3.5/6.3 for the delivery
// contract this service relays).
//
// # Authentication: two paths, one claims-resolution helper
//
// The WebSocket upgrade endpoint accepts EITHER of two credential forms on
// a connection attempt:
//
//  1. A real, signature-verified session JWT (issued by Tenant & Identity
//     Management's Login RPC), carried as a standard HTTP Authorization
//     header on the upgrade request:
//
//     GET /ws
//     Authorization: Bearer <jwt>
//
//     This is the original path (replacing an earlier placeholder scheme
//     that trusted unsigned `?tenant_id=&agent_id=` query parameters --
//     see PROGRESS.md's history for that scheme's known insecurity), kept
//     unchanged and fully working for any client that CAN set a custom
//     header on its WebSocket upgrade request (e.g. a native desktop
//     client, or a server-side test harness).
//
//  2. A short-lived "ws-ticket", carried as a `?ticket=<jwt>` query
//     parameter:
//
//     GET /ws?ticket=<jwt>
//
//     This path exists because a browser's native WebSocket API cannot
//     set custom headers on the upgrade request, so a browser-based Agent
//     Desktop has no way to present the header form above. API Gateway's
//     POST /v1/ws-ticket endpoint (itself requiring a normal, valid
//     session bearer token, like any other authenticated REST route)
//     mints this ticket -- see services/api-gateway/internal/wsticket's
//     doc comment for the full minting design and the short-TTL-not-
//     single-use scoping tradeoff it documents. The ticket carries the
//     SAME `tid`/`sub` claim shape as a session JWT, but is signed by API
//     Gateway's own dedicated ticket-signing key (NOT Tenant & Identity's
//     session-signing key -- deliberately a separate, narrower trust
//     root), so this server verifies it with a SECOND, ticket-scoped
//     Verifier distinct from the one used for path 1.
//
// Both paths resolve to the exact same jwtauth.Claims shape and feed the
// exact same connection-registration logic below (see ResolveClaims,
// factored out so neither path duplicates the tenant/agent-identity
// mapping logic) -- they differ only in which Verifier checks the
// signature and where the token is read from on the HTTP request.
//
// tenant_id is derived exclusively from the verified token's `tid` claim
// (never from client input, per architecture doc Section 1.1 Layer 1) and
// agent_id is derived from the verified token's `sub` (Subject) claim, for
// either path.
//
// # Agent identity: reconciling Task Router's Agent with Tenant &
// Identity's User
//
// Task Router's `Agent` entity (proto/task-router/v1/task_router.proto)
// and Tenant & Identity's `User` entity (proto/tenant-identity/v1/
// tenant_identity.proto) are two separate, unrelated concepts in this
// system today: an Agent is purely a routing-domain profile (status,
// capacity, queues, skills) identified by an operator-chosen
// `agent_id` string with NO reference to any User record, while a User is
// purely an identity/auth-domain principal (username, bcrypt hash, RBAC
// roles) identified by a server-generated UUID. Nothing in either
// service's schema links the two today.
//
// This package resolves that tension the simplest way that is still
// correct for this milestone's scope: it treats the JWT's `sub` claim
// (the authenticated User's user_id) AS the agent_id used to register and
// route WebSocket messages to this connection. This is deliberately a
// pragmatic identity mapping, not a claim that the two entities are "the
// same thing" architecturally:
//   - It requires no schema or proto change to either service to ship
//     this milestone.
//   - It is directionally correct for the real-world shape of this
//     system: a human agent logs into Tenant & Identity as a User, and
//     that login is what proves who they are for the WebSocket
//     connection they then open.
//   - It does mean that, today, an operator provisioning a Task Router
//     `Agent` profile and a Tenant & Identity `User` for the same human
//     must use the SAME string as both the Agent's `agent_id` and the
//     User's `user_id` for task-offer delivery (relay/envelope.go's
//     agentId-based lookup) to reach the right WebSocket connection --
//     there is no automatic reconciliation. A future milestone that
//     formally unifies these two entities (e.g. Task Router's Agent
//     gaining a user_id foreign key, or Tenant & Identity gaining an
//     agent-role-specific profile) would replace this mapping with a
//     real lookup; this is explicitly flagged as follow-up scope, not
//     re-litigated here.
package wsserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/services/agent-presence/internal/registry"
)

// bearerPrefix is the required prefix of the Authorization header value,
// matched case-insensitively per RFC 6750 / HTTP convention -- mirrors
// pkg/tenantctx's gRPC-side bearer parsing so both transports enforce an
// identical wire contract.
const bearerPrefix = "Bearer "

// Authentication failure reasons, all surfaced as HTTP 401 (see
// authenticate/ServeHTTP) -- 401 Unauthorized is the semantically correct
// status for "no/invalid credentials presented," replacing this
// endpoint's earlier placeholder scheme's use of 400 Bad Request for
// malformed query parameters.
var (
	errMissingCredential   = errors.New("missing Authorization header or ?ticket= query parameter")
	errMalformedAuthHeader = errors.New(`Authorization header must be "Bearer <token>"`)
	errMissingSubjectClaim = errors.New("token has no subject (sub) claim to use as agent_id")
)

// invalidTokenError wraps a JWT verification failure (bad signature,
// expired, malformed, ...) with a stable, non-leaky prefix for the HTTP
// error body.
type invalidTokenError struct{ err error }

func (e invalidTokenError) Error() string { return fmt.Sprintf("invalid token: %v", e.err) }
func (e invalidTokenError) Unwrap() error { return e.err }

// Server is an http.Handler serving the WebSocket upgrade endpoint.
type Server struct {
	registry *registry.Registry
	// verifier validates the Authorization header path (session JWTs
	// issued by Tenant & Identity's Login RPC).
	verifier *jwtauth.Verifier
	// ticketVerifier validates the ?ticket= query parameter path
	// (short-lived tickets minted by API Gateway's internal/wsticket).
	// May be nil: a deployment that has not configured API Gateway's
	// ticket public key simply never accepts the ticket path, and every
	// ?ticket= attempt is rejected the same as a missing credential --
	// this keeps the header path fully functional with zero new
	// required configuration, per this package's doc comment ("both
	// should keep working").
	ticketVerifier *jwtauth.Verifier
	logger         *slog.Logger

	mu   sync.Mutex
	conn map[*wsConnection]struct{}
}

// New constructs a Server backed by reg (the process's connection
// Registry, shared with the NATS relay so delivered events reach
// connections registered here), verifier (validates the Authorization
// header path) and ticketVerifier (validates the ?ticket= query parameter
// path -- see package doc comment; nil disables the ticket path only).
// verifier must be non-nil -- this is a security control and fails closed
// at construction time (see cmd/main.go, which refuses to start rather
// than pass a nil verifier here).
func New(reg *registry.Registry, verifier *jwtauth.Verifier, ticketVerifier *jwtauth.Verifier, logger *slog.Logger) *Server {
	if verifier == nil {
		panic("wsserver: New called with a nil verifier -- refusing to serve unauthenticated WebSocket connections")
	}
	return &Server{
		registry:       reg,
		verifier:       verifier,
		ticketVerifier: ticketVerifier,
		logger:         logger,
		conn:           make(map[*wsConnection]struct{}),
	}
}

// ServeHTTP implements http.Handler. Mount this at "/ws".
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/ws" {
		http.NotFound(w, r)
		return
	}

	claims, err := s.authenticate(r)
	if err != nil {
		http.Error(w, "wsserver: "+err.Error(), http.StatusUnauthorized)
		return
	}
	tenantID := claims.TenantID.String()
	agentID := claims.Subject

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		// websocket.Accept has already written an appropriate error
		// response if the upgrade itself failed.
		s.logger.Warn("wsserver: upgrade failed", slog.Any("error", err))
		return
	}

	wc := &wsConnection{
		conn:     c,
		tenantID: tenantID,
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

// TicketQueryParam is the query parameter name the ?ticket= auth path
// reads from -- must match services/api-gateway/internal/wsproxy's
// TicketQueryParam exactly, since that package forwards a client's ticket
// to this server under the same name.
const TicketQueryParam = "ticket"

// authenticate resolves the credential presented on an upgrade request --
// EITHER an Authorization header (session JWT, verified against s.verifier)
// OR a ?ticket= query parameter (ws-ticket, verified against
// s.ticketVerifier) -- and returns the verified claims. The header path is
// tried first (if an Authorization header is present at all, only that
// path is attempted -- a request is not allowed to "fall back" from a
// malformed/invalid header to a ticket, which would blur which credential
// actually authenticated the connection); the ticket path is tried only
// when no Authorization header is present. Returns an error (suitable for
// direct inclusion in an HTTP 401 body) on any missing/malformed/invalid
// credential from either path.
func (s *Server) authenticate(r *http.Request) (jwtauth.Claims, error) {
	if raw := r.Header.Get("Authorization"); raw != "" {
		if len(raw) <= len(bearerPrefix) || !strings.EqualFold(raw[:len(bearerPrefix)], bearerPrefix) {
			return jwtauth.Claims{}, errMalformedAuthHeader
		}
		return ResolveClaims(s.verifier, raw[len(bearerPrefix):])
	}

	if ticket := r.URL.Query().Get(TicketQueryParam); ticket != "" {
		if s.ticketVerifier == nil {
			return jwtauth.Claims{}, errMissingCredential
		}
		return ResolveClaims(s.ticketVerifier, ticket)
	}

	return jwtauth.Claims{}, errMissingCredential
}

// ResolveClaims verifies token against verifier and applies this
// service's shared claims-to-identity mapping rule (tenant_id <- `tid`,
// agent_id <- `sub`, and `sub` must be non-empty -- see package doc
// comment's "Agent identity" section). Both the Authorization-header path
// and the ?ticket= path above call this so neither duplicates the mapping
// logic or its validation rule, and so services/api-gateway's wsproxy
// package can perform the identical resolution when it validates a ticket
// before dialing upstream (fail-fast client-side check; Agent Presence
// itself is still the authority that re-verifies independently on the
// upstream leg -- Layer 2 defense in depth).
func ResolveClaims(verifier *jwtauth.Verifier, token string) (jwtauth.Claims, error) {
	claims, err := verifier.Verify(token)
	if err != nil {
		return jwtauth.Claims{}, invalidTokenError{err}
	}
	if claims.Subject == "" {
		return jwtauth.Claims{}, errMissingSubjectClaim
	}
	return claims, nil
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
