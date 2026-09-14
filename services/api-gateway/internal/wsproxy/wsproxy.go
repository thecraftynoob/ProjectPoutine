// Package wsproxy implements API Gateway's WebSocket upgrade proxying for
// Agent Desktop (architecture doc Section 2.2: "WebSocket upgrade proxying
// for Agent Desktop"), fronting Agent & Presence Service's "/ws" endpoint.
//
// # Design choice: proxy, not redirect
//
// Two designs were considered for how a browser client reaches Agent
// Presence's WebSocket endpoint once it holds a ticket (see
// internal/wsticket):
//
//  1. Redirect/direct-connect: API Gateway's POST /v1/ws-ticket response
//     tells the client Agent Presence's own address, and the client opens
//     the WebSocket directly against it, bypassing the gateway entirely.
//  2. Proxy: the client opens the WebSocket against API Gateway itself
//     (wss://gateway/ws?ticket=...), and the gateway transparently
//     forwards bytes to/from a second WebSocket connection it opens to
//     Agent Presence.
//
// This package implements (2), proxying, chosen over the simpler
// redirect for two reasons:
//   - It is what the architecture doc's Section 2.2 API Gateway row
//     literally specifies ("WebSocket upgrade proxying for Agent
//     Desktop") and what Section 2.1's service map draws (APS <-->
//     WebSocket <--> GW as the edge, not APS reachable directly from
//     outside the cluster) -- API Gateway is meant to be the SINGLE
//     external entry point (Section 2.2's own summary row), and a
//     redirect design would leave Agent Presence as a second externally
//     reachable service, undermining that "single entry point" property
//     for real production topology (a real deployment would not even
//     expose Agent Presence's Service outside the cluster at all).
//   - It keeps API Gateway's REST and WebSocket surfaces consistent: the
//     same host/port a client already trusts (and, in a future milestone,
//     the same TLS termination point -- architecture doc Section 1.1
//     "every external request terminates at an API Gateway / BFF") is
//     where every kind of traffic lands.
//
// The tradeoff, honestly noted: proxying means API Gateway now holds an
// open connection per active WebSocket client (it is no longer purely
// "stateless request-in, request-out" for the lifetime of that
// connection) and adds one extra network hop plus a small amount of
// byte-copying overhead per message versus a direct connection. For this
// system's scale (a home-lab/dev-cluster CCaaS platform, not a
// high-frequency trading feed) this is a clearly acceptable and standard
// reverse-proxy tradeoff, not a scaling concern worth re-litigating this
// milestone.
//
// # How it works
//
// Handler validates the ticket (?ticket=<jwt>) against API Gateway's own
// ticket verifier (internal/wsticket's signing key), then:
//  1. Upgrades the inbound client connection.
//  2. Dials Agent Presence's own "/ws" endpoint as a WebSocket client,
//     forwarding the SAME ticket as a query parameter (Agent Presence
//     re-verifies it independently -- Layer 2 defense in depth, the same
//     principle as the REST bearer-token forwarding in internal/gwauth).
//  3. Pumps frames in both directions until either side closes.
package wsproxy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
)

// TicketQueryParam is the query parameter a client supplies its ws-ticket
// under, both on the inbound connection to API Gateway and the outbound
// connection this package opens to Agent Presence -- must match
// services/agent-presence/internal/wsserver's expected parameter name
// exactly.
const TicketQueryParam = "ticket"

// Handler proxies WebSocket connections to Agent & Presence Service.
type Handler struct {
	// AgentPresenceWSURL is the ws:// base URL of Agent Presence's
	// WebSocket endpoint (e.g. "ws://agent-presence-svc:8085/ws"),
	// sourced from an env var -- never hardcoded, per CLAUDE.md Rule 2.
	AgentPresenceWSURL string
	// TicketVerifier validates the ?ticket=<jwt> query parameter using
	// API Gateway's own ticket-signing public key (see internal/wsticket).
	TicketVerifier *jwtauth.Verifier
	Logger         *slog.Logger

	mu    sync.Mutex
	conns map[*proxiedConn]struct{}
}

type proxiedConn struct {
	client   *websocket.Conn
	upstream *websocket.Conn
}

func (h *Handler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

// ServeHTTP implements http.Handler. Mount this at "/ws".
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ticket := r.URL.Query().Get(TicketQueryParam)
	if ticket == "" {
		http.Error(w, `{"error":"wsproxy: missing ticket query parameter"}`, http.StatusUnauthorized)
		return
	}
	// Validate here too (fail fast with a clear error before ever
	// upgrading the client connection) even though Agent Presence
	// re-validates independently on the upstream leg -- see package doc
	// comment step 2.
	if _, err := h.TicketVerifier.Verify(ticket); err != nil {
		http.Error(w, `{"error":"wsproxy: invalid or expired ticket"}`, http.StatusUnauthorized)
		return
	}

	upstreamURL, err := h.upstreamURL(ticket)
	if err != nil {
		h.logger().Error("wsproxy: bad upstream URL", slog.Any("error", err))
		http.Error(w, `{"error":"wsproxy: misconfigured upstream"}`, http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	upstream, _, err := websocket.Dial(ctx, upstreamURL, nil)
	if err != nil {
		h.logger().Error("wsproxy: failed to dial agent-presence", slog.Any("error", err))
		http.Error(w, `{"error":"wsproxy: upstream unavailable"}`, http.StatusBadGateway)
		return
	}

	client, err := websocket.Accept(w, r, nil)
	if err != nil {
		h.logger().Warn("wsproxy: client upgrade failed", slog.Any("error", err))
		_ = upstream.Close(websocket.StatusInternalError, "client upgrade failed")
		return
	}

	pc := &proxiedConn{client: client, upstream: upstream}
	h.track(pc, true)
	defer h.track(pc, false)

	h.pump(ctx, pc)
}

// upstreamURL builds Agent Presence's WebSocket URL with the same ticket
// forwarded as its query parameter, translating a ws(s):// base into the
// scheme coder/websocket.Dial expects (it accepts ws/wss directly).
func (h *Handler) upstreamURL(ticket string) (string, error) {
	base := h.AgentPresenceWSURL
	if base == "" {
		return "", fmt.Errorf("wsproxy: AgentPresenceWSURL is not configured")
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("wsproxy: parse AgentPresenceWSURL: %w", err)
	}
	q := u.Query()
	q.Set(TicketQueryParam, ticket)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// pump copies frames in both directions between the client and upstream
// connections until either side closes or errors, then closes both.
func (h *Handler) pump(ctx context.Context, pc *proxiedConn) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, 2)
	go copyLoop(ctx, pc.upstream, pc.client, errs) // client -> upstream
	go copyLoop(ctx, pc.client, pc.upstream, errs) // upstream -> client

	err := <-errs
	closeReason := websocket.StatusNormalClosure
	msg := ""
	if err != nil && !isNormalClose(err) {
		closeReason = websocket.StatusInternalError
		msg = "proxy error"
		h.logger().Warn("wsproxy: connection ended with error", slog.Any("error", err))
	}
	_ = pc.client.Close(closeReason, msg)
	_ = pc.upstream.Close(closeReason, msg)
}

// copyLoop reads frames from src and writes them to dst until an error or
// ctx cancellation. Direction-agnostic: called once per direction with src/
// dst swapped.
func copyLoop(ctx context.Context, dst, src *websocket.Conn, errs chan<- error) {
	for {
		typ, data, err := src.Read(ctx)
		if err != nil {
			errs <- err
			return
		}
		if err := dst.Write(ctx, typ, data); err != nil {
			errs <- err
			return
		}
	}
}

// isNormalClose reports whether err represents an expected connection
// close (client disconnect, context cancellation) rather than a genuine
// proxy failure, so Shutdown/normal teardown isn't logged as a warning.
func isNormalClose(err error) bool {
	if err == nil {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "context canceled") ||
		websocket.CloseStatus(err) != -1
}

func (h *Handler) track(pc *proxiedConn, add bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conns == nil {
		h.conns = make(map[*proxiedConn]struct{})
	}
	if add {
		h.conns[pc] = struct{}{}
	} else {
		delete(h.conns, pc)
	}
}

// Shutdown closes every currently-proxied connection pair, mirroring
// wsserver.Server.Shutdown's graceful-drain behavior on SIGINT/SIGTERM.
func (h *Handler) Shutdown(_ context.Context) {
	h.mu.Lock()
	conns := make([]*proxiedConn, 0, len(h.conns))
	for c := range h.conns {
		conns = append(conns, c)
	}
	h.mu.Unlock()

	for _, c := range conns {
		_ = c.client.Close(websocket.StatusServiceRestart, "server shutting down")
		_ = c.upstream.Close(websocket.StatusServiceRestart, "server shutting down")
	}
}
