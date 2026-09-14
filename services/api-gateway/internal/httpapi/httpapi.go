// Package httpapi wires API Gateway's HTTP surface: a grpc-gateway
// runtime.ServeMux translating REST/JSON to Task Router's and Tenant &
// Identity's internal gRPC contracts, API Gateway's own POST /v1/ws-ticket
// endpoint, and the /ws WebSocket proxy to Agent & Presence Service.
//
// See services/api-gateway/internal/gwauth for the bearer-JWT validation
// middleware wrapping all of this (except the exempt bootstrapping
// routes), and services/api-gateway/internal/wsticket /
// services/api-gateway/internal/wsproxy for the WebSocket-ticket flow's
// two halves.
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	tenantidentityv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/tenant-identity/v1"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/services/api-gateway/internal/gwauth"
	"github.com/thecraftynoob/ProjectPoutine/services/api-gateway/internal/wsproxy"
	"github.com/thecraftynoob/ProjectPoutine/services/api-gateway/internal/wsticket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config carries everything httpapi.New needs to build the handler.
type Config struct {
	// TaskRouterGRPCAddr and TenantIdentityGRPCAddr are the in-cluster
	// Service DNS names of the two backend gRPC servers this gateway
	// fronts (e.g. "task-router-svc:50054") -- sourced from env vars by
	// cmd/main.go, never hardcoded (CLAUDE.md Rule 2).
	TaskRouterGRPCAddr     string
	TenantIdentityGRPCAddr string

	// Verifier validates end-user session JWTs (Tenant & Identity's
	// signing public key) on every non-exempt REST route.
	Verifier *jwtauth.Verifier
	// TicketMinter mints short-lived WebSocket tickets for POST
	// /v1/ws-ticket.
	TicketMinter *wsticket.Minter
	// AgentPresenceWSURL is Agent Presence's own WebSocket URL the /ws
	// proxy dials upstream (e.g. "ws://agent-presence-svc:8085/ws").
	AgentPresenceWSURL string

	Logger *slog.Logger
}

// Handler is API Gateway's fully-assembled HTTP handler, plus the
// WebSocket proxy's own Shutdown hook (for graceful drain, mirroring
// wsserver.Server.Shutdown's role in agent-presence's cmd/main.go).
type Handler struct {
	http.Handler
	WSProxy *wsproxy.Handler
}

// New dials both backend gRPC services and assembles the full HTTP
// handler: grpc-gateway's mux for REST routes, the ws-ticket minting
// endpoint, and the WebSocket proxy -- all wrapped in gwauth's bearer-JWT
// middleware except the routes gwauth.IsExempt carves out.
//
// The two backend dials use insecure.NewCredentials() (no transport TLS),
// consistent with every other gRPC connection in this repo today -- see
// PROGRESS.md's "Known deferred items": no service-to-service call in
// this repo uses transport TLS yet, the bearer token is the security
// boundary, not the transport. Real mTLS is documented future work, not
// re-litigated here.
func New(ctx context.Context, cfg Config) (*Handler, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	taskRouterConn, err := grpc.NewClient(cfg.TaskRouterGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("httpapi: dial task-router at %q: %w", cfg.TaskRouterGRPCAddr, err)
	}

	tenantIdentityConn, err := grpc.NewClient(cfg.TenantIdentityGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("httpapi: dial tenant-identity at %q: %w", cfg.TenantIdentityGRPCAddr, err)
	}

	mux := runtime.NewServeMux()
	if err := taskrouterv1.RegisterTaskRouterServiceHandler(ctx, mux, taskRouterConn); err != nil {
		return nil, fmt.Errorf("httpapi: register TaskRouterService gateway handlers: %w", err)
	}
	if err := taskrouterv1.RegisterTaskRouterAdminServiceHandler(ctx, mux, taskRouterConn); err != nil {
		return nil, fmt.Errorf("httpapi: register TaskRouterAdminService gateway handlers: %w", err)
	}
	if err := tenantidentityv1.RegisterTenantServiceHandler(ctx, mux, tenantIdentityConn); err != nil {
		return nil, fmt.Errorf("httpapi: register TenantService gateway handlers: %w", err)
	}
	if err := tenantidentityv1.RegisterIdentityServiceHandler(ctx, mux, tenantIdentityConn); err != nil {
		return nil, fmt.Errorf("httpapi: register IdentityService gateway handlers: %w", err)
	}

	wsProxyHandler := &wsproxy.Handler{
		AgentPresenceWSURL: cfg.AgentPresenceWSURL,
		TicketVerifier:     ticketVerifier(cfg.TicketMinter),
		Logger:             logger,
	}

	top := http.NewServeMux()
	top.Handle("/v1/ws-ticket", wsTicketHandler(cfg.Verifier, cfg.TicketMinter))
	top.Handle("/ws", wsProxyHandler)
	// Every other path (the whole /v1/... REST surface grpc-gateway
	// generated routes for) falls through to the grpc-gateway mux.
	top.Handle("/", mux)

	return &Handler{
		Handler: gwauth.Middleware(cfg.Verifier, top),
		WSProxy: wsProxyHandler,
	}, nil
}

// ticketVerifier exposes the Minter's own signing key as a Verifier, so
// wsproxy.Handler can validate a ticket before dialing upstream without
// this package hand-constructing a second verifier from the same key --
// see wsticket.Minter.Verifier.
func ticketVerifier(m *wsticket.Minter) *jwtauth.Verifier {
	if m == nil {
		return nil
	}
	return m.Verifier()
}

// wsTicketResponse is POST /v1/ws-ticket's JSON response body.
type wsTicketResponse struct {
	Ticket    string    `json:"ticket"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// wsTicketHandler implements POST /v1/ws-ticket: mints a short-lived
// WebSocket ticket for the CALLER'S OWN identity. This route is NOT in
// gwauth.IsExempt's list -- it requires a normal, valid bearer token like
// any other authenticated REST route (see internal/wsticket's doc comment
// on why minting a ticket is not a bootstrapping operation) -- so by the
// time this handler runs, the outer gwauth.Middleware has already
// rejected any request without one. This handler re-parses and
// re-verifies the same header anyway (rather than trusting "the middleware
// already checked") purely to recover the verified Claims to mint the
// ticket from -- gwauth.Middleware intentionally has no side channel for
// passing decoded claims to downstream handlers (see that package's doc
// comment: it deliberately does nothing beyond letting the original
// Authorization header flow through unmodified), so this is the
// lowest-friction way to get them without adding one.
func wsTicketHandler(verifier *jwtauth.Verifier, minter *wsticket.Minter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		if minter == nil {
			http.Error(w, `{"error":"ws-ticket minting is not configured on this gateway"}`, http.StatusServiceUnavailable)
			return
		}

		const bearerPrefix = "Bearer "
		raw := r.Header.Get("Authorization")
		if len(raw) <= len(bearerPrefix) || !strings.EqualFold(raw[:len(bearerPrefix)], bearerPrefix) {
			// Should be unreachable in production (gwauth.Middleware
			// already enforces this), but fail closed rather than assume.
			http.Error(w, `{"error":"missing or malformed Authorization header"}`, http.StatusUnauthorized)
			return
		}
		claims, err := verifier.Verify(raw[len(bearerPrefix):])
		if err != nil {
			http.Error(w, `{"error":"invalid or expired token"}`, http.StatusUnauthorized)
			return
		}

		ticket, expiresAt, err := minter.Mint(claims)
		if err != nil {
			http.Error(w, `{"error":"failed to mint ticket"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(wsTicketResponse{Ticket: ticket, ExpiresAt: expiresAt})
	})
}
