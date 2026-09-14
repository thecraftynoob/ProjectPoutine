// Command api-gateway is the production entrypoint for API Gateway / BFF
// (architecture doc Section 2.2): the single external entry point fronting
// Task Router and Tenant & Identity Management -- REST routing (via
// grpc-gateway), end-user JWT validation, and WebSocket upgrade proxying
// for Agent Desktop (browser clients, via the short-lived ws-ticket
// mechanism -- see internal/wsticket and internal/wsproxy).
//
// Wiring, mirroring task-router/cmd/main.go and tenant-identity/cmd/main.go's
// shape:
//   - Two backend gRPC client connections (Task Router, Tenant & Identity),
//     dialed once at startup and reused by grpc-gateway's generated
//     handlers for the lifetime of the process (internal/httpapi).
//   - JWT verification (Tenant & Identity's signing public key, same
//     ConfigMap every other service mounts) validates every non-exempt
//     REST request BEFORE it reaches grpc-gateway's mux (internal/gwauth)
//     -- this is Layer 1 enforcement (architecture doc Section 1.1); the
//     token is then forwarded unmodified to the backend, which re-verifies
//     it independently (Layer 2).
//   - A second, DEDICATED ECDSA keypair (generated fresh at startup, never
//     persisted) signs short-lived WebSocket tickets (internal/wsticket) --
//     deliberately NOT the same key as the session-JWT verifier above,
//     since this service never holds Tenant & Identity's private key. The
//     public half is logged prominently at startup for manual
//     distribution into Agent Presence's ws-ticket ConfigMap, mirroring
//     Tenant & Identity's own startup-banner pattern exactly.
//   - A single HTTP server serves both the REST surface (grpc-gateway
//     mux), POST /v1/ws-ticket, and the /ws WebSocket proxy to Agent
//     Presence -- there is no separate gRPC server for this service's own
//     RPCs (API Gateway defines none of its own; it only fronts others'),
//     but the standard gRPC health check is still exposed on a small gRPC
//     server for parity with every other service's K8s liveness/readiness
//     probe convention (deploy/k8s/README.md).
//   - Graceful shutdown on SIGINT/SIGTERM: drain WebSocket proxy
//     connections, then stop the HTTP server.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thecraftynoob/ProjectPoutine/pkg/config"
	"github.com/thecraftynoob/ProjectPoutine/pkg/health"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/services/api-gateway/internal/httpapi"
	"github.com/thecraftynoob/ProjectPoutine/services/api-gateway/internal/wsticket"

	"google.golang.org/grpc"
)

type serviceConfig struct {
	// GRPCPort exposes only the standard health check (see package doc
	// comment) -- API Gateway defines no domain gRPC service of its own.
	GRPCPort string `env:"API_GATEWAY_GRPC_PORT" envDefault:"50059"`
	// HTTPAddr is the listen address for the REST/WebSocket HTTP server --
	// this service's real external surface.
	HTTPAddr string `env:"API_GATEWAY_HTTP_ADDR" envDefault:":8080"`

	// TaskRouterGRPCAddr and TenantIdentityGRPCAddr: in-cluster Service
	// DNS names for the two backend gRPC servers this gateway fronts (see
	// CLAUDE.md Rule 2 -- localhost defaults here are ONLY for standalone
	// `go run` outside the cluster; every K8s deployment.yaml supplies a
	// real Service DNS override).
	TaskRouterGRPCAddr     string `env:"TASK_ROUTER_GRPC_ADDR" envDefault:"localhost:50054"`
	TenantIdentityGRPCAddr string `env:"TENANT_IDENTITY_GRPC_ADDR" envDefault:"localhost:50051"`
	// AgentPresenceWSAddr is Agent Presence's own WebSocket base URL,
	// dialed by internal/wsproxy when proxying a ticket-authenticated
	// browser connection upstream.
	AgentPresenceWSAddr string `env:"AGENT_PRESENCE_WS_ADDR" envDefault:"ws://localhost:8085/ws"`

	// JWTPublicKeyPath points at the PEM-encoded ECDSA public key Tenant &
	// Identity Management issues SESSION tokens with (see
	// deploy/k8s/tenant-identity-public-key.example.yaml) -- required to
	// validate every non-exempt REST request's bearer token (Layer 1
	// enforcement). This service fails closed (refuses to start) without
	// it, the same as every other service's tenantctx verifier.
	JWTPublicKeyPath string `env:"JWT_PUBLIC_KEY_PATH,required"`
	// WSTicketTTLSeconds overrides internal/wsticket's default ticket
	// lifetime (45s). See that package's doc comment for why a short TTL,
	// not true single-use tracking, is this milestone's deliberate scope.
	WSTicketTTLSeconds int `env:"API_GATEWAY_WS_TICKET_TTL_SECONDS" envDefault:"45"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load[serviceConfig]()
	if err != nil {
		logger.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- JWT verification (Layer 1 enforcement, architecture doc Section
	// 1.1) -- validates end-user session tokens on every non-exempt REST
	// route before it is translated to a backend gRPC call. ---
	verifier, err := jwtauth.LoadVerifierFromFile(cfg.JWTPublicKeyPath)
	if err != nil {
		logger.Error("failed to load JWT verifier -- refusing to start without one (fail closed)", slog.Any("error", err))
		os.Exit(1)
	}

	// --- WS-ticket minting keypair: generated fresh every startup, never
	// persisted -- see internal/wsticket's doc comment. ---
	ticketMinter, err := wsticket.NewMinter(time.Duration(cfg.WSTicketTTLSeconds) * time.Second)
	if err != nil {
		logger.Error("failed to generate ws-ticket signing keypair", slog.Any("error", err))
		os.Exit(1)
	}
	ticketPublicKeyPEM, err := ticketMinter.PublicKeyPEM()
	if err != nil {
		logger.Error("failed to marshal ws-ticket public key", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("==================== API GATEWAY WS-TICKET SIGNING PUBLIC KEY ====================")
	logger.Info("copy the PEM block below into deploy/k8s/api-gateway-ws-ticket-public-key.example.yaml (see that file's comments) and `kubectl apply` it manually -- Agent Presence needs this to accept ?ticket= WebSocket connections")
	logger.Info("\n" + string(ticketPublicKeyPEM))
	logger.Info("====================================================================================")

	// --- HTTP server: REST (grpc-gateway), POST /v1/ws-ticket, /ws proxy ---
	handler, err := httpapi.New(ctx, httpapi.Config{
		TaskRouterGRPCAddr:     cfg.TaskRouterGRPCAddr,
		TenantIdentityGRPCAddr: cfg.TenantIdentityGRPCAddr,
		Verifier:               verifier,
		TicketMinter:           ticketMinter,
		AgentPresenceWSURL:     cfg.AgentPresenceWSAddr,
		Logger:                 logger,
	})
	if err != nil {
		logger.Error("failed to build HTTP handler", slog.Any("error", err))
		os.Exit(1)
	}

	httpServer := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: handler,
	}

	// --- gRPC server (health check only -- API Gateway defines no domain
	// RPCs of its own; see package doc comment) ---
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("failed to listen", slog.Any("error", err))
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	health.Register(grpcServer)

	// --- Serve both listeners, with graceful shutdown on SIGINT/SIGTERM ---
	grpcServeErr := make(chan error, 1)
	go func() {
		logger.Info("starting api-gateway gRPC health server", slog.String("grpc_port", cfg.GRPCPort))
		grpcServeErr <- grpcServer.Serve(lis)
	}()

	httpServeErr := make(chan error, 1)
	go func() {
		logger.Info("starting api-gateway HTTP server", slog.String("http_addr", cfg.HTTPAddr))
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		httpServeErr <- err
	}()

	select {
	case err := <-grpcServeErr:
		if err != nil {
			logger.Error("grpc server stopped", slog.Any("error", err))
			os.Exit(1)
		}
	case err := <-httpServeErr:
		if err != nil {
			logger.Error("http server stopped", slog.Any("error", err))
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining connections")

		handler.WSProxy.Shutdown(context.Background())

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Warn("http server shutdown did not complete cleanly", slog.Any("error", err))
		}

		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
			logger.Info("graceful shutdown complete")
		case <-time.After(20 * time.Second):
			logger.Warn("graceful shutdown timed out, forcing stop")
			grpcServer.Stop()
		}
	}
}
