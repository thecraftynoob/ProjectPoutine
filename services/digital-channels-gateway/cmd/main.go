// Command digital-channels-gateway is the entrypoint for the Digital
// Channels Gateway service (architecture doc Section 2.2): normalizes
// inbound Chat/SMS/Email/Social into the generic Task abstraction, and
// (future milestone) handles outbound delivery back to the origin
// channel. Stateless.
//
// This milestone adds this service's first real domain logic: an inbound
// webhook HTTP endpoint (POST /webhooks/chat/{tenant_id}, see
// internal/webhookapi) for one generic "chat" channel shape, ending at a
// successfully enqueued Task Router Task via a service-to-service
// EnqueueTask call (pkg/svcauth). No outbound delivery, no Postgres
// persistence of messages (explicitly deferred to future async workers),
// and no webhook signature verification (explicitly deferred -- see
// internal/webhookapi's ServeHTTP doc comment for that tradeoff) exist
// yet.
//
// Wiring mirrors services/api-gateway/cmd/main.go's "HTTP server
// alongside a minimal gRPC health server" shape:
//   - A single HTTP server serves the webhook endpoint -- this service's
//     new real external surface.
//   - The existing gRPC server (tenant-context interceptors + standard
//     health check) is kept exactly as the scaffold had it, even though
//     this milestone's real traffic is all HTTP -- every other service in
//     this repo exposes the same gRPC health check for K8s liveness/
//     readiness probe parity (deploy/k8s/README.md), and this service may
//     grow real gRPC RPCs of its own in a future milestone.
//   - Both listeners run concurrently; graceful shutdown on SIGINT/
//     SIGTERM stops the HTTP server first, then the gRPC server.
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
	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	"github.com/thecraftynoob/ProjectPoutine/pkg/health"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/pkg/tenantctx"
	"github.com/thecraftynoob/ProjectPoutine/services/digital-channels-gateway/internal/webhookapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type serviceConfig struct {
	GRPCPort    string `env:"DIGITAL_CHANNELS_GATEWAY_GRPC_PORT" envDefault:"50053"`
	PostgresDSN string `env:"POSTGRES_DSN"`
	NATSURL     string `env:"NATS_URL"`
	// JWTPublicKeyPath points at the PEM-encoded ECDSA public key Tenant &
	// Identity Management issues tokens with (see
	// deploy/k8s/tenant-identity-public-key.example.yaml). Required even
	// though this milestone's real traffic (the webhook) is unauthenticated
	// HTTP, not gRPC: the tenant-context interceptor is still wired in on
	// the gRPC health server for parity with every other service, and it
	// fails closed without a verifier.
	JWTPublicKeyPath string `env:"JWT_PUBLIC_KEY_PATH,required"`

	// HTTPAddr is the listen address for the webhook HTTP server -- this
	// milestone's real external surface.
	HTTPAddr string `env:"DIGITAL_CHANNELS_GATEWAY_HTTP_ADDR" envDefault:":8086"`

	// TaskRouterGRPCAddr and TenantIdentityGRPCAddr: in-cluster Service DNS
	// names for the two backend gRPC servers this service calls as a
	// service (see CLAUDE.md Rule 2 -- localhost defaults here are ONLY
	// for standalone `go run` outside the cluster; every K8s
	// deployment.yaml supplies a real Service DNS override).
	TaskRouterGRPCAddr     string `env:"TASK_ROUTER_GRPC_ADDR" envDefault:"localhost:50054"`
	TenantIdentityGRPCAddr string `env:"TENANT_IDENTITY_GRPC_ADDR" envDefault:"localhost:50051"`

	// ServiceSharedSecret is the shared credential this service presents
	// to Tenant & Identity's IssueServiceToken RPC when minting a
	// tenant-scoped service token to call Task Router's EnqueueTask (see
	// deploy/k8s/service-credential.example.yaml).
	ServiceSharedSecret string `env:"SERVICE_SHARED_SECRET,required"`
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

	verifier, err := jwtauth.LoadVerifierFromFile(cfg.JWTPublicKeyPath)
	if err != nil {
		logger.Error("failed to load JWT verifier -- refusing to start without one (fail closed)", slog.Any("error", err))
		os.Exit(1)
	}

	// --- Task Router client: dialed once, shared across all tenants'
	// webhook traffic. Per-call auth is a tenant-scoped service token
	// minted lazily per tenant_id -- see webhookapi.TenantScopedTaskRouterClient's
	// doc comment for why this differs from every other current
	// pkg/svcauth caller in this repo (which each mint for one fixed
	// tenant at call time from an already-authenticated caller, not an
	// unauthenticated webhook's path segment). ---
	taskRouterConn, err := grpc.NewClient(cfg.TaskRouterGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to dial task-router", slog.Any("error", err))
		os.Exit(1)
	}
	defer taskRouterConn.Close()

	taskRouterClient := webhookapi.NewTenantScopedTaskRouterClient(
		taskrouterv1.NewTaskRouterServiceClient(taskRouterConn),
		cfg.TenantIdentityGRPCAddr,
		"digital-channels-gateway",
		cfg.ServiceSharedSecret,
	)
	defer taskRouterClient.Close()

	webhookHandler := &webhookapi.Handler{
		EnqueueTask: taskRouterClient.EnqueueTask,
		Logger:      logger,
	}

	httpMux := http.NewServeMux()
	httpMux.Handle("POST /webhooks/chat/{tenant_id}", webhookHandler)

	httpServer := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: httpMux,
	}

	// --- gRPC server: unchanged from the scaffold (tenant-context
	// interceptors + standard health check). See package doc comment for
	// why this is kept even though this milestone's real traffic is all
	// HTTP. ---
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("failed to listen", slog.Any("error", err))
		os.Exit(1)
	}
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(tenantctx.UnaryServerInterceptor(verifier)),
		grpc.ChainStreamInterceptor(tenantctx.StreamServerInterceptor(verifier)),
	)
	health.Register(grpcServer)

	grpcServeErr := make(chan error, 1)
	go func() {
		logger.Info("starting digital-channels-gateway gRPC health server", slog.String("grpc_port", cfg.GRPCPort))
		grpcServeErr <- grpcServer.Serve(lis)
	}()

	httpServeErr := make(chan error, 1)
	go func() {
		logger.Info("starting digital-channels-gateway HTTP server", slog.String("http_addr", cfg.HTTPAddr))
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
