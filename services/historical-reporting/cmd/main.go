// Command historical-reporting is the entrypoint for the Historical
// Reporting & Analytics service (architecture doc Section 2.2): durably
// subscribes to Task Router's full domain event catalog via JetStream and
// materializes it for BI/SLA/compliance reporting. Never sits in the
// real-time path of any other service.
//
// This milestone's scope is ingestion-only: a durable NATS JetStream
// consumer (internal/eventconsumer) that writes every received event into
// one generic Postgres table (internal/pgstore's historical_events), via
// a single Subscribe call covering all three of Task Router's domains
// (task/agent/reservation) with one wildcard FilterSubject. No read/query
// API, no new gRPC RPC, no REST route -- verification is direct SQL, not
// an API (see this service's live smoke test). The scaffold's gRPC health
// server is kept exactly as before, alongside the new ingestion pipeline
// -- still useful for K8s liveness/readiness probes even though this
// service has no domain RPCs of its own.
//
// Wiring:
//   - PostgreSQL (plain pgxpool.Pool, no RLS -- see internal/pgstore's
//     package doc comment for why) backs historical_events.
//   - NATS JetStream (via pkg/eventbus) is where the durable consumer
//     pulls from -- see internal/eventconsumer's package doc comment for
//     the durable-vs-ephemeral choice and the at-least-once/no-dedupe
//     tradeoff.
//   - Graceful shutdown on SIGINT/SIGTERM stops accepting new gRPC work,
//     lets the JetStream consumer's context cancel (pkg/eventbus.consume
//     stops pulling), then closes the NATS connection and Postgres pool.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/thecraftynoob/ProjectPoutine/pkg/config"
	"github.com/thecraftynoob/ProjectPoutine/pkg/eventbus"
	"github.com/thecraftynoob/ProjectPoutine/pkg/health"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/pkg/tenantctx"
	"github.com/thecraftynoob/ProjectPoutine/services/historical-reporting/internal/eventconsumer"
	"github.com/thecraftynoob/ProjectPoutine/services/historical-reporting/internal/pgstore"
)

type serviceConfig struct {
	GRPCPort    string `env:"HISTORICAL_REPORTING_GRPC_PORT" envDefault:"50057"`
	PostgresDSN string `env:"POSTGRES_DSN,required"`
	NATSURL     string `env:"NATS_URL" envDefault:"nats://localhost:4222"`
	// JWTPublicKeyPath points at the PEM-encoded ECDSA public key Tenant &
	// Identity Management issues tokens with (see
	// deploy/k8s/tenant-identity-public-key.example.yaml). Required even
	// for this milestone (no domain RPCs of its own yet): the
	// tenant-context interceptor is wired in for when real RPCs land, and
	// it fails closed without a verifier.
	JWTPublicKeyPath string `env:"JWT_PUBLIC_KEY_PATH,required"`
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

	// --- Postgres (historical_events ingestion table) ---
	pgPool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("failed to construct postgres pool", slog.Any("error", err))
		os.Exit(1)
	}
	defer pgPool.Close()
	if err := pgPool.Ping(ctx); err != nil {
		logger.Error("failed to connect to postgres", slog.Any("error", err))
		os.Exit(1)
	}
	if err := pgstore.Migrate(ctx, pgPool); err != nil {
		logger.Error("failed to run postgres migrations", slog.Any("error", err))
		os.Exit(1)
	}
	store := pgstore.NewStore(pgPool)

	// --- NATS JetStream (durable event ingestion) ---
	natsClient, err := eventbus.Connect(cfg.NATSURL)
	if err != nil {
		logger.Error("failed to connect to nats", slog.Any("error", err))
		os.Exit(1)
	}
	defer natsClient.Close()

	consumer := eventconsumer.NewConsumer(natsClient, store, logger)
	// Defensively ensure the stream exists regardless of Task Router's
	// startup order relative to this service -- see
	// eventconsumer.Consumer.EnsureStream's doc comment for the full
	// reasoning (CreateOrUpdateStream is idempotent and safe to call
	// redundantly from multiple services).
	if err := consumer.EnsureStream(ctx); err != nil {
		logger.Error("failed to ensure event stream", slog.Any("error", err))
		os.Exit(1)
	}
	if err := consumer.Start(ctx); err != nil {
		logger.Error("failed to start durable event consumer", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("durable event consumer started")

	// --- gRPC server: health check only, no domain RPCs this milestone
	// (see package doc comment) ---
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("failed to listen", slog.Any("error", err))
		os.Exit(1)
	}

	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(tenantctx.UnaryServerInterceptor(verifier)),
		grpc.ChainStreamInterceptor(tenantctx.StreamServerInterceptor(verifier)),
	)
	health.Register(server)

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("starting historical-reporting", slog.String("grpc_port", cfg.GRPCPort))
		serveErr <- server.Serve(lis)
	}()

	select {
	case err := <-serveErr:
		if err != nil {
			logger.Error("server stopped", slog.Any("error", err))
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received, stopping event consumption and draining in-flight work")
		// ctx.Done() firing already signals eventconsumer's Subscribe loop
		// (via pkg/eventbus.consume's own goroutine watching this same
		// ctx) to stop pulling -- nothing further to do here to halt
		// ingestion before the deferred natsClient.Close()/pgPool.Close()
		// run.
		stopped := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
			logger.Info("graceful shutdown complete")
		case <-time.After(20 * time.Second):
			logger.Warn("graceful shutdown timed out, forcing stop")
			server.Stop()
		}
	}
}
