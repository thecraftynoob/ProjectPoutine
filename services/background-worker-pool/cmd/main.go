// Command background-worker-pool is the entrypoint for the Background
// Worker Pool service (architecture doc Section 2.2): executes
// transactional, non-real-time jobs pulled from the Database-as-a-Queue
// (see /pkg/pgqueue). Stateless; horizontally scaled, coordinated purely
// via Postgres row locking (FOR UPDATE SKIP LOCKED), not app state.
//
// This milestone wires the first real end-to-end pipeline: a durable NATS
// JetStream consumer (internal/wrapupsync.Consumer) turns Task Router's
// task.completed event into a wrapup_sync background_jobs row, and a
// pgqueue.Poller claims it and dispatches to internal/wrapupsync.Handler,
// which does a real outbound HTTP POST to a tenant-configured URL (read
// from this service's own background_worker_pool_wrapup_targets table --
// a documented stand-in for real tenant settings, see GAPS.md) and marks
// the job done/failed(-with-retry) accordingly.
//
// The scaffold's gRPC server (health check + tenant-context interceptors)
// is kept exactly as before, alongside the new pipeline -- still useful
// for K8s liveness/readiness probes even though this service has no
// domain RPCs of its own.
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
	"github.com/thecraftynoob/ProjectPoutine/pkg/pgqueue"
	"github.com/thecraftynoob/ProjectPoutine/pkg/tenantctx"
	"github.com/thecraftynoob/ProjectPoutine/services/background-worker-pool/internal/pgstore"
	"github.com/thecraftynoob/ProjectPoutine/services/background-worker-pool/internal/wrapupsync"
)

type serviceConfig struct {
	GRPCPort string `env:"BACKGROUND_WORKER_POOL_GRPC_PORT" envDefault:"50058"`
	// PostgresDSN authenticates as the Postgres SUPERUSER ("ccaas" by
	// default -- see deploy/k8s/infra-config.yaml's POSTGRES_USER). Used
	// ONLY for running pgstore.Migrate at startup (CREATE ROLE/GRANT
	// require superuser or table-owner privilege) -- never for this
	// service's ongoing queries. See RuntimePostgresDSN below.
	PostgresDSN string `env:"POSTGRES_DSN,required"`
	// RuntimePostgresDSN authenticates as pgstore.RuntimeRole, the
	// shared, non-superuser, NOBYPASSRLS role pgstore.Migrate provisions
	// (see internal/pgstore/runtime_role.go). This is what backs
	// `targets`, the wrapupsync consumer/handler, and the pgqueue.Poller
	// below -- this service's actual, ongoing reads/writes against
	// background_jobs (including the Poller's FOR UPDATE SKIP LOCKED
	// claim query) and background_worker_pool_wrapup_targets. Neither
	// table carries an RLS policy today (see internal/pgstore's package
	// doc comment), but this service still switches its runtime
	// connection identity for platform-wide consistency -- see
	// ARCHITECTURE_FLOW.md §5. Composed the same $(VAR)-interpolation way
	// POSTGRES_DSN is in
	// deploy/k8s/background-worker-pool/deployment.yaml, from
	// POSTGRES_RUNTIME_USER/POSTGRES_RUNTIME_PASSWORD.
	RuntimePostgresDSN string `env:"RUNTIME_POSTGRES_DSN,required"`
	NATSURL            string `env:"NATS_URL" envDefault:"nats://localhost:4222"`
	// JWTPublicKeyPath points at the PEM-encoded ECDSA public key Tenant &
	// Identity Management issues tokens with (see
	// deploy/k8s/tenant-identity-public-key.example.yaml). Required even
	// though this service still has no domain RPCs of its own: the
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

	// --- Postgres (background_jobs + background_worker_pool_wrapup_targets) ---
	// migratePool: superuser ("ccaas"), used ONLY to run pgstore.Migrate
	// (which itself provisions pgstore.RuntimeRole and GRANTs it
	// privileges -- see internal/pgstore/migrate.go and
	// runtime_role.go). Never used for this service's ongoing domain
	// queries -- closed immediately after Migrate returns.
	migratePool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("failed to construct postgres pool for migrations", slog.Any("error", err))
		os.Exit(1)
	}
	if err := migratePool.Ping(ctx); err != nil {
		migratePool.Close()
		logger.Error("failed to connect to postgres", slog.Any("error", err))
		os.Exit(1)
	}
	if err := pgstore.Migrate(ctx, migratePool); err != nil {
		migratePool.Close()
		logger.Error("failed to run postgres migrations", slog.Any("error", err))
		os.Exit(1)
	}
	migratePool.Close()

	// pgPool: pgstore.RuntimeRole (non-superuser, NOBYPASSRLS) -- what
	// `targets`, the wrapupsync consumer/handler, and the pgqueue.Poller
	// all connect as for their real, ongoing queries. See
	// RuntimePostgresDSN's field doc comment above.
	pgPool, err := pgxpool.New(ctx, cfg.RuntimePostgresDSN)
	if err != nil {
		logger.Error("failed to construct postgres pool as runtime role", slog.Any("error", err))
		os.Exit(1)
	}
	defer pgPool.Close()
	if err := pgPool.Ping(ctx); err != nil {
		logger.Error("failed to connect to postgres as runtime role", slog.Any("error", err))
		os.Exit(1)
	}
	targets := pgstore.NewWrapupTargetStore(pgPool)

	// --- NATS JetStream (durable task.completed ingestion) ---
	natsClient, err := eventbus.Connect(cfg.NATSURL)
	if err != nil {
		logger.Error("failed to connect to nats", slog.Any("error", err))
		os.Exit(1)
	}
	defer natsClient.Close()

	consumer := wrapupsync.NewConsumer(natsClient, pgPool, logger)
	// Defensively ensure the stream exists regardless of Task Router's
	// startup order relative to this service -- see
	// wrapupsync.Consumer.EnsureStream's doc comment (CreateOrUpdateStream
	// is idempotent and safe to call redundantly from multiple services).
	if err := consumer.EnsureStream(ctx); err != nil {
		logger.Error("failed to ensure event stream", slog.Any("error", err))
		os.Exit(1)
	}
	if err := consumer.Start(ctx); err != nil {
		logger.Error("failed to start durable event consumer", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("durable task.completed consumer started")

	// --- pgqueue.Poller (claims wrapup_sync jobs and dispatches to Handler) ---
	handler := wrapupsync.NewHandler(pgPool, targets, logger)
	poller := &pgqueue.Poller{Pool: pgPool, Logger: logger}
	go func() {
		if err := poller.Run(ctx, handler.HandleJob); err != nil && ctx.Err() == nil {
			logger.Error("poller stopped unexpectedly", slog.Any("error", err))
		}
	}()
	logger.Info("background job poller started")

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
		logger.Info("starting background-worker-pool", slog.String("grpc_port", cfg.GRPCPort))
		serveErr <- server.Serve(lis)
	}()

	select {
	case err := <-serveErr:
		if err != nil {
			logger.Error("server stopped", slog.Any("error", err))
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received, stopping event consumption and job polling")
		// ctx.Done() firing already signals wrapupsync's Subscribe loop
		// (via pkg/eventbus.consume's own goroutine watching this same
		// ctx) and the Poller's Run loop to stop -- nothing further to do
		// here to halt either before the deferred natsClient.Close()/
		// pgPool.Close() run.
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
