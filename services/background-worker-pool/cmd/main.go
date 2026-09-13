// Command background-worker-pool is the scaffold entrypoint for the
// Background Worker Pool service (architecture doc Section 2.2): executes
// transactional, non-real-time jobs pulled from the Database-as-a-Queue
// (see /pkg/pgqueue). Stateless; horizontally scaled, coordinated purely
// via Postgres row locking (FOR UPDATE SKIP LOCKED), not app state.
//
// This scaffold starts a gRPC server with the shared tenant-context
// interceptors and standard health service registered (for liveness/
// readiness). The actual pgqueue.Poller wiring and job-type handlers are
// future-milestone domain logic — this binary does not connect to
// Postgres yet, since a worker with no jobs registered has nothing useful
// to poll for.
package main

import (
	"log/slog"
	"net"
	"os"

	"github.com/thecraftynoob/ProjectPoutine/pkg/config"
	"github.com/thecraftynoob/ProjectPoutine/pkg/health"
	"github.com/thecraftynoob/ProjectPoutine/pkg/tenantctx"
	"google.golang.org/grpc"
)

type serviceConfig struct {
	GRPCPort    string `env:"BACKGROUND_WORKER_POOL_GRPC_PORT" envDefault:"50058"`
	PostgresDSN string `env:"POSTGRES_DSN"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load[serviceConfig]()
	if err != nil {
		logger.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("failed to listen", slog.Any("error", err))
		os.Exit(1)
	}

	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(tenantctx.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(tenantctx.StreamServerInterceptor()),
	)
	health.Register(server)

	logger.Info("starting background-worker-pool", slog.String("grpc_port", cfg.GRPCPort))
	if err := server.Serve(lis); err != nil {
		logger.Error("server stopped", slog.Any("error", err))
		os.Exit(1)
	}
}
