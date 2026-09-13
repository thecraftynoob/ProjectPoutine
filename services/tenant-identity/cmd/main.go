// Command tenant-identity is the scaffold entrypoint for the Tenant &
// Identity Management service (architecture doc Section 2.2): CRUD for
// tenant configuration, user/agent identity, RBAC, and JWT issuance. This
// is intentionally the most upstream service — no dependency on any other
// domain service.
//
// This scaffold starts a gRPC server with the shared tenant-context
// interceptors and standard health service registered. No domain RPCs are
// registered yet (future milestone). Postgres/Redis connections are not
// established here so the binary can start and pass a health check
// without live infra during local scaffolding — see the service README
// for the tradeoff.
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
	GRPCPort    string `env:"TENANT_IDENTITY_GRPC_PORT" envDefault:"50051"`
	PostgresDSN string `env:"POSTGRES_DSN"`
	RedisAddr   string `env:"REDIS_ADDR"`
	NATSURL     string `env:"NATS_URL"`
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

	logger.Info("starting tenant-identity", slog.String("grpc_port", cfg.GRPCPort))
	if err := server.Serve(lis); err != nil {
		logger.Error("server stopped", slog.Any("error", err))
		os.Exit(1)
	}
}
