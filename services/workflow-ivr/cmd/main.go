// Command workflow-ivr is the scaffold entrypoint for the Workflow/IVR
// Engine service (architecture doc Section 2.2): executes tenant-defined
// call/chat flows as a state machine, queries external systems before a
// Task is queued to the Router. Stateless compute; flow state persisted
// per-session in Redis so any replica can continue a flow.
//
// This scaffold starts a gRPC server with the shared tenant-context
// interceptors and standard health service registered. No domain RPCs
// exist yet (future milestone).
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
	GRPCPort  string `env:"WORKFLOW_IVR_GRPC_PORT" envDefault:"50056"`
	RedisAddr string `env:"REDIS_ADDR"`
	NATSURL   string `env:"NATS_URL"`
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

	logger.Info("starting workflow-ivr", slog.String("grpc_port", cfg.GRPCPort))
	if err := server.Serve(lis); err != nil {
		logger.Error("server stopped", slog.Any("error", err))
		os.Exit(1)
	}
}
