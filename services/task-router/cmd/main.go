// Command task-router is the scaffold entrypoint for the Task Router
// service (architecture doc Section 2.2 / TASK_ROUTER_SPECIFICATION.md):
// the routing/matching "brain" that assigns queued Tasks to available
// Agents. Stateful (in-memory routing queues, ideally checkpointed).
//
// This scaffold starts a gRPC server with the shared tenant-context
// interceptors and standard health service registered. It does not yet
// define its own proto contract (that comes from
// TASK_ROUTER_SPECIFICATION.md in a later milestone) and does not yet
// dial Agent & Presence Service's PresenceService — both are future-
// milestone wiring, though pkg/genproto/presence/v1 already provides a
// working client stub to dial against once that wiring begins.
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
	GRPCPort  string `env:"TASK_ROUTER_GRPC_PORT" envDefault:"50054"`
	RedisAddr string `env:"REDIS_ADDR"`
	NATSURL   string `env:"NATS_URL"`
	// AgentPresenceAddr is where Agent & Presence Service's PresenceService
	// gRPC server can be reached, for the "who is available" synchronous
	// query described in architecture doc Section 3.2. Not dialed yet.
	AgentPresenceAddr string `env:"AGENT_PRESENCE_GRPC_ADDR" envDefault:"localhost:50055"`
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

	logger.Info("starting task-router", slog.String("grpc_port", cfg.GRPCPort))
	if err := server.Serve(lis); err != nil {
		logger.Error("server stopped", slog.Any("error", err))
		os.Exit(1)
	}
}
