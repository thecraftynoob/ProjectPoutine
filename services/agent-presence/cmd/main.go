// Command agent-presence is the scaffold entrypoint for the Agent &
// Presence Service (architecture doc Section 2.2): live WebSocket
// registry for logged-in Agent Desktops, single source of truth for
// real-time agent status. Stateful (live WebSocket registry).
//
// This scaffold proves the proto pipeline end-to-end: it registers the
// real generated presence.v1.PresenceService server (embedding
// UnimplementedPresenceServiceServer, so GetAvailableAgents currently
// returns codes.Unimplemented) alongside the shared tenant-context
// interceptors and standard health service. The real matching data (live
// WebSocket registry, Redis-backed presence keys) is future-milestone
// domain logic.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"

	presencev1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/presence/v1"
	"github.com/thecraftynoob/ProjectPoutine/pkg/config"
	"github.com/thecraftynoob/ProjectPoutine/pkg/health"
	"github.com/thecraftynoob/ProjectPoutine/pkg/tenantctx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serviceConfig struct {
	GRPCPort  string `env:"AGENT_PRESENCE_GRPC_PORT" envDefault:"50055"`
	RedisAddr string `env:"REDIS_ADDR"`
	NATSURL   string `env:"NATS_URL"`
}

// presenceServer is a stub implementation of presence.v1.PresenceService.
// It embeds UnimplementedPresenceServiceServer so the server compiles
// against the full interface today and can have real methods filled in
// later without a breaking signature change.
type presenceServer struct {
	presencev1.UnimplementedPresenceServiceServer
}

func (s *presenceServer) GetAvailableAgents(ctx context.Context, req *presencev1.GetAvailableAgentsRequest) (*presencev1.GetAvailableAgentsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "agent-presence: GetAvailableAgents not yet implemented (scaffold only)")
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
	presencev1.RegisterPresenceServiceServer(server, &presenceServer{})
	health.Register(server)

	logger.Info("starting agent-presence", slog.String("grpc_port", cfg.GRPCPort))
	if err := server.Serve(lis); err != nil {
		logger.Error("server stopped", slog.Any("error", err))
		os.Exit(1)
	}
}
