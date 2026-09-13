// Command voice-media-gateway is the scaffold entrypoint for the Voice/SIP
// Media Gateway service (architecture doc Section 2.2): terminates SIP/
// WebRTC sessions via FreeSWITCH, converts call setup into generic Tasks,
// executes agent-side call control. Stateful (active call = live RTP
// session + FreeSWITCH channel state) in its real implementation; this
// scaffold itself holds no state.
//
// This scaffold starts a gRPC server with the shared tenant-context
// interceptors and standard health service registered. No domain RPCs or
// FreeSWITCH integration exist yet (future milestone).
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
	GRPCPort  string `env:"VOICE_MEDIA_GATEWAY_GRPC_PORT" envDefault:"50052"`
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

	logger.Info("starting voice-media-gateway", slog.String("grpc_port", cfg.GRPCPort))
	if err := server.Serve(lis); err != nil {
		logger.Error("server stopped", slog.Any("error", err))
		os.Exit(1)
	}
}
