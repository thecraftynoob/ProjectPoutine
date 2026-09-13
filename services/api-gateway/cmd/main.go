// Command api-gateway is the scaffold entrypoint for the API Gateway / BFF
// (architecture doc Section 2.2): single external entry point, TLS
// termination, JWT validation, tenant resolution, REST/WebSocket-to-gRPC
// edge. Described in the architecture doc as "supporting, not a domain
// service." Stateless.
//
// This scaffold is a placeholder gRPC health-checkable service only — it
// does not (yet) terminate the tenant-context interceptor the way
// internal services do, since the Gateway's job is to *originate*
// x-tenant-id from a validated JWT (Layer 1 enforcement, architecture doc
// Section 1.1), not to validate one on the way in from an external,
// unauthenticated caller. The real BFF/REST/WebSocket edge implementation
// is future work.
package main

import (
	"log/slog"
	"net"
	"os"

	"github.com/thecraftynoob/ProjectPoutine/pkg/config"
	"github.com/thecraftynoob/ProjectPoutine/pkg/health"
	"google.golang.org/grpc"
)

type serviceConfig struct {
	GRPCPort string `env:"API_GATEWAY_GRPC_PORT" envDefault:"50059"`
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

	server := grpc.NewServer()
	health.Register(server)

	logger.Info("starting api-gateway", slog.String("grpc_port", cfg.GRPCPort))
	if err := server.Serve(lis); err != nil {
		logger.Error("server stopped", slog.Any("error", err))
		os.Exit(1)
	}
}
