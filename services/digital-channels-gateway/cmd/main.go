// Command digital-channels-gateway is the scaffold entrypoint for the
// Digital Channels Gateway service (architecture doc Section 2.2):
// normalizes inbound Chat/SMS/Email/Social into the generic Task
// abstraction, and handles outbound delivery back to the origin channel.
// Stateless.
//
// This scaffold starts a gRPC server with the shared tenant-context
// interceptors and standard health service registered. No domain RPCs or
// channel-provider integrations exist yet (future milestone).
package main

import (
	"log/slog"
	"net"
	"os"

	"github.com/thecraftynoob/ProjectPoutine/pkg/config"
	"github.com/thecraftynoob/ProjectPoutine/pkg/health"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/pkg/tenantctx"
	"google.golang.org/grpc"
)

type serviceConfig struct {
	GRPCPort    string `env:"DIGITAL_CHANNELS_GATEWAY_GRPC_PORT" envDefault:"50053"`
	PostgresDSN string `env:"POSTGRES_DSN"`
	NATSURL     string `env:"NATS_URL"`
	// JWTPublicKeyPath points at the PEM-encoded ECDSA public key Tenant &
	// Identity Management issues tokens with (see
	// deploy/k8s/tenant-identity-public-key.example.yaml). Required even
	// for this scaffold: the tenant-context interceptor is wired in for
	// when real RPCs land, and it fails closed without a verifier.
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

	verifier, err := jwtauth.LoadVerifierFromFile(cfg.JWTPublicKeyPath)
	if err != nil {
		logger.Error("failed to load JWT verifier -- refusing to start without one (fail closed)", slog.Any("error", err))
		os.Exit(1)
	}

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

	logger.Info("starting digital-channels-gateway", slog.String("grpc_port", cfg.GRPCPort))
	if err := server.Serve(lis); err != nil {
		logger.Error("server stopped", slog.Any("error", err))
		os.Exit(1)
	}
}
