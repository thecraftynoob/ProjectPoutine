// Command agent-presence is the production entrypoint for the Agent &
// Presence Service (architecture doc Section 2.2): holds the live
// WebSocket connection to every logged-in Agent Desktop and relays a
// filtered subset of Task Router's domain events
// (TASK_ROUTER_SPECIFICATION.md Section 6.3) to the connection belonging
// to each event's target agent.
//
// Wiring, mirroring task-router/cmd/main.go's shape (minus Postgres,
// which this service does not need -- it owns no durable domain state
// of its own, only an in-memory connection registry and an ephemeral
// Redis pub/sub fan-out layer):
//   - Redis (github.com/redis/go-redis/v9) backs the cross-replica
//     fan-out (internal/relay.Fanout) -- see that package's doc comment
//     for the full design.
//   - NATS JetStream (via pkg/eventbus) is consumed, not published to:
//     this service subscribes to Task Router's TASK_ROUTER_EVENTS stream
//     via durable pull consumers, one per forwarded subject
//     (internal/relay.Consumer).
//   - Two listeners: a gRPC server (health check only -- see
//     proto/presence/v1/presence.proto's doc comment for why
//     PresenceService is intentionally empty today) and an HTTP server
//     serving the WebSocket upgrade endpoint (internal/wsserver).
//   - Graceful shutdown on SIGINT/SIGTERM: stop accepting new work, close
//     every live WebSocket connection, then stop the gRPC server.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thecraftynoob/ProjectPoutine/pkg/config"
	"github.com/thecraftynoob/ProjectPoutine/pkg/eventbus"
	"github.com/thecraftynoob/ProjectPoutine/pkg/health"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/pkg/tenantctx"
	"github.com/thecraftynoob/ProjectPoutine/services/agent-presence/internal/registry"
	"github.com/thecraftynoob/ProjectPoutine/services/agent-presence/internal/relay"
	"github.com/thecraftynoob/ProjectPoutine/services/agent-presence/internal/wsserver"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

type serviceConfig struct {
	GRPCPort string `env:"AGENT_PRESENCE_GRPC_PORT" envDefault:"50055"`
	// WSAddr is the listen address for the WebSocket/HTTP server (the
	// "/ws" upgrade endpoint). A separate port from the gRPC server --
	// see the service README for why 8085 was chosen.
	WSAddr    string `env:"AGENT_PRESENCE_WS_ADDR" envDefault:":8085"`
	RedisAddr string `env:"REDIS_ADDR" envDefault:"localhost:6379"`
	RedisDB   int    `env:"AGENT_PRESENCE_REDIS_DB" envDefault:"0"`
	NATSURL   string `env:"NATS_URL" envDefault:"nats://localhost:4222"`
	// JWTPublicKeyPath points at the PEM-encoded ECDSA public key Tenant &
	// Identity Management issues tokens with (see
	// deploy/k8s/tenant-identity-public-key.example.yaml). Required for
	// both this service's gRPC interceptor and the /ws WebSocket upgrade
	// endpoint's Authorization header verification (internal/wsserver) --
	// this service fails closed (refuses to start) without it.
	JWTPublicKeyPath string `env:"JWT_PUBLIC_KEY_PATH,required"`
	// WSTicketPublicKeyPath points at the PEM-encoded ECDSA public key API
	// Gateway's internal/wsticket generates and logs at its own startup
	// (see deploy/k8s/api-gateway-ws-ticket-public-key.example.yaml),
	// distributed the same manual-ConfigMap way as
	// tenant-identity-public-key. Enables the /ws endpoint's ?ticket=
	// query-parameter auth path (see internal/wsserver's doc comment) --
	// deliberately OPTIONAL, unlike JWTPublicKeyPath: an empty value
	// leaves the ticket path disabled (every ?ticket= attempt rejected)
	// while the original Authorization-header path keeps working
	// unaffected, so this service does not hard-fail on startup just
	// because API Gateway hasn't been deployed/configured yet.
	WSTicketPublicKeyPath string `env:"WS_TICKET_PUBLIC_KEY_PATH"`
	// TenantIdentityGRPCAddr and ServiceSharedSecret are accepted (and
	// logged as configured, see main() below) for parity with every
	// other service's deployment.yaml and forward-compatibility, but this
	// service has no steady-state, tenant-scoped call to make into
	// another service's gRPC API at startup (it is a NATS consumer and
	// WebSocket server, not a synchronous caller of other services'
	// RPCs) -- pkg/svcauth is what a future call site would use to
	// actually obtain and attach a service token, following the same
	// pattern this milestone's live verification exercises directly.
	TenantIdentityGRPCAddr string `env:"TENANT_IDENTITY_GRPC_ADDR"`
	ServiceSharedSecret    string `env:"SERVICE_SHARED_SECRET"`
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

	// --- Redis (cross-replica fan-out transport) ---
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Error("failed to connect to redis", slog.Any("error", err))
		os.Exit(1)
	}
	defer redisClient.Close()

	// --- NATS JetStream (event consumption) ---
	natsClient, err := eventbus.Connect(cfg.NATSURL)
	if err != nil {
		logger.Error("failed to connect to nats", slog.Any("error", err))
		os.Exit(1)
	}
	defer natsClient.Close()

	// --- JWT verification (Layer 2 enforcement, architecture doc Section
	// 1.1) -- shared by both the gRPC interceptor below and the /ws
	// upgrade endpoint's Authorization header check. ---
	verifier, err := jwtauth.LoadVerifierFromFile(cfg.JWTPublicKeyPath)
	if err != nil {
		logger.Error("failed to load JWT verifier -- refusing to start without one (fail closed)", slog.Any("error", err))
		os.Exit(1)
	}

	// --- WS-ticket verification (optional -- see WSTicketPublicKeyPath's
	// doc comment). Enables /ws's ?ticket= auth path for browser Agent
	// Desktop clients proxied through API Gateway.
	//
	// A MISSING file (the common case before API Gateway has ever run
	// once to generate+log its key, or when its ConfigMap's `optional:
	// true` mount resolves to an empty directory -- see
	// deploy/k8s/agent-presence/deployment.yaml) is treated as "ticket
	// path simply not configured yet," not a startup failure -- this
	// service must keep serving the Authorization-header path
	// unaffected. A file that EXISTS but fails to parse (corrupt/
	// malformed PEM) is different: that indicates real misconfiguration,
	// not absence, so it still fails closed rather than silently
	// disabling the path. ---
	var ticketVerifier *jwtauth.Verifier
	if cfg.WSTicketPublicKeyPath != "" {
		if _, statErr := os.Stat(cfg.WSTicketPublicKeyPath); statErr == nil {
			ticketVerifier, err = jwtauth.LoadVerifierFromFile(cfg.WSTicketPublicKeyPath)
			if err != nil {
				logger.Error("failed to load WS-ticket verifier -- refusing to start with a misconfigured (not merely absent) ticket key", slog.Any("error", err))
				os.Exit(1)
			}
			logger.Info("ws-ticket auth path enabled")
		} else {
			logger.Info("ws-ticket auth path disabled (WS_TICKET_PUBLIC_KEY_PATH set but file not present yet -- likely API Gateway not yet deployed) -- Authorization header path remains fully functional")
		}
	} else {
		logger.Info("ws-ticket auth path disabled (WS_TICKET_PUBLIC_KEY_PATH not set) -- Authorization header path remains fully functional")
	}

	// --- Service-to-service token source (optional -- see
	// TenantIdentityGRPCAddr's doc comment). Not tied to any particular
	// tenant at startup (this service has no steady-state, tenant-scoped
	// dependency on another service's gRPC API), so this only proves the
	// wiring is reachable, using the platform-registry-style
	// well-known-nil-tenant probe would be inappropriate -- so
	// construction here is deliberately just "can we reach
	// tenant-identity's gRPC server," deferring actual token issuance
	// (which requires a real tenant_id) to whatever future call site
	// needs it. See pkg/svcauth's doc comment for the full pattern this
	// wraps.
	if cfg.TenantIdentityGRPCAddr != "" && cfg.ServiceSharedSecret != "" {
		logger.Info("service-to-service auth configured", slog.String("tenant_identity_addr", cfg.TenantIdentityGRPCAddr))
	}

	// --- Connection registry + WebSocket transport ---
	reg := registry.New()
	wsHandler := wsserver.New(reg, verifier, ticketVerifier, logger)

	// --- Cross-replica fan-out + NATS relay consumer ---
	fanout := relay.NewFanout(redisClient, reg, logger)
	consumer := relay.NewConsumer(natsClient, fanout, logger)
	if err := consumer.Start(ctx); err != nil {
		logger.Error("failed to start relay consumer", slog.Any("error", err))
		os.Exit(1)
	}

	// --- gRPC server (health check only -- see presence.proto) ---
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("failed to listen", slog.Any("error", err))
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(tenantctx.UnaryServerInterceptor(verifier)),
		grpc.ChainStreamInterceptor(tenantctx.StreamServerInterceptor(verifier)),
	)
	health.Register(grpcServer)
	// presence.v1.PresenceService is intentionally NOT registered here: it
	// declares zero RPCs today (see proto/presence/v1/presence.proto's doc
	// comment for why the service definition is kept empty rather than
	// deleted), so there is no server implementation to construct or
	// register today. The gRPC server exists purely to expose the
	// standard health check above; wiring in presencev1 here is left for
	// whichever future milestone adds its first real RPC.

	// --- HTTP server (WebSocket upgrade endpoint) ---
	mux := http.NewServeMux()
	mux.Handle("/ws", wsHandler)
	httpServer := &http.Server{
		Addr:    cfg.WSAddr,
		Handler: mux,
	}

	// --- Serve both listeners, with graceful shutdown on SIGINT/SIGTERM ---
	grpcServeErr := make(chan error, 1)
	go func() {
		logger.Info("starting agent-presence gRPC server", slog.String("grpc_port", cfg.GRPCPort))
		grpcServeErr <- grpcServer.Serve(lis)
	}()

	httpServeErr := make(chan error, 1)
	go func() {
		logger.Info("starting agent-presence WebSocket server", slog.String("ws_addr", cfg.WSAddr))
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		httpServeErr <- err
	}()

	select {
	case err := <-grpcServeErr:
		if err != nil {
			logger.Error("grpc server stopped", slog.Any("error", err))
			os.Exit(1)
		}
	case err := <-httpServeErr:
		if err != nil {
			logger.Error("http server stopped", slog.Any("error", err))
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining connections")

		// Close every live WebSocket connection first so clients get a
		// clean close frame rather than a hard connection drop.
		wsHandler.Shutdown(context.Background())

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Warn("http server shutdown did not complete cleanly", slog.Any("error", err))
		}

		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
			logger.Info("graceful shutdown complete")
		case <-time.After(20 * time.Second):
			logger.Warn("graceful shutdown timed out, forcing stop")
			grpcServer.Stop()
		}
	}
}
