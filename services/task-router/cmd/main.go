// Command task-router is the production entrypoint for the Task Router
// service (architecture doc Section 2.2 / TASK_ROUTER_SPECIFICATION.md):
// the routing/matching "brain" that matches queued Tasks to available
// Agents.
//
// Wiring:
//   - Redis (github.com/redis/go-redis/v9) backs the hot-path domain
//     (internal/redisdomain): Agent, Task, Reservation live state, one
//     atomic Lua script per mutation.
//   - PostgreSQL (via pkg/pgtenant, RLS-protected) backs the low-change
//     admin config registries (internal/pgconfig): Queue, Status,
//     Attribute.
//   - NATS JetStream (via pkg/eventbus) receives the full domain event
//     catalog (internal/events) for external consumers.
//   - Both taskrouter.v1 gRPC services (TaskRouterService,
//     TaskRouterAdminService) are registered alongside the shared
//     tenant-context interceptors and standard health check.
//   - A background goroutine subscribes to Redis keyspace notifications
//     for reservation-expiry sweeping (spec Section 4.3) -- see
//     internal/redisdomain/expiry.go and this service's README for the
//     required `notify-keyspace-events Ex` Redis configuration.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	"github.com/thecraftynoob/ProjectPoutine/pkg/config"
	"github.com/thecraftynoob/ProjectPoutine/pkg/eventbus"
	"github.com/thecraftynoob/ProjectPoutine/pkg/health"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/pkg/pgtenant"
	"github.com/thecraftynoob/ProjectPoutine/pkg/tenantctx"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/events"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/grpcapi"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/pgconfig"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/redisdomain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

type serviceConfig struct {
	GRPCPort    string `env:"TASK_ROUTER_GRPC_PORT" envDefault:"50054"`
	RedisAddr   string `env:"REDIS_ADDR" envDefault:"localhost:6379"`
	RedisDB     int    `env:"TASK_ROUTER_REDIS_DB" envDefault:"0"`
	NATSURL     string `env:"NATS_URL" envDefault:"nats://localhost:4222"`
	PostgresDSN string `env:"POSTGRES_DSN,required"`
	// ReservationTTLSeconds is spec Section 2.3's configurable Reservation
	// TTL, default 30 seconds.
	ReservationTTLSeconds int `env:"TASK_ROUTER_RESERVATION_TTL_SECONDS" envDefault:"30"`
	// AgentPresenceAddr is where Agent & Presence Service's PresenceService
	// gRPC server can be reached. Not dialed by this service today -- Task
	// Router only publishes events; a future Agent & Presence Service
	// milestone is the documented consumer that relays them to connected
	// Agent Desktop clients (spec Section 3.5 is explicitly not
	// implemented here).
	AgentPresenceAddr string `env:"AGENT_PRESENCE_GRPC_ADDR" envDefault:"localhost:50055"`
	// BootstrapTenantID, if set, causes this service to eagerly run
	// Postgres migrations and seed default statuses (spec Section 2.5)
	// for one tenant at startup -- a convenience for local/dev
	// environments that don't yet have a dedicated tenant-provisioning
	// lifecycle hook to seed defaults from (see pgconfig.EnsureDefaultStatuses'
	// doc comment). Safe to leave unset; every tenant's default statuses
	// are also lazily ensured on that tenant's first SetAgentStatus/
	// CreateAgent-adjacent admin call in a later milestone if desired --
	// today, an operator invoking RegisterStatus manually for a new
	// tenant is an equally valid path, since the registry is just an
	// open allow-list.
	BootstrapTenantID string `env:"TASK_ROUTER_BOOTSTRAP_TENANT_ID"`
	// JWTPublicKeyPath points at the PEM-encoded ECDSA public key Tenant &
	// Identity Management issues tokens with, mounted from the
	// tenant-identity-public-key ConfigMap (see
	// deploy/k8s/tenant-identity-public-key.example.yaml and
	// deploy/k8s/task-router/deployment.yaml). Required: this service
	// cannot verify any caller's JWT, and therefore cannot safely accept
	// any non-exempt RPC, without it.
	JWTPublicKeyPath string `env:"JWT_PUBLIC_KEY_PATH,required"`
	// TenantIdentityGRPCAddr and ServiceSharedSecret are accepted for
	// parity with every other service's deployment.yaml and forward
	// compatibility, but this service has no steady-state call to make
	// into another service's gRPC API today -- pkg/svcauth is what a
	// future call site would use to obtain and attach a service token
	// via Tenant & Identity's IssueServiceToken RPC, following the same
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

	// --- Postgres (admin config registries) ---
	pgPool, err := pgtenant.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("failed to connect to postgres", slog.Any("error", err))
		os.Exit(1)
	}
	defer pgPool.Close()

	rawPool, err := connectRawPgxPool(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("failed to connect raw postgres pool for migrations", slog.Any("error", err))
		os.Exit(1)
	}
	defer rawPool.Close()
	if err := pgconfig.Migrate(ctx, rawPool); err != nil {
		logger.Error("failed to run postgres migrations", slog.Any("error", err))
		os.Exit(1)
	}

	registry := pgconfig.NewRegistry(pgPool)

	if cfg.BootstrapTenantID != "" {
		tid, err := uuid.Parse(cfg.BootstrapTenantID)
		if err != nil {
			logger.Error("invalid TASK_ROUTER_BOOTSTRAP_TENANT_ID", slog.Any("error", err))
			os.Exit(1)
		}
		if err := registry.EnsureDefaultStatuses(ctx, tid); err != nil {
			logger.Error("failed to seed default statuses for bootstrap tenant", slog.Any("error", err))
			os.Exit(1)
		}
		logger.Info("seeded default statuses for bootstrap tenant", slog.String("tenant_id", tid.String()))
	}

	// --- Redis (hot-path domain) ---
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Error("failed to connect to redis", slog.Any("error", err))
		os.Exit(1)
	}
	defer redisClient.Close()

	store := redisdomain.NewStore(redisClient, time.Duration(cfg.ReservationTTLSeconds)*time.Second)

	// --- NATS JetStream (event publishing) ---
	natsClient, err := eventbus.Connect(cfg.NATSURL)
	if err != nil {
		logger.Error("failed to connect to nats", slog.Any("error", err))
		os.Exit(1)
	}
	defer natsClient.Close()

	publisher := events.NewPublisher(natsClient)
	if err := publisher.EnsureStream(ctx); err != nil {
		logger.Error("failed to ensure event stream", slog.Any("error", err))
		os.Exit(1)
	}

	// --- JWT verification (Layer 2 enforcement, architecture doc Section
	// 1.1) ---
	verifier, err := jwtauth.LoadVerifierFromFile(cfg.JWTPublicKeyPath)
	if err != nil {
		logger.Error("failed to load JWT verifier -- refusing to start without one (fail closed)", slog.Any("error", err))
		os.Exit(1)
	}

	if cfg.TenantIdentityGRPCAddr != "" && cfg.ServiceSharedSecret != "" {
		logger.Info("service-to-service auth configured", slog.String("tenant_identity_addr", cfg.TenantIdentityGRPCAddr))
	}

	// --- gRPC server ---
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("failed to listen", slog.Any("error", err))
		os.Exit(1)
	}

	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(tenantctx.UnaryServerInterceptor(verifier)),
		grpc.ChainStreamInterceptor(tenantctx.StreamServerInterceptor(verifier)),
	)

	taskRouterServer := &grpcapi.TaskRouterServer{
		Store:    store,
		Registry: registry,
		Events:   publisher,
		Logger:   logger,
	}
	adminServer := &grpcapi.TaskRouterAdminServer{
		Registry: registry,
		Store:    store,
		Logger:   logger,
	}
	taskrouterv1.RegisterTaskRouterServiceServer(server, taskRouterServer)
	taskrouterv1.RegisterTaskRouterAdminServiceServer(server, adminServer)
	health.Register(server)

	// --- Reservation expiry subscriber (spec Section 4.3) ---
	go func() {
		err := redisdomain.SubscribeExpiry(ctx, redisClient, cfg.RedisDB, logger, func(exp redisdomain.ExpiredReservation) {
			handleExpiredReservation(ctx, logger, store, publisher, exp)
		})
		if err != nil && ctx.Err() == nil {
			logger.Error("reservation expiry subscriber stopped unexpectedly", slog.Any("error", err))
		}
	}()

	// --- Serve, with graceful shutdown on SIGINT/SIGTERM ---
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("starting task-router", slog.String("grpc_port", cfg.GRPCPort))
		serveErr <- server.Serve(lis)
	}()

	select {
	case err := <-serveErr:
		if err != nil {
			logger.Error("server stopped", slog.Any("error", err))
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining in-flight work")
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

// connectRawPgxPool opens a second, un-tenant-scoped pgxpool.Pool
// connection purely for running schema migrations (DDL), since
// pgtenant.Pool intentionally keeps its underlying pgxpool.Pool
// unexported and every query path scoped through WithTenant's
// per-transaction app.current_tenant setting -- migrations are schema
// changes, not tenant data, so they legitimately need an unscoped
// connection. Both pools point at the same database; this one is closed
// immediately after Migrate returns in spirit (deferred to shutdown here
// for simplicity, since it is a small, idle connection pool that costs
// nothing to keep open for the service's lifetime).
func connectRawPgxPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, dsn)
}

// handleExpiredReservation resolves one expired reservation via the same
// atomic reject path a manual reject uses (spec Section 4.3), tagged
// reason=expired, and publishes the corresponding event plus re-triggers
// the matching pass -- mirroring grpcapi.TaskRouterServer.RejectReservation's
// side effects exactly, since this is a system-triggered equivalent of
// that capability (spec Section 3.4).
func handleExpiredReservation(ctx context.Context, logger *slog.Logger, store *redisdomain.Store, publisher *events.Publisher, exp redisdomain.ExpiredReservation) {
	tid, err := uuid.Parse(exp.TenantID)
	if err != nil {
		logger.Error("invalid tenant id in expiry notification", slog.String("tenant_id", exp.TenantID), slog.Any("error", err))
		return
	}

	result, err := store.RejectReservation(ctx, exp.TenantID, exp.ReservationID, redisdomain.ReasonExpired)
	if err != nil {
		logger.Error("failed to resolve expired reservation", slog.String("reservation_id", exp.ReservationID), slog.Any("error", err))
		return
	}
	if !result.Resolved {
		// Already resolved by something else (accepted, manually
		// rejected, or agent-deleted) between the TTL firing and this
		// handler running -- exactly the race spec Section 5.4 rule 4
		// anticipates. No-op.
		return
	}

	if err := publisher.ReservationRejected(ctx, tid, exp.ReservationID, result.TaskID, result.AgentID, string(redisdomain.ReasonExpired)); err != nil {
		logger.Error("publish reservation rejected (expired) failed", slog.Any("error", err))
	}

	outcomes, err := store.EvaluateOnce(ctx, exp.TenantID)
	if err != nil {
		logger.Error("matching pass after expiry failed", slog.Any("error", err))
		return
	}
	for _, o := range outcomes {
		expiresAt := ""
		if o.Reservation.ExpiresAt != nil {
			expiresAt = o.Reservation.ExpiresAt.Format(time.RFC3339Nano)
		}
		if err := publisher.ReservationCreated(ctx, tid, o.Reservation.ReservationID, o.Reservation.TaskID, o.Reservation.AgentID, expiresAt); err != nil {
			logger.Error("publish reservation created (post-expiry match) failed", slog.Any("error", err))
		}
	}
}
