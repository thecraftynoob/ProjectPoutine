// Command tenant-identity is the production entrypoint for the Tenant &
// Identity Management service (architecture doc Section 2.2): CRUD for
// tenant configuration, user/agent identity, RBAC, and JWT issuance. This
// is intentionally the most upstream service -- no dependency on any other
// domain service.
//
// Scope for this milestone (see services/tenant-identity/README.md):
// core identity + JWT issuance only. Tenant CRUD, user/agent identity with
// username+bcrypt-password login, simple string-tag RBAC roles, and
// short-lived JWT issuance via an ECDSA P-256 keypair this service
// generates (on first startup) and persists in Postgres. Real validation
// of these tokens by pkg/tenantctx or any other service's auth path is
// explicitly a deferred follow-up milestone -- this service issues and can
// verify its own tokens end-to-end, but nothing else in the topology
// consumes them yet.
//
// Wiring:
//   - PostgreSQL (via pkg/pgtenant for tenant-scoped user records, and a
//     second raw pgxpool.Pool for the non-tenant-scoped tenants registry
//     and signing-keypair tables -- see internal/pgstore) is the system of
//     record (architecture doc Section 3.1 Tier 2).
//   - internal/authn wraps bcrypt password hashing and pkg/jwtauth.Signer
//     for JWT minting.
//   - tenantidentity.v1's TenantService and IdentityService gRPC services
//     are registered alongside the shared tenant-context interceptors --
//     EXCEPT for the RPCs that are themselves how a caller first
//     establishes tenant/token context (CreateTenant, GetTenant,
//     ListTenants, CreateUser, Login), which are exempted by full method
//     name via pkg/tenantctx's exemptMethods parameter, the same mechanism
//     already used to exempt the standard health check.
//   - On startup, this service loads its persisted ECDSA signing keypair
//     (generating one on first-ever startup if none exists) and logs the
//     PEM-encoded public key prominently for manual distribution -- see
//     deploy/k8s/tenant-identity-public-key.example.yaml and the service
//     README for the full reasoning and the manual operator step this
//     requires today.
package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	tenantidentityv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/tenant-identity/v1"
	"github.com/thecraftynoob/ProjectPoutine/pkg/config"
	"github.com/thecraftynoob/ProjectPoutine/pkg/health"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/pkg/pgtenant"
	"github.com/thecraftynoob/ProjectPoutine/pkg/tenantctx"
	"github.com/thecraftynoob/ProjectPoutine/services/tenant-identity/internal/authn"
	"github.com/thecraftynoob/ProjectPoutine/services/tenant-identity/internal/grpcapi"
	"github.com/thecraftynoob/ProjectPoutine/services/tenant-identity/internal/pgstore"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

type serviceConfig struct {
	GRPCPort    string `env:"TENANT_IDENTITY_GRPC_PORT" envDefault:"50051"`
	PostgresDSN string `env:"POSTGRES_DSN,required"`
	// JWTTTLSeconds is the short-lived JWT expiry (architecture doc
	// Section 2.2: "issues short-lived JWTs"). Default 1 hour.
	JWTTTLSeconds int `env:"TENANT_IDENTITY_JWT_TTL_SECONDS" envDefault:"3600"`
	// JWTIssuer is stamped into the standard `iss` claim for diagnostics;
	// see pkg/jwtauth.Signer's doc comment -- verification never checks
	// it.
	JWTIssuer string `env:"TENANT_IDENTITY_JWT_ISSUER" envDefault:"tenant-identity"`
}

// tenantctxExemptMethods lists the tenantidentity.v1 RPCs that must be
// reachable with no established tenant/token context, because they are
// themselves how a caller first obtains one -- see
// proto/tenant-identity/v1/tenant_identity.proto's file-level note for the
// full reasoning per RPC. GetUser/ListUsers are deliberately NOT in this
// list: they operate within an already-established tenant context and are
// enforced like every other service's steady-state RPCs.
var tenantctxExemptMethods = []string{
	"/tenantidentity.v1.TenantService/CreateTenant",
	"/tenantidentity.v1.TenantService/GetTenant",
	"/tenantidentity.v1.TenantService/ListTenants",
	"/tenantidentity.v1.IdentityService/CreateUser",
	"/tenantidentity.v1.IdentityService/Login",
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

	// --- Postgres ---
	// Two pools against the same database, mirroring task-router's
	// cmd/main.go pattern: pgtenant.Pool for tenant-scoped queries
	// (users), and a raw pgxpool.Pool for migrations plus the two
	// genuinely non-tenant-scoped tables (tenants registry, signing
	// keypair) that have no tenant_id to funnel through
	// pgtenant.WithTenant with.
	pgPool, err := pgtenant.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("failed to connect to postgres", slog.Any("error", err))
		os.Exit(1)
	}
	defer pgPool.Close()

	rawPool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("failed to connect raw postgres pool", slog.Any("error", err))
		os.Exit(1)
	}
	defer rawPool.Close()

	if err := pgstore.Migrate(ctx, rawPool); err != nil {
		logger.Error("failed to run postgres migrations", slog.Any("error", err))
		os.Exit(1)
	}

	tenantStore := pgstore.NewTenantStore(rawPool)
	userStore := pgstore.NewUserStore(pgPool)
	keyStore := pgstore.NewKeyStore(rawPool)

	// --- JWT signing keypair: load or generate-on-first-startup ---
	privateKey, err := keyStore.LoadOrGenerate(ctx)
	if err != nil {
		logger.Error("failed to load or generate signing keypair", slog.Any("error", err))
		os.Exit(1)
	}

	publicKeyDER, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		logger.Error("failed to marshal public key", slog.Any("error", err))
		os.Exit(1)
	}
	publicKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicKeyDER})

	// Logged prominently: this PEM must be manually copied into
	// deploy/k8s/tenant-identity-public-key.example.yaml (or a
	// git-ignored non-example copy of it) and applied by an operator so
	// other services can eventually verify tokens this instance issues.
	// See that manifest and the service README for the full reasoning
	// behind this manual-for-now distribution step.
	logger.Info("==================== TENANT & IDENTITY SIGNING PUBLIC KEY ====================")
	logger.Info("copy the PEM block below into deploy/k8s/tenant-identity-public-key.example.yaml (see that file's comments) and `kubectl apply` it manually -- this is a manual distribution step for this milestone")
	logger.Info("\n" + string(publicKeyPEM))
	logger.Info("===============================================================================")

	signer := jwtauth.NewSigner(privateKey, cfg.JWTIssuer)
	tokenIssuer := authn.NewTokenIssuer(signer, time.Duration(cfg.JWTTTLSeconds)*time.Second)

	// --- gRPC server ---
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("failed to listen", slog.Any("error", err))
		os.Exit(1)
	}

	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(tenantctx.UnaryServerInterceptor(tenantctxExemptMethods...)),
		grpc.ChainStreamInterceptor(tenantctx.StreamServerInterceptor()),
	)

	tenantServer := &grpcapi.TenantServer{
		Tenants: tenantStore,
		Logger:  logger,
	}
	identityServer := &grpcapi.IdentityServer{
		Tenants: tenantStore,
		Users:   userStore,
		Tokens:  tokenIssuer,
		Logger:  logger,
	}
	tenantidentityv1.RegisterTenantServiceServer(server, tenantServer)
	tenantidentityv1.RegisterIdentityServiceServer(server, identityServer)
	health.Register(server)

	// --- Serve, with graceful shutdown on SIGINT/SIGTERM ---
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("starting tenant-identity", slog.String("grpc_port", cfg.GRPCPort))
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
