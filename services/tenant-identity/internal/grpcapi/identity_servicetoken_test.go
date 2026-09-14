package grpcapi

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"os"
	"testing"
	"time"

	tenantidentityv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/tenant-identity/v1"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/services/tenant-identity/internal/authn"
	"github.com/thecraftynoob/ProjectPoutine/services/tenant-identity/internal/pgstore"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testDSN mirrors internal/pgstore/pgstore_test.go's helper: integration-
// style tests that touch Postgres skip (not fail) when it's unreachable,
// so `go test ./...` stays runnable without live infra, overridable via
// TEST_POSTGRES_DSN.
func testDSN() string {
	if dsn := os.Getenv("TEST_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://ccaas:ccaas_dev_password@localhost:5432/ccaas?sslmode=disable"
}

func newTestIdentityServer(ctx context.Context, t *testing.T, sharedSecret string) (*IdentityServer, *pgstore.TenantStore, *jwtauth.Verifier) {
	t.Helper()

	pool, err := pgxpool.New(ctx, testDSN())
	if err != nil {
		t.Skipf("skipping: cannot construct postgres pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skipping: postgres unreachable: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pgstore.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tenantStore := pgstore.NewTenantStore(pool)
	// UserStore is deliberately left nil: IssueServiceToken never touches
	// it (it only checks tenant existence + the shared secret), and
	// constructing one requires a *pgtenant.Pool rather than the raw
	// *pgxpool.Pool this lightweight test helper opens.

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	signer := jwtauth.NewSigner(key, "identity-servicetoken-test")
	verifier := jwtauth.NewVerifier(&key.PublicKey)
	tokens := authn.NewTokenIssuer(signer, time.Hour)

	server := &IdentityServer{
		Tenants:             tenantStore,
		Tokens:              tokens,
		ServiceSharedSecret: sharedSecret,
	}
	return server, tenantStore, verifier
}

func TestIssueServiceToken_ValidCredentialSucceeds(t *testing.T) {
	ctx := context.Background()
	server, tenantStore, verifier := newTestIdentityServer(ctx, t, "correct-horse-battery-staple")

	tenant, err := tenantStore.CreateTenant(ctx, "acme-corp-servicetoken")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	resp, err := server.IssueServiceToken(ctx, &tenantidentityv1.IssueServiceTokenRequest{
		TenantId:      tenant.TenantID.String(),
		CallerService: "task-router",
		SharedSecret:  "correct-horse-battery-staple",
	})
	if err != nil {
		t.Fatalf("IssueServiceToken: %v", err)
	}
	if resp.GetToken() == "" {
		t.Fatal("expected non-empty token")
	}
	if !resp.GetExpiresAt().AsTime().After(time.Now()) {
		t.Fatalf("expected future expiry, got %v", resp.GetExpiresAt().AsTime())
	}

	// The returned token must actually verify and carry the right claims.
	claims, err := verifier.Verify(resp.GetToken())
	if err != nil {
		t.Fatalf("verify issued service token: %v", err)
	}
	if claims.TenantID != tenant.TenantID {
		t.Errorf("tenant id: want %v, got %v", tenant.TenantID, claims.TenantID)
	}
	if claims.Subject != "service:task-router" {
		t.Errorf("subject: want %q, got %q", "service:task-router", claims.Subject)
	}
	foundServiceRole := false
	for _, r := range claims.Roles {
		if r == "service" {
			foundServiceRole = true
		}
	}
	if !foundServiceRole {
		t.Errorf("expected roles to include \"service\", got %v", claims.Roles)
	}
}

func TestIssueServiceToken_InvalidCredentialRejected(t *testing.T) {
	ctx := context.Background()
	server, tenantStore, _ := newTestIdentityServer(ctx, t, "correct-horse-battery-staple")

	tenant, err := tenantStore.CreateTenant(ctx, "acme-corp-badcred")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	_, err = server.IssueServiceToken(ctx, &tenantidentityv1.IssueServiceTokenRequest{
		TenantId:      tenant.TenantID.String(),
		CallerService: "task-router",
		SharedSecret:  "wrong-secret",
	})
	if err == nil {
		t.Fatal("expected error for wrong shared secret, got nil")
	}
}

func TestIssueServiceToken_EmptyConfiguredSecretFailsClosed(t *testing.T) {
	ctx := context.Background()
	// Server configured with NO shared secret at all -- every call must
	// be rejected, including one supplying an empty string, rather than
	// an empty-matches-empty accidental bypass.
	server, tenantStore, _ := newTestIdentityServer(ctx, t, "")

	tenant, err := tenantStore.CreateTenant(ctx, "acme-corp-nosecret")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	_, err = server.IssueServiceToken(ctx, &tenantidentityv1.IssueServiceTokenRequest{
		TenantId:      tenant.TenantID.String(),
		CallerService: "task-router",
		SharedSecret:  "",
	})
	if err == nil {
		t.Fatal("expected error when no shared secret is configured, got nil")
	}
}

func TestIssueServiceToken_UnknownTenantRejected(t *testing.T) {
	ctx := context.Background()
	server, _, _ := newTestIdentityServer(ctx, t, "correct-horse-battery-staple")

	_, err := server.IssueServiceToken(ctx, &tenantidentityv1.IssueServiceTokenRequest{
		TenantId:      "00000000-0000-0000-0000-000000000000",
		CallerService: "task-router",
		SharedSecret:  "correct-horse-battery-staple",
	})
	if err == nil {
		t.Fatal("expected error for unknown tenant, got nil")
	}
}

func TestIssueServiceToken_MalformedTenantIDRejected(t *testing.T) {
	ctx := context.Background()
	server, _, _ := newTestIdentityServer(ctx, t, "correct-horse-battery-staple")

	_, err := server.IssueServiceToken(ctx, &tenantidentityv1.IssueServiceTokenRequest{
		TenantId:      "not-a-uuid",
		CallerService: "task-router",
		SharedSecret:  "correct-horse-battery-staple",
	})
	if err == nil {
		t.Fatal("expected error for malformed tenant_id, got nil")
	}
}

func TestIssueServiceToken_DefaultsCallerServiceWhenEmpty(t *testing.T) {
	ctx := context.Background()
	server, tenantStore, verifier := newTestIdentityServer(ctx, t, "correct-horse-battery-staple")

	tenant, err := tenantStore.CreateTenant(ctx, "acme-corp-nocaller")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	resp, err := server.IssueServiceToken(ctx, &tenantidentityv1.IssueServiceTokenRequest{
		TenantId:     tenant.TenantID.String(),
		SharedSecret: "correct-horse-battery-staple",
	})
	if err != nil {
		t.Fatalf("IssueServiceToken: %v", err)
	}
	claims, err := verifier.Verify(resp.GetToken())
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if claims.Subject != "service:unknown-service" {
		t.Errorf("subject: want %q, got %q", "service:unknown-service", claims.Subject)
	}
}
