package pgstore

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thecraftynoob/ProjectPoutine/pkg/pgtenant"
)

// testDSN returns the docker-compose Postgres DSN used across this repo's
// local dev (.env.local.example), overridable via TEST_POSTGRES_DSN. Tests
// using it are integration-style and skip (not fail) if Postgres is
// unreachable, so `go test ./...` remains runnable without live infra --
// consistent with this repo's stated tradeoff for DB-touching tests.
func testDSN() string {
	if dsn := os.Getenv("TEST_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://ccaas:ccaas_dev_password@localhost:5432/ccaas?sslmode=disable"
}

// rlsTestRole is a dedicated, deliberately NON-superuser, NOBYPASSRLS
// Postgres role used only by tenantPool in tests, instead of connecting as
// the docker-compose "ccaas" role directly.
//
// This matters because docker-compose's postgres image always creates its
// POSTGRES_USER as a superuser (that's simply how the official image's
// bootstrap works), and Postgres superusers unconditionally bypass Row-
// Level Security -- ALTER TABLE ... FORCE ROW LEVEL SECURITY notwithstanding,
// per Postgres's documented behavior, FORCE only affects the table OWNER,
// never a superuser. Connecting tenantPool as "ccaas" would therefore make
// every RLS-isolation assertion in this file pass VACUOUSLY (the query
// really does return the expected rows, but only because Postgres never
// applied the tenant_isolation policy in the first place -- it would
// "pass" identically even with the policy deleted entirely). This role
// exists so the RLS isolation tests below actually exercise the policy
// tenant_identity_users declares, the same way a real, correctly-deployed
// service would never connect as a superuser.
const rlsTestRole = "tenant_identity_rls_test_role"

// ensureRLSTestRole idempotently creates rlsTestRole (NOSUPERUSER,
// NOBYPASSRLS -- the Postgres default for BYPASSRLS is already "no", listed
// explicitly here for clarity) and grants it exactly the privileges
// internal/pgstore's queries need on tenant_identity_users, mirroring what
// a real deployment's application role would be provisioned with.
func ensureRLSTestRole(ctx context.Context, t *testing.T, rawPool *pgxpool.Pool) {
	t.Helper()
	if _, err := rawPool.Exec(ctx, fmt.Sprintf(`
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %[1]s) THEN
				CREATE ROLE %[2]s LOGIN PASSWORD %[1]s NOSUPERUSER NOBYPASSRLS;
			END IF;
		END
		$$;
	`, pgQuoteLiteral(rlsTestRole), pgQuoteIdent(rlsTestRole))); err != nil {
		t.Fatalf("create rls test role: %v", err)
	}
	if _, err := rawPool.Exec(ctx, fmt.Sprintf(
		`GRANT SELECT, INSERT, UPDATE, DELETE ON tenant_identity_users TO %s`,
		pgQuoteIdent(rlsTestRole),
	)); err != nil {
		t.Fatalf("grant rls test role privileges: %v", err)
	}
}

// pgQuoteIdent and pgQuoteLiteral do the minimum quoting needed for the
// fixed, hardcoded rlsTestRole identifier/literal above -- not a general-
// purpose SQL-safety helper (this package's real queries all use $N
// parameters; this is test-only DDL bootstrapping for a constant name).
func pgQuoteIdent(s string) string   { return `"` + s + `"` }
func pgQuoteLiteral(s string) string { return `'` + s + `'` }

// rlsTestDSN rewrites testDSN() to authenticate as rlsTestRole instead of
// whatever role testDSN() otherwise specifies, so tenantPool in tests
// always connects as a genuinely RLS-subject role. See rlsTestRole's doc
// comment for why this matters.
func rlsTestDSN() string {
	u, err := url.Parse(testDSN())
	if err != nil {
		// testDSN() is either this package's own hardcoded default or an
		// operator-supplied TEST_POSTGRES_DSN; either way an unparseable
		// value is a setup error worth failing loudly on rather than
		// silently falling back to a superuser connection that would
		// defeat the whole point of rlsTestRole.
		panic(fmt.Sprintf("pgstore: TEST_POSTGRES_DSN is not a valid URL: %v", err))
	}
	u.User = url.UserPassword(rlsTestRole, rlsTestRole)
	return u.String()
}

// connectForTest opens both pool flavors this package needs and runs
// migrations, skipping the test entirely if Postgres isn't reachable.
// rawPool connects as the configured (superuser, in local dev)
// credentials for migrations/DDL/setup; tenantPool connects as
// rlsTestRole so RLS is genuinely enforced -- see rlsTestRole's doc
// comment.
func connectForTest(t *testing.T) (raw *pgxpool.Pool, tenantPool *pgtenant.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rawPool, err := pgxpool.New(ctx, testDSN())
	if err != nil {
		t.Skipf("skipping: cannot construct postgres pool: %v", err)
	}
	if err := rawPool.Ping(ctx); err != nil {
		rawPool.Close()
		t.Skipf("skipping: postgres unreachable: %v", err)
	}

	if err := Migrate(context.Background(), rawPool); err != nil {
		rawPool.Close()
		t.Fatalf("migrate: %v", err)
	}

	ensureRLSTestRole(context.Background(), t, rawPool)

	tp, err := pgtenant.Connect(context.Background(), rlsTestDSN())
	if err != nil {
		rawPool.Close()
		t.Fatalf("pgtenant.Connect: %v", err)
	}

	t.Cleanup(func() {
		tp.Close()
		rawPool.Close()
	})

	return rawPool, tp
}

func TestTenantCRUD(t *testing.T) {
	rawPool, _ := connectForTest(t)
	store := NewTenantStore(rawPool)
	ctx := context.Background()

	created, err := store.CreateTenant(ctx, "acme-corp-test")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if created.Name != "acme-corp-test" {
		t.Errorf("name: want acme-corp-test, got %q", created.Name)
	}
	if created.TenantID.String() == "" {
		t.Error("expected a generated tenant_id")
	}

	got, err := store.GetTenant(ctx, created.TenantID)
	if err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	if got.TenantID != created.TenantID {
		t.Errorf("tenant id mismatch: %v vs %v", got.TenantID, created.TenantID)
	}

	exists, err := store.TenantExists(ctx, created.TenantID)
	if err != nil {
		t.Fatalf("tenant exists: %v", err)
	}
	if !exists {
		t.Error("expected tenant to exist")
	}

	tenants, err := store.ListTenants(ctx)
	if err != nil {
		t.Fatalf("list tenants: %v", err)
	}
	found := false
	for _, tt := range tenants {
		if tt.TenantID == created.TenantID {
			found = true
		}
	}
	if !found {
		t.Error("expected created tenant to appear in ListTenants")
	}
}

func TestGetTenantNotFound(t *testing.T) {
	rawPool, _ := connectForTest(t)
	store := NewTenantStore(rawPool)

	_, err := store.GetTenant(context.Background(), uuid.New())
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestUserCRUDAndTenantIsolation(t *testing.T) {
	rawPool, tenantPool := connectForTest(t)
	tenantStore := NewTenantStore(rawPool)
	userStore := NewUserStore(tenantPool)
	ctx := context.Background()

	tenantA, err := tenantStore.CreateTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("create tenant a: %v", err)
	}
	tenantB, err := tenantStore.CreateTenant(ctx, "tenant-b")
	if err != nil {
		t.Fatalf("create tenant b: %v", err)
	}

	userA, err := userStore.CreateUser(ctx, tenantA.TenantID, "alice", "hash-a", []string{"admin"})
	if err != nil {
		t.Fatalf("create user in tenant a: %v", err)
	}

	// Same username in a DIFFERENT tenant must be allowed (unique per
	// tenant, not globally).
	if _, err := userStore.CreateUser(ctx, tenantB.TenantID, "alice", "hash-b", []string{"agent"}); err != nil {
		t.Fatalf("expected same username in different tenant to succeed, got: %v", err)
	}

	// Duplicate username within the SAME tenant must be rejected.
	if _, err := userStore.CreateUser(ctx, tenantA.TenantID, "alice", "hash-a-2", nil); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	// RLS isolation: looking up tenant A's user while scoped to tenant B
	// must not find it.
	if _, err := userStore.GetUser(ctx, tenantB.TenantID, userA.UserID); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound reading tenant A's user under tenant B's scope, got %v", err)
	}

	// But it IS visible correctly scoped to tenant A.
	got, err := userStore.GetUser(ctx, tenantA.TenantID, userA.UserID)
	if err != nil {
		t.Fatalf("get user in correct tenant: %v", err)
	}
	if got.Username != "alice" {
		t.Errorf("username: want alice, got %q", got.Username)
	}

	// GetUserByUsername (Login's lookup path).
	byUsername, err := userStore.GetUserByUsername(ctx, tenantA.TenantID, "alice")
	if err != nil {
		t.Fatalf("get user by username: %v", err)
	}
	if byUsername.UserID != userA.UserID {
		t.Errorf("expected same user id via GetUserByUsername")
	}

	// ListUsers scoped to tenant A must only show tenant A's user(s).
	listA, err := userStore.ListUsers(ctx, tenantA.TenantID)
	if err != nil {
		t.Fatalf("list users tenant a: %v", err)
	}
	for _, u := range listA {
		if u.TenantID != tenantA.TenantID {
			t.Errorf("tenant isolation violated: got user from tenant %v while scoped to %v", u.TenantID, tenantA.TenantID)
		}
	}
}

func TestKeyStoreLoadOrGenerateIsStableAcrossCalls(t *testing.T) {
	rawPool, _ := connectForTest(t)
	// Ensure a clean slate for this specific table so the test is
	// order-independent from other tests in this file.
	if _, err := rawPool.Exec(context.Background(), `DELETE FROM tenant_identity_signing_key`); err != nil {
		t.Fatalf("clear signing key table: %v", err)
	}

	store := NewKeyStore(rawPool)
	ctx := context.Background()

	first, err := store.LoadOrGenerate(ctx)
	if err != nil {
		t.Fatalf("first LoadOrGenerate: %v", err)
	}
	second, err := store.LoadOrGenerate(ctx)
	if err != nil {
		t.Fatalf("second LoadOrGenerate: %v", err)
	}

	if !first.PublicKey.Equal(&second.PublicKey) {
		t.Fatal("expected LoadOrGenerate to return the same persisted key across calls")
	}
}
