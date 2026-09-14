package pgstore

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgconn"
)

// pgExecutor is satisfied by both *pgxpool.Pool and pgx.Tx. Migrate above
// calls ensureRuntimeRole/grantRuntimeRolePrivileges with a pgx.Tx (an
// advisory-lock-guarded transaction, to avoid a concurrent-GRANT race
// across this package's own tests and internal/grpcapi's independent
// Migrate caller -- see Migrate's doc comment), so both functions are
// written against this minimal interface instead of depending on
// pgxpool.Pool directly.
type pgExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// RuntimeRole is the name of the shared, non-superuser Postgres role every
// service's steady-state (post-migration) queries connect as -- see this
// file's doc comment on ensureRuntimeRole for the full story. Exported so
// services/tenant-identity/cmd/main.go can reference it in log/error
// messages without hardcoding the literal a second time.
const RuntimeRole = "ccaas_app"

// runtimeRoleAdvisoryLockKey guards Migrate's runtime-role
// creation/GRANT step (see migrate.go's Migrate doc comment) AND
// pgstore_test.go's ensureRLSTestRole -- deliberately the SAME key
// shared by both non-test and test code in this package, not two
// separate keys: this package's own tests and internal/grpcapi's
// independent Migrate-calling test package both GRANT privileges on
// tenant_identity_users concurrently under `go test ./...`'s default
// concurrent-package execution, and two GRANTs on the same table racing
// from two sessions hit Postgres error XX000 "tuple concurrently
// updated" (observed in practice) unless serialized against EACH OTHER,
// not just within their own call site.
const runtimeRoleAdvisoryLockKey = 0x54494452 // "TIDR" (Tenant Identity) in hex, mnemonic only

// runtimeRolePasswordEnv is the environment variable every service's
// migrate.go reads the runtime role's password from when provisioning it.
// Sourced from the SAME value services/tenant-identity/cmd/main.go
// composes its runtime-role DSN from (RUNTIME_POSTGRES_DSN's embedded
// password, or the discrete POSTGRES_RUNTIME_PASSWORD Kubernetes Secret
// key -- see deploy/k8s/infra-secret.example.yaml and each
// deployment.yaml), so the role this Migrate call creates always matches
// the password the service's real runtime pool will authenticate with.
//
// Falls back to this repo's existing dev-scale shared-secret default
// (mirroring docker-compose.yml's own POSTGRES_PASSWORD default of
// "ccaas_dev_password") ONLY so `go test ./...` and ad-hoc local `go run`
// against docker-compose Postgres work with zero required env setup --
// every real deployment (K8s) supplies POSTGRES_RUNTIME_PASSWORD
// explicitly via the ccaas-infra-secret Secret, exactly like
// POSTGRES_PASSWORD already does for the superuser role (CLAUDE.md Rule 2:
// a localhost-shaped default is fine as an envDefault fallback ONLY when
// every K8s manifest overrides it, which infra-secret.example.yaml/every
// deployment.yaml here does).
const runtimeRolePasswordEnv = "POSTGRES_RUNTIME_PASSWORD"

const defaultRuntimeRolePassword = "ccaas_app_dev_password"

func runtimeRolePassword() string {
	if v := os.Getenv(runtimeRolePasswordEnv); v != "" {
		return v
	}
	return defaultRuntimeRolePassword
}

// ensureRuntimeRole idempotently creates the shared RuntimeRole
// (NOSUPERUSER NOBYPASSRLS LOGIN) that every service in this repo connects
// as for its ongoing, non-migration queries -- see
// deploy/k8s/infra-config.yaml and ARCHITECTURE_FLOW.md §5 for the full
// design.
//
// Why this exists: every service historically connected to Postgres as
// "ccaas", which docker-compose's/Kubernetes' postgres:16 bootstrap always
// creates as a SUPERUSER (that's simply how POSTGRES_USER works on the
// official image). Postgres superusers unconditionally bypass Row-Level
// Security -- FORCE ROW LEVEL SECURITY notwithstanding, FORCE only binds
// the table owner, never a superuser -- so tenant_identity_users' RLS
// policy was silently a no-op against real traffic (ListUsers returned
// every tenant's users, not just the caller's). This mirrors this very
// package's own pgstore_test.go's rlsTestRole/ensureRLSTestRole pattern,
// but provisions the role the real service actually connects as at
// runtime, not a test-only role -- see runtimeRoleQuoteIdent/
// runtimeRoleQuoteLiteral below for why this file declares its own
// quoting helpers instead of reusing pgstore_test.go's identically-shaped
// ones.
//
// Idempotent via CREATE ROLE IF NOT EXISTS (materialized as an EXISTS
// check inside a DO block, since Postgres has no native
// "CREATE ROLE IF NOT EXISTS" syntax): safe for every one of this repo's
// services to attempt at their own startup, since roles are
// cluster-global, not per-database -- whichever service's Migrate runs
// first actually creates it, every other service's call is then a no-op.
// A DO block's body runs inside one implicit transaction, so the
// EXISTS-check-then-CREATE is atomic against another concurrent session
// doing the same thing -- CREATE ROLE itself takes a stronger,
// cluster-wide lock for its whole duration, per Postgres's own documented
// role-creation concurrency behavior, and would need no advisory lock on
// its own. The GRANT that follows it in Migrate is a different story --
// see Migrate's doc comment for why THAT step (not this one) is what
// actually needed the pg_advisory_xact_lock wrapping both.
//
// Must run as a role with CREATEROLE/superuser privilege -- callers pass
// the existing superuser ("ccaas") connection already used for
// migrations/DDL (here, an advisory-lock-guarded tx), never the runtime
// role's own pool.
func ensureRuntimeRole(ctx context.Context, pool pgExecutor) error {
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %[1]s) THEN
				CREATE ROLE %[2]s LOGIN PASSWORD %[3]s NOSUPERUSER NOBYPASSRLS;
			END IF;
		END
		$$;
	`, runtimeRoleQuoteLiteral(RuntimeRole), runtimeRoleQuoteIdent(RuntimeRole), runtimeRoleQuoteLiteral(runtimeRolePassword()))); err != nil {
		return fmt.Errorf("pgstore: ensure runtime role %s: %w", RuntimeRole, err)
	}
	return nil
}

// grantRuntimeRolePrivileges grants RuntimeRole exactly the privileges
// Tenant & Identity's own queries need on Tenant & Identity's own tables
// -- SELECT, INSERT, UPDATE, DELETE on tenants, tenant_identity_users,
// tenant_identity_signing_key, and this package's own
// tenant_identity_schema_migrations tracking table (the migration-check/
// record logic in Migrate above queries and writes it every run, so it
// needs the same grants as any other table this service's runtime pool
// touches). tenants and tenant_identity_signing_key carry no RLS policy
// (see migrations/001_tenants.sql and 003_signing_keypair.sql), but are
// granted the same way for connection-identity consistency, per this
// fix's platform-wide "every service's ongoing queries use the runtime
// role" scope decision -- not because they need RLS enforcement
// themselves.
//
// Deliberately scoped to ONLY this service's own tables, mirroring
// CLAUDE.md Rule 3's per-service table ownership: this function must
// never grant privileges on another service's tables (e.g.
// task_router_queues) -- see ARCHITECTURE_FLOW.md §5's table-ownership
// map for the authoritative per-service table list.
func grantRuntimeRolePrivileges(ctx context.Context, pool pgExecutor) error {
	const tables = `tenants, tenant_identity_users, tenant_identity_signing_key, tenant_identity_schema_migrations`
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`GRANT SELECT, INSERT, UPDATE, DELETE ON %s TO %s`,
		tables, runtimeRoleQuoteIdent(RuntimeRole),
	)); err != nil {
		return fmt.Errorf("pgstore: grant runtime role privileges: %w", err)
	}
	return nil
}

// runtimeRoleQuoteIdent and runtimeRoleQuoteLiteral do the minimum
// quoting needed for the fixed, hardcoded RuntimeRole identifier/literal
// above -- not a general-purpose SQL-safety helper (this package's real
// queries all use $N parameters; this is startup-time DDL bootstrapping
// for a constant name).
//
// Named distinctly from pgstore_test.go's pgQuoteIdent/pgQuoteLiteral
// (which do the identical two-line thing for rlsTestRole's DO block)
// rather than sharing one declaration: this file compiles into the real,
// non-test service binary, so it cannot depend on a _test.go symbol
// (_test.go files are excluded from non-test builds); and Go would
// reject two identically-named funcs in one package even under `go
// test`, which links both file sets together. rlsTestRole and
// RuntimeRole are deliberately separate roles for separate purposes (see
// each one's doc comment) -- this naming makes that separation obvious
// at the call site too, not just avoids a compile error.
func runtimeRoleQuoteIdent(s string) string   { return `"` + s + `"` }
func runtimeRoleQuoteLiteral(s string) string { return `'` + s + `'` }
