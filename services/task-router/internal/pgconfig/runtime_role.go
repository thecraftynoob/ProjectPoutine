package pgconfig

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgconn"
)

// pgExecutor is satisfied by both *pgxpool.Pool and pgx.Tx (the two types
// this package's Migrate ever calls ensureRuntimeRole/
// grantRuntimeRolePrivileges with), so both functions below work whether
// a given service's Migrate runs them directly against its pool or inside
// an advisory-lock-guarded transaction, without depending on pgxpool here.
type pgExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// RuntimeRole is the name of the shared, non-superuser Postgres role every
// service's steady-state (post-migration) queries connect as -- see this
// file's doc comment on ensureRuntimeRole for the full story. Exported so
// services/task-router/cmd/main.go can reference it in log/error messages
// without hardcoding the literal a second time.
const RuntimeRole = "ccaas_app"

// runtimeRolePasswordEnv is the environment variable every service's
// migrate.go reads the runtime role's password from when provisioning it.
// Sourced from the SAME value services/task-router/cmd/main.go composes
// its runtime-role DSN from (RUNTIME_POSTGRES_DSN's embedded password, or
// the discrete POSTGRES_RUNTIME_PASSWORD Kubernetes Secret key -- see
// deploy/k8s/infra-secret.example.yaml and each deployment.yaml), so the
// role this Migrate call creates always matches the password the
// service's real runtime pool will authenticate with.
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
// the table owner, never a superuser -- so every RLS policy in this repo
// (task_router_queues/statuses/attributes here, tenant_identity_users in
// Tenant & Identity) was silently a no-op against real traffic. This
// mirrors services/tenant-identity/internal/pgstore/pgstore_test.go's
// rlsTestRole/ensureRLSTestRole pattern exactly, but as the role real
// services actually connect as at runtime, not just a test-only role.
//
// Idempotent via CREATE ROLE IF NOT EXISTS (materialized as an EXISTS
// check inside a DO block, since Postgres has no native
// "CREATE ROLE IF NOT EXISTS" syntax): safe for every one of this repo's
// services to attempt at their own startup, since roles are
// cluster-global, not per-database -- whichever service's Migrate runs
// first actually creates it, every other service's call is then a no-op.
// A DO block's body runs inside one implicit transaction, so the
// EXISTS-check-then-CREATE is atomic against another concurrent session
// doing the same thing (unlike this repo's own CREATE TABLE IF NOT
// EXISTS/schema_migrations history, which needed pg_advisory_xact_lock
// specifically because two sessions issuing CREATE TABLE concurrently can
// both pass the EXISTS check before either commits, racing on Postgres's
// catalog unique index -- CREATE ROLE takes a stronger, cluster-wide lock
// for its whole duration for exactly this reason, per Postgres's own
// documented role-creation concurrency behavior, so no extra advisory
// lock is needed here).
//
// Must run as a role with CREATEROLE/superuser privilege -- callers pass
// the existing superuser ("ccaas") pool already used for migrations/DDL,
// never the runtime role's own pool.
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
		return fmt.Errorf("pgconfig: ensure runtime role %s: %w", RuntimeRole, err)
	}
	return nil
}

// grantRuntimeRolePrivileges grants RuntimeRole exactly the privileges
// Task Router's own queries need on Task Router's own tables -- SELECT,
// INSERT, UPDATE, DELETE on task_router_queues, task_router_statuses,
// task_router_attributes, and this package's own
// task_router_schema_migrations tracking table (the migration-check/
// record logic in Migrate above queries and writes it every run, so it
// needs the same grants as any other table this service's runtime pool
// touches).
//
// Deliberately scoped to ONLY this service's own tables, mirroring
// CLAUDE.md Rule 3's per-service table ownership: this function must
// never grant privileges on another service's tables (e.g.
// tenant_identity_users) -- see ARCHITECTURE_FLOW.md §5's table-ownership
// map for the authoritative per-service table list.
func grantRuntimeRolePrivileges(ctx context.Context, pool pgExecutor) error {
	const tables = `task_router_queues, task_router_statuses, task_router_attributes, task_router_schema_migrations`
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`GRANT SELECT, INSERT, UPDATE, DELETE ON %s TO %s`,
		tables, runtimeRoleQuoteIdent(RuntimeRole),
	)); err != nil {
		return fmt.Errorf("pgconfig: grant runtime role privileges: %w", err)
	}
	return nil
}

// runtimeRoleQuoteIdent and runtimeRoleQuoteLiteral do the minimum
// quoting needed for the fixed, hardcoded RuntimeRole identifier/literal
// above -- not a general-purpose SQL-safety helper (this package's real
// queries all use $N parameters; this is startup-time DDL bootstrapping
// for a constant name). Functionally identical to (and named after the
// same pattern as) the equivalent helpers in every other service's
// runtime_role.go, each declared independently per-package rather than
// shared via a new pkg, consistent with this repo's existing
// per-service-hand-rolled-migration-runner precedent (no shared migration
// framework dependency).
func runtimeRoleQuoteIdent(s string) string   { return `"` + s + `"` }
func runtimeRoleQuoteLiteral(s string) string { return `'` + s + `'` }
