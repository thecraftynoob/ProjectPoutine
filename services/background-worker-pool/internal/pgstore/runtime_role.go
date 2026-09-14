package pgstore

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgconn"
)

// pgExecutor is satisfied by both *pgxpool.Pool and pgx.Tx. Migrate above
// calls ensureRuntimeRole/grantRuntimeRolePrivileges with a pgx.Tx (the
// same advisory-lock-guarded transaction it already opens for
// background_worker_pool_schema_migrations), so both functions are
// written against this minimal interface instead of depending on
// pgxpool.Pool directly.
type pgExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// RuntimeRole is the name of the shared, non-superuser Postgres role every
// service's steady-state (post-migration) queries connect as -- see this
// file's doc comment on ensureRuntimeRole for the full story. Exported so
// services/background-worker-pool/cmd/main.go can reference it in
// log/error messages without hardcoding the literal a second time.
const RuntimeRole = "ccaas_app"

// runtimeRolePasswordEnv is the environment variable every service's
// migrate.go reads the runtime role's password from when provisioning it.
// Sourced from the SAME value services/background-worker-pool/cmd/main.go
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
// Neither background_jobs nor background_worker_pool_wrapup_targets
// carries an RLS policy today (see this package's own doc comment), but
// this service still switches to RuntimeRole for connection-identity
// consistency across the whole platform, per this fix's scope decision:
// one connection identity everywhere closes the door on a future
// RLS-protected table silently inheriting the superuser-bypasses-RLS bug
// if a service is ever given one without someone remembering to check its
// connection role. See
// services/tenant-identity/internal/pgstore/runtime_role.go's
// ensureRuntimeRole for the full original-bug writeup (Tenant &
// Identity's tenant_identity_users and Task Router's RLS-protected
// tables are where superuser-bypasses-RLS was actually exploitable).
//
// Idempotent via CREATE ROLE IF NOT EXISTS (materialized as an EXISTS
// check inside a DO block, since Postgres has no native
// "CREATE ROLE IF NOT EXISTS" syntax): safe for every one of this repo's
// services to attempt at their own startup, since roles are
// cluster-global, not per-database -- whichever service's Migrate runs
// first actually creates it, every other service's call is then a no-op.
// A DO block's body runs inside one implicit transaction, so the
// EXISTS-check-then-CREATE is atomic against another concurrent session
// doing the same thing -- CREATE ROLE takes a stronger, cluster-wide lock
// for its whole duration, per Postgres's own documented role-creation
// concurrency behavior, so running this inside the pg_advisory_xact_lock
// transaction above is a convenience (reusing an already-open tx), not a
// correctness requirement.
//
// Must run as a role with CREATEROLE/superuser privilege -- callers pass
// the existing superuser ("ccaas") connection already used for
// migrations/DDL (here, the advisory-lock-guarded tx), never the runtime
// role's own pool.
func ensureRuntimeRole(ctx context.Context, exec pgExecutor) error {
	if _, err := exec.Exec(ctx, fmt.Sprintf(`
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
// Background Worker Pool's own queries need on Background Worker Pool's
// own tables -- SELECT, INSERT, UPDATE, DELETE on background_jobs,
// background_worker_pool_wrapup_targets, and this package's own
// background_worker_pool_schema_migrations tracking table (the
// migration-check/record logic in Migrate above queries and writes it
// every run, so it needs the same grants as any other table this
// service's runtime pool touches). background_jobs also needs the
// pgqueue.Poller's FOR UPDATE SKIP LOCKED claim query to work under the
// runtime role -- SELECT+UPDATE (both granted here) is exactly what that
// pattern requires, no separate grant needed.
//
// background_jobs.id is BIGSERIAL (see migrations/001_background_jobs.sql),
// which Postgres implements as an implicit sequence
// (background_jobs_id_seq) plus a column DEFAULT of nextval(...) on that
// sequence. A role only granted table-level INSERT cannot actually insert
// a row relying on that default -- nextval() additionally requires USAGE
// (or SELECT) on the sequence itself, checked independently of the
// table's own grants. Without this, pgqueue.Enqueue's
// "INSERT INTO background_jobs (tenant_id, job_type, payload, run_after)
// VALUES (...)" (deliberately omitting id, relying on the default) would
// fail under RuntimeRole with a permission-denied-for-sequence error the
// moment this fix's runtime-role switch landed -- granted explicitly here
// so that doesn't happen. tenants.tenant_id and
// tenant_identity_signing_key's PK both instead default via
// gen_random_uuid()/a fixed literal (see those migrations), which are
// plain function calls, not sequences, so no equivalent grant is needed
// for any other table in this repo.
//
// Deliberately scoped to ONLY this service's own tables, mirroring
// CLAUDE.md Rule 3's per-service table ownership: this function must
// never grant privileges on another service's tables -- see
// ARCHITECTURE_FLOW.md §5's table-ownership map for the authoritative
// per-service table list.
func grantRuntimeRolePrivileges(ctx context.Context, exec pgExecutor) error {
	const tables = `background_jobs, background_worker_pool_wrapup_targets, background_worker_pool_schema_migrations`
	if _, err := exec.Exec(ctx, fmt.Sprintf(
		`GRANT SELECT, INSERT, UPDATE, DELETE ON %s TO %s`,
		tables, runtimeRoleQuoteIdent(RuntimeRole),
	)); err != nil {
		return fmt.Errorf("pgstore: grant runtime role privileges: %w", err)
	}
	if _, err := exec.Exec(ctx, fmt.Sprintf(
		`GRANT USAGE, SELECT ON SEQUENCE background_jobs_id_seq TO %s`,
		runtimeRoleQuoteIdent(RuntimeRole),
	)); err != nil {
		return fmt.Errorf("pgstore: grant runtime role sequence privileges: %w", err)
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
