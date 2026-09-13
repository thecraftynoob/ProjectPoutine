# background-worker-pool

**Responsibility:** Executes transactional, non-real-time jobs pulled from a
Database-as-a-Queue (architecture doc Section 3.3): post-call wrap-up data
sync, webhook delivery to tenant systems, billing/usage-metering rollups,
scheduled report generation.

**State:** Stateless (horizontally scaled workers; coordination via
Postgres row locking — `FOR UPDATE SKIP LOCKED` — not app state).

**Status:** scaffold only. gRPC server with health check and tenant-context
interceptors wired. The reusable `pgqueue.Poller` type and `background_jobs`
migration already exist in `/pkg/pgqueue`, but this service does not yet
wire a Postgres connection or register any job-type handlers — a worker
with no handlers registered has nothing useful to poll for, so that wiring
is deferred to the milestone that defines the first real job type.
