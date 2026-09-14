# background-worker-pool

**Responsibility:** Executes transactional, non-real-time jobs pulled from a
Database-as-a-Queue (architecture doc Section 3.3): post-call wrap-up data
sync, webhook delivery to tenant systems, billing/usage-metering rollups,
scheduled report generation.

**State:** Stateless (horizontally scaled workers; coordination via
Postgres row locking — `FOR UPDATE SKIP LOCKED` — not app state).

**Status:** first real milestone done. A durable NATS JetStream consumer
(`internal/wrapupsync.Consumer`, fixed consumer name
`background-worker-pool-wrapup`) subscribes to Task Router's
`tenant.*.task.completed` event and enqueues a `wrapup_sync` job into this
service's own `background_jobs` table (`internal/pgstore` — the first real
owner/runner of the Database-as-a-Queue migration; see that package's doc
comment for the migration-ownership decision). A `pkg/pgqueue.Poller`
claims pending jobs and dispatches to `internal/wrapupsync.Handler`, which
looks up a tenant's configured wrap-up target URL
(`background_worker_pool_wrapup_targets` — a documented stand-in for real
tenant settings, see `GAPS.md`) and POSTs the job payload as JSON, with
retry/backoff on failure (fixed linear backoff, capped max attempts — see
`GAPS.md`). Only `job_type = "wrapup_sync"` has a real handler this
milestone; the dispatch structure supports adding more later
(`webhook_delivery`, `billing_rollup`) without restructuring. See
`ARCHITECTURE_FLOW.md` §2's "Subscribed by Background Worker Pool"
subsection for the full design writeup.
