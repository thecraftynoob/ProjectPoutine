# task-router

**Responsibility:** Owns the routing algorithm: matches queued Tasks to
available Agents based on presence, queue membership, and per-channel
capacity (NOT skills/attributes -- see "Deferred scope" below). Full
functional behavior is specified in `TASK_ROUTER_SPECIFICATION.md` at the
repo root (`§1-§6` is the implemented contract; `§7` is an explicit
non-goal for this build).

**State:** Stateful. Redis is the system of record for the hot-path
domain (Agent, Task, Reservation live state) -- see architecture doc
Section 3.1 Tier 1. PostgreSQL (RLS-protected, via `pkg/pgtenant`) is the
system of record for the low-change admin config registries (Queue,
Status, Attribute) -- Tier 2.

**Status:** Full `§1-§6` domain implementation. Two gRPC services:

- `taskrouter.v1.TaskRouterService` -- hot-path domain (Agent CRUD/
  presence/capacity/queues, Task lifecycle, Reservation handshake,
  aggregate dashboard view).
- `taskrouter.v1.TaskRouterAdminService` -- config domain (Queue, Status,
  Attribute registries).

Proto source: `proto/task-router/v1/*.proto`. Generated Go:
`pkg/genproto/task-router/v1/`.

## Architecture

- **Redis** (`internal/redisdomain`): one embedded Lua script (`EVAL`) per
  state-changing mutation (`internal/redisdomain/scripts/*.lua`), so every
  mutation is atomic against concurrent access -- no read-modify-write
  from Go application code, ever, for capacity or uniqueness-on-create.
  The matching algorithm (`§4.1`'s `evaluate_once()`, implemented in
  `matcher.go`) is triggered **in-process, synchronously**, by every
  mutating gRPC handler whose mutation could plausibly create a new
  matching opportunity -- not via a NATS-consuming background loop.
- **Postgres** (`internal/pgconfig`): Queue/Status/Attribute registries,
  RLS-protected per `pkg/pgtenant`'s pattern. Migrations run automatically
  at service startup (`internal/pgconfig/migrate.go`, idempotent, tracked
  via a `schema_migrations` table).
- **NATS JetStream** (`internal/events`): the full event catalog from spec
  `§6.2`, published to one stream (`TASK_ROUTER_EVENTS`) covering
  `tenant.*.{task,agent,reservation}.>`.
- **Reservation expiry** (`§4.3`): via Redis-native TTL + keyspace
  notifications, NOT a fixed-interval sweep. Each `Offered` reservation
  gets a TTL-bearing sentinel key; this service subscribes to
  `__keyevent@<db>__:expired` and resolves the corresponding reservation
  through the same atomic Lua script a manual reject uses
  (`reject_reservation.lua`), tagged `reason="expired"`. Safe under
  multiple replicas: keyspace notifications fan out to every subscriber,
  but the script's atomic re-validation (`status == "Offered"`) is what
  prevents double-processing, not any external lock.

### Required Redis configuration

The Redis instance **must** have `notify-keyspace-events` including `Ex`
(expired-key events) enabled, or reservation expiry will silently never
fire. `docker-compose.yml`'s `redis` service is already configured with
`command: ["redis-server", "--notify-keyspace-events", "Ex"]` for local
development. In a managed/production Redis (e.g. ElastiCache), set the
equivalent parameter-group option.

## Configuration (environment variables)

| Variable | Default | Purpose |
|---|---|---|
| `TASK_ROUTER_GRPC_PORT` | `50054` | gRPC listen port |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `TASK_ROUTER_REDIS_DB` | `0` | Redis logical DB index |
| `NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `POSTGRES_DSN` | *(required)* | Postgres connection string |
| `TASK_ROUTER_RESERVATION_TTL_SECONDS` | `30` | Reservation TTL (spec `§2.3`) |
| `AGENT_PRESENCE_GRPC_ADDR` | `localhost:50055` | Not dialed today; reserved for a future milestone |
| `TASK_ROUTER_BOOTSTRAP_TENANT_ID` | *(unset)* | If set, seeds the four default statuses (spec `§2.5`) for this one tenant at startup -- a dev/local convenience; see below |

### Default Status seeding

Spec `§2.5` says the Status registry is "seeded on first run with four
defaults" (`Available`, `Break`, `Offline`, `Not Responding`). Since this
is a per-tenant registry and the platform has no dedicated
tenant-provisioning lifecycle hook yet to attach seeding to, this build
seeds via `pgconfig.Registry.EnsureDefaultStatuses`, called explicitly for
one tenant if `TASK_ROUTER_BOOTSTRAP_TENANT_ID` is set at startup. For
multiple tenants (or a real provisioning flow), call the same method (or
`RegisterStatus` four times via `TaskRouterAdminService`) whenever a new
tenant is provisioned. This is a deliberately minimal choice for now, not
a structural limitation -- the registry itself is just an open allow-list.

## Deferred scope

Explicitly **not** implemented, per `TASK_ROUTER_SPECIFICATION.md §7`
("Known Gaps for v2 Design") and the production scoping decision for this
build:

- Transfers (cold/queue-to-queue, warm/peer-to-peer) -- `§7.1`.
- Attribute-based (skill) matching -- attributes are stored and
  shape-validated (`§4.4`) but never compared during matching -- `§7.2`.
- Per-channel `ready`/`interruptible` gating in the matching algorithm --
  both fields are stored and toggleable but never read by
  `agent_can_take` -- `§7.3`.
- Supervisor force-routing / manual assignment -- `§7.4`.
- Bullseye routing / attribute relaxation over time -- `§7.5`.
- Queue timeout behavior (a `Pending` task never escalates/overflows on
  its own) -- `§7.6`.
- Real-time notification delivery / streaming push to Agent Desktop
  (spec `§3.5`) -- this service only publishes to NATS JetStream. A
  future, separate **Agent & Presence Service** milestone is the
  documented consumer that would relay events to connected clients (see
  `CCAAS_ENTERPRISE_ARCHITECTURE.md` Section 2.2's service map) -- not
  built here.

## Testing

- `internal/redisdomain/*_test.go`: integration tests against
  `github.com/alicebob/miniredis/v2` (in-memory Redis with Lua/EVAL
  support) -- covers the full `agent_can_take` gate logic, strict FIFO
  task selection, first-eligible-agent-wins/no-tie-break, capacity
  accounting through match/accept/complete and match/reject cycles, the
  agent-deletion-with-in-flight-task edge case (`§5.5`), and concurrent
  match-commit races (`go test -race` clean).
- `internal/pgconfig/validation_test.go`: pure unit tests for the
  attribute shape-validation rule (`§4.4`).
