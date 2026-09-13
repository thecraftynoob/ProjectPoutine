# agent-presence

**Responsibility:** Holds the live WebSocket connection to every logged-in
Agent Desktop; is the single source of truth for which agent connections
are currently open, on this replica, right now. Relays a deliberately
filtered subset of Task Router's domain events
(`TASK_ROUTER_SPECIFICATION.md` Section 6.3) to the connection belonging
to each event's target agent (architecture doc Section 2.2).

**Scope boundary (important):** this service does **not** duplicate Task
Router's `Agent` entity (status, capacity, queues, eligibility). It has no
opinion on whether an agent is eligible for routing -- it only knows "is
this agent's WebSocket currently connected on this replica, and if so,
which connection." Task Router remains the sole owner of agent routing
state and never calls into this service synchronously (it doesn't need
to -- it already has all agent state it needs internally).

**State:** Stateful (live WebSocket registry, in-memory, per-replica) +
an ephemeral Redis pub/sub layer for cross-replica fan-out (see below).
No Postgres dependency -- this service owns no durable domain data of its
own.

---

## ⚠️ WebSocket auth is a placeholder -- NOT real authentication

The `/ws` upgrade endpoint identifies the connecting caller using two
**unsigned, unverified** query parameters:

```
GET /ws?tenant_id=<uuid>&agent_id=<string>
```

- `tenant_id` must parse as a UUID, or the upgrade is rejected with
  **HTTP 400**.
- `agent_id` must be non-empty, or the upgrade is rejected with **HTTP
  400**.
- Otherwise, **any caller who can reach this endpoint can claim to be any
  agent in any tenant** simply by setting these query parameters. There
  is no signature, no token, no verification of any kind.

This is acceptable **only** because Tenant & Identity Management
(architecture doc Section 2.2) does not exist yet in this platform --
there is no JWT issuer to validate against today.

**This must be replaced before any real deployment** with:
- An `Authorization` header bearing a JWT with a `tid` claim (tenant ID)
  and an agent/subject claim.
- Validation the way architecture doc Section 1.1 describes for the API
  Gateway (Layer 1 enforcement: resolve `tenant_id` from the
  authenticated principal, never from client-supplied input).
- Layer 2 re-validation inside this service itself (per that same
  section), rather than trusting the Gateway blindly, matching the
  pattern `pkg/tenantctx` already applies to this service's gRPC surface.

Do not treat the current query-param scheme as "auth that will be
hardened later" -- it provides **no security whatsoever** today. This
warning is intentionally repeated as a doc comment at the top of
`internal/wsserver/wsserver.go`.

---

## WebSocket wire protocol

### Connecting

```
GET /ws?tenant_id=<uuid>&agent_id=<string>
```

On success, the HTTP connection is upgraded to a WebSocket. The server
sends no message on connect; frames arrive only as matching events occur.
There is no client-to-server message contract -- any inbound message from
the client is read and discarded (reads exist purely to detect
disconnects promptly).

### Delivered messages

Every delivered message is a single WebSocket **text frame** containing
one JSON document:

```json
{
  "type": "reservation.created",
  "agentId": "agent-42",
  "payload": { "...": "event-specific fields, see below" }
}
```

| Field | Type | Meaning |
|---|---|---|
| `type` | string | One of the four forwarded event types below. |
| `agentId` | string | The agent this event is being delivered to -- always equal to the `agent_id` this connection was opened with. |
| `payload` | object | The event's own fields, re-marshaled as-is from Task Router's actual published payload (see below) -- no new shape invented. |

### Forwarded event types (and only these)

Per `TASK_ROUTER_SPECIFICATION.md` Section 6.3's "Live notification
delivery (external observers)" contract -- **relocated here** from Task
Router (whose own spec explicitly does not implement the delivery side,
only publishing) -- exactly four event types are ever forwarded to a
connected client. Every other event in Task Router's catalog (Task
Enqueued/Accepted/Completed, Agent Created/Capacity Config Updated/Queues
Updated, Reservation Accepted) is consumed internally for JetStream
bookkeeping but **never** reaches a WebSocket client.

| `type` | Task Router subject (confirmed against `services/task-router/internal/events/events.go`) | `payload` fields |
|---|---|---|
| `reservation.created` | `tenant.{tenantId}.reservation.created` | `reservationId, taskId, agentId, expiresAt` |
| `reservation.rejected` | `tenant.{tenantId}.reservation.rejected` | `reservationId, taskId, agentId, reason` |
| `agent.status_changed` | `tenant.{tenantId}.agent.status.changed` | `agentId, status` |
| `agent.deleted` | `tenant.{tenantId}.agent.deleted` | `agentId` |

The target agent for `reservation.*` events is read directly from the
event's own `agentId` payload field. For `agent.*` events, the event's
subject-of-event agent IS the `agentId` payload field (Task Router always
includes it), so the same lookup logic works uniformly across all four
types (see `internal/relay/envelope.go`).

### Disconnect semantics

Matches Task Router spec Section 5.6's philosophy for its own (removed)
Section 3.5, carried over here: a dropped connection (client close,
network error, or server shutdown) is unregistered and simply forgotten.
**Nothing is replayed on reconnect** -- a client that reconnects starts
receiving new events from that point forward; there is no session or
backlog state tied to a `(tenant_id, agent_id)` pair across connections.

---

## Cross-replica delivery design

Architecture doc Section 2.2 lists this service's Key Dependencies as
including "Redis (pub/sub fan-out across replicas + presence key storage
with TTL heartbeats)". This build implements the **pub/sub fan-out half**
of that (the part needed for this milestone's scope: relaying Task Router
events to the right connection regardless of which replica holds it). The
**presence-key-storage-with-TTL-heartbeats half is explicitly deferred**
(see "Deferred scope" below) -- nothing in this milestone needs a
queryable "who is currently online" registry beyond the live in-memory
one.

**The problem:** the live WebSocket registry (`internal/registry`) is
per-process, in-memory state. With more than one replica running, the
NATS consumer instance that receives a given Task Router event has no way
to know, up front, which replica (if any) is holding that event's target
agent's WebSocket connection.

**The design (one code path, no "check local, then fall back" branch):**

1. On receipt of a forwarded NATS event, `internal/relay.Consumer`
   decodes it, classifies it as one of the four forwarded types, and
   publishes the resulting client `Envelope` as JSON to a per-tenant
   Redis Pub/Sub channel: `agent-presence:tenant:{tenantId}:events`.
2. **Every** replica (including the one that just published) is
   subscribed to every tenant channel it has observed traffic for
   (subscriptions are created lazily, the first time that tenant is seen
   -- there's no tenant-provisioning hook to bootstrap from, so tenants
   are discovered organically from the event stream itself).
3. On receiving a fanned-out message, each replica independently checks
   its **own local** `Registry.Lookup`. If it holds a connection for the
   target agent, it delivers; if not, it silently discards the message.

This means exactly one replica ever actually writes to a WebSocket for
any given event, every replica runs identical simple logic, and there is
no synchronous "ask the cluster who has this connection" round trip. The
cost is that every replica receives every subscribed tenant's fanned-out
traffic regardless of whether it holds a matching connection -- an
acceptable trade at this scale, and it's what the architecture doc's
dependency listing calls for rather than an invented embellishment.

Redis is used purely as an **ephemeral transport** here: nothing is
persisted. See `internal/relay/fanout.go`'s doc comment for the full
design rationale.

---

## Configuration (environment variables)

| Variable | Default | Purpose |
|---|---|---|
| `AGENT_PRESENCE_GRPC_PORT` | `50055` | gRPC listen port (health check only -- see below) |
| `AGENT_PRESENCE_WS_ADDR` | `:8085` | HTTP/WebSocket listen address (the `/ws` upgrade endpoint) |
| `REDIS_ADDR` | `localhost:6379` | Redis address (cross-replica fan-out transport) |
| `AGENT_PRESENCE_REDIS_DB` | `0` | Redis logical DB index |
| `NATS_URL` | `nats://localhost:4222` | NATS server URL (consumes Task Router's event stream) |

Port `8085` was chosen as a free port in this repo's port range (gRPC
services occupy `50051`-`5005x`; HTTP/WS services elsewhere in the
platform use `808x`), documented here as the canonical WS port for this
service -- see `deploy/k8s/agent-presence/deployment.yaml` and
`service.yaml`, both updated to expose it alongside the existing gRPC
port (manifests only -- not redeployed as part of this milestone).

## gRPC surface

`presence.v1.PresenceService` is registered with **zero RPCs** as of this
build (see `proto/presence/v1/presence.proto`'s doc comment). The
previously-scaffolded `GetAvailableAgents` RPC has been removed entirely
-- Task Router never called it, and it duplicated agent routing state
that Task Router already owns as sole source of truth. The gRPC server
this service runs exists only to expose the standard health check
(`pkg/health`); the service definition is kept (as an explicitly empty
`service PresenceService {}`) rather than deleted so the package
structure is stable if a genuinely useful small RPC (e.g. "is agent X
currently connected on any replica") is added in a future milestone. No
RPC in the current scope needs it.

## Testing

- `internal/registry`: concurrent register/lookup/unregister correctness
  (`go test -race`), including the "stale old connection must not evict a
  newer one for the same agent" replacement race.
- `internal/relay`: unit tests for NATS-subject classification (every
  real Task Router subject correctly classified as forwarded or not-
  forwarded) and payload decoding/envelope-building against synthetic
  messages encoded exactly the way Task Router's own publisher encodes
  them (`structpb.NewStruct` + `proto.Marshal`), plus a `miniredis`-backed
  test proving the two-replica pub/sub fan-out design actually delivers
  only through the replica that owns the connection.
- `pkg/eventbus`: an in-process NATS+JetStream server (via
  `github.com/nats-io/nats-server/v2`) backs an integration test proving
  that two independent `SubscribeEphemeral` callers (simulating two
  agent-presence replicas) each receive their OWN full copy of a single
  published event -- the crux of this service's cross-replica fan-out
  design (see `internal/relay/consumer.go`'s `Start` doc comment): each
  replica's own NATS consumer must see every forwarded event, not just
  whichever one replica JetStream happens to route it to. A second test
  confirms `SubscribeEphemeral` consumers don't collide with a durable,
  competing-consumer `Subscribe` group on the same stream/filter.
- `internal/wsserver`: `httptest.Server` + `github.com/coder/websocket`
  client -- valid/invalid query parameter upgrade behavior (400 on bad
  `tenant_id`, missing `agent_id`, missing `tenant_id`), registration/
  unregistration lifecycle on connect/disconnect, actual message delivery
  over a real WebSocket connection, and graceful-shutdown connection
  draining.
- End-to-end live smoke test: see the milestone's final report for the
  actual run performed (real `agent-presence` + real `task-router`, a
  real WebSocket client, a real Task Router gRPC call producing a
  forwarded event, confirmed arriving on the WebSocket).

## Deferred scope

- **Real JWT-based authentication** for the `/ws` endpoint -- the
  single biggest deferred item. See the warning section above.
- **Redis-backed presence keys with TTL heartbeats** (the other half of
  architecture doc Section 2.2's Redis dependency line) -- e.g. a
  queryable "is agent X online anywhere" key with a heartbeat-refreshed
  TTL, independent of any one replica's in-memory registry surviving a
  crash. Not needed by anything in this milestone's scope; the in-memory
  registry plus pub/sub fan-out is sufficient for "deliver this event to
  a currently-connected agent" today.
- Any gRPC RPC surface on `PresenceService` beyond the health check --
  intentionally left empty; see "gRPC surface" above.
- Multiplexing gRPC and HTTP/WebSocket onto a single port -- this build
  runs them as two separate listeners (simplest, most conventional
  option), matching this service's own README's stated preference.
