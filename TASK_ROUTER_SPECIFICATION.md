# Task Router — Functional Specification

**Purpose of this document:** a complete, framework-agnostic and language-agnostic extraction of the business rules, data models, state machines, and functional behavior implemented in the Task Router PoC (v0.0.4), intended as the blueprint for a production-grade rebuild in any technology stack.

**Scope discipline:** every statement in this document describes verified, existing behavior in the current codebase, with no invented or aspirational capability. Where the current system does not implement something a mature CCaaS platform typically has (transfers, attribute-based matching, supervisor override routing, bullseye/priority escalation), that gap is called out explicitly in **§7 — Known Gaps for v2 Design**, rather than fabricated here as if it were extracted from real logic. A rebuild team should treat §1–§6 as ground truth for current behavior, and §7 as the requirements backlog to design from scratch.

---

## 1. Executive Overview & Domain Boundaries

### 1.1 Functional purpose

The Task Router is the real-time matching engine of a contact-center platform: it holds the live, authoritative record of which Agents exist, what work (Tasks) is waiting, and brokers the handshake (Reservation) that assigns a waiting Task to an available Agent. It is one of several planned services in a larger CCaaS ecosystem (Voice, Reporting, Identity, Notification/Alerting are separate, independently owned services); the Task Router's sole responsibility is presence-and-capacity-based work distribution — it does not handle media, identity, or historical analytics itself.

### 1.2 Core domain entities

| Entity | Role |
|---|---|
| **Agent** | A worker's live routing profile: current status, per-channel capacity, and queue memberships. Not an identity record (no credentials/login) — purely "can this worker take this kind of work right now." |
| **Task** | A unit of work to be routed (e.g., a chat, an email, a voice call) — carries a target queue and a type, and moves through a fixed lifecycle from waiting to done. |
| **Reservation** | The offer handshake between one Task and one Agent — the record of "this task is being offered to this agent," with its own accept/reject/expire outcome, independent of the task's own status. |
| **Queue** | A named bucket that groups tasks and defines which agents are eligible to receive them (via membership, not skill/attribute matching). |
| **Attribute** | A registered `{name, type}` definition used only to validate the *shape* of arbitrary key/value data stored on Agents and Tasks — carries no matching behavior today (see §7). |
| **Status (registry)** | An open, extensible allow-list of valid strings an Agent's `status` field may hold — not a fixed enum baked into the domain model. |

### 1.3 Architectural boundary this spec covers

This document describes only the **routing/matching domain** — the rules for entity state and the algorithm that connects Tasks to Agents. It does not cover transport/protocol choices (REST vs. gRPC vs. GraphQL), specific database technology, or deployment topology; those are implementation decisions for the rebuild, informed by but not constrained by this spec.

---

## 2. Domain Data Models & Schema Specifications

### 2.1 Agent

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `agentId` | string | yes | — | Unique identifier. |
| `status` | string | no | `"Offline"` | Value must belong to the open Status registry (§2.5) to be *set* via the status-update capability; the create capability does **not** enforce this (see §3.2). Only the literal value `"Available"` has any behavior significance to the matching algorithm — every other status string, including `"Offline"`, `"Break"`, and the system-assigned `"Not Responding"`, is functionally identical from the router's point of view (not `"Available"` ⇒ not eligible). |
| `attributes` | map of string→(number\|boolean) | no | `{}` | Validated for shape against the Attribute registry (§2.6) at write time. **Not used in matching** — stored for future use only. |
| `capacity` | map of channel-name → ChannelCapacity | no | `{}` | One entry per work type ("channel") this agent can handle. An agent with no entry for a task's type can never be matched to it. |
| `queues` | list of queue IDs | no | `[]` | Queue memberships. An agent only receives tasks from a queue it belongs to. |
| `statusChangedAt` | timestamp | no | now | Server-side clock, updated every time `status` changes (used for UI "time in status" displays). |

**ChannelCapacity** (one entry per channel in `capacity`):

| Field | Type | Default | Notes |
|---|---|---|---|
| `ready` | boolean | `true` | Stored and toggleable via its own capability, but **not read by the matching algorithm** — see §7. |
| `max` | integer | `1` | Maximum concurrent tasks of this channel type this agent may hold. |
| `active` | integer | `0` | Current concurrent count. Exclusively managed by the routing system itself — any value supplied by a client on a capacity-update call is ignored. |
| `interruptible` | boolean | `true` | Stored only. **Not read anywhere in current logic** — see §7. |

### 2.2 Task

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `taskId` | string | yes | auto-generated if omitted | Sequential generation available, or caller-supplied. |
| `queueId` | string | yes | — | Must reference an existing Queue at creation time. |
| `taskType` | string | yes | — | Free-form "channel" identifier (e.g. `"chat"`, `"voice"`) — matched against an Agent's `capacity` keys. |
| `requiredAttributes` | map of string→(number\|boolean) | no | `{}` | Validated for shape against the Attribute registry at creation only (immutable afterward). **Not used in matching.** |
| `enqueuedAt` | timestamp | no | now, or caller-supplied | The FIFO ordering key. A caller-supplied value lets a task be re-inserted at its original position (used internally when a task is returned to Pending). |
| `status` | enum | no | `"Pending"` | See §5.1 for the full state machine. |
| `currentReservationId` | string or null | no | `null` | The active Offered/Accepted reservation for this task, if any. |
| `assignedAgentId` | string or null | no | `null` | Set when matched; cleared when a reservation for this task is rejected; **retained** (not cleared) once the task completes. |

**No cancellation capability exists.** A task's only terminal state is Completed, reached exclusively via the explicit completion capability while the task is Active. There is no "cancel this task" or "delete this task" capability independent of that flow.

### 2.3 Reservation

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `reservationId` | string | yes | auto-generated (sequential) | |
| `taskId` | string | yes | — | |
| `agentId` | string | yes | — | |
| `status` | enum | no | `"Offered"` | See §5.2. |
| `createdAt` | timestamp | no | now | |
| `expiresAt` | timestamp or null | no | `createdAt + TTL` | `null` only for reservations that predate this field's existence (legacy/migration artifact); every reservation created going forward always has this set. |

**Reservation TTL**: a configurable duration (default 30 seconds), applied once at the moment a match is made — `expiresAt = matchedAt + TTL`. Governs the automatic expiry behavior in §5.2 and §5.5.

### 2.4 Queue

| Field | Type | Required | Default |
|---|---|---|---|
| `queueId` | string | yes | — |
| `createdAt` | timestamp | no | now |

No other configuration exists on a Queue in the current system — no priority, no service-level target, no routing strategy per queue. A Queue is purely a named bucket that Agents join and Tasks reference.

### 2.5 Status (registry entry)

| Field | Type | Required | Default |
|---|---|---|---|
| `status` | string | yes | — |
| `createdAt` | timestamp | no | now |

This is an **open, admin-extensible allow-list**, not a fixed domain enum. Any string can be registered. Seeded on first run with four defaults: `Available`, `Break`, `Offline`, `Not Responding`. The domain logic special-cases exactly one literal value (`"Available"`); every other registered status (including the seeded defaults) is otherwise inert from the router's perspective and exists purely for presentation/reporting purposes.

### 2.6 Attribute (registry entry)

| Field | Type | Required | Default |
|---|---|---|---|
| `name` | string | yes | — |
| `type` | enum: `"numeric"` \| `"boolean"` | yes | — |
| `createdAt` | timestamp | no | now |

Used only to validate the shape of values stored in `Agent.attributes` and `Task.requiredAttributes` — every key used in either of those maps must be pre-registered here, and its value must match the registered type (a `"numeric"` attribute rejects a boolean value even though booleans are technically numeric in some type systems — this exclusion is deliberate). No default vocabulary is seeded; a fresh system starts with zero registered attributes, meaning any attribute usage is rejected until explicitly registered.

### 2.7 Transfer audit trail

**Not implemented.** There is no `transferCount`, `isTransferred`, `transferHistory`, or `retainQueuePosition` field anywhere in the domain model, and no transfer capability of any kind exists in the current system. See §7.1.

---

## 3. Functional Capabilities & API Intent Mapping

Capabilities are grouped by business domain. Each entry states purpose, required inputs, state mutation, and emitted events — independent of transport (the current implementation happens to expose these as REST/WebSocket, but the capability itself is the contract).

### 3.1 Queue Configuration

| Capability | Purpose | Inputs | State Mutation | Events |
|---|---|---|---|---|
| **Register a Queue** | Create a new named work bucket. | Queue ID | Adds to the Queue registry. Rejected if the ID already exists. | none |
| **List Queues** | Enumerate all registered queues. | — | read-only | none |
| **Get Queue Detail** | Retrieve one queue plus every Agent currently a member of it. | Queue ID | read-only | none |
| **Remove a Queue** | Delete a queue definition. | Queue ID | Removes from the registry. Any Agent still listing this queue in its memberships, or any Task still referencing it, is **not** updated — the reference becomes orphaned rather than blocking the delete. | none |

### 3.2 Agent Presence & Profile Control

| Capability | Purpose | Inputs | State Mutation | Events |
|---|---|---|---|---|
| **Create an Agent** | Provision a new agent's routing profile. | Agent ID, initial status (unvalidated against the registry at creation time), attributes, per-channel capacity map, queue memberships | Creates the Agent record. Rejected if the ID already exists (atomic uniqueness guarantee — see §5.6). | Agent Created |
| **List Agents** | Enumerate all agents. | — | read-only | none |
| **Get Agent Detail** | Retrieve one agent's full profile. | Agent ID | read-only | none |
| **Remove an Agent** | Deprovision an agent's routing profile. | Agent ID | Deletes the Agent record. **If the agent currently has a Reserved or Active task**, that task is atomically reset to Pending and re-queued at its original FIFO position, and any tied Offered/Accepted reservation is marked Rejected (reason: agent deleted) — see §5.5. | Reservation Rejected (only if a reservation was actually resolved by this action), then Agent Deleted |
| **Set Agent Master Status** | Change an agent's overall presence (e.g. mark Available/Break/Offline). | Agent ID, new status | Rejected if the target status isn't a registered Status value. Updates `status` and `statusChangedAt`. | Agent Status Changed |
| **Replace Agent Capacity Map** | Configure which channels an agent can handle and at what concurrency. | Agent ID, full map of `{channel: {ready, max, interruptible}}` | Full replace — any channel omitted from the input is removed from the agent. `active` in the input is always ignored; a retained channel keeps its live `active` count, a new channel starts at 0 — applied atomically against any concurrent match. | Agent Capacity Config Updated |
| **Toggle One Channel's Ready Flag** | Flip a single channel's readiness without resending the whole capacity map (e.g. a UI toggle switch). | Agent ID, channel name, ready (bool) | Flips only that channel's `ready`; if the channel doesn't exist yet, it's created with default `max=1, interruptible=true`. All other fields/channels untouched. Atomic against a concurrent match. | Agent Capacity Config Updated |
| **Replace Agent Queue Memberships** | Configure which queues an agent belongs to. | Agent ID, full list of queue IDs | Full replace. Rejected entirely (no partial application) if any queue ID doesn't exist. | Agent Queues Updated |
| **Replace Agent Attributes** | Configure an agent's stored attribute values. | Agent ID, full map of attribute values | Full replace, validated for shape against the Attribute registry. Touches nothing else on the agent. **Not used for routing.** | none |
| **List an Agent's Pending Offers** | Let an agent (or the UI polling on its behalf) see currently Offered reservations. | Agent ID | read-only, filtered to Offered status | none |

### 3.3 Task Lifecycle & Ingestion

| Capability | Purpose | Inputs | State Mutation | Events |
|---|---|---|---|---|
| **Enqueue a Task** | Submit a new unit of work for routing. | Target queue ID (must exist), task type, required attributes (validated for shape), optional caller-supplied task ID and enqueue timestamp | Creates the Task record in `Pending` status. Rejected if the queue doesn't exist, an attribute is invalid, or a caller-supplied task ID collides with an existing one. | Task Enqueued |
| **List Tasks** | Enumerate all tasks. | — | read-only | none |
| **Get Task Detail** | Retrieve one task's current state. | Task ID | read-only | none |
| **Complete a Task** | Signal that an Active task's work is finished, freeing the agent's capacity. | Task ID | Rejected unless the task is currently `Active`. Releases one unit of capacity on the assigned agent's channel (floor 0). Sets task to `Completed`. | Task Completed |

### 3.4 Reservation Handshake

| Capability | Purpose | Inputs | State Mutation | Events |
|---|---|---|---|---|
| **Accept a Reservation** | An agent agrees to take the offered task. | Reservation ID | Rejected unless the reservation is currently `Offered`. Sets reservation to `Accepted`, sets the task to `Active`. | Reservation Accepted, then Task Accepted |
| **Reject a Reservation** | An agent explicitly declines the offered task. | Reservation ID | Rejected unless the reservation is currently `Offered` (including a race-safe re-check — see §5.6). Releases the consumed capacity, sets reservation to `Rejected` (reason: agent rejected), returns the task to `Pending` at its **original** enqueue position, and sets the agent's status to the system value `"Not Responding"`. | Reservation Rejected (reason: agent_rejected) |
| *(System-triggered)* **Reservation Expiry** | Automatically resolve an offer the agent never responded to within the TTL window. | — (time-triggered, not caller-invoked) | Identical state mutation to a manual reject, but with reason `"expired"`. Runs on a fixed periodic sweep, not on a per-reservation timer. | Reservation Rejected (reason: expired) |

### 3.5 Real-Time Notification Delivery

| Capability | Purpose | Inputs | State Mutation | Events |
|---|---|---|---|---|
| **Subscribe to Live Routing Events** | Give a connected client (e.g. an Agent Desktop UI) a real-time push feed instead of requiring it to poll. | A persistent connection; no client→server message contract | none (pure read/forward) | Forwards a filtered subset of the full event catalog — see §6.3 — to every currently connected client. |

### 3.6 Administrative Configuration (Statuses & Attributes)

| Capability | Purpose | Inputs | State Mutation | Events |
|---|---|---|---|---|
| **Register a Status Value** | Extend the allow-list of valid Agent statuses. | Status string | Adds to the registry. Rejected if already present. | none |
| **List Statuses** | Enumerate the registry. | — | read-only | none |
| **Remove a Status Value** | Shrink the allow-list. | Status string | Removes from the registry. Any agent currently set to this status is unaffected and keeps it — the removal only blocks *future* assignment of that value. | none |
| **Register an Attribute Definition** | Define a new named, typed field usable in Agent/Task attribute maps. | Name, type (numeric or boolean) | Adds to the registry. Rejected on invalid type or duplicate name. | none |
| **List Attributes** | Enumerate the registry. | — | read-only | none |
| **Get Attribute Detail** | Retrieve one attribute's definition plus every agent currently assigned a value for it. | Attribute name | read-only | none |
| **Remove an Attribute Definition** | Retire a named field. | Attribute name | Removes from the registry. Existing agent/task values that reference it are unaffected — removal only blocks *future* use of that name. | none |

### 3.7 Observability / Operational

| Capability | Purpose | Inputs | State Mutation | Events |
|---|---|---|---|---|
| **Aggregate Dashboard View** | A single call returning all agents (with live assigned-task counts) and all tasks (with current status/assignment), for a monitoring display. | — | read-only | none |

---

## 4. Core Business Rules & Routing Algorithm

### 4.1 The matching algorithm (as implemented today)

The current system implements **two gates**, evaluated in a strict FIFO task loop — not a four-gate model. This is stated precisely here so a rebuild does not assume hidden logic exists.

**Per-candidate eligibility check** (evaluated for one Agent against one Task):

```
FUNCTION agent_can_take(agent, task):
    IF agent.status != "Available":
        RETURN false                          # Gate 1: Presence
    IF task.queueId NOT IN agent.queues:
        RETURN false                          # Gate 2a: Queue membership
    channel = agent.capacity[task.taskType]
    IF channel does not exist:
        RETURN false                          # Gate 2b: Channel capability
    RETURN channel.active < channel.max        # Gate 2c: Capacity headroom
```

No check against `channel.ready`, no check against `channel.interruptible`, and no comparison of `task.requiredAttributes` to `agent.attributes` occurs anywhere in this function or the algorithm that calls it.

**Matching pass** (run whenever a relevant event occurs — see §6 — never on a fixed poll):

```
FUNCTION evaluate_once():
    pending_tasks = all tasks with status "Pending", ordered oldest enqueuedAt first
    all_agents = every agent, in no defined/guaranteed order

    FOR EACH task IN pending_tasks:            # strict FIFO by enqueuedAt
        FOR EACH agent IN all_agents:          # no ordering guarantee among agents
            IF NOT agent_can_take(agent, task):
                CONTINUE
            attempt = atomically_commit_match(agent, task)   # re-validates everything
                                                              # against live state before
                                                              # committing; see §5.6
            IF attempt failed (state changed since the scan):
                CONTINUE                        # try the next agent for this same task
            record the new reservation
            BREAK                               # this task is now claimed; move to next task
```

**Key behavioral facts:**
- **Task selection is strict FIFO** by enqueue timestamp — the oldest waiting task is always considered first.
- **Agent selection has no tie-breaking rule.** Among multiple agents that all pass the eligibility check for a given task, the first one encountered in an unordered iteration wins. There is no longest-idle, round-robin, load-balancing, or scoring logic of any kind.
- **The eligibility check is re-validated atomically at commit time**, not trusted from the initial scan — this is what makes the algorithm safe under concurrent mutation (see §5.6), but it does not change *which* agent is chosen, only whether a chosen candidate's match is honored.
- A task is claimed by exactly one match attempt per pass; if no eligible agent exists, the task remains Pending and is reconsidered on the next triggering event.

### 4.2 Capacity accounting rules

- `active` is a count of concurrently assigned tasks per channel, exclusively managed by the system.
- Incremented by exactly 1 at the moment a match is committed.
- Decremented by exactly 1 (floor 0) when: a task completes, a reservation is rejected (manually or via expiry), or an agent holding an Active/Reserved task is deleted.
- A capacity-configuration change (replacing the whole map, or toggling one channel's ready flag) never resets or recalculates `active` — it is always carried forward from the previous value for a retained channel, and initialized to 0 only for a channel that didn't previously exist on that agent.
- Lowering `max` below the current `active` value is permitted and does not evict in-flight work — it only prevents new matches on that channel until `active` naturally drops back under `max`.

### 4.3 Reservation expiry rule

- Every reservation is stamped with an expiry timestamp at creation, computed as match-time plus a fixed TTL (configurable, default 30 seconds).
- Expiry is **not** enforced by a per-reservation timer/callback. It is enforced by a periodic sweep (fixed interval, current default 2 seconds) that scans all reservations still in `Offered` status whose expiry timestamp has passed, and resolves each one through the identical mechanism a manual reject uses (see §5.2), tagged with a distinguishing reason so downstream consumers can tell "the agent explicitly declined" apart from "the agent never responded."
- An expired-and-resolved reservation immediately makes its task eligible for re-matching (the resolution event itself is what re-triggers the matching pass — see §6.2) and sets the losing agent's status to the system-assigned `"Not Responding"` value, which removes it from eligibility until it (or an operator) explicitly sets it back to `"Available"`.

### 4.4 Attribute validation rule (shape only, not matching)

Whenever a client supplies an `attributes`/`requiredAttributes` map (agent creation, agent attribute replacement, or task creation), every key must already exist in the Attribute registry, and its value must match the registered type exactly:
- A registered `boolean` attribute must receive a literal boolean value.
- A registered `numeric` attribute must receive an `int` or `float` value that is explicitly **not** a boolean (a deliberate exclusion, since some type systems treat booleans as a numeric subtype).
- Any violation rejects the entire write with no partial application.
- This validation exists purely to keep stored data well-typed for future use; it has zero effect on which agent a task is matched to today.

---

## 5. State Machines & Concurrency Rules

### 5.1 Task Lifecycle

| From | To | Trigger |
|---|---|---|
| *(none)* | `Pending` | Task enqueued |
| `Pending` | `Reserved` | Matching algorithm commits a match |
| `Reserved` | `Active` | The offered reservation is accepted |
| `Reserved` | `Pending` | The offered reservation is rejected (manually, by expiry, or as a side effect of the assigned agent being deleted) — re-enters the queue at its **original** enqueue timestamp, not at the back of the line |
| `Active` | `Completed` | Task completion capability invoked |
| `Active` | `Pending` | The assigned agent is deleted while the task is Active (same requeue behavior as above) |
| `Completed` | *(terminal)* | No further transitions. No cancellation path exists independent of this flow. |

### 5.2 Reservation Lifecycle

| From | To | Trigger |
|---|---|---|
| *(none)* | `Offered` | Created as the direct result of a committed match |
| `Offered` | `Accepted` | Accept capability invoked |
| `Offered` | `Rejected` | One of three triggers, all resolving through the identical atomic transition, distinguished only by a reason tag: (a) manual reject invoked by/for the agent, (b) automatic expiry sweep, (c) the offered-to agent is deleted |
| `Accepted` | *(terminal)* | No further reservation-level transition; the task's own lifecycle continues independently from this point. |
| `Rejected` | *(terminal)* | A new, distinct reservation is created if/when the same task is re-matched — a rejected reservation is never reused or reopened. |

**No `Revoked` state exists** — there is no capability for the system or a supervisor to withdraw an Offered reservation for a reason other than the agent's own (in)action or deletion.

### 5.3 Agent Status

Agent presence is a single **master status** field constrained to an open, admin-managed registry (§2.5), not a fixed state machine with defined transitions — any registered value can be set from any other registered value, with no enforced sequencing (e.g., nothing prevents going directly from `"Break"` to `"Available"` or back). The domain logic recognizes exactly one value with matching significance:

| Status value | Significance to routing | How it's set |
|---|---|---|
| `"Available"` | The only status value that makes an agent eligible to receive a match. | Explicitly, via the status-update capability. |
| `"Not Responding"` | No special routing behavior beyond simply not being `"Available"` — but it is **system-assigned**, not settable directly, and specifically signals "this agent just failed to respond to an offer." | Automatically, as a side effect of any reservation-reject transition (manual reject or expiry). Never set as a side effect of agent deletion. |
| *(any other registered value, e.g. `"Offline"`, `"Break"`, custom values)* | Functionally identical to each other — simply "not eligible." | Explicitly, via the status-update capability. |

**Per-channel readiness** (`ChannelCapacity.ready`) is a separate, independent boolean per channel, toggleable via its own capability. It is stored and returned in every agent read, but **currently has no effect on the matching algorithm** (§7.3) — an agent with `ready: false` on a channel is still matched exactly as if it were `true`, provided the master status is `"Available"` and capacity headroom exists.

### 5.4 Concurrency & Atomicity Rules

The system's correctness under concurrent operation rests on a small set of guarantees that any rebuild must preserve regardless of underlying technology:

1. **Uniqueness on creation** (Agent, Task): a create operation must atomically check-and-reserve the identifier such that two simultaneous creation attempts for the same ID can never both succeed. One must win, the other must receive a clear "already exists" rejection.

2. **Capacity mutation is never read-modify-write from ordinary application code.** Every operation that changes an agent's `active` count (matching, completion, rejection, deletion cleanup) or replaces the capacity map must be a single atomic operation against the current live state — never "read the current value in application code, compute a new value, write it back," which would race against a concurrent mutation of the same field.

3. **The match-commit step must re-validate all matching conditions atomically at commit time**, not merely trust an earlier scan. Because the eligibility scan (which agents look eligible) and the commit (actually claiming the capacity slot) are logically separate steps, anything could have changed the underlying state in between (another concurrent match, a status change, a deletion) — the commit step must re-check agent status, task status, queue membership, and capacity headroom as one indivisible operation, and refuse (not partially apply) if any condition no longer holds.

4. **A reservation's terminal resolution (accept vs. reject) must be mutually exclusive and idempotent-safe against races.** Because a manual reject, an automatic expiry, and an agent-deletion-triggered rejection can all attempt to resolve the same Offered reservation, the resolution operation must atomically re-check that the reservation is still `Offered` before applying any change, and must return a distinguishable "already resolved by something else" result rather than silently double-applying (e.g. decrementing capacity twice, or overwriting an `Accepted` reservation back to `Rejected`).

5. **Agent deletion with an in-flight task must be a single atomic operation, not a check-then-act sequence.** Determining "does this agent have a Reserved/Active task, and if so what reservation is tied to it" and then acting on that must happen inside one atomic step. A separate read-then-delete sequence would leave a window where the task could be legitimately completed or its reservation resolved by another operation in between, causing the deletion logic to incorrectly report/publish a rejection for a reservation that was actually already resolved another way.

6. **Multiple concurrent instances of the matching engine must be able to run safely without additional coordination**, by virtue of rule 3 above: since every commit attempt is independently re-validated atomically, two engine instances racing to match the same task/agent pair will have exactly one succeed and the other's attempt will be cleanly refused and retried against the next candidate, with no double-booking possible.

### 5.5 Edge case: agent deletion while holding work

When an agent is removed while it currently holds a task in `Reserved` or `Active` status:
- The task does **not** remain assigned to a now-nonexistent agent.
- It is atomically returned to `Pending`, keeping its **original** enqueue timestamp (so it does not lose its place in the FIFO order relative to tasks that were waiting before it).
- If a reservation was tied to that task (Offered or Accepted), it is marked `Rejected`.
- This is the only path by which an `Accepted` reservation's task can return to `Pending` outside of the reservation-reject flow — deletion overrides even an already-accepted-but-not-yet-completed task.
- Exactly one accurate notification of this resolution must be emitted — a naive two-step "check what's assigned, then delete" implementation risks emitting a false resolution notification if the task's real state changed in the gap between the two steps (see rule 5 above).

### 5.6 Edge case: connection drops / disconnects

The real-time notification delivery capability (§3.5) treats a dropped connection as routine: a disconnected client is simply removed from the active broadcast set on the next attempted delivery to it, with no server-side error and no impact on any other connected client or on any domain state. There is no session/reconnection state tied to routing correctness — a client that reconnects simply starts receiving new events from that point forward; nothing is replayed.

---

## 6. Event Catalog & Inter-Service Contracts

### 6.1 Design principle

Every domain state change that matters to routing decisions is broadcast as an event on one of three logical channels, grouped by entity family (Task, Agent, Reservation) rather than one channel per event type. Every event carries a discriminated type tag. Two categories of consumer exist conceptually: **the matching engine itself** (reacts to relevant events by re-running the matching pass) and **external observers** (a live UI, a historical-reporting service, an alerting service) that only ever read the stream and never influence routing state directly.

### 6.2 Complete event list

| Event | Channel | Triggering Condition | Payload |
|---|---|---|---|
| **Task Enqueued** | Task | A new task is created | `taskId, queueId, taskType` |
| **Task Accepted** | Task | A reservation for this task is accepted | `taskId, agentId` |
| **Task Completed** | Task | The task-completion capability is invoked | `taskId, agentId (nullable)` |
| **Agent Created** | Agent | A new agent is provisioned | `agentId, status` |
| **Agent Status Changed** | Agent | The master-status-update capability is invoked | `agentId, status` |
| **Agent Capacity Config Updated** | Agent | The full capacity map is replaced, or one channel's ready flag is toggled | `agentId` (no field-level detail — consumer must re-fetch if it needs the new values) |
| **Agent Queues Updated** | Agent | Queue memberships are replaced | `agentId, queues` (the new full list) |
| **Agent Deleted** | Agent | An agent is removed | `agentId` |
| **Reservation Created** | Reservation | The matching algorithm commits a new match | `reservationId, taskId, agentId, expiresAt` |
| **Reservation Accepted** | Reservation | The accept capability is invoked | `reservationId, taskId, agentId` |
| **Reservation Rejected** | Reservation | Any of: manual reject, automatic expiry, or agent deletion resolving a held reservation | `reservationId, taskId, agentId, reason` — reason is one of: agent explicitly declined / automatically expired / the agent was deleted |

**Notably absent:** there is no dedicated "attributes changed" event for an Agent (replacing an agent's attribute values produces no event, since attributes have no effect on routing today) and no distinct "reservation revoked/canceled" event separate from "rejected" — every non-acceptance outcome is represented as the same Rejected event, differentiated only by its reason.

### 6.3 Consumption contract

| Consumer | Reacts to | Behavior |
|---|---|---|
| **Matching engine** | Every event on all three channels | Treats *any* event as "something changed, re-scan everything" — it does not special-case which specific event triggered it, with one exception: among Reservation events, only Rejected causes a meaningful re-scan (Created/Accepted are consumed but are harmless no-ops against an idempotent matching pass). |
| **Live notification delivery (external observers)** | A deliberately filtered subset: Reservation Created, Reservation Rejected, Agent Status Changed, Agent Deleted | Forwards these, and only these, to connected real-time clients. Every other event type is still consumed (required for the underlying delivery mechanism's own bookkeeping) but is not forwarded to any client. |
| **Historical/analytics consumers** (conceptual — a separate service's concern) | Whatever subset it chooses | Not part of the Task Router's own contract; the Task Router's only obligation is that every state-changing event above is reliably published exactly once per occurrence, in a form any number of independent, non-competing consumers can each fully observe. |

### 6.4 Delivery guarantees required of the underlying mechanism

- Each of the three logical channels must support **multiple independent consumer groups**, where each group receives its own complete copy of every event on that channel — one consumer group's processing rate or backlog must never cause another consumer group to miss or be delayed on any event.
- Within a single consumer group, multiple competing instances (e.g. horizontally scaled matching engines) must be able to divide the work of processing events without any event being processed twice by that group, and this is relied upon as the basis for safe horizontal scaling of the matching engine (§5.4, rule 6) — but correctness does not depend on this alone, since every state mutation is independently re-validated atomically regardless of how many times an event is delivered.
- Real-time client delivery (§3.5) does not require replay/durability semantics — it is a live, best-effort forwarding feed, not an audit log; a disconnected-then-reconnected client is expected to lose nothing it strictly needs, because a client can always fall back to reading current state directly rather than depending on having seen every historical event.

---

## 7. Known Gaps for v2 Design

The following capabilities were requested for documentation in the original scope of this exercise but **do not exist in any form** in the current codebase — no data model fields, no endpoints, no algorithm logic, no events. Each is expanded below into a real requirements-gathering starting point: what's missing, why it matters, the concrete design questions the rebuild team needs to answer, and how it interacts with the rest of this spec. None of the options listed under each gap are extracted from the code — they are the standard range of approaches used elsewhere in the CCaaS industry, offered so the rebuild team has a starting vocabulary, not a recommendation.

### 7.1 Transfers (Cold/Queue-to-Queue, Warm/Peer-to-Peer)

**What's missing:** No transfer capability of any kind exists. There is no `transferCount`, `isTransferred`, `transferHistory`, or `retainQueuePosition` field anywhere in the domain model (§2.7), no transfer endpoint, and no transfer event. A task cannot currently be moved between queues or handed from one agent to another except by the accidental side effect of agent deletion (§5.5) — which is an *unassignment*, not a transfer: it has no concept of "who initiated this" or "where should it go next," it simply drops the task back to `Pending` in its original queue.

**Why it matters:** Transfers are a baseline expectation of any real contact-center platform — an agent who can't resolve an interaction needs to hand it to another queue (cold/blind transfer) or another specific agent (warm/peer-to-peer transfer), and a supervisor needs visibility into how often and why transfers happen (a high transfer rate on a queue is a common quality signal).

**Design questions the rebuild must answer:**
- **Queue-to-queue (cold) transfer**: When a task moves to a new queue, does it keep its original `enqueuedAt` (preserving its place in line relative to when it *first* entered the system) or does it get a fresh timestamp (going to the back of the new queue)? The current system's only analogous precedent — a task returning to `Pending` after a rejected reservation — always preserves the original timestamp (§5.1), which suggests a consistent answer for transfers, but a queue-to-queue transfer is a materially different scenario (different queue, not the same one) and deserves an explicit decision rather than inheriting this by default.
- **What does `retainQueuePosition` actually mean as a caller-facing option?** Is it a per-transfer flag (the transferring agent/supervisor chooses), a per-queue policy (all transfers into/out of this queue behave one way), or always-on/always-off system behavior?
- **Peer-to-peer (warm) transfer**: Does this require the receiving agent to explicitly accept (mirroring the existing Reservation Offered→Accepted handshake, §5.2) or is it a direct assignment that bypasses the offer stage entirely? If it reuses the Reservation handshake, does the *sending* agent remain the `assignedAgentId` until the receiving agent accepts, or does the task have a period with no clear single owner?
- **Does a transfer respect the normal eligibility gates** (§4.1 — presence, queue membership, capacity) for the destination, or can a transfer target an agent/queue that wouldn't otherwise be eligible? (This has direct bearing on §7.4 below — a transfer that bypasses gates is functionally a form of force-routing.)
- **Transfer count and capping**: what should happen when a task has been transferred some threshold number of times — is there a hard cap that blocks further transfers, a supervisor alert (a natural fit for the Notification/Alerting service), or purely a reporting metric with no behavioral effect?
- **Audit trail shape**: does `transferHistory` need to capture just a sequence of queue/agent hops with timestamps, or richer detail (who initiated it, a reason code/note, whether it was cold or warm)? This has direct implications for what the Reporting service needs to ingest (it already has a `reservation_history`-style table pattern that a `transfer_history` table could mirror).
- **Interaction with capacity accounting** (§4.2): does the *sending* agent's capacity free up the instant a transfer is initiated, or only once the *receiving* side has accepted/taken over? A gap between the two has real consequences for double-counting or under-counting active capacity.

### 7.2 Attribute-Based (Skill) Matching

**What's missing:** Attributes are fully modeled and validated for shape (§2.6, §4.4) — every `Agent.attributes` and `Task.requiredAttributes` key must be pre-registered with a type, and values are type-checked on every write — but the matching algorithm (§4.1) never reads either map. `_agent_can_take`'s only checks are presence, queue membership, and capacity headroom; there is no comparison of a task's required attributes against a candidate agent's attributes anywhere in the matching pass. This is confirmed as deliberate PoC scope in the code's own comments, not an oversight (unlike §7.3 below).

**Why it matters:** Skill-based/attribute-based routing is typically the single most consequential routing feature in a real contact center — it's what lets a platform route "Spanish-speaking, Tier-2 billing" tasks only to agents who actually have those qualities, rather than merely "any agent in the billing queue."

**Design questions the rebuild must answer:**
- **Hard gate vs. soft scoring**: should a task's required attributes function as a strict pass/fail filter (an agent without a required attribute is simply not a candidate at all, the same binary way queue membership works today), or should attributes contribute to a ranking/score among multiple otherwise-eligible agents (e.g. an agent matching more of a task's *preferred* — not required — attributes is favored, but an agent lacking a preferred one is still eligible)?
- **Required vs. preferred distinction**: the current `requiredAttributes` naming implies a hard requirement, but there's no equivalent concept for a "nice to have" attribute today. Does the rebuild need both, or is a single hard-gate model sufficient for launch?
- **Numeric attribute comparison semantics**: for a numeric attribute (e.g. "English proficiency: 90"), does a task's required value mean "agent's value must be ≥ this," "≥ this within some tolerance," or an exact match? The current type system (§2.6) supports numeric values but defines no comparison semantics at all, since none is ever evaluated.
- **Boolean attribute semantics**: presumably a required `true` means the agent must also have `true` — but does a task requiring `false` (or simply omitting the attribute) mean "don't care" or "must explicitly be false"? This needs an explicit rule, since the current validation logic treats `{}` (no attributes) as always valid regardless of what's registered (§2.6), which won't directly answer "what does a *populated* required-attributes map with a missing key on the agent side mean."
- **Performance/scale implication**: this gap is directly connected to the matching algorithm's current agent-selection approach having "no ordering guarantee among agents" and "no tie-breaking rule" (§4.1) — introducing attribute matching is very likely the point at which the rebuild needs to also decide on §7.2's sibling question of *how* multiple qualifying agents get ranked, since "first eligible agent found" stops being an adequate rule once skill-quality differences among eligible agents become visible/meaningful.

### 7.3 Per-Channel Ready Gate & Interruptibility

**What's missing:** Both `ChannelCapacity.ready` and `ChannelCapacity.interruptible` are fully modeled fields, both are toggleable via dedicated capabilities (§3.2 — replacing the whole capacity map, or toggling one channel's ready flag independently), and both round-trip correctly through every read/write path — but **neither is ever read by `_agent_can_take` or the match-commit logic** (§4.1, confirmed by exhaustive code search). An agent with `ready: false` explicitly set on a channel is matched exactly as if it were `true`.

**Why this is flagged separately from the other gaps:** unlike attribute matching (§7.2, explicitly documented in the code as out-of-scope-for-now) or transfers (§7.1, no supporting infrastructure exists at all), this gap has a **complete, working capability built specifically to toggle a flag that then does nothing** — a dedicated "toggle one channel's ready flag without resending the whole capacity map" endpoint exists (§3.2) whose entire plausible purpose (an agent-facing UI switch for "stop sending me new chats but let me finish this one") is never realized in the actual routing decision. This strongly suggests an incomplete feature rather than an intentional design boundary, and is worth prioritizing differently from the other gaps for exactly that reason.

**Design questions the rebuild must answer:**
- **The `ready` gate itself**: should the matching algorithm require `channel.ready == true` in addition to `active < max`, matching what the existing toggle capability's naming and UI-facing use case (a "pause new work on this channel" switch) implies it was built for? This is close to a "complete the feature as clearly intended" decision rather than an open design question.
- **`interruptible`'s purpose is entirely undefined** — there is no comment, docstring, or naming convention in the current code that states what this flag is supposed to *do* once read (unlike `ready`, whose intended purpose is inferable from its own toggle capability's description). The rebuild needs to originate this requirement from scratch: is it meant to gate whether a *new* higher-priority task can preempt an agent's current lower-priority active work? Whether an agent can be given a second concurrent task at all while one channel is "interruptible: false"? This needs product input, not just an engineering decision, since the current codebase gives no signal of intended semantics.
- **Backward-compatibility consideration**: because `ready`/`interruptible` already exist as real, stored, API-visible fields with real default values (`true` for both), any rebuild that changes their meaning or starts enforcing them needs a plan for agents/integrations that set these fields today with no expectation that they'll suddenly start affecting routing.

### 7.4 Supervisor Force-Routing / Manual Assignment

**What's missing:** No capability exists for a supervisor, admin, or any actor other than the automated matching pass to directly assign a specific task to a specific agent. Every task-to-agent assignment happens exclusively through `_TRY_RESERVE_LUA`'s atomic commit inside the standard matching pass (§4.1) — there is no code path that creates a Reservation or sets `assignedAgentId` any other way. There is no "supervisor" concept anywhere in the current codebase at all (no role, no elevated-permission action) — this is consistent with Identity/Presence not existing as a service yet in the broader platform roadmap.

**Why it matters:** Force-routing is a standard supervisor tool for handling exceptions the automated algorithm can't or shouldn't resolve on its own — a VIP caller who must reach a specific named agent regardless of that agent's current queue memberships, or an urgent task that needs to bypass a backlog.

**Design questions the rebuild must answer:**
- **Which gates does a forced assignment still have to respect, if any?** At minimum, does it still need to respect capacity (`active < max`), or can a supervisor force an agent to take on work beyond their configured `max`? Does it need to respect queue membership, or can a supervisor assign any task to any agent regardless of that agent's queues? Bypassing *all* gates (including presence — assigning to an agent who isn't `"Available"`) is the most permissive interpretation of "force," but may not be what's actually wanted.
- **Does a forced assignment still go through the Reservation Offered→Accepted handshake** (giving the agent a chance to decline, and preserving the existing accept/reject event contract, §5.2), or does it assign directly to `Active`, skipping the offer stage entirely? This is a meaningfully different UX and audit story.
- **What happens if the target agent is already at capacity or already holds a different Reserved/Active task on the relevant channel?** Does the forced assignment queue behind the existing work, does it displace/preempt the existing work (interacting directly with the undefined `interruptible` semantics in §7.3), or is it simply rejected with an error back to the supervisor?
- **Authorization model**: this capability inherently depends on knowing who a "supervisor" is, which depends on the Identity/Presence service (already in progress per the broader roadmap) existing and being wired into the Task Router's authorization boundary — force-routing cannot be meaningfully scoped as "supervisor-only" until real roles exist to check against.
- **Event/audit implications**: does a forced assignment need its own distinct event (e.g. a "manually assigned" flag or reason on the Reservation Created event) so that Reporting and any future Quality Management service can distinguish organic matches from supervisor interventions? The current event catalog (§6.2) has no field on `Reservation Created` for provenance/cause today.

### 7.5 Bullseye Routing / Attribute Relaxation Over Time

**What's missing:** No time-based escalation or priority-tier logic exists anywhere in the current system. A task's eligibility criteria (queue, type) are fixed permanently at creation and never change while it waits in `Pending` — the *only* way a stuck task ever finds a match is a new agent becoming eligible (changing status to Available, freeing capacity, joining the queue); the task's own requirements never loosen to make more agents eligible for it.

**Why it matters:** "Bullseye" routing (the term for concentric-ring attribute relaxation over time — starting with a narrow, ideal agent match and progressively widening the pool of eligible agents the longer a task waits) is a standard technique for balancing service quality against service level: prefer the best-matched agent, but don't let a task starve indefinitely waiting for a perfect match that may never come.

**Design questions the rebuild must answer:**
- **This gap is directly dependent on §7.2 existing first.** Bullseye routing is fundamentally about relaxing *attribute* requirements over time — without attribute-based matching existing at all, there is nothing for a bullseye mechanism to relax. The rebuild should treat these as one combined initiative, or at minimum sequence attribute matching (§7.2) before bullseye design begins in earnest.
- **What is the relaxation unit and schedule?** Time-based (every N seconds, drop the least-important required attribute to preferred, or widen a numeric threshold), or tier-based (queue configuration defines explicit "ring 1 / ring 2 / ring 3" agent pools with their own criteria, and a task escalates through them on a schedule)?
- **Does relaxation apply per-task or per-queue?** A per-queue configuration (all tasks in this queue follow the same escalation schedule) is simpler to reason about and configure than a per-task override, but a per-task override may be needed for priority/VIP scenarios.
- **Interaction with the FIFO guarantee** (§4.1): today, task selection order is strictly FIFO by enqueue time, completely independent of *how* eligible any given task's matching pool is. Once tasks can have different, shrinking eligibility pools at different rates, does strict FIFO still hold across all tasks in a queue, or does a task whose pool has widened (via relaxation) get evaluated preferentially over an older task whose pool hasn't relaxed yet? This is a genuine, non-obvious design tension the rebuild needs to resolve explicitly rather than let emerge as an implementation accident.

### 7.6 Queue Timeout Behavior

**What's missing:** There is no maximum-wait-time concept for a `Pending` task independent of the Reservation TTL — the only expiry mechanism that exists today (§4.3) applies exclusively *after* a task has already been offered to a specific agent (Reserved status); a task can sit in `Pending`, having never yet been offered to anyone, indefinitely with no automatic escalation, no automatic requeue-to-a-different-queue, and no automatic abandonment.

**Why it matters:** A queue timeout (sometimes called an overflow or escalation timeout) is the standard mechanism for guaranteeing a task doesn't wait forever when its home queue has no eligible agents at all — after some threshold, it should be able to escalate (a natural fit for the Notification/Alerting service — "this queue has a task that's waited too long" is exactly the shape of alert already proven out in that service's one real rule), overflow to a different queue, or be marked abandoned if the platform supports customer-initiated abandonment (e.g. a caller hanging up) as a distinct concept from agent-side task completion.

**Design questions the rebuild must answer:**
- **Escalation vs. overflow vs. abandonment are three different behaviors** that a "queue timeout" could mean, and the rebuild needs to decide whether it needs one, some, or all three: (a) escalation — raise priority/alert a supervisor but leave the task in the same queue; (b) overflow — actually move the task to a different queue (which is really a system-initiated transfer, directly overlapping with §7.1's design questions about preserving FIFO position across a queue change); (c) abandonment — an entirely new terminal task state that isn't `Completed` (the task was never actually handled), which has no equivalent anywhere in the current state machine (§5.1) and would need its own status value and likely its own event.
- **Is the timeout configured per-queue, globally, or per-task-type?** The current Queue entity (§2.4) has zero configuration fields today beyond an ID and creation timestamp — adding a timeout would be the first piece of real queue-level configuration in the domain model.
- **Sequencing relative to §7.5**: if bullseye/attribute-relaxation (§7.5) is also being designed, a queue timeout and a relaxation schedule are two time-based escalation mechanisms operating on the same waiting task — the rebuild should design them together rather than as two independent, potentially conflicting timers (e.g., does relaxation happen *instead of* a hard timeout, or as an earlier, softer step before a harder timeout eventually fires?).

---

*End of specification. This document reflects the Task Router codebase as of the analysis date; any future behavioral change to the current system should be reflected here before being treated as ground truth for the rebuild.*
