# task-router

**Responsibility:** Owns the routing algorithm: matches queued Tasks to
available agents based on skills, priority, tenant routing policy, and
real-time presence. Treated in the architecture doc as a black box with a
well-defined contract: consumes TaskCreated/TaskCancelled events + agent
availability queries; emits TaskAssigned/TaskRequeued events (architecture
doc Section 2.2). Its full functional behavior is specified in
`TASK_ROUTER_SPECIFICATION.md` at the repo root.

**State:** Stateful (in-memory routing queues, ideally checkpointed).

**Status:** scaffold only. gRPC server with health check and tenant-context
interceptors wired; no routing algorithm, no proto contract of its own yet
(that comes from TASK_ROUTER_SPECIFICATION.md in a later milestone), and no
dial to Agent & Presence Service's PresenceService yet. This is next up for
domain logic.
