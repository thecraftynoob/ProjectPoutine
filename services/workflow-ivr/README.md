# workflow-ivr

**Responsibility:** Executes tenant-defined call/chat flows as a state
machine: plays prompts, collects DTMF or NLU intents, queries external
systems (CRM lookups, business-hours checks) before a Task is hard-queued to
the Router. This is where a future Conversational IVR AI plugs in as a
drop-in "intent" step (architecture doc Section 2.2, Section 4.3).

**State:** Stateless (flow state persisted per-session in Redis so any
replica can continue a flow).

**Status:** scaffold only. gRPC server with health check and tenant-context
interceptors wired; no state machine, domain RPCs, or Redis/NATS
connections yet.
