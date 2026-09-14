# digital-channels-gateway

**Responsibility:** Normalizes inbound Chat, SMS, Email, and Social messages
into the same generic Task abstraction the voice path produces, so the Task
Router never needs to know the origin channel. Handles outbound message
delivery (agent replies) back to the originating channel provider
(architecture doc Section 2.2).

**State:** Stateless.

**Status:** first real domain milestone built. `POST
/webhooks/chat/{tenant_id}` accepts one generic inbound "chat" channel
shape and calls Task Router's `EnqueueTask` RPC as a service (via
`pkg/svcauth`), ending at a successfully enqueued Task -- see
`internal/webhookapi`. Inbound only: no outbound/agent-reply delivery, no
Postgres persistence of messages/threads (explicitly deferred to a future
async-worker milestone), no webhook signature verification (explicitly
deferred -- see `internal/webhookapi`'s `ServeHTTP` doc comment for that
tradeoff), and no real per-provider (Twilio, etc.) integration yet. The
scaffold's gRPC server (health check + tenant-context interceptors) is
unchanged and still runs alongside the new HTTP server.
