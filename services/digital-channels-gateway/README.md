# digital-channels-gateway

**Responsibility:** Normalizes inbound Chat, SMS, Email, and Social messages
into the same generic Task abstraction the voice path produces, so the Task
Router never needs to know the origin channel. Handles outbound message
delivery (agent replies) back to the originating channel provider
(architecture doc Section 2.2).

**State:** Stateless.

**Status:** scaffold only. gRPC server with health check and tenant-context
interceptors wired; no channel-provider integrations, domain RPCs, or
Postgres/NATS connections yet.
