# api-gateway

**Responsibility:** Single external entry point: TLS termination, JWT
validation, tenant resolution, request routing to internal gRPC services,
WebSocket upgrade proxying for Agent Desktop. Described in the architecture
doc as "supporting, not a domain service" (Section 2.2) — it fronts the
other services rather than owning a bounded context itself.

**State:** Stateless.

**Status:** scaffold only, and deliberately more minimal than the other
services: this stub is a placeholder gRPC health-checkable service only. It
does not apply the tenant-context server interceptor, since the Gateway's
real job is to *originate* `x-tenant-id` from a validated JWT claim (Layer 1
enforcement, architecture doc Section 1.1), not to validate one on inbound
external/unauthenticated traffic. The real BFF/REST/WebSocket edge
implementation — JWT validation, REST routing, WebSocket upgrade proxying —
is future work.
