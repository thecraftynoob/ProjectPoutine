# historical-reporting

**Responsibility:** Subscribes to all domain events (durably, via
JetStream) and materializes them into query-optimized storage for BI
dashboards, SLA/QoS reporting, and compliance exports. Never sits in the
real-time path of any other service (architecture doc Section 2.2).

**State:** Stateless ingestion / backed by a stateful analytical store
(PostgreSQL with partitioned tables initially; ClickHouse documented as a
future upgrade path).

**Status:** scaffold only. gRPC server with health check and tenant-context
interceptors wired (for operational/admin RPCs); no JetStream consumers or
Postgres connection yet.
