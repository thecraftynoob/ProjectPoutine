# historical-reporting

**Responsibility:** Subscribes to all domain events (durably, via
JetStream) and materializes them into query-optimized storage for BI
dashboards, SLA/QoS reporting, and compliance exports. Never sits in the
real-time path of any other service (architecture doc Section 2.2).

**State:** Stateless ingestion / backed by a stateful analytical store
(PostgreSQL with partitioned tables initially; ClickHouse documented as a
future upgrade path).

**Status:** first real milestone built — ingestion only. A durable NATS
JetStream consumer (`internal/eventconsumer`, durable consumer name
`historical-reporting-ingest`, single `FilterSubject` of `tenant.*.>`
covering all three of Task Router's domains) materializes the full event
catalog into one generic Postgres table, `historical_events`
(`internal/pgstore`). No read/query API, no new RPC, no REST route this
pass — verification is direct SQL (see `ARCHITECTURE_FLOW.md`'s
"Subscribed by Historical Reporting" section for the full design
writeup, including the documented at-least-once/no-idempotency-key
tradeoff). The gRPC server with health check and tenant-context
interceptors is unchanged from the scaffold, still running alongside for
K8s liveness/readiness probes.
