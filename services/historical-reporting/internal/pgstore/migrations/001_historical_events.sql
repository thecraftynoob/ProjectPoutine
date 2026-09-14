-- Historical Reporting's single generic ingestion table (this milestone's
-- entire scope -- see package doc comment on internal/pgstore for the
-- "one generic table, no partitioning, no ClickHouse yet" rationale).
--
-- Every event Task Router publishes across all three of its domains
-- (task/agent/reservation) lands here as one row, keyed by a fresh
-- event_id generated at ingestion time (NOT derived from the NATS
-- message -- see internal/eventconsumer's package doc comment for why
-- there is no natural idempotency key to reuse instead).

CREATE TABLE historical_events (
    event_id    UUID PRIMARY KEY,
    tenant_id   UUID NOT NULL,
    domain      TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    subject     TEXT NOT NULL,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Composite index for the obvious "this tenant's timeline" query a future
-- reporting API would run first (e.g. "everything for tenant X in the
-- last 24h").
CREATE INDEX idx_historical_events_tenant_received ON historical_events (tenant_id, received_at);

-- Composite index for the obvious "this tenant's events of type Y" query
-- (e.g. "every reservation.rejected for tenant X").
CREATE INDEX idx_historical_events_tenant_domain_type ON historical_events (tenant_id, domain, event_type);
