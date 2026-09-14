package pgstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is one materialized row of historical_events -- the generic shape
// every published Task Router event (task/agent/reservation domains)
// collapses into.
type Event struct {
	EventID    uuid.UUID
	TenantID   uuid.UUID
	Domain     string
	EventType  string
	Subject    string
	Payload    json.RawMessage
	ReceivedAt time.Time
}

// Store wraps a plain (non-RLS) pgxpool.Pool for historical_events reads
// and writes -- see package doc comment for why this table doesn't use
// pkg/pgtenant's RLS pattern.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore constructs a Store.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// InsertEvent generates a fresh event_id and inserts one row into
// historical_events. See package doc comment / internal/eventconsumer for
// why event_id is generated here rather than derived from the inbound
// NATS message.
func (s *Store) InsertEvent(ctx context.Context, tenantID uuid.UUID, domain, eventType, subject string, payload json.RawMessage) (uuid.UUID, error) {
	eventID := uuid.New()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO historical_events (event_id, tenant_id, domain, event_type, subject, payload)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, eventID, tenantID, domain, eventType, subject, payload)
	if err != nil {
		return uuid.Nil, fmt.Errorf("pgstore: insert event: %w", err)
	}
	return eventID, nil
}

// GetEvent reads back one row by event_id -- used today only by this
// package's own round-trip test (no RPC/query API exists yet, per this
// milestone's scope).
func (s *Store) GetEvent(ctx context.Context, eventID uuid.UUID) (Event, error) {
	var e Event
	err := s.pool.QueryRow(ctx, `
		SELECT event_id, tenant_id, domain, event_type, subject, payload, received_at
		FROM historical_events
		WHERE event_id = $1
	`, eventID).Scan(&e.EventID, &e.TenantID, &e.Domain, &e.EventType, &e.Subject, &e.Payload, &e.ReceivedAt)
	if err != nil {
		return Event{}, fmt.Errorf("pgstore: get event: %w", err)
	}
	return e, nil
}
