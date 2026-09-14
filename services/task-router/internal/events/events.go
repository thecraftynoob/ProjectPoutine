// Package events implements Task Router's event-publishing side of the
// full event catalog in TASK_ROUTER_SPECIFICATION.md Section 6.2, via
// /pkg/eventbus, using the tenant.{tenant_id}.{domain}.{event} subject
// convention (architecture doc Section 5) with task/agent/reservation as
// the three domains (spec Section 6.1's "grouped by entity family").
//
// Event-type names are past-tense-verb per architecture doc Section 5
// (e.g. "task.enqueued", not "task.enqueue"). Each publish call is
// best-effort from the caller's perspective in the sense that a
// publish failure is logged and returned to the caller as an error, but
// the domain mutation itself (already committed via a Lua script before
// any Publish call is ever made) is never rolled back on a publish
// failure -- the mutation is the source of truth; the event is a
// downstream notification of a fact that already happened.
package events

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/thecraftynoob/ProjectPoutine/pkg/eventbus"
	"google.golang.org/protobuf/types/known/structpb"
)

// Domain event-subject domains (spec Section 6.1).
const (
	DomainTask        = "task"
	DomainAgent       = "agent"
	DomainReservation = "reservation"
)

// Event-type names (past-tense verb per architecture doc Section 5),
// one per row of spec Section 6.2's event catalog.
const (
	EventTaskEnqueued  = "enqueued"
	EventTaskAccepted  = "accepted"
	EventTaskCompleted = "completed"
	// EventTaskEnded / EventTaskDispositionSet: Wrap Up / Disposition
	// two-step completion lifecycle additions.
	EventTaskEnded          = "ended"
	EventTaskDispositionSet = "disposition.set"

	EventAgentCreated               = "created"
	EventAgentStatusChanged         = "status.changed"
	EventAgentCapacityConfigUpdated = "capacity.config.updated"
	EventAgentQueuesUpdated         = "queues.updated"
	EventAgentDeleted               = "deleted"
	// EventAgentWrapUpTimedOut: the wrap-up timer reached 0 while the task
	// was still WrapUp, resetting the agent's status to Available.
	EventAgentWrapUpTimedOut = "wrapup.timed_out"

	EventReservationCreated  = "created"
	EventReservationAccepted = "accepted"
	EventReservationRejected = "rejected"
)

// StreamName is the single JetStream stream Task Router publishes all
// three domains' events to, per architecture doc Section 1.2's durability
// guidance -- one stream covering the "tenant.*.task.>",
// "tenant.*.agent.>", "tenant.*.reservation.>" subject space so a single
// EnsureStream call at startup provisions everything.
const StreamName = "TASK_ROUTER_EVENTS"

// StreamSubjects is passed to eventbus.Client.EnsureStream at startup.
var StreamSubjects = []string{
	"tenant.*.task.>",
	"tenant.*.agent.>",
	"tenant.*.reservation.>",
}

// Publisher wraps an eventbus.Client with typed helpers for every event
// in spec Section 6.2's catalog, so call sites in grpcapi/redisdomain
// consumers can't typo a subject or forget a payload field.
type Publisher struct {
	client *eventbus.Client
}

// NewPublisher constructs a Publisher.
func NewPublisher(client *eventbus.Client) *Publisher {
	return &Publisher{client: client}
}

// EnsureStream provisions the JetStream stream this publisher writes to.
// Call once at service startup.
func (p *Publisher) EnsureStream(ctx context.Context) error {
	return p.client.EnsureStream(ctx, StreamName, StreamSubjects)
}

func structFrom(fields map[string]any) (*structpb.Struct, error) {
	s, err := structpb.NewStruct(fields)
	if err != nil {
		return nil, fmt.Errorf("events: build payload struct: %w", err)
	}
	return s, nil
}

func (p *Publisher) publish(ctx context.Context, tenantID uuid.UUID, domain, eventType string, fields map[string]any) error {
	payload, err := structFrom(fields)
	if err != nil {
		return err
	}
	if err := p.client.PublishEvent(ctx, tenantID, domain, eventType, payload); err != nil {
		return fmt.Errorf("events: publish %s.%s: %w", domain, eventType, err)
	}
	return nil
}

// --- Task domain (spec Section 6.2) ---

// TaskEnqueued: "A new task is created" -> {taskId, queueId, taskType}.
func (p *Publisher) TaskEnqueued(ctx context.Context, tenantID uuid.UUID, taskID, queueID, taskType string) error {
	return p.publish(ctx, tenantID, DomainTask, EventTaskEnqueued, map[string]any{
		"taskId":   taskID,
		"queueId":  queueID,
		"taskType": taskType,
	})
}

// TaskAccepted: "A reservation for this task is accepted" -> {taskId, agentId}.
func (p *Publisher) TaskAccepted(ctx context.Context, tenantID uuid.UUID, taskID, agentID string) error {
	return p.publish(ctx, tenantID, DomainTask, EventTaskAccepted, map[string]any{
		"taskId":  taskID,
		"agentId": agentID,
	})
}

// TaskCompleted: "The task-completion capability is invoked" ->
// {taskId, agentId (nullable), dispositionId (nullable), dispositionName
// (nullable)}. dispositionId/dispositionName are included so Historical
// Reporting's generic JSONB ingestion (services/historical-reporting/
// internal/eventconsumer) captures the disposition on the historical
// record without any historical-reporting-side schema change -- per the
// Wrap Up / Disposition two-step completion lifecycle's requirement that
// dispositions be visible in historical reporting.
func (p *Publisher) TaskCompleted(ctx context.Context, tenantID uuid.UUID, taskID, agentID, dispositionID, dispositionName string) error {
	fields := map[string]any{"taskId": taskID}
	if agentID != "" {
		fields["agentId"] = agentID
	} else {
		fields["agentId"] = nil
	}
	if dispositionID != "" {
		fields["dispositionId"] = dispositionID
		fields["dispositionName"] = dispositionName
	} else {
		fields["dispositionId"] = nil
		fields["dispositionName"] = nil
	}
	return p.publish(ctx, tenantID, DomainTask, EventTaskCompleted, fields)
}

// TaskEnded: "EndTask is invoked (step 1 of the Wrap Up / Disposition
// two-step completion lifecycle)" -> {taskId, agentId (nullable)}.
func (p *Publisher) TaskEnded(ctx context.Context, tenantID uuid.UUID, taskID, agentID string) error {
	fields := map[string]any{"taskId": taskID}
	if agentID != "" {
		fields["agentId"] = agentID
	} else {
		fields["agentId"] = nil
	}
	return p.publish(ctx, tenantID, DomainTask, EventTaskEnded, fields)
}

// TaskDispositionSet: "SetTaskDisposition is invoked" ->
// {taskId, dispositionId, dispositionName}.
func (p *Publisher) TaskDispositionSet(ctx context.Context, tenantID uuid.UUID, taskID, dispositionID, dispositionName string) error {
	return p.publish(ctx, tenantID, DomainTask, EventTaskDispositionSet, map[string]any{
		"taskId":          taskID,
		"dispositionId":   dispositionID,
		"dispositionName": dispositionName,
	})
}

// --- Agent domain (spec Section 6.2) ---

// AgentCreated: "A new agent is provisioned" -> {agentId, status}.
func (p *Publisher) AgentCreated(ctx context.Context, tenantID uuid.UUID, agentID, status string) error {
	return p.publish(ctx, tenantID, DomainAgent, EventAgentCreated, map[string]any{
		"agentId": agentID,
		"status":  status,
	})
}

// AgentStatusChanged: "The master-status-update capability is invoked" ->
// {agentId, status}.
func (p *Publisher) AgentStatusChanged(ctx context.Context, tenantID uuid.UUID, agentID, status string) error {
	return p.publish(ctx, tenantID, DomainAgent, EventAgentStatusChanged, map[string]any{
		"agentId": agentID,
		"status":  status,
	})
}

// AgentCapacityConfigUpdated: "The full capacity map is replaced, or one
// channel's ready flag is toggled" -> {agentId} (no field-level detail).
func (p *Publisher) AgentCapacityConfigUpdated(ctx context.Context, tenantID uuid.UUID, agentID string) error {
	return p.publish(ctx, tenantID, DomainAgent, EventAgentCapacityConfigUpdated, map[string]any{
		"agentId": agentID,
	})
}

// AgentQueuesUpdated: "Queue memberships are replaced" -> {agentId, queues}.
func (p *Publisher) AgentQueuesUpdated(ctx context.Context, tenantID uuid.UUID, agentID string, queues []string) error {
	q := make([]any, len(queues))
	for i, v := range queues {
		q[i] = v
	}
	return p.publish(ctx, tenantID, DomainAgent, EventAgentQueuesUpdated, map[string]any{
		"agentId": agentID,
		"queues":  q,
	})
}

// AgentDeleted: "An agent is removed" -> {agentId}.
func (p *Publisher) AgentDeleted(ctx context.Context, tenantID uuid.UUID, agentID string) error {
	return p.publish(ctx, tenantID, DomainAgent, EventAgentDeleted, map[string]any{
		"agentId": agentID,
	})
}

// AgentWrapUpTimedOut: "A task's wrap-up timer reached 0 while still
// WrapUp, resetting the agent's status to Available" -> {agentId, taskId}.
func (p *Publisher) AgentWrapUpTimedOut(ctx context.Context, tenantID uuid.UUID, agentID, taskID string) error {
	return p.publish(ctx, tenantID, DomainAgent, EventAgentWrapUpTimedOut, map[string]any{
		"agentId": agentID,
		"taskId":  taskID,
	})
}

// --- Reservation domain (spec Section 6.2) ---

// ReservationCreated: "The matching algorithm commits a new match" ->
// {reservationId, taskId, agentId, expiresAt}.
func (p *Publisher) ReservationCreated(ctx context.Context, tenantID uuid.UUID, reservationID, taskID, agentID, expiresAt string) error {
	return p.publish(ctx, tenantID, DomainReservation, EventReservationCreated, map[string]any{
		"reservationId": reservationID,
		"taskId":        taskID,
		"agentId":       agentID,
		"expiresAt":     expiresAt,
	})
}

// ReservationAccepted: "The accept capability is invoked" ->
// {reservationId, taskId, agentId}.
func (p *Publisher) ReservationAccepted(ctx context.Context, tenantID uuid.UUID, reservationID, taskID, agentID string) error {
	return p.publish(ctx, tenantID, DomainReservation, EventReservationAccepted, map[string]any{
		"reservationId": reservationID,
		"taskId":        taskID,
		"agentId":       agentID,
	})
}

// ReservationRejected: "Any of: manual reject, automatic expiry, or agent
// deletion resolving a held reservation" ->
// {reservationId, taskId, agentId, reason}.
func (p *Publisher) ReservationRejected(ctx context.Context, tenantID uuid.UUID, reservationID, taskID, agentID, reason string) error {
	return p.publish(ctx, tenantID, DomainReservation, EventReservationRejected, map[string]any{
		"reservationId": reservationID,
		"taskId":        taskID,
		"agentId":       agentID,
		"reason":        reason,
	})
}
