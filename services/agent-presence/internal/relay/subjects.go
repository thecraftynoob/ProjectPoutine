package relay

import (
	"fmt"
	"strings"
)

// Subject-filter fragments matching Task Router's actual publish targets
// (services/task-router/internal/events/events.go), confirmed against
// that file rather than the abstract spec description:
//
//	tenant.{tenantId}.reservation.created
//	tenant.{tenantId}.reservation.rejected
//	tenant.{tenantId}.agent.status.changed
//	tenant.{tenantId}.agent.deleted
//
// Task Router's own event-type constants use "status.changed" (two
// subject tokens, matching architecture doc Section 5's
// "past-tense-verb" naming taken literally as "status.changed"), which
// is why the reservation-domain filter below is a single wildcard
// segment but the agent-domain ones enumerate two forwarded event types
// explicitly (Agent Capacity Config Updated / Agent Queues Updated /
// Agent Created all also live under "tenant.*.agent.>" but must NOT be
// forwarded per spec Section 6.3's filtered subset, so a blanket
// "tenant.*.agent.>" filter would over-forward).
const (
	subjectReservationCreated  = "tenant.*.reservation.created"
	subjectReservationRejected = "tenant.*.reservation.rejected"
	subjectAgentStatusChanged  = "tenant.*.agent.status.changed"
	subjectAgentDeleted        = "tenant.*.agent.deleted"
)

// ForwardedSubjectFilters is the exact set of JetStream filter subjects
// this relay subscribes to -- one durable consumer per filter, all
// sharing Task Router's TASK_ROUTER_EVENTS stream (see
// services/task-router/internal/events.StreamName). Deliberately NOT a
// single "tenant.*.>" filter: every other event type in Task Router's
// catalog (Task Enqueued/Accepted/Completed, Agent Created/Capacity/
// Queues Updated, Reservation Accepted) must never reach a connected
// client, per TASK_ROUTER_SPECIFICATION.md Section 6.3's "forwards
// these, and only these" contract.
var ForwardedSubjectFilters = []string{
	subjectReservationCreated,
	subjectReservationRejected,
	subjectAgentStatusChanged,
	subjectAgentDeleted,
}

// classify maps one concrete NATS subject (e.g.
// "tenant.<uuid>.agent.status.changed") to the wire-level EventType and
// the tenant ID segment, or returns ok=false if the subject does not
// match any of the four forwarded shapes -- which should not happen
// given ForwardedSubjectFilters above, but classify is defensive since a
// stream can in principle redeliver anything matching a broader filter
// if this relay's filters are ever loosened.
func classify(subject string) (eventType EventType, tenantID string, ok bool) {
	parts := strings.Split(subject, ".")
	// tenant.{tenantId}.{domain}.{...eventType parts}
	if len(parts) < 4 || parts[0] != "tenant" {
		return "", "", false
	}
	tenantID = parts[1]
	domain := parts[2]
	rest := strings.Join(parts[3:], ".")

	switch {
	case domain == "reservation" && rest == "created":
		return EventTypeReservationCreated, tenantID, true
	case domain == "reservation" && rest == "rejected":
		return EventTypeReservationRejected, tenantID, true
	case domain == "agent" && rest == "status.changed":
		return EventTypeAgentStatusChanged, tenantID, true
	case domain == "agent" && rest == "deleted":
		return EventTypeAgentDeleted, tenantID, true
	default:
		return "", "", false
	}
}

// channelName builds the Redis pub/sub channel name every replica
// publishes to and subscribes on for a given tenant, per the
// cross-replica fan-out design documented in
// services/agent-presence/internal/relay/fanout.go.
func channelName(tenantID string) string {
	return fmt.Sprintf("agent-presence:tenant:%s:events", tenantID)
}
