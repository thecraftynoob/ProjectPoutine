// Package relay bridges Task Router's NATS JetStream event stream to
// connected Agent Desktop WebSocket clients. It implements exactly the
// filtering behavior TASK_ROUTER_SPECIFICATION.md Section 6.3 assigns to
// "Live notification delivery (external observers)" -- forwarding a
// deliberately filtered subset (Reservation Created, Reservation
// Rejected, Agent Status Changed, Agent Deleted) and nothing else -- but
// implemented here in Agent & Presence Service rather than in Task
// Router itself, per this milestone's scope decision (Task Router's own
// spec explicitly does not implement Section 3.5/6.3's delivery side; it
// only publishes).
package relay

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// EventType is the wire-level discriminator sent to clients in every
// Envelope, one per forwarded event per spec Section 6.3's filtered
// subset. Values are "{domain}.{eventType}" using Task Router's own
// internal/events naming so the wire contract reads the same as the
// event catalog in TASK_ROUTER_SPECIFICATION.md Section 6.2.
type EventType string

const (
	EventTypeReservationCreated  EventType = "reservation.created"
	EventTypeReservationRejected EventType = "reservation.rejected"
	EventTypeAgentStatusChanged  EventType = "agent.status_changed"
	EventTypeAgentDeleted        EventType = "agent.deleted"
)

// Envelope is the JSON document delivered as a single WebSocket text
// frame to a connected Agent Desktop client. See
// services/agent-presence/README.md for the documented wire contract.
type Envelope struct {
	Type    EventType      `json:"type"`
	AgentID string         `json:"agentId"`
	Payload map[string]any `json:"payload"`
}

// decodePayload unmarshals a JetStream message body (protobuf-encoded
// structpb.Struct, per pkg/eventbus.PublishEvent / Task Router's
// internal/events.Publisher) back into a plain map[string]any, re-using
// Task Router's actual on-the-wire encoding rather than inventing a new
// one.
func decodePayload(data []byte) (map[string]any, error) {
	var s structpb.Struct
	if err := proto.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("relay: decode event payload: %w", err)
	}
	return s.AsMap(), nil
}

// fieldString reads a string field out of a decoded payload map,
// returning "" if absent or not a string. Task Router's events package
// always writes agentId/taskId/etc. as plain Go strings before
// structpb.NewStruct, so this covers every field this package reads.
func fieldString(fields map[string]any, name string) string {
	v, ok := fields[name]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

// buildEnvelope constructs the client-facing Envelope for one forwarded
// event, given its already-decoded payload fields and the eventType that
// selected it (see subjects.go for how a NATS subject maps to one of the
// four forwarded EventType values). agentId is pulled from the payload
// per event-family shape:
//   - Reservation Created / Rejected: payload carries agentId directly.
//   - Agent Status Changed / Deleted: the event's own subject-of-event
//     agentId is also present in the payload (Task Router's events.go
//     always includes it as a field), so the same lookup works uniformly.
func buildEnvelope(eventType EventType, fields map[string]any) (Envelope, error) {
	agentID := fieldString(fields, "agentId")
	if agentID == "" {
		return Envelope{}, fmt.Errorf("relay: event %s payload missing non-empty agentId", eventType)
	}
	return Envelope{
		Type:    eventType,
		AgentID: agentID,
		Payload: fields,
	}, nil
}

// MarshalJSON is a small convenience used by both the Redis fan-out
// publisher and (in tests) direct assertions, so the exact bytes put on
// the wire to the browser are produced by one code path.
func (e Envelope) MarshalJSON() ([]byte, error) {
	type alias Envelope // avoid infinite recursion through MarshalJSON
	return json.Marshal(alias(e))
}
