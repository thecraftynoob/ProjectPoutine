package relay

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// encodeLikeTaskRouter reproduces the exact wire encoding Task Router's
// internal/events.Publisher uses (structpb.NewStruct + proto.Marshal via
// pkg/eventbus.Client.PublishEvent), without importing that package
// (which is unimportable across service boundaries by Go's own
// internal/ visibility rule) -- this constructs synthetic NATS message
// bodies matching the real publisher's actual JSON/proto shape byte-for-
// byte, per the field lists in
// services/task-router/internal/events/events.go.
func encodeLikeTaskRouter(t *testing.T, fields map[string]any) []byte {
	t.Helper()
	s, err := structpb.NewStruct(fields)
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	data, err := proto.Marshal(s)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	return data
}

func TestClassify(t *testing.T) {
	cases := []struct {
		subject      string
		wantType     EventType
		wantTenantID string
		wantOK       bool
	}{
		{"tenant.acme.reservation.created", EventTypeReservationCreated, "acme", true},
		{"tenant.acme.reservation.rejected", EventTypeReservationRejected, "acme", true},
		{"tenant.acme.agent.status.changed", EventTypeAgentStatusChanged, "acme", true},
		{"tenant.acme.agent.deleted", EventTypeAgentDeleted, "acme", true},
		// Every other real Task Router subject must NOT classify (i.e.
		// must never be forwarded), per spec Section 6.3.
		{"tenant.acme.task.enqueued", "", "", false},
		{"tenant.acme.task.accepted", "", "", false},
		{"tenant.acme.task.completed", "", "", false},
		{"tenant.acme.agent.created", "", "", false},
		{"tenant.acme.agent.capacity.config.updated", "", "", false},
		{"tenant.acme.agent.queues.updated", "", "", false},
		{"tenant.acme.reservation.accepted", "", "", false},
		{"garbage", "", "", false},
	}

	for _, tc := range cases {
		gotType, gotTenant, gotOK := classify(tc.subject)
		if gotOK != tc.wantOK {
			t.Errorf("classify(%q): ok = %v, want %v", tc.subject, gotOK, tc.wantOK)
			continue
		}
		if !tc.wantOK {
			continue
		}
		if gotType != tc.wantType || gotTenant != tc.wantTenantID {
			t.Errorf("classify(%q) = (%q, %q), want (%q, %q)", tc.subject, gotType, gotTenant, tc.wantType, tc.wantTenantID)
		}
	}
}

func TestDecodePayload_ReservationCreated(t *testing.T) {
	// Matches events.Publisher.ReservationCreated's field set exactly.
	data := encodeLikeTaskRouter(t, map[string]any{
		"reservationId": "res-1",
		"taskId":        "task-1",
		"agentId":       "agent-1",
		"expiresAt":     "2026-09-13T00:00:30Z",
	})

	fields, err := decodePayload(data)
	if err != nil {
		t.Fatalf("decodePayload: %v", err)
	}

	env, err := buildEnvelope(EventTypeReservationCreated, fields)
	if err != nil {
		t.Fatalf("buildEnvelope: %v", err)
	}
	if env.Type != EventTypeReservationCreated {
		t.Errorf("Type = %q, want %q", env.Type, EventTypeReservationCreated)
	}
	if env.AgentID != "agent-1" {
		t.Errorf("AgentID = %q, want %q", env.AgentID, "agent-1")
	}
	if env.Payload["reservationId"] != "res-1" || env.Payload["taskId"] != "task-1" || env.Payload["expiresAt"] != "2026-09-13T00:00:30Z" {
		t.Errorf("unexpected payload: %#v", env.Payload)
	}
}

func TestDecodePayload_ReservationRejected(t *testing.T) {
	// Matches events.Publisher.ReservationRejected's field set exactly.
	data := encodeLikeTaskRouter(t, map[string]any{
		"reservationId": "res-2",
		"taskId":        "task-2",
		"agentId":       "agent-2",
		"reason":        "agent_rejected",
	})

	fields, err := decodePayload(data)
	if err != nil {
		t.Fatalf("decodePayload: %v", err)
	}
	env, err := buildEnvelope(EventTypeReservationRejected, fields)
	if err != nil {
		t.Fatalf("buildEnvelope: %v", err)
	}
	if env.AgentID != "agent-2" {
		t.Errorf("AgentID = %q, want %q", env.AgentID, "agent-2")
	}
	if env.Payload["reason"] != "agent_rejected" {
		t.Errorf("unexpected reason: %#v", env.Payload["reason"])
	}
}

func TestDecodePayload_AgentStatusChanged(t *testing.T) {
	// Matches events.Publisher.AgentStatusChanged's field set exactly.
	data := encodeLikeTaskRouter(t, map[string]any{
		"agentId": "agent-3",
		"status":  "Available",
	})

	fields, err := decodePayload(data)
	if err != nil {
		t.Fatalf("decodePayload: %v", err)
	}
	env, err := buildEnvelope(EventTypeAgentStatusChanged, fields)
	if err != nil {
		t.Fatalf("buildEnvelope: %v", err)
	}
	if env.AgentID != "agent-3" {
		t.Errorf("AgentID = %q, want %q", env.AgentID, "agent-3")
	}
	if env.Payload["status"] != "Available" {
		t.Errorf("unexpected status: %#v", env.Payload["status"])
	}
}

func TestDecodePayload_AgentDeleted(t *testing.T) {
	// Matches events.Publisher.AgentDeleted's field set exactly.
	data := encodeLikeTaskRouter(t, map[string]any{
		"agentId": "agent-4",
	})

	fields, err := decodePayload(data)
	if err != nil {
		t.Fatalf("decodePayload: %v", err)
	}
	env, err := buildEnvelope(EventTypeAgentDeleted, fields)
	if err != nil {
		t.Fatalf("buildEnvelope: %v", err)
	}
	if env.AgentID != "agent-4" {
		t.Errorf("AgentID = %q, want %q", env.AgentID, "agent-4")
	}
}

func TestBuildEnvelope_MissingAgentID(t *testing.T) {
	_, err := buildEnvelope(EventTypeAgentDeleted, map[string]any{"somethingElse": "x"})
	if err == nil {
		t.Fatalf("expected error when agentId is missing from payload")
	}
}

func TestEnvelopeMarshalJSON(t *testing.T) {
	env := Envelope{
		Type:    EventTypeAgentDeleted,
		AgentID: "agent-5",
		Payload: map[string]any{"agentId": "agent-5"},
	}
	data, err := env.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	want := `{"type":"agent.deleted","agentId":"agent-5","payload":{"agentId":"agent-5"}}`
	if string(data) != want {
		t.Errorf("MarshalJSON = %s, want %s", data, want)
	}
}
