package eventbus

import (
	"testing"

	"github.com/google/uuid"
)

// Subject() is a pure function, so it's unit-tested here without a live
// NATS server. Integration tests that exercise Connect/PublishEvent/
// Subscribe/SubscribeEphemeral/EnsureStream against a real NATS+JetStream
// instance live in eventbus_integration_test.go (an in-process
// nats-server, no docker-compose/CI wiring needed).
func TestSubject(t *testing.T) {
	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	got := Subject(tenantID, "voice", "call.ended")
	want := "tenant.11111111-1111-1111-1111-111111111111.voice.call.ended"

	if got != want {
		t.Fatalf("Subject() = %q, want %q", got, want)
	}
}

func TestSubject_DifferentDomainsAndEvents(t *testing.T) {
	tenantID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	cases := []struct {
		domain    string
		eventType string
		want      string
	}{
		{"agent", "status.changed", "tenant.22222222-2222-2222-2222-222222222222.agent.status.changed"},
		{"task", "created", "tenant.22222222-2222-2222-2222-222222222222.task.created"},
	}

	for _, tc := range cases {
		got := Subject(tenantID, tc.domain, tc.eventType)
		if got != tc.want {
			t.Errorf("Subject(%q, %q) = %q, want %q", tc.domain, tc.eventType, got, tc.want)
		}
	}
}
