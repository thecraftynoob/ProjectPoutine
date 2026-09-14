package eventconsumer

import (
	"testing"

	"github.com/google/uuid"
)

func TestParseSubject(t *testing.T) {
	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	tests := []struct {
		name          string
		subject       string
		wantTenantID  uuid.UUID
		wantDomain    string
		wantEventType string
	}{
		{
			name:          "single-dot event_type (task.enqueued)",
			subject:       "tenant.11111111-1111-1111-1111-111111111111.task.enqueued",
			wantTenantID:  tenantID,
			wantDomain:    "task",
			wantEventType: "enqueued",
		},
		{
			name:          "two-dot event_type (agent.status.changed)",
			subject:       "tenant.11111111-1111-1111-1111-111111111111.agent.status.changed",
			wantTenantID:  tenantID,
			wantDomain:    "agent",
			wantEventType: "status.changed",
		},
		{
			// The tricky case this test exists to prove: a naive
			// strings.Split on "." and take-a-fixed-index approach would
			// mis-split this, since event_type itself has two dots.
			name:          "three-dot event_type (agent.capacity.config.updated)",
			subject:       "tenant.11111111-1111-1111-1111-111111111111.agent.capacity.config.updated",
			wantTenantID:  tenantID,
			wantDomain:    "agent",
			wantEventType: "capacity.config.updated",
		},
		{
			name:          "reservation domain, single-dot event_type",
			subject:       "tenant.11111111-1111-1111-1111-111111111111.reservation.rejected",
			wantTenantID:  tenantID,
			wantDomain:    "reservation",
			wantEventType: "rejected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSubject(tt.subject)
			if err != nil {
				t.Fatalf("parseSubject(%q): unexpected error: %v", tt.subject, err)
			}
			if got.TenantID != tt.wantTenantID {
				t.Errorf("tenant_id: want %v, got %v", tt.wantTenantID, got.TenantID)
			}
			if got.Domain != tt.wantDomain {
				t.Errorf("domain: want %q, got %q", tt.wantDomain, got.Domain)
			}
			if got.EventType != tt.wantEventType {
				t.Errorf("event_type: want %q, got %q", tt.wantEventType, got.EventType)
			}
		})
	}
}

func TestParseSubjectRoundTripsWithEventbusSubject(t *testing.T) {
	// Proves parseSubject is the true inverse of pkg/eventbus.Subject for
	// a multi-dot event_type, without importing pkg/eventbus (that
	// package's Subject is a one-line fmt.Sprintf; reproducing its exact
	// format string here keeps this test focused on parseSubject's
	// contract, not a redundant re-test of eventbus itself).
	tenantID := uuid.New()
	domain := "agent"
	eventType := "capacity.config.updated"
	subject := "tenant." + tenantID.String() + "." + domain + "." + eventType

	got, err := parseSubject(subject)
	if err != nil {
		t.Fatalf("parseSubject: unexpected error: %v", err)
	}
	if got.TenantID != tenantID {
		t.Errorf("tenant_id: want %v, got %v", tenantID, got.TenantID)
	}
	if got.Domain != domain {
		t.Errorf("domain: want %q, got %q", domain, got.Domain)
	}
	if got.EventType != eventType {
		t.Errorf("event_type: want %q, got %q", eventType, got.EventType)
	}
}

func TestParseSubjectErrors(t *testing.T) {
	tests := []struct {
		name    string
		subject string
	}{
		{"missing segments", "tenant.11111111-1111-1111-1111-111111111111.task"},
		{"wrong literal prefix", "notenant.11111111-1111-1111-1111-111111111111.task.enqueued"},
		{"invalid tenant uuid", "tenant.not-a-uuid.task.enqueued"},
		{"empty subject", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseSubject(tt.subject); err == nil {
				t.Errorf("parseSubject(%q): expected an error, got nil", tt.subject)
			}
		})
	}
}
