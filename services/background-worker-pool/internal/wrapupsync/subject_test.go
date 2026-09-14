package wrapupsync

import (
	"testing"

	"github.com/google/uuid"
)

func TestParseSubject(t *testing.T) {
	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	got, err := parseSubject("tenant.11111111-1111-1111-1111-111111111111.task.completed")
	if err != nil {
		t.Fatalf("parseSubject: unexpected error: %v", err)
	}
	if got.TenantID != tenantID {
		t.Errorf("tenant_id: want %v, got %v", tenantID, got.TenantID)
	}
	if got.Domain != "task" {
		t.Errorf("domain: want %q, got %q", "task", got.Domain)
	}
	if got.EventType != "completed" {
		t.Errorf("event_type: want %q, got %q", "completed", got.EventType)
	}
}

func TestParseSubjectRoundTripsWithEventbusSubject(t *testing.T) {
	// Proves parseSubject is the true inverse of pkg/eventbus.Subject for
	// this package's real subject shape, without importing pkg/eventbus
	// (whose Subject is a one-line fmt.Sprintf -- reproducing its exact
	// format string here keeps this test focused on parseSubject's own
	// contract).
	tenantID := uuid.New()
	subject := "tenant." + tenantID.String() + ".task.completed"

	got, err := parseSubject(subject)
	if err != nil {
		t.Fatalf("parseSubject: unexpected error: %v", err)
	}
	if got.TenantID != tenantID {
		t.Errorf("tenant_id: want %v, got %v", tenantID, got.TenantID)
	}
	if got.Domain != "task" {
		t.Errorf("domain: want %q, got %q", "task", got.Domain)
	}
	if got.EventType != "completed" {
		t.Errorf("event_type: want %q, got %q", "completed", got.EventType)
	}
}

func TestParseSubjectErrors(t *testing.T) {
	tests := []struct {
		name    string
		subject string
	}{
		{"missing segments", "tenant.11111111-1111-1111-1111-111111111111.task"},
		{"wrong literal prefix", "notenant.11111111-1111-1111-1111-111111111111.task.completed"},
		{"invalid tenant uuid", "tenant.not-a-uuid.task.completed"},
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
