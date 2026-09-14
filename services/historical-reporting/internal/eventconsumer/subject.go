package eventconsumer

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// ParsedSubject is the result of splitting a NATS subject built by
// pkg/eventbus.Subject (tenant.{tenant_id}.{domain}.{event_type}) back
// into its parts.
type ParsedSubject struct {
	TenantID  uuid.UUID
	Domain    string
	EventType string
}

// parseSubject inverts pkg/eventbus.Subject's
// "tenant.{tenant_id}.{domain}.{event_type}" format.
//
// This is NOT a naive strings.Split(s, ".") on a fixed index: event_type
// itself can contain dots (e.g. "status.changed",
// "capacity.config.updated" -- see
// services/task-router/internal/events/events.go's EventAgentStatusChanged
// / EventAgentCapacityConfigUpdated constants), so the subject has a
// variable, not fixed, number of segments. Only the first two dots are
// structural (separating the literal "tenant" literal, the tenant_id, and
// the domain); everything after the third segment -- however many
// remaining dot-separated pieces it has -- is the event_type verbatim.
func parseSubject(subject string) (ParsedSubject, error) {
	// SplitN with N=4 caps the split so a multi-dot event_type is never
	// torn apart: "tenant", "{tenant_id}", "{domain}", "{event_type...}"
	// (the 4th piece keeps every remaining dot intact).
	parts := strings.SplitN(subject, ".", 4)
	if len(parts) != 4 {
		return ParsedSubject{}, fmt.Errorf("eventconsumer: subject %q does not have the form tenant.{tenant_id}.{domain}.{event_type}", subject)
	}
	if parts[0] != "tenant" {
		return ParsedSubject{}, fmt.Errorf("eventconsumer: subject %q does not start with the literal %q", subject, "tenant")
	}

	tenantID, err := uuid.Parse(parts[1])
	if err != nil {
		return ParsedSubject{}, fmt.Errorf("eventconsumer: subject %q has an invalid tenant_id segment %q: %w", subject, parts[1], err)
	}

	domain := parts[2]
	if domain == "" {
		return ParsedSubject{}, fmt.Errorf("eventconsumer: subject %q has an empty domain segment", subject)
	}

	eventType := parts[3]
	if eventType == "" {
		return ParsedSubject{}, fmt.Errorf("eventconsumer: subject %q has an empty event_type segment", subject)
	}

	return ParsedSubject{TenantID: tenantID, Domain: domain, EventType: eventType}, nil
}
