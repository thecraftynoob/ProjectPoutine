package wrapupsync

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
// This package only ever subscribes to "tenant.*.task.completed" (see
// consumer.go's subjectFilter), and "completed" happens to be a
// single-word event_type with no embedded dots -- unlike, say,
// "agent.capacity.config.updated" (see
// services/historical-reporting/internal/eventconsumer/subject.go's
// parseSubject, which has to handle a variable, multi-dot event_type for
// its full-catalog subscription). That doesn't make a naive fixed-index
// strings.Split safe to write here, though: this function still parses
// defensively (SplitN, explicit segment-count/prefix/UUID validation)
// rather than assuming task.completed's shape will never change or that
// a malformed/unexpected subject can't reach this handler.
func parseSubject(subject string) (ParsedSubject, error) {
	// SplitN with N=4 mirrors historical-reporting's eventconsumer
	// convention exactly, keeping event_type intact even though this
	// package's own filter never delivers a multi-dot one today.
	parts := strings.SplitN(subject, ".", 4)
	if len(parts) != 4 {
		return ParsedSubject{}, fmt.Errorf("wrapupsync: subject %q does not have the form tenant.{tenant_id}.{domain}.{event_type}", subject)
	}
	if parts[0] != "tenant" {
		return ParsedSubject{}, fmt.Errorf("wrapupsync: subject %q does not start with the literal %q", subject, "tenant")
	}

	tenantID, err := uuid.Parse(parts[1])
	if err != nil {
		return ParsedSubject{}, fmt.Errorf("wrapupsync: subject %q has an invalid tenant_id segment %q: %w", subject, parts[1], err)
	}

	domain := parts[2]
	if domain == "" {
		return ParsedSubject{}, fmt.Errorf("wrapupsync: subject %q has an empty domain segment", subject)
	}

	eventType := parts[3]
	if eventType == "" {
		return ParsedSubject{}, fmt.Errorf("wrapupsync: subject %q has an empty event_type segment", subject)
	}

	return ParsedSubject{TenantID: tenantID, Domain: domain, EventType: eventType}, nil
}
