package pgconfig

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/redisdomain"
)

// AttributeValidationError describes the shape-validation rule violated
// (spec Section 4.4). Wraps enough detail for a clear gRPC
// InvalidArgument message while keeping the whole write rejected with no
// partial application (the caller of ValidateAttributes never applies
// any part of the map if this returns an error).
type AttributeValidationError struct {
	Key    string
	Reason string
}

func (e *AttributeValidationError) Error() string {
	return fmt.Sprintf("pgconfig: attribute %q: %s", e.Key, e.Reason)
}

// ValidateAttributes implements spec Section 4.4 exactly: every key must
// already exist in the Attribute registry, and its value must match the
// registered type exactly -- a registered boolean attribute must receive
// a literal boolean; a registered numeric attribute must receive a
// non-boolean numeric value (deliberately excluding booleans from
// "numeric" even though some type systems treat them as a numeric
// subtype, per the spec's explicit callout). Any violation rejects the
// entire write with no partial application -- this function either
// returns nil (the whole map is valid) or a single
// *AttributeValidationError describing the first violation found.
//
// An empty map is always valid regardless of what's registered (spec
// Section 2.6).
func (r *Registry) ValidateAttributes(ctx context.Context, tenantID uuid.UUID, attrs map[string]redisdomain.AttributeValue) error {
	if len(attrs) == 0 {
		return nil
	}

	names := make([]string, 0, len(attrs))
	for k := range attrs {
		names = append(names, k)
	}
	sort.Strings(names) // deterministic "first violation" ordering

	registered, err := r.AttributeTypes(ctx, tenantID, names)
	if err != nil {
		return fmt.Errorf("pgconfig: validate attributes: %w", err)
	}

	return validateAgainstRegistry(names, attrs, registered)
}

// validateAgainstRegistry is the pure comparison core of ValidateAttributes,
// factored out so it's directly unit-testable without a live Postgres
// connection: given the already-sorted key list, the caller's supplied
// values, and the registry's known types for those keys, apply spec
// Section 4.4's exact rule and return the first violation found (or nil).
func validateAgainstRegistry(names []string, attrs map[string]redisdomain.AttributeValue, registered map[string]AttributeType) error {
	for _, name := range names {
		attrType, ok := registered[name]
		if !ok {
			return &AttributeValidationError{Key: name, Reason: "not registered in the attribute registry"}
		}
		v := attrs[name]
		switch attrType {
		case AttributeBoolean:
			if !v.IsBool {
				return &AttributeValidationError{Key: name, Reason: "registered as boolean but a numeric value was supplied"}
			}
		case AttributeNumeric:
			if v.IsBool {
				return &AttributeValidationError{Key: name, Reason: "registered as numeric but a boolean value was supplied (booleans are deliberately excluded from numeric)"}
			}
		default:
			return &AttributeValidationError{Key: name, Reason: fmt.Sprintf("registered with unrecognized type %q", attrType)}
		}
	}

	return nil
}
