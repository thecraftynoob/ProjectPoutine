package pgconfig

import (
	"testing"

	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/redisdomain"
)

// TestValidateAgainstRegistry covers spec Section 4.4's attribute
// shape-validation rule exactly: every key must be registered, boolean
// attributes must receive a literal boolean, numeric attributes must
// receive a non-boolean numeric value (booleans deliberately excluded
// from "numeric").
func TestValidateAgainstRegistry(t *testing.T) {
	registry := map[string]AttributeType{
		"is_vip":     AttributeBoolean,
		"skill_level": AttributeNumeric,
	}

	tests := []struct {
		name    string
		attrs   map[string]redisdomain.AttributeValue
		wantErr bool
		wantKey string
	}{
		{
			name:    "empty map always valid",
			attrs:   map[string]redisdomain.AttributeValue{},
			wantErr: false,
		},
		{
			name: "valid boolean and numeric",
			attrs: map[string]redisdomain.AttributeValue{
				"is_vip":      {IsBool: true, BoolValue: true},
				"skill_level": {IsBool: false, NumberValue: 42},
			},
			wantErr: false,
		},
		{
			name: "unregistered key rejected",
			attrs: map[string]redisdomain.AttributeValue{
				"unknown_attr": {IsBool: true, BoolValue: true},
			},
			wantErr: true,
			wantKey: "unknown_attr",
		},
		{
			name: "boolean attribute given a numeric value rejected",
			attrs: map[string]redisdomain.AttributeValue{
				"is_vip": {IsBool: false, NumberValue: 1},
			},
			wantErr: true,
			wantKey: "is_vip",
		},
		{
			name: "numeric attribute given a boolean value rejected (deliberate exclusion)",
			attrs: map[string]redisdomain.AttributeValue{
				"skill_level": {IsBool: true, BoolValue: true},
			},
			wantErr: true,
			wantKey: "skill_level",
		},
		{
			name: "partial validity still rejects the whole write (no partial application)",
			attrs: map[string]redisdomain.AttributeValue{
				"is_vip":      {IsBool: true, BoolValue: true},
				"skill_level": {IsBool: true, BoolValue: false}, // invalid
			},
			wantErr: true,
			wantKey: "skill_level",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			names := make([]string, 0, len(tt.attrs))
			for k := range tt.attrs {
				names = append(names, k)
			}
			err := validateAgainstRegistry(names, tt.attrs, registry)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got nil")
				}
				verr, ok := err.(*AttributeValidationError)
				if !ok {
					t.Fatalf("expected *AttributeValidationError, got %T: %v", err, err)
				}
				if verr.Key != tt.wantKey {
					t.Fatalf("expected violation on key %q, got %q", tt.wantKey, verr.Key)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
