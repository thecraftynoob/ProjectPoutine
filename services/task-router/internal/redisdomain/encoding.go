package redisdomain

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// Redis hash field layout for an agent hash (agentKey):
//
//	status            -> string
//	statusChangedAt   -> RFC3339Nano string
//	attributes        -> JSON object {name: {"n":number}|{"b":bool}}
//	queues            -> JSON array of strings
//	cap:<channel>     -> JSON ChannelCapacity, one field per channel so a
//	                     Lua script can HGET/HSET a single channel's
//	                     capacity atomically without deserializing every
//	                     other channel (spec Section 5.4 rule 2).
//
// The "cap:" fields are discovered via HGETALL + prefix filtering when
// reading a full Agent back into Go, since Redis hashes have no native
// sub-namespacing.

// attrJSON is the wire shape used for one AttributeValue inside the
// "attributes"/"requiredAttributes" JSON blob.
type attrJSON struct {
	N *float64 `json:"n,omitempty"`
	B *bool    `json:"b,omitempty"`
}

// EncodeAttributes serializes an attribute map to the JSON form stored in
// a single Redis hash field.
func EncodeAttributes(attrs map[string]AttributeValue) (string, error) {
	if len(attrs) == 0 {
		return "{}", nil
	}
	wire := make(map[string]attrJSON, len(attrs))
	for k, v := range attrs {
		if v.IsBool {
			b := v.BoolValue
			wire[k] = attrJSON{B: &b}
		} else {
			n := v.NumberValue
			wire[k] = attrJSON{N: &n}
		}
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return "", fmt.Errorf("redisdomain: encode attributes: %w", err)
	}
	return string(data), nil
}

// DecodeAttributes parses the JSON form back into an attribute map.
func DecodeAttributes(raw string) (map[string]AttributeValue, error) {
	if raw == "" || raw == "{}" {
		return map[string]AttributeValue{}, nil
	}
	var wire map[string]attrJSON
	if err := json.Unmarshal([]byte(raw), &wire); err != nil {
		return nil, fmt.Errorf("redisdomain: decode attributes: %w", err)
	}
	out := make(map[string]AttributeValue, len(wire))
	for k, v := range wire {
		if v.B != nil {
			out[k] = AttributeValue{IsBool: true, BoolValue: *v.B}
		} else if v.N != nil {
			out[k] = AttributeValue{IsBool: false, NumberValue: *v.N}
		}
	}
	return out, nil
}

// EncodeQueues serializes a queue-membership list to JSON.
func EncodeQueues(queues []string) (string, error) {
	if queues == nil {
		queues = []string{}
	}
	sorted := append([]string(nil), queues...)
	sort.Strings(sorted)
	data, err := json.Marshal(sorted)
	if err != nil {
		return "", fmt.Errorf("redisdomain: encode queues: %w", err)
	}
	return string(data), nil
}

// DecodeQueues parses a queue-membership JSON array.
func DecodeQueues(raw string) ([]string, error) {
	if raw == "" {
		return []string{}, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("redisdomain: decode queues: %w", err)
	}
	return out, nil
}

// EncodeCapacity serializes one ChannelCapacity to the JSON form stored in
// its own "cap:<channel>" hash field.
func EncodeCapacity(c ChannelCapacity) (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("redisdomain: encode capacity: %w", err)
	}
	return string(data), nil
}

// DecodeCapacity parses one "cap:<channel>" hash field value.
func DecodeCapacity(raw string) (ChannelCapacity, error) {
	var c ChannelCapacity
	if raw == "" {
		return c, nil
	}
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return c, fmt.Errorf("redisdomain: decode capacity: %w", err)
	}
	return c, nil
}

const capFieldPrefix = "cap:"

// capField returns the Redis hash field name for one channel's capacity.
func capField(channel string) string {
	return capFieldPrefix + channel
}

// channelFromCapField returns the channel name from a "cap:<channel>"
// field name, or ok=false if field doesn't have that shape.
func channelFromCapField(field string) (string, bool) {
	if len(field) <= len(capFieldPrefix) || field[:len(capFieldPrefix)] != capFieldPrefix {
		return "", false
	}
	return field[len(capFieldPrefix):], true
}

// formatTime renders a time.Time the way it's stored in Redis hash
// fields and Lua script args: RFC3339Nano for stable, sortable,
// human-readable timestamps.
func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// parseTime is the inverse of formatTime. Returns the zero time if raw is
// empty.
func parseTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, raw)
}

// unixMicros renders a timestamp as microseconds-since-epoch, used as the
// ZSET score for strict-FIFO pending-task ordering (spec Section 4.1) — a
// numeric score sorts correctly and cheaply, unlike a string timestamp.
func unixMicros(t time.Time) float64 {
	return float64(t.UnixMicro())
}

// parseUnixMicros is the inverse of unixMicros.
func parseUnixMicros(raw string) (time.Time, error) {
	micros, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("redisdomain: parse unix micros %q: %w", raw, err)
	}
	return time.UnixMicro(micros).UTC(), nil
}
