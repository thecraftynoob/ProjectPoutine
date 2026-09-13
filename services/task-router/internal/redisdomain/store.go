package redisdomain

import (
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Store is the Redis-backed hot-path repository for Agent, Task, and
// Reservation state. Every method that mutates state runs exactly one
// embedded Lua script (see scripts/*.lua) so the operation is atomic
// against concurrent mutation, per spec Section 5.4.
type Store struct {
	client *redis.Client
	// ttl is the reservation TTL applied at match-commit time (spec
	// Section 2.3's "configurable duration, default 30 seconds").
	ttl time.Duration
}

// NewStore constructs a Store. ttl <= 0 falls back to
// DefaultReservationTTL.
func NewStore(client *redis.Client, ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = DefaultReservationTTL
	}
	return &Store{client: client, ttl: ttl}
}

// Client exposes the underlying redis client, for the keyspace-
// notification subscriber and health checks.
func (s *Store) Client() *redis.Client {
	return s.client
}

// ErrNotFound is returned by single-entity getters when the entity does
// not exist.
var ErrNotFound = fmt.Errorf("redisdomain: not found")

// ErrAlreadyExists is returned by create operations on an ID collision.
var ErrAlreadyExists = fmt.Errorf("redisdomain: already exists")

// scriptResult is a small helper for decoding the {code, ...} array shape
// most scripts in this package return.
func asSlice(res any) ([]any, error) {
	slice, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("redisdomain: unexpected script result type %T", res)
	}
	return slice, nil
}

func asInt64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	default:
		return 0
	}
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return ""
	}
}
