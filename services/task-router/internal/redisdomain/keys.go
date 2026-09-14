// Package redisdomain implements the Redis-backed hot-path domain for
// Task Router (Agent, Task, Reservation) per
// TASK_ROUTER_SPECIFICATION.md Sections 2.1-2.3, 4, 5, and the
// architectural decision recorded in the service README: Redis hashes +
// Sets/ZSETs, one Lua script (EVAL) per state-changing mutation, with
// Redis itself as the system of record for this live routing state
// (architecture doc Section 3.1, Tier 1).
//
// Every key is namespaced by tenant ID so a single Redis instance can
// safely serve multiple tenants with simple key-prefix isolation (Redis
// itself has no native RLS-equivalent, so this is the enforcement
// mechanism at this tier — every key-building function in this file
// requires a tenant ID).
package redisdomain

import "fmt"

const keyPrefix = "tr"

// agentKey returns the hash key for one agent's full record.
func agentKey(tenantID, agentID string) string {
	return fmt.Sprintf("%s:%s:agent:%s", keyPrefix, tenantID, agentID)
}

// agentsSetKey returns the set of all agent IDs for a tenant.
func agentsSetKey(tenantID string) string {
	return fmt.Sprintf("%s:%s:agents", keyPrefix, tenantID)
}

// agentOffersKey returns the set of reservation IDs currently in flight
// (Offered OR Accepted, i.e. not yet terminally resolved) for one agent.
// An agent may legitimately hold several of these concurrently under
// ChannelCapacity.max > 1 (spec Sections 2.1, 3.2, 4.2). Used to answer
// "list an agent's pending offers" (spec Section 3.2, filtered to
// Offered-only by the Go caller -- see ListAgentPendingOffers) without a
// full scan, and by delete_agent.lua to resolve every in-flight
// reservation an agent holds when it is deleted (spec Section 5.5).
// Entries are added at Offered-creation time (match_commit.lua) and
// removed only on terminal resolution: complete_task.lua (Accepted ->
// Completed), reject_reservation.lua (Offered -> Rejected), or
// delete_agent.lua (agent-deletion-triggered Rejected). Deliberately NOT
// removed on Offered -> Accepted (accept_reservation.lua) since that is
// not a terminal resolution.
func agentOffersKey(tenantID, agentID string) string {
	return fmt.Sprintf("%s:%s:agent:%s:offers", keyPrefix, tenantID, agentID)
}

// taskKey returns the hash key for one task's full record.
func taskKey(tenantID, taskID string) string {
	return fmt.Sprintf("%s:%s:task:%s", keyPrefix, tenantID, taskID)
}

// tasksSetKey returns the set of all task IDs for a tenant.
func tasksSetKey(tenantID string) string {
	return fmt.Sprintf("%s:%s:tasks", keyPrefix, tenantID)
}

// pendingTasksKey returns the ZSET of Pending task IDs, scored by
// enqueuedAt (unix micros), giving strict FIFO ordering per spec Section
// 4.1.
func pendingTasksKey(tenantID string) string {
	return fmt.Sprintf("%s:%s:tasks:pending", keyPrefix, tenantID)
}

// reservationKey returns the hash key for one reservation's full record.
func reservationKey(tenantID, reservationID string) string {
	return fmt.Sprintf("%s:%s:reservation:%s", keyPrefix, tenantID, reservationID)
}

// reservationExpiryKey returns the TTL-bearing sentinel key whose expiry,
// combined with Redis keyspace notifications, drives reservation expiry
// per spec Section 4.3. The key's value is irrelevant; only its existence
// and TTL matter. Deleted (not just left to expire) when the reservation
// resolves any other way, so a stale expiry notification can never fire
// for an already-resolved reservation (the Lua script's atomic
// re-validation is still the real safety net — see
// scripts/reject_reservation.lua (invoked with reason "expired") — but
// proactive deletion keeps the keyspace clean and avoids a needless
// wakeup).
func reservationExpiryKey(tenantID, reservationID string) string {
	return fmt.Sprintf("%s:%s:resexp:%s", keyPrefix, tenantID, reservationID)
}

// wrapUpExpiryKey returns the TTL-bearing sentinel key whose expiry,
// combined with Redis keyspace notifications, drives the wrap-up timer
// (mirrors reservationExpiryKey's mechanism exactly, per the Wrap Up /
// Disposition two-step completion lifecycle). The key's value is
// irrelevant; only its existence and TTL matter. Deleted (not just left to
// expire) when the task leaves WrapUp any other way (CompleteTask), so a
// stale expiry notification can never fire for a task no longer in
// WrapUp -- the sweep handler's own re-check of the task's live status is
// still the real safety net, but proactive deletion keeps the keyspace
// clean and avoids a needless wakeup.
func wrapUpExpiryKey(tenantID, taskID string) string {
	return fmt.Sprintf("%s:%s:wrapupexp:%s", keyPrefix, tenantID, taskID)
}

// seqKey returns the hash key holding this tenant's monotonically
// increasing ID counters (one field per entity kind: "task",
// "reservation"), used for server-generated sequential IDs.
func seqKey(tenantID string) string {
	return fmt.Sprintf("%s:%s:seq", keyPrefix, tenantID)
}

// KeyspaceExpiryPattern returns the keyspace-notification pattern this
// service subscribes to for reservation expiry, scoped to database 0's
// default keyspace-event channel naming
// ("__keyevent@<db>__:expired"). The tenant/reservation ID is parsed back
// out of the expired key's own name by the subscriber, since Redis
// keyspace notifications deliver the key name as the message payload.
func KeyspaceExpiryPattern(db int) string {
	return fmt.Sprintf("__keyevent@%d__:expired", db)
}

// ReservationIDFromExpiryKey extracts (tenantID, reservationID) from an
// expired key name previously produced by reservationExpiryKey, or ok=false
// if the key doesn't match the expected "tr:{tenant}:resexp:{id}" shape
// (e.g. an unrelated key in the same Redis instance expiring).
func ReservationIDFromExpiryKey(key string) (tenantID, reservationID string, ok bool) {
	const prefix = keyPrefix + ":"
	const mid = ":resexp:"
	if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
		return "", "", false
	}
	rest := key[len(prefix):]
	idx := indexOf(rest, mid)
	if idx < 0 {
		return "", "", false
	}
	tenantID = rest[:idx]
	reservationID = rest[idx+len(mid):]
	if tenantID == "" || reservationID == "" {
		return "", "", false
	}
	return tenantID, reservationID, true
}

// TaskIDFromWrapUpExpiryKey extracts (tenantID, taskID) from an expired
// key name previously produced by wrapUpExpiryKey, or ok=false if the key
// doesn't match the expected "tr:{tenant}:wrapupexp:{id}" shape (e.g. an
// unrelated key, such as a reservation-expiry key, expiring in the same
// Redis instance).
func TaskIDFromWrapUpExpiryKey(key string) (tenantID, taskID string, ok bool) {
	const prefix = keyPrefix + ":"
	const mid = ":wrapupexp:"
	if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
		return "", "", false
	}
	rest := key[len(prefix):]
	idx := indexOf(rest, mid)
	if idx < 0 {
		return "", "", false
	}
	tenantID = rest[:idx]
	taskID = rest[idx+len(mid):]
	if tenantID == "" || taskID == "" {
		return "", "", false
	}
	return tenantID, taskID, true
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
