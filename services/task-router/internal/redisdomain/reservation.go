package redisdomain

import (
	"context"
	"fmt"
	"time"
)

// GetReservation reads one reservation's full record. Returns
// ErrNotFound if absent.
func (s *Store) GetReservation(ctx context.Context, tenantID, reservationID string) (Reservation, error) {
	fields, err := s.client.HGetAll(ctx, reservationKey(tenantID, reservationID)).Result()
	if err != nil {
		return Reservation{}, fmt.Errorf("redisdomain: get reservation: %w", err)
	}
	if len(fields) == 0 {
		return Reservation{}, ErrNotFound
	}
	return decodeReservation(reservationID, fields)
}

func decodeReservation(reservationID string, fields map[string]string) (Reservation, error) {
	createdAt, err := parseTime(fields["createdAt"])
	if err != nil {
		return Reservation{}, err
	}
	var expiresAt *time.Time
	if raw := fields["expiresAt"]; raw != "" {
		t, err := parseTime(raw)
		if err != nil {
			return Reservation{}, err
		}
		expiresAt = &t
	}
	return Reservation{
		ReservationID: reservationID,
		TaskID:        fields["taskId"],
		AgentID:       fields["agentId"],
		Status:        ReservationStatus(fields["status"]),
		CreatedAt:     createdAt,
		ExpiresAt:     expiresAt,
		Reason:        RejectReason(fields["reason"]),
	}, nil
}

// MatchAttemptResult reports the outcome of one MatchCommit call.
type MatchAttemptResult struct {
	Committed     bool
	Reservation   Reservation
	RefusedReason string // only set when Committed == false
}

// MatchCommit attempts to atomically commit a match between one agent and
// one task, re-validating all eligibility conditions at commit time (spec
// Section 4.1's atomically_commit_match, Section 5.4 rule 3). The caller
// supplies a pre-generated reservationID (via NewReservationID) so ID
// generation doesn't happen inside the Lua script.
func (s *Store) MatchCommit(ctx context.Context, tenantID, taskID, agentID, reservationID string) (MatchAttemptResult, error) {
	now := time.Now().UTC()
	expiresAt := now.Add(s.ttl)

	res, err := runScript(ctx, s.client, "match_commit",
		[]string{
			agentKey(tenantID, agentID),
			taskKey(tenantID, taskID),
			pendingTasksKey(tenantID),
			reservationKey(tenantID, reservationID),
			reservationsSetKey(tenantID),
			agentOffersKey(tenantID, agentID),
			reservationExpiryKey(tenantID, reservationID),
		},
		taskID, agentID, reservationID,
		formatTime(now), formatTime(expiresAt), int64(s.ttl/time.Second),
	)
	if err != nil {
		return MatchAttemptResult{}, err
	}
	slice, err := asSlice(res)
	if err != nil {
		return MatchAttemptResult{}, err
	}
	if asInt64(slice[0]) == 0 {
		return MatchAttemptResult{Committed: false, RefusedReason: asString(slice[1])}, nil
	}
	return MatchAttemptResult{
		Committed: true,
		Reservation: Reservation{
			ReservationID: reservationID,
			TaskID:        taskID,
			AgentID:       agentID,
			Status:        ReservationOffered,
			CreatedAt:     now,
			ExpiresAt:     &expiresAt,
		},
	}, nil
}

// reservationsSetKey returns the set of all reservation IDs for a tenant
// (bookkeeping only; not iterated by the hot path).
func reservationsSetKey(tenantID string) string {
	return fmt.Sprintf("%s:%s:reservations", keyPrefix, tenantID)
}

// NewReservationID generates a sequential reservation ID.
func (s *Store) NewReservationID(ctx context.Context, tenantID string) (string, error) {
	return s.nextID(ctx, tenantID, "reservation")
}

// AcceptResult reports the outcome of AcceptReservation.
type AcceptResult struct {
	// Resolved is false if the reservation was already resolved by
	// something else (spec Section 5.4 rule 4) -- CurrentStatus then
	// holds what it actually is now.
	Resolved      bool
	CurrentStatus ReservationStatus
	TaskID        string
	AgentID       string
}

// AcceptReservation atomically accepts an Offered reservation, per spec
// Section 5.4 rule 4's idempotent-safe terminal resolution. Returns
// ErrNotFound if the reservation does not exist at all.
func (s *Store) AcceptReservation(ctx context.Context, tenantID, reservationID string) (AcceptResult, error) {
	// AgentID isn't known until the script reads it, but we need the
	// agent hash key up front for the KEYS array. Read the reservation
	// first (a plain read, not a mutation) to get agentId; the script
	// itself re-validates status atomically regardless of what we read
	// here, so this preliminary read cannot introduce a race in the
	// mutation itself -- worst case the agentId read here is stale and
	// the agent hash key passed to the script no longer matches
	// anything live, which the script tolerates (EXISTS-gated).
	existing, err := s.GetReservation(ctx, tenantID, reservationID)
	if err != nil {
		if err == ErrNotFound {
			return AcceptResult{Resolved: false, CurrentStatus: ""}, nil
		}
		return AcceptResult{}, err
	}

	res, err := runScript(ctx, s.client, "accept_reservation",
		[]string{
			reservationKey(tenantID, reservationID),
			taskKey(tenantID, existing.TaskID),
			agentOffersKey(tenantID, existing.AgentID),
			reservationExpiryKey(tenantID, reservationID),
		},
		reservationID,
	)
	if err != nil {
		return AcceptResult{}, err
	}
	slice, err := asSlice(res)
	if err != nil {
		return AcceptResult{}, err
	}
	if asInt64(slice[0]) == 0 {
		return AcceptResult{Resolved: false, CurrentStatus: ReservationStatus(asString(slice[1]))}, nil
	}
	return AcceptResult{
		Resolved: true,
		TaskID:   asString(slice[1]),
		AgentID:  asString(slice[2]),
	}, nil
}

// RejectResult reports the outcome of RejectReservation.
type RejectResult struct {
	Resolved      bool
	CurrentStatus ReservationStatus
	TaskID        string
	AgentID       string
}

// RejectReservation atomically resolves an Offered reservation to
// Rejected, tagged with reason, per spec Section 5.4 rule 4. Used
// identically by the manual-reject capability (reason=agent_rejected)
// and the automatic expiry sweep (reason=expired) -- spec Section 4.3.
// Returns ErrNotFound if the reservation does not exist at all.
func (s *Store) RejectReservation(ctx context.Context, tenantID, reservationID string, reason RejectReason) (RejectResult, error) {
	existing, err := s.GetReservation(ctx, tenantID, reservationID)
	if err != nil {
		if err == ErrNotFound {
			return RejectResult{Resolved: false, CurrentStatus: ""}, nil
		}
		return RejectResult{}, err
	}

	now := time.Now().UTC()
	res, err := runScript(ctx, s.client, "reject_reservation",
		[]string{
			reservationKey(tenantID, reservationID),
			taskKey(tenantID, existing.TaskID),
			pendingTasksKey(tenantID),
			agentKey(tenantID, existing.AgentID),
			agentOffersKey(tenantID, existing.AgentID),
			reservationExpiryKey(tenantID, reservationID),
		},
		reservationID, string(reason), formatTime(now),
	)
	if err != nil {
		return RejectResult{}, err
	}
	slice, err := asSlice(res)
	if err != nil {
		return RejectResult{}, err
	}
	if asInt64(slice[0]) == 0 {
		return RejectResult{Resolved: false, CurrentStatus: ReservationStatus(asString(slice[1]))}, nil
	}
	return RejectResult{
		Resolved: true,
		TaskID:   asString(slice[1]),
		AgentID:  asString(slice[2]),
	}, nil
}
