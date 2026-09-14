package redisdomain

import (
	"context"
	"fmt"
	"time"
)

// CreateAgentInput carries the fields accepted by the "Create an Agent"
// capability (spec Section 3.2). Status is NOT validated against the
// Status registry here -- the create capability deliberately skips that
// check per spec Section 3.2; the caller (grpcapi layer) is responsible
// for defaulting an empty Status to StatusOffline before calling this.
type CreateAgentInput struct {
	AgentID    string
	Status     string
	Attributes map[string]AttributeValue
	Capacity   map[string]ChannelCapacity
	Queues     []string
	// UserID is optional -- see Agent.UserID's doc comment (types.go).
	UserID string
}

// CreateAgent atomically creates a new agent record, applying
// ChannelCapacity defaults (Ready=true, Max=1, Interruptible=true if the
// caller didn't set Max) is NOT performed here -- the grpcapi layer is
// responsible for applying proto-level defaults before calling this,
// since defaulting is a presentation-layer concern for which fields were
// simply absent from a request vs. explicitly zero. Returns
// ErrAlreadyExists if the agent ID is taken.
func (s *Store) CreateAgent(ctx context.Context, tenantID string, in CreateAgentInput) (Agent, error) {
	now := time.Now().UTC()
	status := in.Status
	if status == "" {
		status = StatusOffline
	}

	attrsJSON, err := EncodeAttributes(in.Attributes)
	if err != nil {
		return Agent{}, err
	}
	queuesJSON, err := EncodeQueues(in.Queues)
	if err != nil {
		return Agent{}, err
	}

	args := []any{
		in.AgentID,
		status,
		formatTime(now),
		attrsJSON,
		queuesJSON,
		in.UserID,
		len(in.Capacity),
	}
	for channel, cap := range in.Capacity {
		capJSON, err := EncodeCapacity(cap)
		if err != nil {
			return Agent{}, err
		}
		args = append(args, channel, capJSON)
	}

	res, err := runScript(ctx, s.client, "create_agent",
		[]string{agentsSetKey(tenantID), agentKey(tenantID, in.AgentID)},
		args...,
	)
	if err != nil {
		return Agent{}, err
	}
	if asInt64(res) == 0 {
		return Agent{}, ErrAlreadyExists
	}

	return Agent{
		AgentID:         in.AgentID,
		Status:          status,
		Attributes:      cloneAttrs(in.Attributes),
		Capacity:        cloneCap(in.Capacity),
		Queues:          append([]string(nil), in.Queues...),
		StatusChangedAt: now,
		UserID:          in.UserID,
	}, nil
}

func cloneAttrs(m map[string]AttributeValue) map[string]AttributeValue {
	out := make(map[string]AttributeValue, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneCap(m map[string]ChannelCapacity) map[string]ChannelCapacity {
	out := make(map[string]ChannelCapacity, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// GetAgent reads one agent's full record. Returns ErrNotFound if absent.
func (s *Store) GetAgent(ctx context.Context, tenantID, agentID string) (Agent, error) {
	fields, err := s.client.HGetAll(ctx, agentKey(tenantID, agentID)).Result()
	if err != nil {
		return Agent{}, fmt.Errorf("redisdomain: get agent: %w", err)
	}
	if len(fields) == 0 {
		return Agent{}, ErrNotFound
	}
	return decodeAgent(agentID, fields)
}

func decodeAgent(agentID string, fields map[string]string) (Agent, error) {
	attrs, err := DecodeAttributes(fields["attributes"])
	if err != nil {
		return Agent{}, err
	}
	queues, err := DecodeQueues(fields["queues"])
	if err != nil {
		return Agent{}, err
	}
	statusChangedAt, err := parseTime(fields["statusChangedAt"])
	if err != nil {
		return Agent{}, err
	}

	capacity := make(map[string]ChannelCapacity)
	for field, raw := range fields {
		channel, ok := channelFromCapField(field)
		if !ok {
			continue
		}
		cap, err := DecodeCapacity(raw)
		if err != nil {
			return Agent{}, err
		}
		capacity[channel] = cap
	}

	return Agent{
		AgentID:         agentID,
		Status:          fields["status"],
		Attributes:      attrs,
		Capacity:        capacity,
		Queues:          queues,
		StatusChangedAt: statusChangedAt,
		UserID:          fields["userId"],
	}, nil
}

// ListAgents returns every agent for the tenant. Order is unspecified
// (mirrors spec Section 4.1's "no defined/guaranteed order" for agent
// iteration, and Section 3.2's plain "enumerate all agents" for the list
// capability).
func (s *Store) ListAgents(ctx context.Context, tenantID string) ([]Agent, error) {
	ids, err := s.client.SMembers(ctx, agentsSetKey(tenantID)).Result()
	if err != nil {
		return nil, fmt.Errorf("redisdomain: list agents: %w", err)
	}
	agents := make([]Agent, 0, len(ids))
	for _, id := range ids {
		a, err := s.GetAgent(ctx, tenantID, id)
		if err == ErrNotFound {
			// Deleted between SMEMBERS and HGETALL; skip rather than error.
			continue
		}
		if err != nil {
			return nil, err
		}
		agents = append(agents, a)
	}
	return agents, nil
}

// SetAgentStatus atomically updates status + statusChangedAt. Status
// string validity against the registry must already have been checked by
// the caller (Postgres-backed, spec Section 2.5) before this is called.
// Returns ErrNotFound if the agent does not exist.
func (s *Store) SetAgentStatus(ctx context.Context, tenantID, agentID, status string) (time.Time, error) {
	now := time.Now().UTC()
	res, err := runScript(ctx, s.client, "set_agent_status",
		[]string{agentKey(tenantID, agentID)},
		status, formatTime(now),
	)
	if err != nil {
		return time.Time{}, err
	}
	if asInt64(res) == 0 {
		return time.Time{}, ErrNotFound
	}
	return now, nil
}

// ReplaceAgentCapacity atomically replaces the full capacity map,
// preserving `active` for retained channels and zeroing it for new ones
// (spec Section 4.2). Returns ErrNotFound if the agent does not exist.
func (s *Store) ReplaceAgentCapacity(ctx context.Context, tenantID, agentID string, capacity map[string]ChannelCapacity) error {
	args := []any{agentID, len(capacity)}
	for channel, cap := range capacity {
		// active is intentionally not encoded -- the script computes it.
		capJSON, err := EncodeCapacity(ChannelCapacity{Ready: cap.Ready, Max: cap.Max, Interruptible: cap.Interruptible})
		if err != nil {
			return err
		}
		args = append(args, channel, capJSON)
	}
	res, err := runScript(ctx, s.client, "replace_capacity",
		[]string{agentKey(tenantID, agentID)},
		args...,
	)
	if err != nil {
		return err
	}
	if asInt64(res) == 0 {
		return ErrNotFound
	}
	return nil
}

// ToggleChannelReady atomically flips one channel's ready flag, creating
// the channel with defaults if absent (spec Section 3.2). Returns
// ErrNotFound if the agent does not exist.
func (s *Store) ToggleChannelReady(ctx context.Context, tenantID, agentID, channel string, ready bool) error {
	readyArg := "0"
	if ready {
		readyArg = "1"
	}
	res, err := runScript(ctx, s.client, "toggle_channel_ready",
		[]string{agentKey(tenantID, agentID)},
		channel, readyArg,
	)
	if err != nil {
		return err
	}
	if asInt64(res) == 0 {
		return ErrNotFound
	}
	return nil
}

// ReplaceAgentQueues atomically replaces the full queue-membership list.
// Queue-ID existence validation must already have been checked by the
// caller. Returns ErrNotFound if the agent does not exist.
func (s *Store) ReplaceAgentQueues(ctx context.Context, tenantID, agentID string, queues []string) error {
	queuesJSON, err := EncodeQueues(queues)
	if err != nil {
		return err
	}
	res, err := runScript(ctx, s.client, "replace_agent_queues",
		[]string{agentKey(tenantID, agentID)},
		queuesJSON,
	)
	if err != nil {
		return err
	}
	if asInt64(res) == 0 {
		return ErrNotFound
	}
	return nil
}

// ReplaceAgentAttributes atomically replaces the full attribute map.
// Shape validation must already have been checked by the caller. Returns
// ErrNotFound if the agent does not exist.
func (s *Store) ReplaceAgentAttributes(ctx context.Context, tenantID, agentID string, attrs map[string]AttributeValue) error {
	attrsJSON, err := EncodeAttributes(attrs)
	if err != nil {
		return err
	}
	res, err := runScript(ctx, s.client, "replace_agent_attributes",
		[]string{agentKey(tenantID, agentID)},
		attrsJSON,
	)
	if err != nil {
		return err
	}
	if asInt64(res) == 0 {
		return ErrNotFound
	}
	return nil
}

// ResolvedReservation is one Offered/Accepted reservation (and its tied
// task) that delete_agent.lua resolved to Rejected (reason:
// agent_deleted) as a side effect of deleting the agent that held it.
type ResolvedReservation struct {
	ReservationID string
	TaskID        string
}

// DeleteAgentResult reports everything delete_agent.lua actually
// resolved, so the caller can publish exactly the right event(s) for EACH
// one (spec Section 5.5: "exactly one accurate notification" -- applied
// per-resolution; an agent may hold several concurrent in-flight
// reservations at once under ChannelCapacity.max > 1, spec Sections 2.1,
// 3.2, 4.2, and every one of them must be reported, not just the first).
type DeleteAgentResult struct {
	Existed  bool
	Resolved []ResolvedReservation
}

// DeleteAgent atomically deletes the agent and, for every Reserved/Active
// task it held, resolves that task back to Pending at its original FIFO
// position and marks the tied reservation Rejected (reason:
// agent_deleted) -- all as one atomic Lua script per spec Section 5.4
// rule 5 and Section 5.5. An agent may hold multiple such tasks
// concurrently (ChannelCapacity.max > 1); every one is resolved and
// reported back.
func (s *Store) DeleteAgent(ctx context.Context, tenantID, agentID string) (DeleteAgentResult, error) {
	res, err := runScript(ctx, s.client, "delete_agent",
		[]string{
			agentsSetKey(tenantID),
			agentKey(tenantID, agentID),
			agentOffersKey(tenantID, agentID),
			pendingTasksKey(tenantID),
		},
		agentID,
		taskKeyPrefix(tenantID),
		reservationKeyPrefix(tenantID),
		resexpKeyPrefix(tenantID),
	)
	if err != nil {
		return DeleteAgentResult{}, err
	}
	slice, err := asSlice(res)
	if err != nil {
		return DeleteAgentResult{}, err
	}
	if asInt64(slice[0]) == 0 {
		return DeleteAgentResult{Existed: false}, nil
	}
	if len(slice) < 2 || asInt64(slice[1]) == 0 {
		return DeleteAgentResult{Existed: true}, nil
	}
	count := int(asInt64(slice[1]))
	resolved := make([]ResolvedReservation, 0, count)
	// slice[2:] is a flat (reservationId, taskId) pairs list of length
	// 2*count.
	for i := 0; i < count; i++ {
		idx := 2 + i*2
		if idx+1 >= len(slice) {
			break
		}
		resolved = append(resolved, ResolvedReservation{
			ReservationID: asString(slice[idx]),
			TaskID:        asString(slice[idx+1]),
		})
	}
	return DeleteAgentResult{
		Existed:  true,
		Resolved: resolved,
	}, nil
}

func taskKeyPrefix(tenantID string) string {
	return fmt.Sprintf("%s:%s:task:", keyPrefix, tenantID)
}

func reservationKeyPrefix(tenantID string) string {
	return fmt.Sprintf("%s:%s:reservation:", keyPrefix, tenantID)
}

func resexpKeyPrefix(tenantID string) string {
	return fmt.Sprintf("%s:%s:resexp:", keyPrefix, tenantID)
}

// ListAgentPendingOffers returns the agent's currently Offered
// reservations (spec Section 3.2).
func (s *Store) ListAgentPendingOffers(ctx context.Context, tenantID, agentID string) ([]Reservation, error) {
	ids, err := s.client.SMembers(ctx, agentOffersKey(tenantID, agentID)).Result()
	if err != nil {
		return nil, fmt.Errorf("redisdomain: list agent pending offers: %w", err)
	}
	out := make([]Reservation, 0, len(ids))
	for _, id := range ids {
		r, err := s.GetReservation(ctx, tenantID, id)
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		if r.Status == ReservationOffered {
			out = append(out, r)
		}
	}
	return out, nil
}

// AgentExists is a lightweight existence check, used by attribute/queue
// validation flows that need to 404 before doing anything else.
func (s *Store) AgentExists(ctx context.Context, tenantID, agentID string) (bool, error) {
	n, err := s.client.SIsMember(ctx, agentsSetKey(tenantID), agentID).Result()
	if err != nil {
		return false, fmt.Errorf("redisdomain: agent exists: %w", err)
	}
	return n, nil
}
