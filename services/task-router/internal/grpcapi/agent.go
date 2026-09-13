package grpcapi

import (
	"context"

	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/pgconfig"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/redisdomain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateAgent implements spec Section 3.2's "Create an Agent". Status is
// NOT validated against the Status registry at creation time (spec
// explicitly calls this out) -- only defaulted to "Offline" if empty.
// Attributes ARE validated for shape (spec Section 4.4). Rejected if the
// agent ID already exists (spec Section 5.4 rule 1).
func (s *TaskRouterServer) CreateAgent(ctx context.Context, req *taskrouterv1.CreateAgentRequest) (*taskrouterv1.Agent, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetAgentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "agent_id is required")
	}

	attrs := attrsToDomain(req.GetAttributes())
	if err := s.Registry.ValidateAttributes(ctx, tid, attrs); err != nil {
		return nil, attributeValidationStatus(err)
	}

	capacity := capacityWithDefaults(req.GetCapacity())

	agent, err := s.Store.CreateAgent(ctx, tid.String(), redisdomain.CreateAgentInput{
		AgentID:    req.GetAgentId(),
		Status:     req.GetStatus(),
		Attributes: attrs,
		Capacity:   capacity,
		Queues:     req.GetQueues(),
	})
	if err == redisdomain.ErrAlreadyExists {
		return nil, status.Errorf(codes.AlreadyExists, "agent %q already exists", req.GetAgentId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create agent: %v", err)
	}

	if err := s.Events.AgentCreated(ctx, tid, agent.AgentID, agent.Status); err != nil {
		s.logger().Error("publish agent created failed", "error", err)
	}

	// A newly created Available agent could immediately satisfy a
	// waiting task.
	s.runMatchingPass(ctx, tid)

	// Re-read so the response reflects any concurrent capacity mutation
	// applied by the matching pass we just ran (e.g. active incremented).
	fresh, err := s.Store.GetAgent(ctx, tid.String(), agent.AgentID)
	if err != nil {
		return agentToProto(agent), nil
	}
	return agentToProto(fresh), nil
}

// capacityWithDefaults applies spec Section 2.1's ChannelCapacity
// defaults (ready=true, max=1, interruptible=true) to every entry the
// caller supplied, since proto3 has no field-presence tracking for plain
// bool/int32 and an omitted-vs-zero distinction matters here: a caller
// that supplies a capacity entry at all is expected to get the spec's
// stated defaults for any field within it they didn't set to a
// meaningful non-zero-equivalent value. Because proto3 cannot distinguish
// "caller wants ready=false" from "caller didn't set ready", and the
// spec's default is ready=true, this function treats every capacity
// entry AS GIVEN except max, which defaults to DefaultChannelMax(1) when
// zero (0 is never a sensible max) -- ready/interruptible are taken
// as-given (defaulting a false-meaning-unset bool is not reliably
// distinguishable from a deliberate false, and CreateAgent's proto
// request shape does not use wrapper types to disambiguate, a scoping
// simplification documented here rather than silently getting it wrong
// in one direction).
func capacityWithDefaults(in map[string]*taskrouterv1.ChannelCapacity) map[string]redisdomain.ChannelCapacity {
	out := capacityToDomain(in)
	for k, v := range out {
		if v.Max == 0 {
			v.Max = redisdomain.DefaultChannelMax
			out[k] = v
		}
	}
	return out
}

func attributeValidationStatus(err error) error {
	if verr, ok := err.(*pgconfig.AttributeValidationError); ok {
		return status.Error(codes.InvalidArgument, verr.Error())
	}
	return status.Errorf(codes.Internal, "validate attributes: %v", err)
}

// ListAgents implements spec Section 3.2's "List Agents".
func (s *TaskRouterServer) ListAgents(ctx context.Context, _ *taskrouterv1.ListAgentsRequest) (*taskrouterv1.ListAgentsResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	agents, err := s.Store.ListAgents(ctx, tid.String())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list agents: %v", err)
	}
	out := make([]*taskrouterv1.Agent, len(agents))
	for i, a := range agents {
		out[i] = agentToProto(a)
	}
	return &taskrouterv1.ListAgentsResponse{Agents: out}, nil
}

// GetAgent implements spec Section 3.2's "Get Agent Detail".
func (s *TaskRouterServer) GetAgent(ctx context.Context, req *taskrouterv1.GetAgentRequest) (*taskrouterv1.Agent, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	agent, err := s.Store.GetAgent(ctx, tid.String(), req.GetAgentId())
	if err == redisdomain.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "agent %q not found", req.GetAgentId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get agent: %v", err)
	}
	return agentToProto(agent), nil
}

// DeleteAgent implements spec Section 3.2's "Remove an Agent" and Section
// 5.5's edge case, via delete_agent.lua's single atomic operation (spec
// Section 5.4 rule 5). An agent may hold multiple concurrent in-flight
// (Offered or Accepted) reservations at once (ChannelCapacity.max > 1,
// spec Sections 2.1, 3.2, 4.2); this publishes one Reservation Rejected
// event (reason: agent_deleted) for EACH one delete_agent.lua resolved --
// zero, one, or many -- then always publishes Agent Deleted.
func (s *TaskRouterServer) DeleteAgent(ctx context.Context, req *taskrouterv1.DeleteAgentRequest) (*taskrouterv1.DeleteAgentResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	result, err := s.Store.DeleteAgent(ctx, tid.String(), req.GetAgentId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete agent: %v", err)
	}
	if !result.Existed {
		return nil, status.Errorf(codes.NotFound, "agent %q not found", req.GetAgentId())
	}

	resolved := len(result.Resolved) > 0
	for _, r := range result.Resolved {
		if err := s.Events.ReservationRejected(ctx, tid, r.ReservationID, r.TaskID, req.GetAgentId(), string(redisdomain.ReasonAgentDeleted)); err != nil {
			s.logger().Error("publish reservation rejected (agent deleted) failed", "error", err, "reservationId", r.ReservationID, "taskId", r.TaskID)
		}
	}
	if err := s.Events.AgentDeleted(ctx, tid, req.GetAgentId()); err != nil {
		s.logger().Error("publish agent deleted failed", "error", err)
	}

	// Every requeued task (if any) is now eligible for re-matching.
	if resolved {
		s.runMatchingPass(ctx, tid)
	}

	return &taskrouterv1.DeleteAgentResponse{ResolvedInFlightReservation: resolved}, nil
}

// SetAgentStatus implements spec Section 3.2's "Set Agent Master Status".
// Rejected if the target status is not a registered Status value.
func (s *TaskRouterServer) SetAgentStatus(ctx context.Context, req *taskrouterv1.SetAgentStatusRequest) (*taskrouterv1.Agent, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetStatus() == "" {
		return nil, status.Error(codes.InvalidArgument, "status is required")
	}

	exists, err := s.Registry.StatusExists(ctx, tid, req.GetStatus())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check status registry: %v", err)
	}
	if !exists {
		return nil, status.Errorf(codes.InvalidArgument, "status %q is not registered", req.GetStatus())
	}

	if _, err := s.Store.SetAgentStatus(ctx, tid.String(), req.GetAgentId(), req.GetStatus()); err != nil {
		if err == redisdomain.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "agent %q not found", req.GetAgentId())
		}
		return nil, status.Errorf(codes.Internal, "set agent status: %v", err)
	}

	if err := s.Events.AgentStatusChanged(ctx, tid, req.GetAgentId(), req.GetStatus()); err != nil {
		s.logger().Error("publish agent status changed failed", "error", err)
	}

	// Becoming Available could satisfy a waiting task.
	if req.GetStatus() == redisdomain.StatusAvailable {
		s.runMatchingPass(ctx, tid)
	}

	agent, err := s.Store.GetAgent(ctx, tid.String(), req.GetAgentId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get agent: %v", err)
	}
	return agentToProto(agent), nil
}

// ReplaceAgentCapacity implements spec Section 3.2's "Replace Agent
// Capacity Map".
func (s *TaskRouterServer) ReplaceAgentCapacity(ctx context.Context, req *taskrouterv1.ReplaceAgentCapacityRequest) (*taskrouterv1.Agent, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	capacity := capacityWithDefaults(req.GetCapacity())

	if err := s.Store.ReplaceAgentCapacity(ctx, tid.String(), req.GetAgentId(), capacity); err != nil {
		if err == redisdomain.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "agent %q not found", req.GetAgentId())
		}
		return nil, status.Errorf(codes.Internal, "replace agent capacity: %v", err)
	}

	if err := s.Events.AgentCapacityConfigUpdated(ctx, tid, req.GetAgentId()); err != nil {
		s.logger().Error("publish agent capacity config updated failed", "error", err)
	}

	// Increased headroom on any channel could satisfy a waiting task.
	s.runMatchingPass(ctx, tid)

	agent, err := s.Store.GetAgent(ctx, tid.String(), req.GetAgentId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get agent: %v", err)
	}
	return agentToProto(agent), nil
}

// ToggleChannelReady implements spec Section 3.2's "Toggle One Channel's
// Ready Flag".
func (s *TaskRouterServer) ToggleChannelReady(ctx context.Context, req *taskrouterv1.ToggleChannelReadyRequest) (*taskrouterv1.Agent, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetChannel() == "" {
		return nil, status.Error(codes.InvalidArgument, "channel is required")
	}

	if err := s.Store.ToggleChannelReady(ctx, tid.String(), req.GetAgentId(), req.GetChannel(), req.GetReady()); err != nil {
		if err == redisdomain.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "agent %q not found", req.GetAgentId())
		}
		return nil, status.Errorf(codes.Internal, "toggle channel ready: %v", err)
	}

	if err := s.Events.AgentCapacityConfigUpdated(ctx, tid, req.GetAgentId()); err != nil {
		s.logger().Error("publish agent capacity config updated failed", "error", err)
	}

	// ready has no effect on matching (spec Section 4.1/7.3) so no
	// matching pass is triggered here -- toggling it can never create a
	// new matching opportunity.

	agent, err := s.Store.GetAgent(ctx, tid.String(), req.GetAgentId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get agent: %v", err)
	}
	return agentToProto(agent), nil
}

// ReplaceAgentQueues implements spec Section 3.2's "Replace Agent Queue
// Memberships". Rejected entirely (no partial application) if any queue
// ID doesn't exist.
func (s *TaskRouterServer) ReplaceAgentQueues(ctx context.Context, req *taskrouterv1.ReplaceAgentQueuesRequest) (*taskrouterv1.Agent, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}

	missing, err := s.Registry.QueuesExist(ctx, tid, req.GetQueues())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check queues exist: %v", err)
	}
	if len(missing) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "queue(s) do not exist: %v", missing)
	}

	if err := s.Store.ReplaceAgentQueues(ctx, tid.String(), req.GetAgentId(), req.GetQueues()); err != nil {
		if err == redisdomain.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "agent %q not found", req.GetAgentId())
		}
		return nil, status.Errorf(codes.Internal, "replace agent queues: %v", err)
	}

	if err := s.Events.AgentQueuesUpdated(ctx, tid, req.GetAgentId(), req.GetQueues()); err != nil {
		s.logger().Error("publish agent queues updated failed", "error", err)
	}

	// New queue membership could satisfy a waiting task in that queue.
	s.runMatchingPass(ctx, tid)

	agent, err := s.Store.GetAgent(ctx, tid.String(), req.GetAgentId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get agent: %v", err)
	}
	return agentToProto(agent), nil
}

// ReplaceAgentAttributes implements spec Section 3.2's "Replace Agent
// Attributes". Emits no event per spec Section 6.2 ("notably absent").
func (s *TaskRouterServer) ReplaceAgentAttributes(ctx context.Context, req *taskrouterv1.ReplaceAgentAttributesRequest) (*taskrouterv1.Agent, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}

	attrs := attrsToDomain(req.GetAttributes())
	if err := s.Registry.ValidateAttributes(ctx, tid, attrs); err != nil {
		return nil, attributeValidationStatus(err)
	}

	if err := s.Store.ReplaceAgentAttributes(ctx, tid.String(), req.GetAgentId(), attrs); err != nil {
		if err == redisdomain.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "agent %q not found", req.GetAgentId())
		}
		return nil, status.Errorf(codes.Internal, "replace agent attributes: %v", err)
	}

	agent, err := s.Store.GetAgent(ctx, tid.String(), req.GetAgentId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get agent: %v", err)
	}
	return agentToProto(agent), nil
}

// ListAgentPendingOffers implements spec Section 3.2's "List an Agent's
// Pending Offers".
func (s *TaskRouterServer) ListAgentPendingOffers(ctx context.Context, req *taskrouterv1.ListAgentPendingOffersRequest) (*taskrouterv1.ListAgentPendingOffersResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	reservations, err := s.Store.ListAgentPendingOffers(ctx, tid.String(), req.GetAgentId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list agent pending offers: %v", err)
	}
	out := make([]*taskrouterv1.Reservation, len(reservations))
	for i, r := range reservations {
		out[i] = reservationToProto(r)
	}
	return &taskrouterv1.ListAgentPendingOffersResponse{Reservations: out}, nil
}
