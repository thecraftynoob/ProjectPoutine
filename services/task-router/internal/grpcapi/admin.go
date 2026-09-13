package grpcapi

import (
	"context"
	"log/slog"

	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/pgconfig"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/redisdomain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TaskRouterAdminServer implements taskrouterv1.TaskRouterAdminServiceServer:
// the low-change config domain (spec Sections 3.1, 3.6), backed by
// Postgres via pgconfig.Registry.
type TaskRouterAdminServer struct {
	taskrouterv1.UnimplementedTaskRouterAdminServiceServer

	Registry *pgconfig.Registry
	// Store is used only for "Get Queue Detail"/"Get Attribute Detail"'s
	// "plus every agent currently a member of it / assigned a value for
	// it" requirement (spec Sections 3.1, 3.6), which requires scanning
	// live Redis agent state -- the one place the admin service reads
	// the hot-path store.
	Store  *redisdomain.Store
	Logger *slog.Logger
}

func (s *TaskRouterAdminServer) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// --- Queue Configuration (spec Section 3.1) ---

func (s *TaskRouterAdminServer) RegisterQueue(ctx context.Context, req *taskrouterv1.RegisterQueueRequest) (*taskrouterv1.Queue, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetQueueId() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue_id is required")
	}
	q, err := s.Registry.RegisterQueue(ctx, tid, req.GetQueueId())
	if err == pgconfig.ErrAlreadyExists {
		return nil, status.Errorf(codes.AlreadyExists, "queue %q already exists", req.GetQueueId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "register queue: %v", err)
	}
	return queueToProto(q), nil
}

func (s *TaskRouterAdminServer) ListQueues(ctx context.Context, _ *taskrouterv1.ListQueuesRequest) (*taskrouterv1.ListQueuesResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	queues, err := s.Registry.ListQueues(ctx, tid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list queues: %v", err)
	}
	out := make([]*taskrouterv1.Queue, len(queues))
	for i, q := range queues {
		out[i] = queueToProto(q)
	}
	return &taskrouterv1.ListQueuesResponse{Queues: out}, nil
}

func (s *TaskRouterAdminServer) GetQueue(ctx context.Context, req *taskrouterv1.GetQueueRequest) (*taskrouterv1.GetQueueResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	q, err := s.Registry.GetQueue(ctx, tid, req.GetQueueId())
	if err == pgconfig.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "queue %q not found", req.GetQueueId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get queue: %v", err)
	}

	agents, err := s.Store.ListAgents(ctx, tid.String())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list agents: %v", err)
	}
	var members []string
	for _, a := range agents {
		for _, membership := range a.Queues {
			if membership == req.GetQueueId() {
				members = append(members, a.AgentID)
				break
			}
		}
	}

	return &taskrouterv1.GetQueueResponse{Queue: queueToProto(q), MemberAgentIds: members}, nil
}

func (s *TaskRouterAdminServer) RemoveQueue(ctx context.Context, req *taskrouterv1.RemoveQueueRequest) (*taskrouterv1.RemoveQueueResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.Registry.RemoveQueue(ctx, tid, req.GetQueueId()); err != nil {
		return nil, status.Errorf(codes.Internal, "remove queue: %v", err)
	}
	return &taskrouterv1.RemoveQueueResponse{}, nil
}

// --- Status Registry (spec Section 3.6) ---

func (s *TaskRouterAdminServer) RegisterStatus(ctx context.Context, req *taskrouterv1.RegisterStatusRequest) (*taskrouterv1.StatusEntry, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetStatus() == "" {
		return nil, status.Error(codes.InvalidArgument, "status is required")
	}
	entry, err := s.Registry.RegisterStatus(ctx, tid, req.GetStatus())
	if err == pgconfig.ErrAlreadyExists {
		return nil, status.Errorf(codes.AlreadyExists, "status %q already exists", req.GetStatus())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "register status: %v", err)
	}
	return statusToProto(entry), nil
}

func (s *TaskRouterAdminServer) ListStatuses(ctx context.Context, _ *taskrouterv1.ListStatusesRequest) (*taskrouterv1.ListStatusesResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	statuses, err := s.Registry.ListStatuses(ctx, tid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list statuses: %v", err)
	}
	out := make([]*taskrouterv1.StatusEntry, len(statuses))
	for i, st := range statuses {
		out[i] = statusToProto(st)
	}
	return &taskrouterv1.ListStatusesResponse{Statuses: out}, nil
}

func (s *TaskRouterAdminServer) RemoveStatus(ctx context.Context, req *taskrouterv1.RemoveStatusRequest) (*taskrouterv1.RemoveStatusResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.Registry.RemoveStatus(ctx, tid, req.GetStatus()); err != nil {
		return nil, status.Errorf(codes.Internal, "remove status: %v", err)
	}
	return &taskrouterv1.RemoveStatusResponse{}, nil
}

// --- Attribute Registry (spec Section 3.6) ---

func (s *TaskRouterAdminServer) RegisterAttribute(ctx context.Context, req *taskrouterv1.RegisterAttributeRequest) (*taskrouterv1.AttributeDefinition, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	attrType, ok := attributeTypeToDomain(req.GetType())
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid attribute type %v", req.GetType())
	}
	def, err := s.Registry.RegisterAttribute(ctx, tid, req.GetName(), attrType)
	if err == pgconfig.ErrAlreadyExists {
		return nil, status.Errorf(codes.AlreadyExists, "attribute %q already exists", req.GetName())
	}
	if err == pgconfig.ErrInvalidAttributeType {
		return nil, status.Errorf(codes.InvalidArgument, "invalid attribute type")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "register attribute: %v", err)
	}
	return attributeDefToProto(def), nil
}

func (s *TaskRouterAdminServer) ListAttributes(ctx context.Context, _ *taskrouterv1.ListAttributesRequest) (*taskrouterv1.ListAttributesResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	attrs, err := s.Registry.ListAttributes(ctx, tid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list attributes: %v", err)
	}
	out := make([]*taskrouterv1.AttributeDefinition, len(attrs))
	for i, a := range attrs {
		out[i] = attributeDefToProto(a)
	}
	return &taskrouterv1.ListAttributesResponse{Attributes: out}, nil
}

func (s *TaskRouterAdminServer) GetAttribute(ctx context.Context, req *taskrouterv1.GetAttributeRequest) (*taskrouterv1.GetAttributeResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	def, err := s.Registry.GetAttribute(ctx, tid, req.GetName())
	if err == pgconfig.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "attribute %q not found", req.GetName())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get attribute: %v", err)
	}

	agents, err := s.Store.ListAgents(ctx, tid.String())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list agents: %v", err)
	}
	var assigned []string
	for _, a := range agents {
		if _, ok := a.Attributes[req.GetName()]; ok {
			assigned = append(assigned, a.AgentID)
		}
	}

	return &taskrouterv1.GetAttributeResponse{Attribute: attributeDefToProto(def), AssignedAgentIds: assigned}, nil
}

func (s *TaskRouterAdminServer) RemoveAttribute(ctx context.Context, req *taskrouterv1.RemoveAttributeRequest) (*taskrouterv1.RemoveAttributeResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.Registry.RemoveAttribute(ctx, tid, req.GetName()); err != nil {
		return nil, status.Errorf(codes.Internal, "remove attribute: %v", err)
	}
	return &taskrouterv1.RemoveAttributeResponse{}, nil
}
