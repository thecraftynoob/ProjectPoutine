package grpcapi

import (
	"context"

	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/redisdomain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetDashboard implements spec Section 3.7's "Aggregate Dashboard View":
// all agents (with live assigned-task counts) and all tasks (with current
// status/assignment), for a monitoring display.
func (s *TaskRouterServer) GetDashboard(ctx context.Context, _ *taskrouterv1.GetDashboardRequest) (*taskrouterv1.GetDashboardResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}

	agents, err := s.Store.ListAgents(ctx, tid.String())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list agents: %v", err)
	}
	tasks, err := s.Store.ListTasks(ctx, tid.String(), "")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list tasks: %v", err)
	}

	assignedCounts := make(map[string]int32, len(agents))
	for _, t := range tasks {
		if t.AssignedAgentID == "" {
			continue
		}
		if t.Status == redisdomain.TaskReserved || t.Status == redisdomain.TaskActive || t.Status == redisdomain.TaskWrapUp {
			assignedCounts[t.AssignedAgentID]++
		}
	}

	dashboardAgents := make([]*taskrouterv1.DashboardAgent, len(agents))
	for i, a := range agents {
		dashboardAgents[i] = &taskrouterv1.DashboardAgent{
			Agent:             agentToProto(a),
			AssignedTaskCount: assignedCounts[a.AgentID],
		}
	}

	protoTasks := make([]*taskrouterv1.Task, len(tasks))
	for i, t := range tasks {
		protoTasks[i] = taskToProto(t)
	}

	return &taskrouterv1.GetDashboardResponse{
		Agents: dashboardAgents,
		Tasks:  protoTasks,
	}, nil
}
