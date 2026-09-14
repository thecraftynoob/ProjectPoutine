package grpcapi

import (
	"context"

	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/pgconfig"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/redisdomain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EnqueueTask implements spec Section 3.3's "Enqueue a Task". Rejected if
// the queue doesn't exist, an attribute is invalid, or a caller-supplied
// task ID collides with an existing one.
func (s *TaskRouterServer) EnqueueTask(ctx context.Context, req *taskrouterv1.EnqueueTaskRequest) (*taskrouterv1.Task, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetQueueId() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue_id is required")
	}
	if req.GetTaskType() == "" {
		return nil, status.Error(codes.InvalidArgument, "task_type is required")
	}

	exists, err := s.Registry.QueueExists(ctx, tid, req.GetQueueId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check queue exists: %v", err)
	}
	if !exists {
		return nil, status.Errorf(codes.InvalidArgument, "queue %q does not exist", req.GetQueueId())
	}

	attrs := attrsToDomain(req.GetRequiredAttributes())
	if err := s.Registry.ValidateAttributes(ctx, tid, attrs); err != nil {
		return nil, attributeValidationStatus(err)
	}

	in := redisdomain.EnqueueTaskInput{
		TaskID:               req.GetTaskId(),
		QueueID:              req.GetQueueId(),
		TaskType:             req.GetTaskType(),
		RequiredAttributes:   attrs,
		WrapUpTimeoutSeconds: req.GetWrapUpTimeoutSeconds(),
	}
	if req.GetEnqueuedAt() != nil {
		in.EnqueuedAt = req.GetEnqueuedAt().AsTime()
	}

	task, err := s.Store.EnqueueTask(ctx, tid.String(), in)
	if err == redisdomain.ErrAlreadyExists {
		return nil, status.Errorf(codes.AlreadyExists, "task %q already exists", req.GetTaskId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue task: %v", err)
	}

	if err := s.Events.TaskEnqueued(ctx, tid, task.TaskID, task.QueueID, task.TaskType); err != nil {
		s.logger().Error("publish task enqueued failed", "error", err)
	}

	s.runMatchingPass(ctx, tid)

	fresh, err := s.Store.GetTask(ctx, tid.String(), task.TaskID)
	if err != nil {
		return taskToProto(task), nil
	}
	return taskToProto(fresh), nil
}

// ListTasks implements spec Section 3.3's "List Tasks".
func (s *TaskRouterServer) ListTasks(ctx context.Context, req *taskrouterv1.ListTasksRequest) (*taskrouterv1.ListTasksResponse, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}

	statusFilter := req.GetStatus()
	if statusFilter != "" && !redisdomain.IsValidTaskStatus(statusFilter) {
		return nil, status.Errorf(codes.InvalidArgument, "status %q is not a valid task status", statusFilter)
	}

	tasks, err := s.Store.ListTasks(ctx, tid.String(), redisdomain.TaskStatus(statusFilter))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list tasks: %v", err)
	}
	out := make([]*taskrouterv1.Task, len(tasks))
	for i, t := range tasks {
		out[i] = taskToProto(t)
	}
	return &taskrouterv1.ListTasksResponse{Tasks: out}, nil
}

// GetTask implements spec Section 3.3's "Get Task Detail".
func (s *TaskRouterServer) GetTask(ctx context.Context, req *taskrouterv1.GetTaskRequest) (*taskrouterv1.Task, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	task, err := s.Store.GetTask(ctx, tid.String(), req.GetTaskId())
	if err == redisdomain.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetTaskId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get task: %v", err)
	}
	return taskToProto(task), nil
}

// CompleteTask formally completes an interaction (Wrap Up / Disposition
// two-step completion lifecycle's step 2): valid from Active (skipping
// WrapUp entirely) or WrapUp (optionally after SetTaskDisposition).
// Rejected unless the task is currently Active or WrapUp. Releases
// capacity and resets the assigned agent's status to "Available" (spec
// Section 4.2, extended by the lifecycle).
func (s *TaskRouterServer) CompleteTask(ctx context.Context, req *taskrouterv1.CompleteTaskRequest) (*taskrouterv1.Task, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}

	result, err := s.Store.CompleteTask(ctx, tid.String(), req.GetTaskId())
	if err == redisdomain.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetTaskId())
	}
	if err == redisdomain.ErrTaskNotActive {
		return nil, status.Errorf(codes.FailedPrecondition, "task %q is not Active or WrapUp", req.GetTaskId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "complete task: %v", err)
	}

	task, err := s.Store.GetTask(ctx, tid.String(), req.GetTaskId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get task: %v", err)
	}

	if err := s.Events.TaskCompleted(ctx, tid, req.GetTaskId(), result.AgentID, task.DispositionID, task.DispositionName); err != nil {
		s.logger().Error("publish task completed failed", "error", err)
	}

	// Freed capacity could satisfy a waiting task.
	s.runMatchingPass(ctx, tid)

	return taskToProto(task), nil
}

// EndTask stops the communication channel for an Active task (Wrap Up /
// Disposition two-step completion lifecycle's step 1): moves the task to
// WrapUp and the assigned agent's status to "WrapUp", and starts the
// wrap-up timer if the task's wrap_up_timeout_seconds > 0. Rejected unless
// the task is currently Active.
func (s *TaskRouterServer) EndTask(ctx context.Context, req *taskrouterv1.EndTaskRequest) (*taskrouterv1.Task, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}

	result, err := s.Store.EndTask(ctx, tid.String(), req.GetTaskId())
	if err == redisdomain.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetTaskId())
	}
	if err == redisdomain.ErrTaskNotActive {
		return nil, status.Errorf(codes.FailedPrecondition, "task %q is not Active", req.GetTaskId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "end task: %v", err)
	}

	task, err := s.Store.GetTask(ctx, tid.String(), req.GetTaskId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get task: %v", err)
	}

	if task.WrapUpTimeoutSeconds > 0 {
		if err := s.Store.StartWrapUpTimer(ctx, tid.String(), req.GetTaskId(), task.WrapUpTimeoutSeconds); err != nil {
			s.logger().Error("start wrap-up timer failed", "error", err)
		}
	}

	if err := s.Events.TaskEnded(ctx, tid, req.GetTaskId(), result.AgentID); err != nil {
		s.logger().Error("publish task ended failed", "error", err)
	}

	return taskToProto(task), nil
}

// SetTaskDisposition tags a WrapUp task with a disposition (step 2's data
// element, settable any time during WrapUp up until the wrap-up timer
// reaches 0). Rejected unless the task is currently WrapUp, and unless
// disposition_id is a registered Disposition.
func (s *TaskRouterServer) SetTaskDisposition(ctx context.Context, req *taskrouterv1.SetTaskDispositionRequest) (*taskrouterv1.Task, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetDispositionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "disposition_id is required")
	}

	disposition, err := s.Registry.GetDisposition(ctx, tid, req.GetDispositionId())
	if err == pgconfig.ErrNotFound {
		return nil, status.Errorf(codes.InvalidArgument, "disposition %q does not exist", req.GetDispositionId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get disposition: %v", err)
	}

	err = s.Store.SetTaskDisposition(ctx, tid.String(), req.GetTaskId(), disposition.DispositionID, disposition.Name)
	if err == redisdomain.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetTaskId())
	}
	if err == redisdomain.ErrTaskNotWrapUp {
		return nil, status.Errorf(codes.FailedPrecondition, "task %q is not in WrapUp", req.GetTaskId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "set task disposition: %v", err)
	}

	if err := s.Events.TaskDispositionSet(ctx, tid, req.GetTaskId(), disposition.DispositionID, disposition.Name); err != nil {
		s.logger().Error("publish task disposition set failed", "error", err)
	}

	task, err := s.Store.GetTask(ctx, tid.String(), req.GetTaskId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get task: %v", err)
	}
	return taskToProto(task), nil
}
