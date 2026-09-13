package redisdomain

import (
	"context"
	"fmt"
	"time"
)

// EnqueueTaskInput carries the fields accepted by the "Enqueue a Task"
// capability (spec Section 3.3).
type EnqueueTaskInput struct {
	// TaskID is optional; if empty, a sequential ID is generated.
	TaskID             string
	QueueID            string
	TaskType           string
	RequiredAttributes map[string]AttributeValue
	// EnqueuedAt is optional; if zero, the server clock is used. A
	// caller-supplied value lets a task be re-inserted at its original
	// position (spec Section 2.2) -- used internally by the reject/expiry
	// paths, and available to external callers for the same reason.
	EnqueuedAt time.Time
}

// EnqueueTask atomically creates a new Pending task and inserts it into
// the FIFO pending index. Returns ErrAlreadyExists if a caller-supplied
// TaskID collides with an existing task.
func (s *Store) EnqueueTask(ctx context.Context, tenantID string, in EnqueueTaskInput) (Task, error) {
	taskID := in.TaskID
	if taskID == "" {
		var err error
		taskID, err = s.nextID(ctx, tenantID, "task")
		if err != nil {
			return Task{}, err
		}
	}
	enqueuedAt := in.EnqueuedAt
	if enqueuedAt.IsZero() {
		enqueuedAt = time.Now().UTC()
	}

	attrsJSON, err := EncodeAttributes(in.RequiredAttributes)
	if err != nil {
		return Task{}, err
	}

	res, err := runScript(ctx, s.client, "enqueue_task",
		[]string{
			tasksSetKey(tenantID),
			taskKey(tenantID, taskID),
			pendingTasksKey(tenantID),
		},
		taskID, in.QueueID, in.TaskType, attrsJSON,
		formatTime(enqueuedAt), int64(enqueuedAt.UnixMicro()),
	)
	if err != nil {
		return Task{}, err
	}
	if asInt64(res) == 0 {
		return Task{}, ErrAlreadyExists
	}

	return Task{
		TaskID:             taskID,
		QueueID:            in.QueueID,
		TaskType:           in.TaskType,
		RequiredAttributes: cloneAttrs(in.RequiredAttributes),
		EnqueuedAt:         enqueuedAt,
		Status:             TaskPending,
	}, nil
}

// GetTask reads one task's full record. Returns ErrNotFound if absent.
func (s *Store) GetTask(ctx context.Context, tenantID, taskID string) (Task, error) {
	fields, err := s.client.HGetAll(ctx, taskKey(tenantID, taskID)).Result()
	if err != nil {
		return Task{}, fmt.Errorf("redisdomain: get task: %w", err)
	}
	if len(fields) == 0 {
		return Task{}, ErrNotFound
	}
	return decodeTask(taskID, fields)
}

func decodeTask(taskID string, fields map[string]string) (Task, error) {
	attrs, err := DecodeAttributes(fields["requiredAttributes"])
	if err != nil {
		return Task{}, err
	}
	enqueuedAt, err := parseTime(fields["enqueuedAt"])
	if err != nil {
		return Task{}, err
	}
	return Task{
		TaskID:               taskID,
		QueueID:              fields["queueId"],
		TaskType:             fields["taskType"],
		RequiredAttributes:   attrs,
		EnqueuedAt:           enqueuedAt,
		Status:               TaskStatus(fields["status"]),
		CurrentReservationID: fields["currentReservationId"],
		AssignedAgentID:      fields["assignedAgentId"],
	}, nil
}

// ListTasks returns every task for the tenant, in unspecified order (the
// "list all tasks" read-only capability, spec Section 3.3 -- FIFO
// ordering only matters for the matching algorithm's own pending scan,
// see PendingTasksFIFO).
func (s *Store) ListTasks(ctx context.Context, tenantID string) ([]Task, error) {
	ids, err := s.client.SMembers(ctx, tasksSetKey(tenantID)).Result()
	if err != nil {
		return nil, fmt.Errorf("redisdomain: list tasks: %w", err)
	}
	tasks := make([]Task, 0, len(ids))
	for _, id := range ids {
		t, err := s.GetTask(ctx, tenantID, id)
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

// PendingTasksFIFO returns every task ID currently Pending, strictly
// ordered oldest-enqueuedAt-first (spec Section 4.1's evaluate_once()
// task loop).
func (s *Store) PendingTasksFIFO(ctx context.Context, tenantID string) ([]string, error) {
	ids, err := s.client.ZRange(ctx, pendingTasksKey(tenantID), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("redisdomain: pending tasks fifo: %w", err)
	}
	return ids, nil
}

// CompleteTaskResult reports the outcome of CompleteTask.
type CompleteTaskResult struct {
	// AgentID is the agent capacity was released on (may be empty if the
	// task existed but had no live assigned agent record).
	AgentID string
}

// ErrTaskNotActive is returned by CompleteTask when the task exists but
// is not currently Active.
var ErrTaskNotActive = fmt.Errorf("redisdomain: task not active")

// CompleteTask atomically completes an Active task and releases the
// assigned agent's capacity (spec Sections 3.3, 4.2, 5.4 rule 2). Returns
// ErrNotFound if the task doesn't exist, ErrTaskNotActive if it exists
// but isn't Active.
func (s *Store) CompleteTask(ctx context.Context, tenantID, taskID string) (CompleteTaskResult, error) {
	task, err := s.GetTask(ctx, tenantID, taskID)
	if err != nil {
		return CompleteTaskResult{}, err
	}
	agentKeyStr := ""
	agentOffersKeyStr := ""
	if task.AssignedAgentID != "" {
		agentKeyStr = agentKey(tenantID, task.AssignedAgentID)
		agentOffersKeyStr = agentOffersKey(tenantID, task.AssignedAgentID)
	} else {
		// Pass a harmless placeholder key; the script checks EXISTS and
		// no-ops safely even for a key that was never meant to be real.
		agentKeyStr = agentKey(tenantID, "__none__")
		agentOffersKeyStr = agentOffersKey(tenantID, "__none__")
	}

	res, err := runScript(ctx, s.client, "complete_task",
		[]string{taskKey(tenantID, taskID), agentKeyStr, agentOffersKeyStr},
		taskID,
	)
	if err != nil {
		return CompleteTaskResult{}, err
	}
	slice, err := asSlice(res)
	if err != nil {
		return CompleteTaskResult{}, err
	}
	if asInt64(slice[0]) == 0 {
		status := asString(slice[1])
		if status == "" {
			return CompleteTaskResult{}, ErrNotFound
		}
		return CompleteTaskResult{}, ErrTaskNotActive
	}
	return CompleteTaskResult{AgentID: asString(slice[1])}, nil
}

// nextID generates a sequential, server-assigned ID for the given entity
// kind ("task" or "reservation") via Redis's atomic HINCRBY, prefixed for
// readability.
func (s *Store) nextID(ctx context.Context, tenantID, kind string) (string, error) {
	n, err := s.client.HIncrBy(ctx, seqKey(tenantID), kind, 1).Result()
	if err != nil {
		return "", fmt.Errorf("redisdomain: generate %s id: %w", kind, err)
	}
	return fmt.Sprintf("%s-%d", kind, n), nil
}
