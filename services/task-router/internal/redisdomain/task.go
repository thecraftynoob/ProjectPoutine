package redisdomain

import (
	"context"
	"fmt"
	"strconv"
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
	// WrapUpTimeoutSeconds is optional (Wrap Up / Disposition lifecycle);
	// 0 means no wrap-up timer is configured for this task. Immutable
	// after enqueue.
	WrapUpTimeoutSeconds int32
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
		int64(in.WrapUpTimeoutSeconds),
	)
	if err != nil {
		return Task{}, err
	}
	if asInt64(res) == 0 {
		return Task{}, ErrAlreadyExists
	}

	return Task{
		TaskID:               taskID,
		QueueID:              in.QueueID,
		TaskType:             in.TaskType,
		RequiredAttributes:   cloneAttrs(in.RequiredAttributes),
		EnqueuedAt:           enqueuedAt,
		Status:               TaskPending,
		WrapUpTimeoutSeconds: in.WrapUpTimeoutSeconds,
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
	var wrapUpTimeoutSeconds int32
	if raw := fields["wrapUpTimeoutSeconds"]; raw != "" {
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return Task{}, fmt.Errorf("redisdomain: decode task wrapUpTimeoutSeconds: %w", err)
		}
		wrapUpTimeoutSeconds = int32(n)
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
		WrapUpTimeoutSeconds: wrapUpTimeoutSeconds,
		DispositionID:        fields["dispositionId"],
		DispositionName:      fields["dispositionName"],
	}, nil
}

// ListTasks returns every task for the tenant, in unspecified order (the
// "list all tasks" read-only capability, spec Section 3.3 -- FIFO
// ordering only matters for the matching algorithm's own pending scan,
// see PendingTasksFIFO), optionally narrowed to a single status.
//
// statusFilter == "" (the proto3 zero value for an unset optional field)
// means no filtering -- return every task regardless of status. There is
// no separate Redis index by status; the tenant's task Set has no
// status-keyed structure to query directly, so filtering is a plain
// in-memory check applied while hydrating each task via GetTask, which
// this call already does unconditionally.
func (s *Store) ListTasks(ctx context.Context, tenantID string, statusFilter TaskStatus) ([]Task, error) {
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
		if statusFilter != "" && t.Status != statusFilter {
			continue
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

// ErrTaskNotActive is returned by CompleteTask when the task exists but is
// not currently in a completable status (Active or WrapUp -- see the Wrap
// Up / Disposition two-step completion lifecycle).
var ErrTaskNotActive = fmt.Errorf("redisdomain: task not active or wrap-up")

// CompleteTask atomically completes a task currently Active or WrapUp,
// releases the assigned agent's capacity, and resets the agent's status to
// "Available" (spec Sections 3.3, 4.2, 5.4 rule 2, extended by the Wrap Up
// / Disposition lifecycle). Returns ErrNotFound if the task doesn't exist,
// ErrTaskNotActive if it exists but isn't Active or WrapUp.
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
		[]string{taskKey(tenantID, taskID), agentKeyStr, agentOffersKeyStr, wrapUpExpiryKey(tenantID, taskID)},
		taskID, formatTime(time.Now().UTC()),
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

// ErrTaskNotWrapUp is returned by SetTaskDisposition when the task exists
// but is not currently WrapUp.
var ErrTaskNotWrapUp = fmt.Errorf("redisdomain: task not in wrap-up")

// EndTaskResult reports the outcome of EndTask.
type EndTaskResult struct {
	// AgentID is the agent moved to WrapUp status (may be empty if the
	// task existed but had no live assigned agent record).
	AgentID string
}

// EndTask atomically stops the communication channel for an Active task
// (step 1 of the Wrap Up / Disposition two-step completion lifecycle):
// moves the task to WrapUp and the assigned agent's status to "WrapUp".
// Does NOT start the wrap-up timer itself -- see
// (*Store).StartWrapUpTimer, called by the grpcapi layer immediately after
// this succeeds, only when the task's WrapUpTimeoutSeconds > 0. Returns
// ErrNotFound if the task doesn't exist, ErrTaskNotActive if it exists but
// isn't currently Active.
func (s *Store) EndTask(ctx context.Context, tenantID, taskID string) (EndTaskResult, error) {
	task, err := s.GetTask(ctx, tenantID, taskID)
	if err != nil {
		return EndTaskResult{}, err
	}
	agentKeyStr := agentKey(tenantID, "__none__")
	if task.AssignedAgentID != "" {
		agentKeyStr = agentKey(tenantID, task.AssignedAgentID)
	}

	res, err := runScript(ctx, s.client, "end_task",
		[]string{taskKey(tenantID, taskID), agentKeyStr},
		taskID, formatTime(time.Now().UTC()),
	)
	if err != nil {
		return EndTaskResult{}, err
	}
	slice, err := asSlice(res)
	if err != nil {
		return EndTaskResult{}, err
	}
	if asInt64(slice[0]) == 0 {
		status := asString(slice[1])
		if status == "" {
			return EndTaskResult{}, ErrNotFound
		}
		return EndTaskResult{}, ErrTaskNotActive
	}
	return EndTaskResult{AgentID: asString(slice[1])}, nil
}

// StartWrapUpTimer sets the TTL-bearing sentinel key that drives the
// wrap-up timer (mirrors MatchCommit's reservation-expiry sentinel
// exactly). Called by the grpcapi layer right after a successful EndTask,
// only when timeoutSeconds > 0 (0 means no wrap-up timer is configured;
// see EnqueueTaskInput.WrapUpTimeoutSeconds's doc comment).
func (s *Store) StartWrapUpTimer(ctx context.Context, tenantID, taskID string, timeoutSeconds int32) error {
	if timeoutSeconds <= 0 {
		return nil
	}
	if err := s.client.Set(ctx, wrapUpExpiryKey(tenantID, taskID), "1", time.Duration(timeoutSeconds)*time.Second).Err(); err != nil {
		return fmt.Errorf("redisdomain: start wrap-up timer: %w", err)
	}
	return nil
}

// SetTaskDisposition atomically tags a WrapUp task with a disposition
// (step 2's data element). dispositionID/dispositionName validity against
// the tenant's Disposition registry must be checked by the caller before
// invoking this (Postgres, pgconfig -- this Store has no knowledge of that
// registry). Returns ErrNotFound if the task doesn't exist, ErrTaskNotWrapUp
// if it exists but isn't currently WrapUp.
func (s *Store) SetTaskDisposition(ctx context.Context, tenantID, taskID, dispositionID, dispositionName string) error {
	res, err := runScript(ctx, s.client, "set_task_disposition",
		[]string{taskKey(tenantID, taskID)},
		taskID, dispositionID, dispositionName,
	)
	if err != nil {
		return err
	}
	slice, err := asSlice(res)
	if err != nil {
		return err
	}
	if asInt64(slice[0]) == 0 {
		status := asString(slice[1])
		if status == "" {
			return ErrNotFound
		}
		return ErrTaskNotWrapUp
	}
	return nil
}

// WrapUpTimeoutResult reports the outcome of ResolveWrapUpTimeout.
type WrapUpTimeoutResult struct {
	// Resolved is true if the agent's status was actually reset to
	// Available by this call (false if the task had already left WrapUp
	// by the time the sweep ran -- spec Section 5.4 rule 4's general
	// idempotent-safe-resolution principle, applied to the wrap-up timer).
	Resolved bool
	AgentID  string
}

// ResolveWrapUpTimeout handles one fired wrap-up-timer sentinel key
// notification (see SubscribeWrapUpTimeout): if taskID is still WrapUp,
// resets the assigned agent's status to "Available" (spec: "If the
// Agent's Wrap up timer reaches 0... the agent's status needs to be
// updated to Available"). The task itself is left untouched.
func (s *Store) ResolveWrapUpTimeout(ctx context.Context, tenantID, taskID string) (WrapUpTimeoutResult, error) {
	task, err := s.GetTask(ctx, tenantID, taskID)
	if err == ErrNotFound {
		return WrapUpTimeoutResult{}, nil
	}
	if err != nil {
		return WrapUpTimeoutResult{}, err
	}
	agentKeyStr := agentKey(tenantID, "__none__")
	if task.AssignedAgentID != "" {
		agentKeyStr = agentKey(tenantID, task.AssignedAgentID)
	}

	res, err := runScript(ctx, s.client, "wrap_up_timeout",
		[]string{taskKey(tenantID, taskID), agentKeyStr},
		formatTime(time.Now().UTC()),
	)
	if err != nil {
		return WrapUpTimeoutResult{}, err
	}
	slice, err := asSlice(res)
	if err != nil {
		return WrapUpTimeoutResult{}, err
	}
	if asInt64(slice[0]) == 0 {
		// Task no longer exists or already left WrapUp -- no-op.
		return WrapUpTimeoutResult{}, nil
	}
	agentID := asString(slice[1])
	if agentID == "" {
		return WrapUpTimeoutResult{}, nil
	}
	return WrapUpTimeoutResult{Resolved: true, AgentID: agentID}, nil
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
