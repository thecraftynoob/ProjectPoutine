package redisdomain

import (
	"context"
	"testing"
)

// --- Wrap Up / Disposition two-step completion lifecycle ---

func TestEndTask_MovesTaskAndAgentToWrapUp(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat", WrapUpTimeoutSeconds: 60})

	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("expected 1 match, got %d outcomes, err=%v", len(outcomes), err)
	}
	if _, err := s.AcceptReservation(ctx, tenant, outcomes[0].Reservation.ReservationID); err != nil {
		t.Fatalf("AcceptReservation failed: %v", err)
	}

	result, err := s.EndTask(ctx, tenant, "task-1")
	if err != nil {
		t.Fatalf("EndTask failed: %v", err)
	}
	if result.AgentID != "agent-1" {
		t.Fatalf("expected agent-1, got %q", result.AgentID)
	}

	task, _ := s.GetTask(ctx, tenant, "task-1")
	if task.Status != TaskWrapUp {
		t.Fatalf("expected task WrapUp, got %s", task.Status)
	}
	agent, _ := s.GetAgent(ctx, tenant, "agent-1")
	if agent.Status != StatusWrapUp {
		t.Fatalf("expected agent status WrapUp, got %s", agent.Status)
	}
}

func TestEndTask_RejectedUnlessActive(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})

	_, err := s.EndTask(ctx, tenant, "task-1")
	if err != ErrTaskNotActive {
		t.Fatalf("expected ErrTaskNotActive for a Pending task, got %v", err)
	}

	if _, err := s.EndTask(ctx, tenant, "no-such-task"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestSetTaskDisposition_OnlyDuringWrapUp(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})

	// Rejected while still Pending.
	if err := s.SetTaskDisposition(ctx, tenant, "task-1", "disp-1", "Ticket Created"); err != ErrTaskNotWrapUp {
		t.Fatalf("expected ErrTaskNotWrapUp for a Pending task, got %v", err)
	}

	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("expected 1 match, got %d outcomes, err=%v", len(outcomes), err)
	}
	if _, err := s.AcceptReservation(ctx, tenant, outcomes[0].Reservation.ReservationID); err != nil {
		t.Fatalf("AcceptReservation failed: %v", err)
	}

	// Rejected while Active (not yet ended).
	if err := s.SetTaskDisposition(ctx, tenant, "task-1", "disp-1", "Ticket Created"); err != ErrTaskNotWrapUp {
		t.Fatalf("expected ErrTaskNotWrapUp for an Active task, got %v", err)
	}

	if _, err := s.EndTask(ctx, tenant, "task-1"); err != nil {
		t.Fatalf("EndTask failed: %v", err)
	}

	if err := s.SetTaskDisposition(ctx, tenant, "task-1", "disp-1", "Ticket Created"); err != nil {
		t.Fatalf("SetTaskDisposition during WrapUp failed: %v", err)
	}

	task, _ := s.GetTask(ctx, tenant, "task-1")
	if task.DispositionID != "disp-1" || task.DispositionName != "Ticket Created" {
		t.Fatalf("expected disposition to be set, got id=%q name=%q", task.DispositionID, task.DispositionName)
	}
}

func TestCompleteTask_FromWrapUp_ReleasesCapacityAndResetsAgentAvailable(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})

	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("expected 1 match, got %d outcomes, err=%v", len(outcomes), err)
	}
	if _, err := s.AcceptReservation(ctx, tenant, outcomes[0].Reservation.ReservationID); err != nil {
		t.Fatalf("AcceptReservation failed: %v", err)
	}
	if _, err := s.EndTask(ctx, tenant, "task-1"); err != nil {
		t.Fatalf("EndTask failed: %v", err)
	}

	agentDuringWrapUp, _ := s.GetAgent(ctx, tenant, "agent-1")
	if agentDuringWrapUp.Status != StatusWrapUp {
		t.Fatalf("expected agent WrapUp before complete, got %s", agentDuringWrapUp.Status)
	}
	if agentDuringWrapUp.Capacity["chat"].Active != 1 {
		t.Fatalf("expected capacity still held during WrapUp, got %d", agentDuringWrapUp.Capacity["chat"].Active)
	}

	completeResult, err := s.CompleteTask(ctx, tenant, "task-1")
	if err != nil {
		t.Fatalf("CompleteTask from WrapUp failed: %v", err)
	}
	if completeResult.AgentID != "agent-1" {
		t.Fatalf("expected agent-1, got %q", completeResult.AgentID)
	}

	task, _ := s.GetTask(ctx, tenant, "task-1")
	if task.Status != TaskCompleted {
		t.Fatalf("expected task Completed, got %s", task.Status)
	}

	agentAfterComplete, _ := s.GetAgent(ctx, tenant, "agent-1")
	if agentAfterComplete.Status != StatusAvailable {
		t.Fatalf("expected agent Available after CompleteTask, got %s", agentAfterComplete.Status)
	}
	if agentAfterComplete.Capacity["chat"].Active != 0 {
		t.Fatalf("expected active=0 after complete, got %d", agentAfterComplete.Capacity["chat"].Active)
	}
}

func TestCompleteTask_DirectlyFromActive_StillWorks(t *testing.T) {
	// A task with no wrap_up_timeout_seconds configured can be completed
	// directly from Active, skipping WrapUp entirely -- the original,
	// unmodified behavior, now also resetting the agent to Available (a
	// new side effect introduced by the Wrap Up / Disposition lifecycle).
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})

	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("expected 1 match, got %d outcomes, err=%v", len(outcomes), err)
	}
	if _, err := s.AcceptReservation(ctx, tenant, outcomes[0].Reservation.ReservationID); err != nil {
		t.Fatalf("AcceptReservation failed: %v", err)
	}

	if _, err := s.CompleteTask(ctx, tenant, "task-1"); err != nil {
		t.Fatalf("CompleteTask from Active failed: %v", err)
	}

	task, _ := s.GetTask(ctx, tenant, "task-1")
	if task.Status != TaskCompleted {
		t.Fatalf("expected task Completed, got %s", task.Status)
	}
	agent, _ := s.GetAgent(ctx, tenant, "agent-1")
	if agent.Status != StatusAvailable {
		t.Fatalf("expected agent Available, got %s", agent.Status)
	}
}

func TestResolveWrapUpTimeout_ResetsAgentButLeavesTaskWrapUp(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat", WrapUpTimeoutSeconds: 30})

	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("expected 1 match, got %d outcomes, err=%v", len(outcomes), err)
	}
	if _, err := s.AcceptReservation(ctx, tenant, outcomes[0].Reservation.ReservationID); err != nil {
		t.Fatalf("AcceptReservation failed: %v", err)
	}
	if _, err := s.EndTask(ctx, tenant, "task-1"); err != nil {
		t.Fatalf("EndTask failed: %v", err)
	}

	result, err := s.ResolveWrapUpTimeout(ctx, tenant, "task-1")
	if err != nil {
		t.Fatalf("ResolveWrapUpTimeout failed: %v", err)
	}
	if !result.Resolved || result.AgentID != "agent-1" {
		t.Fatalf("expected resolved=true agentId=agent-1, got resolved=%v agentId=%q", result.Resolved, result.AgentID)
	}

	agent, _ := s.GetAgent(ctx, tenant, "agent-1")
	if agent.Status != StatusAvailable {
		t.Fatalf("expected agent reset to Available, got %s", agent.Status)
	}
	task, _ := s.GetTask(ctx, tenant, "task-1")
	if task.Status != TaskWrapUp {
		t.Fatalf("expected task to remain WrapUp (timer doesn't complete it), got %s", task.Status)
	}
}

func TestResolveWrapUpTimeout_NoopIfAlreadyCompleted(t *testing.T) {
	// Mirrors spec Section 5.4 rule 4's idempotent-safe-resolution
	// principle: a wrap-up timer notification racing against an already-
	// completed task must be a clean no-op, not a false reset.
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat", WrapUpTimeoutSeconds: 30})

	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("expected 1 match, got %d outcomes, err=%v", len(outcomes), err)
	}
	if _, err := s.AcceptReservation(ctx, tenant, outcomes[0].Reservation.ReservationID); err != nil {
		t.Fatalf("AcceptReservation failed: %v", err)
	}
	if _, err := s.EndTask(ctx, tenant, "task-1"); err != nil {
		t.Fatalf("EndTask failed: %v", err)
	}
	if _, err := s.CompleteTask(ctx, tenant, "task-1"); err != nil {
		t.Fatalf("CompleteTask failed: %v", err)
	}

	// Agent reset to Available by CompleteTask; simulate a racing/stale
	// wrap-up timeout notification arriving after completion.
	result, err := s.ResolveWrapUpTimeout(ctx, tenant, "task-1")
	if err != nil {
		t.Fatalf("ResolveWrapUpTimeout failed: %v", err)
	}
	if result.Resolved {
		t.Fatalf("expected no-op (task already Completed), got resolved=true")
	}
}

func TestStartWrapUpTimer_NoopWhenTimeoutZero(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	if err := s.StartWrapUpTimer(ctx, tenant, "task-1", 0); err != nil {
		t.Fatalf("expected no error for zero timeout, got %v", err)
	}
	// No assertion beyond "does not error" -- there is no sentinel key to
	// observe via this package's public API, and the point of this test is
	// exactly that 0 means "don't start a timer at all" (see
	// EnqueueTaskInput.WrapUpTimeoutSeconds's doc comment).
}
