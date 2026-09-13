package redisdomain

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestStore spins up an in-memory miniredis instance (supports Lua/
// EVAL, per the standard Go approach for testing Redis Lua scripts
// without live infra) and returns a Store wired to it, plus a cleanup
// func.
func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	store := NewStore(client, 30*time.Second)
	tenantID := "11111111-1111-1111-1111-111111111111"
	return store, tenantID
}

func mustCreateAgent(t *testing.T, ctx context.Context, s *Store, tenant string, in CreateAgentInput) Agent {
	t.Helper()
	a, err := s.CreateAgent(ctx, tenant, in)
	if err != nil {
		t.Fatalf("CreateAgent(%q) failed: %v", in.AgentID, err)
	}
	return a
}

func mustEnqueueTask(t *testing.T, ctx context.Context, s *Store, tenant string, in EnqueueTaskInput) Task {
	t.Helper()
	task, err := s.EnqueueTask(ctx, tenant, in)
	if err != nil {
		t.Fatalf("EnqueueTask(%q) failed: %v", in.TaskID, err)
	}
	return task
}

func availableAgentInput(id string, queues []string, channel string, max int) CreateAgentInput {
	return CreateAgentInput{
		AgentID: id,
		Status:  StatusAvailable,
		Queues:  queues,
		Capacity: map[string]ChannelCapacity{
			channel: {Ready: true, Max: max, Active: 0, Interruptible: true},
		},
	}
}

// --- CreateAgent: uniqueness on create (spec Section 5.4 rule 1) ---

func TestCreateAgent_UniquenessOnCreate(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, CreateAgentInput{AgentID: "agent-1"})

	_, err := s.CreateAgent(ctx, tenant, CreateAgentInput{AgentID: "agent-1"})
	if err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists on duplicate create, got %v", err)
	}
}

func TestCreateAgent_DefaultsAndRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	created := mustCreateAgent(t, ctx, s, tenant, CreateAgentInput{
		AgentID: "agent-1",
		Queues:  []string{"q1"},
		Capacity: map[string]ChannelCapacity{
			"chat": {Ready: true, Max: 3, Interruptible: true},
		},
	})
	if created.Status != StatusOffline {
		t.Fatalf("expected default status %q, got %q", StatusOffline, created.Status)
	}

	got, err := s.GetAgent(ctx, tenant, "agent-1")
	if err != nil {
		t.Fatalf("GetAgent failed: %v", err)
	}
	if got.Capacity["chat"].Max != 3 || got.Capacity["chat"].Active != 0 {
		t.Fatalf("unexpected capacity round-trip: %+v", got.Capacity["chat"])
	}
	if len(got.Queues) != 1 || got.Queues[0] != "q1" {
		t.Fatalf("unexpected queues round-trip: %+v", got.Queues)
	}
}

// --- EnqueueTask: uniqueness + FIFO ---

func TestEnqueueTask_UniquenessOnCreate(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})

	_, err := s.EnqueueTask(ctx, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})
	if err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists on duplicate task create, got %v", err)
	}
}

func TestPendingTasksFIFO_StrictOrderByEnqueuedAt(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Insert out of order to prove ordering comes from enqueuedAt, not
	// insertion order.
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-c", QueueID: "q1", TaskType: "chat", EnqueuedAt: base.Add(2 * time.Second)})
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-a", QueueID: "q1", TaskType: "chat", EnqueuedAt: base})
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-b", QueueID: "q1", TaskType: "chat", EnqueuedAt: base.Add(1 * time.Second)})

	ids, err := s.PendingTasksFIFO(ctx, tenant)
	if err != nil {
		t.Fatalf("PendingTasksFIFO failed: %v", err)
	}
	want := []string{"task-a", "task-b", "task-c"}
	if len(ids) != len(want) {
		t.Fatalf("expected %d pending tasks, got %d: %v", len(want), len(ids), ids)
	}
	for i, id := range ids {
		if id != want[i] {
			t.Fatalf("FIFO order mismatch at index %d: got %q, want %q (full: %v)", i, id, want[i], ids)
		}
	}
}

// --- Matching pass: strict FIFO + first-eligible-agent-wins ---

func TestEvaluateOnce_StrictFIFOTaskSelection(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Only one agent, capacity for exactly one concurrent chat -- so only
	// the oldest task should be matched in one pass.
	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))

	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-newer", QueueID: "q1", TaskType: "chat", EnqueuedAt: base.Add(5 * time.Second)})
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-older", QueueID: "q1", TaskType: "chat", EnqueuedAt: base})

	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil {
		t.Fatalf("EvaluateOnce failed: %v", err)
	}
	if len(outcomes) != 1 {
		t.Fatalf("expected exactly 1 match, got %d: %+v", len(outcomes), outcomes)
	}
	if outcomes[0].Reservation.TaskID != "task-older" {
		t.Fatalf("expected the OLDER task to be matched first (strict FIFO), got %q", outcomes[0].Reservation.TaskID)
	}

	olderTask, err := s.GetTask(ctx, tenant, "task-older")
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}
	if olderTask.Status != TaskReserved {
		t.Fatalf("expected task-older to be Reserved, got %s", olderTask.Status)
	}

	newerTask, err := s.GetTask(ctx, tenant, "task-newer")
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}
	if newerTask.Status != TaskPending {
		t.Fatalf("expected task-newer to remain Pending (no capacity left), got %s", newerTask.Status)
	}
}

func TestEvaluateOnce_FirstEligibleAgentWins_NoTieBreak(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	// Two equally eligible agents; the algorithm has no tie-break rule,
	// so exactly one of them should win the single task, and the other
	// should remain untouched (active still 0).
	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-2", []string{"q1"}, "chat", 1))

	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})

	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil {
		t.Fatalf("EvaluateOnce failed: %v", err)
	}
	if len(outcomes) != 1 {
		t.Fatalf("expected exactly 1 match, got %d", len(outcomes))
	}
	winner := outcomes[0].Reservation.AgentID
	if winner != "agent-1" && winner != "agent-2" {
		t.Fatalf("unexpected winning agent %q", winner)
	}
	loser := "agent-2"
	if winner == "agent-2" {
		loser = "agent-1"
	}

	loserAgent, err := s.GetAgent(ctx, tenant, loser)
	if err != nil {
		t.Fatalf("GetAgent failed: %v", err)
	}
	if loserAgent.Capacity["chat"].Active != 0 {
		t.Fatalf("expected losing agent's capacity untouched, got active=%d", loserAgent.Capacity["chat"].Active)
	}

	// Only one task existed, so a second pass should find nothing new.
	outcomes2, err := s.EvaluateOnce(ctx, tenant)
	if err != nil {
		t.Fatalf("second EvaluateOnce failed: %v", err)
	}
	if len(outcomes2) != 0 {
		t.Fatalf("expected no further matches on second pass, got %d", len(outcomes2))
	}
}

// --- Capacity accounting through match -> accept -> complete ---

func TestCapacityLifecycle_MatchAcceptComplete(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})

	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("expected 1 match, got %d outcomes, err=%v", len(outcomes), err)
	}
	reservationID := outcomes[0].Reservation.ReservationID

	agentAfterMatch, _ := s.GetAgent(ctx, tenant, "agent-1")
	if agentAfterMatch.Capacity["chat"].Active != 1 {
		t.Fatalf("expected active=1 after match, got %d", agentAfterMatch.Capacity["chat"].Active)
	}

	acceptResult, err := s.AcceptReservation(ctx, tenant, reservationID)
	if err != nil || !acceptResult.Resolved {
		t.Fatalf("AcceptReservation failed: resolved=%v err=%v", acceptResult.Resolved, err)
	}

	agentAfterAccept, _ := s.GetAgent(ctx, tenant, "agent-1")
	if agentAfterAccept.Capacity["chat"].Active != 1 {
		t.Fatalf("expected active still 1 after accept (accept doesn't change capacity), got %d", agentAfterAccept.Capacity["chat"].Active)
	}

	task, _ := s.GetTask(ctx, tenant, "task-1")
	if task.Status != TaskActive {
		t.Fatalf("expected task Active after accept, got %s", task.Status)
	}

	completeResult, err := s.CompleteTask(ctx, tenant, "task-1")
	if err != nil {
		t.Fatalf("CompleteTask failed: %v", err)
	}
	if completeResult.AgentID != "agent-1" {
		t.Fatalf("expected agent-1 released capacity, got %q", completeResult.AgentID)
	}

	agentAfterComplete, _ := s.GetAgent(ctx, tenant, "agent-1")
	if agentAfterComplete.Capacity["chat"].Active != 0 {
		t.Fatalf("expected active=0 after complete, got %d", agentAfterComplete.Capacity["chat"].Active)
	}

	taskAfterComplete, _ := s.GetTask(ctx, tenant, "task-1")
	if taskAfterComplete.Status != TaskCompleted {
		t.Fatalf("expected task Completed, got %s", taskAfterComplete.Status)
	}
	if taskAfterComplete.AssignedAgentID != "agent-1" {
		t.Fatalf("expected assignedAgentId retained after completion (spec Section 2.2), got %q", taskAfterComplete.AssignedAgentID)
	}
}

// --- Capacity accounting through match -> reject cycle ---

func TestCapacityLifecycle_MatchRejectCycle(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat", EnqueuedAt: base})

	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("expected 1 match, got %d outcomes, err=%v", len(outcomes), err)
	}
	reservationID := outcomes[0].Reservation.ReservationID

	rejectResult, err := s.RejectReservation(ctx, tenant, reservationID, ReasonAgentRejected)
	if err != nil || !rejectResult.Resolved {
		t.Fatalf("RejectReservation failed: resolved=%v err=%v", rejectResult.Resolved, err)
	}

	agentAfterReject, _ := s.GetAgent(ctx, tenant, "agent-1")
	if agentAfterReject.Capacity["chat"].Active != 0 {
		t.Fatalf("expected active=0 after reject, got %d", agentAfterReject.Capacity["chat"].Active)
	}
	if agentAfterReject.Status != StatusNotResponding {
		t.Fatalf("expected agent status Not Responding after reject, got %q", agentAfterReject.Status)
	}

	task, _ := s.GetTask(ctx, tenant, "task-1")
	if task.Status != TaskPending {
		t.Fatalf("expected task back to Pending after reject, got %s", task.Status)
	}
	if !task.EnqueuedAt.Equal(base) {
		t.Fatalf("expected original enqueuedAt preserved, got %v want %v", task.EnqueuedAt, base)
	}

	ids, err := s.PendingTasksFIFO(ctx, tenant)
	if err != nil {
		t.Fatalf("PendingTasksFIFO failed: %v", err)
	}
	if len(ids) != 1 || ids[0] != "task-1" {
		t.Fatalf("expected task-1 back in pending FIFO set, got %v", ids)
	}
}

// TestRejectReservation_IdempotentSafe covers spec Section 5.4 rule 4: a
// second resolution attempt on an already-resolved reservation must
// return a distinguishable "already resolved" result, not double-apply.
func TestRejectReservation_IdempotentSafe(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})

	outcomes, _ := s.EvaluateOnce(ctx, tenant)
	reservationID := outcomes[0].Reservation.ReservationID

	first, err := s.RejectReservation(ctx, tenant, reservationID, ReasonAgentRejected)
	if err != nil || !first.Resolved {
		t.Fatalf("first reject failed: resolved=%v err=%v", first.Resolved, err)
	}

	second, err := s.RejectReservation(ctx, tenant, reservationID, ReasonExpired)
	if err != nil {
		t.Fatalf("second reject call errored: %v", err)
	}
	if second.Resolved {
		t.Fatal("expected second reject to report Resolved=false (already resolved)")
	}
	if second.CurrentStatus != ReservationRejected {
		t.Fatalf("expected CurrentStatus=Rejected, got %q", second.CurrentStatus)
	}

	// Capacity must not have been double-released.
	agent, _ := s.GetAgent(ctx, tenant, "agent-1")
	if agent.Capacity["chat"].Active != 0 {
		t.Fatalf("expected active=0 (floored, not negative), got %d", agent.Capacity["chat"].Active)
	}
}

// TestAcceptReservation_AlreadyRejected covers the accept-vs-reject
// mutual exclusion half of spec Section 5.4 rule 4.
func TestAcceptReservation_AlreadyRejected(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})
	outcomes, _ := s.EvaluateOnce(ctx, tenant)
	reservationID := outcomes[0].Reservation.ReservationID

	if _, err := s.RejectReservation(ctx, tenant, reservationID, ReasonAgentRejected); err != nil {
		t.Fatalf("reject failed: %v", err)
	}

	acceptResult, err := s.AcceptReservation(ctx, tenant, reservationID)
	if err != nil {
		t.Fatalf("accept call errored: %v", err)
	}
	if acceptResult.Resolved {
		t.Fatal("expected accept on an already-Rejected reservation to report Resolved=false")
	}
	if acceptResult.CurrentStatus != ReservationRejected {
		t.Fatalf("expected CurrentStatus=Rejected, got %q", acceptResult.CurrentStatus)
	}
}

// --- Agent deletion with in-flight task (spec Section 5.5) ---

func TestDeleteAgent_WithReservedTask(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat", EnqueuedAt: base})

	outcomes, _ := s.EvaluateOnce(ctx, tenant)
	if len(outcomes) != 1 {
		t.Fatalf("expected 1 match, got %d", len(outcomes))
	}
	reservationID := outcomes[0].Reservation.ReservationID

	result, err := s.DeleteAgent(ctx, tenant, "agent-1")
	if err != nil {
		t.Fatalf("DeleteAgent failed: %v", err)
	}
	if !result.Existed {
		t.Fatal("expected Existed=true")
	}
	if len(result.Resolved) != 1 {
		t.Fatalf("expected exactly 1 resolved reservation, got %d", len(result.Resolved))
	}
	if result.Resolved[0].ReservationID != reservationID {
		t.Fatalf("expected resolved reservation %q, got %q", reservationID, result.Resolved[0].ReservationID)
	}
	if result.Resolved[0].TaskID != "task-1" {
		t.Fatalf("expected resolved task task-1, got %q", result.Resolved[0].TaskID)
	}

	task, err := s.GetTask(ctx, tenant, "task-1")
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}
	if task.Status != TaskPending {
		t.Fatalf("expected task back to Pending, got %s", task.Status)
	}
	if !task.EnqueuedAt.Equal(base) {
		t.Fatalf("expected original enqueuedAt preserved on deletion-triggered requeue, got %v want %v", task.EnqueuedAt, base)
	}
	if task.AssignedAgentID != "" {
		t.Fatalf("expected assignedAgentId cleared, got %q", task.AssignedAgentID)
	}

	reservation, err := s.GetReservation(ctx, tenant, reservationID)
	if err != nil {
		t.Fatalf("GetReservation failed: %v", err)
	}
	if reservation.Status != ReservationRejected {
		t.Fatalf("expected reservation Rejected, got %s", reservation.Status)
	}
	if reservation.Reason != ReasonAgentDeleted {
		t.Fatalf("expected reason agent_deleted, got %q", reservation.Reason)
	}

	exists, err := s.AgentExists(ctx, tenant, "agent-1")
	if err != nil {
		t.Fatalf("AgentExists failed: %v", err)
	}
	if exists {
		t.Fatal("expected agent to no longer exist")
	}
}

func TestDeleteAgent_WithAcceptedActiveTask(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})
	outcomes, _ := s.EvaluateOnce(ctx, tenant)
	reservationID := outcomes[0].Reservation.ReservationID

	acceptResult, err := s.AcceptReservation(ctx, tenant, reservationID)
	if err != nil || !acceptResult.Resolved {
		t.Fatalf("accept failed: resolved=%v err=%v", acceptResult.Resolved, err)
	}

	// Task is now Active (not merely Reserved) -- deletion must still
	// find and resolve it via the agent's in-flight reservations set.
	result, err := s.DeleteAgent(ctx, tenant, "agent-1")
	if err != nil {
		t.Fatalf("DeleteAgent failed: %v", err)
	}
	if len(result.Resolved) != 1 || result.Resolved[0].ReservationID != reservationID {
		t.Fatalf("expected resolved reservation %q for an Active task, got %+v", reservationID, result.Resolved)
	}

	task, _ := s.GetTask(ctx, tenant, "task-1")
	if task.Status != TaskPending {
		t.Fatalf("expected Active task to return to Pending on agent deletion, got %s", task.Status)
	}

	reservation, _ := s.GetReservation(ctx, tenant, reservationID)
	if reservation.Status != ReservationRejected || reservation.Reason != ReasonAgentDeleted {
		t.Fatalf("expected Accepted reservation to be marked Rejected/agent_deleted on deletion, got status=%s reason=%s", reservation.Status, reservation.Reason)
	}
}

func TestDeleteAgent_NoInFlightWork(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))

	result, err := s.DeleteAgent(ctx, tenant, "agent-1")
	if err != nil {
		t.Fatalf("DeleteAgent failed: %v", err)
	}
	if !result.Existed {
		t.Fatal("expected Existed=true")
	}
	if len(result.Resolved) != 0 {
		t.Fatalf("expected no resolved reservations, got %+v", result.Resolved)
	}
}

func TestDeleteAgent_NotFound(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	result, err := s.DeleteAgent(ctx, tenant, "does-not-exist")
	if err != nil {
		t.Fatalf("DeleteAgent failed: %v", err)
	}
	if result.Existed {
		t.Fatal("expected Existed=false for a nonexistent agent")
	}
}

// --- Capacity replacement rules (spec Section 4.2) ---

func TestReplaceAgentCapacity_PreservesActiveForRetainedChannel(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})
	outcomes, _ := s.EvaluateOnce(ctx, tenant)
	if len(outcomes) != 1 {
		t.Fatalf("expected 1 match, got %d", len(outcomes))
	}

	// Replace capacity, retaining "chat" with a higher max, and adding a
	// new "voice" channel.
	err := s.ReplaceAgentCapacity(ctx, tenant, "agent-1", map[string]ChannelCapacity{
		"chat":  {Ready: true, Max: 5, Interruptible: true},
		"voice": {Ready: true, Max: 2, Interruptible: false},
	})
	if err != nil {
		t.Fatalf("ReplaceAgentCapacity failed: %v", err)
	}

	agent, _ := s.GetAgent(ctx, tenant, "agent-1")
	if agent.Capacity["chat"].Active != 1 {
		t.Fatalf("expected retained channel's active preserved at 1, got %d", agent.Capacity["chat"].Active)
	}
	if agent.Capacity["chat"].Max != 5 {
		t.Fatalf("expected retained channel's max updated to 5, got %d", agent.Capacity["chat"].Max)
	}
	if agent.Capacity["voice"].Active != 0 {
		t.Fatalf("expected new channel's active initialized to 0, got %d", agent.Capacity["voice"].Active)
	}
}

func TestReplaceAgentCapacity_RemovesOmittedChannel(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, CreateAgentInput{
		AgentID: "agent-1",
		Status:  StatusAvailable,
		Capacity: map[string]ChannelCapacity{
			"chat":  {Ready: true, Max: 1},
			"voice": {Ready: true, Max: 1},
		},
	})

	err := s.ReplaceAgentCapacity(ctx, tenant, "agent-1", map[string]ChannelCapacity{
		"chat": {Ready: true, Max: 1},
	})
	if err != nil {
		t.Fatalf("ReplaceAgentCapacity failed: %v", err)
	}

	agent, _ := s.GetAgent(ctx, tenant, "agent-1")
	if _, ok := agent.Capacity["voice"]; ok {
		t.Fatal("expected omitted channel 'voice' to be removed")
	}
	if _, ok := agent.Capacity["chat"]; !ok {
		t.Fatal("expected retained channel 'chat' to remain")
	}
}

func TestReplaceAgentCapacity_LoweringMaxBelowActiveDoesNotEvict(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 5))
	for i := 0; i < 3; i++ {
		taskID := "task-" + string(rune('a'+i))
		mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: taskID, QueueID: "q1", TaskType: "chat"})
	}
	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil || len(outcomes) != 3 {
		t.Fatalf("expected 3 matches, got %d, err=%v", len(outcomes), err)
	}

	// Lower max below current active (3) -- allowed, no eviction.
	err = s.ReplaceAgentCapacity(ctx, tenant, "agent-1", map[string]ChannelCapacity{
		"chat": {Ready: true, Max: 1},
	})
	if err != nil {
		t.Fatalf("ReplaceAgentCapacity failed: %v", err)
	}

	agent, _ := s.GetAgent(ctx, tenant, "agent-1")
	if agent.Capacity["chat"].Active != 3 {
		t.Fatalf("expected active still 3 (not evicted), got %d", agent.Capacity["chat"].Active)
	}
	if agent.Capacity["chat"].Max != 1 {
		t.Fatalf("expected max lowered to 1, got %d", agent.Capacity["chat"].Max)
	}

	// New matches should now be blocked since active(3) >= max(1).
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-new", QueueID: "q1", TaskType: "chat"})
	outcomes2, err := s.EvaluateOnce(ctx, tenant)
	if err != nil {
		t.Fatalf("EvaluateOnce failed: %v", err)
	}
	if len(outcomes2) != 0 {
		t.Fatalf("expected no new matches while over capacity, got %d", len(outcomes2))
	}
}

// --- ToggleChannelReady creates channel with defaults if absent ---

func TestToggleChannelReady_CreatesChannelWithDefaultsIfAbsent(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, CreateAgentInput{AgentID: "agent-1", Status: StatusAvailable})

	if err := s.ToggleChannelReady(ctx, tenant, "agent-1", "chat", false); err != nil {
		t.Fatalf("ToggleChannelReady failed: %v", err)
	}

	agent, _ := s.GetAgent(ctx, tenant, "agent-1")
	cap, ok := agent.Capacity["chat"]
	if !ok {
		t.Fatal("expected channel 'chat' to be created")
	}
	if cap.Max != DefaultChannelMax {
		t.Fatalf("expected default max=%d, got %d", DefaultChannelMax, cap.Max)
	}
	if !cap.Interruptible {
		t.Fatal("expected default interruptible=true")
	}
	if cap.Ready {
		t.Fatal("expected ready=false as explicitly set")
	}
}
