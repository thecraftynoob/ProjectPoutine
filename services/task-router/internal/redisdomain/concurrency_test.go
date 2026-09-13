package redisdomain

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"
)

// TestMatchCommit_ConcurrentAttemptsExactlyOneSucceeds covers spec
// Section 5.4 rules 3 and 6: two concurrent match-commit attempts for the
// SAME agent/task pair (simulating two Task Router replicas racing to
// commit the same match, e.g. after both independently scanned the same
// pre-commit state) must result in exactly one success, since
// match_commit.lua re-validates task.status=="Pending" atomically at
// commit time and a single Redis instance serializes Lua script
// execution. Run with `go test -race` to additionally prove the Go-level
// concurrency here (goroutines + shared *redis.Client) has no data races.
func TestMatchCommit_ConcurrentAttemptsExactlyOneSucceeds(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})

	const attempts = 20
	var wg sync.WaitGroup
	results := make([]MatchAttemptResult, attempts)
	errs := make([]error, attempts)

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reservationID, err := s.NewReservationID(ctx, tenant)
			if err != nil {
				errs[i] = err
				return
			}
			res, err := s.MatchCommit(ctx, tenant, "task-1", "agent-1", reservationID)
			results[i] = res
			errs[i] = err
		}(i)
	}
	wg.Wait()

	successCount := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("attempt %d: unexpected error: %v", i, err)
		}
		if results[i].Committed {
			successCount++
		}
	}

	if successCount != 1 {
		t.Fatalf("expected exactly 1 successful commit out of %d concurrent attempts, got %d", attempts, successCount)
	}

	agent, err := s.GetAgent(ctx, tenant, "agent-1")
	if err != nil {
		t.Fatalf("GetAgent failed: %v", err)
	}
	if agent.Capacity["chat"].Active != 1 {
		t.Fatalf("expected active=1 after exactly one commit (never double-incremented), got %d", agent.Capacity["chat"].Active)
	}

	task, err := s.GetTask(ctx, tenant, "task-1")
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}
	if task.Status != TaskReserved {
		t.Fatalf("expected task Reserved, got %s", task.Status)
	}
}

// TestEvaluateOnce_ConcurrentPassesNeverDoubleBook covers the same rule
// via the higher-level EvaluateOnce entrypoint (as multiple Task Router
// replicas would actually invoke it): running two full matching passes
// concurrently against one agent with capacity for exactly one task must
// never result in that agent's capacity exceeding its max.
func TestEvaluateOnce_ConcurrentPassesNeverDoubleBook(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 1))
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat"})

	const passes = 10
	var wg sync.WaitGroup
	totalMatches := make([]int, passes)

	for i := 0; i < passes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outcomes, err := s.EvaluateOnce(ctx, tenant)
			if err != nil {
				t.Errorf("pass %d: EvaluateOnce failed: %v", i, err)
				return
			}
			totalMatches[i] = len(outcomes)
		}(i)
	}
	wg.Wait()

	sum := 0
	for _, n := range totalMatches {
		sum += n
	}
	if sum != 1 {
		t.Fatalf("expected exactly 1 match across all concurrent passes, got %d", sum)
	}

	agent, err := s.GetAgent(ctx, tenant, "agent-1")
	if err != nil {
		t.Fatalf("GetAgent failed: %v", err)
	}
	if agent.Capacity["chat"].Active != 1 {
		t.Fatalf("expected active=1 (never exceeded max via double-booking), got %d", agent.Capacity["chat"].Active)
	}
}

// --- Multi-concurrent-reservation-per-agent bookkeeping (spec Sections
// 2.1, 3.2, 4.2: ChannelCapacity.max is a CONCURRENT limit, so one agent
// can legitimately hold several simultaneous Offered/Accepted
// reservations at once) ---

// TestDeleteAgent_MultipleConcurrentReservations proves the fix for the
// bug where delete_agent.lua's fallback path (a single scalar
// "activeReservationId" field) could find at most one Accepted/Active
// reservation, silently orphaning any others when an agent legitimately
// held more than one at a time. Three tasks are matched to one agent
// whose "chat" channel has max=3 (all three committed concurrently via
// the normal EvaluateOnce/MatchCommit path), then a mix of Offered and
// Accepted states is created (one left Offered, two Accepted) before
// deleting the agent. Every one of the three tasks/reservations must be
// correctly resolved -- none silently lost.
func TestDeleteAgent_MultipleConcurrentReservations(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 3))

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	taskIDs := []string{"task-1", "task-2", "task-3"}
	for i, id := range taskIDs {
		mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{
			TaskID:     id,
			QueueID:    "q1",
			TaskType:   "chat",
			EnqueuedAt: base.Add(time.Duration(i) * time.Second),
		})
	}

	// Match all three tasks to the same agent via the normal matching
	// path -- EvaluateOnce loops pending tasks FIFO and, for max=3 on one
	// channel, all three should commit to agent-1 in a single pass.
	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil {
		t.Fatalf("EvaluateOnce failed: %v", err)
	}
	if len(outcomes) != 3 {
		t.Fatalf("expected all 3 tasks matched to agent-1 (max=3), got %d", len(outcomes))
	}

	agent, err := s.GetAgent(ctx, tenant, "agent-1")
	if err != nil {
		t.Fatalf("GetAgent failed: %v", err)
	}
	if agent.Capacity["chat"].Active != 3 {
		t.Fatalf("expected active=3 after 3 concurrent commits, got %d", agent.Capacity["chat"].Active)
	}

	// Map taskId -> reservationId for the assertions below.
	reservationByTask := make(map[string]string, 3)
	for _, o := range outcomes {
		reservationByTask[o.Reservation.TaskID] = o.Reservation.ReservationID
	}
	if len(reservationByTask) != 3 {
		t.Fatalf("expected 3 distinct task/reservation pairs, got %d", len(reservationByTask))
	}

	// Mix of Offered and Accepted: accept task-1 and task-2's
	// reservations, leave task-3's reservation Offered.
	for _, taskID := range []string{"task-1", "task-2"} {
		rid := reservationByTask[taskID]
		acceptResult, err := s.AcceptReservation(ctx, tenant, rid)
		if err != nil || !acceptResult.Resolved {
			t.Fatalf("accept %s failed: resolved=%v err=%v", rid, acceptResult.Resolved, err)
		}
	}

	// Sanity: task-3's reservation is still Offered (not Accepted).
	stillOffered, err := s.GetReservation(ctx, tenant, reservationByTask["task-3"])
	if err != nil {
		t.Fatalf("GetReservation(task-3's reservation) failed: %v", err)
	}
	if stillOffered.Status != ReservationOffered {
		t.Fatalf("expected task-3's reservation still Offered, got %s", stillOffered.Status)
	}

	// Now delete the agent. ALL THREE in-flight reservations (two
	// Accepted, one Offered) must be resolved -- not just one.
	result, err := s.DeleteAgent(ctx, tenant, "agent-1")
	if err != nil {
		t.Fatalf("DeleteAgent failed: %v", err)
	}
	if !result.Existed {
		t.Fatal("expected Existed=true")
	}
	if len(result.Resolved) != 3 {
		t.Fatalf("expected all 3 in-flight reservations reported resolved by deletion, got %d: %+v", len(result.Resolved), result.Resolved)
	}

	resolvedReservationIDs := make(map[string]string, 3) // reservationId -> taskId
	for _, r := range result.Resolved {
		resolvedReservationIDs[r.ReservationID] = r.TaskID
	}
	for taskID, rid := range reservationByTask {
		gotTaskID, ok := resolvedReservationIDs[rid]
		if !ok {
			t.Fatalf("reservation %q (task %q) missing from DeleteAgent's resolved list: %+v", rid, taskID, result.Resolved)
		}
		if gotTaskID != taskID {
			t.Fatalf("resolved reservation %q reports taskId %q, want %q", rid, gotTaskID, taskID)
		}
	}

	// Every task must be back to Pending at its ORIGINAL enqueuedAt, not
	// lost and not stuck Reserved/Active.
	for i, taskID := range taskIDs {
		task, err := s.GetTask(ctx, tenant, taskID)
		if err != nil {
			t.Fatalf("GetTask(%s) failed: %v", taskID, err)
		}
		if task.Status != TaskPending {
			t.Fatalf("expected %s back to Pending after agent deletion, got %s", taskID, task.Status)
		}
		wantEnqueuedAt := base.Add(time.Duration(i) * time.Second)
		if !task.EnqueuedAt.Equal(wantEnqueuedAt) {
			t.Fatalf("expected %s's original enqueuedAt preserved, got %v want %v", taskID, task.EnqueuedAt, wantEnqueuedAt)
		}
		if task.AssignedAgentID != "" {
			t.Fatalf("expected %s's assignedAgentId cleared, got %q", taskID, task.AssignedAgentID)
		}
	}

	// Every reservation must be marked Rejected/agent_deleted.
	for taskID, rid := range reservationByTask {
		reservation, err := s.GetReservation(ctx, tenant, rid)
		if err != nil {
			t.Fatalf("GetReservation(%s, task %s) failed: %v", rid, taskID, err)
		}
		if reservation.Status != ReservationRejected {
			t.Fatalf("expected reservation %s (task %s) Rejected, got %s", rid, taskID, reservation.Status)
		}
		if reservation.Reason != ReasonAgentDeleted {
			t.Fatalf("expected reservation %s (task %s) reason agent_deleted, got %q", rid, taskID, reservation.Reason)
		}
	}

	// Pending tasks should now be eligible for re-matching, all three back
	// in FIFO order.
	pending, err := s.PendingTasksFIFO(ctx, tenant)
	if err != nil {
		t.Fatalf("PendingTasksFIFO failed: %v", err)
	}
	sortedPending := append([]string(nil), pending...)
	sort.Strings(sortedPending)
	wantPending := append([]string(nil), taskIDs...)
	sort.Strings(wantPending)
	if len(sortedPending) != len(wantPending) {
		t.Fatalf("expected all 3 tasks pending, got %v", pending)
	}
	for i := range sortedPending {
		if sortedPending[i] != wantPending[i] {
			t.Fatalf("pending set mismatch: got %v want (unordered) %v", pending, wantPending)
		}
	}
}

// TestCompleteTask_DoesNotCorruptSiblingReservationBookkeeping is a
// regression test for the second half of the bug: completing one of an
// agent's several concurrent Active tasks used to unconditionally blank a
// shared scalar bookkeeping field, silently orphaning the agent's OTHER
// still-Active task so a later deletion could no longer find/resolve it.
// This proves that completing one concurrent task leaves the other's
// trackability intact, by completing one of two Accepted/Active
// reservations and then confirming deletion still correctly finds and
// resolves the other.
func TestCompleteTask_DoesNotCorruptSiblingReservationBookkeeping(t *testing.T) {
	ctx := context.Background()
	s, tenant := newTestStore(t)

	mustCreateAgent(t, ctx, s, tenant, availableAgentInput("agent-1", []string{"q1"}, "chat", 2))

	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-1", QueueID: "q1", TaskType: "chat", EnqueuedAt: base})
	mustEnqueueTask(t, ctx, s, tenant, EnqueueTaskInput{TaskID: "task-2", QueueID: "q1", TaskType: "chat", EnqueuedAt: base.Add(time.Second)})

	outcomes, err := s.EvaluateOnce(ctx, tenant)
	if err != nil {
		t.Fatalf("EvaluateOnce failed: %v", err)
	}
	if len(outcomes) != 2 {
		t.Fatalf("expected both tasks matched to agent-1 (max=2), got %d", len(outcomes))
	}
	reservationByTask := make(map[string]string, 2)
	for _, o := range outcomes {
		reservationByTask[o.Reservation.TaskID] = o.Reservation.ReservationID
	}

	// Accept both -> both tasks Active, both reservations Accepted.
	for _, taskID := range []string{"task-1", "task-2"} {
		rid := reservationByTask[taskID]
		acceptResult, err := s.AcceptReservation(ctx, tenant, rid)
		if err != nil || !acceptResult.Resolved {
			t.Fatalf("accept %s failed: resolved=%v err=%v", rid, acceptResult.Resolved, err)
		}
	}

	// Complete task-1 only. This must NOT disturb task-2's bookkeeping.
	completeResult, err := s.CompleteTask(ctx, tenant, "task-1")
	if err != nil {
		t.Fatalf("CompleteTask(task-1) failed: %v", err)
	}
	if completeResult.AgentID != "agent-1" {
		t.Fatalf("expected CompleteTask to report agent-1, got %q", completeResult.AgentID)
	}

	task1, err := s.GetTask(ctx, tenant, "task-1")
	if err != nil {
		t.Fatalf("GetTask(task-1) failed: %v", err)
	}
	if task1.Status != TaskCompleted {
		t.Fatalf("expected task-1 Completed, got %s", task1.Status)
	}

	// Capacity should have been released by exactly 1 (task-1's slot),
	// leaving task-2's slot still consumed.
	agent, err := s.GetAgent(ctx, tenant, "agent-1")
	if err != nil {
		t.Fatalf("GetAgent failed: %v", err)
	}
	if agent.Capacity["chat"].Active != 1 {
		t.Fatalf("expected active=1 after completing 1 of 2 concurrent tasks, got %d", agent.Capacity["chat"].Active)
	}

	// task-2 must still be Active and its reservation still Accepted --
	// untouched by task-1's completion.
	task2, err := s.GetTask(ctx, tenant, "task-2")
	if err != nil {
		t.Fatalf("GetTask(task-2) failed: %v", err)
	}
	if task2.Status != TaskActive {
		t.Fatalf("expected task-2 still Active after sibling task-1 completed, got %s", task2.Status)
	}
	task2ReservationID := reservationByTask["task-2"]
	task2Reservation, err := s.GetReservation(ctx, tenant, task2ReservationID)
	if err != nil {
		t.Fatalf("GetReservation(task-2's reservation) failed: %v", err)
	}
	if task2Reservation.Status != ReservationAccepted {
		t.Fatalf("expected task-2's reservation still Accepted after sibling task-1 completed, got %s", task2Reservation.Status)
	}

	// The critical regression check: delete the agent now. task-2's
	// still-Active reservation must be correctly found (via the agent's
	// in-flight set, which task-1's completion must NOT have corrupted)
	// and resolved -- exactly one resolution, for task-2, not zero.
	result, err := s.DeleteAgent(ctx, tenant, "agent-1")
	if err != nil {
		t.Fatalf("DeleteAgent failed: %v", err)
	}
	if len(result.Resolved) != 1 {
		t.Fatalf("expected exactly 1 resolved reservation (task-2's) on deletion, got %d: %+v", len(result.Resolved), result.Resolved)
	}
	if result.Resolved[0].ReservationID != task2ReservationID {
		t.Fatalf("expected resolved reservation %q (task-2's), got %q", task2ReservationID, result.Resolved[0].ReservationID)
	}
	if result.Resolved[0].TaskID != "task-2" {
		t.Fatalf("expected resolved task task-2, got %q", result.Resolved[0].TaskID)
	}

	task2AfterDelete, err := s.GetTask(ctx, tenant, "task-2")
	if err != nil {
		t.Fatalf("GetTask(task-2) after deletion failed: %v", err)
	}
	if task2AfterDelete.Status != TaskPending {
		t.Fatalf("expected task-2 back to Pending after agent deletion, got %s", task2AfterDelete.Status)
	}
	if !task2AfterDelete.EnqueuedAt.Equal(base.Add(time.Second)) {
		t.Fatalf("expected task-2's original enqueuedAt preserved, got %v want %v", task2AfterDelete.EnqueuedAt, base.Add(time.Second))
	}

	task2ReservationAfterDelete, err := s.GetReservation(ctx, tenant, task2ReservationID)
	if err != nil {
		t.Fatalf("GetReservation(task-2's reservation) after deletion failed: %v", err)
	}
	if task2ReservationAfterDelete.Status != ReservationRejected || task2ReservationAfterDelete.Reason != ReasonAgentDeleted {
		t.Fatalf("expected task-2's reservation Rejected/agent_deleted after deletion, got status=%s reason=%s",
			task2ReservationAfterDelete.Status, task2ReservationAfterDelete.Reason)
	}

	// task-1 (already Completed before the deletion) must remain
	// Completed and untouched by the deletion.
	task1AfterDelete, err := s.GetTask(ctx, tenant, "task-1")
	if err != nil {
		t.Fatalf("GetTask(task-1) after deletion failed: %v", err)
	}
	if task1AfterDelete.Status != TaskCompleted {
		t.Fatalf("expected task-1 to remain Completed after agent deletion, got %s", task1AfterDelete.Status)
	}
}
