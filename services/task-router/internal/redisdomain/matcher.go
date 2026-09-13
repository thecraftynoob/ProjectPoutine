package redisdomain

import (
	"context"
)

// MatchOutcome is one successful match produced by a matching pass, for
// the caller (grpcapi/events layer) to publish a Reservation Created
// event for.
type MatchOutcome struct {
	Reservation Reservation
}

// EvaluateOnce implements spec Section 4.1's evaluate_once() exactly, as
// the literal full re-scan-of-all-pending-tasks semantics the spec
// describes (default chosen over a narrower targeted-rescan optimization
// per the production scoping decision -- "default to implementing it
// exactly that way unless you have a strong correctness reason not to"):
//
//	pending_tasks = all tasks with status "Pending", ordered oldest
//	                enqueuedAt first
//	all_agents = every agent, in no defined/guaranteed order
//
//	FOR EACH task IN pending_tasks:            # strict FIFO by enqueuedAt
//	    FOR EACH agent IN all_agents:          # no ordering guarantee
//	        IF NOT agent_can_take(agent, task):
//	            CONTINUE
//	        attempt = atomically_commit_match(agent, task)
//	        IF attempt failed:
//	            CONTINUE                        # try the next agent
//	        record the new reservation
//	        BREAK                               # this task is claimed
//
// agentCanTake pre-filtering here is a scan-time optimization only (spec
// Section 4.1: "does not change *which* agent is chosen, only whether a
// chosen candidate's match is honored") -- MatchCommit re-validates every
// condition atomically at commit time regardless (spec Section 5.4 rule
// 3), so a stale/incorrect scan-time filter can never cause an invalid
// match, only a missed opportunity that the next triggering event will
// retry.
//
// This is invoked in-process, synchronously, by every mutating gRPC
// handler whose mutation could plausibly create a new matching
// opportunity (spec Section 6.3's "any event -> re-scan everything"
// consumption contract, satisfied here without a redundant NATS-
// consuming internal trigger per the architectural decision).
func (s *Store) EvaluateOnce(ctx context.Context, tenantID string) ([]MatchOutcome, error) {
	taskIDs, err := s.PendingTasksFIFO(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if len(taskIDs) == 0 {
		return nil, nil
	}

	agents, err := s.ListAgents(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if len(agents) == 0 {
		return nil, nil
	}

	var outcomes []MatchOutcome

	for _, taskID := range taskIDs {
		task, err := s.GetTask(ctx, tenantID, taskID)
		if err == ErrNotFound {
			// Resolved by a concurrent operation since the scan began;
			// move on to the next task.
			continue
		}
		if err != nil {
			return outcomes, err
		}
		if task.Status != TaskPending {
			// Scan-time staleness; commit-time re-validation in
			// MatchCommit is the real gate, but skip the obviously-stale
			// case here to avoid pointless script invocations.
			continue
		}

		for _, agent := range agents {
			if !agentCanTake(agent, task) {
				continue
			}

			reservationID, err := s.NewReservationID(ctx, tenantID)
			if err != nil {
				return outcomes, err
			}
			attempt, err := s.MatchCommit(ctx, tenantID, task.TaskID, agent.AgentID, reservationID)
			if err != nil {
				return outcomes, err
			}
			if !attempt.Committed {
				// Re-validation refused it (state changed since the
				// scan) -- try the next agent for this same task, exactly
				// per spec Section 4.1.
				continue
			}

			outcomes = append(outcomes, MatchOutcome{Reservation: attempt.Reservation})
			break // this task is now claimed; move to next task
		}
	}

	return outcomes, nil
}

// agentCanTake implements spec Section 4.1's agent_can_take exactly:
//
//	FUNCTION agent_can_take(agent, task):
//	    IF agent.status != "Available":
//	        RETURN false                          # Gate 1: Presence
//	    IF task.queueId NOT IN agent.queues:
//	        RETURN false                          # Gate 2a: Queue membership
//	    channel = agent.capacity[task.taskType]
//	    IF channel does not exist:
//	        RETURN false                          # Gate 2b: Channel capability
//	    RETURN channel.active < channel.max        # Gate 2c: Capacity headroom
//
// No check against channel.ready, no check against channel.interruptible,
// and no comparison of task.requiredAttributes to agent.attributes occurs
// here, deliberately mirroring the spec's stated scope (Section 7.2,
// 7.3 gaps are explicitly out of scope for this build).
func agentCanTake(agent Agent, task Task) bool {
	if agent.Status != StatusAvailable {
		return false
	}
	if !containsString(agent.Queues, task.QueueID) {
		return false
	}
	channel, ok := agent.Capacity[task.TaskType]
	if !ok {
		return false
	}
	return channel.Active < channel.Max
}

func containsString(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// AgentCanTake exports agentCanTake for direct unit testing of the four
// sub-checks without needing a live Store.
func AgentCanTake(agent Agent, task Task) bool {
	return agentCanTake(agent, task)
}
