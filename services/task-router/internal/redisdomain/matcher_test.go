package redisdomain

import "testing"

func baseAgent() Agent {
	return Agent{
		AgentID: "agent-1",
		Status:  StatusAvailable,
		Queues:  []string{"queue-1"},
		Capacity: map[string]ChannelCapacity{
			"chat": {Ready: true, Max: 1, Active: 0, Interruptible: true},
		},
	}
}

func baseTask() Task {
	return Task{
		TaskID:   "task-1",
		QueueID:  "queue-1",
		TaskType: "chat",
		Status:   TaskPending,
	}
}

// TestAgentCanTake_Gate1_Presence covers spec Section 4.1's Gate 1:
// agent.status != "Available" => false, for every non-Available value,
// including the special-cased "Not Responding".
func TestAgentCanTake_Gate1_Presence(t *testing.T) {
	cases := []string{"Offline", "Break", "Not Responding", "", "available", "AVAILABLE"}
	for _, status := range cases {
		agent := baseAgent()
		agent.Status = status
		if AgentCanTake(agent, baseTask()) {
			t.Errorf("status %q: expected agentCanTake=false, got true", status)
		}
	}
}

func TestAgentCanTake_Gate1_AvailablePasses(t *testing.T) {
	agent := baseAgent()
	agent.Status = StatusAvailable
	if !AgentCanTake(agent, baseTask()) {
		t.Fatal("expected agentCanTake=true for a fully-eligible Available agent")
	}
}

// TestAgentCanTake_Gate2a_QueueMembership covers spec Section 4.1's Gate
// 2a: task.queueId NOT IN agent.queues => false.
func TestAgentCanTake_Gate2a_QueueMembership(t *testing.T) {
	agent := baseAgent()
	agent.Queues = []string{"other-queue"}
	if AgentCanTake(agent, baseTask()) {
		t.Fatal("expected agentCanTake=false when agent is not a member of the task's queue")
	}

	agent.Queues = nil
	if AgentCanTake(agent, baseTask()) {
		t.Fatal("expected agentCanTake=false with empty queue membership")
	}

	agent.Queues = []string{"queue-1", "other-queue"}
	if !AgentCanTake(agent, baseTask()) {
		t.Fatal("expected agentCanTake=true when queue membership includes the task's queue among others")
	}
}

// TestAgentCanTake_Gate2b_ChannelMissing covers spec Section 4.1's Gate
// 2b: channel does not exist => false.
func TestAgentCanTake_Gate2b_ChannelMissing(t *testing.T) {
	agent := baseAgent()
	agent.Capacity = map[string]ChannelCapacity{
		"voice": {Ready: true, Max: 5, Active: 0},
	}
	if AgentCanTake(agent, baseTask()) {
		t.Fatal("expected agentCanTake=false when agent has no capacity entry for the task's type")
	}
}

// TestAgentCanTake_Gate2c_CapacityHeadroom covers spec Section 4.1's Gate
// 2c: channel.active < channel.max.
func TestAgentCanTake_Gate2c_CapacityHeadroom(t *testing.T) {
	agent := baseAgent()
	agent.Capacity["chat"] = ChannelCapacity{Ready: true, Max: 2, Active: 2}
	if AgentCanTake(agent, baseTask()) {
		t.Fatal("expected agentCanTake=false when active == max (no headroom)")
	}

	agent.Capacity["chat"] = ChannelCapacity{Ready: true, Max: 2, Active: 1}
	if !AgentCanTake(agent, baseTask()) {
		t.Fatal("expected agentCanTake=true when active < max")
	}

	agent.Capacity["chat"] = ChannelCapacity{Ready: true, Max: 1, Active: 5}
	if AgentCanTake(agent, baseTask()) {
		t.Fatal("expected agentCanTake=false when active > max (over-capacity, e.g. after lowering max)")
	}
}

// TestAgentCanTake_IgnoresReadyAndInterruptible confirms spec Section
// 4.1's explicit statement that ready/interruptible are never read by
// agent_can_take (spec Section 7.3 gap -- deliberately not implemented).
func TestAgentCanTake_IgnoresReadyAndInterruptible(t *testing.T) {
	agent := baseAgent()
	agent.Capacity["chat"] = ChannelCapacity{Ready: false, Max: 1, Active: 0, Interruptible: false}
	if !AgentCanTake(agent, baseTask()) {
		t.Fatal("expected agentCanTake=true even when ready=false and interruptible=false, per spec Section 4.1/7.3")
	}
}

// TestAgentCanTake_IgnoresAttributes confirms spec Section 4.1's explicit
// statement that no comparison of task.requiredAttributes to
// agent.attributes occurs (spec Section 7.2 gap -- deliberately not
// implemented).
func TestAgentCanTake_IgnoresAttributes(t *testing.T) {
	agent := baseAgent()
	agent.Attributes = map[string]AttributeValue{"language": {IsBool: false, NumberValue: 1}}
	task := baseTask()
	task.RequiredAttributes = map[string]AttributeValue{"language": {IsBool: false, NumberValue: 99}}
	if !AgentCanTake(agent, task) {
		t.Fatal("expected agentCanTake=true regardless of attribute mismatch, per spec Section 4.1/7.2")
	}
}
