package redisdomain

import "time"

// AttributeValue is the Go-side mirror of the proto AttributeValue oneof
// (spec Sections 2.1, 2.2, 2.6): exactly one of NumberValue/BoolValue is
// meaningful, discriminated by IsBool.
type AttributeValue struct {
	IsBool      bool
	BoolValue   bool
	NumberValue float64
}

// ChannelCapacity mirrors spec Section 2.1's ChannelCapacity table.
type ChannelCapacity struct {
	Ready         bool `json:"ready"`
	Max           int  `json:"max"`
	Active        int  `json:"active"`
	Interruptible bool `json:"interruptible"`
}

// Agent mirrors spec Section 2.1, plus UserID (a platform-layer addition
// not in the spec -- see proto/task-router/v1/task_router.proto's
// Agent.user_id doc comment for the full rationale and scope).
type Agent struct {
	AgentID         string                     `json:"agentId"`
	Status          string                     `json:"status"`
	Attributes      map[string]AttributeValue  `json:"attributes"`
	Capacity        map[string]ChannelCapacity `json:"capacity"`
	Queues          []string                   `json:"queues"`
	StatusChangedAt time.Time                  `json:"statusChangedAt"`
	UserID          string                     `json:"userId,omitempty"`
}

// TaskStatus enumerates spec Section 5.1's Task lifecycle states.
type TaskStatus string

const (
	TaskPending   TaskStatus = "Pending"
	TaskReserved  TaskStatus = "Reserved"
	TaskActive    TaskStatus = "Active"
	TaskCompleted TaskStatus = "Completed"
)

// Task mirrors spec Section 2.2.
type Task struct {
	TaskID                string                    `json:"taskId"`
	QueueID               string                    `json:"queueId"`
	TaskType              string                    `json:"taskType"`
	RequiredAttributes    map[string]AttributeValue `json:"requiredAttributes"`
	EnqueuedAt            time.Time                 `json:"enqueuedAt"`
	Status                TaskStatus                `json:"status"`
	CurrentReservationID  string                    `json:"currentReservationId"`
	AssignedAgentID       string                    `json:"assignedAgentId"`
}

// ReservationStatus enumerates spec Section 5.2's Reservation lifecycle
// states.
type ReservationStatus string

const (
	ReservationOffered  ReservationStatus = "Offered"
	ReservationAccepted ReservationStatus = "Accepted"
	ReservationRejected ReservationStatus = "Rejected"
)

// RejectReason distinguishes the three triggers that can resolve an
// Offered reservation to Rejected (spec Sections 5.2, 6.2).
type RejectReason string

const (
	ReasonAgentRejected RejectReason = "agent_rejected"
	ReasonExpired       RejectReason = "expired"
	ReasonAgentDeleted  RejectReason = "agent_deleted"
)

// Reservation mirrors spec Section 2.3.
type Reservation struct {
	ReservationID string            `json:"reservationId"`
	TaskID        string            `json:"taskId"`
	AgentID       string            `json:"agentId"`
	Status        ReservationStatus `json:"status"`
	CreatedAt     time.Time         `json:"createdAt"`
	ExpiresAt     *time.Time        `json:"expiresAt"`
	// Reason is only meaningful once Status == Rejected.
	Reason RejectReason `json:"reason,omitempty"`
}

// DefaultReservationTTL is the spec Section 2.3 default TTL applied at
// match time: expiresAt = matchedAt + TTL.
const DefaultReservationTTL = 30 * time.Second

// StatusAvailable is the one status value with matching significance
// (spec Sections 4.1, 5.3).
const StatusAvailable = "Available"

// StatusNotResponding is the system-assigned status applied as a side
// effect of any reservation-reject transition (spec Section 5.3). Never
// applied as a side effect of agent deletion.
const StatusNotResponding = "Not Responding"

// StatusOffline is the default status for a newly created agent whose
// caller did not specify one (spec Section 2.1).
const StatusOffline = "Offline"

// DefaultChannelMax is ChannelCapacity.Max's default (spec Section 2.1).
const DefaultChannelMax = 1
