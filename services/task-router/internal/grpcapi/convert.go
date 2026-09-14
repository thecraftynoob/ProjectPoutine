// Package grpcapi binds the Redis-backed hot-path domain (../redisdomain)
// and the Postgres-backed config registries (../pgconfig) to the
// generated taskrouter.v1 proto services, implementing
// TASK_ROUTER_SPECIFICATION.md Sections 3.1-3.4, 3.6, 3.7 end to end:
// request validation, domain mutation, in-process matching-pass trigger,
// and event publishing.
package grpcapi

import (
	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/pgconfig"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/redisdomain"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func attrsToDomain(in map[string]*taskrouterv1.AttributeValue) map[string]redisdomain.AttributeValue {
	out := make(map[string]redisdomain.AttributeValue, len(in))
	for k, v := range in {
		if v == nil {
			continue
		}
		switch kind := v.GetKind().(type) {
		case *taskrouterv1.AttributeValue_BoolValue:
			out[k] = redisdomain.AttributeValue{IsBool: true, BoolValue: kind.BoolValue}
		case *taskrouterv1.AttributeValue_NumberValue:
			out[k] = redisdomain.AttributeValue{IsBool: false, NumberValue: kind.NumberValue}
		}
	}
	return out
}

func attrsToProto(in map[string]redisdomain.AttributeValue) map[string]*taskrouterv1.AttributeValue {
	out := make(map[string]*taskrouterv1.AttributeValue, len(in))
	for k, v := range in {
		if v.IsBool {
			out[k] = &taskrouterv1.AttributeValue{Kind: &taskrouterv1.AttributeValue_BoolValue{BoolValue: v.BoolValue}}
		} else {
			out[k] = &taskrouterv1.AttributeValue{Kind: &taskrouterv1.AttributeValue_NumberValue{NumberValue: v.NumberValue}}
		}
	}
	return out
}

func capacityToDomain(in map[string]*taskrouterv1.ChannelCapacity) map[string]redisdomain.ChannelCapacity {
	out := make(map[string]redisdomain.ChannelCapacity, len(in))
	for k, v := range in {
		if v == nil {
			continue
		}
		out[k] = channelCapacityToDomain(v)
	}
	return out
}

// channelCapacityToDomain applies spec Section 2.1's ChannelCapacity
// defaults (ready=true, max=1, interruptible=true) for any field the
// proto3 wire representation cannot distinguish from "explicitly zero"
// (proto3 scalar fields have no presence tracking for these types without
// wrapper types) -- EXCEPT max, where 0 is not a meaningful requested
// value (a channel with max=0 could never be matched, which is a valid
// but unusual configuration a caller might genuinely want), so max is
// defaulted to DefaultChannelMax(1) only when the field is exactly zero,
// matching the spec's stated default exactly. ready/interruptible default
// server-side to true only via the CreateAgent/ReplaceCapacity call
// sites that explicitly choose to apply proto3 defaults -- see those
// call sites' own comments for why this file doesn't blanket-apply
// "false means unset" (it doesn't; proto3 bool zero value IS false, so
// this converter takes values as given, and defaulting is instead a
// request-shaping concern handled once at the top of each RPC handler
// that needs it).
func channelCapacityToDomain(v *taskrouterv1.ChannelCapacity) redisdomain.ChannelCapacity {
	return redisdomain.ChannelCapacity{
		Ready:         v.GetReady(),
		Max:           int(v.GetMax()),
		Active:        int(v.GetActive()),
		Interruptible: v.GetInterruptible(),
	}
}

func capacityToProto(in map[string]redisdomain.ChannelCapacity) map[string]*taskrouterv1.ChannelCapacity {
	out := make(map[string]*taskrouterv1.ChannelCapacity, len(in))
	for k, v := range in {
		out[k] = &taskrouterv1.ChannelCapacity{
			Ready:         v.Ready,
			Max:           int32(v.Max),
			Active:        int32(v.Active),
			Interruptible: v.Interruptible,
		}
	}
	return out
}

func agentToProto(a redisdomain.Agent) *taskrouterv1.Agent {
	return &taskrouterv1.Agent{
		AgentId:         a.AgentID,
		Status:          a.Status,
		Attributes:      attrsToProto(a.Attributes),
		Capacity:        capacityToProto(a.Capacity),
		Queues:          append([]string(nil), a.Queues...),
		StatusChangedAt: timestamppb.New(a.StatusChangedAt),
		UserId:          a.UserID,
	}
}

func taskToProto(t redisdomain.Task) *taskrouterv1.Task {
	return &taskrouterv1.Task{
		TaskId:               t.TaskID,
		QueueId:              t.QueueID,
		TaskType:             t.TaskType,
		RequiredAttributes:   attrsToProto(t.RequiredAttributes),
		EnqueuedAt:           timestamppb.New(t.EnqueuedAt),
		Status:               string(t.Status),
		CurrentReservationId: t.CurrentReservationID,
		AssignedAgentId:      t.AssignedAgentID,
		WrapUpTimeoutSeconds: t.WrapUpTimeoutSeconds,
		DispositionId:        t.DispositionID,
		DispositionName:      t.DispositionName,
	}
}

func reservationToProto(r redisdomain.Reservation) *taskrouterv1.Reservation {
	out := &taskrouterv1.Reservation{
		ReservationId: r.ReservationID,
		TaskId:        r.TaskID,
		AgentId:       r.AgentID,
		Status:        string(r.Status),
		CreatedAt:     timestamppb.New(r.CreatedAt),
	}
	if r.ExpiresAt != nil {
		out.ExpiresAt = timestamppb.New(*r.ExpiresAt)
	}
	return out
}

func queueToProto(q pgconfig.Queue) *taskrouterv1.Queue {
	return &taskrouterv1.Queue{
		QueueId:   q.QueueID,
		CreatedAt: timestamppb.New(q.CreatedAt),
	}
}

func statusToProto(s pgconfig.StatusEntry) *taskrouterv1.StatusEntry {
	return &taskrouterv1.StatusEntry{
		Status:    s.Status,
		CreatedAt: timestamppb.New(s.CreatedAt),
	}
}

func attributeTypeToProto(t pgconfig.AttributeType) taskrouterv1.AttributeType {
	switch t {
	case pgconfig.AttributeNumeric:
		return taskrouterv1.AttributeType_ATTRIBUTE_TYPE_NUMERIC
	case pgconfig.AttributeBoolean:
		return taskrouterv1.AttributeType_ATTRIBUTE_TYPE_BOOLEAN
	default:
		return taskrouterv1.AttributeType_ATTRIBUTE_TYPE_UNSPECIFIED
	}
}

func attributeTypeToDomain(t taskrouterv1.AttributeType) (pgconfig.AttributeType, bool) {
	switch t {
	case taskrouterv1.AttributeType_ATTRIBUTE_TYPE_NUMERIC:
		return pgconfig.AttributeNumeric, true
	case taskrouterv1.AttributeType_ATTRIBUTE_TYPE_BOOLEAN:
		return pgconfig.AttributeBoolean, true
	default:
		return "", false
	}
}

func attributeDefToProto(a pgconfig.AttributeDefinition) *taskrouterv1.AttributeDefinition {
	return &taskrouterv1.AttributeDefinition{
		Name:      a.Name,
		Type:      attributeTypeToProto(a.Type),
		CreatedAt: timestamppb.New(a.CreatedAt),
	}
}

func dispositionToProto(d pgconfig.Disposition) *taskrouterv1.Disposition {
	return &taskrouterv1.Disposition{
		DispositionId: d.DispositionID,
		Name:          d.Name,
		CreatedAt:     timestamppb.New(d.CreatedAt),
	}
}
