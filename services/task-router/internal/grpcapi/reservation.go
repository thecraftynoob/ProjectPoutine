package grpcapi

import (
	"context"

	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/redisdomain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AcceptReservation implements spec Section 3.4's "Accept a Reservation".
// Rejected unless the reservation is currently Offered. Emits Reservation
// Accepted, then Task Accepted (spec Section 3.4's event ordering).
func (s *TaskRouterServer) AcceptReservation(ctx context.Context, req *taskrouterv1.AcceptReservationRequest) (*taskrouterv1.Reservation, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}

	result, err := s.Store.AcceptReservation(ctx, tid.String(), req.GetReservationId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "accept reservation: %v", err)
	}
	if !result.Resolved {
		if result.CurrentStatus == "" {
			return nil, status.Errorf(codes.NotFound, "reservation %q not found", req.GetReservationId())
		}
		return nil, status.Errorf(codes.FailedPrecondition, "reservation %q is not Offered (currently %s)", req.GetReservationId(), result.CurrentStatus)
	}

	if err := s.Events.ReservationAccepted(ctx, tid, req.GetReservationId(), result.TaskID, result.AgentID); err != nil {
		s.logger().Error("publish reservation accepted failed", "error", err)
	}
	if err := s.Events.TaskAccepted(ctx, tid, result.TaskID, result.AgentID); err != nil {
		s.logger().Error("publish task accepted failed", "error", err)
	}

	reservation, err := s.Store.GetReservation(ctx, tid.String(), req.GetReservationId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get reservation: %v", err)
	}
	return reservationToProto(reservation), nil
}

// RejectReservation implements spec Section 3.4's "Reject a Reservation".
// Rejected unless the reservation is currently Offered (race-safe
// re-check, spec Section 5.4 rule 4). Releases capacity, requeues the
// task at its original position, and sets the agent to "Not Responding"
// (spec Section 5.3).
func (s *TaskRouterServer) RejectReservation(ctx context.Context, req *taskrouterv1.RejectReservationRequest) (*taskrouterv1.Reservation, error) {
	tid, err := tenantID(ctx)
	if err != nil {
		return nil, err
	}

	result, err := s.Store.RejectReservation(ctx, tid.String(), req.GetReservationId(), redisdomain.ReasonAgentRejected)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reject reservation: %v", err)
	}
	if !result.Resolved {
		if result.CurrentStatus == "" {
			return nil, status.Errorf(codes.NotFound, "reservation %q not found", req.GetReservationId())
		}
		return nil, status.Errorf(codes.FailedPrecondition, "reservation %q is not Offered (currently %s)", req.GetReservationId(), result.CurrentStatus)
	}

	if err := s.Events.ReservationRejected(ctx, tid, req.GetReservationId(), result.TaskID, result.AgentID, string(redisdomain.ReasonAgentRejected)); err != nil {
		s.logger().Error("publish reservation rejected failed", "error", err)
	}

	// The requeued task is now eligible for re-matching (spec Section
	// 4.3's "the resolution event itself is what re-triggers the
	// matching pass").
	s.runMatchingPass(ctx, tid)

	reservation, err := s.Store.GetReservation(ctx, tid.String(), req.GetReservationId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get reservation: %v", err)
	}
	return reservationToProto(reservation), nil
}
