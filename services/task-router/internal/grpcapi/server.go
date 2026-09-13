package grpcapi

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	"github.com/thecraftynoob/ProjectPoutine/pkg/tenantctx"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/events"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/pgconfig"
	"github.com/thecraftynoob/ProjectPoutine/services/task-router/internal/redisdomain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TaskRouterServer implements taskrouterv1.TaskRouterServiceServer: the
// hot-path domain (spec Sections 3.2-3.4, 3.7).
type TaskRouterServer struct {
	taskrouterv1.UnimplementedTaskRouterServiceServer

	Store    *redisdomain.Store
	Registry *pgconfig.Registry
	Events   *events.Publisher
	Logger   *slog.Logger
}

func (s *TaskRouterServer) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// tenantID extracts the caller's tenant ID from gRPC metadata
// (pkg/tenantctx), the only place tenant scoping travels per architecture
// doc Section 1.1. Returned as uuid.UUID for event-publishing call sites;
// Redis/Postgres repository call sites take tid.String() since Redis keys
// and this package's redisdomain.Store API are string-keyed.
func tenantID(ctx context.Context) (uuid.UUID, error) {
	id, err := tenantctx.TenantID(ctx)
	if err != nil {
		return uuid.UUID{}, status.Error(codes.Unauthenticated, err.Error())
	}
	return id, nil
}

// runMatchingPass invokes the in-process matching pass (spec Section 4.1
// evaluate_once(), architectural decision: in-process synchronous trigger
// after every mutation that could plausibly create a new matching
// opportunity) and publishes a Reservation Created event for every
// resulting match (spec Section 6.2). Errors are logged, not propagated
// to the RPC caller, since the triggering mutation has already
// successfully committed by the time this runs -- a matching-pass failure
// must not roll back or fail the RPC that triggered it; the next
// triggering event will simply retry unmatched tasks.
func (s *TaskRouterServer) runMatchingPass(ctx context.Context, tid uuid.UUID) {
	outcomes, err := s.Store.EvaluateOnce(ctx, tid.String())
	if err != nil {
		s.logger().Error("matching pass failed", slog.String("tenant_id", tid.String()), slog.Any("error", err))
		return
	}
	for _, o := range outcomes {
		expiresAt := ""
		if o.Reservation.ExpiresAt != nil {
			expiresAt = o.Reservation.ExpiresAt.Format("2006-01-02T15:04:05.999999999Z07:00")
		}
		if err := s.Events.ReservationCreated(ctx, tid, o.Reservation.ReservationID, o.Reservation.TaskID, o.Reservation.AgentID, expiresAt); err != nil {
			s.logger().Error("publish reservation created failed", slog.Any("error", err))
		}
	}
}
