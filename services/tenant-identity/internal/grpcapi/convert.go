package grpcapi

import (
	"time"

	tenantidentityv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/tenant-identity/v1"
	"github.com/thecraftynoob/ProjectPoutine/services/tenant-identity/internal/pgstore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func tenantToProto(t pgstore.Tenant) *tenantidentityv1.Tenant {
	return &tenantidentityv1.Tenant{
		TenantId:  t.TenantID.String(),
		Name:      t.Name,
		CreatedAt: timestampProto(t.CreatedAt),
	}
}

// userToProto converts a pgstore.User to its proto representation.
// PasswordHash is deliberately never copied over -- it must never leave
// this service.
func userToProto(u pgstore.User) *tenantidentityv1.User {
	return &tenantidentityv1.User{
		UserId:    u.UserID.String(),
		TenantId:  u.TenantID.String(),
		Username:  u.Username,
		Roles:     u.Roles,
		CreatedAt: timestampProto(u.CreatedAt),
	}
}

func timestampProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
