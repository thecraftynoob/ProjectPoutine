package grpcapi

import (
	"context"

	"github.com/google/uuid"
	tenantidentityv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/tenant-identity/v1"
	"github.com/thecraftynoob/ProjectPoutine/services/tenant-identity/internal/pgstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateTenant registers a new tenant in the platform registry. This is
// how a caller obtains a real tenant_id in the first place -- no tenant
// context is required or possible here.
func (s *TenantServer) CreateTenant(ctx context.Context, req *tenantidentityv1.CreateTenantRequest) (*tenantidentityv1.Tenant, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	tenant, err := s.Tenants.CreateTenant(ctx, req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create tenant: %v", err)
	}
	return tenantToProto(tenant), nil
}

// GetTenant retrieves one tenant by ID.
func (s *TenantServer) GetTenant(ctx context.Context, req *tenantidentityv1.GetTenantRequest) (*tenantidentityv1.Tenant, error) {
	tid, err := uuid.Parse(req.GetTenantId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid tenant_id: %v", err)
	}
	tenant, err := s.Tenants.GetTenant(ctx, tid)
	if err == pgstore.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "tenant %q not found", req.GetTenantId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get tenant: %v", err)
	}
	return tenantToProto(tenant), nil
}

// ListTenants enumerates every registered tenant.
func (s *TenantServer) ListTenants(ctx context.Context, _ *tenantidentityv1.ListTenantsRequest) (*tenantidentityv1.ListTenantsResponse, error) {
	tenants, err := s.Tenants.ListTenants(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list tenants: %v", err)
	}
	out := make([]*tenantidentityv1.Tenant, len(tenants))
	for i, t := range tenants {
		out[i] = tenantToProto(t)
	}
	return &tenantidentityv1.ListTenantsResponse{Tenants: out}, nil
}
