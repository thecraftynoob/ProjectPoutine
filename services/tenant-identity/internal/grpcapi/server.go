// Package grpcapi implements tenantidentity.v1's TenantService and
// IdentityService gRPC contracts over internal/pgstore and internal/authn.
package grpcapi

import (
	"log/slog"

	tenantidentityv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/tenant-identity/v1"
	"github.com/thecraftynoob/ProjectPoutine/services/tenant-identity/internal/authn"
	"github.com/thecraftynoob/ProjectPoutine/services/tenant-identity/internal/pgstore"
)

// TenantServer implements tenantidentityv1.TenantServiceServer: CRUD over
// the platform tenant registry (no tenant context required -- see the
// proto's file-level note).
type TenantServer struct {
	tenantidentityv1.UnimplementedTenantServiceServer

	Tenants *pgstore.TenantStore
	Logger  *slog.Logger
}

func (s *TenantServer) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// IdentityServer implements tenantidentityv1.IdentityServiceServer: user/
// agent identity, RBAC role tags, and password-login JWT issuance.
type IdentityServer struct {
	tenantidentityv1.UnimplementedIdentityServiceServer

	Tenants *pgstore.TenantStore
	Users   *pgstore.UserStore
	Tokens  *authn.TokenIssuer
	Logger  *slog.Logger
}

func (s *IdentityServer) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}
