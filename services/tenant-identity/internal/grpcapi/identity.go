package grpcapi

import (
	"context"

	"github.com/google/uuid"
	tenantidentityv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/tenant-identity/v1"
	"github.com/thecraftynoob/ProjectPoutine/pkg/tenantctx"
	"github.com/thecraftynoob/ProjectPoutine/services/tenant-identity/internal/authn"
	"github.com/thecraftynoob/ProjectPoutine/services/tenant-identity/internal/pgstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CreateUser provisions a new user/agent identity record within a
// tenant. tenant_id is an explicit request field (not x-tenant-id
// metadata) -- see the proto's file-level note: this RPC must be
// reachable to bootstrap a brand new tenant's very first user, before any
// admin token for that tenant can possibly exist yet.
func (s *IdentityServer) CreateUser(ctx context.Context, req *tenantidentityv1.CreateUserRequest) (*tenantidentityv1.User, error) {
	tid, err := uuid.Parse(req.GetTenantId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid tenant_id: %v", err)
	}
	if req.GetUsername() == "" {
		return nil, status.Error(codes.InvalidArgument, "username is required")
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "password is required")
	}

	exists, err := s.Tenants.TenantExists(ctx, tid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check tenant exists: %v", err)
	}
	if !exists {
		return nil, status.Errorf(codes.NotFound, "tenant %q not found", req.GetTenantId())
	}

	hash, err := authn.HashPassword(req.GetPassword())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "hash password: %v", err)
	}

	roles := req.GetRoles()
	if roles == nil {
		roles = []string{}
	}

	user, err := s.Users.CreateUser(ctx, tid, req.GetUsername(), hash, roles)
	if err == pgstore.ErrAlreadyExists {
		return nil, status.Errorf(codes.AlreadyExists, "username %q already exists in this tenant", req.GetUsername())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create user: %v", err)
	}
	return userToProto(user), nil
}

// Login verifies username+password within the given tenant and mints a
// short-lived JWT on success. tenant_id is an explicit request field --
// the caller has no token yet; this call is how one is obtained.
//
// Deliberately returns the same generic Unauthenticated error whether the
// tenant doesn't exist, the username doesn't exist, or the password is
// wrong -- never distinguishing "user not found" from "wrong password" to
// a caller, which would let an attacker enumerate valid usernames.
func (s *IdentityServer) Login(ctx context.Context, req *tenantidentityv1.LoginRequest) (*tenantidentityv1.LoginResponse, error) {
	const invalidCredentials = "invalid tenant, username, or password"

	tid, err := uuid.Parse(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, invalidCredentials)
	}

	user, err := s.Users.GetUserByUsername(ctx, tid, req.GetUsername())
	if err == pgstore.ErrNotFound {
		return nil, status.Error(codes.Unauthenticated, invalidCredentials)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "look up user: %v", err)
	}

	if !authn.VerifyPassword(user.PasswordHash, req.GetPassword()) {
		return nil, status.Error(codes.Unauthenticated, invalidCredentials)
	}

	token, expiresAt, err := s.Tokens.IssueToken(tid, user.UserID.String(), user.Roles)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "issue token: %v", err)
	}

	return &tenantidentityv1.LoginResponse{
		Token:     token,
		ExpiresAt: timestamppb.New(expiresAt),
	}, nil
}

// GetUser retrieves one user within the caller's already-established
// tenant context (x-tenant-id metadata, validated by pkg/tenantctx --
// note this RPC is still transport-exempted from that interceptor in
// this milestone, see cmd/main.go, but the handler itself still requires
// the metadata to be present and uses it to scope the lookup).
func (s *IdentityServer) GetUser(ctx context.Context, req *tenantidentityv1.GetUserRequest) (*tenantidentityv1.User, error) {
	tid, err := tenantctx.TenantID(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	uid, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user_id: %v", err)
	}
	user, err := s.Users.GetUser(ctx, tid, uid)
	if err == pgstore.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "user %q not found", req.GetUserId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get user: %v", err)
	}
	return userToProto(user), nil
}

// ListUsers enumerates every user within the caller's tenant context
// (x-tenant-id metadata).
func (s *IdentityServer) ListUsers(ctx context.Context, _ *tenantidentityv1.ListUsersRequest) (*tenantidentityv1.ListUsersResponse, error) {
	tid, err := tenantctx.TenantID(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	users, err := s.Users.ListUsers(ctx, tid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list users: %v", err)
	}
	out := make([]*tenantidentityv1.User, len(users))
	for i, u := range users {
		out[i] = userToProto(u)
	}
	return &tenantidentityv1.ListUsersResponse{Users: out}, nil
}
