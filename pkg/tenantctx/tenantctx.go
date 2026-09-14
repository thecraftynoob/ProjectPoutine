// Package tenantctx implements Layer 2 (service-level) multi-tenant
// enforcement per CCAAS_ENTERPRISE_ARCHITECTURE.md Section 1.1.
//
// Every internal gRPC service re-validates tenant_id itself rather than
// trusting the API Gateway blindly. This package provides gRPC
// interceptors (server-side: extract + verify + store in context;
// client-side: inject on outgoing calls) so that enforcement is applied
// uniformly rather than re-implemented per handler.
//
// Tenant identity is derived from a real, signature-verified JWT (see
// pkg/jwtauth) carried as standard gRPC metadata:
//
//	authorization: Bearer <jwt>
//
// The `x-tenant-id` metadata key still exists as a constant (MetadataKey)
// and the client-side Inject* helpers still attach it, purely for
// logging/observability convenience on the receiving end (e.g. so a log
// line or trace span can cheaply show the tenant without decoding the
// JWT) -- but it is NEVER trusted as a source of truth for authorization.
// Every server-side interceptor in this package derives tenant_id
// exclusively from the verified token's `tid` claim.
package tenantctx

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MetadataKey is the gRPC metadata key that carries the tenant ID for
// logging/observability purposes only -- see the package doc comment.
// Never trusted for authorization.
const MetadataKey = "x-tenant-id"

// AuthorizationMetadataKey is the standard gRPC metadata key carrying the
// bearer token: "authorization: Bearer <jwt>". This is the sole source of
// truth for tenant identity on every non-exempt RPC.
const AuthorizationMetadataKey = "authorization"

// bearerPrefix is the required prefix of the authorization metadata
// value, matched case-insensitively per RFC 6750/ HTTP convention.
const bearerPrefix = "Bearer "

// healthCheckMethod is the full gRPC method name of the standard
// grpc.health.v1.Health/Check RPC (see /pkg/health). Kubernetes liveness/
// readiness probes and other infra tooling call this without any tenant
// context, so it is deliberately exempt from tenant enforcement -- it is
// not tenant-scoped data, just "is the process up." Every other RPC on
// every service remains subject to the Layer 2 enforcement below.
const healthCheckMethod = "/grpc.health.v1.Health/Check"

// healthWatchMethod is the streaming counterpart to healthCheckMethod,
// exempted from tenant enforcement for the same reason.
const healthWatchMethod = "/grpc.health.v1.Health/Watch"

// unexported context key types so this package's context values can never
// collide with keys set by other packages -- or with each other: two
// distinct named types are used (rather than two instances of one shared
// empty-struct type, which would compare equal as context keys and let
// one WithValue call silently overwrite the other).
type tenantIDKey struct{}
type claimsKey struct{}

var tenantIDContextKey = tenantIDKey{}
var claimsContextKey = claimsKey{}

// WithTenantID returns a new context carrying the given tenant ID. Intended
// for tests and for any code path that establishes tenant scope outside of
// the gRPC interceptor (e.g. constructing a context for an internal call).
func WithTenantID(ctx context.Context, tenantID uuid.UUID) context.Context {
	return context.WithValue(ctx, tenantIDContextKey, tenantID)
}

// TenantID retrieves the tenant ID previously stored in ctx by
// WithTenantID or the server interceptor in this package. It returns an
// error if no tenant ID is present.
func TenantID(ctx context.Context) (uuid.UUID, error) {
	v := ctx.Value(tenantIDContextKey)
	if v == nil {
		return uuid.UUID{}, fmt.Errorf("tenantctx: no tenant id in context")
	}
	id, ok := v.(uuid.UUID)
	if !ok {
		return uuid.UUID{}, fmt.Errorf("tenantctx: unexpected value type in context")
	}
	return id, nil
}

// withClaims returns a new context carrying the full verified Claims, so
// handlers that need more than tenant_id (e.g. Subject/Roles) can get at
// them without re-verifying the token.
func withClaims(ctx context.Context, claims jwtauth.Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey, claims)
}

// Claims retrieves the full verified JWT claims previously stored in ctx
// by the server interceptor in this package. Returns an error if no
// claims are present (e.g. in a context built only via WithTenantID, or
// on an exempt method that never verified a token).
func Claims(ctx context.Context) (jwtauth.Claims, error) {
	v := ctx.Value(claimsContextKey)
	if v == nil {
		return jwtauth.Claims{}, fmt.Errorf("tenantctx: no verified claims in context")
	}
	claims, ok := v.(jwtauth.Claims)
	if !ok {
		return jwtauth.Claims{}, fmt.Errorf("tenantctx: unexpected value type in context")
	}
	return claims, nil
}

// verifyBearerToken reads and verifies the authorization metadata on an
// incoming gRPC context, returning the decoded Claims. Returns a gRPC
// Unauthenticated status error on any failure so callers can return it
// directly.
func verifyBearerToken(ctx context.Context, verifier *jwtauth.Verifier) (jwtauth.Claims, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return jwtauth.Claims{}, status.Error(codes.Unauthenticated, "tenantctx: missing gRPC metadata")
	}
	values := md.Get(AuthorizationMetadataKey)
	if len(values) == 0 || values[0] == "" {
		return jwtauth.Claims{}, status.Errorf(codes.Unauthenticated, "tenantctx: missing %s metadata", AuthorizationMetadataKey)
	}
	raw := values[0]
	if len(raw) <= len(bearerPrefix) || !strings.EqualFold(raw[:len(bearerPrefix)], bearerPrefix) {
		return jwtauth.Claims{}, status.Errorf(codes.Unauthenticated, "tenantctx: %s metadata must be \"Bearer <token>\"", AuthorizationMetadataKey)
	}
	token := raw[len(bearerPrefix):]

	claims, err := verifier.Verify(token)
	if err != nil {
		return jwtauth.Claims{}, status.Errorf(codes.Unauthenticated, "tenantctx: invalid token: %v", err)
	}
	return claims, nil
}

// UnaryServerInterceptor validates a bearer JWT on every incoming unary RPC
// and stores the verified tenant ID (and full claims) in the handler's
// context. Requests with a missing, malformed, expired, or wrongly-signed
// token are rejected with codes.Unauthenticated before reaching the
// handler.
//
// verifier is required and must be non-nil: a nil verifier is a
// programming/deployment error, and this is a security control, so it
// fails closed -- every non-exempt call is rejected with
// codes.Internal rather than silently admitting unauthenticated traffic.
// (Server construction should prefer failing fast at startup instead --
// see each service's cmd/main.go, which refuses to start if it cannot
// load a verifier -- this is a defense-in-depth backstop, not the primary
// mechanism.)
//
// exemptMethods lists additional full gRPC method names (e.g.
// "/tenantidentity.v1.IdentityService/Login") to exempt from tenant
// enforcement, beyond the always-exempt health check. This exists for
// services like Tenant & Identity Management that have a small number of
// RPCs which are themselves how a caller FIRST establishes tenant/token
// context (CreateTenant, Login, IssueServiceToken, ...) and therefore
// cannot require that context to already exist -- see that service's
// proto doc comment and cmd/main.go for the concrete list. Every other
// service in this repo passes no exemptions.
func UnaryServerInterceptor(verifier *jwtauth.Verifier, exemptMethods ...string) grpc.UnaryServerInterceptor {
	exempt := exemptSet(exemptMethods)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == healthCheckMethod || exempt[info.FullMethod] {
			return handler(ctx, req)
		}
		if verifier == nil {
			return nil, status.Error(codes.Internal, "tenantctx: server misconfigured, no JWT verifier available")
		}
		claims, err := verifyBearerToken(ctx, verifier)
		if err != nil {
			return nil, err
		}
		ctx = WithTenantID(ctx, claims.TenantID)
		ctx = withClaims(ctx, claims)
		return handler(ctx, req)
	}
}

// exemptSet builds a lookup set from a full-method-name list, so the
// interceptor's hot path is a map lookup rather than a linear scan.
func exemptSet(methods []string) map[string]bool {
	if len(methods) == 0 {
		return nil
	}
	set := make(map[string]bool, len(methods))
	for _, m := range methods {
		set[m] = true
	}
	return set
}

// wrappedServerStream carries a replacement context (with tenant ID set)
// through a streaming RPC, since grpc.ServerStream does not allow directly
// swapping its context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// StreamServerInterceptor is the streaming-RPC equivalent of
// UnaryServerInterceptor: verifies the bearer JWT before the stream
// handler runs, and makes the tenant ID / claims available via
// TenantID(stream.Context()) / Claims(stream.Context()). See
// UnaryServerInterceptor for verifier and exemptMethods' semantics
// (verifier is required; a nil verifier fails closed).
func StreamServerInterceptor(verifier *jwtauth.Verifier, exemptMethods ...string) grpc.StreamServerInterceptor {
	exempt := exemptSet(exemptMethods)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod == healthWatchMethod || exempt[info.FullMethod] {
			return handler(srv, ss)
		}
		if verifier == nil {
			return status.Error(codes.Internal, "tenantctx: server misconfigured, no JWT verifier available")
		}
		claims, err := verifyBearerToken(ss.Context(), verifier)
		if err != nil {
			return err
		}
		ctx := WithTenantID(ss.Context(), claims.TenantID)
		ctx = withClaims(ctx, claims)
		return handler(srv, &wrappedServerStream{
			ServerStream: ss,
			ctx:          ctx,
		})
	}
}

// InjectTenantID returns a client-side unary interceptor that attaches
// tenantID as x-tenant-id gRPC metadata on every outgoing call, purely
// for logging/observability on the receiving end (see package doc
// comment) -- it is NOT a substitute for attaching a bearer token. Use
// alongside pkg/jwtauth's client-side token-attaching interceptor, not
// instead of it.
func InjectTenantID(tenantID uuid.UUID) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, MetadataKey, tenantID.String())
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// InjectTenantIDStream is the streaming-RPC equivalent of InjectTenantID.
func InjectTenantIDStream(tenantID uuid.UUID) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, MetadataKey, tenantID.String())
		return streamer(ctx, desc, cc, method, opts...)
	}
}
