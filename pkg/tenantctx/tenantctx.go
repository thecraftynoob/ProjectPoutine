// Package tenantctx implements Layer 2 (service-level) multi-tenant
// enforcement per CCAAS_ENTERPRISE_ARCHITECTURE.md Section 1.1.
//
// Every internal gRPC service re-validates tenant_id itself rather than
// trusting the API Gateway blindly. This package provides gRPC
// interceptors (server-side: extract + validate + store in context;
// client-side: inject on outgoing calls) so that enforcement is applied
// uniformly rather than re-implemented per handler.
package tenantctx

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MetadataKey is the gRPC metadata key that carries the tenant ID, per
// architecture doc Section 1.1 ("x-tenant-id").
const MetadataKey = "x-tenant-id"

// healthCheckMethod is the full gRPC method name of the standard
// grpc.health.v1.Health/Check RPC (see /pkg/health). Kubernetes liveness/
// readiness probes and other infra tooling call this without any tenant
// context, so it is deliberately exempt from tenant enforcement — it is
// not tenant-scoped data, just "is the process up." Every other RPC on
// every service remains subject to the Layer 2 enforcement below.
const healthCheckMethod = "/grpc.health.v1.Health/Check"

// healthWatchMethod is the streaming counterpart to healthCheckMethod,
// exempted from tenant enforcement for the same reason.
const healthWatchMethod = "/grpc.health.v1.Health/Watch"

// unexported context key type so this package's context values can never
// collide with keys set by other packages.
type contextKey struct{}

var tenantIDContextKey = contextKey{}

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

// extractTenantID reads and validates the x-tenant-id metadata value from
// an incoming gRPC context. Returns a gRPC Unauthenticated status error on
// any failure so callers can return it directly.
func extractTenantID(ctx context.Context) (uuid.UUID, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return uuid.UUID{}, status.Errorf(codes.Unauthenticated, "tenantctx: missing gRPC metadata")
	}
	values := md.Get(MetadataKey)
	if len(values) == 0 || values[0] == "" {
		return uuid.UUID{}, status.Errorf(codes.Unauthenticated, "tenantctx: missing %s metadata", MetadataKey)
	}
	id, err := uuid.Parse(values[0])
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.Unauthenticated, "tenantctx: malformed %s metadata: %v", MetadataKey, err)
	}
	return id, nil
}

// UnaryServerInterceptor validates x-tenant-id on every incoming unary RPC
// and stores it in the handler's context. Requests with a missing or
// malformed tenant ID are rejected with codes.Unauthenticated before
// reaching the handler.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == healthCheckMethod {
			return handler(ctx, req)
		}
		tenantID, err := extractTenantID(ctx)
		if err != nil {
			return nil, err
		}
		return handler(WithTenantID(ctx, tenantID), req)
	}
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
// UnaryServerInterceptor: validates x-tenant-id before the stream handler
// runs, and makes it available via TenantID(stream.Context()).
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod == healthWatchMethod {
			return handler(srv, ss)
		}
		tenantID, err := extractTenantID(ss.Context())
		if err != nil {
			return err
		}
		return handler(srv, &wrappedServerStream{
			ServerStream: ss,
			ctx:          WithTenantID(ss.Context(), tenantID),
		})
	}
}

// InjectTenantID returns a client-side unary interceptor that attaches
// tenantID as x-tenant-id gRPC metadata on every outgoing call, so
// propagation to downstream services is uniform rather than reimplemented
// per call site.
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
