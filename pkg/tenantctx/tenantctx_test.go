package tenantctx

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// testKeypair generates a fresh ECDSA P-256 signer/verifier pair for
// tests.
func testKeypair(t *testing.T) (*jwtauth.Signer, *jwtauth.Verifier) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return jwtauth.NewSigner(key, "tenantctx-test"), jwtauth.NewVerifier(&key.PublicKey)
}

// bearerCtx builds an incoming gRPC context carrying the given
// authorization metadata value (already including any "Bearer " prefix
// the caller wants to test).
func bearerCtx(value string) context.Context {
	if value == "" {
		return metadata.NewIncomingContext(context.Background(), metadata.MD{})
	}
	md := metadata.Pairs(AuthorizationMetadataKey, value)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestWithTenantID_TenantID_RoundTrip(t *testing.T) {
	id := uuid.New()
	ctx := WithTenantID(context.Background(), id)

	got, err := TenantID(ctx)
	if err != nil {
		t.Fatalf("TenantID() returned error: %v", err)
	}
	if got != id {
		t.Fatalf("TenantID() = %v, want %v", got, id)
	}
}

func TestTenantID_MissingFromContext(t *testing.T) {
	_, err := TenantID(context.Background())
	if err == nil {
		t.Fatal("expected error for context with no tenant id, got nil")
	}
}

func TestClaims_MissingFromContext(t *testing.T) {
	_, err := Claims(context.Background())
	if err == nil {
		t.Fatal("expected error for context with no claims, got nil")
	}
}

func TestUnaryServerInterceptor(t *testing.T) {
	signer, verifier := testKeypair(t)
	otherSigner, _ := testKeypair(t)

	validTenantID := uuid.New()
	validToken, _, err := signer.Issue(jwtauth.Claims{TenantID: validTenantID, Subject: "user-1", Roles: []string{"agent"}}, time.Hour)
	if err != nil {
		t.Fatalf("issue valid token: %v", err)
	}
	expiredToken, _, err := signer.Issue(jwtauth.Claims{TenantID: validTenantID, Subject: "user-1"}, -time.Minute)
	if err != nil {
		t.Fatalf("issue expired token: %v", err)
	}
	wrongKeyToken, _, err := otherSigner.Issue(jwtauth.Claims{TenantID: validTenantID, Subject: "user-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue wrong-key token: %v", err)
	}

	tests := []struct {
		name        string
		authHeader  string
		noMetadata  bool
		wantTenant  uuid.UUID
		wantSubject string
		wantErr     bool
	}{
		{
			name:        "valid bearer token accepted and tenant_id derived from tid claim",
			authHeader:  "Bearer " + validToken,
			wantTenant:  validTenantID,
			wantSubject: "user-1",
		},
		{
			name:       "missing authorization metadata rejected",
			authHeader: "",
			wantErr:    true,
		},
		{
			name:       "no incoming metadata at all rejected",
			noMetadata: true,
			wantErr:    true,
		},
		{
			name:       "missing bearer prefix rejected",
			authHeader: validToken,
			wantErr:    true,
		},
		{
			name:       "wrong scheme rejected",
			authHeader: "Basic " + validToken,
			wantErr:    true,
		},
		{
			name:       "malformed token rejected",
			authHeader: "Bearer not-a-jwt-at-all",
			wantErr:    true,
		},
		{
			name:       "expired token rejected",
			authHeader: "Bearer " + expiredToken,
			wantErr:    true,
		},
		{
			name:       "wrong signature rejected",
			authHeader: "Bearer " + wrongKeyToken,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ctx context.Context
			if tt.noMetadata {
				ctx = context.Background()
			} else {
				ctx = bearerCtx(tt.authHeader)
			}

			interceptor := UnaryServerInterceptor(verifier)

			var gotCtx context.Context
			handler := func(hCtx context.Context, req any) (any, error) {
				gotCtx = hCtx
				return "ok", nil
			}

			resp, err := interceptor(ctx, "req", &grpc.UnaryServerInfo{}, handler)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (resp=%v)", resp)
				}
				st, ok := status.FromError(err)
				if !ok {
					t.Fatalf("expected gRPC status error, got %v", err)
				}
				if st.Code() != codes.Unauthenticated {
					t.Fatalf("got code %v, want %v", st.Code(), codes.Unauthenticated)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			gotID, err := TenantID(gotCtx)
			if err != nil {
				t.Fatalf("TenantID() on handler context returned error: %v", err)
			}
			if gotID != tt.wantTenant {
				t.Fatalf("got tenant id %v, want %v", gotID, tt.wantTenant)
			}
			claims, err := Claims(gotCtx)
			if err != nil {
				t.Fatalf("Claims() on handler context returned error: %v", err)
			}
			if claims.Subject != tt.wantSubject {
				t.Fatalf("got subject %q, want %q", claims.Subject, tt.wantSubject)
			}
		})
	}
}

func TestUnaryServerInterceptor_NilVerifierFailsClosed(t *testing.T) {
	// A nil verifier is a deployment/programming error. This is a
	// security control, so it must fail closed: every non-exempt call
	// rejected, never silently admitted.
	interceptor := UnaryServerInterceptor(nil)

	handlerCalled := false
	handler := func(hCtx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	ctx := bearerCtx("Bearer irrelevant")
	_, err := interceptor(ctx, "req", &grpc.UnaryServerInfo{}, handler)
	if err == nil {
		t.Fatal("expected error with nil verifier, got nil")
	}
	if handlerCalled {
		t.Fatal("handler must not be invoked when verifier is nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Fatalf("expected Internal status for misconfigured server, got %v", err)
	}
}

func TestUnaryServerInterceptor_HealthCheckExempt(t *testing.T) {
	// The standard grpc.health.v1.Health/Check RPC must be reachable
	// without a token, since Kubernetes probes and other infra tooling
	// call it with no auth context at all. Uses a nil verifier to prove
	// the exemption is checked BEFORE the verifier is even consulted.
	ctx := context.Background() // no metadata attached
	interceptor := UnaryServerInterceptor(nil)

	handlerCalled := false
	handler := func(hCtx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	info := &grpc.UnaryServerInfo{FullMethod: healthCheckMethod}
	resp, err := interceptor(ctx, "req", info, handler)
	if err != nil {
		t.Fatalf("expected health check to bypass tenant enforcement, got error: %v", err)
	}
	if !handlerCalled {
		t.Fatal("expected handler to be invoked for health check method")
	}
	if resp != "ok" {
		t.Fatalf("unexpected response: %v", resp)
	}
}

func TestUnaryServerInterceptor_CustomExemptMethod(t *testing.T) {
	// A caller-supplied exemption (e.g. Tenant & Identity's Login/
	// CreateTenant/IssueServiceToken RPCs) must bypass tenant enforcement
	// the same way the built-in health check does, while any other
	// method name remains enforced as normal.
	_, verifier := testKeypair(t)
	const exemptMethod = "/tenantidentity.v1.IdentityService/Login"
	interceptor := UnaryServerInterceptor(verifier, exemptMethod)

	handlerCalled := false
	handler := func(hCtx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	ctx := context.Background() // no metadata attached
	info := &grpc.UnaryServerInfo{FullMethod: exemptMethod}
	resp, err := interceptor(ctx, "req", info, handler)
	if err != nil {
		t.Fatalf("expected exempt method to bypass tenant enforcement, got error: %v", err)
	}
	if !handlerCalled {
		t.Fatal("expected handler to be invoked for exempt method")
	}
	if resp != "ok" {
		t.Fatalf("unexpected response: %v", resp)
	}

	// A non-exempt method on the same interceptor instance must still be
	// enforced.
	otherInfo := &grpc.UnaryServerInfo{FullMethod: "/tenantidentity.v1.IdentityService/ListUsers"}
	_, err = interceptor(ctx, "req", otherInfo, handler)
	if err == nil {
		t.Fatal("expected non-exempt method to still require a valid bearer token")
	}
}

func TestStreamServerInterceptor(t *testing.T) {
	signer, verifier := testKeypair(t)
	tenantID := uuid.New()
	token, _, err := signer.Issue(jwtauth.Claims{TenantID: tenantID, Subject: "user-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	interceptor := StreamServerInterceptor(verifier)

	fakeStream := &fakeServerStream{ctx: bearerCtx("Bearer " + token)}
	var gotCtx context.Context
	handler := func(srv any, ss grpc.ServerStream) error {
		gotCtx = ss.Context()
		return nil
	}

	if err := interceptor(nil, fakeStream, &grpc.StreamServerInfo{}, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	gotID, err := TenantID(gotCtx)
	if err != nil {
		t.Fatalf("TenantID() on stream context returned error: %v", err)
	}
	if gotID != tenantID {
		t.Fatalf("got tenant id %v, want %v", gotID, tenantID)
	}
}

func TestStreamServerInterceptor_HealthWatchExempt(t *testing.T) {
	interceptor := StreamServerInterceptor(nil)
	fakeStream := &fakeServerStream{ctx: context.Background()}
	handlerCalled := false
	handler := func(srv any, ss grpc.ServerStream) error {
		handlerCalled = true
		return nil
	}
	info := &grpc.StreamServerInfo{FullMethod: healthWatchMethod}
	if err := interceptor(nil, fakeStream, info, handler); err != nil {
		t.Fatalf("expected health watch to bypass tenant enforcement, got error: %v", err)
	}
	if !handlerCalled {
		t.Fatal("expected handler to be invoked for health watch method")
	}
}

func TestStreamServerInterceptor_InvalidTokenRejected(t *testing.T) {
	_, verifier := testKeypair(t)
	interceptor := StreamServerInterceptor(verifier)
	fakeStream := &fakeServerStream{ctx: bearerCtx("Bearer not-a-jwt")}
	handler := func(srv any, ss grpc.ServerStream) error {
		t.Fatal("handler must not be invoked for an invalid token")
		return nil
	}
	info := &grpc.StreamServerInfo{FullMethod: "/some.Service/Method"}
	err := interceptor(nil, fakeStream, info, handler)
	if err == nil {
		t.Fatal("expected error for invalid token, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated status, got %v", err)
	}
}

// fakeServerStream is a minimal grpc.ServerStream implementation for
// testing StreamServerInterceptor without a real network connection.
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }
