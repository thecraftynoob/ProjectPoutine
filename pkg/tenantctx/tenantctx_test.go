package tenantctx

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

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

func TestUnaryServerInterceptor(t *testing.T) {
	tests := []struct {
		name       string
		mdValue    []string
		noMetadata bool
		wantCode   codes.Code
		wantErr    bool
	}{
		{
			name:    "valid tenant header passes through and is retrievable",
			mdValue: []string{uuid.New().String()},
			wantErr: false,
		},
		{
			name:     "missing header rejected",
			mdValue:  nil,
			wantCode: codes.Unauthenticated,
			wantErr:  true,
		},
		{
			name:     "empty header value rejected",
			mdValue:  []string{""},
			wantCode: codes.Unauthenticated,
			wantErr:  true,
		},
		{
			name:     "malformed UUID rejected",
			mdValue:  []string{"not-a-uuid"},
			wantCode: codes.Unauthenticated,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.mdValue != nil {
				md := metadata.Pairs(MetadataKey, tt.mdValue[0])
				ctx = metadata.NewIncomingContext(ctx, md)
			} else if !tt.noMetadata {
				// Explicitly empty metadata (key present in no-metadata cases we still
				// exercise the "no incoming metadata at all" path separately below).
				ctx = metadata.NewIncomingContext(ctx, metadata.MD{})
			}

			interceptor := UnaryServerInterceptor()

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
				if st.Code() != tt.wantCode {
					t.Fatalf("got code %v, want %v", st.Code(), tt.wantCode)
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
			if gotID.String() != tt.mdValue[0] {
				t.Fatalf("got tenant id %v, want %v", gotID, tt.mdValue[0])
			}
		})
	}
}

func TestUnaryServerInterceptor_HealthCheckExempt(t *testing.T) {
	// The standard grpc.health.v1.Health/Check RPC must be reachable
	// without a tenant header, since Kubernetes probes and other infra
	// tooling call it with no tenant context at all.
	ctx := context.Background() // no metadata attached
	interceptor := UnaryServerInterceptor()

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

func TestUnaryServerInterceptor_NoIncomingMetadataAtAll(t *testing.T) {
	// No metadata.NewIncomingContext call at all -- simulates a truly bare context.
	ctx := context.Background()
	interceptor := UnaryServerInterceptor()
	handler := func(hCtx context.Context, req any) (any, error) {
		return "ok", nil
	}
	_, err := interceptor(ctx, "req", &grpc.UnaryServerInfo{}, handler)
	if err == nil {
		t.Fatal("expected error for missing metadata, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated status, got %v", err)
	}
}
