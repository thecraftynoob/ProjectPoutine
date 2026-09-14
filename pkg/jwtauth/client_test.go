package jwtauth

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// captureInvoker records the outgoing context's metadata so tests can
// assert on what InjectToken/InjectTokenStream attached, without needing
// a real gRPC connection.
func captureInvoker(t *testing.T, gotMD *metadata.MD) grpc.UnaryInvoker {
	t.Helper()
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		*gotMD = md
		return nil
	}
}

func TestInjectToken_AttachesBearerMetadata(t *testing.T) {
	var gotMD metadata.MD
	interceptor := InjectToken(func(ctx context.Context) (string, error) {
		return "my-token", nil
	})

	err := interceptor(context.Background(), "/svc/Method", nil, nil, nil, captureInvoker(t, &gotMD))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	values := gotMD.Get(bearerMetadataKey)
	if len(values) != 1 || values[0] != "Bearer my-token" {
		t.Fatalf("got authorization metadata %v, want [\"Bearer my-token\"]", values)
	}
}

func TestInjectToken_PropagatesTokenFuncError(t *testing.T) {
	wantErr := errors.New("boom")
	interceptor := InjectToken(func(ctx context.Context) (string, error) {
		return "", wantErr
	})

	invokerCalled := false
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		invokerCalled = true
		return nil
	}

	err := interceptor(context.Background(), "/svc/Method", nil, nil, nil, invoker)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped error to match %v, got %v", wantErr, err)
	}
	if invokerCalled {
		t.Fatal("invoker must not be called when token retrieval fails")
	}
}

func TestInjectTokenStream_AttachesBearerMetadata(t *testing.T) {
	var gotMD metadata.MD
	interceptor := InjectTokenStream(func(ctx context.Context) (string, error) {
		return "stream-token", nil
	})

	streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		md, _ := metadata.FromOutgoingContext(ctx)
		gotMD = md
		return nil, nil
	}

	_, err := interceptor(context.Background(), &grpc.StreamDesc{}, nil, "/svc/Method", streamer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	values := gotMD.Get(bearerMetadataKey)
	if len(values) != 1 || values[0] != "Bearer stream-token" {
		t.Fatalf("got authorization metadata %v, want [\"Bearer stream-token\"]", values)
	}
}

func TestTokenSource_CachesUntilNearExpiry(t *testing.T) {
	calls := 0
	src := NewTokenSource(func(ctx context.Context) (string, time.Time, error) {
		calls++
		return "token-1", time.Now().Add(time.Hour), nil
	}, 30*time.Second)

	tok1, err := src.Token(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tok2, err := src.Token(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok1 != "token-1" || tok2 != "token-1" {
		t.Fatalf("expected cached token to be reused, got %q then %q", tok1, tok2)
	}
	if calls != 1 {
		t.Fatalf("expected issue func called exactly once, got %d calls", calls)
	}
}

func TestTokenSource_RefreshesNearExpiry(t *testing.T) {
	calls := 0
	src := NewTokenSource(func(ctx context.Context) (string, time.Time, error) {
		calls++
		// Expires almost immediately -- well within the refresh window,
		// so every call should mint a fresh token.
		return "token", time.Now().Add(time.Millisecond), nil
	}, 30*time.Second)

	if _, err := src.Token(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := src.Token(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected issue func called on every request when within refresh window, got %d calls", calls)
	}
}

func TestTokenSource_PropagatesIssueError(t *testing.T) {
	wantErr := errors.New("issue failed")
	src := NewTokenSource(func(ctx context.Context) (string, time.Time, error) {
		return "", time.Time{}, wantErr
	}, 0)

	_, err := src.Token(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped error to match %v, got %v", wantErr, err)
	}
}

func TestTokenSource_DefaultRefreshMargin(t *testing.T) {
	// A zero/negative refreshBefore should default rather than cause
	// every call to treat the token as immediately stale forever, or
	// never refresh at all.
	calls := 0
	src := NewTokenSource(func(ctx context.Context) (string, time.Time, error) {
		calls++
		return "token", time.Now().Add(time.Hour), nil
	}, 0)

	if _, err := src.Token(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := src.Token(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected default refresh margin to allow caching, got %d calls", calls)
	}
}
