package jwtauth

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// bearerMetadataKey mirrors pkg/tenantctx.AuthorizationMetadataKey. Not
// imported directly: pkg/jwtauth is deliberately dependency-free of every
// other package in this repo (see package doc comment) so it can be
// imported by tenantctx itself without a cycle, and so any future
// non-Go/non-tenantctx consumer of this package's client helper isn't
// forced to pull tenantctx in too. The wire value ("authorization",
// "Bearer <token>") is a fixed, shared contract between the two packages.
const bearerMetadataKey = "authorization"

// InjectToken returns a client-side unary gRPC interceptor that attaches
// token as "authorization: Bearer <token>" metadata on every outgoing
// call -- the client-side half of the contract pkg/tenantctx's server
// interceptors verify. tokenFunc is called on every outgoing call (not
// just once at construction) so a TokenSource's cached-and-refreshed
// token can be plugged in directly.
func InjectToken(tokenFunc func(ctx context.Context) (string, error)) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		token, err := tokenFunc(ctx)
		if err != nil {
			return fmt.Errorf("jwtauth: obtain token: %w", err)
		}
		ctx = metadata.AppendToOutgoingContext(ctx, bearerMetadataKey, "Bearer "+token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// InjectTokenStream is the streaming-RPC equivalent of InjectToken.
func InjectTokenStream(tokenFunc func(ctx context.Context) (string, error)) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		token, err := tokenFunc(ctx)
		if err != nil {
			return nil, fmt.Errorf("jwtauth: obtain token: %w", err)
		}
		ctx = metadata.AppendToOutgoingContext(ctx, bearerMetadataKey, "Bearer "+token)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// issueFunc mints a new token, returning the compact JWT and its expiry.
// Matches the shape of both Signer.Issue (for in-process use, e.g.
// tenant-identity calling out to another service with its own signer) and
// a generated gRPC client's IssueServiceToken RPC (for every other
// service, which has no signing key of its own -- see
// services/tenant-identity/internal/authn and this package's doc comment
// on why only tenant-identity ever holds a private key).
type issueFunc func(ctx context.Context) (token string, expiresAt time.Time, err error)

// TokenSource caches a single token obtained from issue, transparently
// refreshing it shortly before expiry rather than on every call. This is
// the recommended way for a service acting as a gRPC client to another
// service to obtain and attach a service-to-service token (architecture
// doc's "short-lived JWTs" language, without paying a round trip to
// Tenant & Identity's IssueServiceToken RPC on every single outgoing
// call).
//
// Safe for concurrent use.
type TokenSource struct {
	issue issueFunc
	// refreshBefore is how long before actual expiry a cached token is
	// treated as stale and proactively refreshed, so a token is never
	// used right up to (or past) the moment it would fail verification
	// due to clock skew between this process and the verifying service.
	refreshBefore time.Duration

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

// NewTokenSource constructs a TokenSource. refreshBefore is the refresh-
// before-expiry margin; a zero value defaults to 30 seconds, a reasonable
// default given this system's short-lived-token TTLs are measured in
// minutes-to-hours, not seconds (see tenant-identity's
// TENANT_IDENTITY_JWT_TTL_SECONDS default of 1 hour).
func NewTokenSource(issue func(ctx context.Context) (token string, expiresAt time.Time, err error), refreshBefore time.Duration) *TokenSource {
	if refreshBefore <= 0 {
		refreshBefore = 30 * time.Second
	}
	return &TokenSource{issue: issue, refreshBefore: refreshBefore}
}

// Token returns a currently-valid token, reusing the cached one if it is
// not yet within refreshBefore of expiry, or obtaining (and caching) a
// new one otherwise. Matches the func(context.Context) (string, error)
// shape InjectToken/InjectTokenStream expect, so a TokenSource's Token
// method can be passed directly: jwtauth.InjectToken(src.Token).
func (s *TokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.token != "" && time.Now().Add(s.refreshBefore).Before(s.expiresAt) {
		return s.token, nil
	}

	token, expiresAt, err := s.issue(ctx)
	if err != nil {
		return "", fmt.Errorf("jwtauth: refresh token: %w", err)
	}
	s.token = token
	s.expiresAt = expiresAt
	return s.token, nil
}
