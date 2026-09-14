// Package svcauth is a small helper for services that need to call
// another service's gRPC API on their own behalf (not a user's), per
// architecture doc Section 1.1 Layer 2's service-to-service auth
// contract. It wraps the pattern every such caller needs:
//
//  1. Dial Tenant & Identity's gRPC server.
//  2. Call IssueServiceToken (proto/tenant-identity/v1/tenant_identity.proto)
//     with the shared service credential (see
//     deploy/k8s/service-credential.example.yaml) to obtain a short-lived
//     JWT scoped to the target tenant.
//  3. Cache and refresh that token via pkg/jwtauth.TokenSource.
//  4. Attach it as "authorization: Bearer <token>" metadata on outgoing
//     calls to whatever service is actually being called, via
//     pkg/jwtauth.InjectToken.
//
// This package is intentionally thin: it does not attempt to be a full
// service-mesh client library, just enough to make the
// IssueServiceToken-based pattern a one-call setup for any service that
// needs it, rather than duplicated boilerplate per cmd/main.go. See
// IssueServiceToken's proto doc comment for this whole mechanism's
// explicit scoping (a narrow, documented stand-in for a real per-service
// identity scheme -- not a hardened mTLS/zero-trust mesh).
package svcauth

import (
	"context"
	"fmt"
	"time"

	tenantidentityv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/tenant-identity/v1"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TokenSource dials Tenant & Identity at tenantIdentityAddr and returns a
// *jwtauth.TokenSource that mints/caches/refreshes a service token for
// tenantID via IssueServiceToken, authenticated with sharedSecret and
// tagged with callerService (the `sub` claim's "service:<callerService>"
// suffix -- diagnostics/audit only). The returned close func releases the
// underlying gRPC connection to Tenant & Identity and should be deferred
// by the caller.
//
// The dial itself is plaintext (insecure.NewCredentials()) -- consistent
// with every other gRPC connection in this repo today, none of which use
// transport-level TLS yet (see architecture doc Section 1.4's deferred
// mkcert/ingress-nginx TLS setup); the security boundary this milestone
// adds is the bearer token itself, not transport encryption.
func TokenSource(tenantIdentityAddr string, tenantID, callerService, sharedSecret string) (source *jwtauth.TokenSource, closeFunc func() error, err error) {
	conn, err := grpc.NewClient(tenantIdentityAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("svcauth: dial tenant-identity at %q: %w", tenantIdentityAddr, err)
	}

	client := tenantidentityv1.NewIdentityServiceClient(conn)

	issue := func(ctx context.Context) (string, time.Time, error) {
		resp, err := client.IssueServiceToken(ctx, &tenantidentityv1.IssueServiceTokenRequest{
			TenantId:      tenantID,
			CallerService: callerService,
			SharedSecret:  sharedSecret,
		})
		if err != nil {
			return "", time.Time{}, fmt.Errorf("svcauth: IssueServiceToken: %w", err)
		}
		return resp.GetToken(), resp.GetExpiresAt().AsTime(), nil
	}

	return jwtauth.NewTokenSource(issue, 0), conn.Close, nil
}

// DialOption returns a grpc.DialOption that attaches source's current
// token as "authorization: Bearer <token>" metadata on every outgoing
// unary call made through it -- for use when dialing the ACTUAL service
// being called (not Tenant & Identity, which TokenSource above already
// dials separately to obtain the token in the first place).
func DialOption(source *jwtauth.TokenSource) grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(jwtauth.InjectToken(source.Token))
}
