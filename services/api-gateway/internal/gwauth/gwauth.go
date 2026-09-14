// Package gwauth implements API Gateway's Layer 1 multi-tenancy
// enforcement (CCAAS_ENTERPRISE_ARCHITECTURE.md Section 1.1): validating
// the end-user's bearer JWT on every external REST request before it is
// translated to an internal gRPC call.
//
// Re-attaching the token on the outgoing gRPC call to a backend service
// (so its own pkg/tenantctx interceptor can re-verify it independently --
// Layer 2, defense in depth) needs no custom code here: grpc-gateway's
// runtime.AnnotateContext (called by every generated *.pb.gw.go handler)
// already forwards the raw "Authorization" HTTP header through as
// "authorization" gRPC metadata on the outgoing context by default (see
// github.com/grpc-ecosystem/grpc-gateway/v2/runtime/context.go's
// annotateContext -- it special-cases "Authorization" for exactly this
// backwards-compatible pass-through, ahead of and independent of the
// configurable header matcher). This package therefore only has to block
// invalid/missing tokens BEFORE the mux runs; it does not need to inject
// or forward any metadata itself.
package gwauth

import (
	"net/http"
	"strings"

	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
)

// bearerPrefix is the required prefix of the Authorization header value,
// matched case-insensitively -- mirrors pkg/tenantctx's and wsserver's
// bearer parsing so every transport in this system enforces an identical
// wire contract.
const bearerPrefix = "Bearer "

// IsExempt reports whether the given method+path combination must bypass
// bearer-token verification, because it is itself how a caller first
// obtains a token/tenant context -- mirrors
// proto/tenant-identity/v1/tenant_identity.proto's TENANTCTX EXEMPTION
// list (Login, CreateTenant, CreateUser), restricted to the subset
// actually exposed as REST routes at the gateway. IssueServiceToken is a
// service-to-service RPC and deliberately has no google.api.http
// annotation at all (see task_router.proto/tenant_identity.proto), so it
// never reaches the gateway's REST surface and needs no exemption entry.
//
// GetTenant/ListTenants are also exempt here, but for a different reason
// than bootstrapping: per tenant_identity.proto's file-level note they are
// "platform-admin-ish... not scoped by any single tenant_id" registry
// reads, the same category as CreateTenant, not end-user tenant-scoped
// data -- this milestone does not add a separate platform-admin auth
// scheme, so they are treated the same as CreateTenant at the gateway.
//
// The gateway's own POST /v1/ws-ticket endpoint is deliberately NOT
// exempt: unlike Login/CreateTenant/CreateUser, minting a WebSocket
// ticket requires an ALREADY-authenticated caller (see
// internal/wsticket's doc comment) -- it is not a bootstrapping
// operation.
//
// GET /ws (the WebSocket proxy, internal/wsproxy) IS exempt here, but for
// a different reason than any of the above: it authenticates via its OWN
// mechanism (the ?ticket= query parameter, verified against API Gateway's
// dedicated ws-ticket key by wsproxy.Handler itself) rather than the
// Authorization-header bearer token this middleware checks. Exempting it
// here does not weaken auth -- it just means THIS middleware isn't the
// one enforcing it for that one route; wsproxy.Handler still rejects any
// request with a missing/invalid/expired ticket with 401 on its own,
// before ever dialing upstream to Agent Presence.
func IsExempt(method, path string) bool {
	if method == http.MethodPost && path == "/v1/tenants" {
		return true // CreateTenant
	}
	if method == http.MethodGet && (path == "/v1/tenants" || strings.HasPrefix(path, "/v1/tenants/")) {
		return true // ListTenants / GetTenant -- platform registry reads, see doc comment
	}
	if method == http.MethodPost && path == "/v1/auth/login" {
		return true // Login
	}
	if method == http.MethodPost && strings.HasSuffix(path, "/users") && strings.HasPrefix(path, "/v1/tenants/") {
		return true // CreateUser -- bootstraps a tenant's first user, see proto note
	}
	if method == http.MethodGet && path == "/ws" {
		return true // WebSocket proxy -- authenticates via ?ticket=, not this middleware; see doc comment above
	}
	return false
}

// Middleware wraps an http.Handler (the grpc-gateway runtime.ServeMux) with
// bearer-JWT verification. Non-exempt requests without a valid token are
// rejected with 401 before ever reaching the mux. A valid (or exempt)
// request is passed through unmodified -- the ORIGINAL Authorization
// header is still on the *http.Request grpc-gateway itself sees and
// forwards (see package doc comment), so this middleware does not need to
// re-inject anything.
//
// verifier must be non-nil -- this is a security control and fails closed
// at construction time, mirroring every other service's cmd/main.go
// (refuse to start rather than serve with a nil verifier).
func Middleware(verifier *jwtauth.Verifier, next http.Handler) http.Handler {
	if verifier == nil {
		panic("gwauth: Middleware called with a nil verifier -- refusing to serve unauthenticated traffic")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if IsExempt(r.Method, r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		raw := r.Header.Get("Authorization")
		if raw == "" {
			http.Error(w, `{"error":"gwauth: missing Authorization header"}`, http.StatusUnauthorized)
			return
		}
		if len(raw) <= len(bearerPrefix) || !strings.EqualFold(raw[:len(bearerPrefix)], bearerPrefix) {
			http.Error(w, `{"error":"gwauth: Authorization header must be \"Bearer <token>\""}`, http.StatusUnauthorized)
			return
		}
		token := raw[len(bearerPrefix):]

		if _, err := verifier.Verify(token); err != nil {
			http.Error(w, `{"error":"gwauth: invalid or expired token"}`, http.StatusUnauthorized)
			return
		}

		// Valid: let the request through. grpc-gateway's own
		// runtime.AnnotateContext will forward this same Authorization
		// header to the backend as outgoing gRPC metadata (see package
		// doc comment) -- re-verification happens again, independently,
		// at the backend (Layer 2). That is expected and intentional
		// defense in depth, not redundant waste.
		next.ServeHTTP(w, r)
	})
}
