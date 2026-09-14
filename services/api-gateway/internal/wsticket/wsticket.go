// Package wsticket mints and documents the short-lived WebSocket "ticket"
// API Gateway issues so a browser client (whose native WebSocket API
// cannot set custom headers, e.g. "Authorization: Bearer <jwt>") can still
// authenticate its connection to Agent & Presence Service.
//
// # Why a separate signing key from Tenant & Identity's session JWTs
//
// A ticket is, deliberately, NOT a re-issuance of the caller's real
// session JWT (Tenant & Identity's Login token) and API Gateway does NOT
// hold Tenant & Identity's signing private key -- that key belongs
// exclusively to Tenant & Identity (see services/tenant-identity/internal/pgstore/keypair.go
// and pkg/jwtauth's package doc comment: "this service holds
// exclusively"). Minting a ticket therefore cannot reuse pkg/jwtauth.Signer
// with that key.
//
// Instead, API Gateway generates and holds its OWN ECDSA P-256 keypair,
// used for exactly one purpose: signing short-TTL WebSocket tickets. This
// keeps the trust boundary narrow and intentional:
//   - A leaked/compromised ticket-signing key lets an attacker forge only
//     short-lived (30-60s) WebSocket tickets -- never a full session token
//     usable against any REST route or another service's gRPC API.
//   - Symmetrically, a compromised Tenant & Identity session key does not
//     need to be treated as also having compromised the WebSocket ticket
//     path (though in practice an attacker who can forge session tokens
//     has bigger problems to exploit than the WebSocket path).
//
// Agent & Presence Service trusts this key as a SECOND, ticket-scoped
// verifier alongside its existing Tenant & Identity verifier (see
// services/agent-presence/internal/wsserver's updated doc comment) --
// distributed the same manual-ConfigMap way Tenant & Identity's public key
// already is (see deploy/k8s/api-gateway-ws-ticket-public-key.example.yaml).
//
// # Scope: short TTL, not true single-use
//
// A ticket carries the same `tid`/`sub` claims as any other token (so
// Agent Presence's existing claims-to-tenant/agent-id mapping logic needs
// no special case for it) and a deliberately short expiry (default 45s,
// see TTL). This is NOT a single-use/replay-protected token -- there is no
// server-side "used tickets" registry, so a ticket could in principle be
// replayed by anyone who intercepts it within its TTL window to open a
// second WebSocket connection. This is a conscious, documented scope
// decision for this milestone: a short TTL bounds the exposure window
// tightly (an intercepted ticket is useless within under a minute), and a
// database-backed used-ticket registry would add real infrastructure
// (a shared store, replica coordination) for a marginal improvement over
// "the window is already very small" at this system's current scale and
// threat model. A future milestone that needs stronger guarantees (e.g.
// once WebSocket proxying carries genuinely sensitive real-time media, not
// just task/presence notifications) is the right place to revisit this,
// not this pass.
//
// # No persistence, no key rotation across restarts
//
// Unlike Tenant & Identity's signing key, this keypair is generated fresh
// on every process startup and held only in memory -- it is NOT persisted
// to Postgres. API Gateway is documented as Stateless (architecture doc
// Section 2.2) and runs as a single replica in this milestone's
// deployment.yaml; a restart invalidates any outstanding tickets (which,
// given their ~45s TTL, are virtually always already expired or consumed
// by the time a restart happens in practice) and the operator re-copies
// the freshly generated public key into the ConfigMap, mirroring Tenant &
// Identity's own "log the PEM at startup for manual distribution" pattern
// (cmd/main.go). Multi-replica API Gateway deployments or durable-key
// requirements are explicitly out of scope for this milestone -- see
// PROGRESS.md.
package wsticket

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
)

// DefaultTTL is the ticket's default lifetime: long enough for a browser
// client to receive the HTTP response and immediately open the WebSocket
// (well under a second in practice, even over a slow connection), short
// enough that a leaked/intercepted ticket has almost no useful window --
// see the package doc comment's "Scope" section for the full reasoning.
const DefaultTTL = 45 * time.Second

// Minter holds API Gateway's dedicated ticket-signing keypair (see package
// doc comment for why this is separate from Tenant & Identity's session
// key) and mints tickets from already-verified caller claims.
type Minter struct {
	signer    *jwtauth.Signer
	publicKey *ecdsa.PublicKey
	ttl       time.Duration
}

// NewMinter generates a fresh ECDSA P-256 keypair and constructs a Minter
// around it. Called exactly once at startup (cmd/main.go) -- see package
// doc comment on why this key is generated fresh per process rather than
// persisted. ttl <= 0 uses DefaultTTL.
func NewMinter(ttl time.Duration) (*Minter, error) {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("wsticket: generate signing key: %w", err)
	}
	return &Minter{
		signer:    jwtauth.NewSigner(privateKey, "api-gateway-ws-ticket"),
		publicKey: &privateKey.PublicKey,
		ttl:       ttl,
	}, nil
}

// PublicKeyPEM returns the Minter's public key, PEM-encoded
// (SubjectPublicKeyInfo / PKIX DER, ECDSA P-256) -- the same shape
// pkg/jwtauth.LoadVerifierFromPEM expects, for logging at startup so an
// operator can copy it into the api-gateway-ws-ticket-public-key ConfigMap
// Agent Presence mounts (mirrors Tenant & Identity's cmd/main.go banner
// pattern exactly).
func (m *Minter) PublicKeyPEM() ([]byte, error) {
	der, err := x509.MarshalPKIXPublicKey(m.publicKey)
	if err != nil {
		return nil, fmt.Errorf("wsticket: marshal public key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), nil
}

// Mint issues a new ticket carrying the given, already-verified claims
// (the caller's real `tid`/`sub`/`roles`, copied through unchanged from
// the session JWT API Gateway's gwauth middleware already validated on
// this request -- see httpapi's ws-ticket handler) with the Minter's short
// TTL. Returns the compact JWT string and its expiry.
func (m *Minter) Mint(claims jwtauth.Claims) (ticket string, expiresAt time.Time, err error) {
	return m.signer.Issue(claims, m.ttl)
}

// Verifier returns a jwtauth.Verifier for this Minter's own public key --
// used in-process by internal/wsproxy to fail fast on an invalid ticket
// before ever dialing Agent Presence upstream (Agent Presence itself
// re-verifies independently against the same public key, distributed via
// ConfigMap -- see package doc comment; this in-process Verifier is purely
// a convenience so wsproxy doesn't need its own copy of the key).
func (m *Minter) Verifier() *jwtauth.Verifier {
	return jwtauth.NewVerifier(m.publicKey)
}
