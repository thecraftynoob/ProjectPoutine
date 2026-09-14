// Package jwtauth is a small, dependency-light, pure JWT signing/
// verification library shared across services. It implements the token
// shape that CCAAS_ENTERPRISE_ARCHITECTURE.md Section 1.1 Layer 1 refers
// to ("a tenant_id resolved from the authenticated principal, JWT claim
// tid") and that pkg/tenantctx's doc comments describe as its eventual
// future validation source.
//
// This package intentionally has ZERO dependency on Postgres, Redis, NATS,
// or any other infrastructure-specific package in this repo -- it operates
// purely on an in-memory keypair (or, for Verifier, just a public key) and
// a claims struct, so any service can verify a token given only the
// issuer's public key in PEM form (see deploy/k8s/tenant-identity-public-key.example.yaml
// for how that key is distributed in this milestone).
//
// Signing uses ES256 (ECDSA P-256 + SHA-256) rather than RS256: for a
// short-lived token whose only consumers are internal services, ES256
// gives a materially smaller token and faster sign/verify than RSA at an
// equivalent security level, with no offsetting operational downside for
// this system's scale. Either would have been a reasonable, defensible
// choice; this package picks one and is consistent about it.
package jwtauth

import (
	"crypto/ecdsa"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Claims is the minimal claim set every issued token carries. TenantID
// corresponds to the `tid` claim referenced throughout the architecture
// doc and pkg/tenantctx; Subject is the authenticated user/agent's id;
// Roles is the simple string-tag RBAC set (architecture doc Section 2.2:
// "roles (RBAC)" -- kept intentionally simple, not a policy engine).
type Claims struct {
	TenantID uuid.UUID
	Subject  string
	Roles    []string
}

// registeredClaims is the wire representation: jwt.RegisteredClaims plus
// this system's two custom claims. Kept unexported so callers only ever
// interact with the plain Claims struct above; this type exists purely to
// satisfy jwt.Claims via embedding for the golang-jwt/v5 API.
type registeredClaims struct {
	jwt.RegisteredClaims
	TenantID string   `json:"tid"`
	Roles    []string `json:"roles"`
}

// Signer mints tokens. Constructed from an ECDSA P-256 private key, which
// this service holds exclusively (see services/tenant-identity/internal/pgstore
// for how Tenant & Identity persists it).
type Signer struct {
	privateKey *ecdsa.PrivateKey
	// issuer is stamped into the standard `iss` claim purely for
	// diagnostics/log-readability; verification in this package never
	// checks it, since a single issuer's public key is already the sole
	// trust anchor a Verifier is constructed from.
	issuer string
}

// NewSigner constructs a Signer from an ECDSA P-256 private key.
func NewSigner(privateKey *ecdsa.PrivateKey, issuer string) *Signer {
	return &Signer{privateKey: privateKey, issuer: issuer}
}

// Issue mints a signed token for the given claims with the given
// time-to-live, using the current wall-clock time as issued-at. Returns
// the compact JWT string and the computed expiry so callers (e.g. the
// Login RPC) can return both to their caller without re-parsing the
// token.
func (s *Signer) Issue(claims Claims, ttl time.Duration) (token string, expiresAt time.Time, err error) {
	now := time.Now().UTC()
	expiresAt = now.Add(ttl)

	wire := registeredClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   claims.Subject,
			Issuer:    s.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
		TenantID: claims.TenantID.String(),
		Roles:    claims.Roles,
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodES256, wire)
	signed, err := tok.SignedString(s.privateKey)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("jwtauth: sign: %w", err)
	}
	return signed, expiresAt, nil
}

// Verifier checks token signatures and extracts claims. Constructed from
// only the issuer's public key -- any service holding the PEM-encoded
// public key (distributed today via a manually-populated K8s ConfigMap;
// see the tenant-identity service README) can verify tokens without any
// other dependency.
type Verifier struct {
	publicKey *ecdsa.PublicKey
}

// NewVerifier constructs a Verifier from an ECDSA P-256 public key.
func NewVerifier(publicKey *ecdsa.PublicKey) *Verifier {
	return &Verifier{publicKey: publicKey}
}

// Verify checks tokenString's signature against the Verifier's public key
// and expiry, and returns the decoded Claims. Any signature mismatch,
// wrong/unexpected signing algorithm, malformed token, or expired token
// returns a non-nil error.
func (v *Verifier) Verify(tokenString string) (Claims, error) {
	var wire registeredClaims
	_, err := jwt.ParseWithClaims(tokenString, &wire, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodECDSA); !ok {
			return nil, fmt.Errorf("jwtauth: unexpected signing method %v", t.Header["alg"])
		}
		return v.publicKey, nil
	})
	if err != nil {
		return Claims{}, fmt.Errorf("jwtauth: verify: %w", err)
	}

	tid, err := uuid.Parse(wire.TenantID)
	if err != nil {
		return Claims{}, fmt.Errorf("jwtauth: invalid tid claim: %w", err)
	}

	return Claims{
		TenantID: tid,
		Subject:  wire.Subject,
		Roles:    wire.Roles,
	}, nil
}
