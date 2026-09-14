package authn

import (
	"time"

	"github.com/google/uuid"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
)

// TokenIssuer wraps pkg/jwtauth.Signer with this service's default TTL, so
// grpcapi.Login doesn't need to know the configured TTL itself -- it just
// asks for a token for a given user/tenant/roles.
type TokenIssuer struct {
	signer *jwtauth.Signer
	ttl    time.Duration
}

// NewTokenIssuer constructs a TokenIssuer. ttl is the short-lived JWT
// expiry (architecture doc Section 2.2: "issues short-lived JWTs"),
// configurable via TENANT_IDENTITY_JWT_TTL_SECONDS in cmd/main.go,
// defaulting to 1 hour.
func NewTokenIssuer(signer *jwtauth.Signer, ttl time.Duration) *TokenIssuer {
	return &TokenIssuer{signer: signer, ttl: ttl}
}

// IssueToken mints a token for the given tenant/user/roles using the
// configured TTL, returning the compact JWT and its expiry.
func (i *TokenIssuer) IssueToken(tenantID uuid.UUID, userID string, roles []string) (token string, expiresAt time.Time, err error) {
	return i.signer.Issue(jwtauth.Claims{
		TenantID: tenantID,
		Subject:  userID,
		Roles:    roles,
	}, i.ttl)
}
