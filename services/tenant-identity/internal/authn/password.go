// Package authn implements Tenant & Identity Management's authentication
// primitives: bcrypt password hashing/verification and JWT minting (via
// pkg/jwtauth.Signer, so the actual jwt-library dependency and claim-shape
// logic live in exactly one place shared with pkg/jwtauth.Verifier).
package authn

import "golang.org/x/crypto/bcrypt"

// HashPassword returns the bcrypt hash of a plaintext password, using
// bcrypt's default cost. Stored in tenant_identity_users.password_hash --
// never the plaintext.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// VerifyPassword reports whether password matches hash (as produced by
// HashPassword).
func VerifyPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}
