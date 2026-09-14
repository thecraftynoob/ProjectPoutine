package jwtauth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func generateKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return key
}

func TestIssueAndVerifyRoundTrip(t *testing.T) {
	key := generateKey(t)
	signer := NewSigner(key, "tenant-identity-test")
	verifier := NewVerifier(&key.PublicKey)

	tenantID := uuid.New()
	claims := Claims{
		TenantID: tenantID,
		Subject:  "user-123",
		Roles:    []string{"admin", "agent"},
	}

	token, expiresAt, err := signer.Issue(claims, time.Hour)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}
	if expiresAt.Before(time.Now()) {
		t.Fatalf("expected future expiry, got %v", expiresAt)
	}

	got, err := verifier.Verify(token)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got.TenantID != tenantID {
		t.Errorf("tenant id: want %v, got %v", tenantID, got.TenantID)
	}
	if got.Subject != claims.Subject {
		t.Errorf("subject: want %q, got %q", claims.Subject, got.Subject)
	}
	if len(got.Roles) != 2 || got.Roles[0] != "admin" || got.Roles[1] != "agent" {
		t.Errorf("roles: want [admin agent], got %v", got.Roles)
	}
}

func TestVerifyRejectsExpiredToken(t *testing.T) {
	key := generateKey(t)
	signer := NewSigner(key, "tenant-identity-test")
	verifier := NewVerifier(&key.PublicKey)

	token, _, err := signer.Issue(Claims{TenantID: uuid.New(), Subject: "user-1"}, -time.Minute)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}

	if _, err := verifier.Verify(token); err == nil {
		t.Fatal("expected error verifying expired token, got nil")
	}
}

func TestVerifyRejectsWrongKey(t *testing.T) {
	key := generateKey(t)
	otherKey := generateKey(t)
	signer := NewSigner(key, "tenant-identity-test")
	verifier := NewVerifier(&otherKey.PublicKey) // wrong public key

	token, _, err := signer.Issue(Claims{TenantID: uuid.New(), Subject: "user-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}

	if _, err := verifier.Verify(token); err == nil {
		t.Fatal("expected error verifying token against mismatched key, got nil")
	}
}

func TestVerifyRejectsTamperedToken(t *testing.T) {
	key := generateKey(t)
	signer := NewSigner(key, "tenant-identity-test")
	verifier := NewVerifier(&key.PublicKey)

	token, _, err := signer.Issue(Claims{TenantID: uuid.New(), Subject: "user-1", Roles: []string{"agent"}}, time.Hour)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}

	// Corrupt the payload (middle, base64url-encoded segment) so its
	// content no longer matches what the signature was computed over --
	// unlike flipping a single trailing signature character (which can,
	// depending on ECDSA's encoding slack, occasionally still verify),
	// this deterministically invalidates the signature check.
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected a 3-part JWT, got %d parts", len(parts))
	}
	payload := []byte(parts[1])
	if payload[0] == 'A' {
		payload[0] = 'B'
	} else {
		payload[0] = 'A'
	}
	parts[1] = string(payload)
	tampered := strings.Join(parts, ".")

	if _, err := verifier.Verify(tampered); err == nil {
		t.Fatal("expected error verifying tampered token, got nil")
	}
}

func TestVerifyRejectsMalformedToken(t *testing.T) {
	key := generateKey(t)
	verifier := NewVerifier(&key.PublicKey)

	if _, err := verifier.Verify("not-a-jwt-at-all"); err == nil {
		t.Fatal("expected error verifying malformed token, got nil")
	}
}
