package wsticket

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
)

func TestNewMinter_DefaultsTTL(t *testing.T) {
	m, err := NewMinter(0)
	if err != nil {
		t.Fatalf("NewMinter: %v", err)
	}
	if m.ttl != DefaultTTL {
		t.Fatalf("expected default TTL %v, got %v", DefaultTTL, m.ttl)
	}
}

func TestMint_ProducesVerifiableTicketWithSameClaims(t *testing.T) {
	m, err := NewMinter(time.Minute)
	if err != nil {
		t.Fatalf("NewMinter: %v", err)
	}

	tenantID := uuid.New()
	claims := jwtauth.Claims{TenantID: tenantID, Subject: "agent-42", Roles: []string{"agent"}}

	ticket, expiresAt, err := m.Mint(claims)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if ticket == "" {
		t.Fatal("expected a non-empty ticket string")
	}
	if !expiresAt.After(time.Now()) {
		t.Fatalf("expected expiresAt to be in the future, got %v", expiresAt)
	}
	if expiresAt.After(time.Now().Add(2 * time.Minute)) {
		t.Fatalf("expected expiresAt to respect the configured 1-minute TTL, got %v", expiresAt)
	}

	verifier := m.Verifier()
	gotClaims, err := verifier.Verify(ticket)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if gotClaims.TenantID != tenantID {
		t.Fatalf("expected tid claim %v, got %v", tenantID, gotClaims.TenantID)
	}
	if gotClaims.Subject != "agent-42" {
		t.Fatalf("expected sub claim %q, got %q", "agent-42", gotClaims.Subject)
	}
}

func TestMint_TicketExpiresAfterTTL(t *testing.T) {
	m, err := NewMinter(1)
	if err != nil {
		t.Fatalf("NewMinter: %v", err)
	}
	// A TTL of 1 nanosecond (any value <= 0 falls back to DefaultTTL --
	// see NewMinter -- so 1ns is the smallest usable positive duration to
	// exercise an already-expired ticket deterministically).
	claims := jwtauth.Claims{TenantID: uuid.New(), Subject: "agent-1"}
	ticket, _, err := m.Mint(claims)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	if _, err := m.Verifier().Verify(ticket); err == nil {
		t.Fatal("expected an expired ticket to fail verification")
	}
}

func TestVerifier_RejectsTokenSignedByDifferentMinter(t *testing.T) {
	m1, err := NewMinter(time.Minute)
	if err != nil {
		t.Fatalf("NewMinter: %v", err)
	}
	m2, err := NewMinter(time.Minute)
	if err != nil {
		t.Fatalf("NewMinter: %v", err)
	}

	ticket, _, err := m1.Mint(jwtauth.Claims{TenantID: uuid.New(), Subject: "agent-1"})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	// Each Minter generates its OWN fresh keypair (see package doc
	// comment) -- a ticket from one must never verify against another.
	if _, err := m2.Verifier().Verify(ticket); err == nil {
		t.Fatal("expected a ticket signed by a different Minter's key to fail verification")
	}
}

func TestPublicKeyPEM_ProducesLoadableVerifier(t *testing.T) {
	m, err := NewMinter(time.Minute)
	if err != nil {
		t.Fatalf("NewMinter: %v", err)
	}
	pemBytes, err := m.PublicKeyPEM()
	if err != nil {
		t.Fatalf("PublicKeyPEM: %v", err)
	}

	// This is exactly the round-trip Agent Presence performs: PEM bytes
	// distributed via ConfigMap, loaded back into a Verifier via
	// pkg/jwtauth.LoadVerifierFromPEM.
	verifier, err := jwtauth.LoadVerifierFromPEM(pemBytes)
	if err != nil {
		t.Fatalf("LoadVerifierFromPEM: %v", err)
	}

	claims := jwtauth.Claims{TenantID: uuid.New(), Subject: "agent-1"}
	ticket, _, err := m.Mint(claims)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if _, err := verifier.Verify(ticket); err != nil {
		t.Fatalf("expected ticket to verify against the PEM-round-tripped public key: %v", err)
	}
}
