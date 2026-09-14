package jwtauth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func generatePublicKeyPEM(t *testing.T) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("marshal public key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), key
}

func TestLoadVerifierFromPEM_VerifiesRealToken(t *testing.T) {
	pemBytes, key := generatePublicKeyPEM(t)

	verifier, err := LoadVerifierFromPEM(pemBytes)
	if err != nil {
		t.Fatalf("LoadVerifierFromPEM: %v", err)
	}

	signer := NewSigner(key, "loadverifier-test")
	token, _, err := signer.Issue(Claims{TenantID: uuid.New(), Subject: "user-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}

	if _, err := verifier.Verify(token); err != nil {
		t.Fatalf("expected token to verify against loaded public key, got error: %v", err)
	}
}

func TestLoadVerifierFromPEM_RejectsGarbage(t *testing.T) {
	if _, err := LoadVerifierFromPEM([]byte("not a pem block")); err == nil {
		t.Fatal("expected error for non-PEM data, got nil")
	}
}

func TestLoadVerifierFromPEM_RejectsNonECDSAKey(t *testing.T) {
	// A PEM block that parses but isn't PKIX ECDSA (garbage DER bytes
	// inside a well-formed PEM wrapper) must be rejected cleanly rather
	// than panicking.
	block := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: []byte("not valid DER")})
	if _, err := LoadVerifierFromPEM(block); err == nil {
		t.Fatal("expected error for invalid DER content, got nil")
	}
}

func TestLoadVerifierFromFile_RoundTrip(t *testing.T) {
	pemBytes, key := generatePublicKeyPEM(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "public_key.pem")
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("write test key file: %v", err)
	}

	verifier, err := LoadVerifierFromFile(path)
	if err != nil {
		t.Fatalf("LoadVerifierFromFile: %v", err)
	}

	signer := NewSigner(key, "loadverifier-test")
	token, _, err := signer.Issue(Claims{TenantID: uuid.New(), Subject: "user-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if _, err := verifier.Verify(token); err != nil {
		t.Fatalf("expected token to verify, got error: %v", err)
	}
}

func TestLoadVerifierFromFile_MissingFile(t *testing.T) {
	if _, err := LoadVerifierFromFile(filepath.Join(t.TempDir(), "does-not-exist.pem")); err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}
