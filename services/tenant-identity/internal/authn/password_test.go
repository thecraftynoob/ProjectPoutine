package authn

import "testing"

func TestHashAndVerifyPassword(t *testing.T) {
	hash, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	if hash == "" {
		t.Fatal("expected non-empty hash")
	}
	if hash == "correct horse battery staple" {
		t.Fatal("hash must not equal the plaintext password")
	}

	if !VerifyPassword(hash, "correct horse battery staple") {
		t.Error("expected correct password to verify")
	}
	if VerifyPassword(hash, "wrong password") {
		t.Error("expected incorrect password to fail verification")
	}
}

func TestHashPasswordProducesDifferentHashesForSameInput(t *testing.T) {
	// bcrypt salts each hash, so hashing the same password twice must
	// produce different output, even though both verify correctly.
	h1, err := HashPassword("same-password")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	h2, err := HashPassword("same-password")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	if h1 == h2 {
		t.Error("expected different salts to produce different hashes")
	}
	if !VerifyPassword(h1, "same-password") || !VerifyPassword(h2, "same-password") {
		t.Error("expected both hashes to verify against the original password")
	}
}
