package pgstore

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// KeyStore persists the service's single JWT-signing keypair. Like
// TenantStore, this holds a raw *pgxpool.Pool rather than a
// *pgtenant.Pool: the signing key is platform-level data with no
// tenant_id, not tenant-scoped data (see
// migrations/003_signing_keypair.sql for the full reasoning).
type KeyStore struct {
	pool *pgxpool.Pool
}

// NewKeyStore constructs a KeyStore over an unscoped connection pool.
func NewKeyStore(pool *pgxpool.Pool) *KeyStore {
	return &KeyStore{pool: pool}
}

// LoadOrGenerate returns the service's ECDSA P-256 signing keypair,
// generating and persisting a new one if the table is empty (first
// startup), or loading the existing one otherwise (every subsequent
// startup). This is the only place a keypair is ever created --
// subsequent calls across restarts always load the same key, so tokens
// signed before a restart remain verifiable after it.
func (s *KeyStore) LoadOrGenerate(ctx context.Context) (*ecdsa.PrivateKey, error) {
	// Fast path: try to load first.
	key, err := s.load(ctx)
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	// Not found: generate, then attempt to insert. Guard against a
	// concurrent first-startup race (e.g. two replicas booting
	// simultaneously against an empty table) by tolerating a unique/
	// primary-key conflict on insert and re-loading whatever the winner
	// persisted, rather than erroring out.
	newKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("pgstore: generate signing key: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(newKey)
	if err != nil {
		return nil, fmt.Errorf("pgstore: marshal signing key: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(der)

	_, err = s.pool.Exec(ctx, `
		INSERT INTO tenant_identity_signing_key (singleton_guard, private_key_pkcs8_b64)
		VALUES (1, $1)
		ON CONFLICT (singleton_guard) DO NOTHING
	`, encoded)
	if err != nil {
		return nil, fmt.Errorf("pgstore: persist signing key: %w", err)
	}

	// Re-load regardless of whether our insert won the race, so every
	// caller (including the loser of a concurrent-startup race) ends up
	// with the single, consistent, persisted key.
	return s.load(ctx)
}

// load reads the persisted keypair. Returns ErrNotFound if the table is
// empty.
func (s *KeyStore) load(ctx context.Context) (*ecdsa.PrivateKey, error) {
	var encoded string
	err := s.pool.QueryRow(ctx, `SELECT private_key_pkcs8_b64 FROM tenant_identity_signing_key WHERE singleton_guard = 1`).Scan(&encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("pgstore: load signing key: %w", err)
	}

	der, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("pgstore: decode signing key: %w", err)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, fmt.Errorf("pgstore: parse signing key: %w", err)
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("pgstore: persisted signing key is not an ECDSA key (got %T)", parsed)
	}
	return key, nil
}
