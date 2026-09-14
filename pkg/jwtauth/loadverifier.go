package jwtauth

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

// LoadVerifierFromFile reads a PEM-encoded ECDSA public key from path and
// constructs a Verifier from it. This is the standard way every service
// (other than tenant-identity itself, which already holds its private
// key in-process) obtains the Verifier it needs for
// pkg/tenantctx.UnaryServerInterceptor/StreamServerInterceptor: the key
// is distributed via a Kubernetes ConfigMap mounted as a file (see
// deploy/k8s/tenant-identity-public-key.example.yaml), and every
// service's cmd/main.go calls this exactly once at startup.
//
// Failure to load a valid key is treated as fatal by every caller (fail
// closed on a security control -- see pkg/tenantctx's doc comment): a
// service that cannot verify tokens must not start serving traffic it
// would otherwise silently trust.
func LoadVerifierFromFile(path string) (*Verifier, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("jwtauth: read public key file %q: %w", path, err)
	}
	return LoadVerifierFromPEM(pemBytes)
}

// LoadVerifierFromPEM constructs a Verifier from PEM-encoded bytes
// (SubjectPublicKeyInfo / PKIX DER, ECDSA P-256) -- the in-memory
// counterpart of LoadVerifierFromFile, useful for tests or any caller
// that already has the PEM bytes without a file on disk.
func LoadVerifierFromPEM(pemBytes []byte) (*Verifier, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("jwtauth: no PEM block found in public key data")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("jwtauth: parse public key: %w", err)
	}
	ecdsaPub, ok := pub.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("jwtauth: public key is not ECDSA (got %T)", pub)
	}
	return NewVerifier(ecdsaPub), nil
}
