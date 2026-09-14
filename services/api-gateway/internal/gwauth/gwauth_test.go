package gwauth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
)

func testKeypair(t *testing.T) (*jwtauth.Signer, *jwtauth.Verifier) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return jwtauth.NewSigner(key, "gwauth-test"), jwtauth.NewVerifier(&key.PublicKey)
}

func passThroughHandler() (http.Handler, *bool, *http.Request) {
	called := new(bool)
	var captured *http.Request
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*called = true
		captured = r
		w.WriteHeader(http.StatusOK)
	})
	return h, called, captured
}

func TestMiddleware_ValidToken_ForwardsToNext(t *testing.T) {
	signer, verifier := testKeypair(t)
	token, _, err := signer.Issue(jwtauth.Claims{TenantID: uuid.New(), Subject: "user-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	var nextCalled bool
	var gotAuthHeader string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		gotAuthHeader = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	})

	mw := Middleware(verifier, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/agents", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	if !nextCalled {
		t.Fatal("expected next handler to be called for a valid token")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	// The original Authorization header must still be present on the
	// request reaching next -- grpc-gateway forwards it itself (see
	// package doc comment); this middleware must not strip it.
	if gotAuthHeader != "Bearer "+token {
		t.Fatalf("expected Authorization header to be preserved for downstream forwarding, got %q", gotAuthHeader)
	}
}

func TestMiddleware_MissingToken_Rejected(t *testing.T) {
	_, verifier := testKeypair(t)
	next, called, _ := passThroughHandler()
	mw := Middleware(verifier, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/agents", nil)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	if *called {
		t.Fatal("expected next handler NOT to be called for a missing token")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestMiddleware_MalformedHeader_Rejected(t *testing.T) {
	_, verifier := testKeypair(t)
	next, called, _ := passThroughHandler()
	mw := Middleware(verifier, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/agents", nil)
	req.Header.Set("Authorization", "not-a-bearer-token")
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	if *called {
		t.Fatal("expected next handler NOT to be called for a malformed header")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestMiddleware_InvalidSignature_Rejected(t *testing.T) {
	_, verifier := testKeypair(t)
	otherSigner, _ := testKeypair(t)
	token, _, err := otherSigner.Issue(jwtauth.Claims{TenantID: uuid.New(), Subject: "user-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	next, called, _ := passThroughHandler()
	mw := Middleware(verifier, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/agents", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	if *called {
		t.Fatal("expected next handler NOT to be called for a token signed by an untrusted key")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestMiddleware_ExpiredToken_Rejected(t *testing.T) {
	signer, verifier := testKeypair(t)
	token, _, err := signer.Issue(jwtauth.Claims{TenantID: uuid.New(), Subject: "user-1"}, -time.Minute)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	next, called, _ := passThroughHandler()
	mw := Middleware(verifier, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/agents", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	if *called {
		t.Fatal("expected next handler NOT to be called for an expired token")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestMiddleware_ExemptRoutes_BypassVerificationEntirely(t *testing.T) {
	_, verifier := testKeypair(t)

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/tenants"},
		{http.MethodGet, "/v1/tenants"},
		{http.MethodGet, "/v1/tenants/some-tenant-id"},
		{http.MethodPost, "/v1/auth/login"},
		{http.MethodPost, "/v1/tenants/some-tenant-id/users"},
		{http.MethodGet, "/ws"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			next, called, _ := passThroughHandler()
			mw := Middleware(verifier, next)

			// Deliberately NO Authorization header at all.
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			mw.ServeHTTP(rec, req)

			if !*called {
				t.Fatalf("expected exempt route %s %s to bypass verification and reach next handler", tc.method, tc.path)
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200 for exempt route with no token, got %d", rec.Code)
			}
		})
	}
}

func TestMiddleware_NonExemptRoutes_RequireToken(t *testing.T) {
	_, verifier := testKeypair(t)

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/agents"},
		{http.MethodGet, "/v1/agents"},
		{http.MethodGet, "/v1/agents/agent-1"},
		{http.MethodPost, "/v1/tasks"},
		{http.MethodPost, "/v1/ws-ticket"},
		{http.MethodGet, "/v1/dashboard"},
		{http.MethodGet, "/v1/users"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			next, called, _ := passThroughHandler()
			mw := Middleware(verifier, next)

			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			mw.ServeHTTP(rec, req)

			if *called {
				t.Fatalf("expected non-exempt route %s %s to require a token", tc.method, tc.path)
			}
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d", rec.Code)
			}
		})
	}
}

func TestMiddleware_PanicsOnNilVerifier(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected Middleware to panic when constructed with a nil verifier")
		}
	}()
	Middleware(nil, http.NotFoundHandler())
}
