package wsserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/services/agent-presence/internal/registry"
)

// testKeypair generates a fresh ECDSA P-256 signer/verifier pair for
// tests, mirroring pkg/jwtauth's own test helper.
func testKeypair(t *testing.T) (*jwtauth.Signer, *jwtauth.Verifier) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return jwtauth.NewSigner(key, "wsserver-test"), jwtauth.NewVerifier(&key.PublicKey)
}

func newTestServer(t *testing.T) (*httptest.Server, *registry.Registry, *jwtauth.Signer) {
	t.Helper()
	srv, reg, signer, _ := newTestServerWithTicket(t)
	return srv, reg, signer
}

// newTestServerWithTicket is newTestServer's superset, additionally
// returning a ticket signer for tests exercising the ?ticket= auth path.
// The header-path signer/verifier and the ticket-path signer/verifier are
// deliberately DIFFERENT keypairs, mirroring production (Tenant &
// Identity's session key vs. API Gateway's dedicated ticket key -- see
// services/api-gateway/internal/wsticket's doc comment).
func newTestServerWithTicket(t *testing.T) (*httptest.Server, *registry.Registry, *jwtauth.Signer, *jwtauth.Signer) {
	t.Helper()
	signer, verifier := testKeypair(t)
	ticketSigner, ticketVerifier := testKeypair(t)
	reg := registry.New()
	s := New(reg, verifier, ticketVerifier, discardLogger())
	mux := http.NewServeMux()
	mux.Handle("/ws", s)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, reg, signer, ticketSigner
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(devNull{}, nil))
}

type devNull struct{}

func (devNull) Write(p []byte) (int, error) { return len(p), nil }

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http") + "/ws"
}

func dialOpts(token string) *websocket.DialOptions {
	if token == "" {
		return nil
	}
	return &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}},
	}
}

func TestUpgrade_ValidToken(t *testing.T) {
	srv, reg, signer := newTestServer(t)

	tenantID := uuid.New()
	token, _, err := signer.Issue(jwtauth.Claims{TenantID: tenantID, Subject: "agent-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL(srv.URL), dialOpts(token))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.CloseNow()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101 Switching Protocols, got %d", resp.StatusCode)
	}

	// Give the server goroutine a moment to register the connection.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := reg.Lookup(tenantID.String(), "agent-1"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected connection to be registered under tenant/agent derived from token claims")
		}
		time.Sleep(10 * time.Millisecond)
	}

	conn.Close(websocket.StatusNormalClosure, "")

	deadline = time.Now().Add(2 * time.Second)
	for {
		if _, ok := reg.Lookup(tenantID.String(), "agent-1"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected connection to be unregistered after close")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestUpgrade_MissingAuthorizationHeader(t *testing.T) {
	srv, _, _ := newTestServer(t)

	resp, err := http.Get(srv.URL + "/ws")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing Authorization header, got %d", resp.StatusCode)
	}
}

func TestUpgrade_MalformedAuthorizationHeader(t *testing.T) {
	srv, _, _ := newTestServer(t)

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/ws", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "not-a-bearer-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for malformed Authorization header, got %d", resp.StatusCode)
	}
}

func TestUpgrade_InvalidSignature(t *testing.T) {
	srv, _, _ := newTestServer(t)

	// Sign with a DIFFERENT key than the server's verifier trusts.
	otherSigner, _ := testKeypair(t)
	token, _, err := otherSigner.Issue(jwtauth.Claims{TenantID: uuid.New(), Subject: "agent-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/ws", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for a token signed by an untrusted key, got %d", resp.StatusCode)
	}
}

func TestUpgrade_ExpiredToken(t *testing.T) {
	srv, _, signer := newTestServer(t)

	token, _, err := signer.Issue(jwtauth.Claims{TenantID: uuid.New(), Subject: "agent-1"}, -time.Minute)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/ws", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for expired token, got %d", resp.StatusCode)
	}
}

func TestUpgrade_MalformedToken(t *testing.T) {
	srv, _, _ := newTestServer(t)

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/ws", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer not-a-jwt-at-all")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for malformed token, got %d", resp.StatusCode)
	}
}

func TestNew_PanicsOnNilVerifier(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected New to panic when constructed with a nil verifier")
		}
	}()
	New(registry.New(), nil, nil, discardLogger())
}

func TestDelivery_MessageReachesClient(t *testing.T) {
	srv, reg, signer := newTestServer(t)

	tenantID := uuid.New()
	token, _, err := signer.Issue(jwtauth.Claims{TenantID: tenantID, Subject: "agent-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL(srv.URL), dialOpts(token))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.CloseNow()

	// Wait for registration.
	var wsConn registry.Connection
	deadline := time.Now().Add(2 * time.Second)
	for {
		c, ok := reg.Lookup(tenantID.String(), "agent-1")
		if ok {
			wsConn = c
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected connection to be registered")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := wsConn.Send([]byte(`{"type":"agent.deleted","agentId":"agent-1","payload":{}}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer readCancel()
	typ, data, err := conn.Read(readCtx)
	if err != nil {
		t.Fatalf("client Read: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("expected text message, got %v", typ)
	}
	if string(data) != `{"type":"agent.deleted","agentId":"agent-1","payload":{}}` {
		t.Fatalf("unexpected payload: %s", data)
	}
}

func TestShutdownClosesConnections(t *testing.T) {
	signer, verifier := testKeypair(t)
	reg := registry.New()
	s := New(reg, verifier, nil, discardLogger())
	mux := http.NewServeMux()
	mux.Handle("/ws", s)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	tenantID := uuid.New()
	token, _, err := signer.Issue(jwtauth.Claims{TenantID: tenantID, Subject: "agent-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL(srv.URL), dialOpts(token))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.CloseNow()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := reg.Lookup(tenantID.String(), "agent-1"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected connection to be registered")
		}
		time.Sleep(10 * time.Millisecond)
	}

	s.Shutdown(context.Background())

	readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer readCancel()
	_, _, err = conn.Read(readCtx)
	if err == nil {
		t.Fatalf("expected client read to fail after server-initiated shutdown")
	}
}

// --- ?ticket= auth path (services/api-gateway/internal/wsticket's
// counterpart) ---

// ticketWsURL builds the ws:// upgrade URL with a ?ticket= parameter, for
// tests that actually perform a WebSocket Dial.
func ticketWsURL(httpURL, ticket string) string {
	return wsURL(httpURL) + "?ticket=" + ticket
}

// ticketHTTPURL builds the plain http:// URL with a ?ticket= parameter,
// for tests that expect the upgrade to be rejected before ever reaching
// the WebSocket handshake (a plain http.Get, mirroring how the existing
// header-path rejection tests call http.Get(srv.URL+"/ws") rather than
// attempting a real Dial).
func ticketHTTPURL(httpURL, ticket string) string {
	return httpURL + "/ws?ticket=" + ticket
}

func TestUpgrade_ValidTicket(t *testing.T) {
	srv, reg, _, ticketSigner := newTestServerWithTicket(t)

	tenantID := uuid.New()
	ticket, _, err := ticketSigner.Issue(jwtauth.Claims{TenantID: tenantID, Subject: "agent-1"}, time.Minute)
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, ticketWsURL(srv.URL, ticket), nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.CloseNow()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101 Switching Protocols, got %d", resp.StatusCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := reg.Lookup(tenantID.String(), "agent-1"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected connection to be registered under tenant/agent derived from ticket claims")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestUpgrade_TicketSignedByWrongKey(t *testing.T) {
	srv, _, _, _ := newTestServerWithTicket(t)

	// Sign with a key the server's ticketVerifier does NOT trust (e.g. the
	// session-JWT signer, or any other unrelated key) -- must be rejected
	// just like an untrusted session JWT is.
	otherSigner, _ := testKeypair(t)
	ticket, _, err := otherSigner.Issue(jwtauth.Claims{TenantID: uuid.New(), Subject: "agent-1"}, time.Minute)
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}

	resp, err := http.Get(ticketHTTPURL(srv.URL, ticket))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for a ticket signed by an untrusted key, got %d", resp.StatusCode)
	}
}

func TestUpgrade_ExpiredTicket(t *testing.T) {
	srv, _, _, ticketSigner := newTestServerWithTicket(t)

	ticket, _, err := ticketSigner.Issue(jwtauth.Claims{TenantID: uuid.New(), Subject: "agent-1"}, -time.Second)
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}

	resp, err := http.Get(ticketHTTPURL(srv.URL, ticket))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for an expired ticket, got %d", resp.StatusCode)
	}
}

func TestUpgrade_TicketPathDisabledWhenNoTicketVerifierConfigured(t *testing.T) {
	// A deployment that has not configured a ticket verifier (nil) must
	// reject every ?ticket= attempt rather than panic or silently accept
	// it -- see New's doc comment.
	_, verifier := testKeypair(t)
	reg := registry.New()
	s := New(reg, verifier, nil, discardLogger())
	mux := http.NewServeMux()
	mux.Handle("/ws", s)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	signer, _ := testKeypair(t)
	ticket, _, err := signer.Issue(jwtauth.Claims{TenantID: uuid.New(), Subject: "agent-1"}, time.Minute)
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}

	resp, err := http.Get(ticketHTTPURL(srv.URL, ticket))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 when no ticket verifier is configured, got %d", resp.StatusCode)
	}
}
