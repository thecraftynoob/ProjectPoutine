package httpapi

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	tenantidentityv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/tenant-identity/v1"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/services/api-gateway/internal/wsticket"

	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// fakeTaskRouterServer is a minimal in-process TaskRouterService
// implementation, purely to prove grpc-gateway's HTTP<->gRPC translation
// and API Gateway's JWT-forwarding wiring actually works end-to-end --
// NOT to re-test Task Router's own domain logic (which has its own real
// test suite against services/task-router/internal/...). It records the
// incoming gRPC metadata so the test can assert the bearer token API
// Gateway validated on the REST request was faithfully forwarded to the
// backend (Layer 2 defense in depth, architecture doc Section 1.1).
type fakeTaskRouterServer struct {
	taskrouterv1.UnimplementedTaskRouterServiceServer
	taskrouterv1.UnimplementedTaskRouterAdminServiceServer

	lastAuthMetadata string
}

func (f *fakeTaskRouterServer) CreateAgent(ctx context.Context, req *taskrouterv1.CreateAgentRequest) (*taskrouterv1.Agent, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("authorization"); len(vals) > 0 {
			f.lastAuthMetadata = vals[0]
		}
	}
	return &taskrouterv1.Agent{AgentId: req.GetAgentId(), Status: "Offline"}, nil
}

func (f *fakeTaskRouterServer) GetAgent(ctx context.Context, req *taskrouterv1.GetAgentRequest) (*taskrouterv1.Agent, error) {
	return &taskrouterv1.Agent{AgentId: req.GetAgentId(), Status: "Available"}, nil
}

// fakeIdentityServer implements just enough of IdentityService for the
// exempt-route test (Login) below.
type fakeIdentityServer struct {
	tenantidentityv1.UnimplementedIdentityServiceServer
}

func (f *fakeIdentityServer) Login(ctx context.Context, req *tenantidentityv1.LoginRequest) (*tenantidentityv1.LoginResponse, error) {
	return &tenantidentityv1.LoginResponse{Token: "fake-issued-token"}, nil
}

type fakeTenantServer struct {
	tenantidentityv1.UnimplementedTenantServiceServer
}

func testKeypair(t *testing.T) (*jwtauth.Signer, *jwtauth.Verifier) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return jwtauth.NewSigner(key, "httpapi-test"), jwtauth.NewVerifier(&key.PublicKey)
}

// newTestHandler builds a full httpapi.Handler wired to a single shared
// in-process gRPC backend (standing in for BOTH Task Router and Tenant &
// Identity, since the fake server below registers both services' RPCs on
// the same listener -- httpapi.New dials each address independently in
// production, but for this test proving the ROUTING and AUTH wiring work,
// one shared backend is simpler and equally valid). The backend runs on a
// real loopback TCP listener (127.0.0.1:0, OS-assigned port) rather than
// an in-memory bufconn, so httpapi.New's normal grpc.NewClient(addr, ...)
// dial path is exercised exactly as production does it.
func newTestHandler(t *testing.T) (http.Handler, *jwtauth.Signer, *fakeTaskRouterServer) {
	t.Helper()
	signer, verifier := testKeypair(t)
	minter, err := wsticket.NewMinter(time.Minute)
	if err != nil {
		t.Fatalf("NewMinter: %v", err)
	}

	fakeTR := &fakeTaskRouterServer{}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	server := grpc.NewServer()
	taskrouterv1.RegisterTaskRouterServiceServer(server, fakeTR)
	taskrouterv1.RegisterTaskRouterAdminServiceServer(server, fakeTR)
	tenantidentityv1.RegisterIdentityServiceServer(server, &fakeIdentityServer{})
	tenantidentityv1.RegisterTenantServiceServer(server, &fakeTenantServer{})
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	addr := lis.Addr().String()

	handler, err := New(context.Background(), Config{
		TaskRouterGRPCAddr:     addr,
		TenantIdentityGRPCAddr: addr,
		Verifier:               verifier,
		TicketMinter:           minter,
		AgentPresenceWSURL:     "ws://127.0.0.1:0/ws",
	})
	if err != nil {
		t.Fatalf("httpapi.New: %v", err)
	}
	return handler, signer, fakeTR
}

func TestCreateAgent_RoundTripsThroughGatewayToBackend(t *testing.T) {
	handler, signer, fakeTR := newTestHandler(t)

	tenantID := uuid.New()
	token, _, err := signer.Issue(jwtauth.Claims{TenantID: tenantID, Subject: "user-1", Roles: []string{"admin"}}, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	body := strings.NewReader(`{"agent_id":"agent-42"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/agents", body)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal response: %v (body: %s)", err, rec.Body.String())
	}
	if got["agentId"] != "agent-42" {
		t.Fatalf("expected agentId %q in response, got %v", "agent-42", got["agentId"])
	}

	// Layer 2 defense in depth: the backend must have received the SAME
	// bearer token, forwarded automatically as outgoing gRPC metadata by
	// grpc-gateway (see internal/gwauth's package doc comment).
	if fakeTR.lastAuthMetadata != "Bearer "+token {
		t.Fatalf("expected backend to receive forwarded bearer token %q, got %q", "Bearer "+token, fakeTR.lastAuthMetadata)
	}
}

func TestGetAgent_RoundTripsThroughGatewayToBackend(t *testing.T) {
	handler, signer, _ := newTestHandler(t)

	token, _, err := signer.Issue(jwtauth.Claims{TenantID: uuid.New(), Subject: "user-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/agents/agent-7", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if got["agentId"] != "agent-7" {
		t.Fatalf("expected path param agent_id to reach the backend as agentId=%q, got %v", "agent-7", got["agentId"])
	}
}

func TestCreateAgent_NoToken_Rejected(t *testing.T) {
	handler, _, _ := newTestHandler(t)

	body := strings.NewReader(`{"agent_id":"agent-42"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/agents", body)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing token, got %d", rec.Code)
	}
}

func TestLogin_ExemptRoute_ReachesBackendWithNoToken(t *testing.T) {
	handler, _, _ := newTestHandler(t)

	body := strings.NewReader(`{"tenant_id":"` + uuid.New().String() + `","username":"alice","password":"secret"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for exempt Login route with no token, got %d: %s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if got["token"] != "fake-issued-token" {
		t.Fatalf("expected the fake backend's issued token to round-trip, got %v", got)
	}
}

func TestWSTicket_MintsTicketForAuthenticatedCaller(t *testing.T) {
	handler, signer, _ := newTestHandler(t)

	tenantID := uuid.New()
	token, _, err := signer.Issue(jwtauth.Claims{TenantID: tenantID, Subject: "agent-1"}, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/ws-ticket", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var got wsTicketResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if got.Ticket == "" {
		t.Fatal("expected a non-empty ticket")
	}
	if !got.ExpiresAt.After(time.Now()) {
		t.Fatalf("expected ExpiresAt in the future, got %v", got.ExpiresAt)
	}
}

func TestWSTicket_NoToken_Rejected(t *testing.T) {
	handler, _, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/ws-ticket", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing token on /v1/ws-ticket (not a bootstrapping route), got %d", rec.Code)
	}
}
