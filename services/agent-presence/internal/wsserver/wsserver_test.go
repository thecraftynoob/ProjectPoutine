package wsserver

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/thecraftynoob/ProjectPoutine/services/agent-presence/internal/registry"
)

func newTestServer(t *testing.T) (*httptest.Server, *registry.Registry) {
	t.Helper()
	reg := registry.New()
	s := New(reg, discardLogger())
	mux := http.NewServeMux()
	mux.Handle("/ws", s)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, reg
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(devNull{}, nil))
}

type devNull struct{}

func (devNull) Write(p []byte) (int, error) { return len(p), nil }

func wsURL(httpURL, query string) string {
	u := "ws" + strings.TrimPrefix(httpURL, "http")
	return u + "/ws?" + query
}

func TestUpgrade_ValidParams(t *testing.T) {
	srv, reg := newTestServer(t)

	tenantID := uuid.New().String()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL(srv.URL, "tenant_id="+tenantID+"&agent_id=agent-1"), nil)
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
		if _, ok := reg.Lookup(tenantID, "agent-1"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected connection to be registered")
		}
		time.Sleep(10 * time.Millisecond)
	}

	conn.Close(websocket.StatusNormalClosure, "")

	deadline = time.Now().Add(2 * time.Second)
	for {
		if _, ok := reg.Lookup(tenantID, "agent-1"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected connection to be unregistered after close")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestUpgrade_InvalidTenantID(t *testing.T) {
	srv, _ := newTestServer(t)

	resp, err := http.Get(srv.URL + "/ws?tenant_id=not-a-uuid&agent_id=agent-1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid tenant_id, got %d", resp.StatusCode)
	}
}

func TestUpgrade_MissingAgentID(t *testing.T) {
	srv, _ := newTestServer(t)

	tenantID := uuid.New().String()
	resp, err := http.Get(srv.URL + "/ws?tenant_id=" + tenantID)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing agent_id, got %d", resp.StatusCode)
	}
}

func TestUpgrade_MissingTenantID(t *testing.T) {
	srv, _ := newTestServer(t)

	resp, err := http.Get(srv.URL + "/ws?agent_id=agent-1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing tenant_id, got %d", resp.StatusCode)
	}
}

func TestDelivery_MessageReachesClient(t *testing.T) {
	srv, reg := newTestServer(t)

	tenantID := uuid.New().String()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL(srv.URL, "tenant_id="+tenantID+"&agent_id=agent-1"), nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.CloseNow()

	// Wait for registration.
	var wsConn registry.Connection
	deadline := time.Now().Add(2 * time.Second)
	for {
		c, ok := reg.Lookup(tenantID, "agent-1")
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
	reg := registry.New()
	s := New(reg, discardLogger())
	mux := http.NewServeMux()
	mux.Handle("/ws", s)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	tenantID := uuid.New().String()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL(srv.URL, "tenant_id="+tenantID+"&agent_id=agent-1"), nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.CloseNow()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := reg.Lookup(tenantID, "agent-1"); ok {
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
