package webhookapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeEnqueue builds an EnqueueTask func (the shape Handler.EnqueueTask
// expects) backed by a simple closure -- no real gRPC server needed to
// test webhookapi.Handler's own request validation/response-mapping
// logic in isolation, since Handler depends on nothing but that one
// function signature (see webhookapi.go's Handler.EnqueueTask field doc
// comment). This mirrors the narrow-fake-dependency style
// services/api-gateway/internal/httpapi_test.go uses for its own fake
// backend, just without needing a real network listener since this
// package's seam is a plain function, not a generated gRPC client
// interface.
func fakeEnqueue(fn func(ctx context.Context, tenantID string, req *taskrouterv1.EnqueueTaskRequest) (*taskrouterv1.Task, error)) *Handler {
	return &Handler{EnqueueTask: fn}
}

func newRequest(t *testing.T, path, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

// withTenantPath mounts h on a real http.ServeMux using the production
// route pattern (POST /webhooks/chat/{tenant_id}) so r.PathValue("tenant_id")
// is populated exactly as it would be in cmd/main.go -- httptest.NewRequest
// alone does not populate path wildcards without going through a mux that
// declares the pattern.
func withTenantPath(h *Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /webhooks/chat/{tenant_id}", h)
	return mux
}

func TestServeHTTP_ValidRequest_EnqueuesAndReturns201(t *testing.T) {
	var gotTenantID string
	var gotReq *taskrouterv1.EnqueueTaskRequest

	h := fakeEnqueue(func(ctx context.Context, tenantID string, req *taskrouterv1.EnqueueTaskRequest) (*taskrouterv1.Task, error) {
		gotTenantID = tenantID
		gotReq = req
		return &taskrouterv1.Task{
			TaskId:   "task-123",
			QueueId:  req.GetQueueId(),
			TaskType: req.GetTaskType(),
			Status:   "Pending",
		}, nil
	})

	body := `{"session_id":"sess-1","from":"+15550001111","text":"hello","queue_id":"queue-1","attributes":{"vip":{"is_bool":true,"bool_value":true},"priority":{"number_value":5}}}`
	req := newRequest(t, "/webhooks/chat/tenant-abc", body)
	rec := httptest.NewRecorder()

	withTenantPath(h).ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if gotTenantID != "tenant-abc" {
		t.Fatalf("expected tenant_id %q passed through, got %q", "tenant-abc", gotTenantID)
	}
	if gotReq.GetQueueId() != "queue-1" {
		t.Fatalf("expected queue_id %q, got %q", "queue-1", gotReq.GetQueueId())
	}
	if gotReq.GetTaskType() != "chat" {
		t.Fatalf("expected fixed task_type %q, got %q", "chat", gotReq.GetTaskType())
	}
	attrs := gotReq.GetRequiredAttributes()
	if v, ok := attrs["vip"]; !ok || !v.GetBoolValue() {
		t.Fatalf("expected vip=true bool attribute to map through, got %v", attrs["vip"])
	}
	if v, ok := attrs["priority"]; !ok || v.GetNumberValue() != 5 {
		t.Fatalf("expected priority=5 numeric attribute to map through, got %v", attrs["priority"])
	}

	var got taskResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if got.TaskID != "task-123" {
		t.Fatalf("expected taskId %q in response, got %q", "task-123", got.TaskID)
	}
}

func TestServeHTTP_MissingTenantID_404(t *testing.T) {
	h := fakeEnqueue(func(ctx context.Context, tenantID string, req *taskrouterv1.EnqueueTaskRequest) (*taskrouterv1.Task, error) {
		t.Fatal("EnqueueTask should not be called for a missing tenant_id")
		return nil, nil
	})

	// A trailing slash with an empty final path segment is the only way to
	// reach the handler with an empty tenant_id through the real mux
	// pattern (POST /webhooks/chat/{tenant_id}) -- ServeMux itself 404s
	// "/webhooks/chat" (no trailing segment) before ever routing to the
	// handler, so this exercises Handler's own empty-tenant_id guard
	// rather than the mux's routing.
	req := newRequest(t, "/webhooks/chat/", `{}`)
	rec := httptest.NewRecorder()

	withTenantPath(h).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing tenant_id, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestServeHTTP_MissingRequiredField_400(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"missing session_id", `{"from":"a","text":"hi","queue_id":"q1"}`},
		{"missing from", `{"session_id":"s1","text":"hi","queue_id":"q1"}`},
		{"missing text", `{"session_id":"s1","from":"a","queue_id":"q1"}`},
		{"missing queue_id", `{"session_id":"s1","from":"a","text":"hi"}`},
		{"empty body", `{}`},
		{"invalid json", `not json`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := fakeEnqueue(func(ctx context.Context, tenantID string, req *taskrouterv1.EnqueueTaskRequest) (*taskrouterv1.Task, error) {
				t.Fatal("EnqueueTask should not be called for an invalid body")
				return nil, nil
			})

			req := newRequest(t, "/webhooks/chat/tenant-abc", tc.body)
			rec := httptest.NewRecorder()

			withTenantPath(h).ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
			}
			var got errorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("unmarshal error response: %v", err)
			}
			if got.Error == "" {
				t.Fatal("expected a non-empty error message")
			}
		})
	}
}

func TestServeHTTP_QueueNotFound_MapsTo400(t *testing.T) {
	h := fakeEnqueue(func(ctx context.Context, tenantID string, req *taskrouterv1.EnqueueTaskRequest) (*taskrouterv1.Task, error) {
		return nil, status.Errorf(codes.InvalidArgument, "queue %q does not exist", req.GetQueueId())
	})

	body := `{"session_id":"sess-1","from":"a","text":"hi","queue_id":"nonexistent-queue"}`
	req := newRequest(t, "/webhooks/chat/tenant-abc", body)
	rec := httptest.NewRecorder()

	withTenantPath(h).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for queue-not-found-shaped EnqueueTask error, got %d: %s", rec.Code, rec.Body.String())
	}
	var got errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if !strings.Contains(got.Error, "nonexistent-queue") {
		t.Fatalf("expected error message to surface the queue-not-found detail, got %q", got.Error)
	}
}

func TestServeHTTP_InternalError_MapsTo502AndDoesNotLeakDetails(t *testing.T) {
	h := fakeEnqueue(func(ctx context.Context, tenantID string, req *taskrouterv1.EnqueueTaskRequest) (*taskrouterv1.Task, error) {
		return nil, status.Error(codes.Internal, "redis: connection refused at internal-host:6379 -- sensitive detail")
	})

	body := `{"session_id":"sess-1","from":"a","text":"hi","queue_id":"queue-1"}`
	req := newRequest(t, "/webhooks/chat/tenant-abc", body)
	rec := httptest.NewRecorder()

	withTenantPath(h).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 for an internal/unmapped EnqueueTask error, got %d: %s", rec.Code, rec.Body.String())
	}
	var got errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if strings.Contains(got.Error, "internal-host") || strings.Contains(got.Error, "redis:") {
		t.Fatalf("expected raw internal error details NOT to leak to the caller, got %q", got.Error)
	}
}

func TestServeHTTP_NonGRPCError_MapsTo502(t *testing.T) {
	h := fakeEnqueue(func(ctx context.Context, tenantID string, req *taskrouterv1.EnqueueTaskRequest) (*taskrouterv1.Task, error) {
		return nil, context.DeadlineExceeded
	})

	body := `{"session_id":"sess-1","from":"a","text":"hi","queue_id":"queue-1"}`
	req := newRequest(t, "/webhooks/chat/tenant-abc", body)
	rec := httptest.NewRecorder()

	withTenantPath(h).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 for a non-gRPC-status error, got %d: %s", rec.Code, rec.Body.String())
	}
}
