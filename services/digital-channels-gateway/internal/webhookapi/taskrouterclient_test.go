package webhookapi

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	tenantidentityv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/tenant-identity/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeIdentityServer is a minimal in-process IdentityService standing in
// for Tenant & Identity's IssueServiceToken RPC, purely to prove
// TenantScopedTaskRouterClient's mint/attach wiring works end-to-end --
// not to re-test Tenant & Identity's own IssueServiceToken validation
// logic (which has its own test suite,
// services/tenant-identity/internal/grpcapi/identity_servicetoken_test.go).
// Mirrors services/api-gateway/internal/httpapi_test.go's fake-backend
// style: a real loopback TCP listener, not an in-memory bufconn.
type fakeIdentityServer struct {
	tenantidentityv1.UnimplementedIdentityServiceServer

	wantSecret string
	calls      int
}

func (f *fakeIdentityServer) IssueServiceToken(ctx context.Context, req *tenantidentityv1.IssueServiceTokenRequest) (*tenantidentityv1.IssueServiceTokenResponse, error) {
	f.calls++
	// Echo back a fake, obviously-fake "token" that encodes which tenant
	// it was minted for, so the test can assert the right tenant_id
	// reached this RPC without needing a real signer/verifier here.
	return &tenantidentityv1.IssueServiceTokenResponse{
		Token:     "svc-token-for-" + req.GetTenantId(),
		ExpiresAt: timestamppb.New(time.Now().Add(time.Hour)),
	}, nil
}

// fakeTaskRouterServer records the bearer token it received per call, so
// the test can assert each call carried the tenant-scoped token minted
// for that specific tenant_id.
type fakeTaskRouterServer struct {
	taskrouterv1.UnimplementedTaskRouterServiceServer

	mu          sync.Mutex
	authPerCall []string // authorization header seen per call, in call order
}

func (f *fakeTaskRouterServer) EnqueueTask(ctx context.Context, req *taskrouterv1.EnqueueTaskRequest) (*taskrouterv1.Task, error) {
	auth := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("authorization"); len(vals) > 0 {
			auth = vals[0]
		}
	}
	f.mu.Lock()
	f.authPerCall = append(f.authPerCall, auth)
	f.mu.Unlock()
	return &taskrouterv1.Task{TaskId: "task-1", QueueId: req.GetQueueId(), TaskType: req.GetTaskType(), Status: "Pending"}, nil
}

func startFakeGRPCServer(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	return lis.Addr().String()
}

func TestTenantScopedTaskRouterClient_MintsPerTenantTokenAndAttachesIt(t *testing.T) {
	identity := &fakeIdentityServer{wantSecret: "shh"}
	identityAddr := startFakeGRPCServer(t, func(s *grpc.Server) {
		tenantidentityv1.RegisterIdentityServiceServer(s, identity)
	})

	taskRouter := &fakeTaskRouterServer{}
	taskRouterAddr := startFakeGRPCServer(t, func(s *grpc.Server) {
		taskrouterv1.RegisterTaskRouterServiceServer(s, taskRouter)
	})

	conn, err := grpc.NewClient(taskRouterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial task-router: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := NewTenantScopedTaskRouterClient(
		taskrouterv1.NewTaskRouterServiceClient(conn),
		identityAddr,
		"digital-channels-gateway",
		"shh",
	)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()

	// Two different tenants -- each should get its own minted token.
	task1, err := client.EnqueueTask(ctx, "tenant-a", &taskrouterv1.EnqueueTaskRequest{QueueId: "q1", TaskType: "chat"})
	if err != nil {
		t.Fatalf("EnqueueTask (tenant-a): %v", err)
	}
	if task1.GetTaskId() != "task-1" {
		t.Fatalf("expected task-1, got %q", task1.GetTaskId())
	}

	_, err = client.EnqueueTask(ctx, "tenant-b", &taskrouterv1.EnqueueTaskRequest{QueueId: "q2", TaskType: "chat"})
	if err != nil {
		t.Fatalf("EnqueueTask (tenant-b): %v", err)
	}

	// Same tenant again -- should reuse the cached token (no extra
	// IssueServiceToken call).
	_, err = client.EnqueueTask(ctx, "tenant-a", &taskrouterv1.EnqueueTaskRequest{QueueId: "q1", TaskType: "chat"})
	if err != nil {
		t.Fatalf("EnqueueTask (tenant-a again): %v", err)
	}

	if len(taskRouter.authPerCall) != 3 {
		t.Fatalf("expected 3 EnqueueTask calls recorded, got %d", len(taskRouter.authPerCall))
	}
	if taskRouter.authPerCall[0] != "Bearer svc-token-for-tenant-a" {
		t.Fatalf("expected tenant-a's call to carry tenant-a's token, got %q", taskRouter.authPerCall[0])
	}
	if taskRouter.authPerCall[1] != "Bearer svc-token-for-tenant-b" {
		t.Fatalf("expected tenant-b's call to carry tenant-b's token, got %q", taskRouter.authPerCall[1])
	}
	if taskRouter.authPerCall[2] != "Bearer svc-token-for-tenant-a" {
		t.Fatalf("expected tenant-a's second call to carry tenant-a's token again, got %q", taskRouter.authPerCall[2])
	}

	if identity.calls != 2 {
		t.Fatalf("expected exactly 2 IssueServiceToken calls (one per distinct tenant, cached on reuse), got %d", identity.calls)
	}
}
