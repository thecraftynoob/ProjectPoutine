package webhookapi

import (
	"context"
	"fmt"
	"sync"

	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	"github.com/thecraftynoob/ProjectPoutine/pkg/jwtauth"
	"github.com/thecraftynoob/ProjectPoutine/pkg/svcauth"
	"google.golang.org/grpc/metadata"
)

// TenantScopedTaskRouterClient calls Task Router's EnqueueTask RPC as a
// service (not a user), minting a fresh, tenant-scoped service JWT per
// target tenant via pkg/svcauth and attaching it as outgoing gRPC
// metadata -- see that package's doc comment for the full mint/cache/
// refresh/attach mechanism this wraps.
//
// A webhook request's tenant_id is only known once the URL path is
// parsed (it is NOT known at process-startup time the way every other
// current svcauth caller in this repo assumes -- task-router and
// agent-presence each mint tokens for a single, fixed tenant baked in at
// call time from an already-authenticated caller's own JWT, not a path
// segment on an unauthenticated inbound request). This type therefore
// keeps one *jwtauth.TokenSource PER OBSERVED tenant_id, created lazily
// on first use and cached for the lifetime of the process -- each
// TokenSource already caches and refreshes its own token internally (see
// pkg/jwtauth.TokenSource), so this cache only avoids repeatedly calling
// IssueServiceToken for a tenant this gateway sees repeated webhook
// traffic from. The gRPC connection to Task Router itself is dialed once
// at construction and shared across all tenants (Task Router's gRPC
// transport is not tenant-scoped -- only the bearer token per call is),
// and the single connection to Tenant & Identity used to mint tokens is
// likewise shared (via pkg/svcauth.TokenSource, one dial reused for every
// tenant's IssueServiceToken call).
//
// DESIGN NOTE (flagged for review): unlike every other current svcauth
// caller in this repo, this is a genuinely new usage pattern -- per-
// request tenant scoping instead of a single fixed tenant baked in at
// startup. The unbounded, never-evicted map below is judged acceptable
// for this milestone (the set of tenants calling this gateway's webhook
// endpoint is small and operator-provisioned, not attacker-controlled
// cardinality, since the webhook's own undefended-endpoint tradeoff --
// see webhookapi.go's ServeHTTP doc comment -- already requires a real
// per-tenant webhook URL to be provisioned first) but a production build
// fielding many tenants, or wanting to bound memory against a malicious
// caller hitting many different tenant_id path segments, would want an
// LRU/TTL eviction policy here instead of a plain unbounded map.
type TenantScopedTaskRouterClient struct {
	client             taskrouterv1.TaskRouterServiceClient
	tenantIdentityAddr string
	callerService      string
	sharedSecret       string

	mu      sync.Mutex
	sources map[string]*jwtauth.TokenSource
	closers []func() error
}

// NewTenantScopedTaskRouterClient constructs a client bound to a single,
// already-dialed connection to Task Router (taskRouterClient) and the
// parameters needed to mint service tokens per tenant via
// pkg/svcauth.TokenSource: tenantIdentityAddr (Tenant & Identity's gRPC
// address), callerService (this service's name, for the minted token's
// diagnostic "sub" suffix), and sharedSecret (the shared service
// credential -- deploy/k8s/service-credential.example.yaml).
func NewTenantScopedTaskRouterClient(taskRouterClient taskrouterv1.TaskRouterServiceClient, tenantIdentityAddr, callerService, sharedSecret string) *TenantScopedTaskRouterClient {
	return &TenantScopedTaskRouterClient{
		client:             taskRouterClient,
		tenantIdentityAddr: tenantIdentityAddr,
		callerService:      callerService,
		sharedSecret:       sharedSecret,
		sources:            make(map[string]*jwtauth.TokenSource),
	}
}

// Close releases every per-tenant Tenant & Identity connection opened
// lazily by tokenSourceFor. Does NOT close the Task Router connection
// passed into NewTenantScopedTaskRouterClient -- that connection's
// lifecycle belongs to whoever dialed it (cmd/main.go).
func (c *TenantScopedTaskRouterClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for _, closeFn := range c.closers {
		if err := closeFn(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *TenantScopedTaskRouterClient) tokenSourceFor(tenantID string) (*jwtauth.TokenSource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if src, ok := c.sources[tenantID]; ok {
		return src, nil
	}

	src, closeFn, err := svcauth.TokenSource(c.tenantIdentityAddr, tenantID, c.callerService, c.sharedSecret)
	if err != nil {
		return nil, fmt.Errorf("webhookapi: build token source for tenant %q: %w", tenantID, err)
	}
	c.sources[tenantID] = src
	c.closers = append(c.closers, closeFn)
	return src, nil
}

// EnqueueTask mints (or reuses a cached) service token scoped to
// tenantID, attaches it as outgoing "authorization: Bearer <token>"
// metadata, and calls Task Router's EnqueueTask RPC.
func (c *TenantScopedTaskRouterClient) EnqueueTask(ctx context.Context, tenantID string, req *taskrouterv1.EnqueueTaskRequest) (*taskrouterv1.Task, error) {
	src, err := c.tokenSourceFor(tenantID)
	if err != nil {
		return nil, err
	}

	token, err := src.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("webhookapi: obtain service token for tenant %q: %w", tenantID, err)
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	return c.client.EnqueueTask(ctx, req)
}
