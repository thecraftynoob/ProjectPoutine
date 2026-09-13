// Package health registers the standard gRPC health-checking protocol
// (grpc.health.v1) on a service's gRPC server, so every service is
// checkable the same way (kubectl exec grpc_health_probe, a liveness/
// readiness probe, etc.) without each service reimplementing it.
package health

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Register attaches a standard grpc_health_v1.Health server to server and
// marks the overall server status as SERVING. Call this once, after
// registering domain services and before server.Serve(...).
func Register(server *grpc.Server) {
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
}
