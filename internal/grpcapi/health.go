package grpcapi

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// RegisterHealth registers the standard grpc.health.v1.Health service on
// srv and returns the health server handle so the caller owns SERVING/drain
// timing: call MarkServing after bootstrap completes (FR-2.2) and Shutdown
// before GracefulStop on shutdown (FR-2.3). Registering health twice on the
// same *grpc.Server panics in grpc-go ("service already registered") — call
// this exactly once per server.
func RegisterHealth(srv *grpc.Server) *health.Server {
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	return hs
}

// MarkServing pins the default (empty) service name to SERVING (FR-2.2).
// grpc-go's health.NewServer already defaults "" to SERVING; the explicit
// call states the bootstrap-ordering intent in the code and keeps the
// acceptance test deterministic against package-default drift. Idempotent.
func MarkServing(hs *health.Server) {
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
}
