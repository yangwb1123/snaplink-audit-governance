package grpcapi

import (
	"context"
	"testing"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// TestHealthRegisteredAndServing is AC-2: the production construction path
// registers grpc.health.v1.Health (visible via GetServiceInfo) and reports
// SERVING for the default service after bootstrap-equivalent setup
// (MarkServing, FR-2.2).
func TestHealthRegisteredAndServing(t *testing.T) {
	grpcServer, _, listener := newProductionHarness(t, nil, nil)
	info := grpcServer.GetServiceInfo()
	if _, ok := info["grpc.health.v1.Health"]; !ok {
		t.Fatalf("grpc.health.v1.Health not registered; services: %v", info)
	}
	connection := newBufconnClient(t, listener)
	client := healthpb.NewHealthClient(connection)
	resp, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status=%v, want SERVING", resp.GetStatus())
	}
}

// TestHealthDrainAfterShutdown is AC-2's drain leg (FR-2.3): after
// healthServer.Shutdown() (which main.go calls before GracefulStop), probes
// observe NOT_SERVING.
func TestHealthDrainAfterShutdown(t *testing.T) {
	grpcServer, healthServer, listener := newProductionHarness(t, nil, nil)
	connection := newBufconnClient(t, listener)
	client := healthpb.NewHealthClient(connection)

	resp, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("pre-drain Check: %v %v", resp, err)
	}

	healthServer.Shutdown()

	resp, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("post-drain Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("status=%v, want NOT_SERVING after Shutdown", resp.GetStatus())
	}
	_ = grpcServer
}
