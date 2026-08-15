package grpcapi

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

// TestProductionWiringStatic pins AC-3: the production construction path in
// cmd/audit-api/main.go must compose the exported grpcapi pieces — chain
// interceptors, keepalive parameters, TLS server credentials, health
// registration, SERVING after bootstrap, and health drain before
// GracefulStop. It fails closed on identifier removal. It is intentionally
// identifier-level (not line-number-level) so formatting or line moves do not
// break it; AC-1 and AC-2 exercise the same construction path empirically
// (AC-3.3 cross-check).
func TestProductionWiringStatic(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "..", "cmd", "audit-api", "main.go"))
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	text := string(source)
	for _, want := range []string{
		"grpc.KeepaliveParams(grpcapi.KeepaliveParams",
		"grpc.KeepaliveEnforcementPolicy(grpcapi.KeepaliveEnforcementPolicy",
		"grpc.MaxRecvMsgSize(grpcapi.MaxRecvBytes",
		"grpc.ChainUnaryInterceptor(grpcapi.RecoveryUnaryServerInterceptor",
		"grpc.ChainStreamInterceptor(grpcapi.RecoveryStreamServerInterceptor",
		"grpcapi.RegisterHealth(",
		"grpcapi.MarkServing(",
		// TLS wiring (A1 companion): the production construction path must keep
		// loading server credentials through the exported grpcapi seam and
		// pass them to grpc.Creds, and the -grpc-tls-cert/-grpc-tls-key flags
		// must exist — the static pins fail closed on identifier removal, so
		// the transport can never silently drop back to plaintext.
		"grpcapi.ServerCredentials(",
		"grpc.Creds(",
		"-grpc-tls-cert",
		"-grpc-tls-key",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("production construction path missing %q", want)
		}
	}
	shutdown := strings.Index(text, "healthServer.Shutdown()")
	graceful := strings.Index(text, "grpcServer.GracefulStop()")
	if shutdown < 0 {
		t.Error("healthServer.Shutdown() missing from shutdown path")
	}
	if graceful < 0 {
		t.Error("grpcServer.GracefulStop() missing from shutdown path")
	}
	if shutdown >= 0 && graceful >= 0 && shutdown > graceful {
		t.Error("healthServer.Shutdown() must precede grpcServer.GracefulStop() (FR-2.3)")
	}
	// AC-1 (REQ-1): the configured gRPC receive cap is exactly the HTTP
	// request-body cap (domain.MaxEventBytes*2), derived from the same
	// constant so HTTP/gRPC parity cannot drift independently.
	if MaxRecvBytes != domain.MaxEventBytes*2 {
		t.Errorf("MaxRecvBytes=%d, want domain.MaxEventBytes*2=%d (HTTP request-body cap)", MaxRecvBytes, domain.MaxEventBytes*2)
	}
}
