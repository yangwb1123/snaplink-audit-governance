package grpcapi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// generateTestCertificate creates a self-signed server certificate for
// localhost/127.0.0.1/::1 at test time (no checked-in private key). The
// short validity window is fine for the in-process harness; check-config
// never inspects expiry (REQ-7).
func generateTestCertificate(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM
}

// writeTestTLSFiles writes a fresh self-signed cert/key pair into a private
// temp directory and returns the PEM file paths.
func writeTestTLSFiles(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	certPEM, keyPEM := generateTestCertificate(t)
	dir := t.TempDir()
	certFile = filepath.Join(dir, "server.crt")
	keyFile = filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// TestServerCredentials covers REQ-2.1: a valid PEM pair loads into grpc
// transport credentials; missing files and a key that does not match the
// cert are hard errors (never a silent fallback to plaintext).
func TestServerCredentials(t *testing.T) {
	t.Run("valid pair loads", func(t *testing.T) {
		certFile, keyFile := writeTestTLSFiles(t)
		creds, err := ServerCredentials(certFile, keyFile)
		if err != nil {
			t.Fatalf("ServerCredentials: %v", err)
		}
		if creds == nil {
			t.Fatal("creds must be non-nil")
		}
	})
	t.Run("missing files error", func(t *testing.T) {
		_, err := ServerCredentials(filepath.Join(t.TempDir(), "nope.crt"), filepath.Join(t.TempDir(), "nope.key"))
		if err == nil {
			t.Fatal("missing PEM files must error")
		}
	})
	t.Run("mismatched key errors", func(t *testing.T) {
		certFile, _ := writeTestTLSFiles(t)
		_, keyFile := writeTestTLSFiles(t) // a different pair's key
		if _, err := ServerCredentials(certFile, keyFile); err == nil {
			t.Fatal("a key that does not match the cert must error")
		}
	})
	t.Run("empty inputs error", func(t *testing.T) {
		if _, err := ServerCredentials("", ""); err == nil {
			t.Fatal("empty cert/key paths must error")
		}
	})
}

func TestMutualTLSCredentials(t *testing.T) {
	certFile, keyFile := writeTestTLSFiles(t)
	t.Run("valid CA bundle loads", func(t *testing.T) {
		certPEM, _ := generateTestCertificate(t)
		caFile := filepath.Join(t.TempDir(), "client-ca.pem")
		if err := os.WriteFile(caFile, certPEM, 0o600); err != nil {
			t.Fatal(err)
		}
		creds, err := MutualTLSCredentials(certFile, keyFile, caFile)
		if err != nil || creds == nil {
			t.Fatalf("MutualTLSCredentials=%v, want valid credentials", err)
		}
	})
	t.Run("missing CA fails", func(t *testing.T) {
		_, err := MutualTLSCredentials(certFile, keyFile, filepath.Join(t.TempDir(), "missing-ca.pem"))
		if err == nil {
			t.Fatal("missing client CA must fail closed")
		}
	})
	t.Run("malformed CA fails", func(t *testing.T) {
		caFile := filepath.Join(t.TempDir(), "bad-ca.pem")
		if err := os.WriteFile(caFile, []byte("not a certificate"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := MutualTLSCredentials(certFile, keyFile, caFile); err == nil {
			t.Fatal("malformed client CA must fail closed")
		}
	})
}

// newGRPCTLSHarness builds a real loopback gRPC ingest listener with TLS
// credentials (A1/A2 server): the production option set (keepalive, receive
// cap, recovery interceptors — panic-recovery outermost) plus grpc.Creds
// from ServerCredentials over a self-signed PEM pair. Returns the store
// (ledger assertions), the service (receipt/snapshot assertions) and the
// resolved listener address.
func newGRPCTLSHarness(t *testing.T) (*store.Store, *service.Service, string) {
	t.Helper()
	certFile, keyFile := writeTestTLSFiles(t)
	creds, err := ServerCredentials(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(
		grpc.Creds(creds),
		grpc.KeepaliveParams(KeepaliveParams()),
		grpc.KeepaliveEnforcementPolicy(KeepaliveEnforcementPolicy()),
		grpc.MaxRecvMsgSize(MaxRecvBytes),
		grpc.ChainUnaryInterceptor(RecoveryUnaryServerInterceptor(log.New(io.Discard, "", 0))),
		grpc.ChainStreamInterceptor(RecoveryStreamServerInterceptor(log.New(io.Discard, "", 0))),
	)
	Register(grpcServer, svc, auth.Authenticator{AllowDev: true})
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	return st, svc, listener.Addr().String()
}

// TestGRPCTLSWriteEndToEnd is A1: with grpc-tls-cert/key configured (here
// via ServerCredentials + grpc.Creds), a grpc.NewClient with transport
// credentials succeeds against the listener and Write works end-to-end —
// receipt (tenant, sequence, hash) and the event in the store snapshot.
func TestGRPCTLSWriteEndToEnd(t *testing.T) {
	_, svc, addr := newGRPCTLSHarness(t)
	connection, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true, ServerName: "localhost"})))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := auditv1.NewIngestClient(connection)
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer dev:tenant-a:service:crm"))
	receipt, err := client.Write(ctx, &auditv1.WriteRequest{Event: testProtoEvent("grpc-tls-evt-1", "crm")})
	if err != nil {
		t.Fatalf("Write over TLS failed: %v", err)
	}
	if receipt.GetTenantId() != "tenant-a" || receipt.GetSequence() != 1 || receipt.GetHash() == "" {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	ledgered, err := svc.GetEvent("tenant-a", "test", "grpc-tls-evt-1")
	if err != nil {
		t.Fatalf("event must be in the store snapshot: %v", err)
	}
	if ledgered.EventID != "grpc-tls-evt-1" || ledgered.SourceSystem != "crm" {
		t.Fatalf("unexpected ledgered event: %+v", ledgered)
	}
}

// TestGRPCTLSRejectsPlaintextClient is A2: with TLS configured, a plaintext
// (no-creds) client fails to establish the RPC. Per grpc-go the failure
// surfaces at transport level (codes.Unavailable connection error, not an
// application status), and the store snapshot contains no event from the
// plaintext attempt — nothing was ledgered.
func TestGRPCTLSRejectsPlaintextClient(t *testing.T) {
	st, _, addr := newGRPCTLSHarness(t)
	connection, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := auditv1.NewIngestClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer dev:tenant-a:service:crm"))
	_, writeErr := client.Write(ctx, &auditv1.WriteRequest{Event: testProtoEvent("grpc-tls-plaintext-rejected", "crm")})
	if writeErr == nil {
		t.Fatal("plaintext Write against a TLS listener must fail")
	}
	if code := status.Code(writeErr); code != codes.Unavailable {
		t.Fatalf("plaintext failure code=%v, want transport-level codes.Unavailable (got %v)", code, writeErr)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if _, exists := data.Events[store.EventKey("tenant-a", "grpc-tls-plaintext-rejected")]; exists {
			t.Error("plaintext attempt was ledgered against a TLS listener")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Positive control on the same server proves the failure is transport
	// level, not auth level: a TLS client with the same bearer token succeeds.
	tlsConn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true, ServerName: "localhost"})))
	if err != nil {
		t.Fatal(err)
	}
	defer tlsConn.Close()
	tlsClient := auditv1.NewIngestClient(tlsConn)
	ctx = metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer dev:tenant-a:service:crm"))
	if _, err := tlsClient.Write(ctx, &auditv1.WriteRequest{Event: testProtoEvent("grpc-tls-positive-control", "crm")}); err != nil {
		t.Fatalf("TLS client on the same server failed: %v", err)
	}
}
