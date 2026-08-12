package grpcapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// newProductionHarness builds a bufconn gRPC server from the exact exported
// pieces main.go composes (FR-4/FR-5): keepalive parameters + enforcement
// policy, chain interceptors, health registration, and MarkServing. register
// runs before Serve starts (grpc-go forbids registering services after
// Serve), so AC-1/AC-2 drive the production wiring empirically; health tests
// pass a nil register (health is registered inside).
func newProductionHarness(t *testing.T, logger *log.Logger, register func(*grpc.Server)) (*grpc.Server, *health.Server, *bufconn.Listener) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(KeepaliveParams()),
		grpc.KeepaliveEnforcementPolicy(KeepaliveEnforcementPolicy()),
		grpc.ChainUnaryInterceptor(RecoveryUnaryServerInterceptor(logger)),
		grpc.ChainStreamInterceptor(RecoveryStreamServerInterceptor(logger)),
	)
	healthServer := RegisterHealth(grpcServer)
	MarkServing(healthServer)
	if register != nil {
		register(grpcServer)
	}
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	return grpcServer, healthServer, listener
}

// newBufconnClient dials the harness listener with the same client options
// the existing gRPC harness uses (passthrough + insecure bufconn dialer).
func newBufconnClient(t *testing.T, listener *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

// syncBuffer is a mutex-guarded bytes.Buffer for server-side log capture:
// log writes happen on handler goroutines while the test goroutine reads
// after the RPC completes, so -race needs the write side synchronized.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// switchableBackend is a store.Backend that keeps the snapshot in memory and
// can be told to panic on the next Save — simulating a backend fault inside
// Service.Ingest → Store.Update, the exact failure mode the recovery
// interceptor must contain (FR-1.1). The panic fires before persistence, so
// a failed Save never corrupts the snapshot; the follow-up write then
// ingests cleanly.
type switchableBackend struct {
	mu          sync.Mutex
	snapshot    *store.Snapshot
	panicOnSave bool
}

func newSwitchableBackend() *switchableBackend {
	return &switchableBackend{snapshot: store.NewSnapshot()}
}

func (b *switchableBackend) setPanicOnSave(v bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.panicOnSave = v
}

func (b *switchableBackend) Load() (*store.Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return cloneSnapshot(b.snapshot), nil
}

func (b *switchableBackend) LoadForUpdate() (*store.Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return cloneSnapshot(b.snapshot), nil
}

func (b *switchableBackend) Save(data *store.Snapshot) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.panicOnSave {
		panic("simulated backend Save panic: disk fault")
	}
	b.snapshot = cloneSnapshot(data)
	return nil
}

// cloneSnapshot deep-copies a snapshot through the same JSON serialization
// the file backend uses, so Load/LoadForUpdate always hand out private
// copies (Backend contract) and Save commits an immutable version.
func cloneSnapshot(s *store.Snapshot) *store.Snapshot {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	var copy store.Snapshot
	if err := json.Unmarshal(data, &copy); err != nil {
		panic(err)
	}
	return &copy
}

// assertRecoveredPanic asserts the client-visible redaction (M-1) and the
// server-side full-detail logging (value + stack) of a recovered panic.
func assertRecoveredPanic(t *testing.T, err error, logs *syncBuffer, panicValue string) {
	t.Helper()
	if status.Code(err) != codes.Internal {
		t.Fatalf("code=%v, want Internal", status.Code(err))
	}
	if message := status.Convert(err).Message(); message != "internal server error" {
		t.Fatalf("client message=%q, want fixed redacted text", message)
	}
	logged := logs.String()
	if !strings.Contains(logged, "grpc panic recovered") {
		t.Fatalf("panic not logged server-side: %q", logged)
	}
	if !strings.Contains(logged, panicValue) {
		t.Fatalf("panic value %q not logged: %q", panicValue, logged)
	}
	if !strings.Contains(logged, "goroutine ") {
		t.Fatalf("goroutine stack not logged: %q", logged)
	}
}

// TestRecoveryEndToEndStoreBackendPanic is AC-1's preferred route: a real
// Write drives Service.Ingest → Store.Update → backend.Save, whose Save
// panics (the cited failure mode). The interceptor must convert it to the
// redacted codes.Internal status, log the detail server-side, and leave the
// server fully functional for a subsequent successful Write.
func TestRecoveryEndToEndStoreBackendPanic(t *testing.T) {
	var logs syncBuffer
	logger := log.New(&logs, "", 0)

	backend := newSwitchableBackend()
	st := store.NewWithBackend(backend)
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
	_, _, listener := newProductionHarness(t, logger, func(srv *grpc.Server) {
		auditv1.RegisterIngestServer(srv, &Server{Service: svc, Auth: auth.Authenticator{AllowDev: true}, Logger: logger})
	})
	connection := newBufconnClient(t, listener)
	client := auditv1.NewIngestClient(connection)
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer dev:tenant-a:service:crm"))

	backend.setPanicOnSave(true)
	_, writeErr := client.Write(ctx, &auditv1.WriteRequest{Event: testProtoEvent("panic-evt-1", "crm")})
	assertRecoveredPanic(t, writeErr, &logs, "simulated backend Save panic")

	// The panicking Save never committed, so the follow-up write ingests
	// cleanly — proving the server goroutine and process survived (FR-1.3).
	backend.setPanicOnSave(false)
	receipt, err := client.Write(ctx, &auditv1.WriteRequest{Event: testProtoEvent("panic-evt-2", "crm")})
	if err != nil {
		t.Fatalf("Write after recovered panic failed: %v", err)
	}
	if receipt.GetEventId() != "panic-evt-2" || receipt.GetStatus() == "" {
		t.Fatalf("unexpected follow-up receipt: %+v", receipt)
	}
}

// panicOnceUnaryServer is a minimal IngestServer whose Write panics on the
// first call, then behaves normally — so a single test can assert both the
// containment (redaction + logging) and the survival (second call succeeds).
type panicOnceUnaryServer struct {
	auditv1.UnimplementedIngestServer
	mu       sync.Mutex
	panicked bool
}

func (s *panicOnceUnaryServer) Write(ctx context.Context, request *auditv1.WriteRequest) (*auditv1.WriteResponse, error) {
	s.mu.Lock()
	shouldPanic := !s.panicked
	s.panicked = true
	s.mu.Unlock()
	if shouldPanic {
		panic("simulated unary handler panic")
	}
	return &auditv1.WriteResponse{EventId: request.GetEvent().GetEventId(), Status: "ledgered"}, nil
}

// TestRecoveryHandlerLevelPanic is AC-1's handler-level route: a Write
// handler that panics unconditionally on the interceptor-equipped production
// harness. The panic is contained, redacted, logged, and the next RPC on the
// same connection succeeds.
func TestRecoveryHandlerLevelPanic(t *testing.T) {
	var logs syncBuffer
	_, _, listener := newProductionHarness(t, log.New(&logs, "", 0), func(srv *grpc.Server) {
		auditv1.RegisterIngestServer(srv, &panicOnceUnaryServer{})
	})
	connection := newBufconnClient(t, listener)
	client := auditv1.NewIngestClient(connection)

	_, err := client.Write(context.Background(), &auditv1.WriteRequest{Event: testProtoEvent("hpanic-1", "crm")})
	assertRecoveredPanic(t, err, &logs, "simulated unary handler panic")

	receipt, err := client.Write(context.Background(), &auditv1.WriteRequest{Event: testProtoEvent("hpanic-2", "crm")})
	if err != nil {
		t.Fatalf("Write after recovered panic failed: %v", err)
	}
	if receipt.GetEventId() != "hpanic-2" {
		t.Fatalf("unexpected follow-up receipt: %+v", receipt)
	}
}

// panicOnceStreamServer sends one receipt per request, panics after the first
// stream's first message, then behaves normally. This asserts FR-1.2's
// trailer-status semantics (already-sent messages are delivered, then the
// stream ends with codes.Internal) and FR-1.3 (a second stream on the same
// connection completes cleanly).
type panicOnceStreamServer struct {
	auditv1.UnimplementedIngestServer
	mu       sync.Mutex
	panicked bool
}

func (s *panicOnceStreamServer) WriteStream(stream auditv1.Ingest_WriteStreamServer) error {
	s.mu.Lock()
	shouldPanic := !s.panicked
	s.panicked = true
	s.mu.Unlock()
	for {
		request, receiveErr := stream.Recv()
		if receiveErr != nil {
			// Mirror the real Server.WriteStream: client CloseSend ends the
			// stream cleanly (returning io.EOF verbatim would surface as
			// codes.Unknown/EOF on the wire).
			if errors.Is(receiveErr, io.EOF) {
				return nil
			}
			return receiveErr
		}
		if err := stream.Send(&auditv1.WriteResponse{EventId: request.GetEvent().GetEventId(), Status: "ledgered"}); err != nil {
			return err
		}
		if shouldPanic {
			panic("simulated stream handler panic after send")
		}
	}
}

func TestRecoveryStreamPanic(t *testing.T) {
	var logs syncBuffer
	_, _, listener := newProductionHarness(t, log.New(&logs, "", 0), func(srv *grpc.Server) {
		auditv1.RegisterIngestServer(srv, &panicOnceStreamServer{})
	})
	connection := newBufconnClient(t, listener)
	client := auditv1.NewIngestClient(connection)

	// First stream: the receipt sent before the panic is delivered, then the
	// stream terminates with the redacted codes.Internal status (FR-1.2).
	stream, err := client.WriteStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&auditv1.WriteRequest{Event: testProtoEvent("spanic-1", "crm")}); err != nil {
		t.Fatal(err)
	}
	receipt, err := stream.Recv()
	if err != nil || receipt.GetEventId() != "spanic-1" {
		t.Fatalf("pre-panic receipt: %v %v", receipt, err)
	}
	_, streamErr := stream.Recv()
	assertRecoveredPanic(t, streamErr, &logs, "simulated stream handler panic after send")

	// Second stream on the same connection completes cleanly (FR-1.3): the
	// server survived the recovered panic and keeps serving.
	stream2, err := client.WriteStream(context.Background())
	if err != nil {
		t.Fatalf("WriteStream after recovered panic failed: %v", err)
	}
	if err := stream2.Send(&auditv1.WriteRequest{Event: testProtoEvent("spanic-2", "crm")}); err != nil {
		t.Fatal(err)
	}
	receipt2, err := stream2.Recv()
	if err != nil || receipt2.GetEventId() != "spanic-2" {
		t.Fatalf("post-panic stream receipt: %v %v", receipt2, err)
	}
	if err := stream2.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream2.Recv(); err != io.EOF {
		t.Fatalf("post-panic stream end=%v, want clean EOF", err)
	}
}

// errorThenOKServer returns a normal codes.NotFound status on the first
// Write and a receipt afterwards, pinning FR-1.5: a non-panicking handler's
// status error passes through the recovery interceptor unchanged — exact
// code and message, no redaction, no panic logging — and successful
// responses are unchanged too. (The status is what the real grpcapi.Server
// produces via toStatus for domain.ErrNotFound: codes.NotFound, "not found".)
type errorThenOKServer struct {
	auditv1.UnimplementedIngestServer
	mu     sync.Mutex
	failed bool
}

func (s *errorThenOKServer) Write(ctx context.Context, request *auditv1.WriteRequest) (*auditv1.WriteResponse, error) {
	s.mu.Lock()
	shouldFail := !s.failed
	s.failed = true
	s.mu.Unlock()
	if shouldFail {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return &auditv1.WriteResponse{EventId: request.GetEvent().GetEventId(), Status: "ledgered"}, nil
}

func TestRecoveryPreservesNormalErrors(t *testing.T) {
	var logs syncBuffer
	_, _, listener := newProductionHarness(t, log.New(&logs, "", 0), func(srv *grpc.Server) {
		auditv1.RegisterIngestServer(srv, &errorThenOKServer{})
	})
	connection := newBufconnClient(t, listener)
	client := auditv1.NewIngestClient(connection)

	_, err := client.Write(context.Background(), &auditv1.WriteRequest{Event: testProtoEvent("err-1", "crm")})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code=%v, want NotFound (pre-existing mapping)", status.Code(err))
	}
	if message := status.Convert(err).Message(); message != "not found" {
		t.Fatalf("message=%q, want exact pre-existing message (no redaction)", message)
	}
	if strings.Contains(logs.String(), "grpc panic recovered") {
		t.Fatalf("panic logged for a normal domain error: %q", logs.String())
	}

	receipt, err := client.Write(context.Background(), &auditv1.WriteRequest{Event: testProtoEvent("err-2", "crm")})
	if err != nil {
		t.Fatalf("successful Write failed: %v", err)
	}
	if receipt.GetEventId() != "err-2" {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	if strings.Contains(logs.String(), "grpc panic recovered") {
		t.Fatalf("panic logged for a successful RPC: %q", logs.String())
	}
}
