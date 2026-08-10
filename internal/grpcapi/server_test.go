package grpcapi

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWriteAndBatchOverGRPC(t *testing.T) {
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
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "erp", Name: "ERP", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	Register(grpcServer, svc, auth.Authenticator{AllowDev: true})
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := auditv1.NewIngestClient(connection)
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer dev:tenant-a:service:crm"))
	request := &auditv1.WriteRequest{Event: &auditv1.EventEnvelope{EventId: "grpc-evt-1", SourceSystem: "crm", EventType: "audit.event", SchemaId: "audit.event", SchemaVersion: 1, OccurredAt: timestamppb.New(time.Unix(1_700_000_010, 0).UTC()), Actor: &auditv1.Actor{Id: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "grpc-idem-1", WorkflowInstanceId: "wf-1", ExecutionRunId: "run-1", PayloadJson: []byte(`{"value":1}`)}, WaitFor: "ledgered"}
	receipt, err := client.Write(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.GetTenantId() != "tenant-a" || receipt.GetSequence() != 1 {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	// M1: the WriteResponse stream_id is a read-only receipt field; with no
	// aggregate on the envelope the stream is server-derived to the source.
	if receipt.GetStreamId() != "tenant-a:source:crm" {
		t.Fatalf("unexpected derived stream on receipt: %+v", receipt)
	}
	// With aggregate fields set, the receipt stream is derived from
	// tenant + aggregate/operation/source — never client-controlled.
	aggregateRequest := &auditv1.WriteRequest{Event: &auditv1.EventEnvelope{EventId: "grpc-evt-agg", SourceSystem: "crm", EventType: "audit.event", SchemaId: "audit.event", SchemaVersion: 1, OccurredAt: timestamppb.New(time.Unix(1_700_000_012, 0).UTC()), Actor: &auditv1.Actor{Id: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "grpc-idem-agg", AggregateType: "invoice", AggregateId: "inv-1", PayloadJson: []byte(`{"value":3}`)}, WaitFor: "ledgered"}
	aggregateReceipt, err := client.Write(ctx, aggregateRequest)
	if err != nil {
		t.Fatal(err)
	}
	if aggregateReceipt.GetStreamId() != "tenant-a:aggregate:invoice:inv-1" {
		t.Fatalf("aggregate-derived stream mismatch: %+v", aggregateReceipt)
	}
	ledgered, err := svc.GetEvent("tenant-a", "test", "grpc-evt-1")
	if err != nil {
		t.Fatal(err)
	}
	if ledgered.WorkflowInstanceID != "wf-1" || ledgered.ExecutionRunID != "run-1" {
		t.Fatalf("governance fields lost over gRPC: %+v", ledgered)
	}
	batch, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{{EventId: "grpc-evt-2", SourceSystem: "crm", EventType: "audit.event", SchemaId: "audit.event", SchemaVersion: 1, OccurredAt: timestamppb.New(time.Unix(1_700_000_011, 0).UTC()), Actor: &auditv1.Actor{Id: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "grpc-idem-2", PayloadJson: []byte(`{"value":2}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.GetReceipts()) != 1 || batch.GetReceipts()[0].GetSequence() != 2 {
		t.Fatalf("unexpected batch: %+v", batch)
	}
	assertGRPCSourceBinding(t, client, ctx)
}

func assertGRPCSourceBinding(t *testing.T, client auditv1.IngestClient, ctx context.Context) {
	t.Helper()
	spoof := testProtoEvent("grpc-spoof", "erp")
	_, writeErr := client.Write(ctx, &auditv1.WriteRequest{Event: spoof})
	assertPermissionDenied(t, writeErr)
	unknown := testProtoEvent("grpc-unknown", "unknown")
	_, unknownErr := client.Write(ctx, &auditv1.WriteRequest{Event: unknown})
	assertPermissionDenied(t, unknownErr)
	if status.Convert(writeErr).Message() != status.Convert(unknownErr).Message() {
		t.Fatalf("source enumeration leak: existing=%v unknown=%v", writeErr, unknownErr)
	}
	_, batchErr := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{spoof}})
	assertPermissionDenied(t, batchErr)
	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&auditv1.WriteRequest{Event: spoof}); err != nil {
		t.Fatal(err)
	}
	_, streamErr := stream.Recv()
	assertPermissionDenied(t, streamErr)
}

func testProtoEvent(eventID, source string) *auditv1.EventEnvelope {
	return &auditv1.EventEnvelope{EventId: eventID, SourceSystem: source, EventType: "audit.event", SchemaId: "audit.event", SchemaVersion: 1, OccurredAt: timestamppb.New(time.Unix(1_700_000_020, 0).UTC()), Actor: &auditv1.Actor{Id: "service"}, Action: "write", Outcome: "success", PayloadJson: []byte(`{"value":1}`), DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: eventID + "-idem"}
}

func assertPermissionDenied(t *testing.T, err error) {
	t.Helper()
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

// TestToStatusRedactsInternalErrors pins M-1: internal (non-domain) errors
// must never carry their underlying text — filesystem paths, errno strings,
// Vault/pgx details — to gRPC clients. The status is codes.Internal with a
// fixed message, mirroring the HTTP boundary's errorBody redaction. Domain
// errors keep their mapped codes and messages.
func TestToStatusRedactsInternalErrors(t *testing.T) {
	marker := errors.New("marker-path /var/lib/audit/state.json: permission denied")
	converted := toStatus(marker)
	if status.Code(converted) != codes.Internal {
		t.Fatalf("code=%v, want Internal", status.Code(converted))
	}
	if message := converted.(interface{ Error() string }).Error(); strings.Contains(message, "marker-path") || strings.Contains(message, "state.json") || strings.Contains(message, "permission") {
		t.Fatalf("internal detail leaked to client: %q", message)
	}
	if message := status.Convert(converted).Message(); message != "internal server error" {
		t.Fatalf("internal message=%q, want fixed redacted text", message)
	}
	// Domain mapping stays intact below Internal.
	invalid := toStatus(domain.ErrInvalid)
	if status.Code(invalid) != codes.InvalidArgument || !strings.Contains(status.Convert(invalid).Message(), "invalid") {
		t.Fatalf("domain error mapping regressed: %v", invalid)
	}
	forbidden := toStatus(domain.ErrForbidden)
	if status.Code(forbidden) != codes.PermissionDenied {
		t.Fatalf("ErrForbidden mapping regressed: %v", forbidden)
	}
}

// TestStatusErrorLogsDetailServerSide pins the diagnostic half of M-1: the
// full error text is written to the server log before the redacted status is
// returned, so operators can still diagnose without the client seeing it.
func TestStatusErrorLogsDetailServerSide(t *testing.T) {
	var logged strings.Builder
	server := &Server{Logger: log.New(&logged, "", 0)}
	marker := errors.New("marker-path /var/lib/audit/state.json: permission denied")
	converted := server.statusError(marker)
	if status.Code(converted) != codes.Internal {
		t.Fatalf("code=%v, want Internal", status.Code(converted))
	}
	if !strings.Contains(logged.String(), "marker-path") {
		t.Fatalf("internal detail not logged server-side: %q", logged.String())
	}
	if status.Convert(converted).Message() != "internal server error" {
		t.Fatalf("client message=%q, want fixed redacted text", status.Convert(converted).Message())
	}
	// Domain errors are not logged as internal.
	logged.Reset()
	_ = server.statusError(domain.ErrConflict)
	if logged.Len() != 0 {
		t.Fatalf("domain error logged as internal: %q", logged.String())
	}
}

// TestWriteRedactsStoreFailureEndToEnd drives a real Write through a service
// whose snapshot file is unwritable, so the store Save fails with an error
// carrying the state path. The gRPC client must see codes.Internal with the
// fixed message, never the path (M-1 integration leg).
func TestWriteRedactsStoreFailureEndToEnd(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	st, err := store.Open(statePath)
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
	// Make the snapshot file unwritable: every Save now fails with an error
	// carrying the state path and errno text.
	if err := os.Chmod(statePath, 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o700)
		_ = os.Chmod(statePath, 0o600)
	})
	server := &Server{Service: svc, Auth: auth.Authenticator{AllowDev: true}, Logger: log.New(io.Discard, "", 0)}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer dev:tenant-a:service:crm"))
	request := &auditv1.WriteRequest{Event: &auditv1.EventEnvelope{EventId: "redact-evt-1", SourceSystem: "crm", EventType: "audit.event", SchemaId: "audit.event", SchemaVersion: 1, OccurredAt: timestamppb.New(time.Unix(1_700_000_010, 0).UTC()), Actor: &auditv1.Actor{Id: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "redact-idem-1", PayloadJson: []byte(`{"value":1}`)}}
	_, err = server.Write(ctx, request)
	if err == nil {
		t.Fatal("Write succeeded against an unwritable store")
	}
	if status.Code(err) != codes.Internal {
		t.Fatalf("code=%v, want Internal", status.Code(err))
	}
	if message := status.Convert(err).Message(); message != "internal server error" {
		t.Fatalf("client message=%q, want fixed redacted text", message)
	}
	if strings.Contains(status.Convert(err).Message(), "state.json") {
		t.Fatalf("state path leaked to client: %q", status.Convert(err).Message())
	}
}
