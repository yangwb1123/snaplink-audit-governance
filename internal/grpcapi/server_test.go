package grpcapi

import (
	"context"
	"net"
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
	svc := service.New(st, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }})
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
	request := &auditv1.WriteRequest{Event: &auditv1.EventEnvelope{EventId: "grpc-evt-1", SourceSystem: "crm", EventType: "audit.event", SchemaId: "audit.event", SchemaVersion: 1, OccurredAt: timestamppb.New(time.Unix(1_700_000_010, 0).UTC()), Actor: &auditv1.Actor{Id: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "grpc-idem-1", PayloadJson: []byte(`{"value":1}`)}}
	receipt, err := client.Write(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.GetTenantId() != "tenant-a" || receipt.GetSequence() != 1 {
		t.Fatalf("unexpected receipt: %+v", receipt)
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
