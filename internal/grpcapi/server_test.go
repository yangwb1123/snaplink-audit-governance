package grpcapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/httpapi"
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

// testJWTSecret is the shared >=32-byte local HS256 test key for the gRPC
// JWT harness: the registered authenticator and signGRPCJWT must use the
// same constant so minted tokens verify. It must stay >=32 bytes or the
// harness fails the ValidateConfiguration gate.
const testJWTSecret = "test-secret-0123456789abcdefghijklmnopqrs"

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

// mintGRPCJWT mints a locally-signable HS256 JWT for the gRPC ingest
// harness, mirroring httpapi's mintRestoreJWT (fixed far-future exp because
// the harness clock is pinned at time.Unix(1_700_000_000, 0)).
func mintGRPCJWT(t *testing.T, subject, tenantID string, roles []string) string {
	t.Helper()
	return signGRPCJWT(t, map[string]any{"sub": subject, "tenant_id": tenantID, "roles": roles, "exp": 4_100_000_000})
}

// signGRPCJWT signs an arbitrary claim payload with the harness's shared
// testJWTSecret (>=32 bytes, HS256, at+jwt header), so tests can mint tokens
// with the claim under either alias (tenant_id or tenant).
func signGRPCJWT(t *testing.T, payload map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": "HS256", "typ": "at+jwt"})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	signed := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, []byte(testJWTSecret))
	_, _ = mac.Write([]byte(signed))
	return signed + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// newGRPCJWTHarness is newGRPCHarness with a JWT-capable authenticator
// ({AllowDev, JWTSecret, AllowLocalHS256} — the exact tuple the HTTP harness
// uses), so signed-JWT tenant-claim rejection can be exercised on the gRPC
// surface end-to-end. The per-call metadata context is caller-supplied.
func newGRPCJWTHarness(t *testing.T) (*store.Store, auditv1.IngestClient) {
	t.Helper()
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
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	Register(grpcServer, svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true})
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return st, auditv1.NewIngestClient(connection)
}

// TestGRPCRejectsKeyFramingTenantClaimUnauthenticated is REQ-6's gRPC
// surface: a signed JWT whose tenant_id/tenant claim violates the canonical
// key-framing rule fails authentication with codes.Unauthenticated on
// Write before any ingest — nothing reaches the snapshot. The positive
// control proves the fixture verifies end-to-end (an exact-string tenant
// claim ingests normally).
func TestGRPCRejectsKeyFramingTenantClaimUnauthenticated(t *testing.T) {
	st, client := newGRPCJWTHarness(t)
	for _, claimName := range []string{"tenant_id", "tenant"} {
		for _, bad := range []string{"a/b", `a\b`, "a\x1fb"} {
			token := signGRPCJWT(t, map[string]any{"sub": "service-subject", claimName: bad, "roles": []string{"service"}, "exp": 4_100_000_000})
			ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
			if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: testProtoEvent("grpc-jwt-bad", "crm")}); status.Code(err) != codes.Unauthenticated {
				t.Errorf("%s claim %q err=%v, want codes.Unauthenticated", claimName, bad, err)
			}
		}
	}
	assertNoFramedKeys(t, st)
	// Positive control: an exact-string tenant claim authenticates and the
	// event ingests with a receipt. The JWT must carry client_id matching
	// the source ID (fail-closed (client_id, source_system) binding), so the
	// fixture also proves the claim path verifies end-to-end.
	token := signGRPCJWT(t, map[string]any{"sub": "service-subject", "tenant_id": "tenant-a", "client_id": "crm", "roles": []string{"service"}, "exp": 4_100_000_000})
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: testProtoEvent("grpc-jwt-ok", "crm")}); err != nil {
		t.Fatalf("valid tenant claim Write failed: %v", err)
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

// grpcHarnessOptions tunes newGRPCHarnessOpts for tests that need a custom
// server logger, transport-level server options, or a ledgered publisher.
type grpcHarnessOptions struct {
	logger     *log.Logger               // nil => Server.Logger stays nil (nil-guard exercised)
	serverOpts []grpc.ServerOption       // e.g. grpc.MaxRecvMsgSize for the production-cap harness
	publisher  service.LedgeredPublisher // when set, Service.Ingest writes LedgeredOutbox rows
}

// newGRPCHarness builds the bufconn-based ingest harness used by the
// key-framing rejection test (mirrors TestWriteAndBatchOverGRPC's setup).
func newGRPCHarness(t *testing.T) (*store.Store, auditv1.IngestClient, context.Context) {
	t.Helper()
	return newGRPCHarnessOpts(t, grpcHarnessOptions{})
}

// newGRPCHarnessOpts is newGRPCHarness with a caller-supplied Server logger
// and gRPC server options; the ingest Server is constructed directly so the
// logger can be replaced (Register always wires log.Default()).
func newGRPCHarnessOpts(t *testing.T, opts grpcHarnessOptions) (*store.Store, auditv1.IngestClient, context.Context) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true, LedgeredPublisher: opts.publisher})
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
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(opts.serverOpts...)
	auditv1.RegisterIngestServer(grpcServer, &Server{Service: svc, Auth: auth.Authenticator{AllowDev: true}, Logger: opts.logger})
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer dev:tenant-a:service:crm"))
	return st, auditv1.NewIngestClient(connection), ctx
}

// assertNoFramedKeys walks the snapshot asserting no composite-key map
// holds a key with more than one KeySeparator, and that the rejected
// event/source ids were never persisted.
func assertNoFramedKeys(t *testing.T, st *store.Store, rejectedIDs ...string) {
	t.Helper()
	if err := st.Read(func(data *store.Snapshot) error {
		for name, keys := range map[string][]string{
			"Events":         keysOf(data.Events),
			"Streams":        keysOf(data.Streams),
			"Segments":       keysOf(data.Segments),
			"Checkpoints":    keysOf(data.Checkpoints),
			"Receipts":       keysOf(data.Receipts),
			"LedgeredOutbox": keysOf(data.LedgeredOutbox),
		} {
			for _, key := range keys {
				if strings.Count(key, "\x1f") > 1 {
					t.Errorf("%s holds multi-separator key %q", name, key)
				}
			}
		}
		for _, id := range rejectedIDs {
			key := store.EventKey("tenant-a", id)
			if _, exists := data.Events[key]; exists {
				t.Errorf("rejected event %q was persisted", id)
			}
			// Design AC-2: a receipt is the ledgered-state witness, so the
			// Receipts map must stay empty for a rejected event too — not
			// just the Events map.
			if _, exists := data.Receipts[key]; exists {
				t.Errorf("rejected event %q left a receipt in the snapshot", id)
			}
			if _, exists := data.LedgeredOutbox[key]; exists {
				t.Errorf("rejected event %q left a ledgered outbox entry in the snapshot", id)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func keysOf[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// failingLedgeredPublisher always fails publication, so a committed event's
// LedgeredOutbox row is retained instead of being acknowledged away. That
// makes the outbox oracle non-vacuous: a valid event must appear in
// Events, Receipts, AND LedgeredOutbox, while a rejected event must appear
// in none of them.
type failingLedgeredPublisher struct{}

func (p *failingLedgeredPublisher) Publish(_ context.Context, _ domain.Event) error {
	return fmt.Errorf("simulated broker failure")
}

// TestGRPCRejectedConversionDoesNotMutatePersistence closes the F-04 oracle
// gap: with a real file-backed store and a ledgered publisher configured, a
// rejected duplicate/unknown envelope must leave no Events, Receipts, or
// LedgeredOutbox entry, and must not prevent a valid batch prefix or a valid
// prior stream message from committing.
func TestGRPCRejectedConversionDoesNotMutatePersistence(t *testing.T) {
	st, client, ctx := newGRPCHarnessOpts(t, grpcHarnessOptions{publisher: &failingLedgeredPublisher{}})

	// The non-vacuous witness: a valid event lands in all three maps, and the
	// failing publisher keeps its outbox row in place.
	witness := testProtoEvent("witness-persisted", "crm")
	if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: witness}); err != nil {
		t.Fatalf("witness write rejected: %v", err)
	}

	// Duplicate changed-field names: rejected, nothing persisted.
	duplicate := testProtoEvent("dup-not-persisted", "crm")
	duplicate.ChangedFields = []*auditv1.FieldChange{
		{Field: "x", BeforeJson: `"a"`},
		{Field: "x", AfterJson: `"b"`},
	}
	if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: duplicate}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("duplicate Write code=%v, want InvalidArgument", status.Code(err))
	}

	// Unknown field on the envelope: rejected, nothing persisted.
	unknown := testProtoEvent("unknown-not-persisted", "crm")
	unknown.ProtoReflect().SetUnknown(validationUnknownWire())
	if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: roundTripEnvelope(t, unknown)}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown-field Write code=%v, want InvalidArgument", status.Code(err))
	}

	// Mixed batch: the valid prefix commits, the invalid member and every
	// later member leave no trace.
	prefix := testProtoEvent("batch-prefix-persisted", "crm")
	batchInvalid := testProtoEvent("batch-invalid-not-persisted", "crm")
	batchInvalid.ChangedFields = []*auditv1.FieldChange{{Field: "y"}, {Field: "y"}}
	later := testProtoEvent("batch-later-not-persisted", "crm")
	response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{prefix, batchInvalid, later}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteBatch code=%v, want InvalidArgument", status.Code(err))
	}
	if len(response.GetReceipts()) != 0 {
		t.Fatalf("WriteBatch receipts=%d, want none on error", len(response.GetReceipts()))
	}

	// Stream: the valid first message commits; the duplicate terminates the
	// stream and leaves no trace.
	streamValid := testProtoEvent("stream-valid-persisted", "crm")
	streamDuplicate := testProtoEvent("stream-dup-not-persisted", "crm")
	streamDuplicate.ChangedFields = []*auditv1.FieldChange{{Field: "z"}, {Field: "z"}}
	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&auditv1.WriteRequest{Event: streamValid}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("valid stream message rejected: %v", err)
	}
	if err := stream.Send(&auditv1.WriteRequest{Event: streamDuplicate}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteStream duplicate code=%v, want InvalidArgument", status.Code(err))
	}

	rejected := []string{
		duplicate.GetEventId(),
		unknown.GetEventId(),
		batchInvalid.GetEventId(),
		later.GetEventId(),
		streamDuplicate.GetEventId(),
	}
	assertNoFramedKeys(t, st, rejected...)

	if err := st.Read(func(data *store.Snapshot) error {
		for _, id := range []string{
			witness.GetEventId(),
			prefix.GetEventId(),
			streamValid.GetEventId(),
		} {
			key := store.EventKey("tenant-a", id)
			if _, exists := data.Events[key]; !exists {
				t.Errorf("valid event %q was not persisted", id)
			}
			if _, exists := data.Receipts[key]; !exists {
				t.Errorf("valid event %q was not receipted", id)
			}
			if _, exists := data.LedgeredOutbox[key]; !exists {
				t.Errorf("valid event %q is missing from the ledgered outbox (oracle not armed)", id)
			}
		}
		for _, id := range rejected {
			if _, exists := data.LedgeredOutbox[store.EventKey("tenant-a", id)]; exists {
				t.Errorf("rejected event %q left a ledgered outbox row", id)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestGRPCRejectsKeyFramingEventInvalidArgument is F-1: the gRPC ingest
// surface (Write, WriteBatch, WriteStream) funnels through Service.Ingest →
// ValidateBasic, so key-framing violations map to codes.InvalidArgument and
// nothing reaches the snapshot; WriteBatch keeps its partial-receipt
// contract over gRPC.
func TestGRPCRejectsKeyFramingEventInvalidArgument(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	bad := testProtoEvent("grpc-bad-1", "crm")
	bad.EventId = "a\x1fb"
	if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: bad}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Write with 0x1F event_id: code=%v, want InvalidArgument", status.Code(err))
	}

	// The FM-1 length cap funnels through the same boundary: an event_id
	// above MaxArchiveComponentBytes maps to InvalidArgument (the gRPC field
	// cap alone would admit it at 8KB, so this is the archive-component
	// bound doing the rejection, matching the HTTP surface).
	overCap := testProtoEvent("grpc-overcap", "crm")
	overCap.EventId = strings.Repeat("a", domain.MaxArchiveComponentBytes+1)
	if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: overCap}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Write with over-cap event_id: code=%v, want InvalidArgument", status.Code(err))
	}

	// WriteBatch partial-acceptance contract: the valid prefix is committed
	// to the ledger before the invalid tail aborts the batch. Unlike HTTP
	// (which returns {receipts, error} in the body), gRPC error responses
	// carry no message, so the prefix receipt is not delivered on the wire —
	// the ledger side is identical: valid prefix in, invalid tail out.
	valid := testProtoEvent("grpc-bad-valid-1", "crm")
	valid.EventId = "grpc-ok-1"
	invalid := testProtoEvent("grpc-bad-2", "crm")
	invalid.SourceSystem = "x\x1fy"
	response, batchErr := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{valid, invalid}})
	if status.Code(batchErr) != codes.InvalidArgument {
		t.Fatalf("WriteBatch code=%v, want InvalidArgument", status.Code(batchErr))
	}
	if len(response.GetReceipts()) != 0 {
		t.Fatalf("WriteBatch wire receipts=%d, want 0 (gRPC error responses carry no message)", len(response.GetReceipts()))
	}

	// WriteStream: prefix receipt is delivered, the invalid event terminates
	// the stream with InvalidArgument.
	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&auditv1.WriteRequest{Event: testProtoEvent("grpc-bad-stream-ok", "crm")}); err != nil {
		t.Fatal(err)
	}
	prefix, err := stream.Recv()
	if err != nil || prefix.GetEventId() != "grpc-bad-stream-ok" {
		t.Fatalf("stream prefix receipt: %v %v", prefix, err)
	}
	if err := stream.Send(&auditv1.WriteRequest{Event: bad}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("stream termination code=%v, want InvalidArgument", status.Code(err))
	}

	assertNoFramedKeys(t, st, "a\x1fb", "x\x1fy", "grpc-bad-2")
	// The valid prefix from the aborted batch IS in the ledger (committed
	// before the invalid tail aborted the loop) — partial acceptance, same
	// as HTTP's partial-status semantics.
	if err := st.Read(func(data *store.Snapshot) error {
		if _, exists := data.Events[store.EventKey("tenant-a", "grpc-ok-1")]; !exists {
			t.Fatal("valid batch prefix was not ledgered before the abort")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
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
	// Make every v2 persistence tier unwritable: a hot/cold UpdateTenant may
	// commit the tenant file or ledger without touching the control file.
	if err := os.Chmod(statePath, 0o400); err != nil {
		t.Fatal(err)
	}
	layoutDirs := []string{filepath.Join(dir, "tenants"), filepath.Join(dir, "ledger")}
	for _, layoutDir := range layoutDirs {
		if err := os.Chmod(layoutDir, 0o500); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o700)
		_ = os.Chmod(statePath, 0o600)
		for _, layoutDir := range layoutDirs {
			_ = os.Chmod(layoutDir, 0o700)
		}
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

// TestFromProtoRejectsTrailingJSON is AC-1: decodeJSONNumber must reject any
// input whose first JSON value is followed by non-whitespace (trailing
// content would otherwise be silently dropped from the ledgered event),
// while whitespace trailers and clean JSON stay accepted — the same rule the
// HTTP surface's decodeBody enforces. Unit level: no store, no RPC.
func TestFromProtoRejectsTrailingJSON(t *testing.T) {
	// Case A: trailing content after payload_json.
	payload := testProtoEvent("unit-trail-payload", "crm")
	payload.PayloadJson = []byte(`{"a":1} extra`)
	if _, err := fromProto(payload); !errors.Is(err, domain.ErrInvalid) || !strings.Contains(err.Error(), "payload_json is invalid") {
		t.Fatalf("payload trailing: err=%v, want ErrInvalid carrying payload_json is invalid", err)
	}
	// Case B: trailing content after changed_fields[].before_json.
	before := testProtoEvent("unit-trail-before", "crm")
	before.ChangedFields = []*auditv1.FieldChange{{Field: "amount", BeforeJson: `{"v":1} extra`, AfterJson: `{"v":2}`}}
	if _, err := fromProto(before); !errors.Is(err, domain.ErrInvalid) || !strings.Contains(err.Error(), "invalid before_json") {
		t.Fatalf("before_json trailing: err=%v, want ErrInvalid carrying invalid before_json", err)
	}
	// Case C: trailing content after changed_fields[].after_json.
	after := testProtoEvent("unit-trail-after", "crm")
	after.ChangedFields = []*auditv1.FieldChange{{Field: "amount", BeforeJson: `{"v":1}`, AfterJson: `[1,2] trailing`}}
	if _, err := fromProto(after); !errors.Is(err, domain.ErrInvalid) || !strings.Contains(err.Error(), "invalid after_json") {
		t.Fatalf("after_json trailing: err=%v, want ErrInvalid carrying invalid after_json", err)
	}
	// Case D (negative): whitespace trailers and clean JSON are accepted.
	clean := testProtoEvent("unit-trail-clean", "crm")
	clean.PayloadJson = []byte("{\"a\":1}  \n\t")
	clean.ChangedFields = []*auditv1.FieldChange{{Field: "amount", BeforeJson: `{"v":1}`, AfterJson: `[1,2]`}}
	event, err := fromProto(clean)
	if err != nil {
		t.Fatalf("whitespace trailer rejected: %v", err)
	}
	if event.Payload["a"] != json.Number("1") {
		t.Fatalf("payload not decoded: %#v", event.Payload)
	}
	if event.ChangedFields["amount"].After == nil {
		t.Fatalf("changed fields not decoded: %#v", event.ChangedFields)
	}
	// Case E (boundary): two adjacent JSON values are trailing content.
	adjacent := testProtoEvent("unit-trail-adjacent", "crm")
	adjacent.PayloadJson = []byte(`{"a":1}{"b":2}`)
	if _, err := fromProto(adjacent); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("adjacent values: err=%v, want ErrInvalid", err)
	}
	// Negative control: an omitted payload_json stays legal (len 0 skips decode).
	empty := testProtoEvent("unit-trail-empty", "crm")
	empty.PayloadJson = nil
	if _, err := fromProto(empty); err != nil {
		t.Fatalf("empty payload rejected: %v", err)
	}
}

// TestGRPCRejectsTrailingJSONPayload is AC-2: trailing content after the
// first JSON value is rejected with codes.InvalidArgument on Write,
// WriteBatch and WriteStream, and nothing is persisted — fromProto runs
// before Service.Ingest on all three RPCs. The negative control proves the
// harness and envelope are valid.
func TestGRPCRejectsTrailingJSONPayload(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)

	write := testProtoEvent("grpc-trail-write", "crm")
	write.PayloadJson = []byte(`{"a":1} extra`)
	if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: write}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Write code=%v, want InvalidArgument", status.Code(err))
	}

	batch := testProtoEvent("grpc-trail-batch", "crm")
	batch.PayloadJson = []byte(`{"a":1} extra`)
	response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{batch}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteBatch code=%v, want InvalidArgument", status.Code(err))
	}
	if len(response.GetReceipts()) != 0 {
		t.Fatalf("WriteBatch wire receipts=%d, want 0", len(response.GetReceipts()))
	}

	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	streamBad := testProtoEvent("grpc-trail-stream", "crm")
	streamBad.PayloadJson = []byte(`{"a":1} extra`)
	if err := stream.Send(&auditv1.WriteRequest{Event: streamBad}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteStream termination code=%v, want InvalidArgument", status.Code(err))
	}

	assertNoFramedKeys(t, st, "grpc-trail-write", "grpc-trail-batch", "grpc-trail-stream")

	// Negative control: the identical envelope with clean JSON ingests.
	ok := testProtoEvent("grpc-trail-ok", "crm")
	receipt, err := client.Write(ctx, &auditv1.WriteRequest{Event: ok})
	if err != nil {
		t.Fatalf("negative control Write failed: %v", err)
	}
	if receipt.GetEventId() != "grpc-trail-ok" {
		t.Fatalf("negative control receipt: %+v", receipt)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if _, exists := data.Events[store.EventKey("tenant-a", "grpc-trail-ok")]; !exists {
			t.Fatal("negative control event was not ledgered")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// newHTTPIngestLeg builds the HTTP transport's ingest surface from exported
// APIs only (httpapi.NewServer + Handler), mirroring httpapi's unexported
// testHTTPServerWithStore. The parity test cannot reuse the other package's
// helper (both harness helpers are unexported); httpapi never imports
// grpcapi, so this test-only import direction is cycle-free.
func newHTTPIngestLeg(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
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
	server := httptest.NewServer(httpapi.NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	t.Cleanup(server.Close)
	return server, st
}

// parityEnvelope is testProtoEvent with a caller-supplied payload_json,
// keeping the event fields digest-relevant (source, schema, actor, action,
// outcome, classification, retention, idempotency key) identical to the
// HTTP leg built by parityHTTPEvent.
func parityEnvelope(eventID, payloadJSON string) *auditv1.EventEnvelope {
	envelope := testProtoEvent(eventID, "crm")
	envelope.PayloadJson = []byte(payloadJSON)
	return envelope
}

// parityHTTPEvent is the HTTP-transport twin of parityEnvelope: the same
// logical event as a domain.Event JSON body for POST /api/v1/events.
func parityHTTPEvent(eventID string, payload map[string]any) domain.Event {
	return domain.Event{
		EventID: eventID, SourceSystem: "crm", EventType: "audit.event",
		SchemaID: "audit.event", SchemaVersion: 1,
		OccurredAt: time.Unix(1_700_000_020, 0).UTC(),
		Actor:      domain.Actor{ID: "service"},
		Action:     "write", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: eventID + "-idem", Payload: payload,
	}
}

// storedDigest fetches the ledgered SourceDigest for an event, or fails the
// test if the event never reached the snapshot.
func storedDigest(t *testing.T, st *store.Store, eventID string) string {
	t.Helper()
	var digest string
	if err := st.Read(func(data *store.Snapshot) error {
		event, exists := data.Events[store.EventKey("tenant-a", eventID)]
		if !exists {
			t.Fatalf("event %q was never ledgered", eventID)
		}
		digest = event.SourceDigest
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return digest
}

// TestGRPCHTTPDigestParity is AC-3: both transports decode the same logical
// event with UseNumber and hash the same canonical content, so the stored
// SourceDigest must be equal for every accepted input class — including an
// int64 beyond 2^53, whose float64 collapse would silently diverge the two
// transports. The gRPC leg uses newGRPCHarness; the HTTP leg is built inline
// via exported APIs (newHTTPIngestLeg).
func TestGRPCHTTPDigestParity(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	httpServer, httpStore := newHTTPIngestLeg(t)

	changed := parityEnvelope("parity-changed", `{"value":1}`)
	changed.ChangedFields = []*auditv1.FieldChange{{Field: "amount", BeforeJson: `{"v":1}`, AfterJson: `[1,2]`}}
	changedHTTP := parityHTTPEvent("parity-changed", map[string]any{"value": json.Number("1")})
	changedHTTP.ChangedFields = map[string]domain.FieldChange{"amount": {Before: map[string]any{"v": json.Number("1")}, After: []any{json.Number("1"), json.Number("2")}}}

	classes := []struct {
		name      string
		envelope  *auditv1.EventEnvelope
		httpEvent domain.Event
	}{
		{"big int64 beyond 2^53", parityEnvelope("parity-bigint", `{"value":12345678901234567890}`), parityHTTPEvent("parity-bigint", map[string]any{"value": json.Number("12345678901234567890")})},
		{"nested object", parityEnvelope("parity-nested", `{"a":{"b":[1,2,{"c":3}]}}`), parityHTTPEvent("parity-nested", map[string]any{"a": map[string]any{"b": []any{json.Number("1"), json.Number("2"), map[string]any{"c": json.Number("3")}}}})},
		{"array inside payload", parityEnvelope("parity-array", `{"items":[1,2,3]}`), parityHTTPEvent("parity-array", map[string]any{"items": []any{json.Number("1"), json.Number("2"), json.Number("3")}})},
		{"changed-field pair", changed, changedHTTP},
	}
	for _, class := range classes {
		t.Run(class.name, func(t *testing.T) {
			if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: class.envelope}); err != nil {
				t.Fatalf("gRPC ingest failed: %v", err)
			}
			body, err := json.Marshal(class.httpEvent)
			if err != nil {
				t.Fatal(err)
			}
			req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/v1/events?wait_for=ledgered", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusAccepted {
				data, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				t.Fatalf("HTTP ingest status=%d body=%s", resp.StatusCode, data)
			}
			resp.Body.Close()

			grpcDigest := storedDigest(t, st, class.envelope.GetEventId())
			httpDigest := storedDigest(t, httpStore, class.envelope.GetEventId())
			if grpcDigest == "" || grpcDigest != httpDigest {
				t.Fatalf("SourceDigest parity broken: gRPC=%q HTTP=%q", grpcDigest, httpDigest)
			}
		})
	}

	// Rejection parity: a top-level array payload cannot decode into the
	// map-typed domain payload on EITHER transport, so neither side can ever
	// ledger it — both reject before any ingest (gRPC InvalidArgument, HTTP
	// 400), and the input class is invalid on both today and after the fix.
	topLevel := testProtoEvent("parity-array-top", "crm")
	topLevel.PayloadJson = []byte(`[1,2,3]`)
	if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: topLevel}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("top-level array: gRPC code=%v, want InvalidArgument", status.Code(err))
	}
	req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/v1/events", bytes.NewReader([]byte(`[1,2,3]`)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("top-level array: HTTP status=%d, want 400", resp.StatusCode)
	}
}

// TestFromProtoRejectsOversizedEnvelope is AC-2: fromProto rejects every
// verbatim-persisted envelope string above MaxEnvelopeFieldBytes and every
// repeated field above its arity cap with ErrEnvelopeTooLarge (which wraps
// domain.ErrInvalid and maps to codes.InvalidArgument via toStatus), and the
// error message names the field with exact sizes. Boundary controls: fields
// at exactly the cap and counts at exactly the caps convert cleanly, and a
// full Write of a max-size field returns a receipt.
func TestFromProtoRejectsOversizedEnvelope(t *testing.T) {
	over := strings.Repeat("x", MaxEnvelopeFieldBytes+1)
	overJSON := canonicalJSONString(strings.Repeat("x", MaxEnvelopeFieldBytes-1))
	overCases := []struct {
		name   string
		mutate func(*auditv1.EventEnvelope)
	}{
		{"reason", func(e *auditv1.EventEnvelope) { e.Reason = over }},
		{"trace_id", func(e *auditv1.EventEnvelope) { e.TraceId = over }},
		{"targets[0].name", func(e *auditv1.EventEnvelope) { e.Targets = []*auditv1.Target{{Name: over}} }},
		{"changed_fields[0].after_json", func(e *auditv1.EventEnvelope) {
			e.ChangedFields = []*auditv1.FieldChange{{Field: "f", AfterJson: overJSON}}
		}},
	}
	for _, tc := range overCases {
		t.Run(tc.name, func(t *testing.T) {
			env := testProtoEvent("unit-over-"+strings.ReplaceAll(tc.name, " ", "-"), "crm")
			tc.mutate(env)
			_, err := fromProto(env)
			if !errors.Is(err, domain.ErrInvalid) || !errors.Is(err, ErrEnvelopeTooLarge) {
				t.Fatalf("err=%v, want both domain.ErrInvalid and ErrEnvelopeTooLarge", err)
			}
			if code := status.Code(toStatus(err)); code != codes.InvalidArgument {
				t.Fatalf("toStatus code=%v, want InvalidArgument", code)
			}
			want := fmt.Sprintf("%d bytes, max %d", MaxEnvelopeFieldBytes+1, MaxEnvelopeFieldBytes)
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("message %q missing size detail %q", err.Error(), want)
			}
		})
	}
	arityCases := []struct {
		name   string
		mutate func(*auditv1.EventEnvelope)
	}{
		{"actor.roles", func(e *auditv1.EventEnvelope) { e.Actor.Roles = make([]string, MaxActorRoles+1) }},
		{"targets", func(e *auditv1.EventEnvelope) { e.Targets = make([]*auditv1.Target, MaxTargetsPerEvent+1) }},
		{"changed_fields", func(e *auditv1.EventEnvelope) { e.ChangedFields = make([]*auditv1.FieldChange, MaxChangedFields+1) }},
	}
	for _, tc := range arityCases {
		t.Run(tc.name, func(t *testing.T) {
			env := testProtoEvent("unit-over-arity-"+strings.ReplaceAll(tc.name, ".", "-"), "crm")
			tc.mutate(env)
			_, err := fromProto(env)
			if !errors.Is(err, domain.ErrInvalid) || !errors.Is(err, ErrEnvelopeTooLarge) {
				t.Fatalf("err=%v, want both domain.ErrInvalid and ErrEnvelopeTooLarge", err)
			}
			if code := status.Code(toStatus(err)); code != codes.InvalidArgument {
				t.Fatalf("toStatus code=%v, want InvalidArgument", code)
			}
			if !strings.Contains(err.Error(), tc.name) {
				t.Fatalf("message %q missing field name %q", err.Error(), tc.name)
			}
		})
	}

	// Boundary: every string at exactly MaxEnvelopeFieldBytes and counts at
	// exactly the arity caps convert cleanly through fromProto.
	boundary := testProtoEvent("unit-boundary", "crm")
	boundary.Reason = strings.Repeat("r", MaxEnvelopeFieldBytes)
	boundary.TraceId = strings.Repeat("t", MaxEnvelopeFieldBytes)
	boundary.Actor.Roles = make([]string, MaxActorRoles)
	boundary.Targets = make([]*auditv1.Target, MaxTargetsPerEvent)
	boundary.ChangedFields = make([]*auditv1.FieldChange, MaxChangedFields)
	for i := range boundary.ChangedFields {
		boundary.ChangedFields[i] = &auditv1.FieldChange{Field: fmt.Sprintf("field-%d", i)}
	}
	if _, err := fromProto(boundary); err != nil {
		t.Fatalf("boundary envelope rejected: %v", err)
	}

	// Boundary end-to-end: a Write with reason at exactly the cap ingests
	// with a receipt (the service layer imposes no separate length cap).
	st, client, ctx := newGRPCHarness(t)
	maxField := testProtoEvent("grpc-boundary-max", "crm")
	maxField.Reason = strings.Repeat("r", MaxEnvelopeFieldBytes)
	receipt, err := client.Write(ctx, &auditv1.WriteRequest{Event: maxField})
	if err != nil {
		t.Fatalf("boundary Write failed: %v", err)
	}
	if receipt.GetEventId() != "grpc-boundary-max" {
		t.Fatalf("boundary receipt: %+v", receipt)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if _, exists := data.Events[store.EventKey("tenant-a", "grpc-boundary-max")]; !exists {
			t.Fatal("boundary event was not ledgered")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestGRPCRejectsOverCapBatch is AC-3: a WriteBatch beyond MaxBatchEvents is
// rejected with InvalidArgument by the pre-flight count check with zero
// partial commit (none of the batch IDs reach the snapshot), while a batch of
// exactly MaxBatchEvents commits fully with all receipts. 501 x ~280B is
// ~140KB — under both the harness 4MB and the production 512KB ceilings, so
// the pre-flight cap — not transport — deterministically rejects.
func TestGRPCRejectsOverCapBatch(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)

	over := make([]*auditv1.EventEnvelope, 0, MaxBatchEvents+1)
	for i := 0; i < MaxBatchEvents+1; i++ {
		over = append(over, testProtoEvent(fmt.Sprintf("grpc-over-batch-%d", i), "crm"))
	}
	response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: over})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteBatch code=%v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
	if !strings.Contains(status.Convert(err).Message(), fmt.Sprintf("max %d", MaxBatchEvents)) {
		t.Fatalf("rejection message=%q, want cap detail", status.Convert(err).Message())
	}
	if len(response.GetReceipts()) != 0 {
		t.Fatalf("wire receipts=%d, want 0", len(response.GetReceipts()))
	}
	if err := st.Read(func(data *store.Snapshot) error {
		for i := 0; i < MaxBatchEvents+1; i++ {
			if _, exists := data.Events[store.EventKey("tenant-a", fmt.Sprintf("grpc-over-batch-%d", i))]; exists {
				t.Fatalf("over-cap batch event %d was persisted (partial commit)", i)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Boundary: exactly MaxBatchEvents commits fully.
	batch := make([]*auditv1.EventEnvelope, 0, MaxBatchEvents)
	for i := 0; i < MaxBatchEvents; i++ {
		batch = append(batch, testProtoEvent(fmt.Sprintf("grpc-max-batch-%d", i), "crm"))
	}
	response, err = client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: batch})
	if err != nil {
		t.Fatalf("boundary WriteBatch failed: %v", err)
	}
	if len(response.GetReceipts()) != MaxBatchEvents {
		t.Fatalf("boundary receipts=%d, want %d", len(response.GetReceipts()), MaxBatchEvents)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		for i := 0; i < MaxBatchEvents; i++ {
			if _, exists := data.Events[store.EventKey("tenant-a", fmt.Sprintf("grpc-max-batch-%d", i))]; !exists {
				t.Fatalf("boundary batch event %d missing", i)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestGRPCWriteStreamOverCapContinues is AC-4: a WriteStream message whose
// reason exceeds MaxEnvelopeFieldBytes (≈8.6KB total — under the transport
// ceiling, so it reaches fromProto) is rejected without a receipt and the
// stream stays open; the next valid message gets its receipt and CloseSend
// completes with io.EOF. Also pins F7: the server log carries the truncated
// event_id prefix (never the full attacker-controlled value) plus principal
// attribution, and the over-cap event never reaches the snapshot.
func TestGRPCWriteStreamOverCapContinues(t *testing.T) {
	var logged strings.Builder
	st, client, ctx := newGRPCHarnessOpts(t, grpcHarnessOptions{logger: log.New(&logged, "", 0)})

	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	over := testProtoEvent("grpc-stream-over-"+strings.Repeat("e", 80), "crm") // 97-byte event_id
	over.Reason = strings.Repeat("r", MaxEnvelopeFieldBytes+1)
	if err := stream.Send(&auditv1.WriteRequest{Event: over}); err != nil {
		t.Fatal(err)
	}
	valid := testProtoEvent("grpc-stream-valid", "crm")
	if err := stream.Send(&auditv1.WriteRequest{Event: valid}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	receipt, err := stream.Recv()
	if err != nil || receipt.GetEventId() != "grpc-stream-valid" {
		t.Fatalf("stream receipt after skip: receipt=%v err=%v, want the valid event's receipt", receipt, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("stream close err=%v, want io.EOF (clean completion, no termination error)", err)
	}

	if !strings.Contains(logged.String(), "rejecting over-cap message") {
		t.Fatalf("skip not logged: %q", logged.String())
	}
	if !strings.Contains(logged.String(), "client_id=\"crm\"") || !strings.Contains(logged.String(), "tenant=\"tenant-a\"") {
		t.Fatalf("skip log lacks principal attribution: %q", logged.String())
	}
	prefix := "grpc-stream-over-" + strings.Repeat("e", 64-len("grpc-stream-over-"))
	if !strings.Contains(logged.String(), prefix) {
		t.Fatalf("skip log lacks truncated event_id prefix: %q", logged.String())
	}
	if strings.Contains(logged.String(), "grpc-stream-over-"+strings.Repeat("e", 80)) {
		t.Fatalf("full attacker-controlled event_id leaked into log: %q", logged.String())
	}
	if strings.Contains(logged.String(), "grpc-stream-valid") {
		t.Fatalf("accepted message logged as rejection: %q", logged.String())
	}

	if err := st.Read(func(data *store.Snapshot) error {
		if _, exists := data.Events[store.EventKey("tenant-a", "grpc-stream-over-"+strings.Repeat("e", 80))]; exists {
			t.Fatal("over-cap stream event was persisted")
		}
		if _, exists := data.Events[store.EventKey("tenant-a", "grpc-stream-valid")]; !exists {
			t.Fatal("valid stream event was not ledgered")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestGRPCWriteStreamOverCapNilLogger pins the D7 nil-guard: a Server with a
// nil Logger still skips over-cap messages and serves subsequent ones.
func TestGRPCWriteStreamOverCapNilLogger(t *testing.T) {
	st, client, ctx := newGRPCHarness(t) // opts.logger is nil
	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	over := testProtoEvent("grpc-nolog-over", "crm")
	over.Reason = strings.Repeat("r", MaxEnvelopeFieldBytes+1)
	if err := stream.Send(&auditv1.WriteRequest{Event: over}); err != nil {
		t.Fatal(err)
	}
	valid := testProtoEvent("grpc-nolog-valid", "crm")
	if err := stream.Send(&auditv1.WriteRequest{Event: valid}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	receipt, err := stream.Recv()
	if err != nil || receipt.GetEventId() != "grpc-nolog-valid" {
		t.Fatalf("receipt=%v err=%v, want valid receipt", receipt, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("close err=%v, want io.EOF", err)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if _, exists := data.Events[store.EventKey("tenant-a", "grpc-nolog-over")]; exists {
			t.Fatal("over-cap stream event was persisted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestGRPCWriteStreamSkipThenNonSizeTerminates pins REQ-7's boundary on one
// stream: skip-and-continue applies only to ErrEnvelopeTooLarge — an over-cap
// message is skipped, and a subsequent non-size rejection (key-framing
// violation) still terminates the stream with InvalidArgument, exactly like
// the pinned key-framing test. The over-cap event gets no receipt and never
// reaches the snapshot.
func TestGRPCWriteStreamSkipThenNonSizeTerminates(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	over := testProtoEvent("grpc-skip-then-bad", "crm")
	over.Reason = strings.Repeat("r", MaxEnvelopeFieldBytes+1)
	if err := stream.Send(&auditv1.WriteRequest{Event: over}); err != nil {
		t.Fatal(err)
	}
	bad := testProtoEvent("grpc-skip-then-keyframing", "crm")
	bad.EventId = "a\x1fb"
	if err := stream.Send(&auditv1.WriteRequest{Event: bad}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("stream termination code=%v, want InvalidArgument (skip must not swallow non-size errors)", status.Code(err))
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if _, exists := data.Events[store.EventKey("tenant-a", "grpc-skip-then-bad")]; exists {
			t.Fatal("over-cap stream event was persisted")
		}
		if _, exists := data.Events[store.EventKey("tenant-a", "grpc-skip-then-keyframing")]; exists {
			t.Fatal("key-framing-invalid stream event was persisted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestGRPCTransportRejectsOverMaxRecv closes the 8x amplification gap
// end-to-end: on a server built with the production cap
// (grpc.MaxRecvMsgSize(MaxRecvBytes)), a message over 512KB total is rejected
// at the transport with codes.ResourceExhausted before fromProto — on Write
// and on WriteStream (where the stream terminates, grpc-go behavior the
// handler cannot intercept). Negative control: an over-cap field under the
// transport ceiling is rejected by fromProto with InvalidArgument, proving
// the transport accepted the message and the app-layer cap did the rejection.
func TestGRPCTransportRejectsOverMaxRecv(t *testing.T) {
	st, client, ctx := newGRPCHarnessOpts(t, grpcHarnessOptions{serverOpts: []grpc.ServerOption{grpc.MaxRecvMsgSize(MaxRecvBytes)}})

	over := testProtoEvent("grpc-transport-over", "crm")
	over.Reason = strings.Repeat("r", MaxRecvBytes) // wire size ≈ 512KB + framing > cap
	if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: over}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Write code=%v, want ResourceExhausted", status.Code(err))
	}
	assertNoFramedKeys(t, st, "grpc-transport-over")

	// Negative control: the same class of violation under the ceiling is
	// InvalidArgument from the app layer, not ResourceExhausted.
	field := testProtoEvent("grpc-transport-field", "crm")
	field.Reason = strings.Repeat("r", MaxEnvelopeFieldBytes+1)
	if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: field}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Write code=%v, want InvalidArgument (app-layer cap, transport accepted)", status.Code(err))
	}
	assertNoFramedKeys(t, st, "grpc-transport-field")

	// WriteStream: a transport-over-cap message terminates the stream with
	// ResourceExhausted (F1 — inherent grpc-go behavior).
	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	streamOver := testProtoEvent("grpc-stream-transport-over", "crm")
	streamOver.Reason = strings.Repeat("r", MaxRecvBytes)
	if err := stream.Send(&auditv1.WriteRequest{Event: streamOver}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("stream termination code=%v, want ResourceExhausted", status.Code(err))
	}
	assertNoFramedKeys(t, st, "grpc-stream-transport-over")
}

// TestGRPCBatchOverCapMemberPartialCommit pins D6: a per-envelope cap
// violation inside WriteBatch keeps the existing loop semantics — the valid
// prefix is ledgered, the batch aborts with InvalidArgument, and wire
// receipts are dropped (mirrors the pinned key-framing partial-commit
// contract, but for ErrEnvelopeTooLarge). Only the count cap is pre-flight
// atomic.
func TestGRPCBatchOverCapMemberPartialCommit(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	validA := testProtoEvent("grpc-partial-a", "crm")
	over := testProtoEvent("grpc-partial-over", "crm")
	over.Reason = strings.Repeat("r", MaxEnvelopeFieldBytes+1)
	validB := testProtoEvent("grpc-partial-b", "crm")
	response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{validA, over, validB}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteBatch code=%v, want InvalidArgument", status.Code(err))
	}
	if !strings.Contains(status.Convert(err).Message(), fmt.Sprintf("%d bytes, max %d", MaxEnvelopeFieldBytes+1, MaxEnvelopeFieldBytes)) {
		t.Fatalf("rejection message=%q, want per-field size detail", status.Convert(err).Message())
	}
	if len(response.GetReceipts()) != 0 {
		t.Fatalf("wire receipts=%d, want 0 (gRPC error responses carry no message)", len(response.GetReceipts()))
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if _, exists := data.Events[store.EventKey("tenant-a", "grpc-partial-a")]; !exists {
			t.Fatal("valid batch prefix was not ledgered before the abort")
		}
		if _, exists := data.Events[store.EventKey("tenant-a", "grpc-partial-over")]; exists {
			t.Fatal("over-cap batch member was persisted")
		}
		if _, exists := data.Events[store.EventKey("tenant-a", "grpc-partial-b")]; exists {
			t.Fatal("batch member after the abort was persisted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
