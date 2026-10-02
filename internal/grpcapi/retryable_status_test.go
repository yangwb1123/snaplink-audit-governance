package grpcapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// conflictBackend is a store.Backend that behaves like a healthy replica
// until a snapshot carrying conflictKey is saved: that save reports the
// optimistic-lock conflict a stale writer would see when another replica won
// the version check, and persists nothing. The wrapped text carries a marker
// path so the redaction contract can be asserted from both sides of the
// boundary.
type conflictBackend struct {
	mu          sync.Mutex
	seed        *store.Snapshot
	conflictKey string
	saveCalls   int
}

// newConflictBackend seeds a conflictBackend from a real store.Open control
// plane (tenant + source + schema), so every key the ingest path frames is
// normalized exactly the way production normalizes it.
func newConflictBackend(t *testing.T, conflictKey string) *conflictBackend {
	t.Helper()
	seedStore, err := store.Open(filepath.Join(t.TempDir(), "seed.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = seedStore.Close() })
	seedService, err := service.New(seedStore, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := seedService.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := seedService.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := seedService.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	seed, err := seedStore.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	return &conflictBackend{seed: seed, conflictKey: conflictKey}
}

func (b *conflictBackend) Load(ctx context.Context) (*store.Snapshot, error) { return b.clone() }
func (b *conflictBackend) LoadForUpdate(ctx context.Context) (*store.Snapshot, error) {
	return b.clone()
}

// Save commits by keeping a private copy and reports the optimistic-lock
// conflict — persisting nothing — once the closure has added the conflicting
// event to the snapshot. Because nothing is written, the store's retry loop
// is sound and the request ends up committed nowhere.
func (b *conflictBackend) Save(ctx context.Context, data *store.Snapshot) error {
	if _, conflicting := data.Events[b.conflictKey]; conflicting {
		b.mu.Lock()
		b.saveCalls++
		b.mu.Unlock()
		return fmt.Errorf("%w: /var/lib/audit/state.json: simulated stale writer", store.ErrSnapshotConflict)
	}
	persisted, err := cloneConflictSnapshot(data)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.saveCalls++
	b.seed = persisted
	b.mu.Unlock()
	return nil
}

func (b *conflictBackend) calls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.saveCalls
}

func (b *conflictBackend) clone() (*store.Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return cloneConflictSnapshot(b.seed)
}

// cloneConflictSnapshot decodes through JSON with UseNumber, mirroring the
// store's own decodeSnapshot, so json.Number payload values survive the reload
// instead of collapsing to float64.
func cloneConflictSnapshot(source *store.Snapshot) (*store.Snapshot, error) {
	encoded, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	var copyData store.Snapshot
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&copyData); err != nil {
		return nil, err
	}
	return &copyData, nil
}

// TestToStatusMapsSnapshotConflictToRetryableCode pins the transport class:
// an exhausted optimistic-lock retry is a transient condition, so it maps to
// codes.Unavailable (the analogue of the HTTP 503 snapshot_conflict already
// pinned by the route contract) and not to codes.Internal. The message is the
// fixed constant, never err.Error().
func TestToStatusMapsSnapshotConflictToRetryableCode(t *testing.T) {
	oneLevel := fmt.Errorf("save state snapshot: %w", store.ErrSnapshotConflict)
	twoLevel := fmt.Errorf("audit-api ingest: %w", oneLevel)
	for name, err := range map[string]error{
		"bare":      store.ErrSnapshotConflict,
		"one_level": oneLevel,
		"two_level": twoLevel,
	} {
		t.Run(name, func(t *testing.T) {
			converted := toStatus(err)
			if status.Code(converted) != codes.Unavailable {
				t.Fatalf("code=%v, want Unavailable for an exhausted optimistic-lock conflict", status.Code(converted))
			}
			if message := status.Convert(converted).Message(); message != snapshotConflictMessage {
				t.Fatalf("message=%q, want fixed %q", message, snapshotConflictMessage)
			}
		})
	}
	// Negative control: the new case is not a catch-all — an unrelated fault
	// still classifies as a redacted internal error.
	other := toStatus(errors.New("boom"))
	if status.Code(other) != codes.Internal || status.Convert(other).Message() != "internal server error" {
		t.Fatalf("unrelated error=%v, want redacted Internal", other)
	}
}

// TestToStatusConflictPrecedence pins that the idempotency-key conflict keeps
// its AlreadyExists classification: a retryable-transient class must never
// swallow a semantic rejection.
func TestToStatusConflictPrecedence(t *testing.T) {
	for name, err := range map[string]error{
		"bare":       domain.ErrConflict,
		"idempotent": fmt.Errorf("%w: idempotency_key is already associated with another event", domain.ErrConflict),
	} {
		t.Run(name, func(t *testing.T) {
			converted := toStatus(err)
			if status.Code(converted) != codes.AlreadyExists {
				t.Fatalf("code=%v, want AlreadyExists for an idempotency-key conflict", status.Code(converted))
			}
		})
	}
}

// TestStatusErrorDoesNotClassifySnapshotConflictAsInternal pins the log
// taxonomy: the full detail stays server-side, but a normal concurrent-writer
// outcome must not be filed as an internal fault.
func TestStatusErrorDoesNotClassifySnapshotConflictAsInternal(t *testing.T) {
	var logged strings.Builder
	server := &Server{Logger: log.New(&logged, "", 0)}
	detail := fmt.Errorf("%w: /var/lib/audit/state.json: simulated stale writer", store.ErrSnapshotConflict)

	converted := server.statusError(detail)
	if status.Code(converted) != codes.Unavailable {
		t.Fatalf("code=%v, want Unavailable", status.Code(converted))
	}
	if strings.Contains(logged.String(), "grpc internal error") {
		t.Fatalf("snapshot conflict filed as an internal fault: %q", logged.String())
	}
	if !strings.Contains(logged.String(), "grpc snapshot conflict") || !strings.Contains(logged.String(), "state.json: simulated stale writer") {
		t.Fatalf("conflict detail was not logged server-side: %q", logged.String())
	}

	// The internal-fault prefix keeps its own full detail.
	logged.Reset()
	internal := server.statusError(errors.New("open /var/lib/audit/state.json.tmp: permission denied"))
	if status.Code(internal) != codes.Internal || !strings.Contains(logged.String(), "grpc internal error") {
		t.Fatalf("internal logging regressed: code=%v log=%q", status.Code(internal), logged.String())
	}

	// Domain errors are not logged at all (regression guard for the existing
	// contract) and a nil Logger must not panic.
	logged.Reset()
	_ = server.statusError(domain.ErrConflict)
	if logged.Len() != 0 {
		t.Fatalf("domain error logged: %q", logged.String())
	}
	if code := status.Code((&Server{}).statusError(detail)); code != codes.Unavailable {
		t.Fatalf("nil-logger code=%v, want Unavailable without a panic", code)
	}
}

// TestRejectionCodeCoversEveryCodeToStatusProduces pins the machine-readable
// rejection vocabulary for every class the ingest error mapper can produce,
// so a batch member is never published as "unknown".
func TestRejectionCodeCoversEveryCodeToStatusProduces(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"invalid", domain.ErrInvalid, "invalid_argument"},
		{"unauthorized", domain.ErrUnauthorized, "unauthenticated"},
		{"forbidden", domain.ErrForbidden, "permission_denied"},
		{"not_found", domain.ErrNotFound, "not_found"},
		{"conflict", domain.ErrConflict, "already_exists"},
		{"quota", domain.ErrQuotaExceeded, "resource_exhausted"},
		{"schema", domain.ErrSchemaNotFound, "failed_precondition"},
		{"tenant_mismatch", domain.ErrTenantMismatch, "failed_precondition"},
		{"internal", errors.New("boom"), "internal"},
		{"snapshot_conflict", store.ErrSnapshotConflict, "unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			converted := toStatus(tc.err)
			if got := rejectionCode(converted); got != tc.want {
				t.Fatalf("rejection_code=%q for code %v, want %q", got, status.Code(converted), tc.want)
			}
			if rejectionCode(converted) == "unknown" {
				t.Fatal("a reachable code published the unknown token")
			}
		})
	}
}

// TestRejectionCodeFlagsUnreachableCases walks the whole rejection_code
// vocabulary: every code the switch knows must either be reachable from
// toStatus or be explicitly listed as reserved, and both lists together must
// cover the switch exactly — so an arm cannot be added or dropped silently.
func TestRejectionCodeFlagsUnreachableCases(t *testing.T) {
	reachable := map[codes.Code]string{
		codes.InvalidArgument:    "invalid_argument",
		codes.Unauthenticated:    "unauthenticated",
		codes.PermissionDenied:   "permission_denied",
		codes.NotFound:           "not_found",
		codes.AlreadyExists:      "already_exists",
		codes.ResourceExhausted:  "resource_exhausted",
		codes.FailedPrecondition: "failed_precondition",
		codes.Internal:           "internal",
		codes.Unavailable:        "unavailable",
	}
	// Documented in the rejectionCode doc comment as "reserved": no sentinel
	// maps to them yet, so they are part of the vocabulary but unreachable.
	knownUnreachableFromToStatus := map[codes.Code]string{
		codes.Aborted:          "aborted",
		codes.Canceled:         "canceled",
		codes.DeadlineExceeded: "deadline_exceeded",
		codes.OutOfRange:       "out_of_range",
		codes.Unimplemented:    "unimplemented",
		codes.DataLoss:         "data_loss",
	}
	known := map[codes.Code]bool{}
	for code, want := range reachable {
		known[code] = true
		if got := rejectionCode(status.Error(code, "x")); got != want {
			t.Errorf("code %v maps to %q, want %q", code, got, want)
		}
	}
	for code, want := range knownUnreachableFromToStatus {
		known[code] = true
		if got := rejectionCode(status.Error(code, "x")); got != want {
			t.Errorf("reserved code %v maps to %q, want %q", code, got, want)
		}
	}
	// Any code outside both sets must be unknown, and every gRPC code in the
	// switch must be classified by one of the two sets.
	for code := codes.OK; code <= codes.Unauthenticated+20; code++ {
		classified := known[code]
		got := rejectionCode(status.Error(code, "x"))
		if !classified && got != "unknown" {
			t.Errorf("code %v is undocumented but publishes %q", code, got)
		}
		if classified && got == "unknown" {
			t.Errorf("documented code %v publishes unknown", code)
		}
	}
	if got := rejectionCode(errors.New("plain")); got != "unknown" {
		t.Fatalf("non-status error=%q, want unknown", got)
	}
}

// TestWriteBatchSnapshotConflictIsRetryableAndKeepsSuffixUnattempted is the
// end-to-end at-least-once property: once the optimistic lock is exhausted the
// member is rejected as a retryable condition (never as an internal fault),
// the durable committed prefix survives, and the remaining members stay
// NOT_ATTEMPTED with no rejection code.
func TestWriteBatchSnapshotConflictIsRetryableAndKeepsSuffixUnattempted(t *testing.T) {
	committedID := "grpc-conflict-committed"
	conflictingID := "grpc-conflict-rejected"
	suffixID := "grpc-conflict-unattempted"
	backend := newConflictBackend(t, store.EventKey("tenant-a", conflictingID))
	injected := store.NewWithBackend(backend)

	var logged strings.Builder
	st, client, ctx := newGRPCHarnessOpts(t, grpcHarnessOptions{
		logger:        log.New(&logged, "", 0),
		injectedStore: injected,
		seeded:        true,
	})
	committed := testProtoEvent(committedID, "crm")
	conflicting := testProtoEvent(conflictingID, "crm")
	suffix := testProtoEvent(suffixID, "crm")

	response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{committed, conflicting, suffix}})
	if err != nil {
		t.Fatalf("WriteBatch err=%v, want nil for a member-level conflict", err)
	}
	if len(response.GetOutcomes()) != 3 || len(response.GetReceipts()) != 1 {
		t.Fatalf("response=%v, want three outcomes and one receipt", response)
	}
	assertBatchOutcome(t, response.GetOutcomes()[0], 0, committedID, auditv1.WriteBatchOutcome_COMMITTED, "", true)
	assertBatchOutcome(t, response.GetOutcomes()[1], 1, conflictingID, auditv1.WriteBatchOutcome_REJECTED, "unavailable", false)
	assertBatchOutcome(t, response.GetOutcomes()[2], 2, suffixID, auditv1.WriteBatchOutcome_NOT_ATTEMPTED, "", false)
	if response.GetOutcomes()[1].GetRejectionCode() == "internal" {
		t.Fatal("a retryable-transient conflict was published as an internal fault")
	}

	// Non-vacuity: the store's optimistic-lock loop really ran for the
	// conflicting member (snapshotConflictRetries+1 saves) after committing
	// the durable prefix once.
	if calls := backend.calls(); calls < 5 {
		t.Fatalf("save calls=%d, want the commit plus the full conflict retry loop", calls)
	}

	// Nothing of the rejected member reached the durable ledger, and the
	// committed prefix did.
	assertNoFramedKeys(t, st, conflictingID, suffixID)
	if events, receipts := snapshotCounts(t, st); events != 1 || receipts != 1 {
		t.Fatalf("durable counts events=%d receipts=%d, want only the committed prefix", events, receipts)
	}
	if strings.Contains(logged.String(), "grpc internal error") {
		t.Fatalf("conflict filed as an internal fault: %q", logged.String())
	}
	if !strings.Contains(logged.String(), "grpc snapshot conflict") {
		t.Fatalf("conflict was not logged under its own prefix: %q", logged.String())
	}

	// R7/G4 oracle: the service layer returns the sentinel unwrapped with a
	// zero receipt, which is why hasDurableReceipt — the only authority for
	// COMMITTED — could not mark the member committed.
	directService, err := service.New(injected, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	receipt, directErr := directService.Ingest(ctx, "tenant-a", domain.IngestPrincipal{ClientID: "crm"}, conflictTestEvent(t, conflictingID), "")
	if !errors.Is(directErr, store.ErrSnapshotConflict) {
		t.Fatalf("direct Ingest err=%v, want the store sentinel to survive the service layer", directErr)
	}
	if hasDurableReceipt(receipt) {
		t.Fatalf("direct Ingest receipt=%+v, want a zero receipt so the batch cannot report COMMITTED", receipt)
	}
}

// conflictTestEvent mirrors the proto envelope the harness sends, so the
// direct service-level oracle exercises the same ingest input.
func conflictTestEvent(t *testing.T, eventID string) domain.Event {
	t.Helper()
	envelope := testProtoEvent(eventID, "crm")
	event, err := fromProto(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

// TestWriteAndWriteStreamSnapshotConflictStatus pins unary/stream parity: an
// exhausted optimistic-lock conflict must surface codes.Unavailable on both
// surfaces, never codes.Internal, and the stream must terminate rather than
// invent a skip-and-continue class for a conflict.
func TestWriteAndWriteStreamSnapshotConflictStatus(t *testing.T) {
	t.Run("Write", func(t *testing.T) {
		_, client, ctx := newConflictHarness(t, "grpc-conflict-unary")
		_, err := client.Write(ctx, &auditv1.WriteRequest{Event: testProtoEvent("grpc-conflict-unary", "crm")})
		if code := status.Code(err); code != codes.Unavailable {
			t.Fatalf("Write code=%v, want Unavailable", code)
		}
	})
	t.Run("WriteStream", func(t *testing.T) {
		_, client, ctx := newConflictHarness(t, "grpc-conflict-stream")
		stream, err := client.WriteStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&auditv1.WriteRequest{Event: testProtoEvent("grpc-conflict-stream", "crm")}); err != nil {
			t.Fatal(err)
		}
		if _, recvErr := stream.Recv(); status.Code(recvErr) != codes.Unavailable {
			t.Fatalf("WriteStream code=%v, want Unavailable", status.Code(recvErr))
		}
	})
}

// newConflictHarness wires the ingest gRPC surface over a store whose backend
// reports an exhausted optimistic-lock conflict for eventID.
func newConflictHarness(t *testing.T, eventID string) (*conflictBackend, auditv1.IngestClient, context.Context) {
	t.Helper()
	backend := newConflictBackend(t, store.EventKey("tenant-a", eventID))
	_, client, ctx := newGRPCHarnessOpts(t, grpcHarnessOptions{injectedStore: store.NewWithBackend(backend), seeded: true})
	return backend, client, ctx
}

// TestSnapshotConflictStatusMessageIsRedacted pins the redaction contract for
// the new class: a backend may wrap the sentinel with a state path or an errno
// string, and none of it may cross the boundary even though the server still
// logs the full detail.
func TestSnapshotConflictStatusMessageIsRedacted(t *testing.T) {
	detail := fmt.Errorf("%w: /var/lib/audit/state.json: pgx: connection to vault-audit-vault failed", store.ErrSnapshotConflict)
	converted := toStatus(detail)
	message := status.Convert(converted).Message()
	for _, marker := range []string{"state.json", "permission", "pgx", "vault", "/var/lib"} {
		if strings.Contains(message, marker) {
			t.Fatalf("client message=%q leaked %q", message, marker)
		}
	}
	if message != snapshotConflictMessage {
		t.Fatalf("message=%q, want the fixed redacted constant", message)
	}

	// The internal class stays equally redacted, with the detail kept
	// server-side.
	var logged strings.Builder
	internal := (&Server{Logger: log.New(&logged, "", 0)}).statusError(errors.New("open /var/lib/audit/state.json.tmp: permission denied"))
	if status.Convert(internal).Message() != "internal server error" {
		t.Fatalf("internal message=%q, want fixed redacted text", status.Convert(internal).Message())
	}
	if !strings.Contains(logged.String(), "/var/lib/audit/state.json.tmp") {
		t.Fatalf("internal detail was not logged server-side: %q", logged.String())
	}
}
