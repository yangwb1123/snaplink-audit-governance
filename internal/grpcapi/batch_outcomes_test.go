package grpcapi

import (
	"context"
	"errors"
	"log"
	"strings"
	"testing"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestGRPCWriteBatchOutcomeShape(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	valid := testProtoEvent("grpc-batch-shape-valid", "crm")
	invalid := testProtoEvent("grpc-batch-shape-invalid", "crm")
	invalid.Actor = nil
	suffix := testProtoEvent("grpc-batch-shape-suffix", "crm")

	response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{valid, invalid, suffix}})
	if err != nil {
		t.Fatalf("WriteBatch err=%v, want nil for member rejection", err)
	}
	if len(response.GetOutcomes()) != 3 || len(response.GetReceipts()) != 1 {
		t.Fatalf("response=%v, want three outcomes and one backward-compatible receipt", response)
	}
	assertBatchOutcome(t, response.GetOutcomes()[0], 0, valid.GetEventId(), auditv1.WriteBatchOutcome_COMMITTED, "", true)
	assertBatchOutcome(t, response.GetOutcomes()[1], 1, invalid.GetEventId(), auditv1.WriteBatchOutcome_REJECTED, "invalid_argument", false)
	assertBatchOutcome(t, response.GetOutcomes()[2], 2, suffix.GetEventId(), auditv1.WriteBatchOutcome_NOT_ATTEMPTED, "", false)
	if response.GetReceipts()[0].GetEventId() != valid.GetEventId() {
		t.Fatalf("backward-compatible receipt=%v, want committed prefix", response.GetReceipts()[0])
	}
	if events, receipts := snapshotCounts(t, st); events != 1 || receipts != 1 {
		t.Fatalf("durable counts events=%d receipts=%d, want one committed prefix", events, receipts)
	}
}

func assertBatchOutcome(t *testing.T, outcome *auditv1.WriteBatchOutcome, index int, eventID string, wantStatus auditv1.WriteBatchOutcome_Status, rejectionCode string, wantReceipt bool) {
	t.Helper()
	if outcome == nil {
		t.Fatalf("outcome[%d] is nil", index)
	}
	if outcome.GetInputIndex() != int32(index) || outcome.GetEventId() != eventID || outcome.GetStatus() != wantStatus {
		t.Fatalf("outcome[%d]=%v, want index=%d event_id=%q status=%s", index, outcome, index, eventID, wantStatus)
	}
	if outcome.GetRejectionCode() != rejectionCode {
		t.Fatalf("outcome[%d] rejection_code=%q, want %q", index, outcome.GetRejectionCode(), rejectionCode)
	}
	if (outcome.GetReceipt() != nil) != wantReceipt {
		t.Fatalf("outcome[%d] receipt=%v, want present=%t", index, outcome.GetReceipt(), wantReceipt)
	}
}

func TestGRPCWriteBatchDuplicateOutcomePreservesReceipt(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	event := testProtoEvent("grpc-batch-retry", "crm")
	first, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{event}})
	if err != nil || len(first.GetReceipts()) != 1 {
		t.Fatalf("first batch response=%v err=%v, want one receipt", first, err)
	}
	beforeEvents, beforeReceipts := snapshotCounts(t, st)
	original := first.GetReceipts()[0]

	retry, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{event}})
	if err != nil || len(retry.GetOutcomes()) != 1 || len(retry.GetReceipts()) != 1 {
		t.Fatalf("retry response=%v err=%v, want one committed outcome", retry, err)
	}
	outcome := retry.GetOutcomes()[0]
	if outcome.GetStatus() != auditv1.WriteBatchOutcome_COMMITTED || !outcome.GetReceipt().GetDuplicate() {
		t.Fatalf("retry outcome=%v, want committed duplicate", outcome)
	}
	got := outcome.GetReceipt()
	if got.GetSequence() != original.GetSequence() || got.GetHash() != original.GetHash() || got.GetEventId() != original.GetEventId() {
		t.Fatalf("retry receipt=%v, want original identity=%v", got, original)
	}
	if events, receipts := snapshotCounts(t, st); events != beforeEvents || receipts != beforeReceipts {
		t.Fatalf("retry changed durable counts: before events=%d receipts=%d, after events=%d receipts=%d", beforeEvents, beforeReceipts, events, receipts)
	}
}

func TestGRPCWriteBatchIdempotencyConflictIsRejectedMember(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	owner := testProtoEvent("grpc-batch-idem-owner", "crm")
	conflict := testProtoEvent("grpc-batch-idem-conflict", "crm")
	conflict.IdempotencyKey = owner.GetIdempotencyKey()
	conflict.PayloadJson = []byte(`{"value":2}`)
	suffix := testProtoEvent("grpc-batch-idem-suffix", "crm")

	response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{owner, conflict, suffix}})
	if err != nil {
		t.Fatalf("WriteBatch err=%v, want nil for idempotency conflict", err)
	}
	if len(response.GetReceipts()) != 1 || len(response.GetOutcomes()) != 3 {
		t.Fatalf("response=%v, want one receipt and three outcomes", response)
	}
	assertBatchOutcome(t, response.GetOutcomes()[0], 0, owner.GetEventId(), auditv1.WriteBatchOutcome_COMMITTED, "", true)
	assertBatchOutcome(t, response.GetOutcomes()[1], 1, conflict.GetEventId(), auditv1.WriteBatchOutcome_REJECTED, "already_exists", false)
	assertBatchOutcome(t, response.GetOutcomes()[2], 2, suffix.GetEventId(), auditv1.WriteBatchOutcome_NOT_ATTEMPTED, "", false)
	if events, receipts := snapshotCounts(t, st); events != 1 || receipts != 1 {
		t.Fatalf("conflict changed durable counts: events=%d receipts=%d, want one owner", events, receipts)
	}
	if response.GetReceipts()[0].GetEventId() != owner.GetEventId() {
		t.Fatalf("owner receipt=%v, want original event", response.GetReceipts()[0])
	}
}

func TestGRPCWriteBatchPostCommitArchiveFailureIsCommitted(t *testing.T) {
	archiveErr := errors.New("archive backend path /secret/archive unavailable")
	st, client, ctx := newGRPCHarnessOpts(t, grpcHarnessOptions{archiveStore: &batchFailingArchive{err: archiveErr}})
	archived := testProtoEvent("grpc-batch-archive-failure", "crm")
	suffix := testProtoEvent("grpc-batch-archive-suffix", "crm")

	response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{archived, suffix}, WaitFor: "archived"})
	if err != nil {
		t.Fatalf("WriteBatch err=%v, want nil after durable commit", err)
	}
	if len(response.GetReceipts()) != 1 || len(response.GetOutcomes()) != 2 {
		t.Fatalf("response=%v, want one receipt and two outcomes", response)
	}
	assertBatchOutcome(t, response.GetOutcomes()[0], 0, archived.GetEventId(), auditv1.WriteBatchOutcome_COMMITTED, "", true)
	assertBatchOutcome(t, response.GetOutcomes()[1], 1, suffix.GetEventId(), auditv1.WriteBatchOutcome_NOT_ATTEMPTED, "", false)
	if got := response.GetOutcomes()[0].GetReceipt().GetStatus(); got != "indexed" {
		t.Fatalf("post-commit archive failure receipt status=%q, want indexed", got)
	}
	if strings.Contains(response.String(), archiveErr.Error()) {
		t.Fatalf("archive diagnostic leaked in batch response: %s", response)
	}
	if events, receipts := snapshotCounts(t, st); events != 1 || receipts != 1 {
		t.Fatalf("durable counts events=%d receipts=%d, want committed event", events, receipts)
	}
}

func TestGRPCWriteBatchInternalFailureIsRedacted(t *testing.T) {
	marker := "signing key /var/lib/audit/private.key unavailable"
	var logged strings.Builder
	st, client, ctx := newGRPCHarnessOpts(t, grpcHarnessOptions{
		logger:      log.New(&logged, "", 0),
		signer:      failingBatchSigner{err: errors.New(marker)},
		segmentSize: 1,
	})
	event := testProtoEvent("grpc-batch-internal-failure", "crm")

	response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{event}})
	if err != nil || len(response.GetOutcomes()) != 1 {
		t.Fatalf("response=%v err=%v, want one rejected outcome", response, err)
	}
	assertBatchOutcome(t, response.GetOutcomes()[0], 0, event.GetEventId(), auditv1.WriteBatchOutcome_REJECTED, "internal", false)
	if strings.Contains(response.String(), marker) {
		t.Fatalf("internal diagnostic leaked in batch response: %s", response)
	}
	if !strings.Contains(logged.String(), marker) {
		t.Fatalf("internal diagnostic was not logged server-side: %q", logged.String())
	}
	if events, receipts := snapshotCounts(t, st); events != 0 || receipts != 0 {
		t.Fatalf("internal failure changed durable counts: events=%d receipts=%d", events, receipts)
	}
}

func TestGRPCWriteBatchWholeRequestFailuresHaveNoOutcomes(t *testing.T) {
	st, client, authenticated := newGRPCHarness(t)
	event := testProtoEvent("grpc-whole-request-auth", "crm")
	response, err := client.WriteBatch(context.Background(), &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{event}})
	if status.Code(err) != codes.Unauthenticated || response != nil {
		t.Fatalf("unauthenticated response=%v err=%v, want RPC error and nil response", response, err)
	}
	if events, receipts := snapshotCounts(t, st); events != 0 || receipts != 0 {
		t.Fatalf("authentication failure committed data: events=%d receipts=%d", events, receipts)
	}

	response, err = client.WriteBatch(authenticated, &auditv1.WriteBatchRequest{})
	if status.Code(err) != codes.InvalidArgument || response != nil {
		t.Fatalf("empty response=%v err=%v, want RPC error and nil response", response, err)
	}
	if events, receipts := snapshotCounts(t, st); events != 0 || receipts != 0 {
		t.Fatalf("empty batch changed durable counts: events=%d receipts=%d", events, receipts)
	}
}

func TestGRPCWriteBatchTransportFailureHasNoOutcomes(t *testing.T) {
	st, client, ctx := newGRPCHarnessOpts(t, grpcHarnessOptions{
		serverOpts: []grpc.ServerOption{grpc.MaxRecvMsgSize(MaxRecvBytes)},
	})
	event := testProtoEvent("grpc-batch-transport-over", "crm")
	event.Reason = strings.Repeat("r", MaxRecvBytes)
	response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{event}})
	if status.Code(err) != codes.ResourceExhausted || response != nil {
		t.Fatalf("transport response=%v err=%v, want RPC ResourceExhausted and nil response", response, err)
	}
	if events, receipts := snapshotCounts(t, st); events != 0 || receipts != 0 {
		t.Fatalf("transport failure committed data: events=%d receipts=%d", events, receipts)
	}
}

type batchFailingArchive struct {
	err error
}

func (a *batchFailingArchive) Put(context.Context, string, []byte) error   { return a.err }
func (a *batchFailingArchive) Get(context.Context, string) ([]byte, error) { return nil, a.err }
func (a *batchFailingArchive) Ready(context.Context) error                 { return nil }

type failingBatchSigner struct {
	err error
}

func (s failingBatchSigner) Sign(context.Context, []byte) (string, error) { return "", s.err }
func (s failingBatchSigner) Verify(context.Context, []byte, string) (bool, error) {
	return false, s.err
}
func (s failingBatchSigner) Algorithm() string { return "test-failing-signer" }

func TestGRPCAmbiguousAuthorizationRejectedBeforeIngest(t *testing.T) {
	st, client, validContext := newGRPCHarnessOpts(t, grpcHarnessOptions{publisher: &failingLedgeredPublisher{}})
	cases := []struct {
		name   string
		values []string
	}{
		{name: "none", values: nil},
		{name: "identical-valid", values: []string{"Bearer dev:tenant-a:service:crm", "Bearer dev:tenant-a:service:crm"}},
		{name: "valid-and-invalid", values: []string{"Bearer dev:tenant-a:service:crm", "Bearer invalid"}},
		{name: "different-tenants", values: []string{"Bearer dev:tenant-a:service:crm", "Bearer dev:tenant-b:service:crm"}},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/Write", func(t *testing.T) {
			_, err := client.Write(grpcAuthContext(tc.values...), &auditv1.WriteRequest{Event: testProtoEvent("grpc-auth-"+tc.name+"-write", "crm")})
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("Write code=%v, want Unauthenticated", status.Code(err))
			}
		})
		t.Run(tc.name+"/WriteBatch", func(t *testing.T) {
			response, err := client.WriteBatch(grpcAuthContext(tc.values...), &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{testProtoEvent("grpc-auth-"+tc.name+"-batch", "crm")}})
			if status.Code(err) != codes.Unauthenticated || response != nil {
				t.Fatalf("WriteBatch response=%v code=%v, want Unauthenticated and nil response", response, status.Code(err))
			}
		})
		t.Run(tc.name+"/WriteStream", func(t *testing.T) {
			stream, err := client.WriteStream(grpcAuthContext(tc.values...))
			if err != nil {
				if status.Code(err) != codes.Unauthenticated {
					t.Fatalf("WriteStream open code=%v, want Unauthenticated", status.Code(err))
				}
				return
			}
			if _, recvErr := stream.Recv(); status.Code(recvErr) != codes.Unauthenticated {
				t.Fatalf("WriteStream receive code=%v, want Unauthenticated", status.Code(recvErr))
			}
		})
		assertGRPCStoreEmpty(t, st)
	}

	response, err := client.Write(validContext, &auditv1.WriteRequest{Event: testProtoEvent("grpc-auth-single-valid", "crm")})
	if err != nil || response.GetEventId() != "grpc-auth-single-valid" {
		t.Fatalf("single credential response=%v err=%v, want successful receipt", response, err)
	}
	if events, receipts := snapshotCounts(t, st); events != 1 || receipts != 1 {
		t.Fatalf("single credential counts: events=%d receipts=%d, want one each", events, receipts)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if len(data.LedgeredOutbox) != 1 {
			t.Fatalf("single credential outbox=%d, want one", len(data.LedgeredOutbox))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func grpcAuthContext(values ...string) context.Context {
	if len(values) == 0 {
		return context.Background()
	}
	pairs := make([]string, 0, len(values)*2)
	for _, value := range values {
		pairs = append(pairs, "authorization", value)
	}
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs(pairs...))
}

func assertGRPCStoreEmpty(t *testing.T, st *store.Store) {
	t.Helper()
	if events, receipts := snapshotCounts(t, st); events != 0 || receipts != 0 {
		t.Fatalf("ambiguous credentials changed persistence: events=%d receipts=%d", events, receipts)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if len(data.LedgeredOutbox) != 0 {
			t.Fatalf("ambiguous credentials changed ledgered outbox: %d", len(data.LedgeredOutbox))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func snapshotCounts(t *testing.T, st *store.Store) (int, int) {
	t.Helper()
	var events, receipts int
	if err := st.Read(func(data *store.Snapshot) error {
		events = len(data.Events)
		receipts = len(data.Receipts)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return events, receipts
}
