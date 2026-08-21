package service

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// TestArchivedEventEvictionKeepsReadSemantics pins the hot/cold boundary:
// archive success removes only the payload, while receipts and the verified
// archive object keep direct reads, timelines, integrity and export usable.
func TestArchivedEventEvictionKeepsReadSemantics(t *testing.T) {
	svc := testService(t, true)
	at := time.Unix(1_700_000_010, 0).UTC()
	original := testEvent("evicted-event", "evicted-operation", at)
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, original, domain.StatusArchived)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != domain.StatusArchived || receipt.IdempotencyKey != original.IdempotencyKey {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	snapshot, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	key := store.EventKey("tenant-a", original.EventID)
	if _, ok := snapshot.Events[key]; ok {
		t.Fatal("archived event payload remains in the hot snapshot")
	}
	if snapshot.Receipts[key].Status != domain.StatusArchived {
		t.Fatalf("receipt status=%q, want archived", snapshot.Receipts[key].Status)
	}

	got, err := svc.GetEvent("tenant-a", "auditor", original.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if got.EventID != original.EventID || got.Hash != receipt.Hash || got.Payload["resource"] != "invoice" {
		t.Fatalf("archive fallback changed event: got=%+v receipt=%+v", got, receipt)
	}
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil || !result.Valid || result.EventCount != 1 {
		t.Fatalf("archived integrity result=%+v err=%v", result, err)
	}
	operationReplay, err := svc.ReplayOperation("tenant-a", "auditor", "evicted-operation")
	if err != nil || operationReplay.EventCount != 1 {
		t.Fatalf("archived operation replay=%+v err=%v", operationReplay, err)
	}
	aggregateReplay, err := svc.ReplayAggregate("tenant-a", "auditor", "invoice", "inv-1")
	if err != nil || aggregateReplay.EventCount != 1 {
		t.Fatalf("archived aggregate replay=%+v err=%v", aggregateReplay, err)
	}
	timeline, err := svc.OperationTimeline("tenant-a", "auditor", "evicted-operation")
	if err != nil || len(timeline) != 1 || timeline[0].EventID != original.EventID {
		t.Fatalf("archived timeline=%+v err=%v", timeline, err)
	}
	query := domain.Query{From: at.Add(-time.Minute), To: at.Add(time.Minute)}
	queryResult, err := svc.QueryEvents("tenant-a", "auditor", query)
	if err != nil || queryResult.Count != 1 {
		t.Fatalf("archive-inclusive query=%+v err=%v", queryResult, err)
	}
	job, err := svc.CreateExport("tenant-a", "auditor", query)
	if err != nil {
		t.Fatal(err)
	}
	export := waitExportCompleted(t, svc, job.ID)
	if export.Status != "completed" || export.EventCount != 1 {
		t.Fatalf("archived export=%+v", export)
	}

	duplicate, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, original, domain.StatusLedgered)
	if err != nil || !duplicate.Duplicate || duplicate.Sequence != receipt.Sequence {
		t.Fatalf("archived duplicate=%+v err=%v", duplicate, err)
	}
	conflict := original
	conflict.Payload = map[string]any{"resource": "invoice", "value": 99}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, conflict, domain.StatusLedgered); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("archived content conflict err=%v", err)
	}
}

func TestArchivedEventEvictionSurvivesFileRestart(t *testing.T) {
	root := t.TempDir()
	statePath := filepath.Join(root, "state.json")
	archiveDir := filepath.Join(root, "archive")
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := New(st, Config{ArchiveDir: archiveDir, SegmentSize: 2, SigningSecret: "test-secret", Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	seedTestDomain(t, svc)
	event := testEvent("restart-evicted", "restart-operation", time.Unix(1_700_000_010, 0).UTC())
	before, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetEvent("tenant-a", "auditor", event.EventID); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted, err := New(reopened, Config{ArchiveDir: archiveDir, SegmentSize: 2, SigningSecret: "test-secret", Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := restarted.GetEvent("tenant-a", "auditor", event.EventID)
	if err != nil || got.EventID != event.EventID || got.Hash != before.Hash {
		t.Fatalf("restarted archive event=%+v err=%v, want hash %s", got, err, before.Hash)
	}
	receipt, err := restarted.GetReceipt("tenant-a", "auditor", event.EventID)
	if err != nil || receipt.Status != domain.StatusArchived || receipt.Hash != before.Hash {
		t.Fatalf("restarted receipt=%+v err=%v, want archived hash %s", receipt, err, before.Hash)
	}
	verified, err := restarted.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil || !verified.Valid || verified.EventCount != 1 {
		t.Fatalf("restarted integrity=%+v err=%v", verified, err)
	}
}

func TestArchivedExportDigestMatchesHotExport(t *testing.T) {
	svc := testService(t, false)
	event := testEvent("export-equivalence", "export-equivalence", time.Unix(1_700_000_010, 0).UTC())
	archiveDir := filepath.Join(t.TempDir(), "archive")
	svc.Config.Archive = &recordingArchive{fail: true}
	if receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil || receipt.Status != domain.StatusIndexed {
		t.Fatalf("seed hot event receipt=%+v err=%v", receipt, err)
	}
	svc.Config.Archive = &archive.FileStore{Dir: archiveDir}
	query := domain.Query{From: event.OccurredAt.Add(-time.Minute), To: event.OccurredAt.Add(time.Minute)}
	beforeJob, err := svc.CreateExport("tenant-a", "auditor", query)
	if err != nil {
		t.Fatal(err)
	}
	beforeJob = waitExportCompleted(t, svc, beforeJob.ID)
	if beforeJob.Status != "completed" || beforeJob.EventCount != 1 {
		t.Fatalf("hot export=%+v", beforeJob)
	}
	if count, err := svc.ArchivePending(context.Background(), "tenant-a"); err != nil || count != 1 {
		t.Fatalf("archive pending count=%d err=%v", count, err)
	}
	afterJob, err := svc.CreateExport("tenant-a", "auditor", query)
	if err != nil {
		t.Fatal(err)
	}
	afterJob = waitExportCompleted(t, svc, afterJob.ID)
	if afterJob.Status != "completed" || afterJob.EventCount != beforeJob.EventCount || afterJob.Digest != beforeJob.Digest {
		t.Fatalf("hot/cold export mismatch before=%+v after=%+v", beforeJob, afterJob)
	}
}

func TestArchivedReceiptProtectsIdempotencyKey(t *testing.T) {
	svc := testService(t, true)
	first := testEvent("idempotency-archived-1", "idempotency-archived", time.Unix(1_700_000_010, 0).UTC())
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, first, domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	second := testEvent("idempotency-archived-2", "idempotency-archived", first.OccurredAt.Add(time.Second))
	second.IdempotencyKey = first.IdempotencyKey
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, second, domain.StatusLedgered)
	if !errors.Is(err, domain.ErrConflict) || !receipt.Conflict || receipt.ErrorCode != "idempotency_key_conflict" {
		t.Fatalf("archived idempotency conflict receipt=%+v err=%v", receipt, err)
	}
}

func TestArchivePendingEvictsAfterRetry(t *testing.T) {
	svc := testService(t, false)
	event := testEvent("retry-eviction", "retry-eviction", time.Unix(1_700_000_010, 0).UTC())
	archiveStub := &recordingArchive{fail: true}
	svc.Config.Archive = archiveStub
	if receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil || receipt.Status != domain.StatusIndexed {
		t.Fatalf("seed indexed receipt=%+v err=%v", receipt, err)
	}
	archiveDir := filepath.Join(t.TempDir(), "archive")
	svc.Config.Archive = &archive.FileStore{Dir: archiveDir}
	count, err := svc.ArchivePending(context.Background(), "tenant-a")
	if err != nil || count != 1 {
		t.Fatalf("ArchivePending count=%d err=%v", count, err)
	}
	snapshot, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snapshot.Events[store.EventKey("tenant-a", event.EventID)]; ok {
		t.Fatal("retry-archived event remains in hot snapshot")
	}
	if _, err := svc.GetEvent("tenant-a", "auditor", event.EventID); err != nil {
		t.Fatalf("retry archive fallback: %v", err)
	}
}

func TestArchivePendingCleansLegacyArchivedHotBody(t *testing.T) {
	svc := testService(t, true)
	event := testEvent("legacy-archived-hot-body", "legacy-archived-hot-body", time.Unix(1_700_000_010, 0).UTC())
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	key := store.EventKey("tenant-a", event.EventID)
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		receipt := data.Receipts[key]
		if receipt.Status != domain.StatusArchived {
			return errors.New("test receipt is not archived")
		}
		data.Events[key] = domain.Event{
			EventID: event.EventID, TenantID: "tenant-a", StreamID: receipt.StreamID,
			Sequence: receipt.Sequence, Hash: receipt.Hash,
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	count, err := svc.ArchivePending(context.Background(), "tenant-a")
	if err != nil || count != 0 {
		t.Fatalf("cleanup count=%d err=%v, want zero newly archived events", count, err)
	}
	snapshot, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snapshot.Events[key]; ok {
		t.Fatal("archived receipt with legacy hot body was not evicted")
	}
}
