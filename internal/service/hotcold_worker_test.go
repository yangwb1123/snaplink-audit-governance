package service

import (
	"context"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func hotColdService(t *testing.T) (*store.Store, *Service) {
	t.Helper()
	st, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	svc := newServiceOn(t, st)
	seedTestDomain(t, svc)
	return st, svc
}

func TestHotColdArchivePendingUsesTenantLedger(t *testing.T) {
	st, svc := hotColdService(t)
	event := testEvent("evt-hotcold-archive", "op-hotcold", time.Unix(1_700_000_010, 0).UTC())
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != domain.StatusIndexed {
		t.Fatalf("ingest status=%s, want indexed before archive worker", receipt.Status)
	}
	dir := t.TempDir()
	svc.Config.ArchiveDir = dir
	svc.Config.Archive = &archive.FileStore{Dir: dir}
	if count, err := svc.ArchivePending(context.Background(), "tenant-a"); err != nil || count != 1 {
		t.Fatalf("ArchivePending count=%d err=%v, want 1,nil", count, err)
	}
	snapshot, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	key := store.EventKey("tenant-a", event.EventID)
	if _, ok := snapshot.Events[key]; ok {
		t.Fatal("archived event remains in tenant hot snapshot")
	}
	if got := snapshot.Receipts[key]; got.Status != domain.StatusArchived {
		t.Fatalf("receipt status=%s, want archived", got.Status)
	}
	var records []store.LedgerRecord
	if err := st.LedgerScan("tenant-a", func(record store.LedgerRecord) error {
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(records) < 2 {
		t.Fatalf("ledger records=%d, want receipt v1 and archived v2", len(records))
	}
	loaded, err := svc.GetEvent("tenant-a", "auditor", event.EventID)
	if err != nil {
		t.Fatalf("GetEvent after tenant eviction: %v", err)
	}
	if loaded.EventID != event.EventID || loaded.Sequence != 1 {
		t.Fatalf("fallback event=%+v, want event %s", loaded, event.EventID)
	}
}

func TestHotColdSealPendingSegmentsUsesColdFirst(t *testing.T) {
	st, svc := hotColdService(t)
	event := testEvent("evt-hotcold-seal", "op-hotcold-seal", time.Unix(1_700_000_020, 0).UTC())
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if err := svc.SealPendingSegments(testCtx, "tenant-a"); err != nil {
		t.Fatalf("SealPendingSegments: %v", err)
	}
	snapshot, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	streamKey := store.StreamKey("tenant-a", "tenant-a:aggregate:invoice:inv-1")
	stream := snapshot.Streams[streamKey]
	if len(stream.PendingHashes) != 0 || len(stream.PendingEvents) != 0 {
		t.Fatalf("pending stream was not sealed: %+v", stream)
	}
	if len(snapshot.Segments[streamKey]) != 1 || len(snapshot.Checkpoints[streamKey]) != 1 {
		t.Fatalf("segments=%d checkpoints=%d, want one each", len(snapshot.Segments[streamKey]), len(snapshot.Checkpoints[streamKey]))
	}
}
