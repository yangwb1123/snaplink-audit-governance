package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type ingestArchiveFailureStore struct {
	eventErr   error
	segmentErr error
	keys       []string
}

func (s *ingestArchiveFailureStore) Put(_ context.Context, key string, _ []byte) error {
	s.keys = append(s.keys, key)
	if strings.HasPrefix(key, "events/") {
		return s.eventErr
	}
	if strings.HasPrefix(key, "segments/") {
		return s.segmentErr
	}
	return nil
}

func (s *ingestArchiveFailureStore) Get(context.Context, string) ([]byte, error) { return nil, nil }

func (s *ingestArchiveFailureStore) Ready(context.Context) error { return nil }

func TestIngestArchiveFailuresAreRetainedAndAllWritesAttempted(t *testing.T) {
	tests := []struct {
		name string
		hot  bool
	}{
		{name: "legacy", hot: false},
		{name: "hot-cold", hot: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var svc *Service
			if tc.hot {
				_, svc = hotColdService(t)
				svc.Config.SegmentSize = 2
			} else {
				svc = testService(t, true)
			}
			eventErr := errors.New("event archive unavailable")
			segmentErr := errors.New("segment archive unavailable")
			archiveStore := &ingestArchiveFailureStore{eventErr: eventErr, segmentErr: segmentErr}
			svc.Config.Archive = archiveStore

			first := testEvent("archive-failure-first-"+tc.name, "archive-failure", time.Unix(1_700_000_010, 0).UTC())
			if receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, first, domain.StatusLedgered); err != nil || receipt.Status != domain.StatusIndexed {
				t.Fatalf("first ingest receipt=%+v err=%v, want indexed and nil", receipt, err)
			}
			archiveStore.keys = nil

			second := testEvent("archive-failure-second-"+tc.name, "archive-failure", first.OccurredAt.Add(time.Second))
			receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, second, domain.StatusArchived)
			if !errors.Is(err, eventErr) || !errors.Is(err, segmentErr) {
				t.Fatalf("archive error=%v, want both archive sentinels", err)
			}
			if receipt.Status != domain.StatusIndexed || !receipt.ArchivedAt.IsZero() {
				t.Fatalf("receipt=%+v, want indexed with no ArchivedAt", receipt)
			}
			if len(archiveStore.keys) != 2 || !strings.HasPrefix(archiveStore.keys[0], "events/") || !strings.HasPrefix(archiveStore.keys[1], "segments/") {
				t.Fatalf("archive puts=%v, want event and segment attempts", archiveStore.keys)
			}
		})
	}
}

func TestDuplicateWaitForArchivedDoesNotCreateLedgerEntry(t *testing.T) {
	tests := []struct {
		name string
		hot  bool
	}{
		{name: "legacy", hot: false},
		{name: "hot-cold", hot: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var svc *Service
			var st *store.Store
			if tc.hot {
				st, svc = hotColdService(t)
			} else {
				svc = testService(t, true)
				st = svc.Store
			}
			archiveStore := &ingestArchiveFailureStore{eventErr: errors.New("archive unavailable")}
			svc.Config.Archive = archiveStore
			event := testEvent("archive-duplicate-"+tc.name, "archive-duplicate", time.Unix(1_700_000_010, 0).UTC())

			first, firstErr := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived)
			if firstErr == nil || first.Status != domain.StatusIndexed {
				t.Fatalf("first receipt=%+v err=%v, want indexed and an archive error", first, firstErr)
			}
			puts := len(archiveStore.keys)
			recordsBefore := ledgerRecordCount(t, st, tc.hot, event.EventID)

			retry, retryErr := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived)
			if retryErr == nil || !retry.Duplicate || retry.Status != domain.StatusIndexed {
				t.Fatalf("retry receipt=%+v err=%v, want duplicate indexed with retryable error", retry, retryErr)
			}
			if len(archiveStore.keys) != puts {
				t.Fatalf("duplicate retry issued %d archive puts, want no additional puts", len(archiveStore.keys)-puts)
			}
			if got := ledgerRecordCount(t, st, tc.hot, event.EventID); got != recordsBefore {
				t.Fatalf("duplicate retry added %d ledger records, want none", got-recordsBefore)
			}
		})
	}
}

func TestArchivePendingRetriesFailedArchivedWaitWithoutChangingIdentity(t *testing.T) {
	tests := []struct {
		name string
		hot  bool
	}{
		{name: "legacy", hot: false},
		{name: "hot-cold", hot: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var svc *Service
			var st *store.Store
			if tc.hot {
				st, svc = hotColdService(t)
			} else {
				svc = testService(t, true)
				st = svc.Store
			}
			archiveErr := errors.New("archive unavailable")
			svc.Config.Archive = &ingestArchiveFailureStore{eventErr: archiveErr}
			event := testEvent("archive-retry-"+tc.name, "archive-retry", time.Unix(1_700_000_010, 0).UTC())

			failed, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived)
			if !errors.Is(err, archiveErr) {
				t.Fatalf("ingest err=%v, want archive error", err)
			}
			if failed.Status != domain.StatusIndexed || !failed.ArchivedAt.IsZero() {
				t.Fatalf("failed receipt=%+v, want indexed and not archived", failed)
			}
			stored, err := svc.GetReceipt("tenant-a", "", event.EventID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.Status != domain.StatusIndexed || !stored.ArchivedAt.IsZero() {
				t.Fatalf("stored receipt=%+v, want indexed and not archived", stored)
			}
			if stored.EventID != failed.EventID || stored.Sequence != failed.Sequence || stored.Hash != failed.Hash || stored.IdempotencyKey != failed.IdempotencyKey {
				t.Fatalf("stored receipt identity changed: failed=%+v stored=%+v", failed, stored)
			}

			archiveDir := t.TempDir()
			svc.Config.ArchiveDir = archiveDir
			svc.Config.Archive = &archive.FileStore{Dir: archiveDir}
			count, err := svc.ArchivePending(context.Background(), "tenant-a")
			if err != nil || count != 1 {
				t.Fatalf("ArchivePending count=%d err=%v, want 1,nil", count, err)
			}
			archivedReceipt, err := svc.GetReceipt("tenant-a", "", event.EventID)
			if err != nil {
				t.Fatal(err)
			}
			if archivedReceipt.Status != domain.StatusArchived || archivedReceipt.IndexedAt.IsZero() || archivedReceipt.ArchivedAt.IsZero() {
				t.Fatalf("archived receipt=%+v, want archived with timestamps", archivedReceipt)
			}
			if archivedReceipt.EventID != failed.EventID || archivedReceipt.Sequence != failed.Sequence || archivedReceipt.Hash != failed.Hash || archivedReceipt.IdempotencyKey != failed.IdempotencyKey {
				t.Fatalf("archived receipt identity changed: failed=%+v archived=%+v", failed, archivedReceipt)
			}
			archivedEvent, err := svc.GetEvent("tenant-a", "auditor", event.EventID)
			if err != nil {
				t.Fatal(err)
			}
			if archivedEvent.EventID != event.EventID || archivedEvent.Sequence != failed.Sequence || archivedEvent.Hash != failed.Hash || archivedEvent.IdempotencyKey != event.IdempotencyKey {
				t.Fatalf("archived event identity changed: event=%+v archived=%+v failed=%+v", event, archivedEvent, failed)
			}

			recordsAfterFirstRetry := ledgerRecordCount(t, st, tc.hot, event.EventID)
			count, err = svc.ArchivePending(context.Background(), "tenant-a")
			if err != nil || count != 0 {
				t.Fatalf("second ArchivePending count=%d err=%v, want 0,nil", count, err)
			}
			if got := ledgerRecordCount(t, st, tc.hot, event.EventID); got != recordsAfterFirstRetry {
				t.Fatalf("repeat ArchivePending changed receipt records: before=%d after=%d", recordsAfterFirstRetry, got)
			}
		})
	}
}

func ledgerRecordCount(t *testing.T, st *store.Store, hot bool, eventID string) int {
	t.Helper()
	if !hot {
		snapshot, err := st.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := snapshot.Receipts[store.EventKey("tenant-a", eventID)]; ok {
			return 1
		}
		return 0
	}
	count := 0
	if err := st.LedgerScan("tenant-a", func(record store.LedgerRecord) error {
		if record.RecordType == store.LedgerReceipt && record.Key == store.EventKey("tenant-a", eventID) {
			count++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return count
}
