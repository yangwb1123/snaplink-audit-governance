package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

func ledgerRecordCount(t *testing.T, st *store.Store, hot bool, eventID string) int {
	t.Helper()
	if !hot {
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
