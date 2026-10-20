package service

import (
	"errors"
	"fmt"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestOperationSummaryDoesNotInheritWholeTimelineCap(t *testing.T) {
	svc := queryMemoryService(t, false)
	// Construct exact source/chain/receipt coordinates before starting the
	// tested operation; this is a semantic cap oracle, not ingest throughput,
	// crash durability or full-history completeness evidence.
	events := map[string]domain.Event{}
	receipts := map[string]domain.EventReceipt{}
	previous := ""
	for i := 0; i <= domain.MaxTimelineEvents; i++ {
		event := testEvent(fmt.Sprintf("summary-limit-%05d", i), "summary-operation", pinnedNow)
		event.TenantID = "tenant-a"
		event.StreamID = event.Stream()
		event.Sequence, event.PrevHash = int64(i+1), previous
		var err error
		event.SourceDigest, err = domain.EventContentDigest(event)
		if err != nil {
			t.Fatal("could not prepare exact summary content")
		}
		event.Hash, err = svc.eventHash(event)
		if err != nil {
			t.Fatal("could not prepare exact summary chain coordinates")
		}
		previous = event.Hash
		key := store.EventKey("tenant-a", event.EventID)
		events[key] = event
		receipts[key] = domain.EventReceipt{TenantID: "tenant-a", EventID: event.EventID, Status: domain.StatusIndexed, StreamID: event.StreamID, Sequence: event.Sequence, Hash: event.Hash, AcceptedAt: pinnedNow, LedgeredAt: pinnedNow, IndexedAt: pinnedNow}
	}
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error { data.Events, data.Receipts = events, receipts; return nil }); err != nil {
		t.Fatal("could not save exact summary limit fixture")
	}
	summary, err := svc.Operation(testCtx, "tenant-a", "reader", "summary-operation")
	if err != nil || summary.EventCount != domain.MaxTimelineEvents+1 || !summary.FirstAt.Equal(pinnedNow) || !summary.LastAt.Equal(pinnedNow) || len(summary.Outcomes) != 1 || summary.Outcomes[0] != "success" {
		t.Fatal("summary was truncated or inherited the legacy whole-timeline cap")
	}
	if _, err := svc.OperationTimeline(testCtx, "tenant-a", "reader", "summary-operation"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal("legacy whole-timeline safety cap was changed")
	}
	if len(readFacts(t, svc)) != 1 {
		t.Fatal("summary/rejected whole-timeline read facts changed")
	}
}
