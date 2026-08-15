package service

import (
	"errors"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// TestIngestRejectsOutOfRangeOccurredAtMutationFree is AC-2: ingesting an
// event whose occurred_at lies outside the ledger horizon [MinOccurredAt,
// MaxOccurredAt] fails with the ErrInvalid-compatible sentinel before any
// snapshot mutation — no new Events, Receipts, Streams, Segments, Checkpoints
// or admin actions — and a valid ingest after the rejections still succeeds
// (store not wedged). Mirrors the colon-class mutation-free probe.
func TestIngestRejectsOutOfRangeOccurredAtMutationFree(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	counts := func() (events, receipts, streams, segments, checkpoints, actions int) {
		t.Helper()
		if err := svc.Store.Read(func(data *store.Snapshot) error {
			events = len(data.Events)
			receipts = len(data.Receipts)
			streams = len(data.Streams)
			segments = len(data.Segments)
			checkpoints = len(data.Checkpoints)
			actions = len(data.AdminActions)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return events, receipts, streams, segments, checkpoints, actions
	}
	type countsValue struct {
		events, receipts, streams, segments, checkpoints, actions int
	}
	snapshotCounts := func() countsValue {
		e, r, st, se, c, a := counts()
		return countsValue{e, r, st, se, c, a}
	}
	before := snapshotCounts()

	for _, tc := range []struct {
		name string
		at   time.Time
	}{
		{"year-2300-above-ceiling", time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"year-1800-below-floor", time.Date(1800, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		event := testEvent("occ-boundary-"+tc.name, "op-occ", at)
		event.OccurredAt = tc.at
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); !errors.Is(err, domain.ErrInvalid) {
			t.Errorf("%s: Ingest = %v, want ErrInvalid", tc.name, err)
		} else if !errors.Is(err, domain.ErrOccurredAtOutOfRange) {
			t.Errorf("%s: Ingest = %v, want ErrOccurredAtOutOfRange", tc.name, err)
		}
	}
	after := snapshotCounts()
	if after != before {
		t.Fatalf("rejected out-of-range ingests mutated the snapshot: before %+v after %+v", before, after)
	}

	// A valid ingest after the rejections succeeds (store not wedged).
	event := testEvent("occ-boundary-valid", "op-occ-ok", at)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatalf("valid ingest after out-of-range rejections failed: %v", err)
	}
}
