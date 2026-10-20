package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestOperationReplayRetainsWholeTimelineCapWithoutTruncation(t *testing.T) {
	svc := queryMemoryService(t, false)
	// Exact content/chain/receipt fixtures test the old semantic cap, not
	// ingest throughput, persistence or a full-history completeness claim.
	events, receipts := map[string]domain.Event{}, map[string]domain.EventReceipt{}
	previous := ""
	var extra domain.Event
	var extraReceipt domain.EventReceipt
	for i := 0; i <= domain.MaxTimelineEvents; i++ {
		event := testEvent(fmt.Sprintf("replay-limit-%05d", i), "replay-operation", pinnedNow)
		event.TenantID, event.StreamID = "tenant-a", "tenant-a:aggregate:invoice:inv-1"
		event.Sequence, event.PrevHash = int64(i+1), previous
		var err error
		event.SourceDigest, err = domain.EventContentDigest(event)
		if err != nil {
			t.Fatal("could not prepare exact replay cap content")
		}
		event.Hash, err = svc.eventHash(event)
		if err != nil {
			t.Fatal("could not prepare exact replay cap chain coordinates")
		}
		previous = event.Hash
		receipt := domain.EventReceipt{TenantID: "tenant-a", EventID: event.EventID, Status: domain.StatusIndexed, StreamID: event.StreamID, Sequence: event.Sequence, Hash: event.Hash, AcceptedAt: pinnedNow, LedgeredAt: pinnedNow, IndexedAt: pinnedNow}
		if i == domain.MaxTimelineEvents {
			extra, extraReceipt = event, receipt
			continue
		}
		key := store.EventKey("tenant-a", event.EventID)
		events[key], receipts[key] = event, receipt
	}
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error { data.Events, data.Receipts = events, receipts; return nil }); err != nil {
		t.Fatal("could not save replay cap fixture")
	}
	result, err := svc.ReplayOperation(testCtx, "tenant-a", "reader", "replay-operation")
	if err != nil || result.EventCount != domain.MaxTimelineEvents || result.LastSequence != int64(domain.MaxTimelineEvents) || result.State["status"] != "paid" || len(readFacts(t, svc)) != 1 {
		t.Fatal("replay changed its exact whole-timeline boundary")
	}
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", extra.EventID)
		data.Events[key], data.Receipts[key] = extra, extraReceipt
		return nil
	}); err != nil {
		t.Fatal("could not add the one over-cap member")
	}
	result, err = svc.ReplayOperation(testCtx, "tenant-a", "reader", "replay-operation")
	if !errors.Is(err, domain.ErrInvalid) || !reflect.DeepEqual(result, domain.ReplayResult{}) || len(readFacts(t, svc)) != 1 {
		t.Fatal("over-cap replay truncated success, served partial state or appended a successful read fact")
	}
}

func TestOperationReplayFileRestartKeepsOriginalFoldAndEvidence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := newServiceOn(t, st)
	t.Cleanup(func() { _ = st.Close() })
	seedFileTestDomain(t, svc)
	svc.Config.ArchiveDir = filepath.Join(dir, "archive")
	svc.Config.Archive = &archive.FileStore{Dir: svc.Config.ArchiveDir}
	event := testEvent("replay-file-source", "replay-operation", pinnedNow)
	event.ChangedFields["nested"] = domain.FieldChange{After: map[string]any{"items": []any{json.Number("9007199254740993")}}}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); err != nil {
		t.Fatal("could not commit real file replay evidence")
	}
	before := leaseSnapshot(t, svc)
	first, err := svc.ReplayOperation(testCtx, "tenant-a", "reader", "replay-operation")
	if err != nil || first.EventCount != 1 || first.State["status"] != "paid" {
		t.Fatal("real file replay failed before restart")
	}
	first.State["nested"].(map[string]any)["items"].([]any)[0] = "caller-mutation"
	if err := st.Close(); err != nil {
		t.Fatal("could not close the original file writer")
	}
	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal("could not reopen the original file ledger")
	}
	defer reopened.Close()
	next := newServiceOn(t, reopened)
	next.Config = svc.Config
	next.Config.Archive = &archive.FileStore{Dir: svc.Config.ArchiveDir}
	result, err := next.ReplayOperation(testCtx, "tenant-a", "reader", "replay-operation")
	if err != nil || result.EventCount != 1 || result.LastSequence != 1 || result.State["status"] != "paid" || result.State["nested"].(map[string]any)["items"].([]any)[0] != json.Number("9007199254740993") {
		t.Fatal("file restart changed authenticated replay or retained caller aliases")
	}
	after := leaseSnapshot(t, next)
	if !reflect.DeepEqual(before.Events, after.Events) || !reflect.DeepEqual(before.Receipts, after.Receipts) || !reflect.DeepEqual(before.Streams, after.Streams) || !reflect.DeepEqual(before.LedgeredOutbox, after.LedgeredOutbox) || len(after.RestoreRuns) != 0 || len(after.Exports) != 0 || len(readFacts(t, next)) != 2 {
		t.Fatal("file replay changed committed evidence or created business/governance work")
	}
}

func TestOperationReplayRestoreCallersKeepSilentReadsAndDecisionOnlySemantics(t *testing.T) {
	svc, backend, objects := operationReplayFixture(t, true)
	before := leaseSnapshot(t, svc)
	saves := backend.saves
	request := domain.RestoreRequest{OperationID: "replay-operation", Reason: "review original evidence"}
	preview, err := svc.PreviewRestore(testCtx, "tenant-a", request)
	if err != nil || preview.ID == "" || !preview.RequiresApproval || len(preview.ExternalCalls) != 0 || preview.ProposedState["status"] != "latest" || backend.appendCalls != 0 || backend.saves != saves {
		t.Fatal("restore preview lost authenticated fold, silent read compatibility or side-effect-free semantics")
	}
	run, err := svc.CreateRestore(testCtx, "tenant-a", request, "requester")
	if err != nil || run.Status != domain.RestoreStatusPendingApproval || backend.appendCalls != 0 || backend.saves != saves+1 {
		t.Fatal("restore creation changed its separate original control commit or emitted a replay read fact")
	}
	gets := len(objects.deadlines)
	approved, err := svc.ApproveRestore(testCtx, "tenant-a", run.ID, "independent-reviewer")
	if err != nil || approved.Status != domain.RestoreStatusApproved || len(objects.deadlines) != gets || backend.appendCalls != 0 {
		t.Fatal("restore approval became evidence replay or business execution")
	}
	after := leaseSnapshot(t, svc)
	if !reflect.DeepEqual(before.Events, after.Events) || !reflect.DeepEqual(before.Receipts, after.Receipts) || !reflect.DeepEqual(before.LedgeredOutbox, after.LedgeredOutbox) || len(after.RestoreRuns) != 1 || len(after.Exports) != 0 {
		t.Fatal("restore decision changed original evidence or executed additional work")
	}
	created, decided := 0, 0
	for _, fact := range after.AdminActions {
		if fact.Action == domain.AdminActionRestoreCreated {
			created++
		}
		if fact.Action == domain.AdminActionRestoreApproved {
			decided++
		}
	}
	if created != 1 || decided != 1 {
		t.Fatal("restore callers lost their original distinct creation/approval facts")
	}
}

func TestOperationReplayRestoreCallersRejectUnauthenticatedSource(t *testing.T) {
	for _, archived := range []bool{false, true} {
		svc, backend, _ := operationReplayFixture(t, archived)
		mutate := func(event *domain.Event) { event.ChangedFields["status"] = domain.FieldChange{After: "forged"} }
		if archived {
			svc.Config.Archive = &timelineMutatedArchive{Store: svc.Config.Archive, mutate: mutate}
		} else if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
			key := store.EventKey("tenant-a", "replay-a")
			event := data.Events[key]
			mutate(&event)
			data.Events[key] = event
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		saves := backend.saves
		request := domain.RestoreRequest{OperationID: "replay-operation", Reason: "review original evidence"}
		preview, previewErr := svc.PreviewRestore(testCtx, "tenant-a", request)
		run, createErr := svc.CreateRestore(testCtx, "tenant-a", request, "requester")
		if previewErr == nil || createErr == nil || !reflect.DeepEqual(preview, domain.RestorePreview{}) || !reflect.DeepEqual(run, domain.RestoreRun{}) || backend.saves != saves || backend.appendCalls != 0 || len(leaseSnapshot(t, svc).RestoreRuns) != 0 {
			t.Fatal("unauthenticated replay source authorized restore state or created a pending run/fact")
		}
	}
}
