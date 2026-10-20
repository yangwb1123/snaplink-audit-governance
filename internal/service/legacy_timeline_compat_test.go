package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestLegacyTimelineLiveOrderFoldAndOwnedProtectedEvidence(t *testing.T) {
	for _, kind := range legacyTimelineKinds {
		for _, archived := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/archived-%t", kind, archived), func(t *testing.T) {
				svc, backend, _ := operationReplayFixture(t, archived)
				if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
					key := store.SchemaKey("tenant-a", "audit.event", 2)
					schema := data.Schemas[key]
					schema.Active = false
					data.Schemas[key] = schema
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				saves := backend.saves
				first, err := readLegacyTimeline(testCtx, svc, kind, "tenant-a", "reader")
				if err != nil || legacyTimelineCount(first, kind) != 2 || backend.saves != saves || backend.appendCalls != 1 {
					t.Fatal("legacy read changed its exact historical inactive Schema or read-only semantics")
				}
				if kind == "aggregate-replay" {
					if first.replay.State["status"] != "earliest" || first.replay.LastSequence != 2 || first.replay.AggregateID != "inv-1" || first.replay.OperationID != "" {
						t.Fatal("aggregate replay replaced aggregate-version/event-id order with chronological order")
					}
					if first.replay.State["nested"].(map[string]any)["items"].([]any)[0] != json.Number("9007199254740993") {
						t.Fatal("aggregate replay lost exact large-number content")
					}
					first.replay.State["nested"].(map[string]any)["items"].([]any)[0] = "caller-mutation"
				} else {
					want := []string{"replay-a", "replay-b"}
					if kind == "operation" {
						want = []string{"replay-b", "replay-a"}
					}
					if !reflect.DeepEqual(eventIDs(first.events), want) {
						t.Fatal("legacy whole timeline changed its original total order")
					}
					if _, protected := first.events[0].Payload["value"].(string); !protected {
						t.Fatal("legacy timeline replaced a protected response field with plaintext")
					}
					first.events[0].Actor.Roles[0] = "caller-mutation"
					first.events[0].Payload["resource"] = "caller-mutation"
					first.events[0].ChangedFields["nested"].After.(map[string]any)["items"].([]any)[0] = "caller-mutation"
				}
				again, err := readLegacyTimeline(testCtx, svc, kind, "tenant-a", "")
				if err != nil || legacyTimelineCount(again, kind) != 2 || backend.appendCalls != 1 || backend.saves != saves {
					t.Fatal("internal legacy read changed the empty-actor zero-fact compatibility policy")
				}
				if kind == "aggregate-replay" {
					if again.replay.State["nested"].(map[string]any)["items"].([]any)[0] != json.Number("9007199254740993") {
						t.Fatal("aggregate replay state aliases original source evidence")
					}
				} else if again.events[0].Actor.Roles[0] != "operator" || again.events[0].Payload["resource"] != "invoice" || again.events[0].ChangedFields["nested"].After.(map[string]any)["items"].([]any)[0] != json.Number("9007199254740993") {
					t.Fatal("whole timeline output aliases original source evidence")
				}
				late := testEvent("replay-c", "replay-operation", pinnedNow.Add(-time.Second))
				late.AggregateVersion = 2
				late.ChangedFields = map[string]domain.FieldChange{"status": {After: nil}}
				later := testEvent("replay-d", "replay-operation", pinnedNow.Add(3*time.Second))
				later.AggregateVersion = 0
				later.ChangedFields = map[string]domain.FieldChange{"early": {After: "retained"}}
				for _, event := range []domain.Event{late, later} {
					if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
						t.Fatal("could not commit late live timeline members")
					}
				}
				next, err := readLegacyTimeline(testCtx, svc, kind, "tenant-a", "reader")
				if err != nil || legacyTimelineCount(next, kind) != 4 || backend.appendCalls != 2 {
					t.Fatal("legacy whole reads became fixed membership or lost one fact per successful call")
				}
				if kind == "aggregate-replay" {
					_, status := next.replay.State["status"]
					if status || next.replay.State["early"] != "retained" || next.replay.LastSequence != 3 {
						t.Fatal("aggregate replay lost nil deletion or used maximum sequence instead of the last folded sequence")
					}
				} else {
					want := []string{"replay-d", "replay-a", "replay-b", "replay-c"}
					if kind == "operation" {
						want = []string{"replay-c", "replay-b", "replay-a", "replay-d"}
					}
					if !reflect.DeepEqual(eventIDs(next.events), want) {
						t.Fatal("late members changed the original operation/aggregate ordering rules")
					}
				}
			})
		}
	}
}

func TestLegacyTimelineCommittedMembershipAndTenantIsolation(t *testing.T) {
	for _, kind := range legacyTimelineKinds {
		for _, layout := range []int{0, 2} {
			t.Run(fmt.Sprintf("%s/layout-%d", kind, layout), func(t *testing.T) {
				svc, backend, _ := operationReplayFixture(t, false)
				if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
					data.LayoutVersion = layout
					event, err := domain.CloneEvent(data.Events[store.EventKey("tenant-a", "replay-a")])
					if err != nil {
						return err
					}
					event.EventID = "legacy-hot-only"
					data.Events[store.EventKey("tenant-a", event.EventID)] = event
					data.Receipts[store.EventKey("tenant-b", "foreign-corrupt")] = domain.EventReceipt{TenantID: "tenant-b", EventID: "foreign-corrupt", Status: domain.StatusArchived}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				result, err := readLegacyTimeline(testCtx, svc, kind, "tenant-a", "reader")
				if err != nil || legacyTimelineCount(result, kind) != 2 || backend.appendCalls != 1 {
					t.Fatal("legacy read included uncommitted hot residue or authenticated another tenant")
				}
				if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
					delete(data.Receipts, store.EventKey("tenant-a", "replay-a"))
					delete(data.Receipts, store.EventKey("tenant-a", "replay-b"))
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				result, err = readLegacyTimeline(testCtx, svc, kind, "tenant-a", "reader")
				if !errors.Is(err, domain.ErrNotFound) || !reflect.DeepEqual(result, legacyTimelineResult{}) || backend.appendCalls != 1 {
					t.Fatal("legacy read promoted hot-only residue into committed truth")
				}
			})
		}
	}
}

func TestLegacyTimelineRetainsMatchedCapWithoutTruncation(t *testing.T) {
	svc := queryMemoryService(t, false)
	// Synthetic exact content/chain/receipt fixtures observe a semantic
	// boundary, not ingest throughput, persistence or whole-history capacity.
	events, receipts := map[string]domain.Event{}, map[string]domain.EventReceipt{}
	previous := ""
	var extra domain.Event
	var extraReceipt domain.EventReceipt
	for i := 0; i <= domain.MaxTimelineEvents+1; i++ {
		event := testEvent(fmt.Sprintf("legacy-limit-%05d", i), "replay-operation", pinnedNow)
		event.TenantID, event.StreamID = "tenant-a", "tenant-a:aggregate:invoice:inv-1"
		event.Sequence, event.PrevHash = int64(i+1), previous
		if i == domain.MaxTimelineEvents+1 {
			event.OperationID, event.AggregateID = "unmatched-operation", "unmatched-aggregate"
			event.StreamID, event.Sequence, event.PrevHash = "tenant-a:aggregate:invoice:unmatched-aggregate", 1, ""
		}
		var err error
		event.SourceDigest, err = domain.EventContentDigest(event)
		if err != nil {
			t.Fatal("could not construct exact legacy cap content")
		}
		event.Hash, err = svc.eventHash(event)
		if err != nil {
			t.Fatal("could not construct exact legacy cap chain coordinates")
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
		t.Fatal("could not save exact legacy cap fixture")
	}
	for _, kind := range legacyTimelineKinds {
		result, err := readLegacyTimeline(testCtx, svc, kind, "tenant-a", "reader")
		if err != nil || legacyTimelineCount(result, kind) != domain.MaxTimelineEvents {
			t.Fatal("legacy whole-read cap counted unmatched history or rejected its exact boundary")
		}
	}
	if len(readFacts(t, svc)) != 3 {
		t.Fatal("legacy exact-cap calls did not leave exactly one original read fact each")
	}
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", extra.EventID)
		data.Events[key], data.Receipts[key] = extra, extraReceipt
		return nil
	}); err != nil {
		t.Fatal("could not add one over-cap matching member")
	}
	for _, kind := range legacyTimelineKinds {
		result, err := readLegacyTimeline(testCtx, svc, kind, "tenant-a", "reader")
		if !errors.Is(err, domain.ErrInvalid) || !reflect.DeepEqual(result, legacyTimelineResult{}) || len(readFacts(t, svc)) != 3 {
			t.Fatal("over-cap legacy read truncated a success, served partial output or appended a successful read fact")
		}
	}
}

func TestLegacyTimelineFileRestartPreservesEvidenceAndRejectsPersistedPredicateFault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := newServiceOn(t, st)
	seedFileTestDomain(t, svc)
	objects := &archive.FileStore{Dir: filepath.Join(dir, "archive")}
	svc.Config.Archive = objects
	event := testEvent("legacy-file-source", "replay-operation", pinnedNow)
	event.ChangedFields["nested"] = domain.FieldChange{After: map[string]any{"items": []any{json.Number("9007199254740993")}}}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); err != nil {
		t.Fatal("could not commit original real file legacy evidence")
	}
	original, err := st.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range legacyTimelineKinds {
		result, err := readLegacyTimeline(testCtx, svc, kind, "tenant-a", "reader")
		if err != nil || legacyTimelineCount(result, kind) != 1 {
			t.Fatal("real file legacy read failed before restart")
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	next := newServiceOn(t, reopened)
	next.Config = svc.Config
	for _, kind := range legacyTimelineKinds {
		result, err := readLegacyTimeline(testCtx, next, kind, "tenant-a", "reader")
		if err != nil || legacyTimelineCount(result, kind) != 1 {
			t.Fatal("file restart changed the original authenticated legacy read")
		}
		if kind == "aggregate-replay" && (result.replay.LastSequence != 1 || result.replay.State["nested"].(map[string]any)["items"].([]any)[0] != json.Number("9007199254740993")) {
			t.Fatal("real file aggregate replay lost exact content or sequence")
		}
	}
	key := store.EventKey("tenant-a", event.EventID)
	source, err := next.archivedEventContext(testCtx, original.Receipts[key])
	if err != nil {
		t.Fatal("could not capture original file evidence")
	}
	source.AggregateID, source.OperationID = "hidden-aggregate", "hidden-operation"
	if err := reopened.UpdateTenant(testCtx, "tenant-a", store.HotFirst, func(view *store.TenantView) error { view.Hot.Events[key] = source; return nil }); err != nil || reopened.Close() != nil {
		t.Fatal("could not persist owned predicate fault and release writer")
	}
	reopenedAgain, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedAgain.Close()
	last := newServiceOn(t, reopenedAgain)
	last.Config = svc.Config
	persisted, err := reopenedAgain.Snapshot(testCtx)
	if err != nil || persisted.Events[key].AggregateID != "hidden-aggregate" || persisted.Events[key].OperationID != "hidden-operation" {
		t.Fatal("file restart oracle lost the persisted predicate fault")
	}
	for _, kind := range legacyTimelineKinds {
		result, err := readLegacyTimeline(testCtx, last, kind, "tenant-a", "reader")
		if err == nil || errors.Is(err, domain.ErrNotFound) || !reflect.DeepEqual(result, legacyTimelineResult{}) {
			t.Fatal("file restart hid corrupted committed evidence or returned partial legacy output")
		}
	}
	current, err := reopenedAgain.Snapshot(testCtx)
	if err != nil || !reflect.DeepEqual(original.Receipts, current.Receipts) || !reflect.DeepEqual(original.Schemas, current.Schemas) || !reflect.DeepEqual(original.Streams, current.Streams) || len(current.Exports) != 0 || len(current.RestoreRuns) != 0 || len(readFacts(t, last)) != 6 {
		t.Fatal("legacy reads changed original file truth, created work or lost the exact read facts")
	}
}
