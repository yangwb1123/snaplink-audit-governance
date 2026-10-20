package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Authentication/operation-budget matrices use exact bytes and real Store
// Load/Save/append boundaries. The lock test retains real FileStore objects;
// file restart and independent SQL pools are separate durability observations.
func operationReplayFixture(t *testing.T, archived bool) (*Service, *timelineBudgetBackend, *timelineBudgetArchive) {
	t.Helper()
	backend := &timelineBudgetBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	t.Cleanup(func() { _ = svc.Store.Close() })
	seedTestDomain(t, svc)
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, EncryptedFields: []string{"value"}, SearchableFields: []string{"value"}}); err != nil {
		t.Fatal("could not prepare exact protected replay Schema")
	}
	objects := &timelineBudgetArchive{Store: newConsoleMemoryArchive()}
	if archived {
		svc.Config.Archive = objects
	}
	for i, id := range []string{"replay-a", "replay-b"} {
		event := testEvent(id, "replay-operation", pinnedNow.Add(time.Duration(1-i)*time.Second))
		event.SchemaVersion = 2
		event.Payload["value"] = json.Number("9007199254740993")
		status := "latest"
		if i == 1 {
			status = "earliest"
		}
		event.ChangedFields = map[string]domain.FieldChange{
			"status": {After: status},
			"nested": {After: map[string]any{"items": []any{json.Number("9007199254740993")}}},
		}
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal("could not commit original replay evidence")
		}
	}
	backend.readDeadline, backend.auditDeadline = time.Time{}, time.Time{}
	return svc, backend, objects
}

func TestOperationReplayArchiveGetReleasesControlLock(t *testing.T) {
	// Control/trail throughput is not the mutex oracle. These source objects
	// still pass through the real FileStore Put/fsync/Get implementation.
	svc := queryMemoryService(t, true)
	var clock atomic.Int64
	clock.Store(pinnedNow.UnixNano())
	svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("replay-source", "replay-operation", pinnedNow), domain.StatusArchived); err != nil {
		t.Fatal("could not commit the real replay file source")
	}
	seedLeaseExport(t, svc, "replay-concurrent-job", "tenant-a")
	claim, won, err := svc.claimPendingExport(testCtx, "replay-concurrent-job")
	if err != nil || !won {
		t.Fatal("could not prepare the fenced running job")
	}
	paused := &queryPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
	svc.Config.Archive = paused
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	release := sync.OnceFunc(func() { close(paused.release) })
	done, exited := make(chan error, 1), make(chan struct{})
	var result domain.ReplayResult
	defer func() { release(); cancel(); <-exited }()
	go func() {
		defer close(exited)
		var err error
		result, err = svc.ReplayOperation(ctx, "tenant-a", "reader", "replay-operation")
		done <- err
	}()
	select {
	case <-paused.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("replay never reached the actual file Get")
	}
	control, controlExited := make(chan error, 1), make(chan struct{})
	defer func() { cancel(); <-controlExited }()
	go func() {
		defer close(controlExited)
		clock.Store(pinnedNow.Add(ExportHeartbeatInterval).UnixNano())
		if err := svc.renewExportLease(ctx, claim); err != nil {
			control <- err
			return
		}
		if _, err := svc.CancelExport(ctx, "tenant-a", "operator", claim.ID); err != nil {
			control <- err
			return
		}
		control <- svc.Store.UpdateControl(ctx, func(data *store.Snapshot) error {
			data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
			data.LegalHolds["replay-hold"] = domain.LegalHold{ID: "replay-hold", TenantID: "tenant-a", Name: "Concurrent Hold", CreatedAt: svc.Now()}
			return nil
		})
	}()
	select {
	case err := <-control:
		if err != nil {
			t.Fatal("control mutation failed during the paused replay Get")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replay retained Store.mu across source Get and blocked renewal/cancellation")
	}
	release()
	if err := <-done; err != nil || result.EventCount != 1 || result.OperationID != "replay-operation" || result.TenantID != "tenant-a" || result.State["status"] != "paid" {
		t.Fatal("replay did not authenticate and resume its original fold")
	}
	current := leaseSnapshot(t, svc)
	if current.Exports[claim.ID].Status != domain.ExportStatusCancelled || len(current.ExportLeases) != 0 || current.Policies["tenant-a"].ArchiveDays != 365 || current.LegalHolds["replay-hold"].ID == "" {
		t.Fatal("replay overwrote concurrent terminal, lease, Hold or policy control")
	}
	facts := readFacts(t, svc)
	if len(facts) != 1 || facts[0].Actor != "reader" || facts[0].TargetType != "operation" || facts[0].TargetID != "replay-operation" || facts[0].Detail != "replay" {
		t.Fatal("replay did not leave exactly one original read fact")
	}
}

func TestOperationReplayAuthenticatesBeforeSelectionAndFold(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"operation", func(e *domain.Event) { e.OperationID = "hidden-operation" }},
		{"changed-after", func(e *domain.Event) { e.ChangedFields["status"] = domain.FieldChange{After: "forged"} }},
		{"changed-delete", func(e *domain.Event) { e.ChangedFields["nested"] = domain.FieldChange{After: nil} }},
		{"occurrence", func(e *domain.Event) { e.OccurredAt = e.OccurredAt.Add(time.Hour) }},
		{"actor", func(e *domain.Event) { e.Actor.ID = "changed-actor" }},
		{"protected-content", func(e *domain.Event) { e.Payload["value"] = "invalid-protected-value" }},
		{"schema-version", func(e *domain.Event) { e.SchemaVersion++ }},
		{"source-digest", func(e *domain.Event) { e.SourceDigest = "" }},
	}
	for _, archived := range []bool{false, true} {
		mode := "hot"
		if archived {
			mode = "archived"
		}
		for _, mutation := range mutations {
			t.Run(mode+"/"+mutation.name, func(t *testing.T) {
				svc, backend, _ := operationReplayFixture(t, archived)
				if archived {
					svc.Config.Archive = &timelineMutatedArchive{Store: svc.Config.Archive, mutate: mutation.mutate}
				} else if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
					key := store.EventKey("tenant-a", "replay-a")
					event := data.Events[key]
					mutation.mutate(&event)
					data.Events[key] = event
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				result, err := svc.ReplayOperation(testCtx, "tenant-a", "reader", "replay-operation")
				if err == nil || errors.Is(err, domain.ErrNotFound) || !reflect.DeepEqual(result, domain.ReplayResult{}) || backend.appendCalls != 0 || len(backend.trail) != 0 {
					t.Fatal("bad committed evidence became a successful/partial replay or was hidden as not found")
				}
			})
		}
	}
}

func TestOperationReplayReceiptAndExactSchemaBindings(t *testing.T) {
	key, schemaKey := store.EventKey("tenant-a", "replay-a"), store.SchemaKey("tenant-a", "audit.event", 2)
	for _, mutation := range []struct {
		name   string
		mutate func(*store.Snapshot)
	}{
		{"receipt-key", func(d *store.Snapshot) {
			d.Receipts[store.EventKey("tenant-a", "wrong-event")] = d.Receipts[key]
			delete(d.Receipts, key)
		}},
		{"receipt-tenant", func(d *store.Snapshot) { r := d.Receipts[key]; r.TenantID = "tenant-b"; d.Receipts[key] = r }},
		{"receipt-event", func(d *store.Snapshot) { r := d.Receipts[key]; r.EventID = "wrong-event"; d.Receipts[key] = r }},
		{"receipt-hash", func(d *store.Snapshot) { r := d.Receipts[key]; r.Hash = "wrong-hash"; d.Receipts[key] = r }},
		{"schema-tenant", func(d *store.Snapshot) { s := d.Schemas[schemaKey]; s.TenantID = "tenant-b"; d.Schemas[schemaKey] = s }},
		{"schema-id", func(d *store.Snapshot) {
			s := d.Schemas[schemaKey]
			s.SchemaID = "wrong-schema"
			d.Schemas[schemaKey] = s
		}},
		{"schema-version", func(d *store.Snapshot) { s := d.Schemas[schemaKey]; s.Version = 3; d.Schemas[schemaKey] = s }},
		{"missing-exact-schema", func(d *store.Snapshot) {
			s := d.Schemas[schemaKey]
			s.Version = 3
			d.Schemas[store.SchemaKey("tenant-a", "audit.event", 3)] = s
			delete(d.Schemas, schemaKey)
		}},
	} {
		for _, archived := range []bool{false, true} {
			mode := "hot"
			if archived {
				mode = "archived"
			}
			t.Run(mode+"/"+mutation.name, func(t *testing.T) {
				svc, backend, _ := operationReplayFixture(t, archived)
				if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error { mutation.mutate(data); return nil }); err != nil {
					t.Fatal(err)
				}
				result, err := svc.ReplayOperation(testCtx, "tenant-a", "reader", "replay-operation")
				if err == nil || !reflect.DeepEqual(result, domain.ReplayResult{}) || backend.appendCalls != 0 {
					t.Fatal("misbound receipt or exact Schema produced replay state/read fact")
				}
			})
		}
	}
}

func TestOperationReplayPreservesLiveFoldAndOwnedState(t *testing.T) {
	for _, archived := range []bool{false, true} {
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
		first, err := svc.ReplayOperation(testCtx, "tenant-a", "reader", "replay-operation")
		if err != nil || first.EventCount != 2 || first.LastSequence != 1 || first.State["status"] != "latest" || len(first.State) != 2 || first.AggregateID != "" || backend.saves != saves {
			t.Fatal("replay lost chronological folding, last-event sequence or inactive exact Schema compatibility")
		}
		nested := first.State["nested"].(map[string]any)
		if nested["items"].([]any)[0] != json.Number("9007199254740993") {
			t.Fatal("replay fold lost exact large-number content")
		}
		nested["items"].([]any)[0] = "caller-mutation"
		again, err := svc.ReplayOperation(testCtx, "tenant-a", "", "replay-operation")
		if err != nil || again.State["nested"].(map[string]any)["items"].([]any)[0] != json.Number("9007199254740993") || backend.appendCalls != 1 {
			t.Fatal("replay state aliases source evidence or internal empty actor appended a read fact")
		}
		late := testEvent("replay-late", "replay-operation", pinnedNow.Add(2*time.Second))
		late.ChangedFields = map[string]domain.FieldChange{"status": {After: nil}}
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, late, domain.StatusLedgered); err != nil {
			t.Fatal("could not commit the late replay member")
		}
		next, err := svc.ReplayOperation(testCtx, "tenant-a", "", "replay-operation")
		_, retained := next.State["status"]
		if err != nil || next.EventCount != 3 || next.LastSequence != 3 || retained || len(next.State) != 1 || backend.appendCalls != 1 {
			t.Fatal("replay changed live membership or nil-after deletion semantics")
		}
	}
}

func TestOperationReplayAuthenticatesUnmatchedCommittedEvidence(t *testing.T) {
	for _, archived := range []bool{false, true} {
		svc, backend, _ := operationReplayFixture(t, archived)
		other := testEvent("replay-z", "unselected-operation", pinnedNow)
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, other, domain.StatusLedgered); err != nil {
			t.Fatal("could not commit valid unmatched evidence")
		}
		mutate := func(event *domain.Event) {
			if event.EventID == other.EventID {
				event.ChangedFields["status"] = domain.FieldChange{After: "forged"}
			}
		}
		if archived {
			svc.Config.Archive = &timelineMutatedArchive{Store: svc.Config.Archive, mutate: mutate}
		} else if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
			key := store.EventKey("tenant-a", other.EventID)
			event := data.Events[key]
			mutate(&event)
			data.Events[key] = event
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		result, err := svc.ReplayOperation(testCtx, "tenant-a", "reader", "replay-operation")
		if err == nil || !reflect.DeepEqual(result, domain.ReplayResult{}) || backend.appendCalls != 0 {
			t.Fatal("replay filtered unauthenticated committed evidence before authentication")
		}
	}
}
