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

// Pure authentication/budget matrices use exact in-memory object bytes and
// real Store Load/Save/append hooks. The lock regression below keeps actual
// FileStore objects; independent PostgreSQL pools cover durable control CAS.
func summaryEvidenceFixture(t *testing.T, archived bool) (*Service, *timelineBudgetBackend, *timelineBudgetArchive) {
	t.Helper()
	backend := &timelineBudgetBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	t.Cleanup(func() { _ = svc.Store.Close() })
	seedTestDomain(t, svc)
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, EncryptedFields: []string{"value"}, SearchableFields: []string{"value"}}); err != nil {
		t.Fatal("could not prepare exact protected summary Schema")
	}
	objects := &timelineBudgetArchive{Store: newConsoleMemoryArchive()}
	if archived {
		svc.Config.Archive = objects
	}
	for i, id := range []string{"summary-a", "summary-b"} {
		event := testEvent(id, "summary-operation", pinnedNow.Add(time.Duration(1-i)*time.Second))
		event.SchemaVersion = 2
		event.Payload["value"] = json.Number("9007199254740993")
		if i == 1 {
			event.Outcome = "failure"
		}
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal("could not commit summary source evidence")
		}
	}
	backend.readDeadline, backend.auditDeadline = time.Time{}, time.Time{}
	return svc, backend, objects
}

func TestOperationSummaryArchiveGetReleasesControlLock(t *testing.T) {
	// The control/trail use the production in-memory backend so lock exclusion
	// is not confused with filesystem write throughput. Source objects are real.
	svc := queryMemoryService(t, true)
	var clock atomic.Int64
	clock.Store(pinnedNow.UnixNano())
	svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("summary-source", "summary-operation", pinnedNow), domain.StatusArchived); err != nil {
		t.Fatal("could not commit the real file archive source")
	}
	seedLeaseExport(t, svc, "summary-concurrent-job", "tenant-a")
	claim, won, err := svc.claimPendingExport(testCtx, "summary-concurrent-job")
	if err != nil || !won {
		t.Fatal("could not prepare the original fenced running job")
	}
	paused := &queryPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
	svc.Config.Archive = paused
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	release := sync.OnceFunc(func() { close(paused.release) })
	done, exited := make(chan error, 1), make(chan struct{})
	var summary domain.OperationSummary
	defer func() { release(); cancel(); <-exited }()
	go func() {
		defer close(exited)
		var err error
		summary, err = svc.Operation(ctx, "tenant-a", "reader", "summary-operation")
		done <- err
	}()
	select {
	case <-paused.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("summary never reached the actual source Get")
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
			data.LegalHolds["summary-hold"] = domain.LegalHold{ID: "summary-hold", TenantID: "tenant-a", Name: "Concurrent Hold", CreatedAt: svc.Now()}
			return nil
		})
	}()
	select {
	case err := <-control:
		if err != nil {
			t.Fatal("control mutation failed while the real source Get was paused")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("summary source Get retained Store.mu across renewal/cancellation")
	}
	release()
	if err := <-done; err != nil || summary.EventCount != 1 || summary.OperationID != "summary-operation" || summary.TenantID != "tenant-a" {
		t.Fatal("summary did not authenticate and resume after source Get")
	}
	current := leaseSnapshot(t, svc)
	if current.Exports[claim.ID].Status != domain.ExportStatusCancelled || len(current.ExportLeases) != 0 || current.Policies["tenant-a"].ArchiveDays != 365 || current.LegalHolds["summary-hold"].ID == "" {
		t.Fatal("summary read overwrote concurrent terminal, lease, Hold or policy control")
	}
	facts := readFacts(t, svc)
	if len(facts) != 1 || facts[0].Actor != "reader" || facts[0].TargetType != "operation" || facts[0].TargetID != "summary-operation" || facts[0].Detail != "summary" {
		t.Fatal("summary did not leave exactly one original caller read fact")
	}
}

func TestOperationSummaryAuthenticatesBeforeSelectionAndAggregation(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"operation", func(e *domain.Event) { e.OperationID = "hidden-operation" }},
		{"outcome", func(e *domain.Event) { e.Outcome = "hidden-outcome" }},
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
				svc, backend, _ := summaryEvidenceFixture(t, archived)
				if archived {
					svc.Config.Archive = &timelineMutatedArchive{Store: svc.Config.Archive, mutate: mutation.mutate}
				} else if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
					key := store.EventKey("tenant-a", "summary-a")
					event := data.Events[key]
					mutation.mutate(&event)
					data.Events[key] = event
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				summary, err := svc.Operation(testCtx, "tenant-a", "reader", "summary-operation")
				if err == nil || errors.Is(err, domain.ErrNotFound) || !reflect.DeepEqual(summary, domain.OperationSummary{}) || backend.appendCalls != 0 || len(backend.trail) != 0 {
					t.Fatal("bad committed evidence became a successful/partial summary or was hidden as not found")
				}
			})
		}
	}
}

func TestOperationSummaryReceiptAndExactSchemaBindings(t *testing.T) {
	key, schemaKey := store.EventKey("tenant-a", "summary-a"), store.SchemaKey("tenant-a", "audit.event", 2)
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
				svc, backend, _ := summaryEvidenceFixture(t, archived)
				if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error { mutation.mutate(data); return nil }); err != nil {
					t.Fatal(err)
				}
				summary, err := svc.Operation(testCtx, "tenant-a", "reader", "summary-operation")
				if err == nil || !reflect.DeepEqual(summary, domain.OperationSummary{}) || backend.appendCalls != 0 {
					t.Fatal("misbound receipt or exact Schema produced a summary/read fact")
				}
			})
		}
	}
}

func TestOperationSummaryLiveCountsTimesAndOwnedOutcomes(t *testing.T) {
	svc, backend, _ := summaryEvidenceFixture(t, true)
	// Historical inactive exact definitions remain readable; do not substitute
	// a newer Schema or require the ingestion-active flag on historical reads.
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		key := store.SchemaKey("tenant-a", "audit.event", 2)
		schema := data.Schemas[key]
		schema.Active = false
		data.Schemas[key] = schema
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	first, err := svc.Operation(testCtx, "tenant-a", "reader", "summary-operation")
	if err != nil || first.EventCount != 2 || !first.FirstAt.Equal(pinnedNow) || !first.LastAt.Equal(pinnedNow.Add(time.Second)) || !reflect.DeepEqual(first.Outcomes, []string{"failure", "success"}) {
		t.Fatal("summary changed min/max time, exact protected authentication or sorted unique outcomes")
	}
	late := testEvent("summary-late", "summary-operation", pinnedNow.Add(-time.Second))
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, late, domain.StatusArchived); err != nil {
		t.Fatal("could not commit a late summary member")
	}
	first.Outcomes[0] = "caller-mutation"
	next, err := svc.Operation(testCtx, "tenant-a", "", "summary-operation")
	if err != nil || next.EventCount != 3 || !next.FirstAt.Equal(late.OccurredAt) || !next.LastAt.Equal(pinnedNow.Add(time.Second)) || !reflect.DeepEqual(next.Outcomes, []string{"failure", "success"}) || backend.appendCalls != 1 || len(backend.trail) != 1 {
		t.Fatal("summary lost live late membership, owned outcomes or silent internal-read convention")
	}
}
