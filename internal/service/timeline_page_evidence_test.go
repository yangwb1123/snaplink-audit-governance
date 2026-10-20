package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func readTimelinePage(ctx context.Context, svc *Service, kind, tenant, actor string, size int, cursor string) (domain.QueryResult, error) {
	if kind == "operation" {
		return svc.OperationTimelinePage(ctx, tenant, actor, "timeline-operation", size, cursor)
	}
	return svc.AggregateTimelinePage(ctx, tenant, actor, "invoice", "inv-1", size, cursor)
}

func TestTimelinePageArchiveIOReleasesControlLock(t *testing.T) {
	for _, kind := range []string{"operation", "aggregate"} {
		t.Run(kind, func(t *testing.T) {
			// Keep real FileStore source objects, but do not conflate the
			// mutex oracle with unrelated control/trail fsync throughput.
			svc := queryMemoryService(t, true)
			t.Cleanup(func() { _ = svc.Store.Close() })
			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("timeline-source", "timeline-operation", pinnedNow), domain.StatusArchived); err != nil {
				t.Fatal(err)
			}
			paused := &queryPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
			svc.Config.Archive = paused
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			type outcome struct {
				page domain.QueryResult
				err  error
			}
			done := make(chan outcome, 1)
			go func() {
				page, err := readTimelinePage(ctx, svc, kind, "tenant-a", "reader", 1, "")
				done <- outcome{page: page, err: err}
			}()
			release := sync.OnceFunc(func() { close(paused.release) })
			joined := false
			defer func() {
				release()
				cancel()
				if !joined {
					<-done
				}
			}()
			select {
			case <-paused.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("timeline never reached the archive Get boundary")
			}
			updated := make(chan error, 1)
			go func() {
				updated <- svc.Store.UpdateControl(ctx, func(data *store.Snapshot) error {
					data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
					return nil
				})
			}()
			// Start this guard only AFTER Get is in flight. The writer must
			// finish before that socket is released, not after its timeout.
			select {
			case err := <-updated:
				if err != nil {
					t.Fatal("concurrent control mutation failed at paused Get")
				}
			case <-time.After(2 * time.Second):
				t.Error("timeline archive Get retained Store.mu")
				cancel()
				<-updated
				return
			}
			release()
			got := <-done
			joined = true
			if got.err != nil || got.page.Count != 1 || len(got.page.Items) != 1 || got.page.Items[0].EventID != "timeline-source" {
				t.Fatal("timeline did not authenticate and resume after Get release")
			}
			if err := svc.Store.ReadControl(testCtx, func(data *store.Snapshot) error {
				if data.Policies["tenant-a"].ArchiveDays != 365 {
					t.Fatal("read overwrote concurrent control metadata")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if facts := readFacts(t, svc); len(facts) != 1 || facts[0].Actor != "reader" {
				t.Fatal("successful timeline page must append exactly one caller read fact")
			}
		})
	}
}

func timelineEvidenceFixture(t *testing.T, archived bool) (*Service, *checkpointBackend) {
	t.Helper()
	backend := &checkpointBackend{data: store.NewSnapshot()}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	t.Cleanup(func() { _ = svc.Store.Close() })
	seedTestDomain(t, svc)
	if archived {
		// This pure authentication matrix needs exact Put/Get bytes, not
		// unrelated filesystem throughput. Real FileStore locking stays above.
		svc.Config.Archive = newConsoleMemoryArchive()
	}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("timeline-source", "timeline-operation", pinnedNow), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	return svc, backend
}

type timelineMutatedArchive struct {
	archive.Store
	mutate func(*domain.Event)
}

func (a *timelineMutatedArchive) Get(ctx context.Context, key string) ([]byte, error) {
	raw, err := a.Store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	var event domain.Event
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&event); err != nil {
		return nil, err
	}
	a.mutate(&event)
	return domain.CanonicalJSON(event)
}

func TestTimelinePageAuthenticatesBeforeSelection(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"operation", func(e *domain.Event) { e.OperationID = "hidden-operation" }},
		{"aggregate-type", func(e *domain.Event) { e.AggregateType = "hidden-type" }},
		{"aggregate-id", func(e *domain.Event) { e.AggregateID = "hidden-id" }},
		{"aggregate-version", func(e *domain.Event) { e.AggregateVersion++ }},
		{"payload", func(e *domain.Event) { e.Payload["value"] = 99 }},
		{"actor", func(e *domain.Event) { e.Actor.ID = "changed-actor" }},
		{"hash", func(e *domain.Event) { e.Hash = "invalid" }},
		{"source-digest", func(e *domain.Event) { e.SourceDigest = "" }},
		{"schema-version", func(e *domain.Event) { e.SchemaVersion++ }},
	}
	for _, kind := range []string{"operation", "aggregate"} {
		for _, archived := range []bool{false, true} {
			mode := "hot"
			if archived {
				mode = "archived"
			}
			for _, mutation := range mutations {
				t.Run(kind+"/"+mode+"/"+mutation.name, func(t *testing.T) {
					svc, _ := timelineEvidenceFixture(t, archived)
					if archived {
						svc.Config.Archive = &timelineMutatedArchive{Store: svc.Config.Archive, mutate: mutation.mutate}
					} else if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
						key := store.EventKey("tenant-a", "timeline-source")
						event := data.Events[key]
						mutation.mutate(&event)
						data.Events[key] = event
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					page, err := readTimelinePage(testCtx, svc, kind, "tenant-a", "reader", 1, "")
					if err == nil || errors.Is(err, domain.ErrNotFound) || !reflect.DeepEqual(page, domain.QueryResult{}) {
						t.Fatal("corrupted committed evidence was served or hidden by the timeline predicate")
					}
					if len(readFacts(t, svc)) != 0 {
						t.Fatal("rejected evidence appended a successful read fact")
					}
				})
			}
		}
	}
}

func TestTimelinePageReceiptAndExactSchemaBindings(t *testing.T) {
	key := store.EventKey("tenant-a", "timeline-source")
	schemaKey := store.SchemaKey("tenant-a", "audit.event", 1)
	mutations := []struct {
		name   string
		mutate func(*store.Snapshot)
	}{
		{"receipt-key", func(d *store.Snapshot) {
			d.Receipts[store.EventKey("tenant-a", "wrong-event")] = d.Receipts[key]
			delete(d.Receipts, key)
		}},
		{"receipt-tenant", func(d *store.Snapshot) { r := d.Receipts[key]; r.TenantID = "tenant-b"; d.Receipts[key] = r }},
		{"receipt-event", func(d *store.Snapshot) { r := d.Receipts[key]; r.EventID = "wrong-event"; d.Receipts[key] = r }},
		{"missing-schema", func(d *store.Snapshot) {
			s := d.Schemas[schemaKey]
			s.Version = 2
			d.Schemas[store.SchemaKey("tenant-a", s.SchemaID, 2)] = s
			delete(d.Schemas, schemaKey)
		}},
		{"schema-tenant", func(d *store.Snapshot) { s := d.Schemas[schemaKey]; s.TenantID = "tenant-b"; d.Schemas[schemaKey] = s }},
		{"schema-id", func(d *store.Snapshot) {
			s := d.Schemas[schemaKey]
			s.SchemaID = "wrong-schema"
			d.Schemas[schemaKey] = s
		}},
		{"schema-version", func(d *store.Snapshot) { s := d.Schemas[schemaKey]; s.Version = 2; d.Schemas[schemaKey] = s }},
	}
	for _, kind := range []string{"operation", "aggregate"} {
		for _, archived := range []bool{false, true} {
			mode := "hot"
			if archived {
				mode = "archived"
			}
			for _, mutation := range mutations {
				t.Run(kind+"/"+mode+"/"+mutation.name, func(t *testing.T) {
					svc, _ := timelineEvidenceFixture(t, archived)
					if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error { mutation.mutate(data); return nil }); err != nil {
						t.Fatal(err)
					}
					page, err := readTimelinePage(testCtx, svc, kind, "tenant-a", "reader", 1, "")
					if err == nil || !reflect.DeepEqual(page, domain.QueryResult{}) || len(readFacts(t, svc)) != 0 {
						t.Fatal("misbound receipt/schema served a page or successful read fact")
					}
				})
			}
		}
	}
}

func TestTimelinePageCommittedMembershipAndTenantIsolation(t *testing.T) {
	for _, kind := range []string{"operation", "aggregate"} {
		for _, layout := range []int{0, 2} {
			svc, _ := timelineEvidenceFixture(t, false)
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				data.LayoutVersion = layout
				key := store.EventKey("tenant-a", "timeline-source")
				orphan, err := domain.CloneEvent(data.Events[key])
				if err != nil {
					return err
				}
				orphan.EventID = "hot-only-not-committed"
				data.Events[store.EventKey("tenant-a", orphan.EventID)] = orphan
				foreign, err := domain.CloneEvent(data.Events[key])
				if err != nil {
					return err
				}
				foreign.TenantID = "tenant-b"
				foreign.Hash = "foreign-invalid-evidence"
				data.Events[store.EventKey("tenant-b", foreign.EventID)] = foreign
				data.Receipts[store.EventKey("tenant-b", foreign.EventID)] = domain.EventReceipt{TenantID: "tenant-b", EventID: foreign.EventID}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			page, err := readTimelinePage(testCtx, svc, kind, "tenant-a", "reader", 100, "")
			if err != nil || page.Count != 1 || len(page.Items) != 1 || page.Items[0].TenantID != "tenant-a" || page.Items[0].EventID != "timeline-source" {
				t.Fatal("timeline included uncommitted/foreign evidence or authenticated another tenant")
			}
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				delete(data.Receipts, store.EventKey("tenant-a", "timeline-source"))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			page, err = readTimelinePage(testCtx, svc, kind, "tenant-a", "reader", 100, "")
			if !errors.Is(err, domain.ErrNotFound) || !reflect.DeepEqual(page, domain.QueryResult{}) || len(readFacts(t, svc)) != 1 {
				t.Fatal("hot-only residue became committed timeline truth")
			}
		}
	}
}

func TestTimelinePagePreservesLiveCountOrderAndOwnedProtectedEvidence(t *testing.T) {
	for _, kind := range []string{"operation", "aggregate"} {
		t.Run(kind, func(t *testing.T) {
			// Live ordering/ownership is independent of filesystem latency.
			// Ingest, exact bytes, encryption and authentication still run.
			svc := queryMemoryService(t, false)
			svc.Config.Archive = newConsoleMemoryArchive()
			t.Cleanup(func() { _ = svc.Store.Close() })
			if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, EncryptedFields: []string{"value"}, SearchableFields: []string{"value"}}); err != nil {
				t.Fatal(err)
			}
			for i, id := range []string{"timeline-a", "timeline-b", "timeline-c"} {
				event := testEvent(id, "timeline-operation", pinnedNow.Add(time.Duration(3-i)*time.Second))
				event.SchemaVersion = 2
				event.AggregateVersion = int64(i + 1)
				event.Payload["value"] = json.Number("9007199254740993")
				if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); err != nil {
					t.Fatal(err)
				}
			}
			want := []string{"timeline-c", "timeline-b", "timeline-a"}
			if kind == "aggregate" {
				want = []string{"timeline-a", "timeline-b", "timeline-c"}
			}
			first, err := readTimelinePage(testCtx, svc, kind, "tenant-a", "reader", 1, "")
			if err != nil || first.Count != 3 || len(first.Items) != 1 || first.Items[0].EventID != want[0] || first.NextCursor == "" || first.Snapshot != nil {
				t.Fatal("first live timeline page/order/count changed")
			}
			if _, protected := first.Items[0].Payload["value"].(string); !protected {
				t.Fatal("timeline exposed a decrypted protected field")
			}
			// A new request still observes live history, not fixed membership.
			late := testEvent("timeline-late", "timeline-operation", pinnedNow.Add(4*time.Second))
			late.AggregateVersion = 4
			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, late, domain.StatusArchived); err != nil {
				t.Fatal(err)
			}
			next, err := readTimelinePage(testCtx, svc, kind, "tenant-a", "reader", 1, first.NextCursor)
			if err != nil || next.Count != 4 || len(next.Items) != 1 || next.Items[0].EventID != want[1] || next.NextCursor == "" || next.Snapshot != nil {
				t.Fatal("live continuation did not preserve total count and ordering")
			}
			first.Items[0].Payload["resource"] = "caller-mutation"
			first.Items[0].Actor.Roles[0] = "caller-mutation"
			again, err := readTimelinePage(testCtx, svc, kind, "tenant-a", "", 1, "")
			if err != nil || again.Items[0].Payload["resource"] != "invoice" || again.Items[0].Actor.Roles[0] != "operator" {
				t.Fatal("timeline result aliases evidence")
			}
			facts := readFacts(t, svc)
			if len(facts) != 2 {
				t.Fatal("timeline pages did not append exactly one fact each; internal read must stay silent")
			}
			for _, fact := range facts {
				if fact.Actor != "reader" || fact.TenantID != "tenant-a" || fact.TargetType != kind || (kind == "operation" && fact.Detail != "timeline") || (kind == "aggregate" && fact.Detail != "invoice") {
					t.Fatal("timeline read fact identity/target encoding changed")
				}
			}
		})
	}
}
