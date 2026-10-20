package service

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type consoleReadResult struct {
	page   domain.QueryResult
	facets domain.EventFacets
}

func consoleEvidenceQuery() domain.Query {
	return domain.Query{From: pinnedNow.Add(-time.Minute), To: pinnedNow.Add(time.Minute), PageSize: 1}
}

func readConsoleEvidence(ctx context.Context, svc *Service, kind, tenant, actor string, query domain.Query) (consoleReadResult, error) {
	var result consoleReadResult
	var err error
	if kind == "page" {
		result.page, err = svc.QueryConsoleEvents(ctx, tenant, actor, query)
	} else {
		result.facets, err = svc.QueryEventFacets(ctx, tenant, actor, query)
	}
	return result, err
}

func consoleReadFacts(t *testing.T, svc *Service) []domain.AdminAction {
	t.Helper()
	actions, err := svc.ListAdminActions(testCtx, "", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	var facts []domain.AdminAction
	for _, action := range actions {
		if action.Action == domain.AdminActionEventRead {
			facts = append(facts, action)
		}
	}
	return facts
}

func (r consoleReadResult) count(kind string) int {
	if kind == "page" {
		return r.page.Count
	}
	return r.facets.Total
}

func TestConsoleEvidenceArchiveIOReleasesControlLock(t *testing.T) {
	for _, kind := range []string{"page", "facets"} {
		for _, tenant := range []string{"tenant-a", ""} {
			t.Run(kind+"/"+tenant, func(t *testing.T) {
				svc, _ := consoleFileEvidenceFixture(t)
				paused := &queryPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
				svc.Config.Archive = paused
				ctx, cancel := context.WithCancel(testCtx)
				defer cancel()
				done, exited := make(chan error, 1), make(chan struct{})
				var result consoleReadResult
				release := sync.OnceFunc(func() { close(paused.release) })
				t.Cleanup(func() { release(); cancel(); <-exited })
				go func() {
					defer close(exited)
					var err error
					result, err = readConsoleEvidence(ctx, svc, kind, tenant, "reader", consoleEvidenceQuery())
					done <- err
				}()
				select {
				case <-paused.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("Console never reached the actual archive Get boundary")
				}
				updated := make(chan error, 1)
				go func() {
					updated <- svc.Store.UpdateControl(ctx, func(data *store.Snapshot) error {
						data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
						return nil
					})
				}()
				// The preparation guard is over: Get is provably in flight. A
				// control write must complete before that remote read is released.
				select {
				case err := <-updated:
					if err != nil {
						t.Fatal("control mutation failed at paused Console Get")
					}
				case <-time.After(2 * time.Second):
					cancel()
					<-updated
					t.Fatal("Console archive Get retained Store.mu")
				}
				release()
				if err := <-done; err != nil || result.count(kind) != 1 {
					t.Fatal("Console did not authenticate and resume captured evidence")
				}
				if err := svc.Store.ReadControl(testCtx, func(data *store.Snapshot) error {
					if data.Policies["tenant-a"].ArchiveDays != 365 {
						t.Fatal("Console read overwrote concurrent control metadata")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				facts := consoleReadFacts(t, svc)
				if len(facts) != 1 || facts[0].Actor != "reader" || facts[0].TenantID != tenant || reflect.DeepEqual(result, consoleReadResult{}) {
					t.Fatal("Console read must append exactly one correctly scoped fact")
				}
			})
		}
	}
}

func TestConsoleEvidenceAuthenticatesBeforeEveryPredicate(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"outcome", func(e *domain.Event) { e.Outcome = "failure" }},
		{"source", func(e *domain.Event) { e.SourceSystem = "other-source" }},
		{"actor", func(e *domain.Event) { e.Actor.ID = "other-actor" }},
		{"type", func(e *domain.Event) { e.EventType = "other-type" }},
		{"time", func(e *domain.Event) { e.OccurredAt = pinnedNow.Add(time.Hour) }},
		{"operation", func(e *domain.Event) { e.OperationID = "other-operation" }},
		{"payload", func(e *domain.Event) { e.Payload["value"] = 99 }},
		{"tenant", func(e *domain.Event) { e.TenantID = "tenant-b" }},
		{"schema", func(e *domain.Event) { e.SchemaVersion++ }},
	}
	for _, archived := range []bool{false, true} {
		mode := "hot"
		if archived {
			mode = "archived"
		}
		for _, mutation := range mutations {
			t.Run(mode+"/"+mutation.name, func(t *testing.T) {
				svc, _ := consoleEvidenceFixture(t, archived)
				if archived {
					svc.Config.Archive = &timelineMutatedArchive{Store: svc.Config.Archive, mutate: mutation.mutate}
				} else if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
					key := store.EventKey("tenant-a", "console-source")
					event := data.Events[key]
					mutation.mutate(&event)
					data.Events[key] = event
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				query := consoleEvidenceQuery()
				query.Outcome, query.SourceSystem, query.ActorID = "success", "crm", "user-1"
				query.EventType, query.OperationID = "audit.event", "console-operation"
				for _, kind := range []string{"page", "facets"} {
					for _, tenant := range []string{"tenant-a", ""} {
						result, err := readConsoleEvidence(testCtx, svc, kind, tenant, "reader", query)
						if err == nil || errors.Is(err, domain.ErrNotFound) || !reflect.DeepEqual(result, consoleReadResult{}) {
							t.Fatal("corrupted committed evidence was served or hidden by a Console predicate")
						}
					}
				}
				if len(consoleReadFacts(t, svc)) != 0 {
					t.Fatal("rejected Console evidence appended a successful read fact")
				}
			})
		}
	}
}

func TestConsoleEvidenceReceiptAndExactSchemaBindings(t *testing.T) {
	key := store.EventKey("tenant-a", "console-source")
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
		{"receipt-sequence", func(d *store.Snapshot) { r := d.Receipts[key]; r.Sequence++; d.Receipts[key] = r }},
		{"missing-schema", func(d *store.Snapshot) {
			s := d.Schemas[schemaKey]
			s.Version = 2
			d.Schemas[store.SchemaKey("tenant-a", s.SchemaID, 2)] = s
			delete(d.Schemas, schemaKey)
		}},
		{"schema-tenant", func(d *store.Snapshot) { s := d.Schemas[schemaKey]; s.TenantID = "tenant-b"; d.Schemas[schemaKey] = s }},
		{"schema-id", func(d *store.Snapshot) {
			s := d.Schemas[schemaKey]
			s.SchemaID = "other-schema"
			d.Schemas[schemaKey] = s
		}},
		{"schema-version", func(d *store.Snapshot) { s := d.Schemas[schemaKey]; s.Version++; d.Schemas[schemaKey] = s }},
	}
	for _, archived := range []bool{false, true} {
		mode := "hot"
		if archived {
			mode = "archived"
		}
		for _, mutation := range mutations {
			t.Run(mode+"/"+mutation.name, func(t *testing.T) {
				svc, _ := consoleEvidenceFixture(t, archived)
				if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error { mutation.mutate(data); return nil }); err != nil {
					t.Fatal(err)
				}
				for _, kind := range []string{"page", "facets"} {
					for _, tenant := range []string{"tenant-a", ""} {
						result, err := readConsoleEvidence(testCtx, svc, kind, tenant, "reader", consoleEvidenceQuery())
						if err == nil || !reflect.DeepEqual(result, consoleReadResult{}) {
							t.Fatal("misbound receipt or exact Schema served Console data")
						}
					}
				}
				if len(consoleReadFacts(t, svc)) != 0 {
					t.Fatal("misbound evidence appended a successful Console fact")
				}
			})
		}
	}
}

func TestConsoleEvidenceCommittedMembershipScopeAndEmptyResults(t *testing.T) {
	for _, layout := range []int{0, 2} {
		svc, _ := consoleEvidenceFixture(t, false)
		if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
			data.LayoutVersion = layout
			key := store.EventKey("tenant-a", "console-source")
			orphan, err := domain.CloneEvent(data.Events[key])
			if err != nil {
				return err
			}
			orphan.EventID = "hot-only"
			data.Events[store.EventKey("tenant-a", orphan.EventID)] = orphan
			data.Receipts[store.EventKey("tenant-b", "foreign-corrupt")] = domain.EventReceipt{TenantID: "tenant-b", EventID: "foreign-corrupt", Status: domain.StatusIndexed}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		for _, kind := range []string{"page", "facets"} {
			result, err := readConsoleEvidence(testCtx, svc, kind, "tenant-a", "reader", consoleEvidenceQuery())
			if err != nil || result.count(kind) != 1 {
				t.Fatal("scoped Console included hot-only/foreign evidence or read another tenant")
			}
			result, err = readConsoleEvidence(testCtx, svc, kind, "", "reader", consoleEvidenceQuery())
			if err == nil || !reflect.DeepEqual(result, consoleReadResult{}) {
				t.Fatal("all-tenant Console skipped corrupt committed foreign evidence")
			}
		}
		if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
			delete(data.Receipts, store.EventKey("tenant-b", "foreign-corrupt"))
			delete(data.Receipts, store.EventKey("tenant-a", "console-source"))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		for _, tenant := range []string{"tenant-a", "", "tenant-missing"} {
			page, err := svc.QueryConsoleEvents(testCtx, tenant, "reader", consoleEvidenceQuery())
			if err != nil || page.Count != 0 || page.Items == nil || len(page.Items) != 0 || page.NextCursor != "" || page.Snapshot != nil {
				t.Fatal("empty Console query must preserve a live empty page, not promote hot-only truth")
			}
			facets, err := svc.QueryEventFacets(testCtx, tenant, "reader", consoleEvidenceQuery())
			if err != nil || facets.Total != 0 || facets.Outcomes == nil || facets.Types == nil || facets.Clients == nil || facets.Providers == nil || len(facets.Outcomes)+len(facets.Types)+len(facets.Clients)+len(facets.Providers) != 0 {
				t.Fatal("empty Console facets must preserve empty non-nil maps")
			}
		}
		if len(consoleReadFacts(t, svc)) != 8 {
			t.Fatal("successful empty reads must self-audit once; failed all-tenant reads must not")
		}
	}
}
