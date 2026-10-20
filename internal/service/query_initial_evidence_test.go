package service

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

// These matrices exercise real Load/Save/Put/Get/append with owned exact bytes,
// not filesystem throughput. Real file restart/Get and independent SQL pools
// are covered separately; an in-memory fixture is not durability evidence.
func initialQueryFixture(t *testing.T, archived, encrypted bool) (*Service, *timelineBudgetBackend, *timelineBudgetArchive) {
	t.Helper()
	backend := &timelineBudgetBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	t.Cleanup(func() { _ = svc.Store.Close() })
	seedTestDomain(t, svc)
	schema := domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, SearchableFields: []string{"value"}}
	if encrypted {
		schema.EncryptedFields = []string{"value"}
	}
	if err := svc.RegisterSchema(testCtx, "test", schema); err != nil {
		t.Fatal("could not register exact initial-query Schema")
	}
	objects := &timelineBudgetArchive{Store: newConsoleMemoryArchive()}
	if archived {
		svc.Config.Archive = objects
	}
	for i, id := range []string{"initial-a", "initial-b"} {
		event := testEvent(id, "initial-operation", pinnedNow.Add(time.Duration(i)*time.Second))
		event.SchemaVersion = 2
		event.Payload["value"] = json.Number("9007199254740993")
		event.CausationID, event.CorrelationID, event.TraceID = "initial-cause", "initial-correlation", "initial-trace"
		event.Targets = []domain.Target{{ID: "initial-target", Type: "invoice"}}
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal("could not commit exact initial-query evidence")
		}
	}
	backend.readDeadline, backend.auditDeadline = time.Time{}, time.Time{}
	return svc, backend, objects
}

func initialQuery() domain.Query {
	return domain.Query{From: pinnedNow.Add(-time.Minute), To: pinnedNow.Add(time.Minute), PageSize: 1}
}

func mutateInitialQueryEvidence(t *testing.T, svc *Service, archived bool, mutate func(*domain.Event)) {
	t.Helper()
	if archived {
		svc.Config.Archive = &timelineMutatedArchive{Store: svc.Config.Archive, mutate: mutate}
		return
	}
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		for key, event := range data.Events {
			mutate(&event)
			data.Events[key] = event
		}
		return nil
	}); err != nil {
		t.Fatal("could not install owned initial-query evidence fault")
	}
}

func TestInitialQueryAuthenticatesBeforeEveryPredicate(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"occurrence", func(e *domain.Event) { e.OccurredAt = e.OccurredAt.Add(time.Hour) }},
		{"source", func(e *domain.Event) { e.SourceSystem = "hidden-source" }},
		{"type", func(e *domain.Event) { e.EventType = "hidden-type" }},
		{"actor", func(e *domain.Event) { e.Actor.ID = "hidden-actor" }},
		{"target", func(e *domain.Event) { e.Targets[0].ID = "hidden-target" }},
		{"aggregate-type", func(e *domain.Event) { e.AggregateType = "hidden-type" }},
		{"aggregate-id", func(e *domain.Event) { e.AggregateID = "hidden-id" }},
		{"operation", func(e *domain.Event) { e.OperationID = "hidden-operation" }},
		{"causation", func(e *domain.Event) { e.CausationID = "hidden-cause" }},
		{"correlation", func(e *domain.Event) { e.CorrelationID = "hidden-correlation" }},
		{"trace", func(e *domain.Event) { e.TraceID = "hidden-trace" }},
		{"outcome", func(e *domain.Event) { e.Outcome = "hidden-outcome" }},
		{"stream", func(e *domain.Event) { e.StreamID = "hidden-stream" }},
		{"tenant", func(e *domain.Event) { e.TenantID = "tenant-b" }},
		{"protected-content", func(e *domain.Event) { e.Payload["value"] = "corrupt-protected-value" }},
	}
	for _, archived := range []bool{false, true} {
		mode := "hot"
		if archived {
			mode = "archived"
		}
		for _, mutation := range mutations {
			t.Run(mode+"/"+mutation.name, func(t *testing.T) {
				svc, backend, _ := initialQueryFixture(t, archived, true)
				query := initialQuery()
				query.EventType, query.SourceSystem, query.ActorID, query.TargetID = "audit.event", "crm", "user-1", "initial-target"
				query.AggregateType, query.AggregateID, query.OperationID = "invoice", "inv-1", "initial-operation"
				query.CausationID, query.CorrelationID, query.TraceID, query.Outcome = "initial-cause", "initial-correlation", "initial-trace", "success"
				query.StreamID = "tenant-a:aggregate:invoice:inv-1"
				mutateInitialQueryEvidence(t, svc, archived, func(event *domain.Event) {
					if event.EventID == "initial-a" {
						mutation.mutate(event)
					}
				})
				page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", query)
				if err == nil || !reflect.DeepEqual(page, domain.QueryResult{}) || backend.appendCalls != 0 || len(backend.trail) != 0 {
					t.Fatal("corrupted committed evidence was hidden by an initial-query predicate or served as partial membership")
				}
			})
		}
	}
}

func TestInitialQueryReceiptAndExactSchemaBindings(t *testing.T) {
	key, schemaKey := store.EventKey("tenant-a", "initial-a"), store.SchemaKey("tenant-a", "audit.event", 2)
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
		{"schema-version", func(d *store.Snapshot) { s := d.Schemas[schemaKey]; s.Version++; d.Schemas[schemaKey] = s }},
		{"missing-exact-schema", func(d *store.Snapshot) {
			s := d.Schemas[schemaKey]
			s.Version++
			d.Schemas[store.SchemaKey("tenant-a", s.SchemaID, s.Version)] = s
			delete(d.Schemas, schemaKey)
		}},
		{"missing-indexed-body", func(d *store.Snapshot) {
			delete(d.Events, key)
			r := d.Receipts[key]
			r.Status = domain.StatusIndexed
			d.Receipts[key] = r
		}},
	} {
		for _, archived := range []bool{false, true} {
			mode := "hot"
			if archived {
				mode = "archived"
			}
			t.Run(mode+"/"+mutation.name, func(t *testing.T) {
				svc, backend, _ := initialQueryFixture(t, archived, true)
				if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error { mutation.mutate(data); return nil }); err != nil {
					t.Fatal("could not install exact initial-query binding fault")
				}
				page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", initialQuery())
				if err == nil || !reflect.DeepEqual(page, domain.QueryResult{}) || backend.appendCalls != 0 {
					t.Fatal("initial query promoted misbound/missing committed evidence or substituted a different Schema")
				}
			})
		}
	}
}

func TestInitialQuerySensitivePredicatesUseAuthenticatedValues(t *testing.T) {
	for _, archived := range []bool{false, true} {
		for _, encrypted := range []bool{false, true} {
			for _, format := range []string{"bound", "legacy"} {
				for _, fault := range []string{"missing", "wrong-value", "other-format", "forged-match"} {
					mode := "hot/plain/"
					if archived {
						mode = "archived/plain/"
					}
					if encrypted {
						mode += "encrypted/"
					}
					t.Run(mode+format+"/"+fault, func(t *testing.T) {
						svc, backend, _ := initialQueryFixture(t, archived, encrypted)
						query := initialQuery()
						value, want := json.Number("9007199254740993"), 2
						if fault == "forged-match" {
							value, want = json.Number("9007199254740994"), 0
						}
						bound, err := security.SearchDigestBound(value, svc.Config.EncryptionKey, "tenant-a", "value")
						if err != nil {
							t.Fatal("could not prepare synthetic bound predicate")
						}
						legacy, err := security.SearchDigest(value, svc.Config.EncryptionKey)
						if err != nil {
							t.Fatal("could not prepare synthetic legacy predicate")
						}
						query.PayloadField, query.PayloadDigest = "value", bound
						alternate := legacy
						if format == "legacy" {
							query.PayloadDigest, alternate = legacy, bound
						}
						mutateInitialQueryEvidence(t, svc, archived, func(event *domain.Event) {
							field := "value" + security.SearchDigestSuffix
							switch fault {
							case "missing":
								delete(event.Payload, field)
							case "wrong-value":
								event.Payload[field] = "untrusted-derived-value"
							case "other-format":
								event.Payload[field] = alternate
							case "forged-match":
								event.Payload[field] = query.PayloadDigest
							}
						})
						page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", query)
						if err != nil || page.Count != want || len(page.Items) != min(want, 1) || page.Snapshot == nil || page.Snapshot.Source != "ledger" || backend.appendCalls != 1 {
							t.Fatal("initial query trusted a sidecar instead of exact authenticated numeric content")
						}
						if want == 0 {
							return
						}
						if page.NextCursor == "" || page.Items[0].EventID != "initial-a" {
							t.Fatal("authenticated sensitive selection lost chronological fixed membership")
						}
						if encrypted {
							if _, protected := page.Items[0].Payload["value"].(string); !protected {
								t.Fatal("initial query replaced protected response values with plaintext")
							}
						}
						query.Cursor = page.NextCursor
						// Continuation must keep the original member, not re-evaluate a
						// mutable sidecar or reconstruct a new live result set.
						next, err := svc.QueryEvents(context.Background(), "tenant-a", "reader", query)
						if err != nil || next.Count != 2 || len(next.Items) != 1 || next.Items[0].EventID != "initial-b" || next.NextCursor != "" || backend.appendCalls != 2 {
							t.Fatal("sensitive query continuation reselected or lost the authenticated member")
						}
					})
				}
			}
		}
	}
}
