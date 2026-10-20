package service

import (
	"context"
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// These are selected-coordinate reads, not fresh reference selections. Keep
// the original cursor/coordinates while mutating only the owned fixture.
func coordinateReader(t *testing.T, svc *Service, surface, actor string) func(context.Context) (domain.QueryResult, error) {
	t.Helper()
	switch surface {
	case "event":
		return func(ctx context.Context) (domain.QueryResult, error) {
			event, err := svc.GetEvent(ctx, "tenant-a", actor, "initial-b")
			if reflect.DeepEqual(event, domain.Event{}) {
				return domain.QueryResult{}, err
			}
			return domain.QueryResult{Items: []domain.Event{event}, Count: 1}, err
		}
	case "continuation":
		query := initialQuery()
		first, err := svc.QueryEvents(testCtx, "tenant-a", actor, query)
		if err != nil || first.Count != 2 || len(first.Items) != 1 || first.Items[0].EventID != "initial-a" || first.NextCursor == "" {
			t.Fatal("could not prepare authenticated fixed membership before coordinate fault")
		}
		query.Cursor = first.NextCursor
		return func(ctx context.Context) (domain.QueryResult, error) {
			return svc.QueryEvents(ctx, "tenant-a", actor, query)
		}
	case "projection":
		event, err := svc.GetEvent(testCtx, "tenant-a", "", "initial-b")
		if err != nil {
			t.Fatal("could not prepare exact committed projection coordinates")
		}
		svc.Config.QueryProjection = &queryProjectionFixture{refs: []store.EventReference{eventReference(event)}}
		return func(ctx context.Context) (domain.QueryResult, error) {
			return svc.QueryEvents(ctx, "tenant-a", actor, initialQuery())
		}
	default:
		t.Fatal("unknown coordinate surface")
		return nil
	}
}

func TestCoordinateEvidenceExactSchemaBodyIdentity(t *testing.T) {
	for _, archived := range []bool{false, true} {
		mode := "hot"
		if archived {
			mode = "archived"
		}
		for _, surface := range []string{"event", "continuation", "projection"} {
			for _, field := range []string{"tenant", "id", "version", "missing-exact"} {
				t.Run(mode+"/"+surface+"/"+field, func(t *testing.T) {
					svc, backend, _ := initialQueryFixture(t, archived, true)
					read := coordinateReader(t, svc, surface, "reader")
					facts := backend.appendCalls
					key := store.SchemaKey("tenant-a", "audit.event", 2)
					if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
						schema := data.Schemas[key]
						switch field {
						case "tenant":
							schema.TenantID = "tenant-b"
						case "id":
							schema.SchemaID = "wrong-schema"
						case "version":
							schema.Version++
						case "missing-exact":
							schema.Version++
							data.Schemas[store.SchemaKey("tenant-a", schema.SchemaID, schema.Version)] = schema
							delete(data.Schemas, key)
							return nil
						}
						data.Schemas[key] = schema
						return nil
					}); err != nil {
						t.Fatal("could not install owned exact-Schema identity fault")
					}
					page, err := read(testCtx)
					if err == nil || !reflect.DeepEqual(page, domain.QueryResult{}) || backend.appendCalls != facts {
						t.Fatal("coordinate read accepted misbound exact Schema or served/audited partial evidence")
					}
				})
			}
		}
	}
}

func TestCoordinateEvidenceReceiptBodyMustMatchRequestedIdentity(t *testing.T) {
	for _, archived := range []bool{false, true} {
		mode := "hot"
		if archived {
			mode = "archived"
		}
		for _, foreignTenant := range []bool{false, true} {
			name := "different-event"
			if foreignTenant {
				name = "different-tenant"
			}
			t.Run(mode+"/"+name, func(t *testing.T) {
				svc, backend, objects := initialQueryFixture(t, archived, true)
				sourceKey := store.EventKey("tenant-a", "initial-a")
				if foreignTenant {
					if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-b", Name: "Other tenant", Active: true}); err != nil {
						t.Fatal("could not prepare foreign domain")
					}
					if err := svc.AddSource(testCtx, "test", domain.SourceSystem{ID: "crm", TenantID: "tenant-b", Name: "CRM", Active: true}); err != nil {
						t.Fatal("could not prepare foreign source")
					}
					if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-b", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
						t.Fatal("could not prepare foreign exact Schema")
					}
					if _, err := svc.Ingest(testCtx, "tenant-b", crmPrincipal, testEvent("foreign-source", "other-operation", pinnedNow), domain.StatusLedgered); err != nil {
						t.Fatal("could not commit independent foreign evidence")
					}
					sourceKey = store.EventKey("tenant-b", "foreign-source")
				}
				// A coherent *other* event/receipt remains content- and chain-valid.
				// Only its binding to the requested lookup key is wrong. This v1
				// fault fixture does not rewrite a real append-only ledger.
				if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
					target := store.EventKey("tenant-a", "initial-b")
					data.Receipts[target] = data.Receipts[sourceKey]
					if event, hot := data.Events[sourceKey]; hot {
						data.Events[target] = event
					} else {
						delete(data.Events, target)
					}
					return nil
				}); err != nil {
					t.Fatal("could not install coherent receipt/body substitution")
				}
				gets, facts := len(objects.deadlines), backend.appendCalls
				event, err := svc.GetEvent(testCtx, "tenant-a", "reader", "initial-b")
				if err == nil || !reflect.DeepEqual(event, domain.Event{}) || backend.appendCalls != facts || len(objects.deadlines) != gets {
					t.Fatal("single-event read promoted another identity under the requested map key or fetched it before binding validation")
				}
			})
		}
	}
}

func TestCoordinateEvidenceRetainsHistoricalSchemaAndProtectedOwnedOutput(t *testing.T) {
	for _, archived := range []bool{false, true} {
		for _, surface := range []string{"event", "continuation", "projection"} {
			mode := "hot/"
			if archived {
				mode = "archived/"
			}
			t.Run(mode+surface, func(t *testing.T) {
				svc, backend, _ := initialQueryFixture(t, archived, true)
				read := coordinateReader(t, svc, surface, "reader")
				facts := backend.appendCalls
				key := store.SchemaKey("tenant-a", "audit.event", 2)
				if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
					schema := data.Schemas[key]
					schema.Active = false
					data.Schemas[key] = schema
					latest := schema
					latest.Version, latest.EncryptedFields, latest.SearchableFields = 3, nil, nil
					data.Schemas[store.SchemaKey("tenant-a", "audit.event", 3)] = latest
					return nil
				}); err != nil {
					t.Fatal("could not prepare disabled exact historical Schema with a different latest version")
				}
				page, err := read(testCtx)
				if err != nil || len(page.Items) != 1 || page.Items[0].EventID != "initial-b" || backend.appendCalls != facts+1 {
					t.Fatal("coordinate read substituted latest Schema or rejected a valid disabled historical version")
				}
				if _, protected := page.Items[0].Payload["value"].(string); !protected {
					t.Fatal("coordinate evidence replaced a protected exact numeric value with plaintext")
				}
				baseline, err := domain.CloneEvent(page.Items[0])
				if err != nil {
					t.Fatal("could not clone owned positive-control evidence")
				}
				page.Items[0].Payload["value"] = "caller-mutation"
				page.Items[0].Actor.ID = "caller-mutation"
				again, err := read(testCtx)
				if err != nil || len(again.Items) != 1 || !reflect.DeepEqual(again.Items[0], baseline) || backend.appendCalls != facts+2 {
					t.Fatal("coordinate result aliases stored evidence or lost one independent read fact per success")
				}
			})
		}
	}
}

func TestCoordinateEventRejectsCancellationAfterDurableReadFact(t *testing.T) {
	svc, backend, _ := initialQueryFixture(t, true, true)
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	outcome := &initialQueryAuditOutcome{timelineBudgetBackend: backend, afterAppend: cancel}
	svc.Store = store.NewWithBackend(outcome)
	event, err := svc.GetEvent(ctx, "tenant-a", "reader", "initial-b")
	if err == nil || !reflect.DeepEqual(event, domain.Event{}) || backend.appendCalls != 1 || len(backend.trail) != 1 {
		t.Fatal("single-event read served a withdrawn result or retracted its durable attempt fact")
	}
}
