package service

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestConsoleEvidenceSensitivePredicatesUseAuthenticatedValues(t *testing.T) {
	for _, archived := range []bool{false, true} {
		for _, encrypted := range []bool{false, true} {
			mode := "hot/plain"
			if archived {
				mode = "archived/plain"
			}
			if encrypted {
				mode += "/encrypted"
			}
			for _, format := range []string{"bound", "legacy"} {
				for _, sidecar := range []string{"missing", "wrong-value", "other-format", "forged-match"} {
					t.Run(mode+"/"+format+"/"+sidecar, func(t *testing.T) {
						svc, _ := consoleEvidenceFixture(t, false)
						schema := domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, SearchableFields: []string{"value"}}
						if encrypted {
							schema.EncryptedFields = []string{"value"}
						}
						if err := svc.RegisterSchema(testCtx, "test", schema); err != nil {
							t.Fatal(err)
						}
						if archived {
							svc.Config.Archive = newConsoleMemoryArchive()
						}
						event := testEvent("console-protected", "protected-operation", pinnedNow.Add(time.Second))
						event.SchemaVersion = 2
						event.Payload["value"] = json.Number("9007199254740993")
						if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
							t.Fatal(err)
						}
						value := json.Number("9007199254740993")
						want := 1
						if sidecar == "forged-match" {
							value = json.Number("9007199254740994")
							want = 0
						}
						var digest string
						var err error
						if format == "bound" {
							digest, err = security.SearchDigestBound(value, svc.Config.EncryptionKey, "tenant-a", "value")
						} else {
							digest, err = security.SearchDigest(value, svc.Config.EncryptionKey)
						}
						if err != nil {
							t.Fatal("could not prepare synthetic condition")
						}
						alternate, err := security.SearchDigest(value, svc.Config.EncryptionKey)
						if format == "legacy" {
							alternate, err = security.SearchDigestBound(value, svc.Config.EncryptionKey, "tenant-a", "value")
						}
						if err != nil {
							t.Fatal("could not prepare alternate synthetic condition format")
						}
						mutate := func(event *domain.Event) {
							if event.EventID != "console-protected" {
								return
							}
							field := "value" + security.SearchDigestSuffix
							switch sidecar {
							case "missing":
								delete(event.Payload, field)
							case "wrong-value":
								event.Payload[field] = "untrusted-derived-value"
							case "other-format":
								event.Payload[field] = alternate
							case "forged-match":
								event.Payload[field] = digest
							}
						}
						if archived {
							svc.Config.Archive = &timelineMutatedArchive{Store: svc.Config.Archive, mutate: mutate}
						} else if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
							key := store.EventKey("tenant-a", "console-protected")
							event := data.Events[key]
							mutate(&event)
							data.Events[key] = event
							return nil
						}); err != nil {
							t.Fatal(err)
						}
						query := consoleEvidenceQuery()
						query.PayloadField, query.PayloadDigest = "value", digest
						for _, kind := range []string{"page", "facets"} {
							result, err := readConsoleEvidence(testCtx, svc, kind, "tenant-a", "reader", query)
							if err != nil || result.count(kind) != want {
								t.Fatal("Console trusted sidecar presence/value/format instead of authenticated exact numeric content")
							}
							if kind == "page" && want == 1 {
								item := result.page.Items[0]
								if item.EventID != "console-protected" {
									t.Fatal("Console selected an unrelated event")
								}
								if encrypted {
									if _, protected := item.Payload["value"].(string); !protected {
										t.Fatal("Console replaced protected response bytes with decrypted content")
									}
								}
							}
						}
						if len(consoleReadFacts(t, svc)) != 2 {
							t.Fatal("sensitive Console selection did not append exactly one fact per call")
						}
					})
				}
			}
		}
	}
}

func seedConsoleOtherTenant(t *testing.T, svc *Service) {
	t.Helper()
	if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-b", Name: "Console tenant B", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-b", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-b", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
}

func TestConsoleEvidenceLiveOrderCountsFiltersAndOwnedOutput(t *testing.T) {
	svc, _ := consoleEvidenceFixture(t, false)
	if err := svc.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-a", ID: "billing", Name: "Billing", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "billing.event", Version: 1, EventType: "billing.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	other := testEvent("console-newer", "other-operation", pinnedNow.Add(2*time.Second))
	other.SourceSystem, other.SchemaID, other.EventType = "billing", "billing.event", "billing.event"
	other.AggregateID, other.Actor.ID, other.Outcome = "inv-2", "other-actor", "failure"
	if _, err := svc.Ingest(testCtx, "tenant-a", domain.IngestPrincipal{ClientID: "billing"}, other, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	seedConsoleOtherTenant(t, svc)
	foreign := testEvent("console-foreign", "foreign-operation", pinnedNow.Add(3*time.Second))
	if _, err := svc.Ingest(testCtx, "tenant-b", crmPrincipal, foreign, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	filters := []struct {
		name string
		edit func(*domain.Query)
	}{
		{"outcome", func(q *domain.Query) { q.Outcome = "success" }},
		{"source", func(q *domain.Query) { q.SourceSystem = "crm" }},
		{"type", func(q *domain.Query) { q.EventType = "audit.event" }},
		{"actor", func(q *domain.Query) { q.ActorID = "user-1" }},
		{"operation", func(q *domain.Query) { q.OperationID = "console-operation" }},
		{"aggregate", func(q *domain.Query) { q.AggregateID = "inv-1" }},
		{"half-open-time", func(q *domain.Query) { q.From, q.To = pinnedNow, pinnedNow.Add(2*time.Second) }},
	}
	for _, filter := range filters {
		t.Run(filter.name, func(t *testing.T) {
			query := consoleEvidenceQuery()
			filter.edit(&query)
			for _, kind := range []string{"page", "facets"} {
				result, err := readConsoleEvidence(testCtx, svc, kind, "tenant-a", "", query)
				if err != nil || result.count(kind) != 1 || (kind == "page" && result.page.Items[0].EventID != "console-source") {
					t.Fatal("Console filter/half-open window semantics or scoped selection changed")
				}
			}
		})
	}
	all, err := svc.QueryConsoleEvents(testCtx, "", "reader", consoleEvidenceQuery())
	if err != nil || all.Count != 3 || len(all.Items) != 1 || all.Items[0].TenantID != "tenant-b" || all.Snapshot != nil {
		t.Fatal("authorized optional all-tenant Console selection changed")
	}
	query := consoleEvidenceQuery()
	first, err := svc.QueryConsoleEvents(testCtx, "tenant-a", "reader", query)
	if err != nil || first.Count != 2 || len(first.Items) != 1 || first.Items[0].EventID != "console-newer" || first.NextCursor == "" || first.Snapshot != nil {
		t.Fatal("Console first live page lost newest-first order/count")
	}
	late := testEvent("console-late", "console-operation", pinnedNow.Add(time.Second))
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, late, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	query.Cursor = first.NextCursor
	next, err := svc.QueryConsoleEvents(testCtx, "tenant-a", "reader", query)
	if err != nil || next.Count != 2 || len(next.Items) != 1 || next.Items[0].EventID != "console-late" || next.NextCursor == "" || next.Snapshot != nil {
		t.Fatal("Console live continuation must retain legacy remaining-count semantics and observe late facts")
	}
	query.Cursor = next.NextCursor
	last, err := svc.QueryConsoleEvents(testCtx, "tenant-a", "reader", query)
	if err != nil || last.Count != 1 || len(last.Items) != 1 || last.Items[0].EventID != "console-source" || last.NextCursor != "" {
		t.Fatal("Console final live page changed its count or cursor")
	}
	// Facets ignore pagination, count all filtered evidence, and have no
	// trusted provider counterpart. They are not ClickHouse completeness proof.
	facets, err := svc.QueryEventFacets(testCtx, "tenant-a", "reader", query)
	if err != nil || facets.Total != 3 || facets.Outcomes["success"] != 2 || facets.Outcomes["failure"] != 1 || facets.Types["audit.event"] != 2 || facets.Clients["billing"] != 1 || len(facets.Providers) != 0 {
		t.Fatal("Console facets used the cursor/page limit or inferred an untrusted provider")
	}
	last.Items[0].Payload["value"] = "caller-mutation"
	last.Items[0].Actor.Roles[0] = "caller-mutation"
	again, err := svc.QueryConsoleEvents(testCtx, "tenant-a", "", query)
	if err != nil || again.Items[0].Payload["value"] == "caller-mutation" || again.Items[0].Actor.Roles[0] != "operator" {
		t.Fatal("Console output aliases stored evidence")
	}
	if len(consoleReadFacts(t, svc)) != 5 {
		t.Fatal("Console calls did not append once; internal reads must remain silent")
	}
}

func TestConsoleEvidenceInactiveHistoricalSchemaRemainsReadable(t *testing.T) {
	svc, _ := consoleEvidenceFixture(t, false)
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		key := store.SchemaKey("tenant-a", "audit.event", 1)
		schema := data.Schemas[key]
		schema.Active = false
		data.Schemas[key] = schema
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"page", "facets"} {
		result, err := readConsoleEvidence(testCtx, svc, kind, "tenant-a", "reader", consoleEvidenceQuery())
		if err != nil || result.count(kind) != 1 {
			t.Fatal("disabling ingestion Schema made exact historical evidence unreadable")
		}
	}
}
