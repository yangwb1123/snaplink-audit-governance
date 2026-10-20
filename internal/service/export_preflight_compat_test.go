package service

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportPreflightPreservesFastPathsEmptySelectionAndTenantIsolation(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		for _, mode := range []string{"no-Hold", "disjoint", "released", "foreign", "nonmatching", "zero-events", "hot-only", "foreign-corruption", "inactive-exact-schema"} {
			t.Run(surface+"/"+mode, func(t *testing.T) {
				svc, _, objects, source, query := preflightEvidenceFixture(t)
				if mode == "zero-events" {
					query.ActorID = "unselected-actor"
				}
				parent := seedPreflightParent(t, svc, query)
				wantGets := int64(1) // the original audited query, never a Hold Get
				switch mode {
				case "disjoint", "released", "foreign", "nonmatching", "zero-events", "hot-only", "inactive-exact-schema":
					hold := domain.LegalHold{TenantID: "tenant-a", Name: "case", Reason: "case"}
					switch mode {
					case "disjoint":
						hold.Filter = domain.Query{From: query.To, To: query.To.Add(time.Minute)}
					case "foreign":
						if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-b", Name: "Foreign", Active: true}); err != nil {
							t.Fatal("could not seed foreign tenant")
						}
						hold.TenantID = "tenant-b"
					case "nonmatching", "inactive-exact-schema":
						hold.Filter = domain.Query{From: query.From, To: query.To, ActorID: "unselected-actor"}
						wantGets = 2
					case "zero-events":
						wantGets = 2
					case "hot-only":
						wantGets = 0
					}
					created, err := svc.CreateLegalHold(testCtx, hold)
					if err != nil {
						t.Fatal("could not seed scoped Hold")
					}
					if mode == "released" {
						if _, err := svc.ReleaseLegalHold(testCtx, "tenant-a", created.ID, "case-worker"); err != nil {
							t.Fatal("could not release owned Hold")
						}
					}
				}
				if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
					switch mode {
					case "hot-only":
						key := store.EventKey("tenant-a", source.EventID)
						data.Events[key] = source
						delete(data.Receipts, key)
					case "foreign-corruption":
						data.Receipts[store.EventKey("tenant-b", "bad-foreign")] = domain.EventReceipt{TenantID: "tenant-b", EventID: "bad-foreign", Status: domain.StatusArchived}
					case "inactive-exact-schema":
						key := store.SchemaKey("tenant-a", source.SchemaID, source.SchemaVersion)
						schema := data.Schemas[key]
						schema.Active = false
						data.Schemas[key] = schema
					}
					return nil
				}); err != nil {
					t.Fatal("could not install owned isolation/availability fixture")
				}
				observed := &preflightEvidenceArchive{Store: objects}
				svc.Config.Archive = observed
				id, err := callExportPreflight(t, testCtx, svc, surface, query)
				data := leaseSnapshot(t, svc)
				if err != nil || id == "" || len(data.Exports) != 2 || !reflect.DeepEqual(data.Exports[parent.ID], parent) || data.Exports[id].Status != "pending" || data.Exports[id].Query.Cursor != "" || observed.gets.Load() != wantGets {
					t.Fatal("preflight changed original fast-path, isolation, empty-selection or terminal semantics")
				}
				if surface == "retry" && data.Exports[id].RetryOf != parent.ID {
					t.Fatal("retry lost its single new-successor lineage")
				}
				facts := readFacts(t, svc)
				if len(facts) != 1 || facts[0].Actor != "operator" || facts[0].TenantID != "tenant-a" || facts[0].TargetType != "query" {
					t.Fatal("preflight duplicated, skipped or changed the original read self-audit")
				}
			})
		}
	}
}

func TestExportPreflightAuthenticationSurvivesRealFileRestart(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		t.Run(surface, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.json")
			st, err := store.Open(path)
			if err != nil {
				t.Fatal("could not open private original file store")
			}
			t.Cleanup(func() { _ = st.Close() })
			svc := newServiceOn(t, st)
			seedFileTestDomain(t, svc)
			pauseExportDispatch(svc)
			objects := &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
			svc.Config.Archive = objects
			if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "preflight.file", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, EncryptedFields: []string{"value"}, SearchableFields: []string{"value"}}); err != nil {
				t.Fatal("could not register file preflight Schema")
			}
			event := testEvent("preflight-file", "preflight-operation", pinnedNow.Add(10*time.Second))
			event.SchemaID = "preflight.file"
			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); err != nil {
				t.Fatal("could not archive original file evidence")
			}
			query := domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute), PayloadField: "value"}
			query.PayloadDigest, err = security.SearchDigestBound(10, svc.Config.EncryptionKey, "tenant-a", "value")
			if err != nil {
				t.Fatal("could not derive original file predicate")
			}
			parent := seedPreflightParent(t, svc, query)
			if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "case", Reason: "case", Filter: query}); err != nil {
				t.Fatal("could not create file Hold")
			}
			original, err := st.Snapshot(testCtx)
			if err != nil {
				t.Fatal("could not capture original file ledger evidence")
			}
			source, err := svc.archivedEventContext(testCtx, original.Receipts[store.EventKey("tenant-a", event.EventID)])
			if err != nil {
				t.Fatal("could not read exact original FileStore source")
			}
			delete(source.Payload, "value"+security.SearchDigestSuffix)
			if err := st.UpdateTenant(testCtx, "tenant-a", store.HotFirst, func(view *store.TenantView) error {
				view.Hot.Events[store.EventKey("tenant-a", source.EventID)] = source
				return nil
			}); err != nil || st.Close() != nil {
				t.Fatal("could not persist owned sidecar fault and release original file writer")
			}
			reopened, err := store.Open(path)
			if err != nil {
				t.Fatal("could not reopen original file store")
			}
			t.Cleanup(func() { _ = reopened.Close() })
			next := newServiceOn(t, reopened)
			next.Config.Archive = objects
			pauseExportDispatch(next)
			persisted, err := reopened.Snapshot(testCtx)
			if err != nil || persisted.Events[store.EventKey("tenant-a", source.EventID)].EventID != source.EventID {
				t.Fatal("restart oracle lost the owned hot sidecar fault")
			}
			if _, exists := persisted.Events[store.EventKey("tenant-a", source.EventID)].Payload["value"+security.SearchDigestSuffix]; exists {
				t.Fatal("restart oracle silently restored derived metadata before verification")
			}
			if id, err := callExportPreflight(t, testCtx, next, surface, query); id != "" || !errors.Is(err, domain.ErrForbidden) {
				t.Fatal("real restart let saved derived metadata bypass authenticated Hold")
			}
			assertPreflightNoCreation(t, next, parent)
			current, err := next.Store.Snapshot(testCtx)
			if err != nil || !reflect.DeepEqual(original.Receipts, current.Receipts) || !reflect.DeepEqual(original.Schemas, current.Schemas) {
				t.Fatal("preflight rewrote original ledger coordinates or exact Schema")
			}
		})
	}
}
