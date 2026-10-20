package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

// The query can read the original source, then only the subsequent Hold Get
// sees the injected fault. No original source, ciphertext or digest is logged.
// Pure authentication matrices use owned in-memory bytes; the existing real
// FileStore lock oracle, file restart and independent PG cases remain separate.
type preflightEvidenceArchive struct {
	archive.Store
	gets     atomic.Int64
	after    int64
	override []byte
}

func (a *preflightEvidenceArchive) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "events/") && a.gets.Add(1) > a.after && a.override != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return append([]byte(nil), a.override...), nil
	}
	return a.Store.Get(ctx, key)
}

func preflightEvidenceFixture(t *testing.T) (*Service, *checkpointBackend, *checkpointArchive, domain.Event, domain.Query) {
	t.Helper()
	backend := &checkpointBackend{data: store.NewSnapshot()}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	t.Cleanup(func() { _ = svc.Store.Close() })
	seedTestDomain(t, svc)
	pauseExportDispatch(svc)
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{
		TenantID: "tenant-a", SchemaID: "preflight.hold", Version: 1, EventType: "audit.event", Active: true,
		RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"},
		EncryptedFields: []string{"value"}, SearchableFields: []string{"value"},
	}); err != nil {
		t.Fatal("could not register exact preflight Schema")
	}
	objects := &checkpointArchive{Store: newConsoleMemoryArchive()}
	svc.Config.Archive = objects
	event := testEvent("preflight-source", "preflight-operation", pinnedNow.Add(10*time.Second))
	event.SchemaID, event.Payload["value"] = "preflight.hold", json.Number("9007199254740993")
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); err != nil {
		t.Fatal("could not commit original preflight evidence")
	}
	data := leaseSnapshot(t, svc)
	source, err := svc.archivedEventContext(testCtx, data.Receipts[store.EventKey("tenant-a", event.EventID)])
	if err != nil {
		t.Fatal("could not read original protected preflight evidence")
	}
	return svc, backend, objects, source, domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute)}
}

func seedPreflightParent(t *testing.T, svc *Service, query domain.Query) domain.ExportJob {
	t.Helper()
	parent := seedActionExport(t, svc, "preflight-parent", "failed")
	parent.Query = query
	parent.Query.Cursor = "expired-original-query-cursor"
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error { data.Exports[parent.ID] = parent; return nil }); err != nil {
		t.Fatal("could not seed original terminal parent")
	}
	return parent
}

func callExportPreflight(t *testing.T, ctx context.Context, svc *Service, surface string, query domain.Query) (string, error) {
	t.Helper()
	if surface == "create" {
		job, err := svc.CreateExport(ctx, "tenant-a", "operator", query)
		if err != nil && job != (domain.ExportJob{}) {
			t.Error("failed creation returned partial job metadata")
		}
		return job.ID, err
	}
	summary, err := svc.RetryExport(ctx, "tenant-a", "operator", "preflight-parent")
	if err != nil && !reflect.DeepEqual(summary, domain.ExportSummary{}) {
		t.Error("failed retry returned partial successor metadata")
	}
	return summary.ID, err
}

func assertPreflightNoCreation(t *testing.T, svc *Service, parent domain.ExportJob) {
	t.Helper()
	data := leaseSnapshot(t, svc)
	if len(data.Exports) != 1 || !reflect.DeepEqual(data.Exports[parent.ID], parent) || len(data.ExportLeases) != 0 || len(data.ExportPlans) != 0 || len(data.ExportCheckpoints) != 0 {
		t.Fatal("preflight failure created work, reset the parent or changed execution metadata")
	}
	for _, fact := range data.AdminActions {
		if fact.Action == domain.AdminActionExportCreated || fact.Action == domain.AdminActionExportRetried {
			t.Fatal("failed preflight wrote a creation success fact")
		}
	}
}

func TestExportPreflightHoldDoesNotTrustSearchSidecar(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		for _, tier := range []string{"hot", "archive"} {
			for _, format := range []string{"bound", "legacy"} {
				for _, drift := range []string{"removed", "wrong-value", "other-format"} {
					t.Run(surface+"/"+tier+"/"+format+"/"+drift, func(t *testing.T) {
						svc, _, objects, source, query := preflightEvidenceFixture(t)
						query.PayloadField = "value"
						var err error
						value := json.Number("9007199254740993")
						if format == "bound" {
							query.PayloadDigest, err = security.SearchDigestBound(value, svc.Config.EncryptionKey, "tenant-a", "value")
						} else {
							query.PayloadDigest, err = security.SearchDigest(value, svc.Config.EncryptionKey)
						}
						if err != nil {
							t.Fatal("could not derive the exact-number predicate")
						}
						parent := seedPreflightParent(t, svc, query)
						hold, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "case", Reason: "case", Filter: query})
						if err != nil {
							t.Fatal("could not create preflight Hold")
						}
						key := "value" + security.SearchDigestSuffix
						switch drift {
						case "removed":
							delete(source.Payload, key)
						case "wrong-value":
							source.Payload[key], err = security.SearchDigestBound(11, svc.Config.EncryptionKey, "tenant-a", "value")
						case "other-format":
							if format == "bound" {
								source.Payload[key], err = security.SearchDigest(value, svc.Config.EncryptionKey)
							} else {
								source.Payload[key], err = security.SearchDigestBound(value, svc.Config.EncryptionKey, "tenant-a", "value")
							}
						}
						if err != nil {
							t.Fatal("could not install owned derived-index fault")
						}
						if tier == "hot" {
							if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
								data.Events[store.EventKey("tenant-a", source.EventID)] = source
								return nil
							}); err != nil {
								t.Fatal("could not install owned hot sidecar fault")
							}
						} else {
							raw, err := domain.CanonicalJSON(source)
							if err != nil {
								t.Fatal("could not encode owned sidecar fault")
							}
							svc.Config.Archive = &preflightEvidenceArchive{Store: objects, override: raw}
						}
						puts := objects.puts.Load()
						id, err := callExportPreflight(t, testCtx, svc, surface, query)
						var denied interface{ LegalHoldID() string }
						if id != "" || !errors.Is(err, domain.ErrForbidden) || !errors.As(err, &denied) || denied.LegalHoldID() != hold.ID {
							t.Fatal("derived search sidecar hid authenticated evidence from preflight Hold")
						}
						assertPreflightNoCreation(t, svc, parent)
						if objects.puts.Load() != puts || countAdminActions(t, svc, func(f domain.AdminAction) bool {
							return f.Action == domain.AdminActionExportBlocked && f.TargetID == hold.ID
						}) != 1 {
							t.Fatal("Hold denial did not preserve one refusal fact and zero output Put")
						}
					})
				}
			}
		}
	}
}

func TestExportPreflightAuthenticatesBeforeQueryAndHoldPredicates(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		for _, tier := range []string{"hot", "archive"} {
			for _, drift := range []string{"actor", "changed-fields", "ciphertext", "schema-version", "tenant"} {
				t.Run(surface+"/"+tier+"/"+drift, func(t *testing.T) {
					svc, _, objects, source, query := preflightEvidenceFixture(t)
					query.ActorID = "unselected-actor"
					parent := seedPreflightParent(t, svc, query)
					accessNonmatchingHold(t, svc)
					switch drift {
					case "actor":
						source.Actor.ID = "masked-actor"
					case "changed-fields":
						source.ChangedFields["status"] = domain.FieldChange{After: "masked"}
					case "ciphertext":
						source.Payload["value"] = "invalid-protected-value"
					case "schema-version":
						source.SchemaVersion++
					case "tenant":
						source.TenantID = "tenant-b"
					}
					if tier == "hot" {
						if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
							data.Events[store.EventKey("tenant-a", source.EventID)] = source
							return nil
						}); err != nil {
							t.Fatal("could not install owned hot content fault")
						}
					} else {
						raw, err := domain.CanonicalJSON(source)
						if err != nil {
							t.Fatal("could not encode owned current-evidence fault")
						}
						svc.Config.Archive = &preflightEvidenceArchive{Store: objects, after: 1, override: raw}
					}
					id, err := callExportPreflight(t, testCtx, svc, surface, query)
					if err == nil || id != "" {
						t.Fatal("unauthenticated evidence was hidden by query/Hold predicates")
					}
					assertPreflightNoCreation(t, svc, parent)
				})
			}
		}
	}
}

func TestExportPreflightRejectsReceiptAndExactSchemaMisbindings(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		for _, drift := range []string{"receipt-tenant", "receipt-key", "receipt-event", "schema-tenant", "schema-id", "missing-exact-schema", "missing-source"} {
			t.Run(surface+"/"+drift, func(t *testing.T) {
				svc, _, _, source, query := preflightEvidenceFixture(t)
				query.ActorID = "unselected-actor"
				parent := seedPreflightParent(t, svc, query)
				accessNonmatchingHold(t, svc)
				if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
					key := store.EventKey("tenant-a", source.EventID)
					// Hot query prefilter is deliberately nonmatching. Only the
					// later Hold gate can authenticate this committed candidate.
					data.Events[key] = source
					receipt := data.Receipts[key]
					schemaKey := store.SchemaKey("tenant-a", source.SchemaID, source.SchemaVersion)
					schema := data.Schemas[schemaKey]
					switch drift {
					case "receipt-tenant":
						receipt.TenantID = "tenant-b"
						data.Receipts[key] = receipt
					case "receipt-key":
						delete(data.Receipts, key)
						data.Receipts[store.EventKey("tenant-a", "wrong-key")] = receipt
					case "receipt-event":
						receipt.EventID = "wrong-event"
						data.Receipts[key] = receipt
					case "schema-tenant":
						schema.TenantID = "tenant-b"
						data.Schemas[schemaKey] = schema
					case "schema-id":
						schema.SchemaID = "wrong-schema"
						data.Schemas[schemaKey] = schema
					case "missing-exact-schema":
						delete(data.Schemas, schemaKey)
						schema.Version++
						data.Schemas[store.SchemaKey("tenant-a", schema.SchemaID, schema.Version)] = schema
					case "missing-source":
						delete(data.Events, key)
						receipt.Status = domain.StatusIndexed
						data.Receipts[key] = receipt
					}
					return nil
				}); err != nil {
					t.Fatal("could not install owned receipt/Schema availability fault")
				}
				if id, err := callExportPreflight(t, testCtx, svc, surface, query); err == nil || id != "" {
					t.Fatal("invalid committed identity or exact Schema became an empty Hold selection")
				}
				assertPreflightNoCreation(t, svc, parent)
			})
		}
	}
}
