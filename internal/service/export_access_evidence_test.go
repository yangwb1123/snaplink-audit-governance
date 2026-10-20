package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

// Start with a genuinely resumed and completed sealed checkpoint. Later Hold
// access must authenticate current evidence; completion is not a Hold bypass.
func completedCheckpointAccessFixture(t *testing.T, queryField string, value any) (*Service, *checkpointBackend, *checkpointArchive, domain.Event, domain.ExportJob) {
	t.Helper()
	svc, backend, objects, source, cp := checkpointHoldValueFixture(t, queryField, value)
	expireCheckpoint(svc)
	svc.runExport(testCtx, cp.JobID)
	data := leaseSnapshot(t, svc)
	job := data.Exports[cp.JobID]
	if job.Status != "completed" || job.ObjectPath != cp.ObjectPath || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 {
		t.Fatal("fixture did not complete the original sealed checkpoint")
	}
	return svc, backend, objects, source, job
}

func callExportAccess(t *testing.T, ctx context.Context, svc *Service, surface, tenant, jobID string) error {
	t.Helper()
	if surface == "detail" {
		job, err := svc.GetExport(ctx, tenant, jobID)
		if err != nil && job != (domain.ExportJob{}) {
			t.Error("failed detail access returned evidence metadata")
		}
		return err
	}
	return svc.RecordExportDownload(ctx, tenant, "operator", jobID)
}

func installAccessEvidence(t *testing.T, svc *Service, objects *checkpointArchive, source domain.Event, tier string) {
	t.Helper()
	if tier == "hot" {
		if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
			data.Events[store.EventKey("tenant-a", source.EventID)] = source
			return nil
		}); err != nil {
			t.Fatal("could not install owned hot evidence fault")
		}
		return
	}
	raw, err := domain.CanonicalJSON(source)
	if err != nil {
		t.Fatal("could not encode owned evidence fault")
	}
	svc.Config.Archive = &checkpointHoldArchive{Store: objects, source: raw}
}

func TestExportAccessHoldDoesNotTrustSearchSidecar(t *testing.T) {
	for _, surface := range []string{"detail", "download-audit"} {
		for _, tier := range []string{"hot", "archive"} {
			for _, format := range []string{"bound", "legacy"} {
				for _, drift := range []string{"removed", "wrong-value", "other-format"} {
					t.Run(surface+"/"+tier+"/"+format+"/"+drift, func(t *testing.T) {
						value := json.Number("9007199254740993")
						svc, _, objects, source, job := completedCheckpointAccessFixture(t, "digest", value)
						query := domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute), PayloadField: "value"}
						var err error
						if format == "legacy" {
							query.PayloadDigest, err = security.SearchDigest(value, svc.Config.EncryptionKey)
						} else {
							query.PayloadDigest, err = security.SearchDigestBound(value, svc.Config.EncryptionKey, "tenant-a", "value")
						}
						if err != nil {
							t.Fatal("could not derive exact-number Hold predicate")
						}
						hold, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "case", Reason: "case", Filter: query})
						if err != nil {
							t.Fatal("could not create current Hold")
						}
						key := "value" + security.SearchDigestSuffix
						switch drift {
						case "removed":
							delete(source.Payload, key)
						case "wrong-value":
							source.Payload[key], err = security.SearchDigestBound(11, svc.Config.EncryptionKey, "tenant-a", "value")
						case "other-format":
							source.Payload[key], err = security.SearchDigest(value, svc.Config.EncryptionKey)
						}
						if err != nil {
							t.Fatal("could not install derived-index fault")
						}
						installAccessEvidence(t, svc, objects, source, tier)
						puts := objects.puts.Load()
						accessErr := callExportAccess(t, testCtx, svc, surface, "tenant-a", job.ID)
						var denied interface{ LegalHoldID() string }
						if !errors.Is(accessErr, domain.ErrForbidden) || !errors.As(accessErr, &denied) || denied.LegalHoldID() != hold.ID {
							t.Fatal("derived search sidecar hid authenticated evidence from access Hold")
						}
						if !reflect.DeepEqual(leaseSnapshot(t, svc).Exports[job.ID], job) || objects.puts.Load() != puts || checkpointFactCount(t, svc, domain.AdminActionEventExport) != 0 {
							t.Fatal("denied access changed terminal state, regenerated output or recorded success")
						}
					})
				}
			}
		}
	}
}

func TestExportAccessHoldAuthenticatesBeforeFiltering(t *testing.T) {
	for _, surface := range []string{"detail", "download-audit"} {
		for _, tier := range []string{"hot", "archive"} {
			for _, drift := range []string{"hold-actor", "query-actor", "schema-version", "schema-identity", "changed-fields", "ciphertext", "tenant"} {
				t.Run(surface+"/"+tier+"/"+drift, func(t *testing.T) {
					queryField := ""
					if drift == "query-actor" {
						queryField = "actor"
					}
					svc, _, objects, source, job := completedCheckpointAccessFixture(t, queryField, 10)
					query := domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute), ActorID: source.Actor.ID}
					if drift != "hold-actor" && drift != "query-actor" {
						query.ActorID = "nonmatching-actor"
					}
					if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "case", Reason: "case", Filter: query}); err != nil {
						t.Fatal("could not create current Hold")
					}
					switch drift {
					case "hold-actor", "query-actor":
						source.Actor.ID = "masked-actor"
					case "schema-version":
						source.SchemaVersion++
					case "schema-identity":
						if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
							key := store.SchemaKey("tenant-a", source.SchemaID, source.SchemaVersion)
							schema := data.Schemas[key]
							schema.TenantID = "tenant-b"
							data.Schemas[key] = schema
							return nil
						}); err != nil {
							t.Fatal("could not install exact Schema identity fault")
						}
					case "changed-fields":
						source.ChangedFields["status"] = domain.FieldChange{After: "masked"}
					case "ciphertext":
						source.Payload["value"] = "invalid-encrypted-value"
					case "tenant":
						source.TenantID = "tenant-b"
					}
					installAccessEvidence(t, svc, objects, source, tier)
					puts := objects.puts.Load()
					if err := callExportAccess(t, testCtx, svc, surface, "tenant-a", job.ID); err == nil {
						t.Fatal("unauthenticated current evidence was filtered before access Hold verification")
					}
					if !reflect.DeepEqual(leaseSnapshot(t, svc).Exports[job.ID], job) || objects.puts.Load() != puts || checkpointFactCount(t, svc, domain.AdminActionEventExport) != 0 {
						t.Fatal("invalid access evidence changed terminal state or recorded success")
					}
				})
			}
		}
	}
}

func TestExportAccessHoldMissingEvidenceIsNotEmptySelection(t *testing.T) {
	for _, surface := range []string{"detail", "download-audit"} {
		t.Run(surface, func(t *testing.T) {
			svc, _, _, source, job := completedCheckpointAccessFixture(t, "", 10)
			if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "case", Reason: "case"}); err != nil {
				t.Fatal("could not create current Hold")
			}
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				key := store.EventKey("tenant-a", source.EventID)
				receipt := data.Receipts[key]
				receipt.Status = domain.StatusIndexed
				data.Receipts[key] = receipt
				return nil
			}); err != nil {
				t.Fatal("could not install current evidence availability fault")
			}
			if err := callExportAccess(t, testCtx, svc, surface, "tenant-a", job.ID); !errors.Is(err, domain.ErrNotFound) {
				t.Fatal("missing current evidence was treated as an empty Hold selection")
			}
			if checkpointFactCount(t, svc, domain.AdminActionEventExport) != 0 {
				t.Fatal("missing current evidence recorded download success")
			}
		})
	}
}
