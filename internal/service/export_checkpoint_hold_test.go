package service

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

// The original sealed object is never changed. Only subsequent current-Hold
// source reads are faulted; the artifact's MAC/AEAD and receipt remain valid.
// No source content, ciphertext or search digest is printed on test failure.
type checkpointHoldArchive struct {
	archive.Store
	source []byte
}

func (a *checkpointHoldArchive) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "events/") && a.source != nil {
		return append([]byte(nil), a.source...), nil
	}
	return a.Store.Get(ctx, key)
}

func checkpointHoldFixture(t *testing.T, queryField string) (*Service, *checkpointBackend, *checkpointArchive, domain.Event, store.ExportCheckpoint) {
	t.Helper()
	return checkpointHoldValueFixture(t, queryField, 10)
}

func checkpointHoldValueFixture(t *testing.T, queryField string, value any) (*Service, *checkpointBackend, *checkpointArchive, domain.Event, store.ExportCheckpoint) {
	t.Helper()
	backend := &checkpointBackend{data: store.NewSnapshot()}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{
		TenantID: "tenant-a", SchemaID: "checkpoint.hold", Version: 1, EventType: "audit.event", Active: true,
		RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"},
		EncryptedFields: []string{"value"}, SearchableFields: []string{"value"},
	}); err != nil {
		t.Fatal("could not register exact fixture schema")
	}
	objects := &checkpointArchive{Store: &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}}
	svc.Config.Archive = objects
	event := testEvent("checkpoint-source", "checkpoint-operation", pinnedNow.Add(10*time.Second))
	event.SchemaID = "checkpoint.hold"
	event.Payload["value"] = value
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); err != nil {
		t.Fatal("could not commit fixture source evidence")
	}
	seedLeaseExport(t, svc, "checkpoint-job", "tenant-a")
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		job := data.Exports["checkpoint-job"]
		switch queryField {
		case "actor":
			job.Query.ActorID = event.Actor.ID
		case "digest":
			job.Query.PayloadField = "value"
			var err error
			job.Query.PayloadDigest, err = security.SearchDigestBound(value, svc.Config.EncryptionKey, "tenant-a", "value")
			if err != nil {
				return err
			}
		}
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatal("could not bind fixture query")
	}
	cp := leaveSealedCheckpoint(t, svc, backend)
	data, err := svc.Store.Snapshot(testCtx)
	if err != nil {
		t.Fatal("could not capture fixture receipt")
	}
	source, err := svc.archivedEventContext(testCtx, data.Receipts[store.EventKey("tenant-a", event.EventID)])
	if err != nil {
		t.Fatal("could not read original fixture source")
	}
	return svc, backend, objects, source, cp
}

func TestExportCheckpointHoldRevalidationAuthenticatesBeforeFiltering(t *testing.T) {
	for _, tier := range []string{"hot", "archive"} {
		for _, drift := range []string{"hold-actor", "query-actor", "schema-version", "changed-fields", "ciphertext", "tenant"} {
			t.Run(tier+"/"+drift, func(t *testing.T) {
				queryField := ""
				if drift == "query-actor" {
					queryField = "actor"
				}
				svc, _, objects, source, cp := checkpointHoldFixture(t, queryField)
				holdQuery := domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute), ActorID: source.Actor.ID}
				if drift == "schema-version" || drift == "changed-fields" || drift == "ciphertext" || drift == "tenant" {
					holdQuery.ActorID = "nonmatching-actor"
				}
				if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "case", Reason: "case", Filter: holdQuery}); err != nil {
					t.Fatal("could not create current Hold")
				}
				corrupt, err := domain.CloneEvent(source)
				if err != nil {
					t.Fatal("could not clone fixture source")
				}
				switch drift {
				case "hold-actor", "query-actor":
					corrupt.Actor.ID = "masked-actor"
				case "schema-version":
					corrupt.SchemaVersion++
				case "changed-fields":
					corrupt.ChangedFields["status"] = domain.FieldChange{After: "masked"}
				case "ciphertext":
					corrupt.Payload["value"] = "invalid-encrypted-value"
				case "tenant":
					corrupt.TenantID = "tenant-b"
				}
				fault := &checkpointHoldArchive{Store: objects}
				svc.Config.Archive = fault
				if tier == "hot" {
					if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
						data.Events[store.EventKey("tenant-a", source.EventID)] = corrupt
						return nil
					}); err != nil {
						t.Fatal("could not install hot evidence fault")
					}
				} else {
					fault.source, err = domain.CanonicalJSON(corrupt)
					if err != nil {
						t.Fatal("could not install archive evidence fault")
					}
				}
				expireCheckpoint(svc)
				puts := objects.puts.Load()
				svc.runExport(testCtx, cp.JobID)
				got := leaseSnapshot(t, svc)
				if got.Exports[cp.JobID].Status != "running" || got.Exports[cp.JobID].ObjectPath != "" || got.ExportCheckpoints[cp.JobID] != cp || len(got.ExportLeases) != 1 || objects.puts.Load() != puts || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 0 {
					t.Fatal("unauthenticated current-Hold evidence completed or destroyed the sealed checkpoint")
				}
				// Repair only the injected source fault, never the artifact,
				// lease or terminal state. A later genuine claim must recheck.
				fault.source = nil
				if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
					delete(data.Events, store.EventKey("tenant-a", source.EventID))
					return nil
				}); err != nil {
					t.Fatal("could not remove owned fixture fault")
				}
				expireCheckpoint(svc)
				svc.runExport(testCtx, cp.JobID)
				job := leaseSnapshot(t, svc).Exports[cp.JobID]
				if drift == "hold-actor" || drift == "query-actor" {
					if job.Status != "failed" || job.FailureCode != domain.ExportFailureLegalHold {
						t.Fatal("repaired evidence bypassed its current matching Hold")
					}
				} else if job.Status != "completed" || job.Digest != cp.Digest || job.EventCount != cp.EventCount {
					t.Fatal("repaired evidence could not finalize the original artifact under a nonmatching Hold")
				}
				if objects.puts.Load() != puts {
					t.Fatal("Hold revalidation regenerated the sealed artifact")
				}
			})
		}
	}
}

func TestExportCheckpointHoldDigestUsesExactAuthenticatedNumbersAndQueryFormat(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "bound", true: "legacy"}[legacy], func(t *testing.T) {
			value := json.Number("9007199254740993")
			svc, _, objects, source, cp := checkpointHoldValueFixture(t, "digest", value)
			query := domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute), PayloadField: "value"}
			var err error
			if legacy {
				query.PayloadDigest, err = security.SearchDigest(value, svc.Config.EncryptionKey)
			} else {
				query.PayloadDigest, err = security.SearchDigestBound(value, svc.Config.EncryptionKey, "tenant-a", "value")
			}
			if err != nil {
				t.Fatal("could not derive exact-number Hold filter")
			}
			if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "case", Reason: "case", Filter: query}); err != nil {
				t.Fatal("could not create exact-number Hold")
			}
			delete(source.Payload, "value"+security.SearchDigestSuffix)
			raw, err := domain.CanonicalJSON(source)
			if err != nil {
				t.Fatal("could not encode exact-number evidence")
			}
			svc.Config.Archive = &checkpointHoldArchive{Store: objects, source: raw}
			expireCheckpoint(svc)
			svc.runExport(testCtx, cp.JobID)
			if job := leaseSnapshot(t, svc).Exports[cp.JobID]; job.Status != "failed" || job.FailureCode != domain.ExportFailureLegalHold || job.ObjectPath != "" {
				t.Fatal("numeric precision or digest format hid authenticated held evidence")
			}
		})
	}
}

func TestExportCheckpointDisjointHoldStillNeedsNoSourceReads(t *testing.T) {
	svc, backend, objects := newCheckpointService(t)
	cp := leaveSealedCheckpoint(t, svc, backend)
	if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "disjoint", Reason: "case", Filter: domain.Query{From: pinnedNow.Add(time.Minute), To: pinnedNow.Add(2 * time.Minute)}}); err != nil {
		t.Fatal("could not create disjoint Hold")
	}
	objects.armed.Store(true)
	puts, gets := objects.puts.Load(), objects.gets.Load()
	expireCheckpoint(svc)
	svc.runExport(testCtx, cp.JobID)
	job := leaseSnapshot(t, svc).Exports[cp.JobID]
	if job.Status != "completed" || job.Digest != cp.Digest || job.EventCount != cp.EventCount || objects.puts.Load() != puts || objects.gets.Load() != gets+1 {
		t.Fatal("provably disjoint Hold triggered source re-selection or regenerated the artifact")
	}
}

func TestExportCheckpointCurrentHoldKeepsLowestIDAndLateEvidenceSemantics(t *testing.T) {
	svc, _, objects, source, cp := checkpointHoldFixture(t, "")
	late := testEvent("late-source", "checkpoint-operation", pinnedNow.Add(20*time.Second))
	late.SchemaID, late.Actor.ID = source.SchemaID, "late-actor"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, late, domain.StatusArchived); err != nil {
		t.Fatal("could not commit late current-Hold evidence")
	}
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		for id, actor := range map[string]string{"a-late": late.Actor.ID, "z-original": source.Actor.ID} {
			data.LegalHolds[id] = domain.LegalHold{ID: id, TenantID: "tenant-a", Name: "case", Reason: "case", Filter: domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute), ActorID: actor}}
		}
		return nil
	}); err != nil {
		t.Fatal("could not install deterministic current Holds")
	}
	puts := objects.puts.Load()
	expireCheckpoint(svc)
	svc.runExport(testCtx, cp.JobID)
	job := leaseSnapshot(t, svc).Exports[cp.JobID]
	if job.Status != "failed" || job.FailureCode != domain.ExportFailureLegalHold || job.ObjectPath != "" || objects.puts.Load() != puts {
		t.Fatal("current-Hold gate changed query overlap semantics or regenerated the output")
	}
	if n := countAdminActions(t, svc, func(fact domain.AdminAction) bool {
		return fact.Action == domain.AdminActionExportBlocked && fact.TargetID == "a-late"
	}); n != 1 {
		t.Fatal("current-Hold revalidation did not report the lowest matching Hold ID")
	}
}

func TestExportCheckpointMissingCurrentHoldEvidenceRemainsResumable(t *testing.T) {
	svc, _, objects, source, cp := checkpointHoldFixture(t, "")
	if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "case", Reason: "case"}); err != nil {
		t.Fatal("could not create current Hold")
	}
	setStatus := func(status string) {
		t.Helper()
		if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
			key := store.EventKey("tenant-a", source.EventID)
			receipt := data.Receipts[key]
			receipt.Status = status
			data.Receipts[key] = receipt
			return nil
		}); err != nil {
			t.Fatal("could not install owned receipt availability fault")
		}
	}
	setStatus(domain.StatusIndexed)
	puts := objects.puts.Load()
	expireCheckpoint(svc)
	svc.runExport(testCtx, cp.JobID)
	data := leaseSnapshot(t, svc)
	if data.Exports[cp.JobID].Status != "running" || data.ExportCheckpoints[cp.JobID] != cp || data.Exports[cp.JobID].ObjectPath != "" {
		t.Fatal("missing current evidence was treated as an empty Hold selection")
	}
	setStatus(domain.StatusArchived)
	expireCheckpoint(svc)
	svc.runExport(testCtx, cp.JobID)
	job := leaseSnapshot(t, svc).Exports[cp.JobID]
	if job.Status != "failed" || job.FailureCode != domain.ExportFailureLegalHold || objects.puts.Load() != puts {
		t.Fatal("restored evidence bypassed Hold or repeated Put")
	}
}

func TestExportCheckpointHoldRevalidationDoesNotTrustDerivedSearchDigest(t *testing.T) {
	for _, queryField := range []string{"", "digest"} {
		for _, drift := range []string{"removed", "wrong-value", "legacy-format"} {
			t.Run(queryField+"/"+drift, func(t *testing.T) {
				svc, _, objects, source, cp := checkpointHoldFixture(t, queryField)
				holdQuery := domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute), PayloadField: "value"}
				var err error
				holdQuery.PayloadDigest, err = security.SearchDigestBound(10, svc.Config.EncryptionKey, "tenant-a", "value")
				if err != nil {
					t.Fatal("could not derive fixture Hold filter")
				}
				if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "case", Reason: "case", Filter: holdQuery}); err != nil {
					t.Fatal("could not create digest Hold")
				}
				key := "value" + security.SearchDigestSuffix
				switch drift {
				case "removed":
					delete(source.Payload, key)
				case "wrong-value":
					source.Payload[key], err = security.SearchDigestBound(11, svc.Config.EncryptionKey, "tenant-a", "value")
				case "legacy-format":
					source.Payload[key], err = security.SearchDigest(10, svc.Config.EncryptionKey)
				}
				if err != nil {
					t.Fatal("could not install derived-index fault")
				}
				raw, err := domain.CanonicalJSON(source)
				if err != nil {
					t.Fatal("could not encode fixture source")
				}
				svc.Config.Archive = &checkpointHoldArchive{Store: objects, source: raw}
				expireCheckpoint(svc)
				puts := objects.puts.Load()
				svc.runExport(context.Background(), cp.JobID)
				data := leaseSnapshot(t, svc)
				job := data.Exports[cp.JobID]
				if job.Status != "failed" || job.FailureCode != domain.ExportFailureLegalHold || job.ObjectPath != "" || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 0 || objects.puts.Load() != puts {
					t.Fatal("derived search index hid authenticated evidence from the current Hold")
				}
			})
		}
	}
}
