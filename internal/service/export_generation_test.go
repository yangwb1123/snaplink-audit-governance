package service

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportGenerationUnknownOrFailedProgressSaveUsesOnlyDurablePrefix(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-committed", true: "committed-result-unknown"}[unknown], func(t *testing.T) {
			svc, backend, objects := newGenerationService(t)
			backend.failChunk.Store(!unknown)
			backend.unknownChunk.Store(unknown)
			probe := &generationArchive{Store: objects}
			svc.Config.Archive = probe
			svc.runExport(testCtx, "checkpoint-job")
			before := leaseSnapshot(t, svc)
			job, plan := before.Exports["checkpoint-job"], before.ExportPlans["checkpoint-job"]
			wantPages := 0
			if unknown {
				wantPages = 1
			}
			if job.Status != "running" || job.ObjectPath != "" || len(plan.Chunks) != wantPages || probe.chunkPuts.Load() != 1 || probe.finalPuts.Load() != 0 || len(before.ExportCheckpoints) != 0 || checkpointFactCount(t, svc, domain.AdminActionExportProgressed) != wantPages {
				t.Fatal("unknown/failed Save invented, lost or published generation progress")
			}
			backend.failChunk.Store(false)
			backend.unknownChunk.Store(false)
			expireCheckpoint(svc)
			late, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("generation-late", "late-operation", pinnedNow.Add(30*time.Second)), domain.StatusArchived)
			if err != nil {
				t.Fatal(err)
			}
			probe = &generationArchive{Store: objects, forbiddenSources: generatedSourceKeys(plan)}
			probe.forbiddenSources[eventArchiveKey("tenant-a", late.StreamID, late.Sequence, late.EventID)] = true
			svc.Config.Archive = probe
			if n, err := svc.RecoverStuckExports(testCtx, "tenant-a"); err != nil || n != 0 {
				t.Fatal("generation checkpoint was failed or reported as completed recovery")
			}
			svc.runExport(testCtx, job.ID)
			data := leaseSnapshot(t, svc)
			completed := data.Exports[job.ID]
			wantSources := int64(len(plan.Members) - wantPages*store.ExportGenerationPageEvents)
			if completed.Status != "completed" || completed.EventCount != len(plan.Members) || !completed.StartedAt.Equal(*job.StartedAt) || len(data.ExportPlans) != 0 || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 || probe.sourceGets.Load() != wantSources || probe.chunkGets.Load() != int64(wantPages) || probe.chunkPuts.Load() != int64(2-wantPages) || probe.finalPuts.Load() != 1 {
				t.Fatal("resume regenerated a committed page, changed membership, or lost phase/owner cleanup")
			}
			sealed, err := objects.Get(testCtx, completed.ObjectPath)
			if err != nil {
				t.Fatal(err)
			}
			plain, err := svc.VerifyExportDownload(testCtx, "tenant-a", job.ID, sealed)
			if err != nil || !exportPlainMatchesPlan(plain, plan) || bytes.Contains(plain, []byte("__search_digest")) {
				t.Fatal("final compatible download did not preserve exact ordered fixed membership")
			}
			if checkpointFactCount(t, svc, domain.AdminActionExportProgressed) != 2 || checkpointFactCount(t, svc, domain.AdminActionExportPlanned) != 1 || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 1 {
				t.Fatal("page/selection/terminal facts are not atomic and unique")
			}
			puts := objects.puts.Load()
			svc.runExport(testCtx, job.ID)
			if !reflect.DeepEqual(data.Exports[job.ID], leaseSnapshot(t, svc).Exports[job.ID]) || objects.puts.Load() != puts {
				t.Fatal("terminal generation was replayed or regressed")
			}
		})
	}
}

func TestExportGenerationCurrentLateHoldBlocksWithoutChangingFixedMembers(t *testing.T) {
	svc, _, objects := newGenerationService(t)
	old := leaveGeneratedPage(t, svc)
	late := testEvent("late-protected-source", "later-held-operation", pinnedNow.Add(30*time.Second))
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, late, domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	hold, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "late", Reason: "case", Filter: domain.Query{OperationID: "later-held-operation"}})
	if err != nil {
		t.Fatal(err)
	}
	expireCheckpoint(svc)
	probe := &generationArchive{Store: objects}
	svc.Config.Archive = probe
	svc.runExport(testCtx, old.ID)
	data := leaseSnapshot(t, svc)
	job := data.Exports[old.ID]
	if job.Status != "failed" || job.FailureCode != domain.ExportFailureLegalHold || job.ObjectPath != "" || len(data.ExportPlans) != 0 || len(data.ExportLeases) != 0 || probe.chunkGets.Load() != 0 || probe.chunkPuts.Load() != 0 || probe.finalPuts.Load() != 0 {
		t.Fatal("resumed generation checked only original members or bypassed current Hold")
	}
	if _, err := objects.Get(testCtx, old.Plan.Chunks[0].ObjectPath); err != nil {
		t.Fatal("Hold cleanup deleted immutable page")
	}
	if _, err := svc.ReleaseLegalHold(testCtx, "tenant-a", hold.ID, "case-worker"); err != nil {
		t.Fatal(err)
	}
	svc.runExport(testCtx, old.ID)
	if !reflect.DeepEqual(job, leaseSnapshot(t, svc).Exports[old.ID]) {
		t.Fatal("Hold release regressed terminal job")
	}
}

func TestExportGenerationAuditOrEvidenceFailureRetainsExactPrefix(t *testing.T) {
	svc, backend, objects := newGenerationService(t)
	old := leaveGeneratedPage(t, svc)
	expireCheckpoint(svc)
	backend.failGenerationRead.Store(true)
	probe := &generationArchive{Store: objects}
	svc.Config.Archive = probe
	svc.runExport(testCtx, old.ID)
	data := leaseSnapshot(t, svc)
	if data.Exports[old.ID].Status != "running" || !reflect.DeepEqual(data.ExportPlans[old.ID], old.Plan) || probe.chunkGets.Load() != 0 || probe.chunkPuts.Load() != 0 || probe.finalPuts.Load() != 0 {
		t.Fatal("read self-audit failure consumed, regenerated or published prefix")
	}
	backend.failGenerationRead.Store(false)
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		key := store.SchemaKey(old.TenantID, old.Plan.Members[0].SchemaID, old.Plan.Members[0].SchemaVersion)
		schema := data.Schemas[key]
		schema.Classification = "drifted"
		data.Schemas[key] = schema
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	expireCheckpoint(svc)
	svc.runExport(testCtx, old.ID)
	if got := leaseSnapshot(t, svc); got.Exports[old.ID].Status != "running" || !reflect.DeepEqual(got.ExportPlans[old.ID], old.Plan) || probe.chunkGets.Load() != 0 || probe.chunkPuts.Load() != 0 || probe.finalPuts.Load() != 0 {
		t.Fatal("current schema drift was hidden by authenticated page progress")
	}
	raw, err := json.Marshal(domain.SummarizeExport(data.Exports[old.ID]))
	if err != nil || bytes.Contains(raw, []byte("chunks")) || bytes.Contains(raw, []byte(old.Owner)) || bytes.Contains(raw, []byte(old.Plan.Chunks[0].Digest)) {
		t.Fatal("private generation metadata escaped operational summary")
	}
}
