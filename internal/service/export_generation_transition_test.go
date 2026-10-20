package service

import (
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportGenerationCompletePrefixOnlyNeedsAuthenticationAndFinalWholePut(t *testing.T) {
	svc, _, objects := newGenerationService(t)
	old := leaveGeneratedPage(t, svc)
	// Even a well-formed full JSONL cannot advance to sealed before all its
	// page references really committed under this exact progress generation.
	events, err := svc.readExportPlanEvidence(testCtx, old)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := encodeExportEvents(testCtx, events)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := security.EncryptBytesBound(plain, svc.Config.EncryptionKey, security.ExportBinding(old.TenantID, old.ID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.saveExportCheckpoint(exportClaimContext(testCtx, old), old, sealed, plain, len(old.Plan.Members)); err == nil {
		t.Fatal("partial page prefix was consumed as a complete sealed stage")
	}
	if got := leaseSnapshot(t, svc); len(got.ExportCheckpoints) != 0 || !reflect.DeepEqual(got.ExportPlans[old.ID], old.Plan) {
		t.Fatal("rejected partial sealed transition changed progress")
	}
	complete, _, err := svc.generateExportPage(exportClaimContext(testCtx, old), old)
	if err != nil || len(complete.Plan.Chunks) != 2 || exportGeneratedMembers(complete.Plan) != len(complete.Plan.Members) {
		t.Fatal("final partial page did not commit exact remaining member")
	}
	facts := checkpointFactCount(t, svc, domain.AdminActionExportProgressed)
	if _, err := svc.saveExportChunk(exportClaimContext(testCtx, old), old, complete.Plan.Chunks[1]); err == nil {
		t.Fatal("same-owner stale progress claim forked/replayed a committed prefix")
	}
	if !reflect.DeepEqual(leaseSnapshot(t, svc).ExportPlans[old.ID], complete.Plan) || checkpointFactCount(t, svc, domain.AdminActionExportProgressed) != facts {
		t.Fatal("stale prefix replay changed durable progress/facts")
	}
	expireCheckpoint(svc)
	probe := &generationArchive{Store: objects, forbiddenSources: generatedSourceKeys(complete.Plan)}
	svc.Config.Archive = probe
	svc.runExport(testCtx, old.ID)
	data := leaseSnapshot(t, svc)
	if data.Exports[old.ID].Status != "completed" || data.Exports[old.ID].EventCount != len(old.Plan.Members) || probe.sourceGets.Load() != 0 || probe.chunkGets.Load() != 2 || probe.chunkPuts.Load() != 0 || probe.finalPuts.Load() != 1 || len(data.ExportPlans) != 0 {
		t.Fatal("complete prefix was regenerated or did not use existing final sealed protocol")
	}
	if checkpointFactCount(t, svc, domain.AdminActionExportProgressed) != facts {
		t.Fatal("authentication-only continuation invented new page progress")
	}
}

func TestExportGenerationInvalidDurablePrefixCannotBeResumedCancelledOrCleared(t *testing.T) {
	svc, _, objects := newGenerationService(t)
	old := leaveGeneratedPage(t, svc)
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		plan := data.ExportPlans[old.ID]
		plan.Chunks[0].First++
		plan.Signature = svc.signExportPlan(plan)
		data.ExportPlans[old.ID] = plan
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	expireCheckpoint(svc)
	before := leaseSnapshot(t, svc)
	probe := &generationArchive{Store: objects}
	svc.Config.Archive = probe
	svc.runExport(testCtx, old.ID)
	if _, err := svc.RecoverStuckExports(testCtx, old.TenantID); err == nil {
		t.Fatal("recovery silently erased malformed progress")
	}
	if _, err := svc.CancelExport(testCtx, old.TenantID, old.RequestedBy, old.ID); err == nil {
		t.Fatal("cancellation cleared unknown progress")
	}
	if !reflect.DeepEqual(before, leaseSnapshot(t, svc)) || probe.chunkGets.Load() != 0 || probe.chunkPuts.Load() != 0 || probe.finalPuts.Load() != 0 {
		t.Fatal("malformed progress mutated durable state or touched objects")
	}
}
