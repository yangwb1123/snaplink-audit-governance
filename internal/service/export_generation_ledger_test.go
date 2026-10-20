package service

import (
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportGenerationCommittedLedgerIdentityDriftPreservesPrefix(t *testing.T) {
	svc, _, objects := newGenerationService(t)
	old := leaveGeneratedPage(t, svc)
	member := old.Plan.Members[0]
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		key := store.EventKey(old.TenantID, member.EventID)
		receipt := data.Receipts[key]
		receipt.EventID = "other-evidence-identity" // original map key/coordinates unchanged
		data.Receipts[key] = receipt
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	expireCheckpoint(svc)
	probe := &generationArchive{Store: objects, forbiddenSources: generatedSourceKeys(old.Plan)}
	svc.Config.Archive = probe
	svc.runExport(testCtx, old.ID)
	data := leaseSnapshot(t, svc)
	if data.Exports[old.ID].Status != "running" || !reflect.DeepEqual(data.ExportPlans[old.ID], old.Plan) || data.Exports[old.ID].ObjectPath != "" || probe.chunkGets.Load() != 0 || probe.chunkPuts.Load() != 0 || probe.finalPuts.Load() != 0 {
		t.Fatal("committed receipt body identity drift was hidden by its map key or authenticated page")
	}
}
