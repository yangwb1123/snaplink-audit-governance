package store

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestExportGenerationProgressCloneRestartAndConcurrentTenantDelta(t *testing.T) {
	plan := planMetadataFixture()
	plan.Version = 2
	plan.Chunks = []ExportPlanChunk{{SealVersion: 2, First: 0, Count: ExportGenerationPageEvents,
		ObjectPath: "opaque-page-reference", SealedDigest: "sealed-digest", Digest: "plain-digest", CreatedAt: time.Unix(1700000001, 0).UTC()}}
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateControlChecked(testCtx, func(data *Snapshot) (bool, error) { data.ExportPlans[plan.JobID] = plan; return true, nil }); err != nil {
		t.Fatal(err)
	}
	if err := st.ReadControl(testCtx, func(data *Snapshot) error { data.ExportPlans[plan.JobID].Chunks[0].Count = 0; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateControlChecked(testCtx, func(data *Snapshot) (bool, error) {
		data.ExportPlans[plan.JobID].Chunks[0].Digest = "changed"
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.ReadControl(testCtx, func(data *Snapshot) error {
		if !reflect.DeepEqual(data.ExportPlans[plan.JobID], plan) || len(data.Events) != 0 || len(data.Receipts) != 0 {
			t.Fatal("restart/deep clone lost page references or created ledger truth")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	baseline := NewSnapshot()
	original := planMetadataFixture()
	original.Version = 2
	baseline.ExportPlans[plan.JobID] = original
	mutated, err := cloneSnapshot(baseline)
	if err != nil {
		t.Fatal(err)
	}
	mutated.AdminActions = append(mutated.AdminActions, domain.AdminAction{ID: "tenant-commit"})
	current, err := cloneSnapshot(baseline)
	if err != nil {
		t.Fatal(err)
	}
	current.ExportPlans[plan.JobID] = plan
	mergeControlDelta(current, baseline, mutated)
	if !reflect.DeepEqual(current.ExportPlans[plan.JobID], plan) || len(current.AdminActions) != 1 {
		t.Fatal("stale tenant delta erased newer generation progress")
	}
}
