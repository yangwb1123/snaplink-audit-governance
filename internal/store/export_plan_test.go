package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func planMetadataFixture() ExportPlan {
	return ExportPlan{Version: 1, FormatVersion: 1, JobID: "job", TenantID: "tenant-a", Binding: "keyed-binding", CreatedAt: time.Unix(1700000000, 0).UTC(),
		Members: []ExportPlanMember{{EventReference: EventReference{EventID: "event", StreamID: "stream", Sequence: 1, Hash: "coordinate-hash"},
			SchemaID: "schema", SchemaVersion: 1, SchemaDigest: "keyed-schema-digest"}}, Signature: "plan-mac"}
}

func TestExportPlanControlDeepCloneRestartAndTenantSeparation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := OpenWithLedgerWriteFault(path, func() error { return errors.New("plan must not append ledger facts") })
	if err != nil {
		t.Fatal(err)
	}
	plan := planMetadataFixture()
	if err := st.UpdateControlChecked(testCtx, func(data *Snapshot) (bool, error) {
		data.ExportPlans[plan.JobID] = plan
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Snapshot){
		func(data *Snapshot) { data.ExportPlans[plan.JobID].Members[0].SchemaVersion = 99 },
		func(data *Snapshot) { delete(data.ExportPlans, plan.JobID) },
	} {
		if err := st.ReadControl(testCtx, func(data *Snapshot) error { mutate(data); return nil }); err != nil {
			t.Fatal(err)
		}
		if err := st.UpdateControlChecked(testCtx, func(data *Snapshot) (bool, error) { mutate(data); return false, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Read(testCtx, func(data *Snapshot) error {
		if !reflect.DeepEqual(data.ExportPlans[plan.JobID], plan) {
			t.Fatal("read/no-op aliases changed durable plan membership")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	hot, err := json.Marshal(emptyTenantHot(plan.TenantID))
	if err != nil || bytes.Contains(hot, []byte("export_plans")) {
		t.Fatal("private membership escaped into tenant hot state")
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
			t.Fatal("restart lost plan or copied ledger/payload state")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestExportPlanTenantDeltaCannotEraseOrResurrectConcurrentPlan(t *testing.T) {
	for _, removed := range []bool{false, true} {
		baseline := NewSnapshot()
		if removed {
			baseline.ExportPlans["job"] = planMetadataFixture()
		}
		mutated, err := cloneSnapshot(baseline)
		if err != nil {
			t.Fatal(err)
		}
		mutated.AdminActions = append(mutated.AdminActions, domain.AdminAction{ID: "tenant-write"})
		current, err := cloneSnapshot(baseline)
		if err != nil {
			t.Fatal(err)
		}
		if removed {
			delete(current.ExportPlans, "job")
		} else {
			current.ExportPlans["job"] = planMetadataFixture()
		}
		mergeControlDelta(current, baseline, mutated)
		_, exists := current.ExportPlans["job"]
		if exists == removed || len(current.AdminActions) != 1 {
			t.Fatal("stale tenant delta erased or resurrected selected stage")
		}
	}
}

func TestExportPlanIsNotEmptyMigrationBaseline(t *testing.T) {
	data := NewSnapshot()
	data.LayoutVersion = hotColdLayoutVersion
	data.ExportPlans["job"] = planMetadataFixture()
	if isPostgresHotColdZeroBaseline(data) {
		t.Fatal("plan-bearing history masqueraded as empty migration")
	}
}
