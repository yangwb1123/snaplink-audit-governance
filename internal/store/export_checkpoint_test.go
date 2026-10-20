package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func checkpointMetadataFixture() ExportCheckpoint {
	return ExportCheckpoint{Version: 1, SealVersion: 2, JobID: "job", TenantID: "tenant-a", Binding: "keyed-fixture-binding", ObjectPath: "exports/opaque-fixture.jsonl", SealedDigest: "sealed-fixture-hash", Digest: "plain-fixture-hash", EventCount: 1, CreatedAt: time.Unix(1700000000, 0).UTC(), Signature: "fixture-mac"}
}

func TestExportCheckpointControlCloneRestartAndTenantSeparation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := OpenWithLedgerWriteFault(path, func() error { return errors.New("checkpoint must not write ledger") })
	if err != nil {
		t.Fatal(err)
	}
	cp := checkpointMetadataFixture()
	if err := st.UpdateControlChecked(testCtx, func(data *Snapshot) (bool, error) {
		data.Exports[cp.JobID] = domain.ExportJob{ID: cp.JobID, TenantID: cp.TenantID, Status: "running"}
		data.ExportCheckpoints[cp.JobID] = cp
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.ReadControl(testCtx, func(data *Snapshot) error {
		data.ExportCheckpoints[cp.JobID] = ExportCheckpoint{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateControlChecked(testCtx, func(data *Snapshot) (bool, error) {
		delete(data.ExportCheckpoints, cp.JobID)
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Read(testCtx, func(data *Snapshot) error {
		if data.ExportCheckpoints[cp.JobID] != cp {
			t.Fatal("clone/no-op mutated durable checkpoint")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	hot, err := json.Marshal(emptyTenantHot(cp.TenantID))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(hot, []byte("export_checkpoints")) {
		t.Fatal("private control metadata escaped into tenant hot state")
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
		if data.ExportCheckpoints[cp.JobID] != cp || len(data.Events) != 0 || len(data.Receipts) != 0 {
			t.Fatal("restart lost control checkpoint or copied evidence")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestExportCheckpointTenantDeltaPreservesConcurrentCreationAndRemoval(t *testing.T) {
	for _, removed := range []bool{false, true} {
		baseline := NewSnapshot()
		if removed {
			baseline.ExportCheckpoints["job"] = checkpointMetadataFixture()
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
			delete(current.ExportCheckpoints, "job")
		} else {
			current.ExportCheckpoints["job"] = checkpointMetadataFixture()
		}
		mergeControlDelta(current, baseline, mutated)
		_, exists := current.ExportCheckpoints["job"]
		if exists == removed || len(current.AdminActions) != 1 {
			t.Fatal("stale tenant delta erased or resurrected a checkpoint")
		}
	}
}

func TestExportCheckpointCannotMasqueradeAsEmptyMigrationBaseline(t *testing.T) {
	data := NewSnapshot()
	data.LayoutVersion = hotColdLayoutVersion
	if !isPostgresHotColdZeroBaseline(data) {
		t.Fatal("empty baseline changed")
	}
	data.ExportCheckpoints["job"] = checkpointMetadataFixture()
	if isPostgresHotColdZeroBaseline(data) {
		t.Fatal("checkpoint-bearing history was treated as a fresh migration")
	}
}
