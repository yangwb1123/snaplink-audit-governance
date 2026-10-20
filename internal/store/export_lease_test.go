package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

type leaseControlBackend struct {
	memoryControlDeltaBackend
	saves int
	err   error
}

func (b *leaseControlBackend) Save(ctx context.Context, data *Snapshot) error {
	b.saves++
	if b.err != nil {
		return b.err
	}
	return b.memoryControlDeltaBackend.Save(ctx, data)
}

func TestControlCheckedNoopDiscardsPrivateLeaseMutation(t *testing.T) {
	backend := &leaseControlBackend{memoryControlDeltaBackend: memoryControlDeltaBackend{data: NewSnapshot()}}
	st := NewWithBackend(backend)
	lease := ExportLease{Version: 1, JobID: "export-1", TenantID: "tenant-a", Owner: "private-owner",
		HeartbeatAt: time.Unix(1700000000, 0), ExpiresAt: time.Unix(1700000120, 0)}
	if err := st.UpdateControlChecked(testCtx, func(data *Snapshot) (bool, error) {
		data.ExportLeases[lease.JobID] = lease
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, changed := range []bool{false, true} {
		primary := errors.New("injected callback failure")
		err := st.UpdateControlChecked(testCtx, func(data *Snapshot) (bool, error) {
			data.ExportLeases[lease.JobID] = ExportLease{Owner: "wrong-owner"}
			if changed {
				return true, primary
			}
			return false, nil
		})
		if (changed && !errors.Is(err, primary)) || (!changed && err != nil) {
			t.Fatal(err)
		}
	}
	if backend.saves != 1 || backend.data.ExportLeases[lease.JobID] != lease {
		t.Fatal("no-op/error wrote or leaked a private lease mutation")
	}
	if err := st.ReadControl(testCtx, func(data *Snapshot) error {
		data.ExportLeases[lease.JobID] = ExportLease{Owner: "read-only-mutation"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if backend.data.ExportLeases[lease.JobID] != lease {
		t.Fatal("read clone shared mutable lease storage")
	}
}

func TestFileControlLeaseSurvivesCloneAndRestartWithoutEvidenceIO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	coldFailure := errors.New("cold append must not be used")
	st, err := OpenWithLedgerWriteFault(path, func() error { return coldFailure })
	if err != nil {
		t.Fatal(err)
	}
	lease := ExportLease{Version: 1, JobID: "export-1", TenantID: "tenant-a", Owner: "private-owner",
		HeartbeatAt: time.Unix(1700000000, 0), ExpiresAt: time.Unix(1700000120, 0)}
	if err := st.UpdateControlChecked(testCtx, func(data *Snapshot) (bool, error) {
		data.Exports[lease.JobID] = domain.ExportJob{ID: lease.JobID, TenantID: lease.TenantID, Status: "running"}
		data.ExportLeases[lease.JobID] = lease
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Read(testCtx, func(data *Snapshot) error {
		if data.ExportLeases[lease.JobID] != lease {
			t.Fatal("materialized clone discarded lease")
		}
		return nil
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
		if data.ExportLeases[lease.JobID] != lease || len(data.Events) != 0 || len(data.Receipts) != 0 {
			t.Fatal("lease disappeared or control became evidence")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTenantControlDeltaPreservesConcurrentLeaseRenewal(t *testing.T) {
	baseline := NewSnapshot()
	baseline.ExportLeases["job"] = ExportLease{Owner: "owner", ExpiresAt: time.Unix(1700000000, 0)}
	mutated, err := cloneSnapshot(baseline)
	if err != nil {
		t.Fatal(err)
	}
	mutated.AdminActions = append(mutated.AdminActions, domain.AdminAction{ID: "tenant-fact"})
	current, err := cloneSnapshot(baseline)
	if err != nil {
		t.Fatal(err)
	}
	renewed := current.ExportLeases["job"]
	renewed.ExpiresAt = renewed.ExpiresAt.Add(time.Minute)
	current.ExportLeases["job"] = renewed
	mergeControlDelta(current, baseline, mutated)
	if current.ExportLeases["job"] != renewed || len(current.AdminActions) != 1 {
		t.Fatal("untouched stale tenant control erased a concurrent lease renewal")
	}
}
