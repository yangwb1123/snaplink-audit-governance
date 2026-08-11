package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

// conflictBackend is a Backend that fails Save with ErrSnapshotConflict a
// fixed number of times before succeeding, so Update's bounded retry loop
// can be exercised without PostgreSQL.
type conflictBackend struct {
	conflicts int // remaining conflicts before Save succeeds
	saves     int
	loads     int
}

func (b *conflictBackend) Load() (*Snapshot, error) { return NewSnapshot(), nil }

func (b *conflictBackend) LoadForUpdate() (*Snapshot, error) {
	b.loads++
	return NewSnapshot(), nil
}

func (b *conflictBackend) Save(data *Snapshot) error {
	b.saves++
	if b.conflicts > 0 {
		b.conflicts--
		return ErrSnapshotConflict
	}
	return nil
}

// TestUpdateRetriesSnapshotConflictWithBoundedBackoff pins the F-01 contract:
// a concurrent writer's optimistic-lock conflict is retried in place (closure
// re-run on the fresh snapshot) with bounded jitter, and the update converges
// instead of surfacing a bare error.
func TestUpdateRetriesSnapshotConflictWithBoundedBackoff(t *testing.T) {
	backend := &conflictBackend{conflicts: 2}
	s := &Store{backend: backend}
	var runs int
	started := time.Now()
	err := s.Update(func(data *Snapshot) error {
		runs++
		data.Tenants["t1"] = domain.Tenant{ID: "t1", Name: "T1", Active: true}
		return nil
	})
	if err != nil {
		t.Fatalf("Update after transient conflicts = %v, want nil", err)
	}
	if runs != 3 {
		t.Fatalf("closure runs=%d, want 3 (initial + 2 retries)", runs)
	}
	if backend.saves != 3 {
		t.Fatalf("saves=%d, want 3", backend.saves)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("retry backoff unbounded: took %v", elapsed)
	}
}

// TestUpdateConflictExhaustionReturnsErrSnapshotConflict pins the fail-loud
// boundary: once the bounded retries are exhausted the conflict is returned
// to the caller (mapped to 503 by the HTTP layer) instead of being swallowed.
func TestUpdateConflictExhaustionReturnsErrSnapshotConflict(t *testing.T) {
	backend := &conflictBackend{conflicts: snapshotConflictRetries + 5}
	s := &Store{backend: backend}
	err := s.Update(func(data *Snapshot) error {
		data.Tenants["t1"] = domain.Tenant{ID: "t1", Name: "T1", Active: true}
		return nil
	})
	if !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("Update exhausted err=%v, want ErrSnapshotConflict", err)
	}
	if backend.saves != snapshotConflictRetries+1 {
		t.Fatalf("saves=%d, want %d", backend.saves, snapshotConflictRetries+1)
	}
}

// TestUpdateDoesNotRetryClosureErrors pins the retry boundary: an error from
// the closure itself (validation, conflict, not-found) must surface
// immediately — it is not an optimistic-lock condition and must never be
// re-run.
func TestUpdateDoesNotRetryClosureErrors(t *testing.T) {
	backend := &conflictBackend{}
	s := &Store{backend: backend}
	sentinel := errors.New("closure failed")
	var runs int
	err := s.Update(func(data *Snapshot) error {
		runs++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Update closure err=%v, want sentinel", err)
	}
	if runs != 1 || backend.saves != 0 {
		t.Fatalf("closure retried: runs=%d saves=%d, want 1/0", runs, backend.saves)
	}
}

// TestUpdateCheckedNoSaveOnCleanClosure pins the FR-2 primitive contract: a
// closure reporting no mutation must not reach the backend Save at all (no
// version bump, no updated_at, no byte change).
func TestUpdateCheckedNoSaveOnCleanClosure(t *testing.T) {
	backend := &conflictBackend{}
	s := &Store{backend: backend}
	if err := s.UpdateChecked(func(data *Snapshot) (bool, error) {
		data.Tenants["t1"] = domain.Tenant{ID: "t1", Name: "T1", Active: true} // must be discarded
		return false, nil
	}); err != nil {
		t.Fatalf("UpdateChecked clean closure = %v, want nil", err)
	}
	if backend.saves != 0 {
		t.Fatalf("saves=%d, want 0 (clean closure must not persist)", backend.saves)
	}
	if backend.loads != 1 {
		t.Fatalf("loads=%d, want 1", backend.loads)
	}
}

// TestUpdateCheckedSavesOnMutation pins the mutating branch: a closure
// reporting mutation persists exactly once.
func TestUpdateCheckedSavesOnMutation(t *testing.T) {
	backend := &conflictBackend{}
	s := &Store{backend: backend}
	if err := s.UpdateChecked(func(data *Snapshot) (bool, error) {
		data.Tenants["t1"] = domain.Tenant{ID: "t1", Name: "T1", Active: true}
		return true, nil
	}); err != nil {
		t.Fatalf("UpdateChecked mutation = %v, want nil", err)
	}
	if backend.saves != 1 {
		t.Fatalf("saves=%d, want 1", backend.saves)
	}
}

// TestUpdateCheckedRetriesConflicts pins that UpdateChecked keeps Update's
// optimistic-lock semantics: conflicts re-run the closure on fresh snapshots
// with the same bounded budget.
func TestUpdateCheckedRetriesConflicts(t *testing.T) {
	backend := &conflictBackend{conflicts: 2}
	s := &Store{backend: backend}
	var runs int
	if err := s.UpdateChecked(func(data *Snapshot) (bool, error) {
		runs++
		data.Tenants["t1"] = domain.Tenant{ID: "t1", Name: "T1", Active: true}
		return true, nil
	}); err != nil {
		t.Fatalf("UpdateChecked after transient conflicts = %v, want nil", err)
	}
	if runs != 3 || backend.saves != 3 {
		t.Fatalf("runs=%d saves=%d, want 3/3 (initial + 2 retries)", runs, backend.saves)
	}
}

// TestUpdateCheckedCommittingRunDecidesPersistence pins the contract that
// the mutated flag of the run that commits decides persistence: a mutating
// first attempt that loses the race is discarded when the retried closure
// reports no mutation.
func TestUpdateCheckedCommittingRunDecidesPersistence(t *testing.T) {
	backend := &conflictBackend{conflicts: 1}
	s := &Store{backend: backend}
	var runs int
	if err := s.UpdateChecked(func(data *Snapshot) (bool, error) {
		runs++
		data.Tenants["t1"] = domain.Tenant{ID: "t1", Name: "T1", Active: true}
		return runs == 1, nil // first attempt mutates, retry reports a no-op
	}); err != nil {
		t.Fatalf("UpdateChecked = %v, want nil", err)
	}
	if backend.saves != 1 {
		t.Fatalf("saves=%d, want 1 (only the failed attempt; retry skipped the Save)", backend.saves)
	}
	if runs != 2 {
		t.Fatalf("runs=%d, want 2", runs)
	}
}

// TestUpdateCheckedDoesNotRetryClosureErrors pins the retry boundary for the
// checked variant: closure errors surface immediately and never retry.
func TestUpdateCheckedDoesNotRetryClosureErrors(t *testing.T) {
	backend := &conflictBackend{}
	s := &Store{backend: backend}
	sentinel := errors.New("closure failed")
	var runs int
	err := s.UpdateChecked(func(data *Snapshot) (bool, error) {
		runs++
		return false, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("UpdateChecked closure err=%v, want sentinel", err)
	}
	if runs != 1 || backend.saves != 0 {
		t.Fatalf("closure retried: runs=%d saves=%d, want 1/0", runs, backend.saves)
	}
}

// TestSnapshotConflictBackoffBounded pins the jitter envelope: every retry
// delay stays within [5ms, 30ms] so concurrent replicas cannot stampede and
// the total retry budget is small.
func TestSnapshotConflictBackoffBounded(t *testing.T) {
	for attempt := 0; attempt < 10; attempt++ {
		delay := snapshotConflictBackoff(attempt)
		if delay < 5*time.Millisecond || delay > 30*time.Millisecond {
			t.Fatalf("attempt=%d backoff=%v outside [5ms, 30ms]", attempt, delay)
		}
	}
}

// TestStoreReadyProbesBackend pins the readyz contract: file-backed stores
// are always ready; a backend with a failing probe reports the failure.
func TestStoreReadyProbesBackend(t *testing.T) {
	fileStore, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	if err := fileStore.Ready(context.Background()); err != nil {
		t.Fatalf("file store Ready = %v, want nil", err)
	}
	failing := &conflictBackend{}
	probeStore := &Store{backend: readyProbeBackend{Backend: failing, err: errors.New("postgres down")}}
	if err := probeStore.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), "postgres down") {
		t.Fatalf("probe store Ready = %v, want postgres down", err)
	}
}

type readyProbeBackend struct {
	Backend
	err error
}

func (b readyProbeBackend) Ready(context.Context) error { return b.err }

// TestFileBackendSaveFsyncAtomicity pins the B1-2 fsync acceptance: Save
// leaves a durable regular file at the target path (no .tmp residue), the
// persisted bytes are the current snapshot, and the mode is the documented
// 0640.
func TestFileBackendSaveFsyncAtomicity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	backend, err := openFileBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Save(NewSnapshot()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("state file missing: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file left behind after Save")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o640 {
		t.Fatalf("state file mode=%o, want 640", got)
	}
	// A subsequent Save replaces the file atomically (same inode path).
	if err := backend.Save(NewSnapshot()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file left behind after second Save")
	}
}
