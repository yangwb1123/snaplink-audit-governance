package main

import (
	"bytes"
	"encoding/json"
	"log"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// scriptedConflictBackend is the worker-test counterpart of the service
// package's double: LoadForUpdate returns a deep copy (a failed Save must
// not leak closure mutations into the committed snapshot), Save fails with
// ErrSnapshotConflict while `conflicts` remain without committing, then
// commits. Save calls are counted so tests can prove whether the counter
// write happened at all.
type scriptedConflictBackend struct {
	data      *store.Snapshot
	conflicts int // remaining conflicts before Save commits
	saves     int
}

func (b *scriptedConflictBackend) Load() (*store.Snapshot, error) { return b.data, nil }

func (b *scriptedConflictBackend) LoadForUpdate() (*store.Snapshot, error) {
	encoded, err := json.Marshal(b.data)
	if err != nil {
		return nil, err
	}
	copyData := store.NewSnapshot()
	if err := json.Unmarshal(encoded, copyData); err != nil {
		return nil, err
	}
	return copyData, nil
}

func (b *scriptedConflictBackend) Save(data *store.Snapshot) error {
	b.saves++
	if b.conflicts > 0 {
		b.conflicts--
		return store.ErrSnapshotConflict
	}
	b.data = data
	return nil
}

// workerService builds a Service over the scripted backend; the conflict
// counter needs no tenant domain (tenant IDs are plain map keys).
func workerService(t *testing.T, backend *scriptedConflictBackend) *service.Service {
	t.Helper()
	svc, err := service.New(store.NewWithBackend(backend), service.Config{SigningSecret: "test-secret", AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestHandleArchiveErrorPersistsExhaustedConflict(t *testing.T) {
	// An exhausted ErrSnapshotConflict pass must increment the persisted
	// counter and log it as conflict_failures=1.
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	handleArchiveError(logger, svc, "tenant-a", 0, store.ErrSnapshotConflict)

	if backend.saves != 1 {
		t.Fatalf("Save calls=%d, want 1 (one counter-increment window)", backend.saves)
	}
	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 1 {
		t.Fatalf("counter=%d, %v; want 1", n, err)
	}
	out := buf.String()
	if !strings.Contains(out, "archive_error=state snapshot changed concurrently") || !strings.Contains(out, "conflict_failures=1") {
		t.Fatalf("log line missing exhaustion signal, got: %q", out)
	}
}

func TestHandleArchiveErrorSkipsNonConflictErrors(t *testing.T) {
	// A non-conflict pass error must not record anything: the counter stays
	// untouched and no counter write window is opened.
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	handleArchiveError(logger, svc, "tenant-a", 3, domain.ErrInvalid)

	if backend.saves != 0 {
		t.Fatalf("Save calls=%d, want 0 (no counter write for a non-conflict error)", backend.saves)
	}
	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 0 {
		t.Fatalf("counter=%d, %v; want 0 untouched", n, err)
	}
	out := buf.String()
	if !strings.Contains(out, "archive_error=invalid") || !strings.Contains(out, "archived=3") || !strings.Contains(out, "conflict_failures=0") {
		t.Fatalf("log line malformed, got: %q", out)
	}
	if strings.Contains(out, "archive_conflict_record_error") {
		t.Fatalf("record error logged for a non-conflict error, got: %q", out)
	}
}

func TestHandleArchiveErrorCounterWriteFailureDoesNotMaskPassError(t *testing.T) {
	// When even the counter increment exhausts under sustained load, the
	// record error is logged separately, the log still reports the original
	// pass error, and the counter reads back at its previous value.
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	backend.conflicts = 100 // the increment write itself can never commit
	handleArchiveError(logger, svc, "tenant-a", 0, store.ErrSnapshotConflict)

	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 0 {
		t.Fatalf("counter=%d, %v; want 0 (increment failed atomically)", n, err)
	}
	out := buf.String()
	if !strings.Contains(out, "archive_conflict_record_error") {
		t.Fatalf("counter-write failure not logged, got: %q", out)
	}
	if !strings.Contains(out, "archive_error=state snapshot changed concurrently") || !strings.Contains(out, "conflict_failures=0") {
		t.Fatalf("original pass error or counter missing from log, got: %q", out)
	}
}
