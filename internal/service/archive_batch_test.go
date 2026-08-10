package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// scriptedConflictBackend keeps one in-memory Snapshot; Save fails with
// ErrSnapshotConflict `conflicts` times (without committing) before
// committing. It counts Save and LoadForUpdate calls so service-level tests
// can assert how many optimistic-lock windows a pass opens. It mirrors the
// store package's conflictBackend (conflict_retry_test.go) but is stateful:
// receipts survive retries because Save commits the mutated copy.
type scriptedConflictBackend struct {
	data      *store.Snapshot
	conflicts int // remaining conflicts before Save commits
	saves     int
	loads     int
}

func (b *scriptedConflictBackend) Load() (*store.Snapshot, error) { return b.data, nil }

func (b *scriptedConflictBackend) LoadForUpdate() (*store.Snapshot, error) {
	b.loads++
	return cloneTestSnapshot(b.data)
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

// cloneTestSnapshot deep-copies a snapshot via JSON round-trip with
// UseNumber, mirroring the store package's cloneSnapshot so payload numbers
// survive the copy without collapsing through float64.
func cloneTestSnapshot(data *store.Snapshot) (*store.Snapshot, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	copyData := store.NewSnapshot()
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(copyData); err != nil {
		return nil, err
	}
	return copyData, nil
}

// newServiceOn builds a Service over an existing store. Seeding of the test
// domain (tenant/source/schema) is separate so a store reopened from a state
// file can be wrapped without re-creating an existing tenant.
func newServiceOn(t *testing.T, st *store.Store) *Service {
	t.Helper()
	svc, err := New(st, Config{SegmentSize: 100, SigningSecret: "test-secret", Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

// seedTestDomain registers the standard tenant-a/crm/audit.event domain used
// by the archive tests.
func seedTestDomain(t *testing.T, svc *Service) {
	t.Helper()
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}
}

// testServiceWithBackend builds a Service over a scripted backend. SegmentSize
// is high so ingest never seals a segment, keeping ArchivePending's Put count
// exactly the pending-event count.
func testServiceWithBackend(t *testing.T, backend *scriptedConflictBackend) *Service {
	t.Helper()
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	return svc
}

// seedPendingEvents ingests n events for tenant-a through a failing archive
// stub so every receipt lands at StatusIndexed, ready for ArchivePending.
func seedPendingEvents(t *testing.T, svc *Service, archiveStub *recordingArchive, n int) {
	t.Helper()
	svc.Config.Archive = archiveStub
	base := time.Unix(1_700_000_010, 0).UTC()
	for i := 1; i <= n; i++ {
		receipt, err := svc.Ingest("tenant-a", crmPrincipal, testEvent(fmt.Sprintf("evt-%d", i), "op-pending", base.Add(time.Duration(i)*time.Second)), domain.StatusLedgered)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Status != domain.StatusIndexed {
			t.Fatalf("event %d status=%s, want StatusIndexed (failing archive)", i, receipt.Status)
		}
	}
}

func assertAllArchived(t *testing.T, svc *Service, n int, passTime time.Time) {
	t.Helper()
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		receipt := snap.Receipts[store.EventKey("tenant-a", fmt.Sprintf("evt-%d", i))]
		if receipt.Status != domain.StatusArchived {
			t.Fatalf("event %d status=%s, want archived", i, receipt.Status)
		}
		if receipt.IndexedAt.IsZero() || receipt.ArchivedAt.IsZero() || !receipt.IndexedAt.Equal(receipt.ArchivedAt) || !receipt.IndexedAt.Equal(passTime) {
			t.Fatalf("event %d timestamps indexed=%v archived=%v, want the single pass time %v", i, receipt.IndexedAt, receipt.ArchivedAt, passTime)
		}
	}
}

// TestArchivePendingBatchesSingleUpdate is AC-1: one ArchivePending pass
// commits every receipt in exactly one Store.Update (one optimistic-lock
// window, one full-snapshot rewrite) instead of one Update per event. The
// same atomic write resets the tenant's conflict counter, so no extra window
// is opened for the reset.
func TestArchivePendingBatchesSingleUpdate(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	archiveStub := &recordingArchive{fail: true}
	seedPendingEvents(t, svc, archiveStub, 5)
	// Simulate prior exhausted passes: the counter must be reset inside the
	// batch commit, not by a second window.
	if err := svc.RecordArchivePassConflict("tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := svc.RecordArchivePassConflict("tenant-a"); err != nil {
		t.Fatal(err)
	}

	backend.saves = 0
	archiveStub.fail = false
	archiveStub.puts = nil
	passTime := svc.Now()
	count, err := svc.ArchivePending("tenant-a")
	if err != nil || count != 5 {
		t.Fatalf("ArchivePending = %d, %v; want 5, nil", count, err)
	}
	if backend.saves != 1 {
		t.Fatalf("pass opened %d Save windows, want exactly 1", backend.saves)
	}
	if len(archiveStub.puts) != 5 {
		t.Fatalf("archive Puts = %d, want 5", len(archiveStub.puts))
	}
	assertAllArchived(t, svc, 5, passTime)
	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 0 {
		t.Fatalf("conflict counter after successful pass = %d, %v; want 0 (reset in the batch commit)", n, err)
	}
}

// TestArchivePendingConvergesWithinRetryBudget is AC-1 conflict-heavy: when
// the batch Save conflicts twice (2 of 3 attempts fail), Store.Update re-runs
// the closure on fresh snapshots and the pass converges within the bounded
// retry budget — exactly one conflict window total, not one per event.
func TestArchivePendingConvergesWithinRetryBudget(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	archiveStub := &recordingArchive{fail: true}
	seedPendingEvents(t, svc, archiveStub, 5)

	backend.conflicts = 2
	backend.saves = 0
	backend.loads = 0
	archiveStub.fail = false
	archiveStub.puts = nil
	started := time.Now()
	count, err := svc.ArchivePending("tenant-a")
	if err != nil || count != 5 {
		t.Fatalf("ArchivePending = %d, %v; want 5, nil", count, err)
	}
	if backend.saves != 3 {
		t.Fatalf("saves=%d, want 3 (initial + 2 retries)", backend.saves)
	}
	if backend.loads != 3 {
		t.Fatalf("LoadForUpdate calls=%d, want 3 (closure re-ran on fresh snapshots)", backend.loads)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("retry backoff unbounded: took %v", elapsed)
	}
	assertAllArchived(t, svc, 5, svc.Now())
}

// TestArchivePendingExhaustionAtomicAndConverges is AC-2: when the batch Save
// exhausts the retry budget, the pass fails loudly exactly once with
// ErrSnapshotConflict, commits nothing (zero receipts marked — atomicity),
// and the next pass re-Puts byte-identical content idempotently and commits.
func TestArchivePendingExhaustionAtomicAndConverges(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	archiveStub := &recordingArchive{fail: true}
	seedPendingEvents(t, svc, archiveStub, 5)

	backend.conflicts = 100 // never succeeds within the pass budget
	backend.saves = 0
	archiveStub.fail = false
	archiveStub.puts = nil
	count, err := svc.ArchivePending("tenant-a")
	if !errors.Is(err, store.ErrSnapshotConflict) {
		t.Fatalf("ArchivePending err=%v, want ErrSnapshotConflict", err)
	}
	if count != 0 {
		t.Fatalf("count=%d, want 0 on atomic abort", count)
	}
	// One window, its whole budget spent: mirrors the store package's own
	// pin (snapshotConflictRetries + 1 Save attempts).
	if backend.saves != 4 {
		t.Fatalf("saves=%d, want 4 (retry budget exhausted on a single window)", backend.saves)
	}
	if len(archiveStub.puts) != 5 {
		t.Fatalf("archive Puts = %d, want 5 (all Puts happened before the failed commit)", len(archiveStub.puts))
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if receipt := snap.Receipts[store.EventKey("tenant-a", fmt.Sprintf("evt-%d", i))]; receipt.Status == domain.StatusArchived {
			t.Fatalf("event %d marked archived by an aborted pass — partial marking must be impossible", i)
		}
	}

	// Heal the backend: the next pass re-Puts the same objects (idempotent
	// byte-identical content) and converges all receipts.
	backend.conflicts = 0
	backend.saves = 0
	archiveStub.puts = nil
	count, err = svc.ArchivePending("tenant-a")
	if err != nil || count != 5 {
		t.Fatalf("ArchivePending after heal = %d, %v; want 5, nil", count, err)
	}
	if len(archiveStub.puts) != 5 {
		t.Fatalf("re-Puts = %d, want 5", len(archiveStub.puts))
	}
	assertAllArchived(t, svc, 5, svc.Now())
}

// TestRecordArchivePassConflictIncrements is AC-3: the failure-recording
// path increments the persisted counter on a conflict-free backend and
// converges (one retry) when the increment write itself conflicts.
func TestRecordArchivePassConflictIncrements(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	for i := 1; i <= 2; i++ {
		if err := svc.RecordArchivePassConflict("tenant-a"); err != nil {
			t.Fatalf("increment %d: %v", i, err)
		}
	}
	backend.conflicts = 1
	if err := svc.RecordArchivePassConflict("tenant-a"); err != nil {
		t.Fatalf("increment with one conflict retry: %v", err)
	}
	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 3 {
		t.Fatalf("counter = %d, %v; want 3", n, err)
	}
}

// TestArchivePassConflictCounterPersistsAndResets is AC-3: the counter
// survives a store reopen from the same state path (restart-surviving) and
// is reset to zero by the next successful ArchivePending pass.
func TestArchivePassConflictCounterPersistsAndResets(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := newServiceOn(t, st)
	seedTestDomain(t, svc)
	archiveStub := &recordingArchive{fail: true}
	seedPendingEvents(t, svc, archiveStub, 5)
	if err := svc.RecordArchivePassConflict("tenant-a"); err != nil {
		t.Fatal(err)
	}
	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 1 {
		t.Fatalf("counter before reopen = %d, %v; want 1", n, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: reopen the same state file; the counter must survive.
	st2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	svc2 := newServiceOn(t, st2)
	if n, err := svc2.ArchivePassConflictFailures("tenant-a"); err != nil || n != 1 {
		t.Fatalf("counter after reopen = %d, %v; want 1 (survives restart)", n, err)
	}

	// A successful pass resets the counter inside the batch commit.
	svc2.Config.Archive = &archive.FileStore{Dir: filepath.Join(dir, "archive")}
	count, err := svc2.ArchivePending("tenant-a")
	if err != nil || count != 5 {
		t.Fatalf("ArchivePending after reopen = %d, %v; want 5, nil", count, err)
	}
	if n, err := svc2.ArchivePassConflictFailures("tenant-a"); err != nil || n != 0 {
		t.Fatalf("counter after successful pass = %d, %v; want 0", n, err)
	}
}
