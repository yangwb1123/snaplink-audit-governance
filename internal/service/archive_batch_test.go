package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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
	// saveErr, when set, makes Save return a non-conflict error immediately
	// (no retries, no sleeps). It is a mutable field so fail-closed tests can
	// seed through a clean backend and arm the append failure afterwards —
	// arming at construction would fail the seeding writes themselves.
	saveErr error
	// alwaysConflict makes Save fail with ErrSnapshotConflict forever, so
	// Update's bounded retry loop exhausts regardless of the unexported
	// snapshotConflictRetries value. Mutable for the same seed-then-arm
	// reason.
	alwaysConflict bool
}

func (b *scriptedConflictBackend) Load() (*store.Snapshot, error) { return b.data, nil }

func (b *scriptedConflictBackend) LoadForUpdate() (*store.Snapshot, error) {
	b.loads++
	return cloneTestSnapshot(b.data)
}

func (b *scriptedConflictBackend) Save(data *store.Snapshot) error {
	b.saves++
	if b.saveErr != nil {
		return b.saveErr
	}
	if b.alwaysConflict || b.conflicts > 0 {
		if b.conflicts > 0 {
			b.conflicts--
		}
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
		receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent(fmt.Sprintf("evt-%d", i), "op-pending", base.Add(time.Duration(i)*time.Second)), domain.StatusLedgered)
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
	count, err := svc.ArchivePending(context.Background(), "tenant-a")
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
	count, err := svc.ArchivePending(context.Background(), "tenant-a")
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
	count, err := svc.ArchivePending(context.Background(), "tenant-a")
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
	count, err = svc.ArchivePending(context.Background(), "tenant-a")
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
	count, err := svc2.ArchivePending(context.Background(), "tenant-a")
	if err != nil || count != 5 {
		t.Fatalf("ArchivePending after reopen = %d, %v; want 5, nil", count, err)
	}
	if n, err := svc2.ArchivePassConflictFailures("tenant-a"); err != nil || n != 0 {
		t.Fatalf("counter after successful pass = %d, %v; want 0", n, err)
	}
}

// TestArchivePendingMissingReceiptAbortsAtomically pins the whole-batch abort
// on a missing receipt (FM-3): the closure's ErrNotFound aborts the pass
// before any Save, so zero receipts are marked and the counter is untouched.
// No receipt-deletion path exists in the service; the corrupted-snapshot
// case is simulated directly through the store.
func TestArchivePendingMissingReceiptAbortsAtomically(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	archiveStub := &recordingArchive{fail: true}
	seedPendingEvents(t, svc, archiveStub, 5)
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		delete(data.Receipts, store.EventKey("tenant-a", "evt-3"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	backend.saves = 0
	archiveStub.fail = false
	archiveStub.puts = nil
	count, err := svc.ArchivePending(context.Background(), "tenant-a")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("ArchivePending err=%v, want ErrNotFound", err)
	}
	if count != 0 {
		t.Fatalf("count=%d, want 0 (whole-batch abort)", count)
	}
	if backend.saves != 0 {
		t.Fatalf("saves=%d, want 0 (closure error commits nothing)", backend.saves)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		receipt, ok := snap.Receipts[store.EventKey("tenant-a", fmt.Sprintf("evt-%d", i))]
		if i == 3 {
			if ok {
				t.Fatalf("event 3 receipt still present, want deleted")
			}
			continue
		}
		if !ok || receipt.Status != domain.StatusIndexed {
			t.Fatalf("event %d receipt ok=%v status=%+v, want untouched StatusIndexed", i, ok, receipt.Status)
		}
	}
	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 0 {
		t.Fatalf("counter = %d, %v; want 0 untouched", n, err)
	}
}

// TestArchivePendingEmptyPassLeavesCounter pins the zero-event semantics: a
// pass with nothing to mark opens no window and must not reset the counter
// (it counts consecutive passes aborted by exhaustion; an empty pass cannot
// have been aborted).
func TestArchivePendingEmptyPassLeavesCounter(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	if err := svc.RecordArchivePassConflict("tenant-a"); err != nil {
		t.Fatal(err)
	}
	svc.Config.Archive = &recordingArchive{}

	backend.saves = 0
	count, err := svc.ArchivePending(context.Background(), "tenant-a")
	if err != nil || count != 0 {
		t.Fatalf("ArchivePending = %d, %v; want 0, nil", count, err)
	}
	if backend.saves != 0 {
		t.Fatalf("saves=%d, want 0 (empty pass opens no window)", backend.saves)
	}
	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 1 {
		t.Fatalf("counter = %d, %v; want 1 (empty pass must not reset)", n, err)
	}
}

// TestArchivePendingPutFailureReturnsZeroWithoutWindow pins the mid-loop Put
// failure contract: the pass aborts on the first Put failure with count 0,
// no receipts are marked and no Update window was opened (Put happens before
// the batch commit).
func TestArchivePendingPutFailureReturnsZeroWithoutWindow(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	archiveStub := &recordingArchive{fail: true}
	seedPendingEvents(t, svc, archiveStub, 5)
	// The stub stays failing: the pass's Put loop aborts on the first event.
	archiveStub.puts = nil
	backend.saves = 0

	count, err := svc.ArchivePending(context.Background(), "tenant-a")
	if err == nil {
		t.Fatal("ArchivePending err=nil, want Put failure")
	}
	if count != 0 {
		t.Fatalf("count=%d, want 0 (no receipts committed on Put failure)", count)
	}
	if backend.saves != 0 {
		t.Fatalf("saves=%d, want 0 (no window before all Puts succeed)", backend.saves)
	}
	if len(archiveStub.puts) != 1 {
		t.Fatalf("pass Puts = %d, want 1 (aborts on the first event)", len(archiveStub.puts))
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if receipt := snap.Receipts[store.EventKey("tenant-a", fmt.Sprintf("evt-%d", i))]; receipt.Status == domain.StatusArchived {
			t.Fatalf("event %d marked archived despite Put failure", i)
		}
	}
}

// ctxErrorArchive is a Store stub whose Put returns the given error until
// heal is set, then succeeds; it counts Puts. It pins AC-3's transient
// classification: a cancelled/timed-out Put (what a signal-cancelled pass
// produces) must not dead-letter or commit anything, and the same events
// must archive on the next pass.
type ctxErrorArchive struct {
	err  error
	heal bool
	puts int
}

func (c *ctxErrorArchive) Put(context.Context, string, []byte) error {
	c.puts++
	if c.heal {
		return nil
	}
	return c.err
}

func (c *ctxErrorArchive) Get(context.Context, string) ([]byte, error) { return nil, nil }

func (c *ctxErrorArchive) Ready(context.Context) error { return nil }

// TestArchivePendingContextErrorIsTransientAndRetried is AC-3: a cancelled
// Put (context.Canceled) is transient by isPermanentArchiveError
// classification — ArchivePending returns (0, err), zero receipts are marked
// StatusArchived, zero dead letters are recorded, and no Store.Update window
// is opened (scriptedConflictBackend.saves probe). The next pass over the
// healed store archives the same events, so retry semantics are preserved
// end-to-end (idempotency tests stay green).
func TestArchivePendingContextErrorIsTransientAndRetried(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	archiveStub := &recordingArchive{fail: true}
	seedPendingEvents(t, svc, archiveStub, 5)
	ctxErr := &ctxErrorArchive{err: context.Canceled}
	svc.Config.Archive = ctxErr

	backend.saves = 0
	count, err := svc.ArchivePending(context.Background(), "tenant-a")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ArchivePending err=%v, want context.Canceled", err)
	}
	if count != 0 {
		t.Fatalf("count=%d, want 0 (nothing committed on a transient abort)", count)
	}
	if backend.saves != 0 {
		t.Fatalf("saves=%d, want 0 (no Store.Update window on a transient abort)", backend.saves)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if receipt := snap.Receipts[store.EventKey("tenant-a", fmt.Sprintf("evt-%d", i))]; receipt.Status == domain.StatusArchived {
			t.Fatalf("event %d marked archived despite a cancelled Put", i)
		}
	}
	if len(snap.DeadLetters) != 0 {
		t.Fatalf("dead letters=%d, want 0 (ctx errors are transient, never dead-lettered)", len(snap.DeadLetters))
	}

	// Heal: the next pass archives the same events — retry preserved.
	ctxErr.heal = true
	backend.saves = 0
	count, err = svc.ArchivePending(context.Background(), "tenant-a")
	if err != nil || count != 5 {
		t.Fatalf("ArchivePending after heal = %d, %v; want 5, nil", count, err)
	}
	if backend.saves != 1 {
		t.Fatalf("saves=%d, want 1 (one atomic batch window on the healed pass)", backend.saves)
	}
	assertAllArchived(t, svc, 5, svc.Now())
}

// exportBlockingArchive is a Store stub whose Put blocks until the passed
// context is done (mirroring minio-go's per-request context honoring) and
// returns the context error; when heal is set, Put succeeds instantly. It
// records the context error the Put observed, for asserting the runExport
// per-write bound reached the store (REQ-3).
type exportBlockingArchive struct {
	heal    bool
	lastErr error
}

func (e *exportBlockingArchive) Put(ctx context.Context, _ string, _ []byte) error {
	if e.heal {
		return nil
	}
	<-ctx.Done()
	e.lastErr = ctx.Err()
	return ctx.Err()
}

func (e *exportBlockingArchive) Get(context.Context, string) ([]byte, error) { return nil, nil }

func (e *exportBlockingArchive) Ready(context.Context) error { return nil }

// TestRunExportPutTimeoutFailsJobAndHeals is REQ-3 acceptance (QA review
// F-1): runExport's fire-and-forget Put carries its own per-write bound
// (archivePutTimeout, var seam shrunk here). A store whose Put blocks past
// the bound fails the job with context deadline exceeded, ObjectPath empty
// (no object path recorded); a healed re-request — a fresh CreateExport —
// completes normally.
func TestRunExportPutTimeoutFailsJobAndHeals(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	base := time.Unix(1_700_000_010, 0).UTC()
	// Ingest with no archive configured: the receipt lands ledgered, and the
	// event is selectable by the export regardless of receipt status.
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("exp-timeout-1", "op-exp", base), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	archiveStub := &exportBlockingArchive{}
	svc.Config.Archive = archiveStub
	original := archivePutTimeout
	archivePutTimeout = 50 * time.Millisecond
	t.Cleanup(func() { archivePutTimeout = original })
	query := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC()}

	job, err := svc.CreateExport("tenant-a", "compliance-1", query)
	if err != nil {
		t.Fatal(err)
	}
	failed := waitExportCompleted(t, svc, job.ID)
	if failed.Status != "failed" {
		t.Fatalf("export status=%s, want failed on a timed-out Put", failed.Status)
	}
	if failed.ObjectPath != "" {
		t.Fatalf("ObjectPath=%q, want empty on failure", failed.ObjectPath)
	}
	if !strings.Contains(failed.Error, "context deadline exceeded") {
		t.Fatalf("export error=%q, want context deadline exceeded", failed.Error)
	}
	if !errors.Is(archiveStub.lastErr, context.DeadlineExceeded) {
		t.Fatalf("store observed ctx error=%v, want context.DeadlineExceeded", archiveStub.lastErr)
	}

	// Healed re-request: a fresh CreateExport completes normally.
	archiveStub.heal = true
	job2, err := svc.CreateExport("tenant-a", "compliance-1", query)
	if err != nil {
		t.Fatal(err)
	}
	completed := waitExportCompleted(t, svc, job2.ID)
	if completed.Status != "completed" {
		t.Fatalf("healed export status=%s, want completed", completed.Status)
	}
	if completed.ObjectPath == "" {
		t.Fatal("healed export must set ObjectPath")
	}
}
