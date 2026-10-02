package service

// The WORM fallback bound. One derivation per service operation, taken from
// min(archiveReadTimeout, Store.OperationTimeout()), derived INSIDE the store
// closure — the placement that makes the already-declared 503 store_timeout
// reachable instead of an opaque 500.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// countingBlockingArchive black-holes every Get until its context ends and
// counts the calls it received. It is not an *archive.FileStore or
// *archive.S3Store, so archive.Configured reports it as configured and the
// counter observes real fallback reads instead of short-circuiting.
//
// settle, when set, is a short pause AFTER the read context ends. The archive
// fallback bound is min(archiveReadTimeout, Store.OperationTimeout()), so when
// the shipped ceiling is above the operation bound the two derived deadlines
// carry the SAME duration and differ only by the microseconds between
// Store.begin and the fallback derivation. Without the pause, whether
// Store.end observes op.ctx as already expired — i.e. whether the retryable
// store.ErrOperationDeadline classification is applied — would be a coin flip
// between two timer callbacks that are due at the same instant. A real remote
// read (S3, pgx) returns slightly after its context ends; the pause reproduces
// that shape instead of racing it.
type countingBlockingArchive struct {
	mu      sync.Mutex
	gets    int
	started chan struct{}
	settle  time.Duration
}

func (a *countingBlockingArchive) Put(context.Context, string, []byte) error { return nil }

func (a *countingBlockingArchive) Get(ctx context.Context, _ string) ([]byte, error) {
	a.mu.Lock()
	a.gets++
	first := a.gets == 1
	settle := a.settle
	a.mu.Unlock()
	if first && a.started != nil {
		close(a.started)
	}
	<-ctx.Done()
	if settle > 0 {
		time.Sleep(settle)
	}
	return nil, ctx.Err()
}

func (a *countingBlockingArchive) Ready(context.Context) error { return nil }

func (a *countingBlockingArchive) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.gets
}

// withArchiveReadTimeout shrinks the package-level ceiling for one test and
// restores it afterwards, so a regression that waits the full ceiling is
// visible as a hang rather than as a silent pass.
func withArchiveReadTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	original := archiveReadTimeout
	archiveReadTimeout = d
	t.Cleanup(func() { archiveReadTimeout = original })
}

// boundTestService builds a service on a real file store with the given
// per-operation bound, so Store.begin really arms op.ctx with it.
func boundTestService(t *testing.T, operationTimeout time.Duration, archiveStore *countingBlockingArchive) *Service {
	t.Helper()
	st, err := store.OpenWithOptions(filepath.Join(t.TempDir(), "state.json"), store.Options{OperationTimeout: operationTimeout})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	svc, err := New(st, Config{Archive: archiveStore, SegmentSize: 100, SigningSecret: "test-secret", Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func boundTestReceipt(eventID string) domain.EventReceipt {
	return domain.EventReceipt{TenantID: "tenant-a", StreamID: "stream-a", Sequence: 1, EventID: eventID}
}

// TestArchiveFallbackIsBoundedByStoreOperationTimeout pins AC-2 in both
// directions, and pins the placement rule that the classification depends on:
// the fallback bound is derived INSIDE the Store.Read closure, so op.ctx (armed
// earlier, with the same duration) is already expired when the fallback fails
// and Store.end yields the retryable store.ErrOperationDeadline.
func TestArchiveFallbackIsBoundedByStoreOperationTimeout(t *testing.T) {
	t.Run("store operation timeout is the tighter bound", func(t *testing.T) {
		withArchiveReadTimeout(t, 5*time.Second)
		arch := &countingBlockingArchive{settle: 10 * time.Millisecond}
		svc := boundTestService(t, 25*time.Millisecond, arch)
		var readErr error
		started := time.Now()
		err := svc.Store.Read(testCtx, func(data *store.Snapshot) error {
			_, readErr = svc.archivedEventContext(testCtx, boundTestReceipt("bound-store"))
			return readErr
		})
		elapsed := time.Since(started)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error=%v, want the fallback bound to expire", err)
		}
		if !errors.Is(err, store.ErrOperationDeadline) {
			t.Fatalf("error=%v, want store.ErrOperationDeadline so HTTP can answer 503 store_timeout", err)
		}
		if elapsed > 2*time.Second {
			t.Fatalf("fallback took %s, want the ~25ms store bound to win over the 5s ceiling", elapsed)
		}
		if got := arch.count(); got != 1 {
			t.Fatalf("archive gets=%d, want 1", got)
		}
		if got := svc.Store.OperationDeadlineExhaustions(); got != 1 {
			t.Fatalf("audit_store_operation_deadline_exceeded_total=%d, want 1", got)
		}
	})

	// Failure mode F5: archiveReadTimeout can only be the tighter bound when a
	// test shrinks it (the shipped wiring validates OperationTimeout strictly
	// below the 30s ceiling). The fallback then expires before op.ctx, so the
	// store bound was NOT exhausted and the failure must stay unclassified —
	// asserting 503 here would pin a race, not the invariant.
	t.Run("shrunken ceiling is the tighter bound and is not classified", func(t *testing.T) {
		withArchiveReadTimeout(t, 25*time.Millisecond)
		arch := &countingBlockingArchive{}
		svc := boundTestService(t, 5*time.Second, arch)
		var readErr error
		started := time.Now()
		err := svc.Store.Read(testCtx, func(data *store.Snapshot) error {
			_, readErr = svc.archivedEventContext(testCtx, boundTestReceipt("bound-ceiling"))
			return readErr
		})
		elapsed := time.Since(started)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error=%v, want the ceiling to expire", err)
		}
		if errors.Is(err, store.ErrOperationDeadline) {
			t.Fatalf("error=%v, want NO classification: the store bound was not exhausted", err)
		}
		if elapsed > 2*time.Second {
			t.Fatalf("fallback took %s, want the 25ms ceiling", elapsed)
		}
		if got := svc.Store.OperationDeadlineExhaustions(); got != 0 {
			t.Fatalf("audit_store_operation_deadline_exceeded_total=%d, want 0", got)
		}
	})

	t.Run("nil store degrades to the ceiling without panicking", func(t *testing.T) {
		withArchiveReadTimeout(t, 25*time.Millisecond)
		arch := &countingBlockingArchive{}
		// &Service{Config: ...} with a nil Store is a supported shape: the
		// -check-config path never opens one, and the existing
		// archive_read_timeout_test.go fixtures use it.
		svc := &Service{Config: Config{Archive: arch}}
		if got := svc.archiveReadBound(); got != 25*time.Millisecond {
			t.Fatalf("archiveReadBound()=%s, want the %s ceiling for a nil store", got, 25*time.Millisecond)
		}
		started := time.Now()
		if _, err := svc.archivedEventContext(context.Background(), boundTestReceipt("bound-nil-store")); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error=%v, want deadline exceeded", err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("nil-store fallback took %s, want the bounded read", elapsed)
		}
	})
}

// TestArchiveFallbackStopsReadingOnceTheBoundIsExhausted pins AC-5: the pass
// derives ONE bound for the whole read, so N archived receipts cost at most one
// bound of remote work and one Archive.Get — never N × archiveReadTimeout.
func TestArchiveFallbackStopsReadingOnceTheBoundIsExhausted(t *testing.T) {
	withArchiveReadTimeout(t, 5*time.Second)
	arch := &countingBlockingArchive{}
	svc := boundTestService(t, 100*time.Millisecond, arch)
	const receipts = 25
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		for i := 0; i < receipts; i++ {
			eventID := fmt.Sprintf("bound-evt-%02d", i)
			data.Receipts[store.EventKey("tenant-a", eventID)] = domain.EventReceipt{
				EventID: eventID, TenantID: "tenant-a", StreamID: "stream-a", Sequence: int64(i + 1),
				Status: domain.StatusArchived, Hash: "hash-" + eventID,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed archived receipts: %v", err)
	}

	var events []domain.Event
	var resolveErr error
	started := time.Now()
	err := svc.Store.Read(testCtx, func(data *store.Snapshot) error {
		events, resolveErr = svc.eventsFromSnapshot(testCtx, data, "tenant-a", func(domain.Event) bool { return true })
		return resolveErr
	})
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want the exhausted fallback bound", err)
	}
	if !errors.Is(err, store.ErrOperationDeadline) {
		t.Fatalf("error=%v, want store.ErrOperationDeadline", err)
	}
	// Fail-closed (INV-3): a fallback that cannot be completed returns no
	// partial event set.
	if events != nil {
		t.Fatalf("fail-closed read returned %d events, want none", len(events))
	}
	if got := arch.count(); got != 1 {
		t.Fatalf("archive gets=%d, want 1 of %d receipts (the bound is exhausted at the first read)", got, receipts)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("pass took %s, want the ~100ms derived bound rather than %d x bound", elapsed, receipts)
	}
}

// TestArchiveFallbackHonoursCallerCancellation pins AC-6: caller-side
// cancellation wins over both bounds and is never laundered into another
// wait. archiveReadTimeout is deliberately enlarged to 5s so a regression that
// ignores cancellation is a visible 5s hang, not a fast pass.
func TestArchiveFallbackHonoursCallerCancellation(t *testing.T) {
	t.Run("cancelled before the read issues no remote work", func(t *testing.T) {
		withArchiveReadTimeout(t, 5*time.Second)
		arch := &countingBlockingArchive{}
		// A 30s store bound and a 5s ceiling both dwarf the assertion window:
		// only cancellation can end this read quickly.
		svc := boundTestService(t, 30*time.Second, arch)
		ctx, cancel := context.WithCancel(testCtx)
		cancel()
		started := time.Now()
		_, err := svc.archivedEventContext(ctx, boundTestReceipt("cancel-before"))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v, want context.Canceled", err)
		}
		if got := arch.count(); got != 0 {
			t.Fatalf("archive gets=%d, want 0 for an already-cancelled caller", got)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("cancelled read took %s, want an immediate return", elapsed)
		}
	})

	t.Run("cancelled while the read is in flight releases it", func(t *testing.T) {
		withArchiveReadTimeout(t, 5*time.Second)
		arch := &countingBlockingArchive{started: make(chan struct{})}
		svc := boundTestService(t, 30*time.Second, arch)
		ctx, cancel := context.WithCancel(testCtx)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := svc.archivedEventContext(ctx, boundTestReceipt("cancel-inflight"))
			done <- err
		}()
		<-arch.started
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error=%v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("the in-flight archive read was not released by cancellation")
		}
	})
}

// cancellingArchive cancels the caller's context from inside the archive WRITE
// — strictly after the ledger commit — and then fails the write. It is the F1
// probe: a commit that is already durable must never be reported as rolled
// back by the client disconnect that follows it.
type cancellingArchive struct {
	cancel context.CancelFunc
}

func (a *cancellingArchive) Put(context.Context, string, []byte) error {
	a.cancel()
	return context.Canceled
}

func (a *cancellingArchive) Get(context.Context, string) ([]byte, error) { return nil, nil }

func (a *cancellingArchive) Ready(context.Context) error { return nil }

// TestIngestSurvivesCallerCancellationDuringArchiveWrite pins AC-8: the commit
// closure and the post-commit receipt transition still run on the detached
// context, so a caller that withdraws after the ledger commit leaves a durable
// receipt and is told nothing about a rollback. The fallback change adds a
// deadline at the archived read only; it adds no rollback path.
func TestIngestSurvivesCallerCancellationDuringArchiveWrite(t *testing.T) {
	for _, hot := range []bool{false, true} {
		hot := hot
		t.Run(fmt.Sprintf("hotcold=%v", hot), func(t *testing.T) {
			svc := testService(t, hot)
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			svc.Config.Archive = &cancellingArchive{cancel: cancel}
			at := time.Unix(1_700_000_010, 0).UTC()
			receipt, err := svc.Ingest(ctx, "tenant-a", crmPrincipal, testEvent("cancel-write-1", "cancel-op", at), domain.StatusLedgered)
			if err != nil {
				t.Fatalf("a cancelled caller must not roll back a durable commit: %v", err)
			}
			if errors.Is(err, context.Canceled) {
				t.Fatal("context.Canceled must never be reported as a rollback")
			}
			if receipt.EventID != "cancel-write-1" || receipt.Status == domain.StatusAccepted {
				t.Fatalf("receipt=%+v, want the committed event, not an accepted-only stub", receipt)
			}
			stored, err := svc.GetReceipt(testCtx, "tenant-a", "", "cancel-write-1")
			if err != nil {
				t.Fatalf("durable receipt must survive the cancellation: %v", err)
			}
			if stored.Status != domain.StatusIndexed {
				t.Fatalf("durable receipt=%+v, want indexed (the archive write was the part that lost)", stored)
			}
			assertOwnerReceiptClean(t, svc, hot, "cancel-write-1")
		})
	}
}
