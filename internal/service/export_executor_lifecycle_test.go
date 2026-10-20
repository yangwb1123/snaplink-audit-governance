package service

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Gate the actual final backend Load, after the executor has stopped and
// joined its heartbeat. The existing instance-local tick seam only arms this
// observation at stop; production intervals, claim/Save/Put and join stay intact.
// LoadForUpdate deliberately retains the embedded backend's normal behavior.
// The gate is instance-local and never holds the backend mutex while waiting.
type schedulerTailBackend struct {
	*checkpointBackend
	armed          atomic.Bool
	gated          atomic.Bool
	loadsAfterStop atomic.Int64
	entered        chan context.Context
	release        chan struct{}
	exited         chan struct{}
	readError      error
	waitDeadline   bool
}

func (b *schedulerTailBackend) Load(ctx context.Context) (*store.Snapshot, error) {
	if b.armed.Load() {
		b.loadsAfterStop.Add(1)
		if b.gated.CompareAndSwap(false, true) {
			defer close(b.exited)
			b.entered <- ctx
			if b.waitDeadline {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			select {
			case <-b.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if b.readError != nil {
				return nil, b.readError
			}
		}
	}
	return b.checkpointBackend.Load(ctx)
}

func waitSchedulerBoundary(t *testing.T, ready func() bool) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !ready() {
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatal("owned scheduler did not reach its actual lifecycle boundary")
		}
	}
}

func TestExportExecutorReservationCoversFinalControlRead(t *testing.T) {
	for _, mode := range []string{"completed", "failed", "cancelled", "retained-sealed", "read-error", "caller-cancel", "read-deadline"} {
		t.Run(mode, func(t *testing.T) {
			base := &checkpointBackend{data: store.NewSnapshot()}
			backend := &schedulerTailBackend{checkpointBackend: base, entered: make(chan context.Context, 1),
				release: make(chan struct{}), exited: make(chan struct{}), waitDeadline: mode == "read-deadline"}
			if mode == "read-error" {
				backend.readError = errors.New("injected final scheduler control read failure")
			}
			opts := store.Options{}
			if mode == "read-deadline" {
				opts.OperationTimeout = 150 * time.Millisecond
			}
			svc := newServiceOn(t, store.NewWithBackendOptions(backend, opts))
			seedTestDomain(t, svc)
			seedLeaseExport(t, svc, "tail-job", "tenant-a")
			objects := newConsoleMemoryArchive()
			paused := &schedulerPausedArchive{Store: objects, entered: make(chan string, 1), release: make(chan struct{})}
			if mode == "failed" {
				paused.Store = &recordingArchive{fail: true}
			}
			svc.Config.Archive = paused
			svc.exportHeartbeat = func() (<-chan time.Time, func()) {
				return make(chan time.Time), func() { backend.armed.Store(true) }
			}
			if mode == "retained-sealed" {
				base.failTerminal.Store(true)
			}
			ctx, cancel := context.WithCancel(testCtx)
			var putOnce, tailOnce sync.Once
			releasePut := func() { putOnce.Do(func() { close(paused.release) }) }
			releaseTail := func() { tailOnce.Do(func() { close(backend.release) }) }
			defer func() {
				releasePut()
				releaseTail()
				cancel()
				awaitSchedulerFixtureIdle(t, svc)
				if err := svc.Store.Close(); err != nil {
					t.Error("could not close owned scheduler fixture")
				}
			}()
			svc.wakeExports(ctx)
			select {
			case <-paused.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("scheduled executor did not enter the actual Put")
			}
			// Let the finite selection pass finish before releasing Put. The
			// next read after heartbeat join can only be this executor's tail.
			passFinished := func() bool {
				svc.exportMu.Lock()
				defer svc.exportMu.Unlock()
				return !svc.exportDispatching && len(svc.exportActive) == 1
			}
			waitSchedulerBoundary(t, passFinished)
			if mode == "cancelled" {
				svc.exportMu.Lock()
				beforeCancel := svc.exportWakeGeneration
				svc.exportMu.Unlock()
				if _, err := svc.CancelExport(testCtx, "tenant-a", "operator", "tail-job"); err != nil {
					t.Fatal("could not revoke the actual running owner at Put")
				}
				svc.exportMu.Lock()
				cancelWakes := svc.exportWakeGeneration - beforeCancel
				svc.exportMu.Unlock()
				if cancelWakes != 1 {
					t.Fatal("owner cancellation lost its original independent dispatcher wake")
				}
				// CancelExport itself wakes a finite pass. Drain that actual
				// pass while Put is still held, before arming the tail oracle;
				// active=1 plus dispatching=true is legitimate concurrent work.
				waitSchedulerBoundary(t, passFinished)
				t.Log("cancel_wake_drained_before_put_release=true")
			}
			releasePut()
			var tailCtx context.Context
			select {
			case tailCtx = <-backend.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("executor did not join heartbeat and enter its final control Load")
			}
			deadline, bounded := tailCtx.Deadline()
			if !bounded || time.Until(deadline) > svc.Store.OperationTimeout() {
				t.Fatal("final control read lost the original store operation bound")
			}
			if !svc.exportMu.TryLock() {
				t.Fatal("scheduler held its mutex across the final backend read")
			}
			active, tenant, dispatching := len(svc.exportActive), svc.exportActive["tail-job"], svc.exportDispatching
			generation := svc.exportWakeGeneration
			svc.exportMu.Unlock()
			t.Logf("executor_tail_boundary active=%d dispatching=%t heartbeat_joined=%t", active, dispatching, backend.armed.Load())
			if active != 1 || tenant != "tenant-a" || dispatching {
				t.Error("scheduler advertised idle while its executor still owned a bounded control read")
			}
			before, err := base.Load(testCtx)
			if err != nil {
				t.Fatal("could not inspect committed scheduler outcome")
			}
			want := "completed"
			if mode == "failed" {
				want = "failed"
			} else if mode == "cancelled" {
				want = domain.ExportStatusCancelled
			} else if mode == "retained-sealed" {
				want = "running"
			}
			if before.Exports["tail-job"].Status != want {
				t.Fatal("final-read oracle did not retain the real executor outcome")
			}
			if mode == "caller-cancel" {
				cancel()
			} else if mode != "read-deadline" {
				releaseTail()
			}
			select {
			case <-backend.exited:
			case <-time.After(2 * time.Second):
				t.Fatal("bounded final control read did not exit")
			}
			awaitSchedulerFixtureIdle(t, svc)
			svc.exportMu.Lock()
			wakes := svc.exportWakeGeneration - generation
			svc.exportMu.Unlock()
			wantWakes, wantLoads := uint64(0), int64(1)
			if mode == "completed" || mode == "failed" || mode == "cancelled" {
				wantWakes, wantLoads = 1, 2 // tail and one finite follow-up pass
			}
			if wakes != wantWakes || backend.loadsAfterStop.Load() != wantLoads {
				t.Fatal("executor lost its finite completion wake or hot-spun a failed/deferred read")
			}
			after, err := base.Load(testCtx)
			if err != nil || !reflect.DeepEqual(after, before) {
				t.Fatal("local lifecycle bookkeeping rewrote a terminal/lease/stage or its facts")
			}
			if mode == "read-deadline" && svc.Store.OperationDeadlineExhaustions() != 1 {
				t.Fatal("final control deadline was extended or miscounted")
			}
		})
	}
}
