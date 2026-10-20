package service

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// The original oracle included the post-Put durable receipt commit in the
// five-second Put bound. Pause at the REAL second ledger append to separate
// them deterministically. All state/commits remain on the original file backend.
func TestDetachedPutMeasurementExcludesPostPutLedgerCommit(t *testing.T) {
	entered, release, stop, joined := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	blocking := &archivePutBlockingStore{}
	calls, armed, afterPut := 0, false, false
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.OpenWithLedgerWriteFault(path, func() error {
		if !armed {
			return nil
		}
		calls++
		if calls == 2 {
			afterPut = !blocking.finishedAt.IsZero() && errors.Is(blocking.observedErr, context.DeadlineExceeded)
			close(entered)
			<-release
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if st != nil {
			_ = st.Close()
		}
	})
	svc := newServiceOn(t, st)
	seedFileTestDomain(t, svc)
	svc.Config.Archive = blocking
	armed = true
	go func() {
		defer close(joined)
		defer releaseGate()
		select {
		case <-entered:
			// Put itself already waited five seconds. Only 1.25s of
			// post-Put commit delay is enough to falsify the old <=6s
			// whole-Ingest oracle; the production store budget is unchanged.
			timer := time.NewTimer(time.Second + 250*time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-stop:
			}
		case <-stop:
		}
	}()
	t.Cleanup(func() {
		close(stop)
		releaseGate()
		<-joined
	})
	assertDetachedPutCancellation(t, svc, blocking)
	if calls != 2 || !afterPut {
		t.Fatal("measurement regression did not reach the real post-Put receipt append")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st = nil
	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	data, err := reopened.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Receipts) != 1 || len(data.Events) != 1 {
		t.Fatal("post-Put delay lost or duplicated the original file evidence")
	}
	receipt := data.Receipts[store.EventKey("tenant-a", "ingest-detached-blocking")]
	if receipt.Status != domain.StatusIndexed || !receipt.ArchivedAt.IsZero() {
		t.Fatal("failed Put was recorded as archived after file restart")
	}
}
