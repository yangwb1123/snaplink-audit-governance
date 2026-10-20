package service

import (
	"sync"
	"testing"

	"github.com/snaplink/audit-governance/internal/store"
)

// An early return (including Fatal's cleanup path) must not leave a publisher
// and its key/round gates alive in the next test. Release/join is also installed
// in the parent so this regression is safe even against the unfixed fixture.
func TestFlushFixtureCleanupJoinsPublisherBeforeNextTest(t *testing.T) {
	svc := testService(t, false) // retain real file persistence and original limits
	publisher := newGatedLedgeredPublisher("cleanup-owned")
	svc.Config.LedgeredPublisher = publisher
	seedLedgeredOutbox(t, svc, "cleanup-owned")
	var done chan struct{}
	release := sync.OnceFunc(func() { close(publisher.block["cleanup-owned"]) })
	t.Cleanup(func() {
		release()
		if done != nil {
			awaitChannel(t, done, "the owned flush goroutine to exit")
		}
		if err := svc.Store.Close(); err != nil {
			t.Error("could not release owned fixture file lock")
		}
	})
	t.Run("early-return", func(t *testing.T) {
		done, _ = startFlushRound(t, svc, 100)
		awaitChannel(t, publisher.entered["cleanup-owned"], "the actual publisher boundary")
		// Intentionally do not release the broker: child cleanup owns shutdown.
	})
	select {
	case <-done:
	default:
		t.Fatal("fixture returned with an unjoined background publisher")
	}
	if len(pendingLedgeredEvents(t, svc)) != 1 || svc.LedgeredPublishSuccesses() != 0 {
		t.Fatal("cancelled fixture invented a broker confirmation or removed its durable intent")
	}
	svc.ledgeredGatesMu.Lock()
	gates := len(svc.ledgeredGates)
	svc.ledgeredGatesMu.Unlock()
	if gates != 0 || !svc.ledgeredFlushMu.TryLock() {
		t.Fatal("joined fixture retained a key or round gate")
	}
	svc.ledgeredFlushMu.Unlock()
	if err := svc.Store.ReadControl(testCtx, func(data *store.Snapshot) error { return nil }); err != nil {
		t.Fatal("joined fixture retained Store.mu")
	}
}
