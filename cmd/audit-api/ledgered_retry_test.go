package main

import (
	"context"
	"io"
	"log"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/service"
)

func TestLedgeredFlushDelayBackoffJitterAndReset(t *testing.T) {
	minimum := func(int64) int64 { return 0 }
	maximum := func(limit int64) int64 { return limit - 1 }
	previous := time.Duration(0)
	for _, failures := range []int{0, 1, 2, 3, 4, 8, 1_000_000} {
		low := ledgeredFlushDelay(failures, minimum)
		high := ledgeredFlushDelay(failures, maximum)
		if low < time.Second || high >= 30*time.Second || low < previous || high <= low {
			t.Fatalf("invalid retry bounds at %d: [%s,%s], previous=%s", failures, low, high, previous)
		}
		previous = low
	}
	if got := ledgeredFlushDelay(1, minimum); got != 2*time.Second {
		t.Fatalf("first failure delay=%s", got)
	}
	if got := ledgeredFlushDelay(0, minimum); got != time.Second {
		t.Fatalf("successful round did not reset: %s", got)
	}
}

func TestLedgeredOutboxLoopStopsDuringRetryWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		runLedgeredOutboxLoop(ctx, &service.Service{}, log.New(io.Discard, "", 0))
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retry loop ignored cancellation")
	}
}
