package service

import (
	"context"
	"errors"
	"time"
)

// Each bounded export executor owns one heartbeat goroutine. The timer factory
// is instance-local for deterministic tests at actual Store/Put boundaries.
// A failed renewal cancels that executor; it cannot revive an expired lease.
func (s *Service) startExportHeartbeat(ctx context.Context, claim exportClaim) (context.Context, func()) {
	ctx, cancel := context.WithCancel(exportClaimContext(ctx, claim))
	ticks, stopTimer := (<-chan time.Time)(nil), func() {}
	if s.exportHeartbeat != nil {
		ticks, stopTimer = s.exportHeartbeat()
	} else {
		timer := time.NewTicker(ExportHeartbeatInterval)
		ticks, stopTimer = timer.C, timer.Stop
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer stopTimer()
		for {
			select {
			case <-ctx.Done():
				return
			case _, open := <-ticks:
				if !open {
					cancel()
					return
				}
				if err := s.renewExportLease(ctx, claim); err != nil {
					category := "write"
					if errors.Is(err, errExportLeaseLost) {
						category = "lease"
					}
					s.logf("export lease renewal failed job_id=%s category=%s", claim.ID, category)
					cancel()
					return
				}
			}
		}
	}()
	return ctx, func() { cancel(); <-done }
}
