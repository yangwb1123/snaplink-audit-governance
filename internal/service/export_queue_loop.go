package service

import (
	"context"
	"time"
)

// RunExportQueue wakes durable pending work immediately and periodically.
// The bounded dispatcher owns selection/claim; this loop owns no job goroutine
// per pending record. It has no business execution side effects.
func (s *Service) RunExportQueue(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		s.wakeExports(context.WithoutCancel(ctx))
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
