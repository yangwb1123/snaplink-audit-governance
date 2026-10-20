package service

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/store"
)

// These observations distinguish fixture cost, actual archive IO and durable
// export stage. They never log keys, owners, errors, bytes or search metadata,
// and do not replace the existing file backend or extend any wait/deadline.
type fileProgressArchive struct {
	archive.Store
	t       *testing.T
	svc     *Service
	started time.Time
	mu      sync.Mutex
	active  bool
}

func observeFileProgress(t *testing.T, svc *Service, started time.Time) {
	t.Helper()
	observer := &fileProgressArchive{Store: svc.Config.Archive, t: t, svc: svc, started: started, active: true}
	svc.Config.Archive = observer
	t.Cleanup(func() {
		observer.mu.Lock()
		defer observer.mu.Unlock()
		observer.active = false // late IO must not log through a finished test
	})
}

func (a *fileProgressArchive) Get(ctx context.Context, key string) ([]byte, error) {
	started := time.Now()
	data, err := a.Store.Get(ctx, key)
	a.record("get", key, started, err)
	return data, err
}

func (a *fileProgressArchive) Put(ctx context.Context, key string, data []byte) error {
	if strings.HasPrefix(key, "exports/") {
		// The actual export Put is outside Store.mu. Capture durable selected
		// membership at this boundary, not a timer-based guess of progress.
		a.mu.Lock()
		if claim, ok := exportClaimFromContext(ctx); ok && a.active {
			logExportFileStage(a.t, a.svc, claim.ID)
		}
		a.mu.Unlock()
	}
	started := time.Now()
	err := a.Store.Put(ctx, key, data)
	a.record("put", key, started, err)
	return err
}

func (a *fileProgressArchive) record(operation, key string, started time.Time, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.active {
		return
	}
	kind := "other"
	for _, candidate := range []string{"events", "segments", "exports"} {
		if strings.HasPrefix(key, candidate+"/") {
			kind = candidate
			break
		}
	}
	a.t.Logf("file_io operation=%s kind=%s success=%t elapsed_ms=%d since_start_ms=%d", operation, kind, err == nil, time.Since(started).Milliseconds(), time.Since(a.started).Milliseconds())
}

func logExportFileStage(t *testing.T, svc *Service, jobID string) {
	t.Helper()
	var status, stage string
	var members, pages int
	var leased bool
	err := svc.Store.ReadControl(testCtx, func(data *store.Snapshot) error {
		status = data.Exports[jobID].Status
		stage = "unplanned"
		if plan, ok := data.ExportPlans[jobID]; ok {
			stage, members, pages = "selected", len(plan.Members), len(plan.Chunks)
			if pages > 0 {
				stage = "generating"
			}
		}
		if _, ok := data.ExportCheckpoints[jobID]; ok {
			stage = "sealed"
		}
		_, leased = data.ExportLeases[jobID]
		return nil
	})
	t.Logf("export_observation success=%t status=%s stage=%s members=%d pages=%d leased=%t", err == nil, status, stage, members, pages, leased)
}
