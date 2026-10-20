package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportCheckpointFileRestartUsesOriginalEvidenceAfterCursorLifetime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	var armed atomic.Bool
	var saves atomic.Int64
	st, err := store.OpenWithControlWriteFault(path, func() error {
		if !armed.Load() {
			return nil
		}
		saves.Add(1)
		// Read the already-durable control at the actual Save boundary,
		// not a numbered write (selected-stage persistence adds a Save).
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var persisted store.Snapshot
		if err := json.Unmarshal(raw, &persisted); err != nil {
			return err
		}
		if _, sealed := persisted.ExportCheckpoints["checkpoint-job"]; sealed {
			return errors.New("injected terminal control write fault")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	svc := newServiceOn(t, st)
	seedTestDomain(t, svc)
	objects := &checkpointArchive{Store: &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}}
	svc.Config.Archive = objects
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("checkpoint-original", "operation", pinnedNow.Add(10*time.Second)), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	seedLeaseExport(t, svc, "checkpoint-job", "tenant-a")
	if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
		job := data.Exports["checkpoint-job"]
		job.Query.Cursor = "retired-query-cursor-fixture"
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	svc.runExport(testCtx, "checkpoint-job")
	before := leaseSnapshot(t, svc)
	cp, exists := before.ExportCheckpoints["checkpoint-job"]
	if !exists || before.Exports[cp.JobID].Status != "running" || saves.Load() < 3 {
		t.Fatal("actual post-checkpoint terminal fault was not reached")
	}
	armed.Store(false)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := newServiceOn(t, reopened)
	restarted.Config.Archive = objects
	restarted.Config.Now = func() time.Time { return pinnedNow.Add(time.Hour) }
	// Late evidence matches the original filter but must not enter the
	// checkpointed output. The old 15-minute cursor is deliberately unusable.
	if _, err := restarted.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("checkpoint-late", "operation", pinnedNow.Add(20*time.Second)), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	objects.armed.Store(true)
	puts, gets := objects.puts.Load(), objects.gets.Load()
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	done := make(chan struct{})
	go func() { restarted.RunExportQueue(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	deadline := time.After(10 * time.Second)
	for {
		data := leaseSnapshot(t, restarted)
		job := data.Exports[cp.JobID]
		if job.Status == "completed" {
			if job.Digest != cp.Digest || job.EventCount != 1 || job.ObjectPath != cp.ObjectPath || !job.StartedAt.Equal(*before.Exports[cp.JobID].StartedAt) || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 {
				t.Fatal("restart changed checkpoint membership or execution identity")
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("queue startup did not resume persisted sealed work")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if objects.puts.Load() != puts || objects.gets.Load() != gets+1 {
		t.Fatal("restart regenerated the output or read historical source objects")
	}
	if checkpointFactCount(t, restarted, domain.AdminActionExportCheckpointed) != 1 || checkpointFactCount(t, restarted, domain.AdminActionExportResumed) != 1 || checkpointFactCount(t, restarted, domain.AdminActionExportCompleted) != 1 {
		t.Fatal("restart lost or duplicated atomic checkpoint facts")
	}
	// Wait for the finite executor's final control read before closing its
	// store; no detached test goroutine may leak into subsequent cases.
	for {
		restarted.exportMu.Lock()
		idle := len(restarted.exportActive) == 0 && !restarted.exportDispatching
		restarted.exportMu.Unlock()
		if idle {
			break
		}
		select {
		case <-deadline:
			t.Fatal("dispatcher did not become idle")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
