package service

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportPlanFileRestartPastCursorLifetimeUsesOnlyOriginalCoordinates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := newServiceOn(t, st)
	seedTestDomain(t, svc)
	objects := &checkpointArchive{Store: &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}}
	svc.Config.Archive = objects
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("plan-original", "operation", pinnedNow.Add(10*time.Second)), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	seedLeaseExport(t, svc, "checkpoint-job", "tenant-a")
	if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
		job := data.Exports["checkpoint-job"]
		job.Query.Cursor = "expired-query-cursor-fixture"
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	old := leaveSelectedPlan(t, svc) // actual selected-stage CAS, no JSONL/Put yet
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
	late, err := restarted.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("plan-later", "operation", pinnedNow.Add(20*time.Second)), domain.StatusArchived)
	if err != nil {
		t.Fatal(err)
	}
	probe := &planSourceArchive{Store: objects, forbidden: eventArchiveKey("tenant-a", late.StreamID, late.Sequence, late.EventID)}
	restarted.Config.Archive = probe
	puts := objects.puts.Load()
	ctx, cancel := context.WithCancel(testCtx)
	done := make(chan struct{})
	go func() { restarted.RunExportQueue(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	deadline := time.After(10 * time.Second)
	var job domain.ExportJob
	for {
		data := leaseSnapshot(t, restarted)
		job = data.Exports[old.ID]
		restarted.exportMu.Lock()
		idle := len(restarted.exportActive) == 0 && !restarted.exportDispatching
		restarted.exportMu.Unlock()
		if job.Status == "completed" && idle {
			if job.EventCount != 1 || !job.StartedAt.Equal(*old.StartedAt) || len(data.ExportPlans) != 0 || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 {
				t.Fatal("restarted selected stage changed membership/identity or retained live metadata")
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("queue startup did not continue the durable selected stage")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if objects.puts.Load() != puts+1 || probe.sourceGets.Load() != 1 {
		t.Fatal("restart reselected current history rather than original committed coordinates")
	}
	sealed, err := objects.Get(testCtx, job.ObjectPath)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := restarted.VerifyExportDownload(testCtx, "tenant-a", old.ID, sealed)
	if err != nil || !exportPlainMatchesPlan(plain, old.Plan) {
		t.Fatal("restarted output did not retain the authenticated ordered plan")
	}
}

func TestExportPlanRespectsSharedRunningSlotsDuringTakeover(t *testing.T) {
	svc, _, _ := newPlanService(t)
	old := leaveSelectedPlan(t, svc)
	seedLeaseExport(t, svc, "tenant-a-pending", "tenant-a")
	seedLeaseExport(t, svc, "tenant-b-running", "tenant-b")
	seedLeaseExport(t, svc, "tenant-c-pending", "tenant-c")
	if _, won, err := svc.claimPendingExport(testCtx, "tenant-a-pending"); err != nil || won {
		t.Fatal("selected stage did not occupy tenant running slot")
	}
	if _, won, err := svc.claimPendingExport(testCtx, "tenant-b-running"); err != nil || !won {
		t.Fatal("selected stage occupied more than its own global slot")
	}
	if _, won, err := svc.claimPendingExport(testCtx, "tenant-c-pending"); err != nil || won {
		t.Fatal("selected stage allowed extra global running claim")
	}
	expireCheckpoint(svc)
	var winners atomic.Int64
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	for range 2 {
		go func() {
			<-start
			if _, won, err := svc.claimExport(testCtx, old.ID, true); err != nil {
				t.Error("selected takeover failed")
			} else if won {
				winners.Add(1)
			}
			done <- struct{}{}
		}()
	}
	close(start)
	<-done
	<-done
	if winners.Load() != 1 || len(leaseSnapshot(t, svc).ExportLeases) != ExportConcurrency {
		t.Fatal("selected takeover multiplied its existing running slot")
	}
}
