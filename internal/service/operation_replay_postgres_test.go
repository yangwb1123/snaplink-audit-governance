package service

import (
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestPostgresOperationReplayPreservesIndependentTerminalControl(t *testing.T) {
	first, second := newExportLeasePostgres(t)
	var clock atomic.Int64
	clock.Store(pinnedNow.UnixNano())
	for _, svc := range []*Service{first, second} {
		svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	}
	seedTestDomain(t, first)
	objects := &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
	first.Config.Archive, second.Config.Archive = objects, objects
	if _, err := first.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("replay-source", "replay-operation", pinnedNow), domain.StatusArchived); err != nil {
		t.Fatal("could not commit PostgreSQL replay source evidence")
	}
	seedLeaseExport(t, first, "replay-concurrent-job", "tenant-a")
	claim, won, err := first.claimPendingExport(testCtx, "replay-concurrent-job")
	if err != nil || !won {
		t.Fatal("could not prepare the fenced PostgreSQL job")
	}
	paused := &queryPausedArchive{Store: objects, entered: make(chan struct{}, 1), release: make(chan struct{})}
	first.Config.Archive = paused
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	release := sync.OnceFunc(func() { close(paused.release) })
	done, exited := make(chan error, 1), make(chan struct{})
	var result domain.ReplayResult
	defer func() { release(); cancel(); <-exited }()
	go func() {
		defer close(exited)
		var err error
		result, err = first.ReplayOperation(ctx, "tenant-a", "reader", "replay-operation")
		done <- err
	}()
	select {
	case <-paused.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("PostgreSQL replay missed the actual source Get")
	}
	control, controlExited := make(chan error, 1), make(chan struct{})
	defer func() { cancel(); <-controlExited }()
	go func() {
		defer close(controlExited)
		clock.Store(pinnedNow.Add(ExportHeartbeatInterval).UnixNano())
		if err := first.renewExportLease(ctx, claim); err != nil {
			control <- err
			return
		}
		if _, err := second.CancelExport(ctx, "tenant-a", "operator", claim.ID); err != nil {
			control <- err
			return
		}
		control <- second.Store.UpdateControl(ctx, func(data *store.Snapshot) error {
			data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
			data.LegalHolds["replay-concurrent-hold"] = domain.LegalHold{ID: "replay-concurrent-hold", TenantID: "tenant-a", Name: "Concurrent Hold", CreatedAt: first.Now()}
			return nil
		})
	}()
	select {
	case err := <-control:
		if err != nil {
			t.Fatal("replay Get blocked renewal or independent control mutation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replay retained a lock across the PostgreSQL source Get")
	}
	release()
	if err := <-done; err != nil || result.EventCount != 1 || result.TenantID != "tenant-a" || result.OperationID != "replay-operation" || result.State["status"] != "paid" {
		t.Fatal("PostgreSQL replay did not authenticate the original local fold")
	}
	observed, current := leaseSnapshot(t, first), leaseSnapshot(t, second)
	if observed.LayoutVersion != 2 || current.LayoutVersion != 2 || current.Exports[claim.ID].Status != domain.ExportStatusCancelled || len(current.ExportLeases) != 0 || current.Policies["tenant-a"].ArchiveDays != 365 || current.LegalHolds["replay-concurrent-hold"].ID == "" || !reflect.DeepEqual(observed.Exports[claim.ID], current.Exports[claim.ID]) {
		t.Fatal("replay overwrote a cross-replica terminal, lease, Hold or policy update")
	}
	facts := readFacts(t, second)
	if len(facts) != 1 || facts[0].Actor != "reader" || facts[0].TargetType != "operation" || facts[0].TargetID != "replay-operation" || facts[0].Detail != "replay" {
		t.Fatal("replay read fact was missing, duplicated or misbound across SQL pools")
	}
}
