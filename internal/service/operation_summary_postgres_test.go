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

func TestPostgresOperationSummaryPreservesIndependentTerminalControl(t *testing.T) {
	first, second := newExportLeasePostgres(t)
	var clock atomic.Int64
	clock.Store(pinnedNow.UnixNano())
	for _, svc := range []*Service{first, second} {
		svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	}
	seedTestDomain(t, first)
	objects := &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
	first.Config.Archive, second.Config.Archive = objects, objects
	if _, err := first.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("summary-source", "summary-operation", pinnedNow), domain.StatusArchived); err != nil {
		t.Fatal("could not commit PostgreSQL summary source evidence")
	}
	seedLeaseExport(t, first, "summary-concurrent-job", "tenant-a")
	claim, won, err := first.claimPendingExport(testCtx, "summary-concurrent-job")
	if err != nil || !won {
		t.Fatal("could not prepare the original fenced PostgreSQL job")
	}
	paused := &queryPausedArchive{Store: objects, entered: make(chan struct{}, 1), release: make(chan struct{})}
	first.Config.Archive = paused
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	release := sync.OnceFunc(func() { close(paused.release) })
	done, exited := make(chan error, 1), make(chan struct{})
	var summary domain.OperationSummary
	defer func() { release(); cancel(); <-exited }()
	go func() {
		defer close(exited)
		var err error
		summary, err = first.Operation(ctx, "tenant-a", "reader", "summary-operation")
		done <- err
	}()
	select {
	case <-paused.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("PostgreSQL summary missed the actual source Get boundary")
	}
	control, controlExited := make(chan error, 1), make(chan struct{})
	defer func() { cancel(); <-controlExited }()
	go func() {
		defer close(controlExited)
		clock.Store(pinnedNow.Add(ExportHeartbeatInterval).UnixNano())
		// The same Store renews during the blocked Get. A separate SQL pool
		// cancels and mutates Hold/policy under the real shared control CAS.
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
			data.LegalHolds["summary-concurrent-hold"] = domain.LegalHold{ID: "summary-concurrent-hold", TenantID: "tenant-a", Name: "Concurrent Hold", CreatedAt: first.Now()}
			return nil
		})
	}()
	select {
	case err := <-control:
		if err != nil {
			t.Fatal("summary Get blocked renewal or independent control mutation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("summary retained a lock across the actual PostgreSQL source Get")
	}
	release()
	if err := <-done; err != nil || summary.EventCount != 1 || summary.TenantID != "tenant-a" || summary.OperationID != "summary-operation" {
		t.Fatal("PostgreSQL summary did not authenticate captured evidence")
	}
	observed, current := leaseSnapshot(t, first), leaseSnapshot(t, second)
	if observed.LayoutVersion != 2 || current.LayoutVersion != 2 || current.Exports[claim.ID].Status != domain.ExportStatusCancelled || len(current.ExportLeases) != 0 || current.Policies["tenant-a"].ArchiveDays != 365 || current.LegalHolds["summary-concurrent-hold"].ID == "" || !reflect.DeepEqual(observed.Exports[claim.ID], current.Exports[claim.ID]) {
		t.Fatal("summary overwrote a cross-replica terminal, lease, Hold or policy update")
	}
	facts := readFacts(t, second)
	if len(facts) != 1 || facts[0].Actor != "reader" || facts[0].TargetType != "operation" || facts[0].TargetID != "summary-operation" || facts[0].Detail != "summary" {
		t.Fatal("summary read fact was missing, duplicated or misbound across SQL pools")
	}
}
