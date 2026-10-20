package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type exportPreflightArchive struct {
	archive.Store
	gets      atomic.Int64
	deadlines chan time.Time
	entered   chan struct{}
	release   chan struct{}
}

func (a *exportPreflightArchive) Get(ctx context.Context, key string) ([]byte, error) {
	deadline, _ := ctx.Deadline()
	a.deadlines <- deadline
	// QueryEvents reads first; only block the later current-Hold preflight.
	if a.gets.Add(1) == 2 {
		close(a.entered)
		select {
		case <-a.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return a.Store.Get(ctx, key)
}

func TestExportPreflightArchiveIOReleasesStoreLockAndSharesDeadline(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		for _, interrupted := range []bool{false, true} {
			t.Run(surface+map[bool]string{false: "/hold-denial", true: "/withdrawal"}[interrupted], func(t *testing.T) {
				svc := testService(t, true)
				pauseExportDispatch(svc)
				at := pinnedNow.Add(10 * time.Second)
				if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("preflight-evidence", "operation", at), domain.StatusArchived); err != nil {
					t.Fatal(err)
				}
				if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "case", Reason: "case"}); err != nil {
					t.Fatal(err)
				}
				query := domain.Query{From: at.Add(-time.Second), To: at.Add(time.Second)}
				seedActionExport(t, svc, "healthy-executor", "pending")
				claim, won, err := svc.claimPendingExport(testCtx, "healthy-executor")
				if err != nil || !won {
					t.Fatal(err)
				}
				initialJobs := 1
				if surface == "retry" {
					source := seedActionExport(t, svc, "preflight-source", "failed")
					source.Query = query
					if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error { data.Exports[source.ID] = source; return nil }); err != nil {
						t.Fatal(err)
					}
					initialJobs++
				}
				paused := &exportPreflightArchive{Store: svc.Config.Archive, entered: make(chan struct{}), release: make(chan struct{}), deadlines: make(chan time.Time, 8)}
				svc.Config.Archive = paused
				ctx, cancel := context.WithCancel(testCtx)
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(paused.release) }) }
				done := make(chan struct{})
				var resultErr error
				t.Cleanup(func() { cancel(); release(); <-done })
				go func() {
					defer close(done)
					if surface == "create" {
						_, resultErr = svc.CreateExport(ctx, "tenant-a", "operator", query)
					} else {
						_, resultErr = svc.RetryExport(ctx, "tenant-a", "operator", "preflight-source")
					}
				}()
				select {
				case <-paused.entered:
				case <-time.After(10 * time.Second):
					t.Fatal("Hold preflight never entered real Get boundary")
				}
				firstDeadline, holdDeadline := <-paused.deadlines, <-paused.deadlines
				if firstDeadline.IsZero() || !firstDeadline.Equal(holdDeadline) {
					t.Fatal("preflight restarted the service operation deadline")
				}
				// These must complete while the second Get is still blocked, not
				// after a timer releases it. Neither operation reads evidence.
				controlDone := make(chan error, 1)
				go func() {
					if err := svc.renewExportLease(testCtx, claim); err != nil {
						controlDone <- err
						return
					}
					_, err := svc.CancelExport(testCtx, "tenant-a", "operator", claim.ID)
					controlDone <- err
				}()
				select {
				case err := <-controlDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("Hold Get retained Store.mu and blocked renewal/cancel")
				}
				if interrupted {
					cancel()
				} else {
					release()
				}
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("preflight failed to converge")
				}
				want := domain.ErrForbidden
				if interrupted {
					want = context.Canceled
				}
				if !errors.Is(resultErr, want) {
					t.Fatal("preflight failed open", resultErr)
				}
				data := leaseSnapshot(t, svc)
				if len(data.Exports) != initialJobs || data.Exports[claim.ID].Status != domain.ExportStatusCancelled {
					t.Fatal("failed preflight created work or lost owner cancellation")
				}
				for _, fact := range data.AdminActions {
					if fact.Action == domain.AdminActionExportCreated || fact.Action == domain.AdminActionExportRetried {
						t.Fatal("failed preflight published governance success")
					}
				}
			})
		}
	}
}
