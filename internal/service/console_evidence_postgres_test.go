package service

import (
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

func TestPostgresConsoleEvidencePreservesIndependentTerminalControl(t *testing.T) {
	for _, kind := range []string{"page", "facets"} {
		for _, tenant := range []string{"tenant-a", ""} {
			t.Run(kind+"/"+tenant, func(t *testing.T) {
				first, second := newExportLeasePostgres(t)
				var clock atomic.Int64
				clock.Store(pinnedNow.UnixNano())
				for _, svc := range []*Service{first, second} {
					svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
				}
				seedTestDomain(t, first)
				seedConsoleOtherTenant(t, first)
				objects := &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
				first.Config.Archive, second.Config.Archive = objects, objects
				for _, id := range []string{"tenant-a", "tenant-b"} {
					if _, err := first.Ingest(testCtx, id, crmPrincipal, testEvent("console-source-"+id, "console-operation", pinnedNow), domain.StatusArchived); err != nil {
						t.Fatal("could not commit PostgreSQL Console evidence")
					}
				}
				seedLeaseExport(t, first, "console-concurrent-job", "tenant-a")
				claim, won, err := first.claimPendingExport(testCtx, "console-concurrent-job")
				if err != nil || !won {
					t.Fatal("could not prepare the original fenced running job")
				}
				paused := &queryPausedArchive{Store: objects, entered: make(chan struct{}, 1), release: make(chan struct{})}
				first.Config.Archive = paused
				done, exited := make(chan error, 1), make(chan struct{})
				var result consoleReadResult
				release := sync.OnceFunc(func() { close(paused.release) })
				t.Cleanup(func() { release(); <-exited })
				go func() {
					defer close(exited)
					var err error
					result, err = readConsoleEvidence(testCtx, first, kind, tenant, "reader", consoleEvidenceQuery())
					done <- err
				}()
				select {
				case <-paused.entered:
				case <-time.After(10 * time.Second):
					t.Fatal("PostgreSQL Console never reached the actual source Get")
				}
				control := make(chan error, 1)
				go func() {
					clock.Store(pinnedNow.Add(ExportHeartbeatInterval).UnixNano())
					// Renewal uses the exact Store whose Get is blocked; cancellation
					// and policy/Hold mutation use an independent SQL pool and CAS.
					if err := first.renewExportLease(testCtx, claim); err != nil {
						control <- err
						return
					}
					if _, err := second.CancelExport(testCtx, "tenant-a", "operator", claim.ID); err != nil {
						control <- err
						return
					}
					control <- second.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
						data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
						data.LegalHolds["console-concurrent-hold"] = domain.LegalHold{ID: "console-concurrent-hold", TenantID: "tenant-a", Name: "Concurrent Hold", CreatedAt: first.Now()}
						return nil
					})
				}()
				select {
				case err := <-control:
					if err != nil {
						t.Fatal("Console Get blocked renewal or independent control mutation")
					}
				case <-time.After(5 * time.Second):
					release()
					<-control
					t.Fatal("Console retained a lock across source Get")
				}
				release()
				want := 1
				if tenant == "" {
					want = 2
				}
				if err := <-done; err != nil || result.count(kind) != want {
					t.Fatal("PostgreSQL Console could not authenticate its captured evidence")
				}
				observed, current := leaseSnapshot(t, first), leaseSnapshot(t, second)
				if observed.LayoutVersion != 2 || current.LayoutVersion != 2 || current.Exports[claim.ID].Status != domain.ExportStatusCancelled || len(current.ExportLeases) != 0 || current.Policies["tenant-a"].ArchiveDays != 365 || current.LegalHolds["console-concurrent-hold"].ID == "" || !reflect.DeepEqual(observed.Exports[claim.ID], current.Exports[claim.ID]) {
					t.Fatal("Console read overwrote terminal, lease, Hold or independent policy control")
				}
				facts := consoleReadFacts(t, second)
				if len(facts) != 1 || facts[0].TenantID != tenant || facts[0].Actor != "reader" {
					t.Fatal("PostgreSQL Console read fact was missing, duplicated or incorrectly scoped")
				}
			})
		}
	}
}
