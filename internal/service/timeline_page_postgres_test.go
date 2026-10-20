package service

import (
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestPostgresTimelinePageCapturedEvidencePreservesIndependentControl(t *testing.T) {
	for _, kind := range []string{"operation", "aggregate"} {
		t.Run(kind, func(t *testing.T) {
			first, second := newExportLeasePostgres(t)
			seedTestDomain(t, first)
			objects := &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
			first.Config.Archive, second.Config.Archive = objects, objects
			if _, err := first.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("timeline-source", "timeline-operation", pinnedNow), domain.StatusArchived); err != nil {
				t.Fatal("could not commit PostgreSQL timeline evidence")
			}
			seedLeaseExport(t, first, "timeline-concurrent-job", "tenant-a")
			claim, won, err := first.claimPendingExport(testCtx, "timeline-concurrent-job")
			if err != nil || !won {
				t.Fatal("could not prepare the fenced concurrent control job")
			}
			paused := &queryPausedArchive{Store: objects, entered: make(chan struct{}, 1), release: make(chan struct{})}
			first.Config.Archive = paused
			done, exited := make(chan error, 1), make(chan struct{})
			var page domain.QueryResult
			release := sync.OnceFunc(func() { close(paused.release) })
			t.Cleanup(func() { release(); <-exited })
			go func() {
				defer close(exited)
				var err error
				page, err = readTimelinePage(testCtx, first, kind, "tenant-a", "reader", 1, "")
				done <- err
			}()
			select {
			case <-paused.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("PostgreSQL timeline never reached its real Get boundary")
			}
			control := make(chan error, 1)
			go func() {
				// Same-Store lease renewal must not wait for the source socket;
				// an independent SQL pool then cancels under the original CAS.
				first.Config.Now = func() time.Time { return pinnedNow.Add(ExportHeartbeatInterval) }
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
					return nil
				})
			}()
			select {
			case err := <-control:
				if err != nil {
					t.Fatal("concurrent renewal/independent cancellation failed at timeline Get")
				}
			case <-time.After(5 * time.Second):
				release()
				<-control
				t.Fatal("timeline retained a lock across source Get")
			}
			release()
			if err := <-done; err != nil || page.Count != 1 || len(page.Items) != 1 || page.Items[0].EventID != "timeline-source" {
				t.Fatal("PostgreSQL timeline did not resume its authenticated captured evidence")
			}
			observed := leaseSnapshot(t, first)
			current := leaseSnapshot(t, second)
			if observed.LayoutVersion != 2 || current.LayoutVersion != 2 || current.Exports[claim.ID].Status != domain.ExportStatusCancelled || len(current.ExportLeases) != 0 || current.Policies["tenant-a"].ArchiveDays != 365 || !reflect.DeepEqual(observed.Exports[claim.ID], current.Exports[claim.ID]) {
				t.Fatal("timeline read overwrote a cross-replica terminal/control update")
			}
			if len(readFacts(t, second)) != 1 {
				t.Fatal("PostgreSQL timeline read fact was missing or duplicated across pools")
			}
		})
	}
}
