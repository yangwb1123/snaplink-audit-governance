package service

import (
	"context"
	"errors"
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

func TestPostgresLegacyTimelinePreservesIndependentTerminalControl(t *testing.T) {
	for _, kind := range legacyTimelineKinds {
		t.Run(kind, func(t *testing.T) {
			first, second := newExportLeasePostgres(t)
			var clock atomic.Int64
			clock.Store(pinnedNow.UnixNano())
			for _, svc := range []*Service{first, second} {
				svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
			}
			seedTestDomain(t, first)
			objects := &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
			first.Config.Archive, second.Config.Archive = objects, objects
			if _, err := first.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("legacy-source", "replay-operation", pinnedNow), domain.StatusArchived); err != nil {
				t.Fatal("could not commit PostgreSQL legacy source evidence")
			}
			original, err := first.Store.Snapshot(testCtx)
			if err != nil {
				t.Fatal(err)
			}
			seedLeaseExport(t, first, "legacy-concurrent-job", "tenant-a")
			claim, won, err := first.claimPendingExport(testCtx, "legacy-concurrent-job")
			if err != nil || !won {
				t.Fatal("could not prepare the fenced PostgreSQL job")
			}
			paused := &queryPausedArchive{Store: objects, entered: make(chan struct{}, 1), release: make(chan struct{})}
			first.Config.Archive = paused
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			release := sync.OnceFunc(func() { close(paused.release) })
			done, exited := make(chan error, 1), make(chan struct{})
			var result legacyTimelineResult
			defer func() { release(); cancel(); <-exited }()
			go func() {
				defer close(exited)
				var err error
				result, err = readLegacyTimeline(ctx, first, kind, "tenant-a", "reader")
				done <- err
			}()
			select {
			case <-paused.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("PostgreSQL legacy read missed the actual source Get")
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
					data.LegalHolds["legacy-hold"] = domain.LegalHold{ID: "legacy-hold", TenantID: "tenant-a", Name: "Concurrent Hold", CreatedAt: first.Now()}
					return nil
				})
			}()
			select {
			case err := <-control:
				if err != nil {
					t.Fatal("legacy Get blocked renewal or independent control mutation")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("legacy read retained a lock across PostgreSQL source Get")
			}
			release()
			if err := <-done; err != nil || legacyTimelineCount(result, kind) != 1 {
				t.Fatal("PostgreSQL legacy read did not authenticate the original source")
			}
			observed, current := leaseSnapshot(t, first), leaseSnapshot(t, second)
			if observed.LayoutVersion != 2 || current.LayoutVersion != 2 || current.Exports[claim.ID].Status != domain.ExportStatusCancelled || len(current.ExportLeases) != 0 || current.Policies["tenant-a"].ArchiveDays != 365 || current.LegalHolds["legacy-hold"].ID == "" || !reflect.DeepEqual(observed.Exports[claim.ID], current.Exports[claim.ID]) {
				t.Fatal("legacy read overwrote a cross-replica terminal, lease, Hold or policy update")
			}
			facts := readFacts(t, second)
			if len(facts) != 1 {
				t.Fatal("legacy read fact was missing or duplicated across SQL pools")
			}
			assertLegacyTimelineFact(t, facts[0], kind, "reader")
			key := store.EventKey("tenant-a", "legacy-source")
			source, err := second.archivedEventContext(testCtx, original.Receipts[key])
			if err != nil {
				t.Fatal("could not capture original PostgreSQL archived evidence")
			}
			source.OperationID, source.AggregateID = "hidden-operation", "hidden-aggregate"
			if err := first.Store.UpdateTenant(testCtx, "tenant-a", store.HotFirst, func(view *store.TenantView) error { view.Hot.Events[key] = source; return nil }); err != nil {
				t.Fatal("could not persist owned PostgreSQL hot predicate fault")
			}
			persisted, err := second.Store.Snapshot(testCtx)
			if err != nil || persisted.Events[key].OperationID != "hidden-operation" || persisted.Events[key].AggregateID != "hidden-aggregate" || !reflect.DeepEqual(original.Receipts, persisted.Receipts) || !reflect.DeepEqual(original.Schemas, persisted.Schemas) {
				t.Fatal("independent SQL pool did not observe the actual fault with original receipt/Schema preserved")
			}
			rejected, err := readLegacyTimeline(testCtx, second, kind, "tenant-a", "reader")
			if err == nil || errors.Is(err, domain.ErrNotFound) || !reflect.DeepEqual(rejected, legacyTimelineResult{}) || len(readFacts(t, first)) != 1 {
				t.Fatal("persisted PostgreSQL predicate fault became hidden/partial legacy output or a successful read fact")
			}
		})
	}
}
