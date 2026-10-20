package service

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

// Only the explicit disposable lease-suite DSN is accepted. This is not a
// mocked shared Store: capture and recheck use a genuinely independent pool.
func TestPostgresExportAccessCurrentHoldChangeFencesDetailAndDownload(t *testing.T) {
	for _, surface := range []string{"detail", "download-audit"} {
		t.Run(surface, func(t *testing.T) {
			first, second, objects, clock, old := postgresSealedCheckpoint(t)
			clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
			second.runExport(testCtx, old.ID)
			job := leaseSnapshot(t, first).Exports[old.ID]
			if job.Status != "completed" || job.ObjectPath != old.Checkpoint.ObjectPath {
				t.Fatal("cross-pool fixture did not genuinely resume the sealed checkpoint")
			}
			accessNonmatchingHold(t, first)
			seedLeaseExport(t, first, "healthy-access-neighbor", "tenant-a")
			healthy, won, err := first.claimPendingExport(testCtx, "healthy-access-neighbor")
			if err != nil || !won {
				t.Fatal("could not claim neighboring executor")
			}
			boundary := newCheckpointHoldBoundaryArchive(objects)
			second.Config.Archive = boundary
			var once sync.Once
			release := func() { once.Do(func() { close(boundary.release) }) }
			done, exited := make(chan error, 1), make(chan struct{})
			t.Cleanup(func() { release(); <-exited })
			puts := objects.puts.Load()
			go func() { defer close(exited); done <- callExportAccess(t, testCtx, second, surface, "tenant-a", job.ID) }()
			requireCheckpointContext(t, boundary.entered)
			clock.Store(first.Now().Add(ExportHeartbeatInterval).UnixNano())
			if err := first.renewExportLease(testCtx, healthy); err != nil {
				t.Fatal("independent SQL pool could not renew during access Get")
			}
			if _, err := first.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "matching", Reason: "case"}); err != nil {
				t.Fatal("independent SQL pool could not change current Hold")
			}
			release()
			if err := awaitExportAccess(t, done); !errors.Is(err, domain.ErrConflict) {
				t.Fatal("access reused a stale Hold set across independent SQL pools")
			}
			data := leaseSnapshot(t, first)
			if !reflect.DeepEqual(data.Exports[job.ID], job) || data.Exports[healthy.ID].Status != "running" || len(data.ExportLeases) != 1 || len(data.ExportCheckpoints) != 0 || objects.puts.Load() != puts || checkpointFactCount(t, first, domain.AdminActionEventExport) != 0 {
				t.Fatal("stale access lost terminal state, neighboring lease or output isolation")
			}
			if err := callExportAccess(t, testCtx, second, surface, "tenant-a", job.ID); !errors.Is(err, domain.ErrForbidden) {
				t.Fatal("fresh cross-pool access ignored the committed matching Hold")
			}
		})
	}
}
