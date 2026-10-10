package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportQueueAdmissionIsBoundedAndAuditedAtomically(t *testing.T) {
	for _, global := range []bool{false, true} {
		t.Run(fmt.Sprint(global), func(t *testing.T) {
			svc := testService(t, true)
			at := time.Unix(1700000010, 0).UTC()
			count := MaxPendingExportsPerTenant
			if global {
				count = MaxPendingExports
			}
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				for i := 0; i < count; i++ {
					tenant := "tenant-a"
					if global {
						tenant = fmt.Sprintf("other-%d", i)
					}
					id := fmt.Sprintf("queued-%d", i)
					data.Exports[id] = domain.ExportJob{ID: id, TenantID: tenant, Status: "pending"}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.CreateExport(testCtx, "tenant-a", "actor", domain.Query{From: at, To: at.Add(time.Second)}); !errors.Is(err, domain.ErrQuotaExceeded) {
				t.Fatal(err)
			}
			if err := svc.Store.ReadControl(testCtx, func(data *store.Snapshot) error {
				if len(data.Exports) != count {
					t.Fatalf("exports=%d", len(data.Exports))
				}
				for _, fact := range data.AdminActions {
					if fact.Action == domain.AdminActionExportCreated {
						t.Fatal("job creation audited without a job", fact)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExportDownloadArchiveIOReleasesStoreLock(t *testing.T) {
	svc := testService(t, true)
	if err := svc.Config.Archive.Put(testCtx, "exports/lock-test.jsonl", []byte("sealed-fixture")); err != nil {
		t.Fatal(err)
	}
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		data.Exports["export-lock"] = domain.ExportJob{ID: "export-lock", TenantID: "tenant-a", Status: "completed", ObjectPath: "exports/lock-test.jsonl"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	paused := &queryPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
	svc.Config.Archive = paused
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := svc.ReadExportObject(ctx, "tenant-a", "export-lock"); done <- err }()
	select {
	case <-paused.entered:
	case <-ctx.Done():
		t.Fatal("Get not entered")
	}
	saved := make(chan error, 1)
	go func() {
		saved <- svc.Store.UpdateControl(ctx, func(data *store.Snapshot) error {
			data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a"}
			return nil
		})
	}()
	select {
	case err := <-saved:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("archive Get held Store.mu")
	}
	close(paused.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("download did not resume")
	}
}
