package service

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type cancelUnknownBackend struct {
	*scriptedConflictBackend
	primary error
	armed   bool
}

func (b *cancelUnknownBackend) Save(ctx context.Context, data *store.Snapshot) error {
	if err := b.scriptedConflictBackend.Save(ctx, data); err != nil {
		return err
	}
	if b.armed && data.Exports["unknown-cancel"].Status == domain.ExportStatusCancelled {
		return b.primary
	}
	return nil
}

func TestCancelExportUnknownDurableSaveReconcilesWithoutDuplicateFact(t *testing.T) {
	backend := &cancelUnknownBackend{scriptedConflictBackend: &scriptedConflictBackend{data: store.NewSnapshot()}, primary: errors.New("post-commit failure")}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	pauseExportDispatch(svc)
	seedActionExport(t, svc, "unknown-cancel", "pending")
	backend.armed = true
	if result, err := svc.CancelExport(testCtx, "tenant-a", "operator", "unknown-cancel"); !errors.Is(err, backend.primary) || result.ID != "" {
		t.Fatal("unknown save became success")
	}
	backend.armed = false
	saves := backend.saves
	if result, err := svc.CancelExport(testCtx, "tenant-a", "operator", "unknown-cancel"); err != nil || result.Status != domain.ExportStatusCancelled || backend.saves != saves {
		t.Fatal("reconciliation rewrote committed cancellation", err)
	}
	facts := 0
	for _, fact := range leaseSnapshot(t, svc).AdminActions {
		if fact.Action == domain.AdminActionExportCancelled {
			facts++
		}
	}
	if facts != 1 {
		t.Fatal("unknown commit duplicated cancellation fact")
	}
}

func TestExportActionsFileRestartPreservesCancellationAndRetryLineage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := newServiceOn(t, st)
	seedTestDomain(t, svc)
	pauseExportDispatch(svc)
	source := seedActionExport(t, svc, "restart-cancel", "pending")
	if _, err := svc.CancelExport(testCtx, "tenant-a", "operator", source.ID); err != nil {
		t.Fatal(err)
	}
	child, err := svc.RetryExport(testCtx, "tenant-a", "operator", source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after := newServiceOn(t, reopened)
	pauseExportDispatch(after)
	result, err := after.RetryExport(testCtx, "tenant-a", "operator", source.ID)
	if err != nil || result.ID != child.ID {
		t.Fatal("restart duplicated retry child", err)
	}
	data := leaseSnapshot(t, after)
	if data.Exports[source.ID].Status != domain.ExportStatusCancelled || data.Exports[source.ID].FailureCode != domain.ExportFailureCancelled || data.Exports[child.ID].RetryOf != source.ID || len(data.ExportLeases) != 0 || len(data.Exports) != 2 {
		t.Fatal("restart dropped cancellation, lineage or lease fencing")
	}
}
