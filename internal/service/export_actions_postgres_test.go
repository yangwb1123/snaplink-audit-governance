package service

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func postgresActionServices(t *testing.T) (*Service, *Service) {
	t.Helper()
	first, second := newExportLeasePostgres(t) // dedicated, disposable opt-in DB
	pauseExportDispatch(first)
	pauseExportDispatch(second)
	if err := first.Store.Update(testCtx, func(data *store.Snapshot) error {
		data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if leaseSnapshot(t, first).LayoutVersion != 2 {
		t.Fatal("must exercise real PostgreSQL v2 control CAS")
	}
	return first, second
}

func TestPostgresExportActionsCancelAndRetryAcrossIndependentReplicas(t *testing.T) {
	first, second := postgresActionServices(t)
	source := seedActionExport(t, first, "pg-cancel-retry", "pending")
	claim, won, err := first.claimPendingExport(testCtx, source.ID)
	if err != nil || !won {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, replica := range []*Service{first, second} {
		wg.Add(1)
		go func(s *Service) {
			defer wg.Done()
			<-start
			result, err := s.CancelExport(testCtx, "tenant-a", "operator", source.ID)
			if err != nil || result.Status != domain.ExportStatusCancelled {
				t.Errorf("cancel: %v %+v", err, result)
			}
		}(replica)
	}
	close(start)
	wg.Wait()
	second.finishExport(exportClaimContext(testCtx, claim), source.ID, "completed", "stale-object", "stale-digest", 1, "")
	results := make(chan domain.ExportSummary, 2)
	start = make(chan struct{})
	for _, replica := range []*Service{first, second} {
		wg.Add(1)
		go func(s *Service) {
			defer wg.Done()
			<-start
			result, err := s.RetryExport(testCtx, "tenant-a", "operator", source.ID)
			if err != nil {
				t.Error(err)
				return
			}
			results <- result
		}(replica)
	}
	close(start)
	wg.Wait()
	close(results)
	if len(results) != 2 {
		t.Fatal("retry race lost replies")
	}
	child := <-results
	if other := <-results; other.ID != child.ID {
		t.Fatal("independent SQL replicas created duplicate successors")
	}
	data := leaseSnapshot(t, first)
	if len(data.Exports) != 2 || data.Exports[source.ID].Status != domain.ExportStatusCancelled || data.Exports[source.ID].ObjectPath != "" || len(data.ExportLeases) != 0 || child.RetryOf != source.ID {
		t.Fatal("cancel/retry lost control isolation or lease fence")
	}
	cancelled, retried, created := 0, 0, 0
	for _, fact := range data.AdminActions {
		if fact.Action == domain.AdminActionExportCancelled {
			cancelled++
		}
		if fact.Action == domain.AdminActionExportRetried {
			retried++
		}
		if fact.Action == domain.AdminActionExportCreated {
			created++
		}
	}
	if cancelled != 1 || retried != 1 || created != 1 {
		t.Fatal("cross-replica actions duplicated self-audit")
	}
	if _, err := second.RetryExport(testCtx, "tenant-a", "different", source.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatal("replica changed owner policy")
	}
}

func TestPostgresExportRetrySharedTenantBudgetAndCancellationRelease(t *testing.T) {
	first, second := postgresActionServices(t)
	source := seedActionExport(t, first, "pg-budget-retry", "failed")
	if err := first.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		for i := 0; i < MaxPendingExportsPerTenant; i++ {
			id := fmt.Sprintf("pending-%d", i)
			data.Exports[id] = domain.ExportJob{ID: id, TenantID: "tenant-a", RequestedBy: "operator", Status: "pending"}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := second.RetryExport(testCtx, "tenant-a", "operator", source.ID); !errors.Is(err, domain.ErrQuotaExceeded) || result.ID != "" {
		t.Fatal("independent replica enlarged tenant budget")
	}
	if _, err := second.CancelExport(testCtx, "tenant-a", "operator", "pending-0"); err != nil {
		t.Fatal(err)
	}
	result, err := first.RetryExport(testCtx, "tenant-a", "operator", source.ID)
	if err != nil || result.RetryOf != source.ID {
		t.Fatal("durable cancellation did not release shared quota", err)
	}
	if again, err := second.RetryExport(testCtx, "tenant-a", "operator", source.ID); err != nil || again.ID != result.ID {
		t.Fatal("full-budget idempotent repeat created/rejected another child", err)
	}
}
