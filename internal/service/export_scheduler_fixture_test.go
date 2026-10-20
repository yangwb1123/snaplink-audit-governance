package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Observe every actual successful control Save, not just one lucky polling
// snapshot. This does not enforce or repair the production scheduler's budget.
// The embedded backend retains real deep-copy Save and independent trail append.
type schedulerObservationBackend struct {
	*checkpointBackend
	observationMu sync.Mutex
	peakRunning   int
	overTenant    bool
}

func newSchedulerObservationBackend() *schedulerObservationBackend {
	return &schedulerObservationBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
}

func (b *schedulerObservationBackend) Save(ctx context.Context, data *store.Snapshot) error {
	if err := b.checkpointBackend.Save(ctx, data); err != nil {
		return err
	}
	b.observationMu.Lock()
	defer b.observationMu.Unlock()
	total, tenants := 0, map[string]int{}
	for _, job := range data.Exports {
		if job.Status == "running" {
			total++
			tenants[job.TenantID]++
			if tenants[job.TenantID] > ExportTenantConcurrency {
				b.overTenant = true
			}
		}
	}
	if total > b.peakRunning {
		b.peakRunning = total
	}
	return nil
}

func (b *schedulerObservationBackend) assertCompletedWithinCeilings(t *testing.T, ids []string) {
	t.Helper()
	b.observationMu.Lock()
	peak, overTenant := b.peakRunning, b.overTenant
	b.observationMu.Unlock()
	if peak != ExportConcurrency || overTenant {
		t.Fatal("a real successful scheduler Save violated the shared running ceilings")
	}
	data, err := b.Load(testCtx)
	if err != nil {
		t.Fatal("could not inspect the actual terminal Saves")
	}
	facts := map[string]int{}
	for _, fact := range data.AdminActions {
		if fact.Action == domain.AdminActionExportCompleted {
			facts[fact.TargetID]++
		}
	}
	for _, id := range ids {
		job := data.Exports[id]
		if job.Status != "completed" || job.FinishedAt == nil || job.ObjectPath == "" || job.Digest == "" || job.EventCount != 0 || facts[id] != 1 {
			t.Fatal("scheduler did not commit each original empty export exactly once")
		}
	}
	if len(data.ExportLeases) != 0 || len(data.ExportPlans) != 0 || len(data.ExportCheckpoints) != 0 {
		t.Fatal("completed scheduling left active execution metadata")
	}
}

// Joining queue loops alone is not sufficient: they intentionally detach
// already authorized executors. Release Put, stop the loops, then observe both
// dispatchers and all workers actually exit before closing fixture resources.
func awaitSchedulerFixtureIdle(t *testing.T, services ...*Service) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		idle := true
		for _, svc := range services {
			svc.exportMu.Lock()
			idle = idle && !svc.exportDispatching && len(svc.exportActive) == 0
			svc.exportMu.Unlock()
		}
		if idle {
			return
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Error("scheduler fixture still owns a dispatcher or executor after cleanup")
			return
		}
	}
}
