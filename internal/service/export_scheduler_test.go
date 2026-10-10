package service

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestNextExportRoundRobinAndSharedCeilings(t *testing.T) {
	at := time.Unix(1700000010, 0).UTC()
	jobs := map[string]domain.ExportJob{}
	for _, tenant := range []string{"a", "b", "c"} {
		for i := 0; i < 3; i++ {
			id := fmt.Sprintf("%s-%d", tenant, i)
			jobs[id] = domain.ExportJob{ID: id, TenantID: tenant, CreatedAt: at.Add(time.Duration(i) * time.Second), Status: "pending"}
		}
	}
	if got := nextExport(jobs, nil, "a"); got.ID != "b-0" {
		t.Fatal(got)
	}
	if got := nextExport(jobs, nil, "z"); got.ID != "a-0" {
		t.Fatal(got)
	}
	active := map[string]string{"a-0": "a"}
	if got := nextExport(jobs, active, ""); got.ID != "b-0" {
		t.Fatal(got)
	}
	value := jobs["a-0"]
	value.Status = "running"
	jobs[value.ID] = value
	if got := nextExport(jobs, active, ""); got.ID != "b-0" {
		t.Fatal("reservation double counted", got)
	}
	if exportClaimHasCapacity(jobs, "a") || !exportClaimHasCapacity(jobs, "b") {
		t.Fatal("tenant claim budget")
	}
	value = jobs["b-0"]
	value.Status = "running"
	jobs[value.ID] = value
	if got := nextExport(jobs, nil, "b"); got.ID != "" || exportClaimHasCapacity(jobs, "c") {
		t.Fatal("global budget", got)
	}
}

type schedulerPausedArchive struct {
	archive.Store
	entered chan string
	release chan struct{}
}

func (a *schedulerPausedArchive) Put(ctx context.Context, key string, data []byte) error {
	select {
	case a.entered <- key:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-a.release:
		return a.Store.Put(ctx, key, data)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestExportSchedulerBoundedAcrossServiceReplicas(t *testing.T) {
	svc := testService(t, true)
	other, err := New(svc.Store, svc.Config)
	if err != nil {
		t.Fatal(err)
	}
	paused := &schedulerPausedArchive{Store: svc.Config.Archive, entered: make(chan string, 16), release: make(chan struct{})}
	svc.Config.Archive, other.Config.Archive = paused, paused
	at := time.Unix(1700000010, 0).UTC()
	ids := []string{}
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		for _, tenant := range []string{"tenant-a", "tenant-b", "tenant-c"} {
			data.Tenants[tenant] = domain.Tenant{ID: tenant, Active: true}
			for i := 0; i < 4; i++ {
				id := fmt.Sprintf("export-sched-%s-%d", tenant, i)
				ids = append(ids, id)
				data.Exports[id] = domain.ExportJob{ID: id, TenantID: tenant, Status: "pending", CreatedAt: at.Add(time.Duration(i) * time.Second), Query: domain.Query{From: at, To: at.Add(time.Second)}}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(paused.release) }) }
	defer release()
	// Concurrent resume passes do not enqueue one goroutine for every pending
	// record; shared durable CAS prevents multiplying the work on replicas.
	var wg sync.WaitGroup
	for _, replica := range []*Service{svc, other} {
		wg.Add(1)
		go func(s *Service) {
			defer wg.Done()
			_, err := s.ResumePendingExports(testCtx, "tenant-a")
			if err != nil {
				t.Error(err)
			}
		}(replica)
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// A replica can lose a capacity race and retain pending work. Run the
	// same periodic wake used by the API; completion alone is not a lease.
	go svc.RunExportQueue(ctx)
	go other.RunExportQueue(ctx)
	for i := 0; i < ExportConcurrency; i++ {
		select {
		case <-paused.entered:
		case <-ctx.Done():
			t.Fatal("workers did not reach Put")
		}
	}
	if err := svc.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		total := 0
		tenantRunning := map[string]int{}
		for _, job := range data.Exports {
			if job.Status == "running" {
				total++
				tenantRunning[job.TenantID]++
			}
		}
		if total != ExportConcurrency {
			t.Errorf("running=%d", total)
		}
		for tenant, count := range tenantRunning {
			if count > ExportTenantConcurrency {
				t.Errorf("tenant %s running=%d", tenant, count)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	release()
	for {
		completed := 0
		if err := svc.Store.ReadControl(ctx, func(data *store.Snapshot) error {
			for _, id := range ids {
				job := data.Exports[id]
				if job.Status == "failed" {
					t.Errorf("export failed: %s", id)
				}
				if job.Status == "completed" {
					completed++
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if completed == len(ids) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("only %d of %d exports completed", completed, len(ids))
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestExportClaimCapacityDoesNotMutatePending(t *testing.T) {
	svc := testService(t, false)
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		for _, id := range []string{"running-a", "running-b"} {
			data.Exports[id] = domain.ExportJob{ID: id, TenantID: id, Status: "running"}
		}
		data.Exports["queued"] = domain.ExportJob{ID: "queued", TenantID: "tenant-a", Status: "pending"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := svc.claimPendingExport(testCtx, "queued"); err != nil || claimed {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
	if err := svc.Store.ReadControl(testCtx, func(data *store.Snapshot) error {
		if job := data.Exports["queued"]; job.Status != "pending" || job.StartedAt != nil {
			t.Fatal(job)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
