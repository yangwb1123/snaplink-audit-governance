package service

import (
	"context"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Fixed operational ceilings, not throughput promises. Durable claim checks
// enforce the same ceiling across API/worker replicas sharing a control store.
const ExportConcurrency = 2
const ExportTenantConcurrency = 1
const MaxPendingExportsPerTenant = 32
const MaxPendingExports = 128

func exportQueueHasCapacity(jobs map[string]domain.ExportJob, tenant string) bool {
	total, local := 0, 0
	for _, job := range jobs {
		if job.Status == "pending" || job.Status == "running" {
			total++
			if job.TenantID == tenant {
				local++
			}
		}
	}
	return total < MaxPendingExports && local < MaxPendingExportsPerTenant
}

// wakeExports starts one bounded dispatcher, not a goroutine per queued job.
// No scheduler mutex is ever held across a Store call or external archive IO.
func (s *Service) wakeExports(ctx context.Context) {
	s.exportMu.Lock()
	defer s.exportMu.Unlock()
	s.wakeExportsLocked(ctx)
}

// Caller holds exportMu. A finishing executor hands off its reservation and
// any follow-up pass atomically: no idle window before a later Store read.
// Starting the goroutine here does not run Store/archive IO under this lock.
func (s *Service) wakeExportsLocked(ctx context.Context) {
	s.exportWakeGeneration++
	if s.exportDispatching {
		return
	}
	s.exportDispatching = true
	go s.dispatchExports(ctx)
}

func (s *Service) dispatchExports(ctx context.Context) {
	// A finite dispatcher pass. Completions and later resume/create calls wake
	// another pass. A failed store read must not create a hot retry loop.
	for {
		s.exportMu.Lock()
		if len(s.exportActive) >= ExportConcurrency || ctx.Err() != nil {
			s.exportDispatching = false
			s.exportMu.Unlock()
			return
		}
		active := make(map[string]string, len(s.exportActive))
		for id, tenant := range s.exportActive {
			active[id] = tenant
		}
		after := s.exportLastTenant
		generation := s.exportWakeGeneration
		s.exportMu.Unlock()
		var chosen domain.ExportJob
		err := s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
			chosen = nextRunnableExport(data, active, after, s.Now())
			return nil
		})
		s.exportMu.Lock()
		if err != nil || chosen.ID == "" {
			if err == nil && generation != s.exportWakeGeneration {
				s.exportMu.Unlock()
				continue
			}
			s.exportDispatching = false
			s.exportMu.Unlock()
			return
		}
		if s.exportActive == nil {
			s.exportActive = map[string]string{}
		}
		s.exportActive[chosen.ID] = chosen.TenantID
		s.exportLastTenant = chosen.TenantID
		s.exportMu.Unlock()
		go func(jobID string) {
			s.runExport(ctx, jobID)
			// Keep the local reservation through the executor's final bounded
			// Store read, not merely through durable terminal persistence. Idle
			// must mean there is no remaining Store IO or unregistered wake.
			// A claim/read failure can leave pending or checkpointed running
			// work. Do not hot-spin on either; a later queue/worker pass retries.
			progressed := false
			err := s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
				job, ok := data.Exports[jobID]
				progressed = !ok || job.Status == "completed" || job.Status == "failed" || job.Status == domain.ExportStatusCancelled
				return nil
			})
			s.exportMu.Lock()
			delete(s.exportActive, jobID)
			if err == nil && progressed {
				s.wakeExportsLocked(ctx)
			}
			s.exportMu.Unlock()
		}(chosen.ID)
	}
}

// nextExport round-robins tenant identity, oldest-first WITHIN each tenant.
// Local reservations and persisted running claims count once, so multiple
// dispatchers cannot multiply running work through a shared CAS backend.
func nextExport(jobs map[string]domain.ExportJob, active map[string]string, after string) domain.ExportJob {
	occupied := map[string]int{}
	running := map[string]bool{}
	total := 0
	for id, job := range jobs {
		if job.Status == "running" {
			occupied[job.TenantID]++
			total++
			running[id] = true
		}
	}
	for id, tenant := range active {
		if !running[id] {
			occupied[tenant]++
			total++
		}
	}
	if total >= ExportConcurrency {
		return domain.ExportJob{}
	}
	byTenant := map[string]domain.ExportJob{}
	for id, job := range jobs {
		if job.Status != "pending" || occupied[job.TenantID] >= ExportTenantConcurrency {
			continue
		}
		if _, ok := active[id]; ok {
			continue
		}
		current, ok := byTenant[job.TenantID]
		if !ok || job.CreatedAt.Before(current.CreatedAt) || (job.CreatedAt.Equal(current.CreatedAt) && job.ID < current.ID) {
			byTenant[job.TenantID] = job
		}
	}
	tenants := make([]string, 0, len(byTenant))
	for tenant := range byTenant {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)
	if len(tenants) == 0 {
		return domain.ExportJob{}
	}
	for _, tenant := range tenants {
		if tenant > after {
			return byTenant[tenant]
		}
	}
	return byTenant[tenants[0]]
}

func exportClaimHasCapacity(jobs map[string]domain.ExportJob, tenant string) bool {
	total, local := 0, 0
	for _, job := range jobs {
		if job.Status == "running" {
			total++
			if job.TenantID == tenant {
				local++
			}
		}
	}
	return total < ExportConcurrency && local < ExportTenantConcurrency
}
