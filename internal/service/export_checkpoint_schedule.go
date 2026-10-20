package service

import (
	"sort"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// A selected/sealed job is already counted as running: replacement of its expired
// owner consumes the same slot, never a new pending admission or extra slot.
// Legacy over-budget state blocks takeovers until it converges; no cap grows.
func exportCheckpointHasCapacity(jobs map[string]domain.ExportJob, tenantID string) bool {
	total, local := 0, 0
	for _, job := range jobs {
		if job.Status == "running" {
			total++
			if job.TenantID == tenantID {
				local++
			}
		}
	}
	return total <= ExportConcurrency && local <= ExportTenantConcurrency
}

func nextRunnableExport(data *store.Snapshot, active map[string]string, after string, now time.Time) domain.ExportJob {
	var candidates []domain.ExportJob
	for id, job := range data.Exports {
		_, sealed := data.ExportCheckpoints[id]
		_, planned := data.ExportPlans[id]
		if !sealed && !planned {
			continue
		}
		lease := data.ExportLeases[id]
		if _, reserved := active[id]; reserved || job.ID != id || job.Status != "running" || job.FinishedAt != nil ||
			!validExportLease(job, lease) || lease.ExpiresAt.After(now) || !exportCheckpointHasCapacity(data.Exports, job.TenantID) {
			continue
		}
		candidates = append(candidates, job)
	}
	// Reclaim existing slots first. Both paths still use the same finite
	// dispatcher and the authoritative shared claim CAS revalidates metadata.
	if len(candidates) == 0 {
		return nextExport(data.Exports, active, after)
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.TenantID != b.TenantID {
			return a.TenantID < b.TenantID
		}
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.ID < b.ID
	})
	for _, job := range candidates {
		if job.TenantID > after {
			return job
		}
	}
	return candidates[0]
}
