package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Operational defaults, not a progress or availability SLA. Persisted UTC
// deadlines require synchronized replica clocks. No lease is inferred for old
// running jobs without metadata; their StartedAt/CreatedAt recovery is retained.
const ExportLeaseDuration = 2 * time.Minute
const ExportHeartbeatInterval = 30 * time.Second

var errExportLeaseLost = errors.New("export execution lease lost")

type exportClaim struct {
	domain.ExportJob
	Owner       string
	Checkpoint  store.ExportCheckpoint
	Plan        store.ExportPlan
	HoldBinding string
}

type exportClaimKey struct{}

func exportClaimContext(ctx context.Context, claim exportClaim) context.Context {
	return context.WithValue(ctx, exportClaimKey{}, claim)
}

func exportClaimFromContext(ctx context.Context) (exportClaim, bool) {
	claim, ok := ctx.Value(exportClaimKey{}).(exportClaim)
	return claim, ok && claim.ID != "" && claim.Owner != "" && claim.TenantID != ""
}

func validExportLease(job domain.ExportJob, lease store.ExportLease) bool {
	return job.ID != "" && job.TenantID != "" && lease.Version == 1 &&
		lease.JobID == job.ID && lease.TenantID == job.TenantID &&
		lease.Owner != "" && job.StartedAt != nil && !job.StartedAt.IsZero() &&
		!lease.HeartbeatAt.IsZero() && !lease.HeartbeatAt.Before(*job.StartedAt) &&
		lease.ExpiresAt.Equal(lease.HeartbeatAt.Add(ExportLeaseDuration))
}

func ownsExportLease(data *store.Snapshot, job domain.ExportJob, claim exportClaim, now time.Time) bool {
	lease, ok := data.ExportLeases[job.ID]
	return ok && validExportLease(job, lease) && job.ID == claim.ID && job.TenantID == claim.TenantID &&
		lease.Owner == claim.Owner && job.Status == "running" && job.FinishedAt == nil &&
		!now.Before(lease.HeartbeatAt) && lease.ExpiresAt.After(now)
}

func (s *Service) renewExportLease(ctx context.Context, claim exportClaim) error {
	return s.Store.UpdateControlChecked(ctx, func(data *store.Snapshot) (bool, error) {
		job := data.Exports[claim.ID]
		now := s.Now()
		if !ownsExportLease(data, job, claim, now) {
			return false, errExportLeaseLost
		}
		lease := data.ExportLeases[claim.ID]
		if !now.After(lease.HeartbeatAt) {
			return false, nil
		}
		lease.HeartbeatAt = now
		lease.ExpiresAt = now.Add(ExportLeaseDuration)
		data.ExportLeases[claim.ID] = lease
		return true, nil
	})
}

func (s *Service) checkExportLease(ctx context.Context) error {
	claim, ok := exportClaimFromContext(ctx)
	if !ok {
		return errExportLeaseLost
	}
	return s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		if !ownsExportLease(data, data.Exports[claim.ID], claim, s.Now()) {
			return errExportLeaseLost
		}
		return nil
	})
}

// Only leased work is reaped here. The API queue calls this before its periodic
// dispatch. Legacy running jobs remain the governance worker's age-based task.
// No archive I/O, no original-job requeue and no mutation on an idle pass.
func (s *Service) recoverExpiredExportLeases(ctx context.Context) (int, error) {
	return s.recoverExportJobs(ctx, "", false)
}

func (s *Service) recoverExportJobs(ctx context.Context, tenantID string, includeLegacy bool) (int, error) {
	recovered := 0
	err := s.Store.UpdateControlChecked(ctx, func(data *store.Snapshot) (bool, error) {
		recovered = 0 // CAS retries count only their committed attempt
		now := s.Now()
		for jobID, job := range data.Exports {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if (tenantID != "" && job.TenantID != tenantID) || job.Status != "running" || job.FinishedAt != nil {
				continue
			}
			detail, failure, failureCode := "", "", ""
			lease, leased := data.ExportLeases[jobID]
			_, checkpointed := data.ExportCheckpoints[jobID]
			_, planned := data.ExportPlans[jobID]
			if err := s.validExportArtifacts(data, job, now); err != nil {
				return false, err
			}
			if (checkpointed || planned) && !leased {
				return false, errExportPlanInvalid
			}
			if leased {
				if job.ID != jobID || !validExportLease(job, lease) {
					return false, fmt.Errorf("%w: invalid export lease metadata", domain.ErrConflict)
				}
				if lease.ExpiresAt.After(now) || checkpointed || planned {
					continue // expired planned/sealed work is reclaimed by the bounded dispatcher
				}
				failureCode = domain.ExportFailureLease
				detail = "lease_expired status=failed"
				failure = "export interrupted: execution lease expired; re-request the export"
			} else {
				started := job.CreatedAt
				if job.StartedAt != nil && !job.StartedAt.IsZero() {
					started = *job.StartedAt
				}
				if !includeLegacy || !started.Before(now.Add(-s.Config.StuckExportAge)) {
					continue
				}
				failureCode = domain.ExportFailureLegacy
				detail = fmt.Sprintf("stuck_running_since=%s status=failed", started.Format(time.RFC3339))
				failure = fmt.Sprintf("export interrupted: stuck in running since %s; re-request the export", started.Format(time.RFC3339))
			}
			job.Status, job.Error, job.FailureCode = "failed", failure, failureCode
			job.FinishedAt = &now
			job.ObjectPath, job.Digest, job.EventCount = "", "", 0
			data.Exports[jobID] = job
			delete(data.ExportLeases, jobID)
			s.appendAdminAction(data, s.adminAction(job.TenantID, "governance-worker", domain.AdminActionExportRecovered, "export", jobID, detail))
			recovered++
		}
		return recovered != 0, nil
	})
	if err != nil {
		return 0, err // do not report a discarded or unknown Save as recovered
	}
	return recovered, nil
}
