package service

import (
	"context"
	"crypto/hmac"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

var errExportCheckpointInvalid = fmt.Errorf("%w: invalid export checkpoint metadata", domain.ErrConflict)

// Purpose-separated keyed bindings never persist another copy of the original
// filters (including a sensitive search digest or an expired query cursor).
// Execution format v1 is fixed JSONL sealed with export:v2:, not a build ID.
func (s *Service) exportCheckpointBinding(job domain.ExportJob) string {
	query := job.Query
	query.From, query.To = query.From.UTC(), query.To.UTC()
	return s.queryMAC("export.checkpoint.scope.v1", struct {
		ID, Tenant, Requester, RetryOf string
		CreatedAt                      time.Time
		StartedAt                      *time.Time
		Query                          domain.Query
	}{job.ID, job.TenantID, job.RequestedBy, job.RetryOf, job.CreatedAt.UTC(), job.StartedAt, query})
}

func (s *Service) signExportCheckpoint(checkpoint store.ExportCheckpoint) string {
	checkpoint.Signature = ""
	return s.queryMAC("export.checkpoint.sealed.v1", checkpoint)
}

func (s *Service) validExportCheckpoint(job domain.ExportJob, cp store.ExportCheckpoint, now time.Time) bool {
	prefix := "exports/" + safeName(job.ID) + "-"
	suffix := strings.TrimSuffix(strings.TrimPrefix(cp.ObjectPath, prefix), ".jsonl")
	ownerHash, hashErr := hex.DecodeString(suffix)
	return cp.Version == 1 && cp.SealVersion == 2 && job.ID != "" && job.TenantID != "" &&
		cp.JobID == job.ID && cp.TenantID == job.TenantID && !job.CreatedAt.IsZero() &&
		job.Status == "running" && job.FinishedAt == nil && job.StartedAt != nil && !job.StartedAt.IsZero() &&
		job.ObjectPath == "" && job.Digest == "" && job.EventCount == 0 &&
		!cp.CreatedAt.IsZero() && !cp.CreatedAt.Before(*job.StartedAt) && !now.Before(cp.CreatedAt) &&
		cp.EventCount >= 0 && cp.EventCount <= store.MaxQuerySnapshotEvents &&
		strings.HasPrefix(cp.ObjectPath, prefix) && strings.HasSuffix(cp.ObjectPath, ".jsonl") &&
		hashErr == nil && len(ownerHash) == 16 && suffix == strings.ToLower(suffix) &&
		exportDigestPattern.MatchString(cp.SealedDigest) && exportDigestPattern.MatchString(cp.Digest) &&
		hmac.Equal([]byte(cp.Binding), []byte(s.exportCheckpointBinding(job))) &&
		hmac.Equal([]byte(cp.Signature), []byte(s.signExportCheckpoint(cp)))
}

func exportObjectKey(claim exportClaim) string {
	return fmt.Sprintf("exports/%s-%s.jsonl", safeName(claim.ID), domain.HashBytes([]byte(claim.Owner))[:32])
}

// Only the exact unexpired owner may publish a checkpoint after a successful
// Put. A failed/unknown Save never triggers a guessed completion or a deletion.
func (s *Service) saveExportCheckpoint(ctx context.Context, claim exportClaim, sealed, plain []byte, count int) (store.ExportCheckpoint, error) {
	if claim.Plan.Version != 0 && (count != len(claim.Plan.Members) || !exportPlainMatchesPlan(plain, claim.Plan) ||
		(claim.Plan.Version == 2 && exportGeneratedMembers(claim.Plan) != count)) {
		return store.ExportCheckpoint{}, errExportPlanInvalid
	}
	cp := store.ExportCheckpoint{Version: 1, SealVersion: 2, JobID: claim.ID, TenantID: claim.TenantID,
		Binding: s.exportCheckpointBinding(claim.ExportJob), ObjectPath: exportObjectKey(claim),
		SealedDigest: domain.HashBytes(sealed), Digest: domain.HashBytes(plain), EventCount: count, CreatedAt: s.Now().UTC()}
	cp.Signature = s.signExportCheckpoint(cp)
	err := s.Store.UpdateControlChecked(ctx, func(data *store.Snapshot) (bool, error) {
		job := data.Exports[claim.ID]
		if !ownsExportLease(data, job, claim, s.Now()) {
			return false, errExportLeaseLost
		}
		if !s.validExportCheckpoint(job, cp, s.Now()) {
			return false, errExportCheckpointInvalid
		}
		if previous, exists := data.ExportCheckpoints[claim.ID]; exists {
			if previous != cp {
				return false, errExportCheckpointInvalid
			}
			return false, nil
		}
		if !s.exportPlanMatches(data, job, claim) || (claim.Plan.Version != 0 && !s.exportPlanSchemasMatch(data, claim.Plan)) {
			return false, errExportPlanInvalid
		}
		data.ExportCheckpoints[claim.ID] = cp
		delete(data.ExportPlans, claim.ID)
		s.appendAdminAction(data, s.adminAction(job.TenantID, "governance-worker", domain.AdminActionExportCheckpointed, "export", job.ID, "stage=sealed version=1"))
		return true, nil
	})
	if err != nil {
		return store.ExportCheckpoint{}, err
	}
	return cp, nil
}

func (s *Service) exportHoldBinding(data *store.Snapshot, tenantID string) string {
	holds := map[string]domain.LegalHold{}
	for id, hold := range data.LegalHolds {
		if hold.TenantID == tenantID && hold.ReleasedAt == nil {
			holds[id] = hold
		}
	}
	return s.queryMAC("export.checkpoint.holds.v1", holds)
}

// A checkpoint cannot disappear/change underneath a finalizer, even if a
// malicious/obsolete control writer left its lease owner untouched.
func (s *Service) terminalExportCheckpointMatches(data *store.Snapshot, job domain.ExportJob, claim exportClaim, status, path, digest string, count int) bool {
	cp, exists := data.ExportCheckpoints[job.ID]
	if !exists {
		return claim.Checkpoint.Version == 0
	}
	if cp != claim.Checkpoint || !s.validExportCheckpoint(job, cp, s.Now()) {
		return false
	}
	return status != "completed" || (path == cp.ObjectPath && digest == cp.Digest && count == cp.EventCount &&
		claim.HoldBinding != "" && claim.HoldBinding == s.exportHoldBinding(data, job.TenantID))
}
