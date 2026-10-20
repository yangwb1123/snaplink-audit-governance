package service

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

// The stable generation identity excludes only mutable progress and its MAC.
// It still binds every original member/schema, job/query/lineage and format.
func (s *Service) exportGenerationID(plan store.ExportPlan) string {
	plan.Chunks, plan.Signature = nil, ""
	return s.queryMAC("export.generation.plan.v1", plan)
}

func (s *Service) exportChunkBinding(plan store.ExportPlan, index int, chunk store.ExportPlanChunk) []byte {
	return []byte(s.queryMAC("export.generation.chunk.v1", struct {
		Plan, Object                     string
		Index, First, Count, SealVersion int
	}{s.exportGenerationID(plan), chunk.ObjectPath, index, chunk.First, chunk.Count, chunk.SealVersion}))
}

func (s *Service) validExportGeneration(plan store.ExportPlan, now time.Time) bool {
	if plan.Version == 1 {
		return len(plan.Chunks) == 0
	}
	page := store.ExportGenerationPageEvents
	if len(plan.Members) <= page || len(plan.Chunks) > (len(plan.Members)+page-1)/page {
		return false
	}
	prefix := "export-chunks/" + s.exportGenerationID(plan) + "/"
	previous := plan.CreatedAt
	for i, chunk := range plan.Chunks {
		count := min(page, len(plan.Members)-i*page)
		stem := fmt.Sprintf("%s%04d-", prefix, i)
		attempt := strings.TrimSuffix(strings.TrimPrefix(chunk.ObjectPath, stem), ".bin")
		if chunk.SealVersion != 2 || chunk.First != i*page || chunk.Count != count ||
			!strings.HasPrefix(chunk.ObjectPath, stem) || !strings.HasSuffix(chunk.ObjectPath, ".bin") ||
			len(attempt) != 32 || !exportDigestPattern.MatchString(attempt+attempt) ||
			!exportDigestPattern.MatchString(chunk.SealedDigest) || !exportDigestPattern.MatchString(chunk.Digest) ||
			chunk.CreatedAt.IsZero() || chunk.CreatedAt.Before(previous) || now.Before(chunk.CreatedAt) {
			return false
		}
		previous = chunk.CreatedAt
	}
	return true
}

// Recheck after remote work and the independent read-trail append. A changed
// Hold set is not permission to commit progress based on the older observation.
func (s *Service) checkExportGenerationAccess(ctx context.Context, claim exportClaim) error {
	return s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		job := data.Exports[claim.ID]
		if !ownsExportLease(data, job, claim, s.Now()) {
			return errExportLeaseLost
		}
		if !s.exportPlanMatches(data, job, claim) || !s.exportPlanSchemasMatch(data, claim.Plan) ||
			claim.HoldBinding == "" || claim.HoldBinding != s.exportHoldBinding(data, job.TenantID) {
			return errExportPlanInvalid
		}
		return nil
	})
}

func exportGeneratedMembers(plan store.ExportPlan) int {
	if len(plan.Chunks) == 0 {
		return 0
	}
	last := plan.Chunks[len(plan.Chunks)-1]
	return last.First + last.Count
}

// No raw bytes enter control. A Save error/unknown result stops this executor;
// only a later expired owner may inspect actual durable progress. The old MAC
// is an exact CAS precondition, so progress cannot fork, regress or skip pages.
func (s *Service) saveExportChunk(ctx context.Context, claim exportClaim, chunk store.ExportPlanChunk) (store.ExportPlan, error) {
	plan := copyExportPlan(claim.Plan)
	plan.Chunks = append(plan.Chunks, chunk)
	plan.Signature = s.signExportPlan(plan)
	err := s.Store.UpdateControlChecked(ctx, func(data *store.Snapshot) (bool, error) {
		job := data.Exports[claim.ID]
		if !ownsExportLease(data, job, claim, s.Now()) {
			return false, errExportLeaseLost
		}
		_, sealed := data.ExportCheckpoints[claim.ID]
		if claim.Plan.Version != 2 || sealed || !s.exportPlanMatches(data, job, claim) ||
			!s.validExportPlan(job, plan, s.Now()) || !s.exportPlanSchemasMatch(data, plan) ||
			!exportCheckpointHasCapacity(data.Exports, job.TenantID) || claim.HoldBinding == "" ||
			claim.HoldBinding != s.exportHoldBinding(data, job.TenantID) {
			return false, errExportPlanInvalid
		}
		data.ExportPlans[job.ID] = copyExportPlan(plan)
		s.appendAdminAction(data, s.adminAction(job.TenantID, "governance-worker", domain.AdminActionExportProgressed, "export", job.ID,
			fmt.Sprintf("stage=generating version=1 pages=%d", len(plan.Chunks))))
		return true, nil
	})
	if err != nil {
		return store.ExportPlan{}, err
	}
	return plan, nil
}

func (s *Service) authenticateExportChunk(plan store.ExportPlan, index int, sealed []byte) ([]byte, error) {
	chunk := plan.Chunks[index]
	if !bytes.HasPrefix(sealed, []byte("export:v2:")) || domain.HashBytes(sealed) != chunk.SealedDigest {
		return nil, errExportPlanInvalid
	}
	plain, err := security.DecryptExport(sealed, s.Config.EncryptionKey, s.exportChunkBinding(plan, index, chunk))
	page := plan
	page.Members = plan.Members[chunk.First : chunk.First+chunk.Count]
	if err != nil || domain.HashBytes(plain) != chunk.Digest || !exportPlainMatchesPlan(plain, page) {
		return nil, errExportPlanInvalid
	}
	return plain, nil
}
