package service

import (
	"context"
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

// Each page is a distinct bounded service operation, not a fresh deadline per
// receipt/Get. Committed prefix bytes are authenticated, never regenerated.
// Final concatenation/sealing remains whole-object and is not streaming I/O.
func (s *Service) generateExportPages(ctx context.Context, claim exportClaim) (exportClaim, []byte, bool) {
	plain, err := s.readExportGenerationPrefix(ctx, claim)
	for err == nil && exportGeneratedMembers(claim.Plan) < len(claim.Plan.Members) {
		var page []byte
		claim, page, err = s.generateExportPage(exportClaimContext(ctx, claim), claim)
		if err == nil {
			plain = append(plain, page...)
		}
	}
	if err != nil {
		s.logf("export generation deferred job_id=%s category=verification", claim.ID)
		return claim, nil, false
	}
	return claim, plain, true
}

func (s *Service) readExportGenerationPrefix(ctx context.Context, claim exportClaim) ([]byte, error) {
	if len(claim.Plan.Chunks) == 0 {
		return nil, nil
	}
	var plain []byte
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		if err := s.checkExportPlan(opCtx, claim); err != nil {
			return err
		}
		references := make([]store.EventReference, 0, len(claim.Plan.Members))
		for _, member := range claim.Plan.Members {
			references = append(references, member.EventReference)
		}
		if err := s.verifyQueryReferences(opCtx, claim.TenantID, references); err != nil {
			return err
		}
		binding, err := s.exportGenerationAccess(opCtx, claim)
		if err != nil {
			return err
		}
		claim.HoldBinding = binding
		for i, chunk := range claim.Plan.Chunks {
			readCtx, cancel := s.archiveFallbackContext(opCtx)
			sealed, err := s.Config.Archive.Get(readCtx, chunk.ObjectPath)
			cancel()
			if err != nil {
				return err
			}
			page, err := s.authenticateExportChunk(claim.Plan, i, sealed)
			if err != nil {
				return err // never regenerate a missing or unauthentic committed page
			}
			plain = append(plain, page...)
		}
		return s.checkExportGenerationAccess(opCtx, claim)
	})
	if err != nil {
		return nil, err
	}
	return plain, nil
}

// The current Hold evaluates the original query, including later committed
// facts, not just the fixed output members. All source I/O is outside Store.mu.
func (s *Service) exportGenerationAccess(ctx context.Context, claim exportClaim) (string, error) {
	data, err := s.Store.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	hold, blocked, err := s.holdBlockingExportEvidence(ctx, data, claim.ExportJob)
	if err != nil {
		return "", err
	}
	if blocked {
		s.failExportBlocked(exportClaimContext(ctx, claim), claim.ID, hold)
		return "", exportBlockedError{holdID: hold.ID}
	}
	if err := s.recordReadAction(ctx, claim.TenantID, "governance-worker", domain.AdminActionEventRead, "export", claim.ID, "stage=generating verification"); err != nil {
		return "", err
	}
	return s.exportHoldBinding(data, claim.TenantID), nil
}

func (s *Service) generateExportPage(ctx context.Context, claim exportClaim) (exportClaim, []byte, error) {
	var plain []byte
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		if err := s.checkExportPlan(opCtx, claim); err != nil {
			return err
		}
		first := exportGeneratedMembers(claim.Plan)
		count := min(store.ExportGenerationPageEvents, len(claim.Plan.Members)-first)
		if claim.Plan.Version != 2 || count <= 0 {
			return errExportPlanInvalid
		}
		events, err := s.readExportPlanMembers(opCtx, claim, claim.Plan.Members[first:first+count])
		if err != nil {
			return err
		}
		claim.HoldBinding, err = s.exportGenerationAccess(opCtx, claim)
		if err != nil {
			return err
		}
		plain, err = encodeExportEvents(opCtx, events)
		if err != nil {
			return err
		}
		index := len(claim.Plan.Chunks)
		chunk := store.ExportPlanChunk{SealVersion: 2, First: first, Count: count, CreatedAt: s.Now().UTC(),
			ObjectPath: fmt.Sprintf("export-chunks/%s/%04d-%s.bin", s.exportGenerationID(claim.Plan), index, domain.HashBytes([]byte(claim.Owner))[:32])}
		sealed, err := security.EncryptBytesBound(plain, s.Config.EncryptionKey, s.exportChunkBinding(claim.Plan, index, chunk))
		if err != nil {
			return err
		}
		if err := s.checkExportGenerationAccess(opCtx, claim); err != nil {
			return err
		}
		putCtx, cancel := context.WithTimeout(opCtx, exportPutTimeout)
		defer cancel()
		if err := s.Config.Archive.Put(putCtx, chunk.ObjectPath, sealed); err != nil {
			// Preserve the known Put-error terminal semantics. Cancellation or
			// a replaced owner cannot clear a newer prefix via this old claim.
			s.finishExport(exportClaimContext(opCtx, claim), claim.ID, "failed", "", "", 0, "export generation object write failed")
			return err
		}
		chunk.SealedDigest, chunk.Digest = domain.HashBytes(sealed), domain.HashBytes(plain)
		plan, err := s.saveExportChunk(opCtx, claim, chunk)
		if err != nil {
			return err // unknown Save: do not advance the local claim or publish
		}
		claim.Plan = plan
		return nil
	})
	if err != nil {
		return claim, nil, err
	}
	return claim, plain, nil
}

func encodeExportEvents(ctx context.Context, events []domain.Event) ([]byte, error) {
	var plain []byte
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stripped, err := security.StripSearchDigests(event.Payload)
		if err != nil {
			return nil, err
		}
		event.Payload = stripped
		line, err := domain.CanonicalJSON(event)
		if err != nil {
			return nil, err
		}
		plain = append(plain, line...)
		plain = append(plain, '\n')
	}
	return plain, nil
}
