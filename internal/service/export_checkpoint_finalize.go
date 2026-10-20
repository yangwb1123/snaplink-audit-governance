package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

// This resumes a real post-Put stage. It never reselects the output, reseals it
// or calls Put. The normal executor can reuse its sealed bytes; a restarted
// executor reads exactly the checkpoint's original object outside Store.mu.
func (s *Service) finalizeExportCheckpoint(ctx context.Context, claim exportClaim, sealed []byte) {
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		if err := s.checkExportLease(opCtx); err != nil {
			return err
		}
		if sealed == nil {
			readCtx, cancel := s.archiveFallbackContext(opCtx)
			defer cancel()
			var err error
			sealed, err = s.Config.Archive.Get(readCtx, claim.Checkpoint.ObjectPath)
			if err != nil {
				return err // unavailable is not proof that the original object is invalid
			}
		}
		if err := opCtx.Err(); err != nil {
			return err
		}
		references, err := s.authenticateExportCheckpoint(claim, sealed)
		if err != nil {
			s.rejectExportCheckpoint(opCtx, claim)
			return nil
		}
		if err := s.verifyQueryReferences(opCtx, claim.TenantID, references); err != nil {
			if errors.Is(err, domain.ErrConflict) || errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrQuotaExceeded) {
				s.rejectExportCheckpoint(opCtx, claim)
				return nil
			}
			return err
		}
		// Owned snapshot only; Hold may require remote current evidence. No
		// Store callback calls another Store method or retains its lock for Get.
		data, err := s.Store.Snapshot(opCtx)
		if err != nil {
			return err
		}
		hold, blocked, err := s.holdBlockingExportEvidence(opCtx, data, claim.ExportJob)
		if err != nil {
			return err
		}
		if blocked {
			s.failExportBlocked(opCtx, claim.ID, hold)
			return nil
		}
		if err := s.recordReadAction(opCtx, claim.TenantID, "governance-worker", domain.AdminActionEventRead, "export", claim.ID, "stage=sealed verification"); err != nil {
			return err
		}
		claim.HoldBinding = s.exportHoldBinding(data, claim.TenantID)
		s.finishExport(exportClaimContext(opCtx, claim), claim.ID, "completed", claim.Checkpoint.ObjectPath, claim.Checkpoint.Digest, claim.Checkpoint.EventCount, "")
		return nil
	})
	if err != nil {
		// Leave the authenticated checkpoint available for the next expired
		// lease claim. Never log errors, object keys, hashes, filters or owner.
		s.logf("export checkpoint finalization deferred job_id=%s category=verification", claim.ID)
	}
}

func (s *Service) authenticateExportCheckpoint(claim exportClaim, sealed []byte) ([]store.EventReference, error) {
	invalid := fmt.Errorf("%w: sealed export checkpoint evidence verification failed", domain.ErrConflict)
	cp := claim.Checkpoint
	if !bytes.HasPrefix(sealed, []byte("export:v2:")) || domain.HashBytes(sealed) != cp.SealedDigest {
		return nil, invalid
	}
	plain, err := security.DecryptExport(sealed, s.Config.EncryptionKey, security.ExportBinding(claim.TenantID, claim.ID))
	if err != nil || domain.HashBytes(plain) != cp.Digest {
		return nil, invalid
	}
	if cp.EventCount == 0 && len(plain) == 0 {
		return nil, nil
	}
	lines := bytes.Split(plain, []byte{'\n'})
	if len(lines) != cp.EventCount+1 || len(lines[len(lines)-1]) != 0 {
		return nil, invalid
	}
	references := make([]store.EventReference, 0, cp.EventCount)
	for _, line := range lines[:len(lines)-1] {
		// Decode coordinates only, not another in-memory payload/ciphertext
		// history. These still have to match the authoritative cold receipts.
		var identity struct {
			store.EventReference
			TenantID      string `json:"tenant_id"`
			SchemaID      string `json:"schema_id"`
			SchemaVersion int    `json:"schema_version"`
		}
		if json.Unmarshal(line, &identity) != nil || identity.TenantID != claim.TenantID || identity.SchemaID == "" || identity.SchemaVersion <= 0 {
			return nil, invalid
		}
		references = append(references, identity.EventReference)
	}
	return references, nil
}

func (s *Service) rejectExportCheckpoint(ctx context.Context, claim exportClaim) {
	s.persistTerminalExport(ctx, claim.ID, "failed", "", "", 0, "sealed export checkpoint evidence verification failed",
		func(data *store.Snapshot, job *domain.ExportJob) {
			s.appendAdminAction(data, s.adminAction(job.TenantID, "governance-worker", domain.AdminActionExportCheckpointRejected, "export", job.ID, "stage=sealed verification_failed"))
		})
}
