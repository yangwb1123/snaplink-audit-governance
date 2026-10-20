package service

import (
	"context"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportCheckpointAuthenticatedArtifactStillChecksFormatBindingCountAndLedger(t *testing.T) {
	for _, kind := range []string{"v1-downgrade", "other-job-aead", "other-tenant-aead", "plain-digest", "cardinality", "missing-receipt", "changed-receipt"} {
		t.Run(kind, func(t *testing.T) {
			svc, backend, objects := newCheckpointService(t)
			cp := leaveSealedCheckpoint(t, svc, backend)
			sealed, err := objects.Store.Get(testCtx, cp.ObjectPath)
			if err != nil {
				t.Fatal(err)
			}
			plain, err := security.DecryptExport(sealed, svc.Config.EncryptionKey, security.ExportBinding(cp.TenantID, cp.JobID))
			if err != nil {
				t.Fatal("fixture object authentication failed")
			}
			switch kind {
			case "v1-downgrade":
				sealed, err = security.EncryptBytes(plain, svc.Config.EncryptionKey)
			case "other-job-aead":
				sealed, err = security.EncryptBytesBound(plain, svc.Config.EncryptionKey, security.ExportBinding(cp.TenantID, "other-job"))
			case "other-tenant-aead":
				sealed, err = security.EncryptBytesBound(plain, svc.Config.EncryptionKey, security.ExportBinding("tenant-b", cp.JobID))
			case "plain-digest":
				cp.Digest = domain.HashBytes(nil)
			case "cardinality":
				cp.EventCount++
			}
			if err != nil {
				t.Fatal("fixture sealing failed")
			}
			cp.SealedDigest = domain.HashBytes(sealed)
			cp.Signature = svc.signExportCheckpoint(cp)
			objects.override = sealed
			if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
				data.ExportCheckpoints[cp.JobID] = cp
				key := store.EventKey(cp.TenantID, "checkpoint-source")
				if kind == "missing-receipt" {
					delete(data.Receipts, key)
				}
				if kind == "changed-receipt" {
					receipt := data.Receipts[key]
					receipt.Hash = domain.HashBytes(nil)
					data.Receipts[key] = receipt
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			expireCheckpoint(svc)
			puts := objects.puts.Load()
			svc.runExport(testCtx, cp.JobID)
			data := leaseSnapshot(t, svc)
			job := data.Exports[cp.JobID]
			if job.Status != "failed" || job.ObjectPath != "" || job.Digest != "" || job.EventCount != 0 || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 || checkpointFactCount(t, svc, domain.AdminActionExportCheckpointRejected) != 1 || objects.puts.Load() != puts {
				t.Fatal("trusted-metadata artifact drift was not rejected at finalization")
			}
		})
	}
}

func TestExportCheckpointRejectsCorruptCommittedHotContentBeforePut(t *testing.T) {
	svc := testService(t, false)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("checkpoint-hot", "operation", pinnedNow.Add(10*time.Second)), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	seedLeaseExport(t, svc, "checkpoint-job", "tenant-a")
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", "checkpoint-hot")
		event := data.Events[key]
		event.Payload["value"] = 99 // retained SourceDigest/hash still describe the original
		data.Events[key] = event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	objects := &checkpointArchive{Store: svc.Config.Archive}
	svc.Config.Archive = objects
	svc.runExport(context.Background(), "checkpoint-job")
	data := leaseSnapshot(t, svc)
	if data.Exports["checkpoint-job"].Status != "failed" || len(data.ExportCheckpoints) != 0 || objects.puts.Load() != 0 || checkpointFactCount(t, svc, domain.AdminActionExportCheckpointed) != 0 {
		t.Fatal("unverified hot evidence became a resumable artifact")
	}
}
