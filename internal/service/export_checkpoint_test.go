package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportCheckpointResumesSealedStageWithoutSelectionOrPut(t *testing.T) {
	svc, backend, objects := newCheckpointService(t)
	cp := leaveSealedCheckpoint(t, svc, backend)
	original := leaseSnapshot(t, svc)
	oldClaim := exportClaim{ExportJob: original.Exports[cp.JobID], Owner: original.ExportLeases[cp.JobID].Owner, Checkpoint: cp}
	if _, won, err := svc.claimExport(testCtx, cp.JobID, true); err != nil || won {
		t.Fatal("healthy sealed owner was stolen")
	}
	if checkpointFactCount(t, svc, domain.AdminActionExportCheckpointed) != 1 {
		t.Fatal("checkpoint fact did not share its Save")
	}
	expireCheckpoint(svc)
	if n, err := svc.recoverExpiredExportLeases(testCtx); err != nil || n != 0 {
		t.Fatal("sealed work was failed or reported completed by expiry recovery")
	}
	objects.armed.Store(true)
	beforePuts, beforeGets := objects.puts.Load(), objects.gets.Load()
	replacement, won, err := svc.claimExport(testCtx, cp.JobID, true)
	if err != nil || !won || replacement.Owner == oldClaim.Owner || !replacement.StartedAt.Equal(*oldClaim.StartedAt) {
		t.Fatal("expired checkpoint did not receive a fresh fenced owner preserving started_at")
	}
	before := leaseSnapshot(t, svc)
	svc.finishExport(exportClaimContext(testCtx, oldClaim), oldClaim.ID, "completed", cp.ObjectPath, cp.Digest, cp.EventCount, "")
	if _, err := svc.saveExportCheckpoint(exportClaimContext(testCtx, oldClaim), oldClaim, nil, nil, 0); !errors.Is(err, errExportLeaseLost) {
		t.Fatal("old owner published a checkpoint")
	}
	if !reflect.DeepEqual(before, leaseSnapshot(t, svc)) {
		t.Fatal("late owner changed durable control")
	}
	svc.finalizeExportCheckpoint(exportClaimContext(testCtx, replacement), replacement, nil)
	got := leaseSnapshot(t, svc)
	job := got.Exports[cp.JobID]
	if job.Status != "completed" || job.ObjectPath != cp.ObjectPath || job.Digest != cp.Digest || job.EventCount != 1 || len(got.ExportCheckpoints) != 0 || len(got.ExportLeases) != 0 {
		t.Fatal("sealed-stage continuation did not publish only the original authenticated object")
	}
	if objects.puts.Load() != beforePuts || objects.gets.Load() != beforeGets+1 {
		t.Fatal("resume reselected source evidence or repeated Put")
	}
	if checkpointFactCount(t, svc, domain.AdminActionExportResumed) != 1 || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 1 || checkpointFactCount(t, svc, domain.AdminActionExportRecovered) != 0 {
		t.Fatal("continuation facts did not commit once")
	}
	before = leaseSnapshot(t, svc)
	svc.runExport(testCtx, cp.JobID)
	svc.finishExport(exportClaimContext(testCtx, oldClaim), oldClaim.ID, "failed", "", "", 0, "late")
	if !reflect.DeepEqual(before, leaseSnapshot(t, svc)) {
		t.Fatal("terminal state regressed or duplicated a fact")
	}
}

func TestExportCheckpointUnknownSaveAndBeforeCheckpointFailure(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-committed", true: "committed-result-unknown"}[unknown], func(t *testing.T) {
			svc, backend, objects := newCheckpointService(t)
			backend.failCheckpoint.Store(!unknown)
			backend.unknownCheckpoint.Store(unknown)
			svc.runExport(testCtx, "checkpoint-job")
			data := leaseSnapshot(t, svc)
			if data.Exports["checkpoint-job"].Status != "running" || data.Exports["checkpoint-job"].ObjectPath != "" {
				t.Fatal("checkpoint error guessed a terminal result")
			}
			_, exists := data.ExportCheckpoints["checkpoint-job"]
			if exists != unknown {
				t.Fatal("Save fault did not represent its actual durable boundary")
			}
			backend.failCheckpoint.Store(false)
			backend.unknownCheckpoint.Store(false)
			expireCheckpoint(svc)
			objects.armed.Store(unknown) // only a committed sealed stage forbids all further Put
			puts := objects.puts.Load()
			n, err := svc.recoverExpiredExportLeases(testCtx)
			if err != nil {
				t.Fatal(err)
			}
			if unknown {
				if n != 0 {
					t.Fatal("durable checkpoint was failed")
				}
				svc.runExport(testCtx, "checkpoint-job")
				if leaseSnapshot(t, svc).Exports["checkpoint-job"].Status != "completed" {
					t.Fatal("unknown committed checkpoint could not resume")
				}
			} else {
				plan := data.ExportPlans["checkpoint-job"]
				if n != 0 || plan.Version != 1 || checkpointFactCount(t, svc, domain.AdminActionExportCheckpointed) != 0 {
					t.Fatal("failed sealed Save lost the already-durable selected stage")
				}
				oldKey := exportObjectKey(exportClaim{ExportJob: data.Exports["checkpoint-job"], Owner: data.ExportLeases["checkpoint-job"].Owner})
				svc.runExport(testCtx, "checkpoint-job")
				job := leaseSnapshot(t, svc).Exports["checkpoint-job"]
				if job.Status != "completed" || job.ObjectPath == oldKey || objects.puts.Load() != puts+1 {
					t.Fatal("selected continuation did not create one distinct attempt artifact")
				}
				sealed, err := objects.Get(testCtx, job.ObjectPath)
				if err != nil {
					t.Fatal(err)
				}
				plain, err := svc.VerifyExportDownload(testCtx, job.TenantID, job.ID, sealed)
				if err != nil || !exportPlainMatchesPlan(plain, plan) {
					t.Fatal("selected continuation reselected the unknown-Put output")
				}
				if _, err := objects.Get(testCtx, oldKey); err != nil {
					t.Fatal("continuation deleted the unknown-result sealed object")
				}
			}
			if unknown && objects.puts.Load() != puts {
				t.Fatal("sealed-stage continuation wrote another object")
			}
		})
	}
}

func TestExportCheckpointMetadataAndJobDriftFailClosed(t *testing.T) {
	cases := map[string]func(*domain.ExportJob, *store.ExportCheckpoint){
		"version":           func(_ *domain.ExportJob, cp *store.ExportCheckpoint) { cp.Version++ },
		"seal-version":      func(_ *domain.ExportJob, cp *store.ExportCheckpoint) { cp.SealVersion = 1 },
		"job-coordinate":    func(_ *domain.ExportJob, cp *store.ExportCheckpoint) { cp.JobID = "other" },
		"tenant-coordinate": func(_ *domain.ExportJob, cp *store.ExportCheckpoint) { cp.TenantID = "tenant-b" },
		"object":            func(_ *domain.ExportJob, cp *store.ExportCheckpoint) { cp.ObjectPath = "exports/other.jsonl" },
		"digest":            func(_ *domain.ExportJob, cp *store.ExportCheckpoint) { cp.Digest = domain.HashBytes(nil) },
		"sealed-digest":     func(_ *domain.ExportJob, cp *store.ExportCheckpoint) { cp.SealedDigest = domain.HashBytes(nil) },
		"negative-count":    func(_ *domain.ExportJob, cp *store.ExportCheckpoint) { cp.EventCount = -1 },
		"excessive-count": func(_ *domain.ExportJob, cp *store.ExportCheckpoint) {
			cp.EventCount = store.MaxQuerySnapshotEvents + 1
		},
		"future":            func(_ *domain.ExportJob, cp *store.ExportCheckpoint) { cp.CreatedAt = cp.CreatedAt.Add(time.Hour) },
		"before-start":      func(_ *domain.ExportJob, cp *store.ExportCheckpoint) { cp.CreatedAt = cp.CreatedAt.Add(-time.Second) },
		"binding":           func(_ *domain.ExportJob, cp *store.ExportCheckpoint) { cp.Binding = "" },
		"signature":         func(_ *domain.ExportJob, cp *store.ExportCheckpoint) { cp.Signature = "" },
		"query":             func(job *domain.ExportJob, _ *store.ExportCheckpoint) { job.Query.SourceSystem = "other" },
		"requester":         func(job *domain.ExportJob, _ *store.ExportCheckpoint) { job.RequestedBy = "other" },
		"lineage":           func(job *domain.ExportJob, _ *store.ExportCheckpoint) { job.RetryOf = "other" },
		"created-at":        func(job *domain.ExportJob, _ *store.ExportCheckpoint) { job.CreatedAt = job.CreatedAt.Add(time.Second) },
		"map-body-identity": func(job *domain.ExportJob, cp *store.ExportCheckpoint) { job.ID = "other"; cp.JobID = "other" },
		"common-tenant-drift": func(job *domain.ExportJob, cp *store.ExportCheckpoint) {
			job.TenantID = "tenant-b"
			cp.TenantID = "tenant-b"
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			svc, backend, objects := newCheckpointService(t)
			leaveSealedCheckpoint(t, svc, backend)
			if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
				job, cp := data.Exports["checkpoint-job"], data.ExportCheckpoints["checkpoint-job"]
				corrupt(&job, &cp)
				data.Exports["checkpoint-job"], data.ExportCheckpoints["checkpoint-job"] = job, cp
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			expireCheckpoint(svc)
			before := leaseSnapshot(t, svc)
			puts, gets := objects.puts.Load(), objects.gets.Load()
			if n, err := svc.recoverExpiredExportLeases(testCtx); n != 0 || !errors.Is(err, domain.ErrConflict) {
				t.Fatal("invalid checkpoint was cleaned or treated as completed")
			}
			if _, won, err := svc.claimExport(testCtx, "checkpoint-job", true); won || !errors.Is(err, domain.ErrConflict) {
				t.Fatal("invalid checkpoint was claimed")
			}
			if !reflect.DeepEqual(before, leaseSnapshot(t, svc)) || objects.puts.Load() != puts || objects.gets.Load() != gets {
				t.Fatal("invalid checkpoint caused writes or evidence IO")
			}
		})
	}
}

func TestExportCheckpointReadAuditAndArchiveUnavailableRemainResumable(t *testing.T) {
	for _, surface := range []string{"self-audit", "archive"} {
		t.Run(surface, func(t *testing.T) {
			svc, backend, objects := newCheckpointService(t)
			cp := leaveSealedCheckpoint(t, svc, backend)
			backend.failTrail.Store(surface == "self-audit")
			objects.unavailable.Store(surface == "archive")
			expireCheckpoint(svc)
			svc.runExport(testCtx, cp.JobID)
			got := leaseSnapshot(t, svc)
			if got.Exports[cp.JobID].Status != "running" || got.ExportCheckpoints[cp.JobID] != cp || got.Exports[cp.JobID].ObjectPath != "" || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 0 {
				t.Fatal("verification failure published a result or destroyed its checkpoint")
			}
			backend.failTrail.Store(false)
			objects.unavailable.Store(false)
			expireCheckpoint(svc)
			svc.runExport(testCtx, cp.JobID)
			if leaseSnapshot(t, svc).Exports[cp.JobID].Status != "completed" {
				t.Fatal("transient verification failure could not continue later")
			}
		})
	}
}

func TestExportCheckpointSealedCorruptionRejectsWithoutPublishing(t *testing.T) {
	svc, backend, objects := newCheckpointService(t)
	cp := leaveSealedCheckpoint(t, svc, backend)
	objects.corrupt.Store(true)
	expireCheckpoint(svc)
	svc.runExport(testCtx, cp.JobID)
	got := leaseSnapshot(t, svc)
	job := got.Exports[cp.JobID]
	if job.Status != "failed" || job.FailureCode != domain.ExportFailureExecution || job.ObjectPath != "" || job.Digest != "" || job.EventCount != 0 || len(got.ExportCheckpoints) != 0 || len(got.ExportLeases) != 0 || checkpointFactCount(t, svc, domain.AdminActionExportCheckpointRejected) != 1 {
		t.Fatal("bad sealed evidence was published or rejection was not atomic")
	}
	if raw, err := objects.Store.Get(context.Background(), cp.ObjectPath); err != nil || len(raw) == 0 {
		t.Fatal("rejection deleted the original WORM object")
	}
}
