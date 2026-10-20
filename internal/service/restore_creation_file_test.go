package service

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Capture real hot/cold evidence too; leaseSnapshot intentionally reads only
// control and cannot prove original receipt bytes/coordinates were retained.
func restoreCreationSnapshot(t *testing.T, svc *Service) *store.Snapshot {
	t.Helper()
	data, err := svc.Store.Snapshot(testCtx)
	if err != nil {
		t.Fatal("could not capture original materialized creation evidence")
	}
	return data
}

// Only the new request and its original creation fact may be added. Evidence,
// other requests/decisions, current Hold and export execution authority survive.
func assertRestoreCreationDelta(t *testing.T, before, after *store.Snapshot, run domain.RestoreRun) {
	t.Helper()
	if run.ID == "" || run.TenantID != "tenant-a" || run.Status != domain.RestoreStatusPendingApproval ||
		run.CreatedBy != "new-requester" || run.Reason != "case" || run.ApprovedAt != nil || run.RejectedAt != nil ||
		!reflect.DeepEqual(after.RestoreRuns[run.ID], run) || len(after.AdminActions) != len(before.AdminActions)+1 {
		t.Fatal("creation lost its original pending request or one atomic fact")
	}
	fact := after.AdminActions[len(after.AdminActions)-1]
	if fact.ID == "" || fact.TenantID != run.TenantID || fact.Actor != run.CreatedBy || fact.Action != domain.AdminActionRestoreCreated ||
		fact.TargetType != "restore" || fact.TargetID != run.ID || fact.Detail != run.OperationID {
		t.Fatal("creation changed the original fact identity, scope or encoding")
	}
	copy, err := cloneTestSnapshot(after)
	if err != nil {
		t.Fatal("could not inspect owned creation delta")
	}
	delete(copy.RestoreRuns, run.ID)
	copy.AdminActions = copy.AdminActions[:len(copy.AdminActions)-1]
	if !reflect.DeepEqual(copy, before) {
		t.Error("creation replaced unrelated control or original evidence")
	}
}

func TestRestoreCreationFilePreservesEvidenceAndSurvivesRestart(t *testing.T) {
	svc, path, _ := restoreDecisionFileFixture(t)
	before := restoreCreationSnapshot(t, svc)
	images := admissionEvidenceFiles(t, filepath.Dir(path))
	if len(images) != 4 || len(before.Events) != 0 || len(before.ExportPlans["admission-neighbor"].Members) != 1 {
		t.Fatal("creation oracle requires two real tenant layouts, evicted source and selected stage")
	}
	reads := len(readFacts(t, svc))
	objects := &checkpointArchive{Store: svc.Config.Archive}
	svc.Config.Archive = objects
	run, err := svc.CreateRestore(testCtx, "tenant-a", domain.RestoreRequest{OperationID: "admission-operation", Reason: "case"}, "new-requester")
	if err != nil {
		t.Fatal("original authenticated restore creation failed")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
	assertRestoreCreationDelta(t, before, restoreCreationSnapshot(t, svc), run)
	if objects.gets.Load() != 1 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != reads {
		t.Fatal("creation repeated source work, executed a business write or changed silent replay reads")
	}
	if err := svc.Store.Close(); err != nil {
		t.Fatal("could not close original creation writer")
	}
	reopened := reopenHoldControlFile(t, path)
	reopened.Config.Archive = objects
	assertRestoreCreationDelta(t, before, restoreCreationSnapshot(t, reopened), run)
	assertAdmissionEvidenceFilesUnchanged(t, images)
	if len(readFacts(t, reopened)) != reads {
		t.Error("restart lost or invented an independent read fact")
	}
	if _, err := reopened.GetExport(testCtx, "tenant-a", "admission-neighbor"); !errors.Is(err, domain.ErrForbidden) {
		t.Error("unrelated restore creation bypassed the current export Hold")
	}
}

func TestRestoreCreationFailedFileSavePreservesUnknownTempAndEvidence(t *testing.T) {
	svc, path, _ := restoreDecisionFileFixture(t)
	before := restoreCreationSnapshot(t, svc)
	images := admissionEvidenceFiles(t, filepath.Dir(path))
	reads := len(readFacts(t, svc))
	unknown := []byte("owned-restore-creation-unknown-temp")
	if err := os.WriteFile(path+".tmp", unknown, 0o600); err != nil {
		t.Fatal("could not establish owned unknown control temporary file")
	}
	run, err := svc.CreateRestore(testCtx, "tenant-a", domain.RestoreRequest{OperationID: "admission-operation", Reason: "case"}, "new-requester")
	if err == nil || run != (domain.RestoreRun{}) {
		t.Error("failed control Save exposed an unconfirmed request")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
	raw, err := os.ReadFile(path + ".tmp")
	if err != nil || !bytes.Equal(raw, unknown) || !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) || len(readFacts(t, svc)) != reads {
		t.Error("failed creation discarded unknown bytes, changed evidence or published partial control/facts")
	}
}
