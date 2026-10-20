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

// Retain real archived source, two tenant layouts and an authenticated selected
// export. Only the decision under test is observed; preparation still uses the
// original CreateRestore/replay and actual file Save/append/Put/Get boundaries.
func restoreDecisionFileFixture(t *testing.T) (*Service, string, domain.RestoreRun) {
	t.Helper()
	svc, path, _ := admissionFileFixture(t)
	data := leaseSnapshot(t, svc)
	claim := exportClaim{ExportJob: data.Exports["admission-neighbor"], Owner: data.ExportLeases["admission-neighbor"].Owner}
	claim, events, ready := svc.exportExecutionEvidence(exportClaimContext(testCtx, claim), claim)
	if !ready || len(events) != 1 || len(claim.Plan.Members) != 1 {
		t.Fatal("could not prepare the real neighboring selected stage")
	}
	run, err := svc.CreateRestore(testCtx, "tenant-a", domain.RestoreRequest{OperationID: "admission-operation", Reason: "case"}, "requester")
	if err != nil {
		t.Fatal("could not prepare the original audited restore request")
	}
	if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{ID: "decision-current-hold", TenantID: "tenant-a", Name: "case", Reason: "case"}); err != nil {
		t.Fatal("could not prepare the independent current Hold")
	}
	return svc, path, run
}

func assertRestoreDecisionNeighbors(t *testing.T, before, after *store.Snapshot) {
	t.Helper()
	if !reflect.DeepEqual(before.Exports, after.Exports) || !reflect.DeepEqual(before.ExportLeases, after.ExportLeases) ||
		!reflect.DeepEqual(before.ExportPlans, after.ExportPlans) || !reflect.DeepEqual(before.ExportCheckpoints, after.ExportCheckpoints) ||
		!reflect.DeepEqual(before.LegalHolds, after.LegalHolds) || !reflect.DeepEqual(before.Policies, after.Policies) ||
		!reflect.DeepEqual(before.Receipts, after.Receipts) || !reflect.DeepEqual(before.Streams, after.Streams) ||
		!reflect.DeepEqual(before.Schemas, after.Schemas) || !reflect.DeepEqual(before.Events, after.Events) {
		t.Error("restore decision changed original evidence, Hold or neighboring execution authority")
	}
}

func TestRestoreDecisionFileDoesNotRewriteEvidenceAndSurvivesRestart(t *testing.T) {
	for _, target := range []string{domain.RestoreStatusApproved, domain.RestoreStatusRejected} {
		t.Run(target, func(t *testing.T) {
			svc, path, run := restoreDecisionFileFixture(t)
			original := leaseSnapshot(t, svc)
			images := admissionEvidenceFiles(t, filepath.Dir(path))
			if len(images) != 4 || len(original.Events) != 0 || len(original.ExportPlans["admission-neighbor"].Members) != 1 {
				t.Fatal("oracle requires two real layouts, evicted source and durable selected membership")
			}
			reads := len(readFacts(t, svc))
			objects := &checkpointArchive{Store: svc.Config.Archive}
			svc.Config.Archive = objects
			got, err := callRestoreDecision(testCtx, svc, "tenant-a", run.ID, "decider", target)
			if err != nil || got.Status != target {
				t.Fatal("control decision failed to commit its original result")
			}
			assertAdmissionEvidenceFilesUnchanged(t, images)
			if objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != reads {
				t.Fatal("decision performed source I/O or changed the existing silent-read policy")
			}
			beforeRepeat, err := os.Stat(path)
			if err != nil {
				t.Fatal("could not inspect the committed control boundary")
			}
			if again, err := callRestoreDecision(testCtx, svc, "tenant-a", run.ID, "later-decider", target); !errors.Is(err, domain.ErrConflict) || again != (domain.RestoreRun{}) {
				t.Error("already-decided request did not refuse repeated decision")
			}
			afterRepeat, err := os.Stat(path)
			if err != nil || !os.SameFile(beforeRepeat, afterRepeat) {
				t.Error("rejected repeated decision rewrote control")
			}
			if err := svc.Store.Close(); err != nil {
				t.Fatal("could not close original decision file writer")
			}
			reopened := reopenHoldControlFile(t, path)
			reopened.Config.Archive = objects // same real source, not an unconfigured new handle
			current := leaseSnapshot(t, reopened)
			if !reflect.DeepEqual(current.RestoreRuns[run.ID], got) || len(readFacts(t, reopened)) != reads {
				t.Error("restart lost the exact decision or changed independent read facts")
			}
			assertRestoreDecisionNeighbors(t, original, current)
			facts := restoreDecisionFacts(t, current, run.ID)
			if facts[domain.AdminActionRestoreCreated] != 1 || facts[domain.AdminActionRestoreApproved]+facts[domain.AdminActionRestoreRejected] != 1 {
				t.Error("restart lost or duplicated original request/decision facts")
			}
			if _, err := reopened.GetExport(testCtx, "tenant-a", "admission-neighbor"); !errors.Is(err, domain.ErrForbidden) {
				t.Error("restore decision bypassed the independent current export Hold")
			}
		})
	}
}

func TestRestoreDecisionFailedFileSavePreservesUnknownTempAndEvidence(t *testing.T) {
	for _, target := range []string{domain.RestoreStatusApproved, domain.RestoreStatusRejected} {
		t.Run(target, func(t *testing.T) {
			svc, path, run := restoreDecisionFileFixture(t)
			original := leaseSnapshot(t, svc)
			images := admissionEvidenceFiles(t, filepath.Dir(path))
			unknown := []byte("owned-restore-decision-unknown-temp")
			if err := os.WriteFile(path+".tmp", unknown, 0o600); err != nil {
				t.Fatal("could not establish owned unknown temporary file")
			}
			got, err := callRestoreDecision(testCtx, svc, "tenant-a", run.ID, "decider", target)
			if err == nil || got != (domain.RestoreRun{}) {
				t.Error("failed file Save exposed an uncommitted decision")
			}
			assertAdmissionEvidenceFilesUnchanged(t, images)
			raw, err := os.ReadFile(path + ".tmp")
			if err != nil || !bytes.Equal(raw, unknown) || !reflect.DeepEqual(leaseSnapshot(t, svc), original) {
				t.Error("failed decision discarded unknown bytes or published partial control/facts")
			}
		})
	}
}
