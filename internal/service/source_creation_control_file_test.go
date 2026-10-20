package service

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestSourceCreationControlFilePreservesEvidenceUnknownTempAndRestart(t *testing.T) {
	for _, suppliedTime := range []bool{false, true} {
		for _, blocked := range []bool{false, true} {
			name := "default-time"
			if suppliedTime {
				name = "supplied-time"
			}
			if blocked {
				name += "/unknown-temp"
			} else {
				name += "/committed-restart"
			}
			t.Run(name, func(t *testing.T) {
				svc, path, _ := restoreDecisionFileFixture(t)
				before := restoreCreationSnapshot(t, svc)
				images := admissionEvidenceFiles(t, filepath.Dir(path))
				if len(images) != 4 || len(before.Events) != 0 || len(before.ExportPlans["admission-neighbor"].Members) != 1 {
					t.Fatal("registration oracle requires two real layouts, evicted source and authenticated selected stage")
				}
				reads := len(readFacts(t, svc))
				objects := &checkpointArchive{Store: svc.Config.Archive}
				svc.Config.Archive = objects
				unknown := []byte("owned-source-creation-unknown-temp")
				if blocked {
					if err := os.WriteFile(path+".tmp", unknown, 0o600); err != nil {
						t.Fatal("could not establish owned unknown control temp")
					}
				}
				request := sourceCreationControlCandidate()
				if suppliedTime {
					request.CreatedAt = svc.Now().Add(-time.Hour)
				}
				want := expectedSourceCreation(request, svc.Now())
				err := svc.AddSource(testCtx, "source-manager", request)
				if objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != reads {
					t.Error("registration performed WORM I/O or changed independent read facts")
				}
				assertAdmissionEvidenceFilesUnchanged(t, images)
				if blocked {
					raw, readErr := os.ReadFile(path + ".tmp")
					if err == nil || readErr != nil || !bytes.Equal(raw, unknown) || !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) {
						t.Error("failed registration Save discarded unknown bytes or published partial registration/facts")
					}
					return
				}
				if err != nil {
					t.Fatal("valid original registration did not commit")
				}
				assertSourceCreationControlDelta(t, before, restoreCreationSnapshot(t, svc), want)
				controlBefore, err := os.Stat(path)
				if err != nil {
					t.Fatal("could not inspect committed control inode")
				}
				if err := svc.AddSource(testCtx, "source-manager", request); !errors.Is(err, domain.ErrConflict) {
					t.Error("duplicate registration did not retain conflict")
				}
				controlAfter, err := os.Stat(path)
				if err != nil || !os.SameFile(controlBefore, controlAfter) {
					t.Error("rejected duplicate registration replaced control")
				}
				if err := svc.Store.Close(); err != nil {
					t.Fatal("could not close original registration writer")
				}
				reopened := reopenHoldControlFile(t, path)
				reopened.Config.Archive = objects
				assertSourceCreationControlDelta(t, before, restoreCreationSnapshot(t, reopened), want)
				assertAdmissionEvidenceFilesUnchanged(t, images)
				if len(readFacts(t, reopened)) != reads {
					t.Error("registration restart changed independent read facts")
				}
				for key, receipt := range before.Receipts {
					observed, err := reopened.GetReceipt(testCtx, receipt.TenantID, "", receipt.EventID)
					if err != nil || !reflect.DeepEqual(observed, receipt) || !reflect.DeepEqual(restoreCreationSnapshot(t, reopened).Receipts[key], receipt) {
						t.Error("registration or restart changed original exact cold receipt metadata")
					}
				}
				if _, err := reopened.GetExport(testCtx, "tenant-a", "admission-neighbor"); !errors.Is(err, domain.ErrForbidden) {
					t.Error("registration bypassed independent current Hold or historical evidence authentication")
				}
			})
		}
	}
}

// Keep the real three-call bootstrap, original cumulative counter and fault
// sequence. Source and Schema are distinct control writes, not Ingest writes.
func TestSourceCreationControlFileFaultBoundaryCountsBootstrapAndActualSave(t *testing.T) {
	svc, arm, writes := faultedLedgeredService(t, nil, true)
	before := restoreCreationSnapshot(t, svc)
	if writes.Load() != 2 || len(before.Sources) != 1 || len(before.Schemas) != 1 || len(before.AdminActions) != 3 || len(before.Receipts) != 0 {
		t.Fatal("real Source and Schema initialization were not independently counted or changed the original domain/facts")
	}
	images := admissionEvidenceFiles(t, filepath.Dir(svc.Config.ArchiveDir))
	if len(images) != 2 {
		t.Fatal("registration fault oracle requires the original tenant hot/cold files")
	}
	reads := len(readFacts(t, svc))
	objects := &checkpointArchive{}
	svc.Config.Archive = objects
	request := sourceCreationControlCandidate()
	want := expectedSourceCreation(request, svc.Now())
	arm(true)
	if err := svc.AddSource(testCtx, "source-manager", request); err == nil || writes.Load() != 3 || !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) {
		t.Fatal("armed actual registration control Save was skipped, reset or published partial registration/facts")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
	arm(false)
	if err := svc.AddSource(testCtx, "source-manager", request); err != nil || writes.Load() != 4 {
		t.Fatal("disarmed registration lost the cumulative initialization/fault/success sequence")
	}
	assertSourceCreationControlDelta(t, before, restoreCreationSnapshot(t, svc), want)
	assertAdmissionEvidenceFilesUnchanged(t, images)
	if objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != reads {
		t.Error("registration fault boundary performed source I/O or changed independent read facts")
	}
}
