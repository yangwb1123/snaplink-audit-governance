package service

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestSchemaControlFilePreservesEvidenceAndUnknownTemp(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		name := "committed-restart"
		if blocked {
			name = "unknown-temp"
		}
		t.Run(name, func(t *testing.T) {
			svc, path, _ := restoreDecisionFileFixture(t)
			before := restoreCreationSnapshot(t, svc)
			images := admissionEvidenceFiles(t, filepath.Dir(path))
			if len(images) != 4 || len(before.Events) != 0 || len(before.ExportPlans["admission-neighbor"].Members) != 1 {
				t.Fatal("schema oracle requires two real layouts, evicted source and authenticated selected stage")
			}
			reads := len(readFacts(t, svc))
			objects := &checkpointArchive{Store: svc.Config.Archive}
			svc.Config.Archive = objects
			unknown := []byte("owned-schema-control-unknown-temp")
			if blocked {
				if err := os.WriteFile(path+".tmp", unknown, 0o600); err != nil {
					t.Fatal("could not establish owned unknown control temp")
				}
			}
			schema := schemaControlCandidate(svc.Now())
			err := svc.RegisterSchema(testCtx, "schema-manager", schema)
			if objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != reads {
				t.Error("schema registration performed source I/O or changed independent read facts")
			}
			assertAdmissionEvidenceFilesUnchanged(t, images)
			if blocked {
				raw, readErr := os.ReadFile(path + ".tmp")
				if err == nil || readErr != nil || !bytes.Equal(raw, unknown) || !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) {
					t.Error("failed schema Save discarded unknown bytes or published partial definition/facts")
				}
				return
			}
			if err != nil {
				t.Fatal("original compatible schema did not commit")
			}
			assertSchemaControlDelta(t, before, restoreCreationSnapshot(t, svc), schema)
			if err := svc.Store.Close(); err != nil {
				t.Fatal("could not close original schema writer")
			}
			reopened := reopenHoldControlFile(t, path)
			reopened.Config.Archive = objects
			assertSchemaControlDelta(t, before, restoreCreationSnapshot(t, reopened), schema)
			assertAdmissionEvidenceFilesUnchanged(t, images)
			if len(readFacts(t, reopened)) != reads {
				t.Error("schema restart changed independent read facts")
			}
			if _, err := reopened.GetExport(testCtx, "tenant-a", "admission-neighbor"); !errors.Is(err, domain.ErrForbidden) {
				t.Error("new schema bypassed the independent current Hold or changed exact historical definition")
			}
		})
	}
}

// Keep the existing fault fixture's real three-call bootstrap and cumulative
// counter. Source and Schema are now separate observable control writes, not
// Ingest writes. Neither initialization nor the fault Save sequence is skipped.
func TestSchemaControlFileFaultBoundaryRetainsBootstrapCounter(t *testing.T) {
	svc, arm, writes := faultedLedgeredService(t, nil, true)
	before := restoreCreationSnapshot(t, svc)
	if writes.Load() != 2 || len(before.Sources) != 1 || len(before.Schemas) != 1 || len(before.AdminActions) != 3 || len(before.Receipts) != 0 {
		t.Fatal("real Source and Schema initialization were not independently counted or changed the original domain/facts")
	}
	images := admissionEvidenceFiles(t, filepath.Dir(svc.Config.ArchiveDir))
	if len(images) != 2 {
		t.Fatal("fault oracle requires the original tenant hot/cold files")
	}
	reads := len(readFacts(t, svc))
	objects := &checkpointArchive{}
	svc.Config.Archive = objects
	schema := schemaControlCandidate(svc.Now())
	arm(true)
	if err := svc.RegisterSchema(testCtx, "schema-manager", schema); err == nil || writes.Load() != 3 || !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) {
		t.Fatal("armed real control fault was skipped, reset or published a partial Schema/fact")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
	arm(false)
	if err := svc.RegisterSchema(testCtx, "schema-manager", schema); err != nil || writes.Load() != 4 {
		t.Fatal("disarmed real Schema write lost the cumulative initialization/fault/success sequence")
	}
	assertSchemaControlDelta(t, before, restoreCreationSnapshot(t, svc), schema)
	assertAdmissionEvidenceFilesUnchanged(t, images)
	if objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != reads {
		t.Error("real Schema fault boundary performed source I/O or changed independent read facts")
	}
}
