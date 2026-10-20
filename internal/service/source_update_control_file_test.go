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

func TestSourceUpdateControlFilePreservesEvidenceAndUnknownTemp(t *testing.T) {
	for _, active := range []bool{true, false} {
		for _, blocked := range []bool{false, true} {
			name := "enabled"
			if !active {
				name = "disabled"
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
					t.Fatal("source update oracle requires two real layouts, evicted source and authenticated selected stage")
				}
				reads := len(readFacts(t, svc))
				objects := &checkpointArchive{Store: svc.Config.Archive}
				svc.Config.Archive = objects
				unknown := []byte("owned-source-update-unknown-temp")
				if blocked {
					if err := os.WriteFile(path+".tmp", unknown, 0o600); err != nil {
						t.Fatal("could not establish owned unknown control temp")
					}
				}
				request := sourceUpdateControlCandidate(svc.Now())
				request.Active = active
				got, err := svc.UpdateSource(testCtx, "source-manager", request)
				if objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != reads {
					t.Error("source update performed WORM I/O or changed independent read facts")
				}
				assertAdmissionEvidenceFilesUnchanged(t, images)
				if blocked {
					raw, readErr := os.ReadFile(path + ".tmp")
					if err == nil || !reflect.DeepEqual(got, domain.SourceSystem{}) || readErr != nil || !bytes.Equal(raw, unknown) || !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) {
						t.Error("failed source Save exposed a result, discarded unknown bytes or published partial replacement/facts")
					}
					return
				}
				if err != nil || !reflect.DeepEqual(got, expectedSourceUpdate(before, request)) {
					t.Fatal("valid source update did not commit its original result")
				}
				assertSourceUpdateControlDelta(t, before, restoreCreationSnapshot(t, svc), request)
				if err := svc.Store.Close(); err != nil {
					t.Fatal("could not close original source writer")
				}
				reopened := reopenHoldControlFile(t, path)
				reopened.Config.Archive = objects
				assertSourceUpdateControlDelta(t, before, restoreCreationSnapshot(t, reopened), request)
				assertAdmissionEvidenceFilesUnchanged(t, images)
				if len(readFacts(t, reopened)) != reads {
					t.Error("source update restart changed independent read facts")
				}
				for key, receipt := range before.Receipts {
					observed, err := reopened.GetReceipt(testCtx, receipt.TenantID, "", receipt.EventID)
					if err != nil || !reflect.DeepEqual(observed, receipt) || !reflect.DeepEqual(restoreCreationSnapshot(t, reopened).Receipts[key], receipt) {
						t.Error("source update or restart changed original exact cold receipt metadata")
					}
				}
				if _, err := reopened.GetExport(testCtx, "tenant-a", "admission-neighbor"); !errors.Is(err, domain.ErrForbidden) {
					t.Error("source update bypassed independent current Hold or historical evidence authentication")
				}
			})
		}
	}
}
