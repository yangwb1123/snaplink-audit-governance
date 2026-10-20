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

// Keep the original real file/archived evidence fixture and two tenant layouts.
// Hold metadata must never replace even byte-identical hot/ledger inodes.
func TestHoldControlFileWritesAndRestart(t *testing.T) {
	svc, path, _ := admissionFileFixture(t)
	original := leaseSnapshot(t, svc)
	images := admissionEvidenceFiles(t, filepath.Dir(path))
	if len(images) != 4 || len(original.Events) != 0 {
		t.Fatal("Hold oracle requires two real layouts and evicted committed source")
	}
	objects := &checkpointArchive{Store: svc.Config.Archive}
	svc.Config.Archive = objects
	hold, err := svc.CreateLegalHold(testCtx, domain.LegalHold{ID: "hold-control-file", TenantID: "tenant-a", Name: "case", Reason: "case", CreatedBy: "case-worker"})
	if err != nil {
		t.Fatal("could not create control Hold")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
	if objects.gets.Load() != 0 || objects.puts.Load() != 0 {
		t.Fatal("Hold control mutation touched source objects")
	}
	if _, err := svc.GetExport(testCtx, "tenant-a", "admission-neighbor"); !errors.Is(err, domain.ErrForbidden) {
		t.Fatal("new Hold stopped protecting the original running export")
	}
	gets := objects.gets.Load()
	released, err := svc.ReleaseLegalHold(testCtx, "tenant-a", hold.ID, "first-releaser")
	if err != nil || released.ReleasedAt == nil || released.ReleasedBy != "first-releaser" {
		t.Fatal("Hold release lost its original decision")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal("could not capture released control inode")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("could not capture released control bytes")
	}
	again, err := svc.ReleaseLegalHold(testCtx, "tenant-a", hold.ID, "later-releaser")
	if err != nil || !reflect.DeepEqual(again, released) {
		t.Fatal("repeated release changed the first decision")
	}
	afterInfo, statErr := os.Stat(path)
	afterRaw, readErr := os.ReadFile(path)
	if statErr != nil || readErr != nil || !os.SameFile(info, afterInfo) || !bytes.Equal(raw, afterRaw) {
		t.Error("already-released Hold performed a durable control rewrite")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
	if objects.gets.Load() != gets || objects.puts.Load() != 0 {
		t.Fatal("Hold release touched source objects")
	}
	if err := svc.Store.Close(); err != nil {
		t.Fatal("could not close original Hold file writer")
	}
	reopened := reopenHoldControlFile(t, path)
	current := leaseSnapshot(t, reopened)
	if !reflect.DeepEqual(current.LegalHolds[hold.ID], released) ||
		!reflect.DeepEqual(current.Receipts, original.Receipts) || !reflect.DeepEqual(current.Streams, original.Streams) ||
		!reflect.DeepEqual(current.Exports, original.Exports) || !reflect.DeepEqual(current.ExportLeases, original.ExportLeases) ||
		!reflect.DeepEqual(current.ExportPlans, original.ExportPlans) || !reflect.DeepEqual(current.ExportCheckpoints, original.ExportCheckpoints) ||
		!reflect.DeepEqual(current.Schemas, original.Schemas) {
		t.Fatal("Hold/restart rewrote evidence or neighboring execution metadata")
	}
	assertHoldControlFacts(t, current.AdminActions, hold.ID, 1, 1)
	if job, err := reopened.GetExport(testCtx, "tenant-a", "admission-neighbor"); err != nil || job.Status != "running" {
		t.Fatal("released Hold still blocked the unchanged original job")
	}
}

func TestHoldControlFailedFileSavePreservesUnknownTempAndEvidence(t *testing.T) {
	for _, release := range []bool{false, true} {
		name := "create"
		if release {
			name = "release"
		}
		t.Run(name, func(t *testing.T) {
			svc, path, _ := admissionFileFixture(t)
			hold := domain.LegalHold{ID: "hold-control-failed", TenantID: "tenant-a", Name: "case", Reason: "case"}
			if release {
				if _, err := svc.CreateLegalHold(testCtx, hold); err != nil {
					t.Fatal("could not prepare original active Hold")
				}
			}
			original := leaseSnapshot(t, svc)
			images := admissionEvidenceFiles(t, filepath.Dir(path))
			unknown := []byte("owned-Hold-control-unknown-temp")
			if err := os.WriteFile(path+".tmp", unknown, 0o600); err != nil {
				t.Fatal("could not prepare owned unknown temporary file")
			}
			var got domain.LegalHold
			var err error
			if release {
				got, err = svc.ReleaseLegalHold(testCtx, "tenant-a", hold.ID, "case-worker")
			} else {
				got, err = svc.CreateLegalHold(testCtx, hold)
			}
			if err == nil || !reflect.DeepEqual(got, domain.LegalHold{}) {
				t.Fatal("failed Hold commit returned a successful decision")
			}
			assertAdmissionEvidenceFilesUnchanged(t, images)
			raw, err := os.ReadFile(path + ".tmp")
			if err != nil || !bytes.Equal(raw, unknown) || !reflect.DeepEqual(leaseSnapshot(t, svc), original) {
				t.Fatal("failed Hold commit discarded unknown bytes or published partial control/facts")
			}
		})
	}
}
