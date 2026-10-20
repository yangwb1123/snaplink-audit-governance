package service

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type admissionFileImage struct {
	info os.FileInfo
	raw  []byte
}

// Capture actual hot/cold files, not elapsed-time estimates or virtual sizes.
// Even a byte-identical replacement is a needless durable write of evidence.
func admissionEvidenceFiles(t *testing.T, root string) map[string]admissionFileImage {
	t.Helper()
	images := map[string]admissionFileImage{}
	for _, directory := range []string{"tenants", "ledger"} {
		entries, err := os.ReadDir(filepath.Join(root, directory))
		if err != nil {
			t.Fatal("could not inspect owned evidence directory")
		}
		for _, entry := range entries {
			path := filepath.Join(root, directory, entry.Name())
			info, err := os.Stat(path)
			if err != nil || !info.Mode().IsRegular() {
				t.Fatal("evidence fixture lost its regular file boundary")
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal("could not capture owned evidence file")
			}
			images[path] = admissionFileImage{info: info, raw: raw}
		}
	}
	return images
}

func assertAdmissionEvidenceFilesUnchanged(t *testing.T, images map[string]admissionFileImage) {
	t.Helper()
	replaced := 0
	for path, before := range images {
		after, err := os.Stat(path)
		if err != nil {
			t.Fatal("admission removed an original evidence file")
		}
		raw, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(raw, before.raw) {
			t.Error("control-only admission rewrote original hot/cold bytes or versions")
		}
		if !os.SameFile(before.info, after) {
			replaced++
		}
	}
	t.Logf("admission_file_boundary observed=%d replaced=%d", len(images), replaced)
	if replaced != 0 {
		t.Error("control-only admission replaced unrelated evidence inodes")
	}
}

func admissionFileFixture(t *testing.T) (*Service, string, domain.Query) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal("could not open owned admission file fixture")
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := newServiceOn(t, st)
	seedFileTestDomain(t, svc)
	pauseExportDispatch(svc) // inspect only admission, never suppress claim/CAS checks
	svc.Config.Archive = &archive.FileStore{Dir: filepath.Join(filepath.Dir(path), "archive")}
	if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-b", Name: "Neighbor", Active: true}); err != nil {
		t.Fatal("could not create independent file tenant")
	}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("admission-file", "admission-operation", pinnedNow.Add(time.Second)), domain.StatusArchived); err != nil {
		t.Fatal("could not commit real original archived evidence")
	}
	seedActionExport(t, svc, "admission-neighbor", "pending")
	if _, won, err := svc.claimPendingExport(testCtx, "admission-neighbor"); err != nil || !won {
		t.Fatal("could not prepare original fenced neighboring claim")
	}
	return svc, path, domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute)}
}

func TestExportAdmissionControlOnlyFileWritesAndRestart(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		name := "allowed"
		if blocked {
			name = "Hold-denied"
		}
		t.Run(name, func(t *testing.T) {
			svc, path, query := admissionFileFixture(t)
			if blocked {
				if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "case", Reason: "case"}); err != nil {
					t.Fatal("could not prepare current matching Hold")
				}
			}
			original := leaseSnapshot(t, svc)
			images := admissionEvidenceFiles(t, filepath.Dir(path))
			if len(images) != 4 || len(original.Events) != 0 {
				t.Fatal("oracle requires two real tenant layouts and evicted committed source")
			}
			job, err := svc.CreateExport(testCtx, "tenant-a", "operator", query)
			if blocked {
				if !errors.Is(err, domain.ErrForbidden) || job.ID != "" || countAdminActions(t, svc, func(f domain.AdminAction) bool { return f.Action == domain.AdminActionExportBlocked }) != 1 {
					t.Fatal("Hold denial lost its separate fail-closed mutation fact")
				}
			} else if err != nil || job.Status != "pending" || countAdminActions(t, svc, func(f domain.AdminAction) bool { return f.Action == domain.AdminActionExportCreated }) != 1 {
				t.Fatal("control-only admission lost its atomic pending job/creation fact")
			}
			assertAdmissionEvidenceFilesUnchanged(t, images)
			if len(readFacts(t, svc)) != 1 {
				t.Fatal("admission skipped or duplicated its original audited query")
			}
			if err := svc.Store.Close(); err != nil {
				t.Fatal("could not close original file writer")
			}
			reopened, err := store.Open(path)
			if err != nil {
				t.Fatal("control-only admission did not survive original file restart")
			}
			defer reopened.Close()
			next := newServiceOn(t, reopened)
			current := leaseSnapshot(t, next)
			if !reflect.DeepEqual(current.Receipts, original.Receipts) || !reflect.DeepEqual(current.Streams, original.Streams) || !reflect.DeepEqual(current.Schemas, original.Schemas) || !reflect.DeepEqual(current.ExportLeases, original.ExportLeases) || !reflect.DeepEqual(current.Exports["admission-neighbor"], original.Exports["admission-neighbor"]) || !reflect.DeepEqual(current.LegalHolds, original.LegalHolds) {
				t.Fatal("admission/restart changed original evidence, current Hold or neighboring claim")
			}
			want := len(original.Exports)
			if !blocked {
				want++
				if !reflect.DeepEqual(current.Exports[job.ID], job) {
					t.Fatal("restart lost the exact pending admission")
				}
			}
			if len(current.Exports) != want || len(readFacts(t, next)) != 1 {
				t.Fatal("restart invented a denied job or lost its independent read fact")
			}
		})
	}
}

func TestExportAdmissionControlFailurePreservesUnknownTempAndEvidence(t *testing.T) {
	svc, path, query := admissionFileFixture(t)
	original := leaseSnapshot(t, svc)
	images := admissionEvidenceFiles(t, filepath.Dir(path))
	unknown := []byte("owned-test-unknown-control-temp")
	if err := os.WriteFile(path+".tmp", unknown, 0o600); err != nil {
		t.Fatal("could not establish owned unknown temporary file")
	}
	job, err := svc.CreateExport(testCtx, "tenant-a", "operator", query)
	if err == nil || job.ID != "" {
		t.Fatal("failed control commit returned a successful admission")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
	raw, err := os.ReadFile(path + ".tmp")
	current := leaseSnapshot(t, svc)
	if err != nil || !bytes.Equal(raw, unknown) || !reflect.DeepEqual(current, original) || len(readFacts(t, svc)) != 1 {
		t.Fatal("control failure discarded unknown evidence, published a job or retracted its durable read attempt")
	}
}
