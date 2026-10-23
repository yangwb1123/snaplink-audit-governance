package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestHoldListControlFileKeepsEvidenceFactsUnknownTempAndRestart(t *testing.T) {
	for _, unknownTemp := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown-temp=%t", unknownTemp), func(t *testing.T) {
			svc, path, _ := restoreDecisionFileFixture(t)
			released, err := svc.CreateLegalHold(testCtx, domain.LegalHold{ID: "hold-list-file-released", TenantID: "tenant-a", Name: "released case", Reason: "released reason", CreatedBy: "case-worker", CreatedAt: pinnedNow.Add(-time.Minute)})
			if err != nil {
				t.Fatal("could not prepare original released Hold")
			}
			if released, err = svc.ReleaseLegalHold(testCtx, "tenant-a", released.ID, "release-worker"); err != nil {
				t.Fatal("could not release original Hold")
			}
			before := restoreCreationSnapshot(t, svc)
			images := admissionEvidenceFiles(t, filepath.Dir(path))
			hold, ok := before.LegalHolds["decision-current-hold"]
			want := []domain.LegalHold{before.LegalHolds[released.ID], hold}
			if len(images) != 4 || !ok || len(want) != 2 || want[0].ReleasedAt == nil || len(before.Events) != 0 || len(before.ExportPlans["admission-neighbor"].Members) != 1 {
				t.Fatal("Hold list oracle requires two real layouts, evicted source, released/current Hold and authenticated selected stage")
			}
			control, err := os.ReadFile(path)
			if err != nil {
				t.Fatal("could not capture owned control bytes")
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal("could not inspect owned control inode")
			}
			unknown := []byte("owned-Hold-list-unknown-temp")
			if unknownTemp {
				if err := os.WriteFile(path+".tmp", unknown, 0o600); err != nil {
					t.Fatal("could not establish owned unknown control temp")
				}
			}
			reads := len(readFacts(t, svc))
			objects := &checkpointArchive{Store: svc.Config.Archive}
			svc.Config.Archive = objects
			check := func(current *Service) {
				t.Helper()
				got, err := current.ListLegalHolds(testCtx, "tenant-a")
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Error("Hold list/restart lost the exact original control record")
				}
				for i := range got {
					if got[i].ReleasedAt != nil {
						*got[i].ReleasedAt = pinnedNow
						got[i].Name = "caller-copy"
					}
				}
				if again, err := current.ListLegalHolds(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(again, want) {
					t.Error("returned Hold ReleasedAt pointer was not independently owned")
				}
				// A successful no-match still owns a non-nil empty list, so a
				// real HTTP transport renders items:[] instead of items:null.
				if got, err := current.ListLegalHolds(testCtx, "tenant-b"); err != nil || got == nil || len(got) != 0 {
					t.Error("Hold list invented a neighboring tenant entry or returned nil for an empty success")
				}
				if got, err := current.ListLegalHolds(testCtx, ""); !errors.Is(err, domain.ErrInvalid) || len(got) != 0 {
					t.Error("Hold list changed empty-scope refusal")
				}
				raw, readErr := os.ReadFile(path)
				after, statErr := os.Stat(path)
				if readErr != nil || statErr != nil || !bytes.Equal(raw, control) || !os.SameFile(info, after) || !reflect.DeepEqual(restoreCreationSnapshot(t, current), before) {
					t.Error("Hold list rewrote control/evidence or execution authority")
				}
				assertAdmissionEvidenceFilesUnchanged(t, images)
				if unknownTemp {
					if raw, err := os.ReadFile(path + ".tmp"); err != nil || !bytes.Equal(raw, unknown) {
						t.Error("Hold list discarded or guessed success from unknown temp bytes")
					}
				}
				if objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, current)) != reads {
					t.Error("Hold list performed WORM I/O or changed original silent-read facts")
				}
			}
			check(svc)
			if err := svc.Store.Close(); err != nil {
				t.Fatal("could not close original owned file writer")
			}
			reopened := reopenHoldControlFile(t, path)
			reopened.Config.Archive = objects
			check(reopened)
			// The list observation itself is read-only, but the independent
			// current Hold still gates the existing real export evidence.
			if _, err := reopened.GetExport(testCtx, "tenant-a", "admission-neighbor"); !errors.Is(err, domain.ErrForbidden) {
				t.Error("Hold list observation bypassed the independent current Hold gate")
			}
			if err := reopened.RecordExportDownload(testCtx, "tenant-a", "case-worker", "admission-neighbor"); !errors.Is(err, domain.ErrForbidden) {
				t.Error("Hold list observation bypassed the current Hold download gate")
			}
		})
	}
}

// Measure the same Hold list read with identical control and 1 versus 32 real
// retained hot events. This measures allocation work, not Ingest or wall time.
func TestHoldListControlFileAllocationDoesNotScaleWithUnrelatedEvidence(t *testing.T) {
	svc, path, _ := restoreDecisionFileFixture(t)
	hold := leaseSnapshot(t, svc).LegalHolds["decision-current-hold"]
	if hold.ID == "" {
		t.Fatal("fixture lost its original current Hold")
	}
	if err := svc.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-b", ID: "crm", Name: "Neighbor", Active: true}); err != nil {
		t.Fatal("could not prepare independent real source")
	}
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-b", SchemaID: "audit.event", Version: 1, EventType: "audit.event", RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal("could not prepare independent exact schema")
	}
	// The original fixture's archived source/Put/fsync stays unchanged. Only
	// these additional samples intentionally remain in the real hot files.
	sourceArchive := svc.Config.Archive
	svc.Config.Archive = nil
	ingest := func(i int) {
		t.Helper()
		event := testEvent(fmt.Sprintf("hold-list-cost-%d", i), "hold-list-cost-operation", pinnedNow.Add(time.Duration(i)*time.Second))
		event.TenantID = "tenant-b"
		event.Payload["value"] = json.Number("10")
		if _, err := svc.Ingest(testCtx, "tenant-b", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal("could not persist original real-file evidence sample")
		}
	}
	measure := func() float64 {
		t.Helper()
		return testing.AllocsPerRun(3, func() {
			if got, err := svc.ListLegalHolds(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(got, []domain.LegalHold{hold}) {
				t.Fatal("Hold list allocation sample changed its original result")
			}
		})
	}
	ingest(0)
	control, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("could not capture original control workload")
	}
	small := measure()
	for i := 1; i < 32; i++ {
		ingest(i)
	}
	if raw, err := os.ReadFile(path); err != nil || !bytes.Equal(raw, control) {
		t.Fatal("allocation comparison changed control instead of only unrelated evidence")
	}
	before := restoreCreationSnapshot(t, svc)
	images := admissionEvidenceFiles(t, filepath.Dir(path))
	objects := &checkpointArchive{Store: sourceArchive}
	svc.Config.Archive = objects
	reads := len(readFacts(t, svc))
	large := measure()
	t.Logf("hold_list_control_read_allocation hot_samples=1,32 measured_calls_each=4 allocs_small=%.0f allocs_large=%.0f", small, large)
	if large > small+16 {
		t.Error("Hold list metadata read allocated in proportion to unrelated hot/cold evidence")
	}
	if len(before.Events) != 32 || len(before.Receipts) != 33 || !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) || objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != reads {
		t.Error("allocation samples lost real evidence, changed facts or performed WORM I/O")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
	if err := svc.Store.Close(); err != nil {
		t.Fatal("could not close owned measured file store")
	}
	reopened := reopenHoldControlFile(t, path)
	reopened.Config.Archive = objects
	if got, err := reopened.ListLegalHolds(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(got, []domain.LegalHold{hold}) || !reflect.DeepEqual(restoreCreationSnapshot(t, reopened), before) {
		t.Error("real evidence or original Hold did not survive allocation-test restart")
	}
	if receipt, err := reopened.GetReceipt(testCtx, "tenant-b", "", "hold-list-cost-31"); err != nil || !reflect.DeepEqual(receipt, before.Receipts[store.EventKey("tenant-b", "hold-list-cost-31")]) {
		t.Error("real measured tenant lost its exact committed receipt")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
}
