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

func TestRestoreReadControlFileKeepsEvidenceFactsUnknownTempAndRestart(t *testing.T) {
	for _, target := range []string{domain.RestoreStatusApproved, domain.RestoreStatusRejected} {
		for _, unknownTemp := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unknown-temp=%t", target, unknownTemp), func(t *testing.T) {
				svc, path, run := restoreDecisionFileFixture(t)
				if pending, err := svc.GetRestore(testCtx, "tenant-a", run.ID); err != nil || !reflect.DeepEqual(pending, run) {
					t.Fatal("original pending request was not observable")
				}
				want, err := callRestoreDecision(testCtx, svc, "tenant-a", run.ID, "decider", target)
				if err != nil {
					t.Fatal("could not commit original independent decision")
				}
				before := restoreCreationSnapshot(t, svc)
				images := admissionEvidenceFiles(t, filepath.Dir(path))
				if len(images) != 4 || len(before.Events) != 0 || len(before.ExportPlans["admission-neighbor"].Members) != 1 {
					t.Fatal("read oracle requires two real layouts, evicted source and authenticated selected stage")
				}
				control, err := os.ReadFile(path)
				if err != nil {
					t.Fatal("could not capture owned control bytes")
				}
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal("could not inspect owned control inode")
				}
				unknown := []byte("owned-restore-read-unknown-temp")
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
					got, err := current.GetRestore(testCtx, "tenant-a", run.ID)
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Error("control read/restart lost the original exact decision")
					}
					if got.ApprovedAt != nil {
						*got.ApprovedAt = pinnedNow
					}
					if got.RejectedAt != nil {
						*got.RejectedAt = pinnedNow
					}
					if got, err := current.GetRestore(testCtx, "tenant-b", run.ID); !errors.Is(err, domain.ErrNotFound) || got != (domain.RestoreRun{}) {
						t.Error("restore detail acquired an all-tenant or foreign-record fallback")
					}
					raw, readErr := os.ReadFile(path)
					after, statErr := os.Stat(path)
					if readErr != nil || statErr != nil || !bytes.Equal(raw, control) || !os.SameFile(info, after) || !reflect.DeepEqual(restoreCreationSnapshot(t, current), before) {
						t.Error("restore detail rewrote control/evidence or retained caller-owned timestamps")
					}
					assertAdmissionEvidenceFilesUnchanged(t, images)
					if unknownTemp {
						if raw, err := os.ReadFile(path + ".tmp"); err != nil || !bytes.Equal(raw, unknown) {
							t.Error("metadata read guessed success from or discarded unknown temp bytes")
						}
					}
					if objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, current)) != reads {
						t.Error("restore detail performed WORM I/O or changed the existing silent-read facts")
					}
				}
				check(svc)
				if err := svc.Store.Close(); err != nil {
					t.Fatal("could not close original owned file writer")
				}
				reopened := reopenHoldControlFile(t, path)
				reopened.Config.Archive = objects
				check(reopened)
				if _, err := reopened.GetExport(testCtx, "tenant-a", "admission-neighbor"); !errors.Is(err, domain.ErrForbidden) {
					t.Error("restore metadata observation bypassed the independent current Hold gate")
				}
			})
		}
	}
}

// Measure allocation work of the same metadata read, not wall time or Ingest.
// Preparation and every sample keep the real default-filesystem file backend.
// The two captures have byte-equivalent control but 1 versus 32 hot events.
func TestRestoreReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence(t *testing.T) {
	svc, path, run := restoreDecisionFileFixture(t)
	if err := svc.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-b", ID: "crm", Name: "Neighbor", Active: true}); err != nil {
		t.Fatal("could not prepare independent real source")
	}
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-b", SchemaID: "audit.event", Version: 1, EventType: "audit.event", RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal("could not prepare independent exact schema")
	}
	// Keep the fixture's original archived source/Put/fsync. The additional
	// unrelated sample intentionally exercises real retained hot documents.
	sourceArchive := svc.Config.Archive
	svc.Config.Archive = nil
	ingest := func(i int) {
		t.Helper()
		event := testEvent(fmt.Sprintf("read-cost-%d", i), "read-cost-operation", pinnedNow.Add(time.Duration(i)*time.Second))
		event.TenantID = "tenant-b"
		event.Payload["value"] = json.Number("10") // same exact representation before/after real file restart
		if _, err := svc.Ingest(testCtx, "tenant-b", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal("could not persist original real-file evidence sample")
		}
	}
	measure := func() float64 {
		t.Helper()
		return testing.AllocsPerRun(3, func() {
			if got, err := svc.GetRestore(testCtx, "tenant-a", run.ID); err != nil || !reflect.DeepEqual(got, run) {
				t.Fatal("metadata allocation sample changed its original result")
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
	t.Logf("restore_control_read_allocation hot_samples=1,32 measured_calls_each=4 allocs_small=%.0f allocs_large=%.0f", small, large)
	// A small constant margin covers runtime bookkeeping, not per-event clones.
	if large > small+16 {
		t.Error("restore metadata read allocated in proportion to unrelated hot/cold evidence")
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
	if got, err := reopened.GetRestore(testCtx, "tenant-a", run.ID); err != nil || !reflect.DeepEqual(got, run) || !reflect.DeepEqual(restoreCreationSnapshot(t, reopened), before) {
		t.Error("real evidence or original request did not survive allocation-test restart")
	}
	if receipt, err := reopened.GetReceipt(testCtx, "tenant-b", "", "read-cost-31"); err != nil || !reflect.DeepEqual(receipt, before.Receipts[store.EventKey("tenant-b", "read-cost-31")]) {
		t.Error("real measured tenant lost its exact committed receipt")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
}
