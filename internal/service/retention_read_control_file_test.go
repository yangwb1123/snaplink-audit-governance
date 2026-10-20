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

func retentionReadControlFileFixture(t *testing.T) (*Service, string) {
	t.Helper()
	svc, path, _ := restoreDecisionFileFixture(t)
	for _, policy := range []domain.RetentionPolicy{retentionControlPolicy(), {TenantID: "tenant-b", ArchiveDays: 730, RetentionClass: "neighbor"}} {
		if err := svc.SetRetentionPolicy(testCtx, "policy-manager", policy); err != nil {
			t.Fatal("could not commit original independent tenant policy")
		}
	}
	return svc, path
}

func TestRetentionReadControlFileKeepsEvidenceFactsUnknownTempAndRestart(t *testing.T) {
	for _, unknownTemp := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown-temp=%t", unknownTemp), func(t *testing.T) {
			svc, path := retentionReadControlFileFixture(t)
			before := restoreCreationSnapshot(t, svc)
			images := admissionEvidenceFiles(t, filepath.Dir(path))
			if len(images) != 4 || len(before.Events) != 0 || len(before.ExportPlans["admission-neighbor"].Members) != 1 {
				t.Fatal("policy read oracle requires two real layouts, evicted source and authenticated selected stage")
			}
			control, err := os.ReadFile(path)
			if err != nil {
				t.Fatal("could not capture owned control bytes")
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal("could not inspect owned control inode")
			}
			unknown := []byte("owned-retention-read-unknown-temp")
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
				for _, tenant := range []string{"tenant-a", "tenant-b"} {
					got, err := current.GetRetentionPolicy(testCtx, tenant)
					if err != nil || got != before.Policies[tenant] {
						t.Error("policy read/restart changed exact tenant configuration")
					}
				}
				if got, err := current.GetRetentionPolicy(testCtx, "missing"); !errors.Is(err, domain.ErrNotFound) || got != (domain.RetentionPolicy{}) {
					t.Error("policy read invented a missing tenant configuration")
				}
				raw, readErr := os.ReadFile(path)
				after, statErr := os.Stat(path)
				if readErr != nil || statErr != nil || !bytes.Equal(raw, control) || !os.SameFile(info, after) || !reflect.DeepEqual(restoreCreationSnapshot(t, current), before) {
					t.Error("policy observation rewrote control/evidence or execution authority")
				}
				assertAdmissionEvidenceFilesUnchanged(t, images)
				if unknownTemp {
					if raw, err := os.ReadFile(path + ".tmp"); err != nil || !bytes.Equal(raw, unknown) {
						t.Error("policy observation discarded or guessed success from unknown temp bytes")
					}
				}
				if objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, current)) != reads {
					t.Error("policy observation performed WORM I/O or changed original silent-read facts")
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
				t.Error("policy observation bypassed the independent current Hold gate")
			}
		})
	}
}

// Measure the same policy read with identical control and 1 versus 32 real
// retained hot events. This measures allocation work, not Ingest or wall time.
func TestRetentionReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence(t *testing.T) {
	svc, path := retentionReadControlFileFixture(t)
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
		event := testEvent(fmt.Sprintf("policy-read-cost-%d", i), "policy-read-cost-operation", pinnedNow.Add(time.Duration(i)*time.Second))
		event.TenantID = "tenant-b"
		event.Payload["value"] = json.Number("10")
		if _, err := svc.Ingest(testCtx, "tenant-b", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal("could not persist original real-file evidence sample")
		}
	}
	measure := func() float64 {
		t.Helper()
		return testing.AllocsPerRun(3, func() {
			if got, err := svc.GetRetentionPolicy(testCtx, "tenant-a"); err != nil || got != retentionControlPolicy() {
				t.Fatal("policy allocation sample changed its original result")
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
	t.Logf("retention_control_read_allocation hot_samples=1,32 measured_calls_each=4 allocs_small=%.0f allocs_large=%.0f", small, large)
	if large > small+16 {
		t.Error("policy metadata read allocated in proportion to unrelated hot/cold evidence")
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
	if got, err := reopened.GetRetentionPolicy(testCtx, "tenant-a"); err != nil || got != retentionControlPolicy() || !reflect.DeepEqual(restoreCreationSnapshot(t, reopened), before) {
		t.Error("real evidence or original configuration did not survive allocation-test restart")
	}
	if receipt, err := reopened.GetReceipt(testCtx, "tenant-b", "", "policy-read-cost-31"); err != nil || !reflect.DeepEqual(receipt, before.Receipts[store.EventKey("tenant-b", "policy-read-cost-31")]) {
		t.Error("real measured tenant lost its exact committed receipt")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
}
