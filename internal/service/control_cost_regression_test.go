package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Keep the original three tests read-only. This additional observation records
// both metadata readers against the SAME real-file fixture/control bytes, and
// distinguishes unrelated evidence from the fields a control clone must own.
func TestControlCostRealFileReadersWorkAndRestart(t *testing.T) {
	svc, path := retentionReadControlFileFixture(t)
	for _, setup := range []func() error{
		func() error {
			return svc.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-b", ID: "crm", Name: "Neighbor", Active: true})
		},
		func() error {
			return svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-b", SchemaID: "audit.event", Version: 1, EventType: "audit.event", RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}})
		},
	} {
		if err := setup(); err != nil {
			t.Fatal(err)
		}
	}
	archive := svc.Config.Archive // retain the fixture's original real archive
	svc.Config.Archive = nil      // only the additional samples remain hot
	hold := leaseSnapshot(t, svc).LegalHolds["decision-current-hold"]
	var control []byte
	for i := 0; i < 32; i++ {
		event := testEvent(fmt.Sprintf("control-cost-file-%d", i), "control-cost-operation", pinnedNow.Add(time.Duration(i)*time.Second))
		event.TenantID = "tenant-b"
		event.Payload["value"] = json.Number("10")
		if _, err := svc.Ingest(testCtx, "tenant-b", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
		if i != 0 && i != 31 {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			control = raw
		} else if !bytes.Equal(raw, control) {
			t.Fatal("unrelated hot samples changed the measured control workload")
		}
		snapshot := restoreCreationSnapshot(t, svc)
		holdAllocs := testing.AllocsPerRun(3, func() {
			if got, err := svc.ListLegalHolds(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(got, []domain.LegalHold{hold}) {
				t.Fatal("Hold result changed during allocation sample")
			}
		})
		policyAllocs := testing.AllocsPerRun(3, func() {
			if got, err := svc.GetRetentionPolicy(testCtx, "tenant-a"); err != nil || got != retentionControlPolicy() {
				t.Fatal("policy result changed during allocation sample")
			}
		})
		t.Logf("real_file_work hot_samples=%d receipts=%d control_bytes=%d outbox=%d admin_actions=%d hold_allocs=%.0f policy_allocs=%.0f measured_calls_each=4", i+1, len(snapshot.Receipts), len(raw), len(snapshot.LedgeredOutbox), len(snapshot.AdminActions), holdAllocs, policyAllocs)
	}
	before := restoreCreationSnapshot(t, svc)
	images := admissionEvidenceFiles(t, filepath.Dir(path))
	reads := len(readFacts(t, svc))
	objects := &checkpointArchive{Store: archive}
	svc.Config.Archive = objects
	if err := svc.Store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := reopenHoldControlFile(t, path)
	reopened.Config.Archive = objects
	if got, err := reopened.ListLegalHolds(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(got, []domain.LegalHold{hold}) {
		t.Fatal("Hold metadata lost across real-file restart")
	}
	if got, err := reopened.GetRetentionPolicy(testCtx, "tenant-a"); err != nil || got != retentionControlPolicy() {
		t.Fatal("policy metadata lost across real-file restart")
	}
	if raw, err := os.ReadFile(path); err != nil || !bytes.Equal(raw, control) || !reflect.DeepEqual(restoreCreationSnapshot(t, reopened), before) || len(readFacts(t, reopened)) != reads || objects.gets.Load() != 0 || objects.puts.Load() != 0 {
		t.Fatal("metadata read/restart changed evidence, control, original read facts or WORM I/O")
	}
	assertAdmissionEvidenceFilesUnchanged(t, images)
	if receipt, err := reopened.GetReceipt(testCtx, "tenant-b", "", "control-cost-file-31"); err != nil || !reflect.DeepEqual(receipt, before.Receipts[store.EventKey("tenant-b", "control-cost-file-31")]) {
		t.Fatal("real committed ledger receipt lost after measured reads/restart")
	}
}

func TestControlCostArchivedLedgerIngestObservation(t *testing.T) {
	for _, archived := range []int{0, 10000} {
		duration, allocs := measureArchivedIngestCost(t, archived)
		t.Logf("original_ingest_work archived_receipts=%d allocs/call=%.0f duration/call=%s allocation_calls=6 timed_calls=10", archived, allocs, duration)
	}
}
