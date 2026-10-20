package service

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportPlanSelectionAuthenticatesBeforeSensitivePredicate(t *testing.T) {
	for _, drift := range []string{"missing-sidecar", "wrong-sidecar", "corrupted-filtered-content"} {
		t.Run(drift, func(t *testing.T) {
			svc, _, objects := newPlanService(t)
			if err := svc.RegisterSchema(testCtx, "operator", domain.EventSchema{TenantID: "tenant-a", SchemaID: "plan.exact", Version: 1, EventType: "audit.event", Active: true,
				RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, EncryptedFields: []string{"value"}, SearchableFields: []string{"value"}}); err != nil {
				t.Fatal(err)
			}
			value := json.Number("9007199254740993")
			event := testEvent("plan-sensitive", "operation", pinnedNow.Add(15*time.Second))
			event.SchemaID, event.Payload["value"] = "plan.exact", value
			receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived)
			if err != nil {
				t.Fatal("could not commit exact sensitive fixture")
			}
			source, err := svc.archivedEventContext(testCtx, receipt)
			if err != nil {
				t.Fatal("could not read committed sensitive fixture")
			}
			delete(source.Payload, "value"+security.SearchDigestSuffix)
			if drift == "wrong-sidecar" {
				source.Payload["value"+security.SearchDigestSuffix] = "invalid-sidecar"
			}
			if drift == "corrupted-filtered-content" {
				source.Actor.ID = "different-from-query"
			}
			digest, err := security.SearchDigestBound(value, svc.Config.EncryptionKey, "tenant-a", "value")
			if err != nil {
				t.Fatal("could not construct sensitive predicate")
			}
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				data.Events[store.EventKey("tenant-a", event.EventID)] = source
				job := data.Exports["checkpoint-job"]
				job.Query.PayloadField, job.Query.PayloadDigest, job.Query.ActorID = "value", digest, event.Actor.ID
				data.Exports[job.ID] = job
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			puts := objects.puts.Load()
			claim, won, err := svc.claimPendingExport(testCtx, "checkpoint-job")
			if err != nil || !won {
				t.Fatal("selection fixture could not claim")
			}
			claim, _, ready := svc.exportExecutionEvidence(exportClaimContext(testCtx, claim), claim)
			if drift == "corrupted-filtered-content" {
				if ready || len(leaseSnapshot(t, svc).ExportPlans) != 0 || leaseSnapshot(t, svc).Exports[claim.ID].Status != "failed" {
					t.Fatal("corrupt nonmatching metadata hid committed evidence from plan authentication")
				}
			} else if !ready || len(claim.Plan.Members) != 1 || claim.Plan.Members[0].EventID != event.EventID {
				t.Fatal("derived sidecar pollution changed authenticated selected membership")
			}
			if objects.puts.Load() != puts {
				t.Fatal("selection stage generated an object before its durable boundary")
			}
		})
	}
}

func TestExportPlanConcurrentSchemaChangeDuringPutCannotAdvanceSealedStage(t *testing.T) {
	svc, _, objects := newPlanService(t)
	paused := &leasePausedArchive{Store: objects, entered: make(chan leasePutObservation, 1), release: make(chan struct{})}
	svc.Config.Archive = paused
	var once sync.Once
	release := func() { once.Do(func() { close(paused.release) }) }
	done := make(chan struct{})
	t.Cleanup(func() { release(); <-done })
	go func() { svc.runExport(context.Background(), "checkpoint-job"); close(done) }()
	var put leasePutObservation
	select {
	case put = <-paused.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("executor did not reach actual Put boundary")
	}
	plan := leaseSnapshot(t, svc).ExportPlans["checkpoint-job"]
	member := plan.Members[0]
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		key := store.SchemaKey(plan.TenantID, member.SchemaID, member.SchemaVersion)
		schema := data.Schemas[key]
		schema.Classification = "concurrent-version-drift"
		data.Schemas[key] = schema
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("schema-drifted executor did not stop")
	}
	got := leaseSnapshot(t, svc)
	if got.Exports[plan.JobID].Status != "running" || got.Exports[plan.JobID].ObjectPath != "" || got.ExportPlans[plan.JobID].Signature != plan.Signature || len(got.ExportCheckpoints) != 0 {
		t.Fatal("concurrent schema drift was published as an authenticated sealed checkpoint")
	}
	if raw, err := objects.Get(testCtx, put.key); err != nil || len(raw) == 0 {
		t.Fatal("fencing guessed external rollback or removed unknown-result object")
	}
}
