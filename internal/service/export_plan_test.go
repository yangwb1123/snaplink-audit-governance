package service

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportPlanUnknownSaveResumesOriginalMembershipWithoutReselection(t *testing.T) {
	svc, backend, objects := newPlanService(t)
	backend.unknownPlan.Store(true)
	puts := objects.puts.Load()
	svc.runExport(testCtx, "checkpoint-job")
	data := leaseSnapshot(t, svc)
	plan := data.ExportPlans["checkpoint-job"]
	started := data.Exports[plan.JobID].StartedAt
	if !svc.validExportPlan(data.Exports[plan.JobID], plan, svc.Now()) || objects.puts.Load() != puts || len(data.ExportCheckpoints) != 0 {
		t.Fatal("unknown selected Save was guessed successful or did not really persist")
	}
	if checkpointFactCount(t, svc, domain.AdminActionExportPlanned) != 1 {
		t.Fatal("selected stage and its fact were not committed once")
	}
	backend.unknownPlan.Store(false)
	expireCheckpoint(svc)
	late := testEvent("plan-late", "checkpoint-operation", pinnedNow.Add(20*time.Second))
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, late, domain.StatusArchived)
	if err != nil {
		t.Fatal(err)
	}
	probe := &planSourceArchive{Store: objects, forbidden: eventArchiveKey("tenant-a", receipt.StreamID, receipt.Sequence, receipt.EventID)}
	svc.Config.Archive = probe
	puts = objects.puts.Load()
	if n, err := svc.RecoverStuckExports(testCtx, "tenant-a"); err != nil || n != 0 {
		t.Fatal("selected checkpoint was failed or misreported as completed recovery")
	}
	svc.runExport(testCtx, plan.JobID)
	data = leaseSnapshot(t, svc)
	job := data.Exports[plan.JobID]
	if job.Status != "completed" || job.EventCount != 1 || !job.StartedAt.Equal(*started) || len(data.ExportPlans) != 0 || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 || objects.puts.Load() != puts+1 || probe.sourceGets.Load() != 1 {
		t.Fatal("selected continuation reselected output, reset identity or lost the phase transition")
	}
	sealed, err := objects.Get(testCtx, job.ObjectPath)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := svc.VerifyExportDownload(testCtx, "tenant-a", job.ID, sealed)
	if err != nil || !exportPlainMatchesPlan(plain, plan) {
		t.Fatal("completed artifact does not match the original authenticated members")
	}
	if checkpointFactCount(t, svc, domain.AdminActionExportPlanned) != 1 || checkpointFactCount(t, svc, domain.AdminActionExportResumed) != 1 || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 1 {
		t.Fatal("continuation duplicated or lost stage facts")
	}
	svc.runExport(testCtx, job.ID)
	if !reflect.DeepEqual(leaseSnapshot(t, svc).Exports[job.ID], job) || objects.puts.Load() != puts+1 {
		t.Fatal("terminal export was regenerated or regressed")
	}
}

func TestExportPlanUncommittedSaveNeverPutsAndStillExpiresAsUncheckpointed(t *testing.T) {
	svc, backend, objects := newPlanService(t)
	backend.failPlan.Store(true)
	puts := objects.puts.Load()
	svc.runExport(testCtx, "checkpoint-job")
	if got := leaseSnapshot(t, svc); got.Exports["checkpoint-job"].Status != "running" || len(got.ExportPlans) != 0 || objects.puts.Load() != puts || checkpointFactCount(t, svc, domain.AdminActionExportPlanned) != 0 {
		t.Fatal("failed plan Save invented progress or generated output")
	}
	backend.failPlan.Store(false)
	expireCheckpoint(svc)
	if n, err := svc.RecoverStuckExports(testCtx, "tenant-a"); err != nil || n != 1 {
		t.Fatal("pre-plan crash no longer uses uncheckpointed lease failure")
	}
	if leaseSnapshot(t, svc).Exports["checkpoint-job"].Status != "failed" {
		t.Fatal("uncheckpointed task was requeued instead of failed")
	}
}

func TestExportPlanResumeFailsClosedOnReadAuditAndSchemaDrift(t *testing.T) {
	for _, mode := range []string{"read-audit", "schema-definition", "schema-version", "missing-source", "content", "ledger-coordinate"} {
		t.Run(mode, func(t *testing.T) {
			svc, backend, objects := newPlanService(t)
			old := leaveSelectedPlan(t, svc)
			member := old.Plan.Members[0]
			if mode == "read-audit" {
				backend.failRead.Store(true)
			} else if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				key := store.EventKey(old.TenantID, member.EventID)
				schemaKey := store.SchemaKey(old.TenantID, member.SchemaID, member.SchemaVersion)
				switch mode {
				case "schema-definition":
					schema := data.Schemas[schemaKey]
					schema.Classification = "drifted"
					data.Schemas[schemaKey] = schema
				case "schema-version":
					delete(data.Schemas, schemaKey)
				case "missing-source":
					delete(data.Events, key)
					receipt := data.Receipts[key]
					receipt.Status = domain.StatusLedgered
					data.Receipts[key] = receipt
				case "content":
					event := testEvent(member.EventID, "checkpoint-operation", member.OccurredAt)
					event.TenantID = old.TenantID
					event.StreamID, event.Sequence, event.Hash = member.StreamID, member.Sequence, member.Hash
					event.Actor.ID = "changed"
					data.Events[key] = event
				case "ledger-coordinate":
					receipt := data.Receipts[key]
					receipt.Hash = strings.Repeat("a", 64)
					data.Receipts[key] = receipt
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			expireCheckpoint(svc)
			puts := objects.puts.Load()
			svc.runExport(testCtx, old.ID)
			got := leaseSnapshot(t, svc)
			if got.Exports[old.ID].Status != "running" || got.ExportPlans[old.ID].Signature != old.Plan.Signature || objects.puts.Load() != puts || got.Exports[old.ID].ObjectPath != "" || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 0 {
				t.Fatal("unknown evidence/audit failure was reselected, published or discarded")
			}
		})
	}
}

func TestExportPlanOwnerCancellationCleansOnlyActiveMetadata(t *testing.T) {
	svc, _, objects := newPlanService(t)
	old := leaveSelectedPlan(t, svc)
	pauseExportDispatch(svc)
	puts := objects.puts.Load()
	if _, err := svc.CancelExport(testCtx, old.TenantID, old.RequestedBy, old.ID); err != nil {
		t.Fatal(err)
	}
	got := leaseSnapshot(t, svc)
	if got.Exports[old.ID].Status != domain.ExportStatusCancelled || len(got.ExportPlans) != 0 || len(got.ExportLeases) != 0 || checkpointFactCount(t, svc, domain.AdminActionExportPlanned) != 1 {
		t.Fatal("cancellation lost selected fact or retained active execution metadata")
	}
	if err := svc.saveExportPlan(exportClaimContext(testCtx, old), old, old.Plan); err == nil {
		t.Fatal("cancelled owner republished selected stage")
	}
	expireCheckpoint(svc)
	svc.runExport(testCtx, old.ID)
	if objects.puts.Load() != puts || leaseSnapshot(t, svc).Exports[old.ID].Status != domain.ExportStatusCancelled {
		t.Fatal("cancelled terminal was resumed")
	}
	raw, err := json.Marshal(domain.SummarizeExport(got.Exports[old.ID]))
	if err != nil || strings.Contains(string(raw), old.Plan.Signature) || strings.Contains(string(raw), old.Owner) || strings.Contains(string(raw), "members") {
		t.Fatal("private plan/owner escaped public summary")
	}
}
