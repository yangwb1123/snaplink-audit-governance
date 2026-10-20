package service

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportPlanMetadataDriftCannotResumeCancelOrDisappear(t *testing.T) {
	cases := map[string]func(*domain.ExportJob, *store.ExportPlan){
		"version":        func(_ *domain.ExportJob, p *store.ExportPlan) { p.Version++ },
		"format":         func(_ *domain.ExportJob, p *store.ExportPlan) { p.FormatVersion++ },
		"job":            func(_ *domain.ExportJob, p *store.ExportPlan) { p.JobID = "other" },
		"tenant":         func(_ *domain.ExportJob, p *store.ExportPlan) { p.TenantID = "tenant-b" },
		"future":         func(_ *domain.ExportJob, p *store.ExportPlan) { p.CreatedAt = p.CreatedAt.Add(time.Hour) },
		"pre-start":      func(_ *domain.ExportJob, p *store.ExportPlan) { p.CreatedAt = p.CreatedAt.Add(-time.Second) },
		"binding":        func(_ *domain.ExportJob, p *store.ExportPlan) { p.Binding = "" },
		"member-hash":    func(_ *domain.ExportJob, p *store.ExportPlan) { p.Members[0].Hash = "" },
		"schema-version": func(_ *domain.ExportJob, p *store.ExportPlan) { p.Members[0].SchemaVersion = 0 },
		"duplicate":      func(_ *domain.ExportJob, p *store.ExportPlan) { p.Members = append(p.Members, p.Members[0]) },
		"order": func(_ *domain.ExportJob, p *store.ExportPlan) {
			other := p.Members[0]
			other.EventID, other.Sequence = "earlier", other.Sequence+1
			other.OccurredAt = other.OccurredAt.Add(-time.Second)
			p.Members = append(p.Members, other)
		},
		"query":     func(j *domain.ExportJob, _ *store.ExportPlan) { j.Query.ActorID = "changed" },
		"cursor":    func(j *domain.ExportJob, _ *store.ExportPlan) { j.Query.Cursor = "changed" },
		"requester": func(j *domain.ExportJob, _ *store.ExportPlan) { j.RequestedBy = "changed" },
		"retry-of":  func(j *domain.ExportJob, _ *store.ExportPlan) { j.RetryOf = "changed" },
		"created":   func(j *domain.ExportJob, _ *store.ExportPlan) { j.CreatedAt = j.CreatedAt.Add(-time.Second) },
		"started":   func(j *domain.ExportJob, _ *store.ExportPlan) { at := j.StartedAt.Add(-time.Second); j.StartedAt = &at },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			svc, _, objects := newPlanService(t)
			old := leaveSelectedPlan(t, svc)
			if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
				job, plan := data.Exports[old.ID], data.ExportPlans[old.ID]
				mutate(&job, &plan)
				plan.Signature = svc.signExportPlan(plan) // malformed structure, even with a valid MAC
				data.Exports[old.ID], data.ExportPlans[old.ID] = job, plan
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			expireCheckpoint(svc)
			before := leaseSnapshot(t, svc)
			puts, gets := objects.puts.Load(), objects.gets.Load()
			svc.runExport(testCtx, old.ID)
			if _, err := svc.RecoverStuckExports(testCtx, old.TenantID); err == nil {
				t.Fatal("recovery accepted unknown or drifted plan metadata")
			}
			actor := before.Exports[old.ID].RequestedBy
			if _, err := svc.CancelExport(testCtx, old.TenantID, actor, old.ID); err == nil {
				t.Fatal("cancellation silently cleared unknown plan metadata")
			}
			if !reflect.DeepEqual(before, leaseSnapshot(t, svc)) || objects.puts.Load() != puts || objects.gets.Load() != gets {
				t.Fatal("invalid plan changed durable state or touched source objects")
			}
		})
	}
}

func TestExportPlanMACAndBudgetsFailClosedWithoutCopiesOfSensitiveFilters(t *testing.T) {
	svc, _, _ := newPlanService(t)
	claim := leaveSelectedPlan(t, svc)
	plan := copyExportPlan(claim.Plan)
	plan.Members[0].SchemaDigest = strings.Repeat("0", 64)
	if svc.validExportPlan(claim.ExportJob, plan, svc.Now()) {
		t.Fatal("member mutation retained a valid plan MAC")
	}
	plan = copyExportPlan(claim.Plan)
	plan.Members = make([]store.ExportPlanMember, store.MaxExportPlanEvents+1)
	plan.Signature = svc.signExportPlan(plan)
	if svc.validExportPlan(claim.ExportJob, plan, svc.Now()) {
		t.Fatal("oversized member list passed the fixed identity budget")
	}
	member := claim.Plan.Members[0]
	events := make([]domain.Event, 5000)
	for i := range events {
		events[i] = domain.Event{EventID: fmt.Sprintf("%05d-", i) + strings.Repeat("a", 1024), TenantID: claim.TenantID,
			StreamID: member.StreamID, Sequence: int64(i + 1), Hash: member.Hash, OccurredAt: member.OccurredAt,
			SchemaID: member.SchemaID, SchemaVersion: member.SchemaVersion}
	}
	data, err := svc.Store.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.newExportPlan(claim.ExportJob, events, data.Schemas); err == nil {
		t.Fatal("oversized encoded plan passed the unchanged 4MiB budget")
	}
	// The only filter binding is keyed and purpose-separated, not another
	// raw search digest or query cursor in the plan or its operational facts.
	if strings.Contains(fmt.Sprintf("%#v", claim.Plan), "payload_digest") || claim.Plan.Binding == svc.exportCheckpointBinding(claim.ExportJob) {
		t.Fatal("selected metadata copied a sensitive filter or reused a MAC purpose")
	}
}
