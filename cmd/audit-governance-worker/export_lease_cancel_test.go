package main

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestRunEvaluatePassCanceledLeaseRecoveryDoesNotPublishSuccess(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	now := time.Unix(1700000000, 0).UTC()
	started := now.Add(-service.ExportLeaseDuration)
	svc.Config.Now = func() time.Time { return now }
	backend.data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Active: true}
	backend.data.Tenants["tenant-b"] = domain.Tenant{ID: "tenant-b", Active: true}
	backend.data.Exports["export-canceled"] = domain.ExportJob{ID: "export-canceled", TenantID: "tenant-a", Status: "running", StartedAt: &started, CreatedAt: started}
	lease := store.ExportLease{Version: 1, JobID: "export-canceled", TenantID: "tenant-a", Owner: "worker-private-owner", HeartbeatAt: started, ExpiresAt: now}
	backend.data.ExportLeases[lease.JobID] = lease
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	runEvaluatePass(ctx, log.New(&out, "", 0), svc)
	text := out.String()
	if !strings.Contains(text, "tenant=tenant-a export_recovery_error=context canceled") || strings.Contains(text, "stuck_exports_recovered=") || strings.Contains(text, "tenant=tenant-b") || strings.Count(text, "pass_aborted=") != 1 {
		t.Fatalf("canceled recovery was reported as success or advanced tenant: %q", text)
	}
	if backend.saves != 0 || backend.data.Exports[lease.JobID].Status != "running" || backend.data.ExportLeases[lease.JobID] != lease {
		t.Fatal("canceled recovery saved terminal state or retired its lease")
	}
	for _, fact := range backend.data.AdminActions {
		if fact.Action == domain.AdminActionExportRecovered {
			t.Fatal("canceled recovery committed a false recovered fact")
		}
	}
}
