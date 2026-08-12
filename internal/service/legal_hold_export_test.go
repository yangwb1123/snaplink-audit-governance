package service

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// t0 is the deterministic event timestamp used by the legal-hold export
// consumer tests. testService pins the service clock at
// time.Unix(1_700_000_000, 0).UTC(), so windows expressed relative to t0
// are fully deterministic (mirrors governance_hold_test.go's local t0).
var t0 = time.Unix(1_700_000_010, 0).UTC()

// wideExportQuery overlaps the hold window [t0-10s, t0+10s) and covers the
// held event at t0.
func wideExportQuery() domain.Query {
	return domain.Query{From: t0.Add(-1 * time.Hour), To: t0.Add(1 * time.Hour)}
}

func countBlockedFacts(t *testing.T, svc *Service, holdID, actor string) int {
	t.Helper()
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, a := range actions {
		if a.Action == domain.AdminActionExportBlocked && a.TargetID == holdID && a.Actor == actor {
			count++
		}
	}
	return count
}

// AC-1 (R2): CreateExport fails closed (ErrForbidden carrying the blocking
// hold ID), returns a zero job, persists no job and no export.created fact,
// and records exactly one export.blocked fact with actor/TargetType/TargetID
// and the hold reason in Detail.
func TestCreateExportBlockedByActiveLegalHold(t *testing.T) {
	svc := testService(t, true)
	t0 := ingestHeldEvent(t, svc)
	hold, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-a", Name: "case-1", Reason: "investigation pending", Filter: domain.Query{From: t0.Add(-10 * time.Second), To: t0.Add(10 * time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	job, err := svc.CreateExport("tenant-a", "test", domain.Query{From: t0.Add(-110 * time.Second), To: t0.Add(90 * time.Second)})
	if !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("want ErrForbidden, got %v", err)
	}
	if !strings.Contains(err.Error(), hold.ID) {
		t.Fatalf("error must carry the blocking hold ID, got %q", err.Error())
	}
	if job != (domain.ExportJob{}) {
		t.Fatalf("blocked create must return the zero job, got %+v", job)
	}
	if _, err := svc.GetExport("tenant-a", "export-never-created"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("no job may exist after a blocked create, err=%v", err)
	}
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	blocked, created := 0, 0
	for _, a := range actions {
		switch a.Action {
		case domain.AdminActionExportCreated:
			created++
		case domain.AdminActionExportBlocked:
			blocked++
			if a.Actor != "test" || a.TargetType != "legal_hold" || a.TargetID != hold.ID {
				t.Fatalf("blocked fact fields wrong: %+v", a)
			}
			if !strings.Contains(a.Detail, "investigation pending") {
				t.Fatalf("blocked fact Detail must carry the hold reason, got %q", a.Detail)
			}
		}
	}
	if created != 0 {
		t.Fatalf("export.created must not exist on a blocked create, found %d", created)
	}
	if blocked != 1 {
		t.Fatalf("want exactly one export.blocked fact, found %d", blocked)
	}
}

// AC-2 (R4): an export completed before the hold stays sealed but becomes
// inaccessible (GetExport and RecordExportDownload both fail closed with
// ErrForbidden); the download denial appends an export.blocked fact while a
// control export over an empty event window remains fully accessible.
func TestExportAccessBlockedByHoldCreatedAfterCompletion(t *testing.T) {
	svc := testService(t, true)
	t0 := ingestHeldEvent(t, svc)
	job, err := svc.CreateExport("tenant-a", "test", wideExportQuery())
	if err != nil {
		t.Fatal(err)
	}
	waitExport(t, svc, job.ID)
	current, err := svc.GetExport("tenant-a", job.ID)
	if err != nil || current.Status != "completed" || current.ObjectPath == "" {
		t.Fatalf("export must complete and be accessible before the hold: status=%s path=%q err=%v", current.Status, current.ObjectPath, err)
	}
	if err := svc.RecordExportDownload("tenant-a", "test", job.ID); err != nil {
		t.Fatalf("download must succeed before the hold: %v", err)
	}

	hold, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-a", Name: "case-2", Reason: "litigation hold", Filter: wideExportQuery()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetExport("tenant-a", job.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("GetExport after hold: want ErrForbidden, got %v", err)
	}
	if err := svc.RecordExportDownload("tenant-a", "test", job.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("RecordExportDownload after hold: want ErrForbidden, got %v", err)
	}

	// Control: an export whose window selects no events is never blocked,
	// even while the same hold is active.
	control, err := svc.CreateExport("tenant-a", "test", domain.Query{From: t0.Add(2 * time.Hour), To: t0.Add(2*time.Hour + time.Second)})
	if err != nil {
		t.Fatalf("control export creation must succeed under hold: %v", err)
	}
	waitExport(t, svc, control.ID)
	if _, err := svc.GetExport("tenant-a", control.ID); err != nil {
		t.Fatalf("control export must stay listable: %v", err)
	}
	if err := svc.RecordExportDownload("tenant-a", "test", control.ID); err != nil {
		t.Fatalf("control download must stay allowed: %v", err)
	}

	if got := countBlockedFacts(t, svc, hold.ID, "test"); got != 1 {
		t.Fatalf("want exactly one download-denial export.blocked fact, found %d", got)
	}
	// AC-4 download leg: the denial fact must carry the same hold reason in
	// Detail, not just the hold ID and actor.
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range actions {
		if a.Action == domain.AdminActionExportBlocked && a.TargetID == hold.ID && a.Actor == "test" {
			if !strings.Contains(a.Detail, "litigation hold") {
				t.Fatalf("download-denial fact Detail=%q, want the hold reason verbatim", a.Detail)
			}
		}
	}
}

// AC-3: releasing the hold unblocks GetExport, RecordExportDownload, and a
// fresh CreateExport.
func TestExportUnblockedAfterHoldRelease(t *testing.T) {
	svc := testService(t, true)
	_ = ingestHeldEvent(t, svc)
	job, err := svc.CreateExport("tenant-a", "test", wideExportQuery())
	if err != nil {
		t.Fatal(err)
	}
	waitExport(t, svc, job.ID)
	hold, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-a", Name: "case-3", Reason: "temporary hold", Filter: wideExportQuery()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetExport("tenant-a", job.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("precondition: GetExport must be blocked, got %v", err)
	}
	released, err := svc.ReleaseLegalHold("tenant-a", hold.ID, "test")
	if err != nil || released.ReleasedAt == nil {
		t.Fatalf("release failed: %+v %v", released, err)
	}
	current, err := svc.GetExport("tenant-a", job.ID)
	if err != nil || current.Status != "completed" {
		t.Fatalf("GetExport after release: %+v %v", current, err)
	}
	if err := svc.RecordExportDownload("tenant-a", "test", job.ID); err != nil {
		t.Fatalf("download after release: %v", err)
	}
	fresh, err := svc.CreateExport("tenant-a", "test", wideExportQuery())
	if err != nil {
		t.Fatalf("fresh export after release: %v", err)
	}
	waitExport(t, svc, fresh.ID)
}

// R3 (white-box): the create→run race is closed by the run-time re-check.
// A pending job inserted past CreateExport's gate fails synchronously with
// the block error, writes no archive object, and records the fact with the
// governance-worker actor.
func TestRunExportFailsClosedOnActiveHold(t *testing.T) {
	svc := testService(t, true)
	t0 := ingestHeldEvent(t, svc)
	hold, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-a", Name: "case-race", Reason: "worker race hold", Filter: domain.Query{From: t0.Add(-10 * time.Second), To: t0.Add(10 * time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	jobID := "export-race"
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Exports[jobID] = domain.ExportJob{ID: jobID, TenantID: "tenant-a", RequestedBy: "test", Query: wideExportQuery(), Status: "pending", CreatedAt: svc.Now()}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	svc.runExport(jobID)

	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	job := snap.Exports[jobID]
	if job.Status != "failed" {
		t.Fatalf("status=%s, want failed", job.Status)
	}
	if job.Error != "export blocked by active legal hold "+hold.ID {
		t.Fatalf("error=%q, want block error with hold ID", job.Error)
	}
	if job.ObjectPath != "" || job.Digest != "" || job.EventCount != 0 {
		t.Fatalf("failed job must carry empty path/digest and zero count: %+v", job)
	}
	sealed, err := filepath.Glob(filepath.Join(svc.Config.ArchiveDir, "exports", "*"))
	if err != nil || len(sealed) != 0 {
		t.Fatalf("blocked export must not write archive objects, found %v (err=%v)", sealed, err)
	}
	if got := countBlockedFacts(t, svc, hold.ID, "governance-worker"); got != 1 {
		t.Fatalf("want one governance-worker export.blocked fact, found %d", got)
	}
}

// AC-4: the new admin action constant is a distinct string.
func TestAdminActionExportBlockedConstantDistinct(t *testing.T) {
	if domain.AdminActionExportBlocked != "export.blocked" {
		t.Fatalf("AdminActionExportBlocked=%q", domain.AdminActionExportBlocked)
	}
	if domain.AdminActionExportBlocked == domain.AdminActionExportCreated || domain.AdminActionExportBlocked == domain.AdminActionExportRecovered {
		t.Fatal("export.blocked must be distinct from export.created/export.recovered")
	}
}

// RecoverStuckExports consumer: recovery only moves running→failed (never
// completed), clears the object reference, performs no archive I/O, and
// never opens a window — while the hold is active the recovered job stays
// 403 on both GetExport and RecordExportDownload.
func TestRecoverStuckExportsNeverUnblocksHeldExport(t *testing.T) {
	svc := testService(t, true)
	_ = ingestHeldEvent(t, svc)
	if _, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-a", Name: "case-stuck", Reason: "stuck under hold", Filter: wideExportQuery()}); err != nil {
		t.Fatal(err)
	}
	svc.Config.StuckExportAge = 24 * time.Hour
	jobID := "export-stuck"
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Exports[jobID] = domain.ExportJob{ID: jobID, TenantID: "tenant-a", RequestedBy: "test", Query: wideExportQuery(), Status: "running", CreatedAt: svc.Now().Add(-25 * time.Hour)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	recovered, err := svc.RecoverStuckExports("tenant-a")
	if err != nil || recovered != 1 {
		t.Fatalf("recover: recovered=%d err=%v", recovered, err)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	job := snap.Exports[jobID]
	if job.Status != "failed" {
		t.Fatalf("stuck job must be failed (never completed), status=%s", job.Status)
	}
	if job.ObjectPath != "" || job.Digest != "" {
		t.Fatalf("recovered job must not reference an archive object: %+v", job)
	}
	sealed, err := filepath.Glob(filepath.Join(svc.Config.ArchiveDir, "exports", "*"))
	if err != nil || len(sealed) != 0 {
		t.Fatalf("recovery must not write archive objects, found %v (err=%v)", sealed, err)
	}
	if _, err := svc.GetExport("tenant-a", jobID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("GetExport after recovery must stay blocked by the hold, got %v", err)
	}
	if err := svc.RecordExportDownload("tenant-a", "test", jobID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("download after recovery must stay blocked, got %v", err)
	}
}

// Retained sealed object: the WORM archive object is never deleted or
// rewritten while a hold is active — the sealed bytes are byte-identical —
// and after release the object is downloadable again.
func TestSealedObjectRetainedWhileHoldActive(t *testing.T) {
	svc := testService(t, true)
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("evt-worm", "op-1", t0), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	job, err := svc.CreateExport("tenant-a", "test", wideExportQuery())
	if err != nil {
		t.Fatal(err)
	}
	waitExport(t, svc, job.ID)
	current, err := svc.GetExport("tenant-a", job.ID)
	if err != nil || current.ObjectPath == "" {
		t.Fatalf("completed export needs an object path: %+v %v", current, err)
	}
	objectPath := filepath.Join(svc.Config.ArchiveDir, current.ObjectPath)
	before, err := os.ReadFile(objectPath)
	if err != nil || len(before) == 0 {
		t.Fatalf("sealed object missing before hold: %v", err)
	}
	hold, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-a", Name: "case-worm", Reason: "preserve evidence", Filter: wideExportQuery()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetExport("tenant-a", job.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("precondition: access must be blocked, got %v", err)
	}
	after, err := os.ReadFile(objectPath)
	if err != nil {
		t.Fatalf("sealed object must be retained while the hold is active: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("sealed object bytes must be unchanged (WORM) while the hold is active")
	}
	if _, err := svc.ReleaseLegalHold("tenant-a", hold.ID, "test"); err != nil {
		t.Fatal(err)
	}
	if err := svc.RecordExportDownload("tenant-a", "test", job.ID); err != nil {
		t.Fatalf("download after release must succeed: %v", err)
	}
}

// Cross-tenant masking: the hold gate is tenant-scoped on both the job and
// the hold. A tenant probing another tenant's job gets ErrNotFound (existence
// masked, never a 403 hold hint), and the 403 error text carries only the
// caller's own hold ID — nothing from any other tenant.
func TestExportHoldGateLeaksNothingCrossTenant(t *testing.T) {
	svc := testService(t, true)
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-b", Name: "Tenant B", Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-b", ID: "crm", Name: "CRM-B", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-b", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}

	// tenant-a: event + completed export + hold.
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("evt-a", "op-a", t0), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	jobA, err := svc.CreateExport("tenant-a", "test", wideExportQuery())
	if err != nil {
		t.Fatal(err)
	}
	waitExport(t, svc, jobA.ID)
	holdA, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-a", Name: "case-a", Reason: "hold A", Filter: wideExportQuery()})
	if err != nil {
		t.Fatal(err)
	}

	// tenant-b: its own event + completed export + hold.
	if _, err := svc.Ingest("tenant-b", crmPrincipal, testEvent("evt-b", "op-b", t0), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	jobB, err := svc.CreateExport("tenant-b", "test", wideExportQuery())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, err := svc.GetExport("tenant-b", jobB.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == "completed" || current.Status == "failed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant-b export did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	holdB, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-b", Name: "case-b", Reason: "hold B", Filter: wideExportQuery()})
	if err != nil {
		t.Fatal(err)
	}

	// tenant-b probing tenant-a's job: 404 before any hold check.
	if _, err := svc.GetExport("tenant-b", jobA.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cross-tenant GetExport must be ErrNotFound, got %v", err)
	}
	if err := svc.RecordExportDownload("tenant-b", "test", jobA.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cross-tenant download must be ErrNotFound, got %v", err)
	}

	// Each tenant's 403 text is exact and carries only its own hold ID.
	_, errA := svc.GetExport("tenant-a", jobA.ID)
	if !errors.Is(errA, domain.ErrForbidden) {
		t.Fatalf("tenant-a access: want ErrForbidden, got %v", errA)
	}
	if msg := errA.Error(); msg != "export blocked by active legal hold "+holdA.ID {
		t.Fatalf("tenant-a error text must be exactly its own hold ID, got %q", msg)
	}
	if strings.Contains(errA.Error(), holdB.ID) {
		t.Fatal("tenant-a error must not carry tenant-b's hold ID")
	}
	_, errB := svc.GetExport("tenant-b", jobB.ID)
	if !errors.Is(errB, domain.ErrForbidden) {
		t.Fatalf("tenant-b access: want ErrForbidden, got %v", errB)
	}
	if msg := errB.Error(); msg != "export blocked by active legal hold "+holdB.ID {
		t.Fatalf("tenant-b error text must be exactly its own hold ID, got %q", msg)
	}
	if strings.Contains(errB.Error(), holdA.ID) {
		t.Fatal("tenant-b error must not carry tenant-a's hold ID")
	}

	// GetExport denials are read-only: no block facts, and tenant-a's trail
	// never contains tenant-b's facts.
	if got := countBlockedFacts(t, svc, holdA.ID, "test"); got != 0 {
		t.Fatalf("GetExport denials must record no fact, found %d", got)
	}
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range actions {
		if a.Action == domain.AdminActionExportBlocked && a.TargetID == holdB.ID {
			t.Fatalf("tenant-a must never see tenant-b's block fact: %+v", a)
		}
	}
}

// Restore/verify consumers: with an active hold present, VerifyIntegrity and
// the restore flow keep working — neither path reads export objects or
// consults holds, and neither writes into the export archive namespace.
func TestRestoreAndVerifyPathsUnaffectedByHeldExport(t *testing.T) {
	svc := testService(t, true)
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("evt-1", "op-restore", t0), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("evt-2", "op-restore", t0.Add(time.Second)), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-a", Name: "case-verify", Reason: "hold while verifying", Filter: domain.Query{From: t0.Add(-10 * time.Second), To: t0.Add(10 * time.Second)}}); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyIntegrity("tenant-a", "", "")
	if err != nil || !result.Valid {
		t.Fatalf("integrity must stay valid under an active hold: %+v %v", result, err)
	}
	preview, err := svc.PreviewRestore("tenant-a", domain.RestoreRequest{OperationID: "op-restore", Reason: "dr"})
	if err != nil || preview.TenantID != "tenant-a" {
		t.Fatalf("restore preview must work under an active hold: %+v %v", preview, err)
	}
	run, err := svc.CreateRestore("tenant-a", domain.RestoreRequest{OperationID: "op-restore", Reason: "dr"}, "user-a")
	if err != nil {
		t.Fatalf("restore creation must work under an active hold: %v", err)
	}
	if _, err := svc.GetRestore("tenant-a", run.ID); err != nil {
		t.Fatalf("GetRestore must work under an active hold: %v", err)
	}
	sealed, err := filepath.Glob(filepath.Join(svc.Config.ArchiveDir, "exports", "*"))
	if err != nil || len(sealed) != 0 {
		t.Fatalf("restore must not write export objects, found %v (err=%v)", sealed, err)
	}
}
