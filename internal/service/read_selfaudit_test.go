package service

import (
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

// TestReadSelfAuditOnQueryAndGet pins F-06: every event read through the
// service layer appends an audit.event.read fact with the caller identity,
// so no transport can serve an un-audited read.
func TestReadSelfAuditOnQueryAndGet(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("selfaudit-evt", "op-sa", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	// Empty actor (system-internal read) records nothing.
	query := domain.Query{From: at.Add(-time.Second), To: at.Add(time.Second), PageSize: 100}
	if _, err := svc.QueryEvents("tenant-a", "", query); err != nil {
		t.Fatal(err)
	}
	// Actor-bearing reads record one audit.event.read each.
	if _, err := svc.QueryEvents("tenant-a", "auditor-1", query); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetEvent("tenant-a", "auditor-1", "selfaudit-evt"); err != nil {
		t.Fatal(err)
	}
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	var reads []domain.AdminAction
	for _, action := range actions {
		if action.Action == domain.AdminActionEventRead {
			reads = append(reads, action)
		}
	}
	if len(reads) != 2 {
		t.Fatalf("audit.event.read rows=%d, want 2 (query + get); actions=%v", len(reads), actions)
	}
	for _, read := range reads {
		if read.Actor != "auditor-1" {
			t.Fatalf("read actor=%q, want auditor-1", read.Actor)
		}
	}
	gotTargets := map[string]bool{}
	for _, read := range reads {
		gotTargets[read.TargetType+":"+read.TargetID] = true
	}
	if !gotTargets["query:"] || !gotTargets["event:selfaudit-evt"] {
		t.Fatalf("read targets=%v, want query and event:selfaudit-evt", gotTargets)
	}
}

// TestReadSelfAuditOnExportPins F-06 export legs: creating an export runs the
// query (audit.event.read for the exporter) and downloading records
// audit.event.export for the downloader.
func TestReadSelfAuditOnExport(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("export-sa-evt", "op-exp", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	query := domain.Query{From: at.Add(-time.Second), To: at.Add(time.Second), PageSize: 100}
	job, err := svc.CreateExport("tenant-a", "compliance-1", query)
	if err != nil {
		t.Fatal(err)
	}
	// runExport 是异步 goroutine：等待任务收敛，避免其写入在临时目录清理时
	// 仍在进行（race 模式下更容易触发）。
	// runExport 是异步 goroutine：等待收敛，避免其写入与 TempDir 清理竞争。
	waitExport(t, svc, job.ID)
	if err := svc.RecordExportDownload("tenant-a", "compliance-1", job.ID); err != nil {
		t.Fatal(err)
	}
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	var reads, exports int
	for _, action := range actions {
		switch action.Action {
		case domain.AdminActionEventRead:
			reads++
		case domain.AdminActionEventExport:
			exports++
			if action.TargetID != job.ID || action.Actor != "compliance-1" {
				t.Fatalf("export action=%+v, want target %s actor compliance-1", action, job.ID)
			}
		}
	}
	if reads != 1 {
		t.Fatalf("audit.event.read rows=%d, want 1 (export's internal query)", reads)
	}
	if exports != 1 {
		t.Fatalf("audit.event.export rows=%d, want 1", exports)
	}
	// Empty actor: download recording is skipped (system-internal path).
	if err := svc.RecordExportDownload("tenant-a", "", job.ID); err != nil {
		t.Fatal(err)
	}
}

// TestReadSelfAuditFailClosed documents the fail-closed rule: the read is
// only returned after the self-audit append succeeded. The append failure
// path itself is exercised at the store level (Update aborts on closure
// error); here we pin that a healthy store serves reads normally.
func TestReadSelfAuditFailClosed(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("failclosed-evt", "op-fc", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	query := domain.Query{From: at.Add(-time.Second), To: at.Add(time.Second), PageSize: 100}
	if _, err := svc.QueryEvents("tenant-a", "auditor-1", query); err != nil {
		t.Fatalf("healthy read must succeed: %v", err)
	}
}

// waitExport polls an export job until it converges (completed/failed), so
// tests that trigger the asynchronous runExport goroutine never race its
// writes against TempDir cleanup.
func waitExport(t *testing.T, svc *Service, jobID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, err := svc.GetExport("tenant-a", jobID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == "completed" || current.Status == "failed" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("export %s did not finish (status=%s)", jobID, current.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
