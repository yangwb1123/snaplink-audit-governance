package service

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// pinnedNow is the clock base shared by newServiceOn (testService sets the
// same value), so seeds can express relative ages deterministically.
var pinnedNow = time.Unix(1_700_000_000, 0).UTC()

// seedExportJob inserts one ExportJob directly into the scripted backend's
// snapshot (no Store window, so save-count assertions stay clean).
func seedExportJob(backend *scriptedConflictBackend, job domain.ExportJob) {
	backend.data.Exports[job.ID] = job
}

// stuckJob builds an ExportJob stuck in "running" since created; the query
// is a wide window so any event would match (matching is never exercised by
// the fail path, which reads no event data).
func stuckJob(id, tenantID string, created time.Time) domain.ExportJob {
	return domain.ExportJob{
		ID: id, TenantID: tenantID, RequestedBy: "compliance-1",
		Query:     domain.Query{From: pinnedNow.Add(-48 * time.Hour), To: pinnedNow.Add(24 * time.Hour), PageSize: 100},
		Status:    "running",
		CreatedAt: created,
	}
}

func TestRecoverStuckExportsFailsStuckRunningJob(t *testing.T) {
	// AC-1: a stuck "running" job older than the threshold is failed with a
	// clear error, atomically with exactly one export.recovered self-audit
	// fact, in exactly one Save.
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	svc.Config.StuckExportAge = 24 * time.Hour
	seedExportJob(backend, stuckJob("export-1", "tenant-a", pinnedNow.Add(-25*time.Hour)))
	savesBefore := backend.saves

	recovered, err := svc.RecoverStuckExports("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered=%d, want 1", recovered)
	}
	if backend.saves != savesBefore+1 {
		t.Fatalf("saves=%d, want %d (one atomic window)", backend.saves, savesBefore+1)
	}

	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	job := snap.Exports["export-1"]
	if job.Status != "failed" {
		t.Fatalf("status=%s, want failed", job.Status)
	}
	if !strings.Contains(job.Error, "stuck in running since") || !strings.Contains(job.Error, "re-request the export") {
		t.Fatalf("error must be clear and operator-readable, got: %q", job.Error)
	}
	if job.FinishedAt == nil || job.FinishedAt.After(pinnedNow) {
		t.Fatalf("finished_at=%v, want set and <= now", job.FinishedAt)
	}
	if job.ObjectPath != "" || job.Digest != "" || job.EventCount != 0 {
		t.Fatalf("failed job must carry empty path/digest and zero count, got path=%q digest=%q count=%d", job.ObjectPath, job.Digest, job.EventCount)
	}

	facts := 0
	for _, action := range snap.AdminActions {
		if action.Action == domain.AdminActionExportRecovered {
			facts++
			if action.TenantID != "tenant-a" || action.TargetID != "export-1" || action.Actor != "governance-worker" || action.TargetType != "export" {
				t.Fatalf("fact fields wrong: %+v", action)
			}
			if !strings.Contains(action.Detail, "stuck_running_since=") || !strings.Contains(action.Detail, "status=failed") {
				t.Fatalf("fact detail malformed: %q", action.Detail)
			}
		}
	}
	if facts != 1 {
		t.Fatalf("export.recovered facts=%d, want exactly 1", facts)
	}
}

func TestRecoverStuckExportsBoundary(t *testing.T) {
	// AC-1 boundary: age is measured from CreatedAt with a strict "older
	// than" cut — exactly 24h is untouched, fresh is untouched, and the same
	// job fails once the pinned clock advances past the threshold.
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	svc.Config.StuckExportAge = 24 * time.Hour
	seedExportJob(backend, stuckJob("at-threshold", "tenant-a", pinnedNow.Add(-24*time.Hour)))
	seedExportJob(backend, stuckJob("fresh", "tenant-a", pinnedNow))
	savesBefore := backend.saves

	recovered, err := svc.RecoverStuckExports("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 0 {
		t.Fatalf("recovered=%d, want 0 (both jobs at/below the threshold)", recovered)
	}
	if backend.saves != savesBefore {
		t.Fatalf("saves=%d, want %d (no mutation -> no Save)", backend.saves, savesBefore)
	}

	// Advance the clock past the threshold: only at-threshold qualifies.
	svc.Config.Now = func() time.Time { return pinnedNow.Add(2 * time.Hour) }
	recovered, err = svc.RecoverStuckExports("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered=%d, want 1 after clock advance", recovered)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Exports["at-threshold"].Status != "failed" || snap.Exports["fresh"].Status != "running" {
		t.Fatalf("unexpected states after advance: threshold=%s fresh=%s", snap.Exports["at-threshold"].Status, snap.Exports["fresh"].Status)
	}
}

func TestRecoverStuckExportsNeverTouchesTerminalOrFresh(t *testing.T) {
	// AC-2: a completed job (old enough to match the age filter) and a fresh
	// running job are never touched — byte-for-byte unchanged, no fact
	// appended, zero Saves across repeated calls (the mutated=false path).
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	svc.Config.StuckExportAge = 24 * time.Hour
	past := pinnedNow.Add(-48 * time.Hour)
	completed := stuckJob("export-done", "tenant-a", pinnedNow.Add(-25*time.Hour))
	completed.Status = "completed"
	completed.FinishedAt = &past
	completed.ObjectPath = "exports/export-done.jsonl"
	completed.Digest = "deadbeef"
	completed.EventCount = 7
	seedExportJob(backend, completed)
	seedExportJob(backend, stuckJob("export-fresh", "tenant-a", pinnedNow))
	savesBefore := backend.saves

	for i := 0; i < 2; i++ {
		recovered, err := svc.RecoverStuckExports("tenant-a")
		if err != nil {
			t.Fatal(err)
		}
		if recovered != 0 {
			t.Fatalf("attempt %d recovered=%d, want 0", i, recovered)
		}
	}
	if backend.saves != savesBefore {
		t.Fatalf("saves=%d, want %d (no terminal regression, no idle rewrite)", backend.saves, savesBefore)
	}

	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	done := snap.Exports["export-done"]
	if done.Status != "completed" || done.FinishedAt == nil || !done.FinishedAt.Equal(past) ||
		done.ObjectPath != "exports/export-done.jsonl" || done.Digest != "deadbeef" || done.EventCount != 7 || done.Error != "" {
		t.Fatalf("completed job mutated: %+v", done)
	}
	if fresh := snap.Exports["export-fresh"]; fresh.Status != "running" || fresh.FinishedAt != nil || fresh.Error != "" {
		t.Fatalf("fresh running job mutated: %+v", fresh)
	}
	for _, action := range snap.AdminActions {
		if action.Action == domain.AdminActionExportRecovered {
			t.Fatalf("no export.recovered fact may be appended, got %+v", action)
		}
	}
}

func TestRecoverStuckExportsConflictRetry(t *testing.T) {
	// AC-2 variant / REQ-6: one conflicted Save re-runs the closure on the
	// fresh snapshot; the job still ends failed with exactly one fact, the
	// count reflects only the committed attempt, and saves == 2 (conflicted
	// attempt + committed attempt).
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	svc.Config.StuckExportAge = 24 * time.Hour
	seedExportJob(backend, stuckJob("export-1", "tenant-a", pinnedNow.Add(-25*time.Hour)))
	backend.conflicts = 1
	savesBefore := backend.saves
	loadsBefore := backend.loads

	recovered, err := svc.RecoverStuckExports("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered=%d, want 1 from the committed attempt", recovered)
	}
	if backend.saves != savesBefore+2 {
		t.Fatalf("saves=%d, want %d (one conflicted + one committed)", backend.saves, savesBefore+2)
	}
	if backend.loads != loadsBefore+2 {
		t.Fatalf("loads=%d, want %d (closure re-ran on the fresh snapshot)", backend.loads, loadsBefore+2)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Exports["export-1"].Status != "failed" {
		t.Fatalf("status=%s, want failed after retry", snap.Exports["export-1"].Status)
	}
	facts := 0
	for _, action := range snap.AdminActions {
		if action.Action == domain.AdminActionExportRecovered {
			facts++
		}
	}
	if facts != 1 {
		t.Fatalf("export.recovered facts=%d, want exactly 1 (no double-count on retry)", facts)
	}
}

func TestRecoverStuckExportsTenantScoping(t *testing.T) {
	// AC-3: only the caller's tenant is recovered; a foreign tenant's stuck
	// job (and its events, which would match the export query window) are
	// untouched, no foreign fact is attributed, and the fail path issues
	// zero archive writes.
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	svc.Config.StuckExportAge = 24 * time.Hour
	backend.data.Tenants["tenant-b"] = domain.Tenant{ID: "tenant-b", Name: "Tenant B", Active: true}
	seedExportJob(backend, stuckJob("export-a", "tenant-a", pinnedNow.Add(-25*time.Hour)))
	seedExportJob(backend, stuckJob("export-b", "tenant-b", pinnedNow.Add(-25*time.Hour)))
	// Events that WOULD match tenant-a's export query window — the fail path
	// must be invariant to them (it reads no event data at all).
	for i := 1; i <= 3; i++ {
		key := store.EventKey("tenant-b", fmt.Sprintf("evt-%d", i))
		backend.data.Events[key] = domain.Event{
			EventID: fmt.Sprintf("evt-%d", i), TenantID: "tenant-b", StreamID: "s",
			EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1,
			Sequence: int64(i), OccurredAt: pinnedNow.Add(-24 * time.Hour),
		}
		backend.data.Receipts[key] = domain.EventReceipt{EventID: fmt.Sprintf("evt-%d", i), TenantID: "tenant-b", Status: domain.StatusLedgered}
	}
	archiveStub := &recordingArchive{}
	svc.Config.Archive = archiveStub

	recovered, err := svc.RecoverStuckExports("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered=%d, want 1 (only tenant-a's job)", recovered)
	}
	if len(archiveStub.puts) != 0 {
		t.Fatalf("puts=%v, want zero archive writes from the fail path", archiveStub.puts)
	}

	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if a := snap.Exports["export-a"]; a.Status != "failed" || a.TenantID != "tenant-a" {
		t.Fatalf("tenant-a job=%+v, want failed and attributed to tenant-a", a)
	}
	if b := snap.Exports["export-b"]; b.Status != "running" || b.TenantID != "tenant-b" {
		t.Fatalf("tenant-b job=%+v, want untouched running", b)
	}
	for _, action := range snap.AdminActions {
		if action.Action != domain.AdminActionExportRecovered {
			continue
		}
		if action.TenantID != "tenant-a" || action.TargetID != "export-a" {
			t.Fatalf("fact attributed wrongly: %+v", action)
		}
	}
	// tenant-b's job must still be running with no fact for tenant-b.
	for _, action := range snap.AdminActions {
		if action.Action == domain.AdminActionExportRecovered && action.TenantID == "tenant-b" {
			t.Fatalf("foreign-tenant fact appended: %+v", action)
		}
	}
}
