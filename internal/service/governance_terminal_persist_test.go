package service

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

var terminalTestNow = time.Unix(1_700_000_000, 0).UTC()

// seedRunningJob writes a pending export job and claims it to "running" using
// the real CAS path, so terminal-persistence tests start from a durable
// running record (mirroring a worker that has claimed the job).
func seedRunningJob(t *testing.T, svc *Service, jobID string) domain.ExportJob {
	t.Helper()
	job := domain.ExportJob{
		ID:          jobID,
		TenantID:    "tenant-a",
		RequestedBy: "test",
		Query:       domain.Query{From: terminalTestNow.Add(-time.Hour), To: terminalTestNow.Add(time.Hour)},
		Status:      "pending",
		CreatedAt:   terminalTestNow,
	}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := svc.claimPendingExport(job.ID); err != nil || !claimed {
		t.Fatalf("claimPendingExport: claimed=%v err=%v", claimed, err)
	}
	return job
}

// T1 / AC-1: a single transient terminal-write failure converges. The bounded
// retry succeeds on the next attempt, the job reaches the requested terminal
// state, and the committed ExportJob carries the path/digest/count (no
// orphan, no late overwrite). Mirrors the run-leg block path in
// TestRunExportBlockFactUpdateFailure.
func TestFinishExportRetriesTransientSaveFailure(t *testing.T) {
	backend, svc := holdGateService(t)
	seedRunningJob(t, svc, "export-t1")

	// Fail only the next Save (the first terminal-write attempt); the retry
	// Save succeeds and commits the transition.
	backend.armSaveFailure(1)

	svc.finishExport("export-t1", "completed", "exports/export-t1.jsonl", "digest-t1", 7, "")

	var got domain.ExportJob
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		got = data.Exports["export-t1"]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got.Status != "completed" {
		t.Fatalf("status=%s, want completed (single transient failure must converge)", got.Status)
	}
	if got.FinishedAt == nil || !got.FinishedAt.Equal(terminalTestNow) {
		t.Fatalf("FinishedAt=%v, want %v", got.FinishedAt, terminalTestNow)
	}
	if got.ObjectPath != "exports/export-t1.jsonl" || got.Digest != "digest-t1" || got.EventCount != 7 {
		t.Fatalf("terminal fields not persisted: path=%q digest=%q count=%d", got.ObjectPath, got.Digest, got.EventCount)
	}
}

// T2 / AC-1: a persistent terminal-write failure leaves the durable "running"
// record in place so RecoverStuckExports converges it to "failed" with exactly
// one export.recovered fact; a second recovery pass is a no-op.
func TestFinishExportPersistentFailureThenRecover(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	seedRunningJob(t, svc, "export-t2")

	// Fail every Save from here on so all terminal-write attempts exhaust.
	backend.saveErr = errors.New("storage write unavailable")

	svc.finishExport("export-t2", "completed", "exports/export-t2.jsonl", "digest-t2", 3, "")

	var before domain.ExportJob
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		before = data.Exports["export-t2"]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if before.Status != "running" {
		t.Fatalf("status=%s, want running (exhausted retry must not overwrite — left for recovery)", before.Status)
	}

	// Recovery must converge the stuck job now that storage is healthy.
	backend.saveErr = nil
	svc.Config.Now = func() time.Time { return terminalTestNow.Add(48 * time.Hour) }
	recovered, err := svc.RecoverStuckExports("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("RecoverStuckExports recovered=%d, want 1", recovered)
	}

	var got domain.ExportJob
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		got = data.Exports["export-t2"]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got.Status != "failed" {
		t.Fatalf("status=%s, want failed", got.Status)
	}
	if got.ObjectPath != "" || got.Digest != "" || got.EventCount != 0 {
		t.Fatalf("recovered job must clear payload fields: path=%q digest=%q count=%d", got.ObjectPath, got.Digest, got.EventCount)
	}
	if n := countAdminActions(t, svc, func(a domain.AdminAction) bool {
		return a.Action == domain.AdminActionExportRecovered
	}); n != 1 {
		t.Fatalf("export.recovered facts=%d, want 1", n)
	}

	// A second recovery pass finds no running job: zero mutations, zero Saves.
	savesAfterFirst := backend.saves
	recovered2, err := svc.RecoverStuckExports("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if recovered2 != 0 {
		t.Fatalf("second RecoverStuckExports recovered=%d, want 0", recovered2)
	}
	if backend.saves != savesAfterFirst {
		t.Fatalf("second recovery Saves=%d, want %d (no-op pass must not rewrite)", backend.saves, savesAfterFirst)
	}
	if n := countAdminActions(t, svc, func(a domain.AdminAction) bool {
		return a.Action == domain.AdminActionExportRecovered
	}); n != 1 {
		t.Fatalf("export.recovered facts after second pass=%d, want 1 (no duplicate)", n)
	}
}

// T3 / AC-2: a late completion must not overwrite a worker-recovered "failed"
// job (and vice versa), so a terminal job never regresses to running.
func TestLateCompletionAfterRecoveryNoOverwrite(t *testing.T) {
	_, svc := holdGateService(t)
	seedRunningJob(t, svc, "export-t3a")

	svc.Config.Now = func() time.Time { return terminalTestNow.Add(48 * time.Hour) }
	if _, err := svc.RecoverStuckExports("tenant-a"); err != nil {
		t.Fatal(err)
	}

	// A late finishExport(completed) must no-op: job stays "failed".
	svc.finishExport("export-t3a", "completed", "exports/late.jsonl", "digest-late", 9, "")

	var got domain.ExportJob
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		got = data.Exports["export-t3a"]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got.Status != "failed" {
		t.Fatalf("status=%s, want failed (late completion must not overwrite recovered)", got.Status)
	}
	if got.ObjectPath != "" || got.Digest != "" || got.EventCount != 0 {
		t.Fatalf("recovered fields must remain cleared: path=%q digest=%q count=%d", got.ObjectPath, got.Digest, got.EventCount)
	}
	if n := countAdminActions(t, svc, func(a domain.AdminAction) bool {
		return a.Action == domain.AdminActionExportRecovered || a.Action == domain.AdminActionExportBlocked
	}); n != 1 {
		t.Fatalf("admin facts=%d, want 1 (only the recovery fact; no duplicate/blocked)", n)
	}
}

func TestLateFailureAfterCompletionNoOverwrite(t *testing.T) {
	_, svc := holdGateService(t)
	// Seed an already-completed job directly (terminal state).
	job := domain.ExportJob{
		ID:          "export-t3b",
		TenantID:    "tenant-a",
		RequestedBy: "test",
		Query:       domain.Query{From: terminalTestNow.Add(-time.Hour), To: terminalTestNow.Add(time.Hour)},
		Status:      "completed",
		CreatedAt:   terminalTestNow,
		FinishedAt:  &terminalTestNow,
		ObjectPath:  "exports/done.jsonl",
		Digest:      "digest-done",
		EventCount:  4,
	}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// A late finishExport(failed) must no-op: job stays "completed".
	svc.finishExport("export-t3b", "failed", "", "", 0, "late failure")

	var got domain.ExportJob
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		got = data.Exports["export-t3b"]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got.Status != "completed" {
		t.Fatalf("status=%s, want completed (late failure must not overwrite)", got.Status)
	}
	if got.ObjectPath != "exports/done.jsonl" || got.Digest != "digest-done" || got.EventCount != 4 {
		t.Fatalf("completed fields must be preserved: path=%q digest=%q count=%d", got.ObjectPath, got.Digest, got.EventCount)
	}
}

// T4 / FR-5/FR-6 is covered in governance_terminal_http_test.go (package
// service_test) because it exercises the HTTP redaction boundary hosted by the
// httpapi package, which cannot be imported from this internal test package.

// T5 / FR-7: a persistent terminal-write failure emits bounded, redacted
// observability — a per-attempt "failed" signal and a final "exhausted"
// signal carrying only job_id, state, attempt, category (never raw error
// text, payload, path, or credential).
func TestTerminalPersistenceFailureLogged(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	seedRunningJob(t, svc, "export-t5")

	var mu sync.Mutex
	var lines []string
	svc.Config.Logf = func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, fmt.Sprintf(format, args...))
	}

	rawErr := "disk I/O error at /mnt/audit/state.json: read-only"
	backend.saveErr = errors.New(rawErr)
	svc.finishExport("export-t5", "failed", "", "", 0, rawErr)

	var failed, exhausted int
	for _, l := range lines {
		if strings.Contains(l, "export terminal persistence failed") {
			failed++
			for _, want := range []string{"job_id=export-t5", "state=failed", "attempt=", "category=write"} {
				if !strings.Contains(l, want) {
					t.Fatalf("failure log missing %q: %q", want, l)
				}
			}
		}
		if strings.Contains(l, "export terminal persistence exhausted") {
			exhausted++
			if !strings.Contains(l, "job_id=export-t5") || !strings.Contains(l, "category=write") {
				t.Fatalf("exhausted log missing required fields: %q", l)
			}
		}
	}
	if failed == 0 {
		t.Fatalf("expected per-attempt failure logs, got none (lines=%v)", lines)
	}
	if exhausted != 1 {
		t.Fatalf("exhausted logs=%d, want 1", exhausted)
	}
	// No sensitive content may ever be logged.
	for _, leak := range []string{rawErr, "/mnt/audit", "read-only"} {
		for _, l := range lines {
			if strings.Contains(l, leak) {
				t.Fatalf("log leaks %q: %q", leak, l)
			}
		}
	}
}
