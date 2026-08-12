package service

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// holdGateBackend scripts failure injection and hold-release interleaving for
// the legal-hold export gate paths. Counters are armed after seeding (mutable
// fields), so the seeding writes themselves never fail: failLoadOn fails the
// Nth Load (1-based), failSaveOn fails the Nth Save (1-based), and
// releaseOn+releaseHold release the hold inside the Nth LoadForUpdate to
// simulate a release landing between a gate check (Store.Read) and the
// block-fact append (Store.Update).
//
// All counter state is guarded by mu: runExport is a background goroutine
// (CreateExport spawns it) and races the test goroutine's GetExport/waitExport
// and arming reads under -race otherwise.
type holdGateBackend struct {
	mu           sync.Mutex
	data         *store.Snapshot
	loads        int
	loadFors     int
	saves        int
	failLoadOn   int
	failSaveOn   int
	releaseOn    int
	releaseHold  string
	released     bool
	injectOnLoad int
	injectHold   domain.LegalHold
}

func (b *holdGateBackend) Load() (*store.Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.loads++
	if b.failLoadOn > 0 && b.loads == b.failLoadOn {
		return nil, errors.New("storage read unavailable")
	}
	if b.injectOnLoad > 0 && b.loads == b.injectOnLoad {
		if _, ok := b.data.LegalHolds[b.injectHold.ID]; !ok {
			b.data.LegalHolds[b.injectHold.ID] = b.injectHold
		}
	}
	return b.data, nil
}

func (b *holdGateBackend) LoadForUpdate() (*store.Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.loadFors++
	if b.releaseOn > 0 && b.loadFors == b.releaseOn && !b.released {
		b.released = true
		now := time.Unix(1_700_000_000, 0).UTC()
		hold := b.data.LegalHolds[b.releaseHold]
		hold.ReleasedAt = &now
		b.data.LegalHolds[b.releaseHold] = hold
	}
	return cloneTestSnapshot(b.data)
}

func (b *holdGateBackend) Save(data *store.Snapshot) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.saves++
	if b.failSaveOn > 0 && b.saves == b.failSaveOn {
		return errors.New("storage write unavailable")
	}
	b.data = data
	return nil
}

// armSaveFailure fails the save that is offset saves after the arming call.
func (b *holdGateBackend) armSaveFailure(offset int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failSaveOn = b.saves + offset
}

// armLoadFailure fails the load that is offset loads after the arming call.
func (b *holdGateBackend) armLoadFailure(offset int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failLoadOn = b.loads + offset
}

// armReleaseOnLoadFor releases holdID on the LoadForUpdate that is offset
// loadForUpdates after the arming call.
func (b *holdGateBackend) armReleaseOnLoadFor(offset int, holdID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.releaseOn = b.loadFors + offset
	b.releaseHold = holdID
}

// armInjectHoldOnLoad injects hold into the snapshot on the Load that is
// offset loads after the arming call — a concurrent CreateLegalHold landing
// between two Store.Read calls (e.g. the create gate and runExport's R3
// scan). The hold persists once injected, as a real commit would.
func (b *holdGateBackend) armInjectHoldOnLoad(offset int, hold domain.LegalHold) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.injectOnLoad = b.loads + offset
	b.injectHold = hold
}

// holdGateService builds a Service over a scripted holdGateBackend with the
// standard tenant-a/crm/audit.event domain.
func holdGateService(t *testing.T) (*holdGateBackend, *Service) {
	t.Helper()
	backend := &holdGateBackend{data: store.NewSnapshot()}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	return backend, svc
}

// ingestHeldEvent ingests one audit.event for tenant-a at the deterministic
// t0 and returns t0.
func ingestHeldEvent(t *testing.T, svc *Service) time.Time {
	t.Helper()
	t0 := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("evt-held", "op-held", t0), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	return t0
}

// activeWindowedHold creates an active legal hold over [t0-10s, t0+10s).
func activeWindowedHold(t *testing.T, svc *Service, t0 time.Time) domain.LegalHold {
	t.Helper()
	hold, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-a", Name: "case-1", Reason: "investigation pending", Filter: domain.Query{From: t0.Add(-10 * time.Second), To: t0.Add(10 * time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	return hold
}

// seedCompletedJobAndHold writes a completed export job and an active hold
// directly into the snapshot (white-box, bypassing the create-time gate).
func seedCompletedJobAndHold(t *testing.T, svc *Service, t0 time.Time) {
	t.Helper()
	job := domain.ExportJob{ID: "export-1", TenantID: "tenant-a", RequestedBy: "test", Query: domain.Query{From: t0.Add(-time.Hour), To: t0.Add(time.Hour)}, Status: "completed", CreatedAt: t0, FinishedAt: &t0, ObjectPath: "exports/export-1.jsonl", Digest: "digest", EventCount: 1}
	hold := domain.LegalHold{ID: "hold-1", TenantID: "tenant-a", Name: "case-1", Reason: "investigation pending", Filter: domain.Query{From: t0.Add(-10 * time.Second), To: t0.Add(10 * time.Second)}, CreatedBy: "test", CreatedAt: t0}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Exports[job.ID] = job
		data.LegalHolds[hold.ID] = hold
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// countAdminActions returns how many admin actions match the predicate
// (platform view: every tenant).
func countAdminActions(t *testing.T, svc *Service, match func(domain.AdminAction) bool) int {
	t.Helper()
	actions, err := svc.ListAdminActions("tenant-a", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, action := range actions {
		if match(action) {
			count++
		}
	}
	return count
}

// §4 FM-2 (create leg): block-fact Update failure in CreateExport returns the
// raw store error (a silent 403 would hide a lost audit fact), the job is
// still not created, and nothing is committed.
func TestCreateExportBlockFactUpdateFailure(t *testing.T) {
	backend, svc := holdGateService(t)
	t0 := ingestHeldEvent(t, svc)
	activeWindowedHold(t, svc, t0)
	// CreateExport's Saves: #1 query-read fact, #2 block fact. Fail #2.
	backend.armSaveFailure(2)
	_, err := svc.CreateExport("tenant-a", "test", domain.Query{From: t0.Add(-110 * time.Second), To: t0.Add(90 * time.Second)})
	if err == nil || errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("CreateExport error=%v, want a raw store error, not a 403", err)
	}
	if !strings.Contains(err.Error(), "storage write unavailable") {
		t.Fatalf("CreateExport error=%q, want the store error surfaced", err.Error())
	}
	if _, gerr := svc.GetExport("tenant-a", "anything"); !errors.Is(gerr, domain.ErrNotFound) {
		t.Fatalf("job created despite failed block fact: GetExport error=%v, want ErrNotFound", gerr)
	}
	if n := countAdminActions(t, svc, func(a domain.AdminAction) bool {
		return a.Action == domain.AdminActionExportBlocked || a.Action == domain.AdminActionExportCreated
	}); n != 0 {
		t.Fatalf("export.* facts=%d, want 0 (failed append commits nothing)", n)
	}
}

// §4 FM-2 (download leg): block-fact Update failure in RecordExportDownload
// aborts the download with the raw store error; no audit fact of either kind
// is committed.
func TestRecordExportDownloadBlockFactUpdateFailure(t *testing.T) {
	backend, svc := holdGateService(t)
	t0 := ingestHeldEvent(t, svc)
	seedCompletedJobAndHold(t, svc, t0)
	// The block-fact append is the only Save this call performs.
	backend.armSaveFailure(1)
	err := svc.RecordExportDownload("tenant-a", "test", "export-1")
	if err == nil || errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("RecordExportDownload error=%v, want a raw store error (download aborted)", err)
	}
	if !strings.Contains(err.Error(), "storage write unavailable") {
		t.Fatalf("RecordExportDownload error=%q, want the store error surfaced", err.Error())
	}
	if n := countAdminActions(t, svc, func(a domain.AdminAction) bool {
		return a.Action == domain.AdminActionExportBlocked || a.Action == domain.AdminActionEventExport
	}); n != 0 {
		t.Fatalf("download facts=%d, want 0 (failed append commits nothing)", n)
	}
}

// §4 FM-2 (run leg): block-fact Update failure inside runExport is ignored
// (mirrors finishExport) — no export.blocked fact is committed and the
// status transition is discarded with the failed atomic Save (the job stays
// non-terminal, exactly the stuck state RecoverStuckExports exists for), so
// no archive object is written; access stays fail-closed via the R4 gate
// until the store recovers.
func TestRunExportBlockFactUpdateFailure(t *testing.T) {
	backend, svc := holdGateService(t)
	t0 := ingestHeldEvent(t, svc)
	hold := activeWindowedHold(t, svc, t0)
	job := domain.ExportJob{ID: "export-race", TenantID: "tenant-a", RequestedBy: "test", Query: domain.Query{From: t0.Add(-110 * time.Second), To: t0.Add(90 * time.Second)}, Status: "pending", CreatedAt: svc.Now()}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// runExport Saves: #1 "running" stamp, #2 failExportBlocked. Fail #2.
	backend.armSaveFailure(2)
	svc.runExport(job.ID)
	var got domain.ExportJob
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		got = data.Exports[job.ID]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// The failed atomic Save discarded the status change AND the fact: the
	// job is not regressed to failed (it stays running — worker-recoverable)
	// and no export.blocked fact exists. R4 keeps access closed meanwhile.
	if got.Status != "running" {
		t.Fatalf("status=%s, want running (failed Save discards the block transition)", got.Status)
	}
	if n := countAdminActions(t, svc, func(a domain.AdminAction) bool {
		return a.Action == domain.AdminActionExportBlocked
	}); n != 0 {
		t.Fatalf("export.blocked facts=%d, want 0 (fact append failed and was ignored)", n)
	}
	if _, err := svc.GetExport("tenant-a", job.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("GetExport error=%v, want ErrForbidden (R4 gate still authoritative)", err)
	}
	_ = hold
}

// §4 FM-1 (create leg): a store read error during the create gate propagates
// fail-closed — no job, no export facts.
func TestCreateExportStoreReadErrorPropagates(t *testing.T) {
	backend, svc := holdGateService(t)
	t0 := ingestHeldEvent(t, svc)
	activeWindowedHold(t, svc, t0)
	// Relative to the reads already consumed by seeding (ingest performs
	// Store.Read calls): the next Load is QueryEvents' read, the one after
	// is the R2 gate read. Fail the gate read.
	backend.armLoadFailure(2)
	_, err := svc.CreateExport("tenant-a", "test", domain.Query{From: t0.Add(-110 * time.Second), To: t0.Add(90 * time.Second)})
	if err == nil || !strings.Contains(err.Error(), "storage read unavailable") {
		t.Fatalf("CreateExport error=%v, want the store read error propagated", err)
	}
	if _, gerr := svc.GetExport("tenant-a", "anything"); !errors.Is(gerr, domain.ErrNotFound) {
		t.Fatalf("job created despite gate read failure: GetExport error=%v, want ErrNotFound", gerr)
	}
	if n := countAdminActions(t, svc, func(a domain.AdminAction) bool {
		return a.Action == domain.AdminActionExportBlocked || a.Action == domain.AdminActionExportCreated
	}); n != 0 {
		t.Fatalf("export.* facts=%d, want 0", n)
	}
}

// §4 FM-1 (access leg): a store read error during GetExport propagates and no
// job data is returned.
func TestGetExportStoreReadErrorPropagates(t *testing.T) {
	backend, svc := holdGateService(t)
	t0 := time.Unix(1_700_000_010, 0).UTC()
	job := domain.ExportJob{ID: "export-1", TenantID: "tenant-a", RequestedBy: "test", Query: domain.Query{From: t0.Add(-time.Hour), To: t0.Add(time.Hour)}, Status: "completed", CreatedAt: t0, FinishedAt: &t0, ObjectPath: "exports/export-1.jsonl", Digest: "digest", EventCount: 1}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// No Store.Read happened during this setup: the next Load is GetExport's.
	backend.armLoadFailure(1)
	got, err := svc.GetExport("tenant-a", "export-1")
	if err == nil || !strings.Contains(err.Error(), "storage read unavailable") {
		t.Fatalf("GetExport error=%v, want the store read error propagated", err)
	}
	if got != (domain.ExportJob{}) {
		t.Fatalf("GetExport returned job %+v on read failure, want zero value (fail-closed)", got)
	}
}

// §4 FM-1 (download leg): a store read error during RecordExportDownload
// propagates and nothing is appended.
func TestRecordExportDownloadStoreReadErrorPropagates(t *testing.T) {
	backend, svc := holdGateService(t)
	t0 := ingestHeldEvent(t, svc)
	seedCompletedJobAndHold(t, svc, t0)
	// Relative to the reads already consumed by seeding: fail the download
	// gate's own read.
	backend.armLoadFailure(1)
	err := svc.RecordExportDownload("tenant-a", "test", "export-1")
	if err == nil || !strings.Contains(err.Error(), "storage read unavailable") {
		t.Fatalf("RecordExportDownload error=%v, want the store read error propagated", err)
	}
	if n := countAdminActions(t, svc, func(a domain.AdminAction) bool {
		return a.Action == domain.AdminActionExportBlocked || a.Action == domain.AdminActionEventExport
	}); n != 0 {
		t.Fatalf("facts=%d, want 0 (read failed before any append)", n)
	}
}

// §4 FM-10: a blank-filter active hold (both From/To zero) covers every event
// and therefore blocks every tenant export that selects at least one event;
// a zero-event export is never blocked; holds are tenant-scoped.
func TestBlankFilterHoldBlocksAllTenantExports(t *testing.T) {
	svc := testService(t, true)
	t0 := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("evt-1", "op-1", t0), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	query := domain.Query{From: t0.Add(-time.Hour), To: t0.Add(time.Hour)}
	job, err := svc.CreateExport("tenant-a", "compliance", query)
	if err != nil {
		t.Fatal(err)
	}
	waitExport(t, svc, job.ID)
	if _, err := svc.GetExport("tenant-a", job.ID); err != nil {
		t.Fatalf("GetExport before hold: %v", err)
	}
	// A blank hold in another tenant never touches tenant-a.
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-b", Name: "Tenant B", Active: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-b", Name: "blank-b", Reason: "tenant-b blanket", Filter: domain.Query{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateExport("tenant-a", "compliance", query); err != nil {
		t.Fatalf("tenant-a export blocked by tenant-b hold: %v", err)
	}
	// tenant-a's own blank hold blocks create and access paths.
	if _, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-a", Name: "blank-a", Reason: "blanket investigation", Filter: domain.Query{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateExport("tenant-a", "test", query); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("create under blank hold error=%v, want ErrForbidden", err)
	}
	if _, err := svc.GetExport("tenant-a", job.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("GetExport under blank hold error=%v, want ErrForbidden", err)
	}
	if err := svc.RecordExportDownload("tenant-a", "test", job.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("download under blank hold error=%v, want ErrForbidden", err)
	}
	// Zero-event export query is never blocked.
	empty := domain.Query{From: t0.Add(time.Hour + time.Second), To: t0.Add(time.Hour + 2*time.Second)}
	emptyJob, err := svc.CreateExport("tenant-a", "test", empty)
	if err != nil {
		t.Fatalf("zero-event export under blank hold: %v", err)
	}
	waitExport(t, svc, emptyJob.ID)
	if _, err := svc.GetExport("tenant-a", emptyJob.ID); err != nil {
		t.Fatalf("GetExport zero-event under blank hold: %v", err)
	}
}

// §4 FM-5 (create leg): hold released between the create gate check and the
// block-fact append — the fact still records the block (append-only trail),
// no job is created, and the live snapshot governs subsequent access.
func TestHoldReleasedBetweenCreateGateAndFactAppend(t *testing.T) {
	backend, svc := holdGateService(t)
	t0 := ingestHeldEvent(t, svc)
	hold := activeWindowedHold(t, svc, t0)
	query := domain.Query{From: t0.Add(-110 * time.Second), To: t0.Add(90 * time.Second)}
	// Release the hold inside the block-fact append's LoadForUpdate
	// (LoadForUpdate #1 of CreateExport is the query-read fact append).
	backend.armReleaseOnLoadFor(2, hold.ID)
	job, err := svc.CreateExport("tenant-a", "test", query)
	if !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("CreateExport error=%v, want ErrForbidden (denial follows the gate snapshot)", err)
	}
	if job != (domain.ExportJob{}) {
		t.Fatalf("blocked CreateExport returned job %+v, want zero value", job)
	}
	if _, err := svc.GetExport("tenant-a", "anything"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("GetExport error=%v, want ErrNotFound (job never created)", err)
	}
	blocked := 0
	var fact *domain.AdminAction
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := range actions {
		if actions[i].Action == domain.AdminActionExportBlocked {
			blocked++
			fact = &actions[i]
		}
	}
	if blocked != 1 || fact == nil || fact.TargetID != hold.ID || fact.Actor != "test" {
		t.Fatalf("export.blocked facts=%d fact=%+v, want 1 naming hold %s actor test", blocked, fact, hold.ID)
	}
	var current domain.LegalHold
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		current = data.LegalHolds[hold.ID]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if current.ReleasedAt == nil {
		t.Fatal("hold not released in the live snapshot")
	}
	fresh, err := svc.CreateExport("tenant-a", "test", query)
	if err != nil {
		t.Fatalf("fresh create after release: %v", err)
	}
	if fresh.Status != "pending" {
		t.Fatalf("fresh job status=%s, want pending", fresh.Status)
	}
	waitExport(t, svc, fresh.ID)
}

// §4 FM-8 / R1 determinism: when multiple active holds cover the same event,
// the reported blocking hold is the one with the lowest ID, independent of
// creation order (hold IDs sort lexically), so the error text and the admin
// fact are reproducible across snapshot map orderings (R7).
func TestMultipleHoldsReportLowestID(t *testing.T) {
	svc := testService(t, true)
	_ = ingestHeldEvent(t, svc)
	query := wideExportQuery()
	job, err := svc.CreateExport("tenant-a", "test", query)
	if err != nil {
		t.Fatal(err)
	}
	waitExport(t, svc, job.ID)
	// Create the higher-ID hold first: the reported blocker must still be the
	// lexically-lowest ID (hold-aaa < hold-zzz), never creation order.
	if _, err := svc.CreateLegalHold(domain.LegalHold{ID: "hold-zzz", TenantID: "tenant-a", Name: "case-z", Reason: "higher id", Filter: query}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateLegalHold(domain.LegalHold{ID: "hold-aaa", TenantID: "tenant-a", Name: "case-a", Reason: "lowest id", Filter: query}); err != nil {
		t.Fatal(err)
	}
	// Access gate (read-only denial): error names the lowest-ID hold.
	if _, err := svc.GetExport("tenant-a", job.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("GetExport error=%v, want ErrForbidden", err)
	} else if msg := err.Error(); msg != "export blocked by active legal hold hold-aaa" {
		t.Fatalf("GetExport error=%q, want the lowest-ID hold reported", msg)
	}
	// Download denial: fact names the same lowest-ID hold.
	if err := svc.RecordExportDownload("tenant-a", "test", job.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("RecordExportDownload error=%v, want ErrForbidden", err)
	}
	// Create gate: same report.
	if _, err := svc.CreateExport("tenant-a", "test", query); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("CreateExport error=%v, want ErrForbidden", err)
	} else if msg := err.Error(); msg != "export blocked by active legal hold hold-aaa" {
		t.Fatalf("CreateExport error=%q, want the lowest-ID hold reported", msg)
	}
	// GetExport denies are read-only, so exactly one download fact plus one
	// create fact exist, both naming hold-aaa.
	facts, wrongHold := 0, 0
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range actions {
		if a.Action == domain.AdminActionExportBlocked {
			facts++
			if a.TargetID != "hold-aaa" {
				wrongHold++
			}
		}
	}
	if facts != 2 {
		t.Fatalf("export.blocked facts=%d, want 2 (download + create denials)", facts)
	}
	if wrongHold != 0 {
		t.Fatalf("%d facts name a hold other than the lowest-ID hold-aaa", wrongHold)
	}
}

// §4 FM-1 (run leg): a store read error during runExport's selection pass
// (the R3 gate read) fails the job with the store error surfaced in
// job.Error, writes no archive object, and records no export.blocked fact
// (the failure is a store error, not a hold denial).
func TestRunExportSelectionReadErrorFailsClosed(t *testing.T) {
	backend, svc := holdGateService(t)
	svc.Config.ArchiveDir = t.TempDir()
	svc.Config.Archive = &archive.FileStore{Dir: svc.Config.ArchiveDir}
	job := domain.ExportJob{ID: "export-readerr", TenantID: "tenant-a", RequestedBy: "test", Query: domain.Query{From: t0.Add(-time.Hour), To: t0.Add(time.Hour)}, Status: "pending", CreatedAt: svc.Now()}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// runExport Loads: #1 job lookup, #2 selection read. Fail #2 (the R3
	// gate read), which happens after the "running" stamp (a LoadForUpdate).
	backend.armLoadFailure(2)
	svc.runExport(job.ID)
	var got domain.ExportJob
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		got = data.Exports[job.ID]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got.Status != "failed" {
		t.Fatalf("status=%s, want failed (read error fails the job)", got.Status)
	}
	if got.Error != "storage read unavailable" {
		t.Fatalf("job.Error=%q, want the store read error surfaced", got.Error)
	}
	if got.ObjectPath != "" || got.Digest != "" || got.EventCount != 0 {
		t.Fatalf("failed job must carry empty path/digest and zero count: %+v", got)
	}
	sealed, err := filepath.Glob(filepath.Join(svc.Config.ArchiveDir, "exports", "*"))
	if err != nil || len(sealed) != 0 {
		t.Fatalf("read-error path must not write archive objects, found %v (err=%v)", sealed, err)
	}
	if n := countAdminActions(t, svc, func(a domain.AdminAction) bool {
		return a.Action == domain.AdminActionExportBlocked || a.Action == domain.AdminActionExportRecovered
	}); n != 0 {
		t.Fatalf("export.blocked/recovered facts=%d, want 0 (store error is not a hold denial)", n)
	}
}

// §4 FM-4 (R3 scan leg): a hold created AFTER the job was persisted but
// BEFORE runExport's selection scan — the exact create→run race — is caught
// by the R3 re-check: the hold is injected into the snapshot on the selection
// read itself, the seal is skipped, and the job fails naming that hold.
func TestRunExportBlocksHoldCreatedAfterJobPersisted(t *testing.T) {
	backend, svc := holdGateService(t)
	svc.Config.ArchiveDir = t.TempDir()
	svc.Config.Archive = &archive.FileStore{Dir: svc.Config.ArchiveDir}
	t0 := ingestHeldEvent(t, svc)
	job := domain.ExportJob{ID: "export-late-hold", TenantID: "tenant-a", RequestedBy: "test", Query: domain.Query{From: t0.Add(-time.Hour), To: t0.Add(time.Hour)}, Status: "pending", CreatedAt: svc.Now()}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	hold := domain.LegalHold{ID: "hold-late", TenantID: "tenant-a", Name: "case-late", Reason: "late hold", Filter: job.Query}
	// runExport Loads: #1 job lookup, #2 R3 selection read. The hold appears
	// on Load #2 — between the running stamp and the selection scan.
	backend.armInjectHoldOnLoad(2, hold)
	svc.runExport(job.ID)
	var got domain.ExportJob
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		got = data.Exports[job.ID]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got.Status != "failed" {
		t.Fatalf("status=%s, want failed (R3 re-check blocks the seal)", got.Status)
	}
	if got.Error != "export blocked by active legal hold hold-late" {
		t.Fatalf("job.Error=%q, want the late hold named", got.Error)
	}
	if got.ObjectPath != "" || got.Digest != "" || got.EventCount != 0 {
		t.Fatalf("blocked job must carry empty path/digest and zero count: %+v", got)
	}
	sealed, err := filepath.Glob(filepath.Join(svc.Config.ArchiveDir, "exports", "*"))
	if err != nil || len(sealed) != 0 {
		t.Fatalf("late-hold block must not write archive objects, found %v (err=%v)", sealed, err)
	}
	if got := countBlockedFacts(t, svc, hold.ID, "governance-worker"); got != 1 {
		t.Fatalf("want one governance-worker export.blocked fact, found %d", got)
	}
	// The injected hold is a real committed hold now: R4 stays authoritative.
	if _, err := svc.GetExport("tenant-a", job.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("GetExport error=%v, want ErrForbidden after the late hold commits", err)
	}
}

// §4 FM (job deleted while runExport runs): runExport is fire-and-forget; if
// the job has vanished by the time the goroutine starts, it returns silently
// (existing behavior) — no zombie job is resurrected, no admin fact and no
// archive object is written.
func TestRunExportJobDeletedIsSilent(t *testing.T) {
	backend, svc := holdGateService(t)
	svc.Config.ArchiveDir = t.TempDir()
	svc.Config.Archive = &archive.FileStore{Dir: svc.Config.ArchiveDir}
	job := domain.ExportJob{ID: "export-gone", TenantID: "tenant-a", RequestedBy: "test", Query: domain.Query{From: t0.Add(-time.Hour), To: t0.Add(time.Hour)}, Status: "pending", CreatedAt: svc.Now()}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		delete(data.Exports, job.ID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	svc.runExport(job.ID)
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Exports[job.ID]; ok {
		t.Fatalf("deleted job must not be resurrected: %+v", snap.Exports[job.ID])
	}
	if n := countAdminActions(t, svc, func(a domain.AdminAction) bool {
		return a.Action == domain.AdminActionExportCreated || a.Action == domain.AdminActionExportBlocked || a.Action == domain.AdminActionExportRecovered
	}); n != 0 {
		t.Fatalf("export.* facts=%d, want 0 (silent return appends nothing)", n)
	}
	sealed, err := filepath.Glob(filepath.Join(svc.Config.ArchiveDir, "exports", "*"))
	if err != nil || len(sealed) != 0 {
		t.Fatalf("silent return must not write archive objects, found %v (err=%v)", sealed, err)
	}
	_ = backend
}

// §4 FM-5 (download leg): hold released between the download gate check and
// the block-fact append — the denial is still returned, the fact names the
// hold, and a retry after the release succeeds.
func TestHoldReleasedBetweenDownloadGateAndFactAppend(t *testing.T) {
	backend, svc := holdGateService(t)
	t0 := ingestHeldEvent(t, svc)
	seedCompletedJobAndHold(t, svc, t0)
	// The block-fact append is the first LoadForUpdate of this call.
	backend.armReleaseOnLoadFor(1, "hold-1")
	if err := svc.RecordExportDownload("tenant-a", "test", "export-1"); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("RecordExportDownload error=%v, want ErrForbidden", err)
	}
	blocked, exported := 0, 0
	var fact *domain.AdminAction
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := range actions {
		switch actions[i].Action {
		case domain.AdminActionExportBlocked:
			blocked++
			fact = &actions[i]
		case domain.AdminActionEventExport:
			exported++
		}
	}
	if blocked != 1 || fact == nil || fact.TargetID != "hold-1" {
		t.Fatalf("export.blocked facts=%d fact=%+v, want 1 naming hold-1", blocked, fact)
	}
	if exported != 0 {
		t.Fatalf("audit.event.export facts=%d, want 0 (the block fact replaces it)", exported)
	}
	// Live snapshot: the release committed, so a retry now succeeds.
	if err := svc.RecordExportDownload("tenant-a", "test", "export-1"); err != nil {
		t.Fatalf("retry download after release: %v", err)
	}
	exported = 0
	actions, err = svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range actions {
		if action.Action == domain.AdminActionEventExport {
			exported++
		}
	}
	if exported != 1 {
		t.Fatalf("audit.event.export facts after retry=%d, want 1", exported)
	}
}
