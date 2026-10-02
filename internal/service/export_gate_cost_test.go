package service

// Cost and exactness regression for the legal-hold export gate.
//
// holdBlockingExport proves its candidate set (the tenant's active holds that
// can still match the export query) from the in-memory control-plane hold map
// before it materializes any event, so a tenant with no active hold — or only
// holds whose time window provably cannot intersect the query — is answered
// without cloning one event or reading one WORM object.
//
// The cost oracle is archive.Get: eventsFromSnapshot performs Archive.Get
// BEFORE applying the selection predicate, so
//
//	gets == 0  <=>  eventsFromSnapshot never ran on the gate path
//	          <=>  the gate cloned no event and issued no WORM read
//
// It is not a claim about the whole request: Store.Read itself still clones the
// snapshot's hot events before the closure runs, and that cost is unchanged.
// If a future refactor moves the predicate ahead of archivedEventContext the
// counter stops being a sound proxy and these tests must be re-derived.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// gateArchive is an archive.Store wrapper that counts Put/Get calls. It is not
// an *archive.FileStore or *archive.S3Store, so archive.Configured reports it
// as configured through the default branch — the counter therefore observes
// real gate reads instead of short-circuiting on an unconfigured archive.
type gateArchive struct {
	inner *archive.FileStore
	mu    sync.Mutex
	gets  int
	puts  int
}

func (a *gateArchive) Put(ctx context.Context, key string, data []byte) error {
	a.mu.Lock()
	a.puts++
	a.mu.Unlock()
	return a.inner.Put(ctx, key, data)
}

func (a *gateArchive) Get(ctx context.Context, key string) ([]byte, error) {
	a.mu.Lock()
	a.gets++
	a.mu.Unlock()
	return a.inner.Get(ctx, key)
}

func (a *gateArchive) Ready(ctx context.Context) error { return a.inner.Ready(ctx) }

func (a *gateArchive) counts() (gets, puts int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.gets, a.puts
}

func (a *gateArchive) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.gets, a.puts = 0, 0
}

// failingEventArchive answers every archived event object as unreadable while
// delegating the exports/ namespace, so the run leg's own sealed write path
// would still work if it were reached.
type failingEventArchive struct {
	inner archive.Store
}

func (a *failingEventArchive) Put(ctx context.Context, key string, data []byte) error {
	return a.inner.Put(ctx, key, data)
}

func (a *failingEventArchive) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "events/") {
		return nil, errors.New("worm event object unavailable")
	}
	return a.inner.Get(ctx, key)
}

func (a *failingEventArchive) Ready(ctx context.Context) error { return a.inner.Ready(ctx) }

const (
	gateHotEvents    = 5
	gateEvictedCount = 6
	gateEventCount   = gateHotEvents + gateEvictedCount
	gateJobID        = "export-gate-cost"
	gateRunJobID     = "export-gate-run"
)

// gateCostFixture is a tenant whose event set is split across both tiers, so a
// materialization would be expensive: five hot events (cloned by the first
// loop) and six archived-and-evicted events (one verified WORM read each).
type gateCostFixture struct {
	t       *testing.T
	svc     *Service
	archive *gateArchive
	// query starts exactly at t0, the instant of the first event, so boundary
	// holds can be expressed at t0 and t0+1ns.
	query domain.Query
}

// gateCostQuery is the export window used by every fixture. Its lower bound is
// an occupied instant, which is what makes a one-nanosecond overlapping hold
// observable end to end.
func gateCostQuery() domain.Query {
	return domain.Query{From: t0, To: t0.Add(time.Hour)}
}

func newGateCostFixture(t *testing.T) *gateCostFixture {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc := newServiceOn(t, st)
	seedTestDomain(t, svc)
	// Hot events first, with no archive configured, so their receipts stay
	// StatusIndexed and the immutable body stays in the hot tier.
	for i := 0; i < gateHotEvents; i++ {
		gateIngest(t, svc, fmt.Sprintf("gate-hot-%d", i), t0.Add(time.Duration(i)*time.Second))
	}
	counting := &gateArchive{inner: &archive.FileStore{Dir: t.TempDir()}}
	svc.Config.Archive = counting
	// With the archive configured, ingest commits each event to WORM and
	// evicts the hot body in the same transaction: "archived receipt, no hot
	// payload" is the steady state, not an edge case.
	for i := 0; i < gateEvictedCount; i++ {
		gateIngest(t, svc, fmt.Sprintf("gate-cold-%d", i), t0.Add(time.Duration(gateHotEvents+i)*time.Second))
	}
	f := &gateCostFixture{t: t, svc: svc, archive: counting, query: gateCostQuery()}
	f.seedCompletedJob(gateJobID)
	f.requireSplitTiers()
	return f
}

func gateIngest(t *testing.T, svc *Service, eventID string, at time.Time) {
	t.Helper()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent(eventID, "op-"+eventID, at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
}

// update applies a snapshot mutation and fails the test on error.
func (f *gateCostFixture) update(mutate func(data *store.Snapshot) error) {
	f.t.Helper()
	if err := f.svc.Store.Update(testCtx, mutate); err != nil {
		f.t.Fatal(err)
	}
}

func (f *gateCostFixture) seedCompletedJob(jobID string) {
	f.update(func(data *store.Snapshot) error {
		data.Exports[jobID] = domain.ExportJob{ID: jobID, TenantID: "tenant-a", RequestedBy: "test", Query: f.query, Status: "completed", CreatedAt: t0, ObjectPath: "exports/" + jobID + ".jsonl", Digest: "digest", EventCount: gateEventCount}
		return nil
	})
}

func (f *gateCostFixture) seedPendingJob(jobID string) {
	f.update(func(data *store.Snapshot) error {
		data.Exports[jobID] = domain.ExportJob{ID: jobID, TenantID: "tenant-a", RequestedBy: "test", Query: f.query, Status: "pending", CreatedAt: t0}
		return nil
	})
}

// seedHolds writes holds straight into the snapshot so a fixture can express
// states the public API rejects or cannot reach (one-sided filters, foreign
// tenants, explicit release stamps) without widening production validation.
func (f *gateCostFixture) seedHolds(holds ...domain.LegalHold) {
	for index := range holds {
		if holds[index].TenantID == "" {
			holds[index].TenantID = "tenant-a"
		}
		if holds[index].CreatedAt.IsZero() {
			holds[index].CreatedAt = t0
		}
	}
	f.update(func(data *store.Snapshot) error {
		for _, hold := range holds {
			data.LegalHolds[hold.ID] = hold
		}
		return nil
	})
}

func (f *gateCostFixture) job(jobID string) domain.ExportJob {
	f.t.Helper()
	var value domain.ExportJob
	if err := f.svc.Store.Read(testCtx, func(data *store.Snapshot) error {
		value = data.Exports[jobID]
		return nil
	}); err != nil {
		f.t.Fatal(err)
	}
	return value
}

// requireSplitTiers proves the fixture is what the cost claims assume: hot
// events still carry a body, evicted events carry only an archived receipt, and
// every receipt that lost its hot body has a readable WORM object. A fixture
// that silently degraded to a single tier would make every count below vacuous.
func (f *gateCostFixture) requireSplitTiers() {
	f.t.Helper()
	if err := f.svc.Store.Read(testCtx, func(data *store.Snapshot) error {
		hot, evicted := 0, 0
		for _, event := range data.Events {
			if event.TenantID == "tenant-a" {
				hot++
			}
		}
		for key, receipt := range data.Receipts {
			if receipt.TenantID != "tenant-a" || receipt.Status != domain.StatusArchived {
				continue
			}
			if _, hot := data.Events[key]; hot {
				return errors.New("an archived receipt must have no hot payload")
			}
			evicted++
		}
		if hot != gateHotEvents || evicted != gateEvictedCount {
			return fmt.Errorf("fixture tiers: hot=%d evicted=%d, want %d/%d", hot, evicted, gateHotEvents, gateEvictedCount)
		}
		return nil
	}); err != nil {
		f.t.Fatal(err)
	}
	// Every evicted object must be readable, so a zero count below can only
	// mean "not read", never "read and failed".
	f.archive.reset()
	events, err := f.svc.QueryEvents(testCtx, "tenant-a", "test", f.query)
	if err != nil {
		f.t.Fatalf("fixture must materialize cleanly: %v", err)
	}
	if len(events.Items) != gateEventCount {
		f.t.Fatalf("query selected %d events, want %d", len(events.Items), gateEventCount)
	}
	if gets, _ := f.archive.counts(); gets != gateEvictedCount {
		f.t.Fatalf("control materialization performed %d archive reads, want %d", gets, gateEvictedCount)
	}
	f.archive.reset()
}

// runGate drives the gate white-box so the counter covers the gate read alone.
func (f *gateCostFixture) runGate() (domain.LegalHold, bool, error) {
	var (
		hold    domain.LegalHold
		blocked bool
		gateErr error
	)
	err := f.svc.Store.Read(testCtx, func(data *store.Snapshot) error {
		hold, blocked, gateErr = f.svc.holdBlockingExport(data, "tenant-a", f.query)
		return gateErr
	})
	if err != nil {
		return domain.LegalHold{}, false, err
	}
	return hold, blocked, gateErr
}

func (f *gateCostFixture) assertNoArchiveWork(surface string) {
	f.t.Helper()
	if gets, puts := f.archive.counts(); gets != 0 || puts != 0 {
		f.t.Fatalf("%s performed gets=%d puts=%d, want 0/0", surface, gets, puts)
	}
}

func (f *gateCostFixture) assertArchiveRead(surface string) {
	f.t.Helper()
	if gets, _ := f.archive.counts(); gets == 0 {
		f.t.Fatalf("%s: the hold must be decided from the materialized events, not from the window test", surface)
	}
}

func (f *gateCostFixture) exportFactCount(action string) int {
	f.t.Helper()
	actions, err := f.svc.ListAdminActions(testCtx, "tenant-a", false, 100)
	if err != nil {
		f.t.Fatal(err)
	}
	count := 0
	for _, candidate := range actions {
		if candidate.Action == action {
			count++
		}
	}
	return count
}

func (f *gateCostFixture) sealedObjectCount() int {
	f.t.Helper()
	objects, err := filepath.Glob(filepath.Join(f.archive.inner.Dir, "exports", "*"))
	if err != nil {
		f.t.Fatal(err)
	}
	return len(objects)
}

// AC-1: with zero active holds the gate, GetExport and RecordExportDownload
// are answered from the hold map alone. CreateExport is deliberately excluded
// from the zero-read claim — it runs the audited read (QueryEvents) first, so
// it legitimately materializes the very same events, and that path is
// unchanged.
func TestHoldGateZeroActiveHoldsReadsNoEvents(t *testing.T) {
	f := newGateCostFixture(t)

	hold, blocked, err := f.runGate()
	if err != nil || blocked || hold != (domain.LegalHold{}) {
		t.Fatalf("gate: hold=%+v blocked=%v err=%v, want (zero,false,nil)", hold, blocked, err)
	}
	f.assertNoArchiveWork("white-box holdBlockingExport")

	job, err := f.svc.GetExport(testCtx, "tenant-a", gateJobID)
	if err != nil {
		t.Fatalf("GetExport: %v", err)
	}
	if job.ID != gateJobID || job.Query != f.query {
		t.Fatalf("GetExport returned %+v, want the stored job unchanged", job)
	}
	f.assertNoArchiveWork("GetExport")
	if n := f.exportFactCount(domain.AdminActionExportBlocked); n != 0 {
		t.Fatalf("export.blocked facts=%d, want 0 on a not-blocked gate", n)
	}
	if n := f.exportFactCount(domain.AdminActionEventExport); n != 0 {
		t.Fatalf("audit.event.export facts=%d, want 0 (a status read records none)", n)
	}

	if err := f.svc.RecordExportDownload(testCtx, "tenant-a", "test", gateJobID); err != nil {
		t.Fatalf("RecordExportDownload: %v", err)
	}
	f.assertNoArchiveWork("RecordExportDownload")
	if n := f.exportFactCount(domain.AdminActionEventExport); n != 1 {
		t.Fatalf("audit.event.export facts=%d, want exactly 1 on the allowed download", n)
	}
	if n := f.exportFactCount(domain.AdminActionExportBlocked); n != 0 {
		t.Fatalf("export.blocked facts=%d, want 0 (no hold can block)", n)
	}
}

// AC-2a: a fully bound hold whose window cannot intersect the query is dropped
// before materialization. The two touching rows pin the half-open convention
// matches already uses (occurred_at == To is rejected), so a hold that starts
// exactly at the query's exclusive upper bound is disjoint, not a candidate.
func TestHoldGateDisjointWindowReadsNoEvents(t *testing.T) {
	cases := []struct {
		name   string
		filter domain.Query
	}{
		{"strictly-after", domain.Query{From: t0.Add(2 * time.Hour), To: t0.Add(3 * time.Hour)}},
		{"touching-upper-bound", domain.Query{From: t0.Add(time.Hour), To: t0.Add(2 * time.Hour)}},
		{"touching-lower-bound", domain.Query{From: t0.Add(-time.Hour), To: t0}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			f := newGateCostFixture(t)
			f.seedHolds(domain.LegalHold{ID: "hold-disjoint", Name: "case", Reason: "disjoint window", Filter: testCase.filter})
			hold, blocked, err := f.runGate()
			if err != nil || blocked || hold != (domain.LegalHold{}) {
				t.Fatalf("gate: hold=%+v blocked=%v err=%v, want (zero,false,nil)", hold, blocked, err)
			}
			f.assertNoArchiveWork("gate with a provably disjoint hold")
			if _, err := f.svc.GetExport(testCtx, "tenant-a", gateJobID); err != nil {
				t.Fatalf("GetExport: %v", err)
			}
			f.assertNoArchiveWork("GetExport with a provably disjoint hold")
			if n := f.exportFactCount(domain.AdminActionExportBlocked); n != 0 {
				t.Fatalf("export.blocked facts=%d, want 0", n)
			}
		})
	}
}

// AC-2b: a one-nanosecond overlap is a real candidate. The gate must still
// materialize, still block, and still report the exact same denial text.
func TestHoldGateOneNanosecondOverlapStillBlocks(t *testing.T) {
	f := newGateCostFixture(t)
	f.seedHolds(domain.LegalHold{ID: "hold-ns", Name: "case", Reason: "one nanosecond overlap", Filter: domain.Query{From: t0, To: t0.Add(time.Nanosecond)}})

	hold, blocked, err := f.runGate()
	if err != nil || !blocked {
		t.Fatalf("gate: hold=%+v blocked=%v err=%v, want a block", hold, blocked, err)
	}
	if hold.ID != "hold-ns" {
		t.Fatalf("reported hold=%q, want hold-ns", hold.ID)
	}
	f.assertArchiveRead("gate with a one-nanosecond overlap")

	const want = "export blocked by active legal hold hold-ns"
	if _, err := f.svc.GetExport(testCtx, "tenant-a", gateJobID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("GetExport error=%v, want ErrForbidden", err)
	} else if err.Error() != want {
		t.Fatalf("GetExport error text=%q, want %q", err.Error(), want)
	}
	if err := f.svc.RecordExportDownload(testCtx, "tenant-a", "test", gateJobID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("RecordExportDownload error=%v, want ErrForbidden", err)
	} else if err.Error() != want {
		t.Fatalf("download error text=%q, want %q", err.Error(), want)
	}
	if _, err := f.svc.CreateExport(testCtx, "tenant-a", "test", f.query); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("CreateExport error=%v, want ErrForbidden", err)
	}
	if n := f.exportFactCount(domain.AdminActionExportBlocked); n != 2 {
		t.Fatalf("export.blocked facts=%d, want 2 (the download denial and the create denial)", n)
	}
}

// AC-2b (mirror): the filter must not be over-eager in the other direction. A
// hold whose window overlaps the query by a single nanosecond at the exclusive
// upper bound is a real candidate — the gate must materialize and decide — even
// though no event happens to fall in that nanosecond, so nothing is blocked.
func TestHoldGateOneNanosecondOverlapStillMaterializes(t *testing.T) {
	f := newGateCostFixture(t)
	f.seedHolds(domain.LegalHold{ID: "hold-edge", Name: "case", Reason: "one nanosecond before the query upper bound", Filter: domain.Query{From: t0.Add(time.Hour - time.Nanosecond), To: t0.Add(2 * time.Hour)}})

	hold, blocked, err := f.runGate()
	if err != nil || blocked || hold != (domain.LegalHold{}) {
		t.Fatalf("gate: hold=%+v blocked=%v err=%v, want (zero,false,nil) (no event falls in the overlapping nanosecond)", hold, blocked, err)
	}
	f.assertArchiveRead("gate with a one-nanosecond overlap")
}

// AC-2c: lowest-ID determinism survives the drop. hold-aaa sorts first but is
// provably disjoint, so the reported blocker is the next candidate, hold-mmm —
// the same hold the unfiltered scan would have reported, because a dropped hold
// contributes no match.
func TestHoldGateDropsDisjointHoldWithoutChangingReportedHold(t *testing.T) {
	f := newGateCostFixture(t)
	f.seedHolds(
		domain.LegalHold{ID: "hold-aaa", Name: "disjoint", Reason: "disjoint window", Filter: domain.Query{From: t0.Add(time.Hour), To: t0.Add(2 * time.Hour)}},
		domain.LegalHold{ID: "hold-mmm", Name: "intersecting", Reason: "overlapping window", Filter: domain.Query{From: t0, To: t0.Add(time.Nanosecond)}},
	)
	hold, blocked, err := f.runGate()
	if err != nil || !blocked {
		t.Fatalf("gate: hold=%+v blocked=%v err=%v, want a block", hold, blocked, err)
	}
	if hold.ID != "hold-mmm" {
		t.Fatalf("reported hold=%q, want hold-mmm (the disjoint lowest-ID hold is dropped)", hold.ID)
	}
	if _, err := f.svc.GetExport(testCtx, "tenant-a", gateJobID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("GetExport error=%v, want ErrForbidden", err)
	} else if want := "export blocked by active legal hold hold-mmm"; err.Error() != want {
		t.Fatalf("GetExport error text=%q, want %q", err.Error(), want)
	}
}

// AC-3.1: an unbound (both-bounds-zero) hold is the fail-closed case. It must
// remain a candidate, block all three service entry points, and still fail the
// run leg with the exact denial in job.Error and no sealed object.
func TestHoldGateUnboundFilterRemainsFailClosed(t *testing.T) {
	f := newGateCostFixture(t)
	hold, err := f.svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "blank", Reason: "blanket investigation", Filter: domain.Query{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, blocked, gateErr := f.runGate(); gateErr != nil || !blocked {
		t.Fatalf("gate: blocked=%v err=%v, want an unbound hold to block", blocked, gateErr)
	}
	f.assertArchiveRead("gate with an unbound hold")

	if _, err := f.svc.CreateExport(testCtx, "tenant-a", "test", f.query); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("CreateExport error=%v, want ErrForbidden", err)
	}
	if _, err := f.svc.GetExport(testCtx, "tenant-a", gateJobID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("GetExport error=%v, want ErrForbidden", err)
	}
	if err := f.svc.RecordExportDownload(testCtx, "tenant-a", "test", gateJobID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("RecordExportDownload error=%v, want ErrForbidden", err)
	}

	// Run leg: the same hold fails the seal without writing an object.
	f.seedPendingJob(gateRunJobID)
	f.svc.runExport(testCtx, gateRunJobID)
	job := f.job(gateRunJobID)
	if job.Status != "failed" {
		t.Fatalf("run status=%s, want failed", job.Status)
	}
	if want := "export blocked by active legal hold " + hold.ID; job.Error != want {
		t.Fatalf("job.Error=%q, want %q", job.Error, want)
	}
	if job.ObjectPath != "" || job.Digest != "" || job.EventCount != 0 {
		t.Fatalf("blocked job must carry empty path/digest and zero count: %+v", job)
	}
	if objects := f.sealedObjectCount(); objects != 0 {
		t.Fatalf("a blocked export must not write archive objects, found %d", objects)
	}
	if n := countBlockedFacts(t, f.svc, hold.ID, "governance-worker"); n != 1 {
		t.Fatalf("governance-worker export.blocked facts=%d, want 1", n)
	}
}

// AC-3.2: a one-sided filter is unreachable through CreateLegalHold (which
// rejects it), so this is a decision-equality pin rather than a policy claim:
// holdMatchesEvent returns false for every event for such a hold, and the
// candidate filter must agree exactly instead of materializing to find out.
func TestHoldGateOneSidedFilterIsNotACandidate(t *testing.T) {
	cases := map[string]domain.Query{
		"from-only": {From: t0, To: time.Time{}},
		"to-only":   {From: time.Time{}, To: t0.Add(time.Hour)},
	}
	for name, filter := range cases {
		t.Run(name, func(t *testing.T) {
			f := newGateCostFixture(t)
			f.seedHolds(domain.LegalHold{ID: "hold-one-sided", Name: "case", Reason: "one-sided window", Filter: filter})
			hold, blocked, err := f.runGate()
			if err != nil || blocked || hold != (domain.LegalHold{}) {
				t.Fatalf("gate: hold=%+v blocked=%v err=%v, want (zero,false,nil)", hold, blocked, err)
			}
			f.assertNoArchiveWork("gate with a one-sided hold filter")
			if _, err := f.svc.GetExport(testCtx, "tenant-a", gateJobID); err != nil {
				t.Fatalf("GetExport: %v", err)
			}
			f.assertNoArchiveWork("GetExport with a one-sided hold filter")
		})
	}
}

// AC-3.3/AC-3.4: a released hold and a foreign-tenant hold are not candidates
// either — both are excluded by exactly the tenant/liveness scoping that
// already governed the unfiltered scan, and neither may cost a WORM read.
func TestHoldGateIgnoresReleasedAndForeignTenantHolds(t *testing.T) {
	released := t0.Add(-time.Minute)
	f := newGateCostFixture(t)
	f.seedHolds(
		domain.LegalHold{ID: "hold-released", Name: "case", Reason: "already released", Filter: domain.Query{}, ReleasedAt: &released},
		domain.LegalHold{ID: "hold-foreign", TenantID: "tenant-b", Name: "case", Reason: "other tenant", Filter: domain.Query{}},
	)
	hold, blocked, err := f.runGate()
	if err != nil || blocked || hold != (domain.LegalHold{}) {
		t.Fatalf("gate: hold=%+v blocked=%v err=%v, want (zero,false,nil)", hold, blocked, err)
	}
	f.assertNoArchiveWork("gate with only released/foreign holds")
	if _, err := f.svc.GetExport(testCtx, "tenant-a", gateJobID); err != nil {
		t.Fatalf("GetExport: %v", err)
	}
	f.assertNoArchiveWork("GetExport with only released/foreign holds")
	if n := f.exportFactCount(domain.AdminActionExportBlocked); n != 0 {
		t.Fatalf("export.blocked facts=%d, want 0", n)
	}
}

// The run leg stays fail-closed after the gate clears: runExport performs its
// own materialization, so an unreadable WORM object still fails the job with
// the archive error and writes no sealed object. The short circuit removes one
// redundant pass; it does not remove the run leg's own read.
func TestRunExportStillFailsClosedOnUnreadableArchivedObject(t *testing.T) {
	f := newGateCostFixture(t)
	f.svc.Config.Archive = &failingEventArchive{inner: f.archive}
	f.seedPendingJob(gateRunJobID)
	f.svc.runExport(testCtx, gateRunJobID)

	job := f.job(gateRunJobID)
	if job.Status != "failed" {
		t.Fatalf("run status=%s, want failed (an unreadable archived object still fails the export)", job.Status)
	}
	if !strings.Contains(job.Error, "worm event object unavailable") {
		t.Fatalf("job.Error=%q, want the archive read error surfaced", job.Error)
	}
	if job.ObjectPath != "" || job.Digest != "" || job.EventCount != 0 {
		t.Fatalf("failed job must carry empty path/digest and zero count: %+v", job)
	}
	if objects := f.sealedObjectCount(); objects != 0 {
		t.Fatalf("a failed export must not write archive objects, found %d", objects)
	}
	if n := countBlockedFacts(t, f.svc, "", "governance-worker"); n != 0 {
		t.Fatalf("export.blocked facts=%d, want 0 (a read error is not a hold denial)", n)
	}
}

// Conservative-keep vs. exact-drop for hold windows that cannot overlap a
// usable query window. An empty (inverted or zero-span) hold window is
// retained as a candidate and decided from the events, because
// holdMatchesEvent rejects every event for it and the gate does not shortcut
// on row-level data no production writer can produce. A well-formed hold window
// that is genuinely outside the query is dropped exactly.
func TestHoldGateDegenerateHoldWindowsAreKeptAndRemoteOnesAreDropped(t *testing.T) {
	cases := []struct {
		name           string
		filter         domain.Query
		expectMaterial bool
	}{
		{"inverted", domain.Query{From: t0.Add(2 * time.Hour), To: t0}, true},
		{"zero-span", domain.Query{From: t0.Add(2 * time.Hour), To: t0.Add(2 * time.Hour)}, true},
		{"inverted-inside-query", domain.Query{From: t0.Add(time.Minute), To: t0}, true},
		{"far-after-query", domain.Query{From: t0.Add(400 * time.Hour), To: t0.Add(401 * time.Hour)}, false},
		{"far-before-query", domain.Query{From: t0.Add(-400 * time.Hour), To: t0.Add(-399 * time.Hour)}, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			f := newGateCostFixture(t)
			f.seedHolds(domain.LegalHold{ID: "hold-degenerate", Name: "case", Reason: "hold window outside the query", Filter: testCase.filter})
			hold, blocked, err := f.runGate()
			if err != nil || blocked || hold != (domain.LegalHold{}) {
				t.Fatalf("gate: hold=%+v blocked=%v err=%v, want (zero,false,nil)", hold, blocked, err)
			}
			if testCase.expectMaterial {
				f.assertArchiveRead("gate with an empty hold window")
			} else {
				f.assertNoArchiveWork("gate with a remote hold window")
			}
		})
	}
}

// gateWindow is one row of the AC-4 predicate truth table. Zero bounds,
// inverted and zero-span windows are deliberate: they are the shapes a
// hand-edited or legacy snapshot row can carry.
type gateWindow struct {
	name string
	from time.Time
	to   time.Time
}

func gateWindows(base time.Time) []gateWindow {
	ns := time.Nanosecond
	return []gateWindow{
		{"unbound", time.Time{}, time.Time{}},
		{"from-only", base, time.Time{}},
		{"to-only", time.Time{}, base.Add(time.Hour)},
		{"inverted", base.Add(time.Hour), base},
		{"zero-span", base, base},
		{"single-nanosecond", base, base.Add(ns)},
		{"unit", base, base.Add(time.Hour)},
		{"shifted-unit", base.Add(12 * time.Hour), base.Add(13 * time.Hour)},
		{"wide", base.Add(-time.Hour), base.Add(24 * time.Hour)},
	}
}

// usableQuery mirrors QueryEvents' own bound validation: a job can only carry
// a window that is fully bound and forward-going.
func (w gateWindow) usableQuery() bool {
	return !w.from.IsZero() && !w.to.IsZero() && w.from.Before(w.to)
}

// AC-4: the candidate filter must agree exactly with brute-force existence of a
// shared instant. For two half-open windows any non-empty intersection has a
// witness at max(F.From, Q.From), so enumerating every boundary instant and its
// ±1ns neighbours is a complete search. The comparison is spelled out with
// !Before/Before rather than one derived expression, so an inclusive bound
// introduced in holdCanSelectAnySelected would be caught here.
func TestHoldCanSelectAnySelectedMatchesBruteForceExistence(t *testing.T) {
	inside := func(instant, from, to time.Time) bool {
		return !instant.Before(from) && instant.Before(to)
	}
	for _, holdWindow := range gateWindows(t0.Add(-24 * time.Hour)) {
		for _, queryWindow := range gateWindows(t0.Add(-24 * time.Hour)) {
			hold := domain.LegalHold{Filter: domain.Query{From: holdWindow.from, To: holdWindow.to}}
			query := domain.Query{From: queryWindow.from, To: queryWindow.to}
			got := holdCanSelectAnySelected(hold, query)
			fullyBound := !holdWindow.from.IsZero() && !holdWindow.to.IsZero()
			switch {
			case fullyBound && !queryWindow.usableQuery():
				// A degenerate export window must never drop a fully bound
				// hold, even one whose own window is inverted or zero-span.
				if !got {
					t.Fatalf("hold=%s query=%s: got false, want true (a degenerate window must never drop a hold)", holdWindow.name, queryWindow.name)
				}
			default:
				want := bruteForceWitness(holdWindow, queryWindow, inside)
				if got != want {
					t.Fatalf("hold=[%s,%s) query=[%s,%s): got %v, want %v (brute-force search found no shared boundary instant)",
						holdWindow.from, holdWindow.to, queryWindow.from, queryWindow.to, got, want)
				}
			}
		}
	}
}

// bruteForceWitness answers "can this hold possibly match a selected event?"
// by sampling every window edge and its neighbours, applying matches' own
// half-open membership test, and applying holdMatchesEvent's own three
// branches for the degenerate filter shapes.
func bruteForceWitness(holdWindow, queryWindow gateWindow, inside func(instant, from, to time.Time) bool) bool {
	if holdWindow.from.IsZero() && holdWindow.to.IsZero() {
		return true // holdMatchesEvent branch 1: unbound matches every event
	}
	if holdWindow.from.IsZero() || holdWindow.to.IsZero() {
		return false // holdMatchesEvent branch 2: one-sided matches no event
	}
	if !holdWindow.from.Before(holdWindow.to) {
		// An inverted or zero-span hold window is empty, so matches would
		// reject every event. The gate keeps it as a candidate rather than
		// proving that from the window alone: the shape is only reachable
		// from a hand-edited or legacy snapshot row, and keeping it costs one
		// materialization while dropping it would trust row-level data that
		// no production writer can produce.
		return true
	}
	for _, edge := range []time.Time{holdWindow.from, holdWindow.to, queryWindow.from, queryWindow.to} {
		for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
			if inside(edge.Add(offset), queryWindow.from, queryWindow.to) && inside(edge.Add(offset), holdWindow.from, holdWindow.to) {
				return true
			}
		}
	}
	return false
}
