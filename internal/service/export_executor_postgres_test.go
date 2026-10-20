package service

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Occupy only this test's one-connection pool, after real terminal persistence
// and heartbeat shutdown. A late cleanup also closes a not-yet-acquired Conn;
// no test failure can strand a connection while joining its executor.
type schedulerPoolGate struct {
	mu      sync.Mutex
	conn    *sql.Conn
	closing bool
	ready   chan bool
	waits   int64
}

func (g *schedulerPoolGate) hold(db *sql.DB) {
	ctx, cancel := context.WithTimeout(testCtx, 2*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	g.mu.Lock()
	g.waits = db.Stats().WaitCount
	if g.closing && conn != nil {
		_ = conn.Close()
	} else {
		g.conn = conn
	}
	g.mu.Unlock()
	g.ready <- err == nil
}

func (g *schedulerPoolGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closing = true
	if g.conn != nil {
		_ = g.conn.Close()
		g.conn = nil
	}
}

func TestPostgresExportExecutorFinalReadRetainsReservationAcrossPoolWait(t *testing.T) {
	first, second := newExportLeasePostgres(t) // disposable opt-in DB, independent pools
	seedTestDomain(t, first)
	objects := &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
	first.Config.Archive, second.Config.Archive = objects, objects
	if _, err := first.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("executor-source", "executor-operation", pinnedNow.Add(time.Second)), domain.StatusArchived); err != nil {
		t.Fatal("could not prepare original PG-anchored real file evidence")
	}
	seedLeaseExport(t, first, "executor-tail", "tenant-a")
	original, err := first.Store.Snapshot(testCtx)
	if err != nil || original.LayoutVersion != 2 || len(original.Events) != 0 {
		t.Fatal("executor integration requires active v2 with evicted committed evidence")
	}
	// This extra pool reaches only the explicit disposable test database.
	// One connection is a test synchronization device, not a production default.
	db, err := sql.Open("pgx", os.Getenv("AUDIT_TEST_POSTGRES_EXPORT_LEASE_DSN"))
	if err != nil {
		t.Fatal("could not open owned executor pool")
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	st, err := store.OpenPostgres(db)
	if err != nil {
		t.Fatal("could not open owned executor store")
	}
	worker := newServiceOn(t, st)
	paused := &schedulerPausedArchive{Store: objects, entered: make(chan string, 1), release: make(chan struct{})}
	worker.Config.Archive = paused
	gate := &schedulerPoolGate{ready: make(chan bool, 1)}
	worker.exportHeartbeat = func() (<-chan time.Time, func()) {
		return make(chan time.Time), func() { gate.hold(db) }
	}
	ctx, cancel := context.WithCancel(testCtx)
	releasePut := sync.OnceFunc(func() { close(paused.release) })
	t.Cleanup(func() {
		gate.release()
		releasePut()
		cancel()
		awaitSchedulerFixtureIdle(t, worker)
	})
	worker.wakeExports(ctx)
	select {
	case <-paused.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("real PG executor did not reach its actual file Put")
	}
	waitSchedulerBoundary(t, func() bool {
		worker.exportMu.Lock()
		defer worker.exportMu.Unlock()
		return !worker.exportDispatching && len(worker.exportActive) == 1
	})
	releasePut()
	select {
	case ok := <-gate.ready:
		if !ok {
			t.Fatal("could not hold the actual pool after heartbeat shutdown")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("real executor did not finish persistence and stop its heartbeat")
	}
	gate.mu.Lock()
	baselineWaits := gate.waits
	gate.mu.Unlock()
	waitSchedulerBoundary(t, func() bool { return db.Stats().WaitCount > baselineWaits })
	if !worker.exportMu.TryLock() {
		t.Fatal("scheduler held its mutex across the final real pool wait")
	}
	active, dispatching := len(worker.exportActive), worker.exportDispatching
	worker.exportMu.Unlock()
	t.Logf("executor_pg_tail_boundary active=%d dispatching=%t actual_pool_wait=%t", active, dispatching, db.Stats().WaitCount > baselineWaits)
	if active != 1 || dispatching {
		t.Error("executor disappeared while its final real SQL operation waited for a pool connection")
	}
	completed := leaseSnapshot(t, second)
	job := completed.Exports["executor-tail"]
	if job.Status != "completed" || job.EventCount != 1 || job.FinishedAt == nil || job.ObjectPath == "" || len(completed.ExportLeases) != 0 || len(completed.ExportPlans) != 0 || len(completed.ExportCheckpoints) != 0 {
		t.Fatal("pool wait oracle did not follow a real fenced terminal commit")
	}
	// An independent pool remains authoritative while the local tail waits.
	// A Hold created after completion must still block subsequent downloads.
	if _, err := second.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "case", Reason: "case"}); err != nil {
		t.Fatal("independent pool could not commit a current Hold during executor tail")
	}
	before := leaseSnapshot(t, second)
	gate.release()
	awaitSchedulerFixtureIdle(t, worker)
	after := leaseSnapshot(t, first)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("executor tail overwrote concurrent Hold, terminal metadata or original facts")
	}
	if !reflect.DeepEqual(after.Exports["executor-tail"], job) {
		t.Fatal("lifecycle handoff changed the original completed output")
	}
	current, err := first.Store.Snapshot(testCtx)
	if err != nil || !reflect.DeepEqual(original.Receipts, current.Receipts) || !reflect.DeepEqual(original.Schemas, current.Schemas) {
		t.Fatal("scheduler tail rewrote original committed receipt or exact Schema")
	}
	if err := worker.RecordExportDownload(testCtx, "tenant-a", "reader", job.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatal("terminal completion or lifecycle join bypassed the independent current Hold gate")
	}
}
