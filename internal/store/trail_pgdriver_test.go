package store

// The Postgres branch of the amortized trail compaction, proven against a
// real database/sql driver.
//
// The defect this pins is that postgresBackend.AppendAdminFact never reset
// p.trailCount after a compaction, so every audited read past the cap issued
// trailCompactQuery — an O(cap) `DELETE ... OFFSET` scan — while holding
// trailMu, which serializes every other audited read's append behind it.
//
// The DSN-gated TestPostgresTrailCompactionWatermark covers the same SQL
// against a real PostgreSQL, but it skips without AUDIT_TEST_POSTGRES_DSN, so
// it cannot be the primary pin. This file therefore uses a hand-written
// database/sql driver (~150 lines, no new module dependency — go.mod has no
// sqlmock) that records every (query, args) pair and answers only what the
// trail append path issues. Each test registers its own driver name, matching
// the probe driver in operation_deadline_test.go: database/sql's registry is
// process-global and append-only, so per-test names keep every recorder
// private to its test and keep -race clean without a shared package var.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// pgCall is one recorded statement: the query text and its bound arguments.
type pgCall struct {
	query string
	args  []driver.Value
}

// pgRecorder is the scripted database/sql driver behind the trail compaction
// tests. It recognises the four statements postgresBackend.AppendAdminFact
// issues and records them; anything else is an error so a new statement on the
// trail path cannot pass unnoticed.
type pgRecorder struct {
	mu sync.Mutex
	// seedCount answers trailCountQuery, standing in for the rows a shared
	// table already holds.
	seedCount int64
	// compactFailures makes that many leading compaction DELETEs fail, so the
	// retry-after-a-failed-compaction discipline can be pinned.
	compactFailures int
	calls           []pgCall
}

func (r *pgRecorder) Open(string) (driver.Conn, error) { return &pgConn{recorder: r}, nil }

func (r *pgRecorder) record(query string, args []driver.NamedValue) {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, pgCall{query: query, args: values})
}

func (r *pgRecorder) takeCompactFailure() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.compactFailures > 0 {
		r.compactFailures--
		return true
	}
	return false
}

// callsMatching returns the recorded calls whose query contains fragment.
func (r *pgRecorder) callsMatching(fragment string) []pgCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	var matched []pgCall
	for _, call := range r.calls {
		if strings.Contains(call.query, fragment) {
			matched = append(matched, call)
		}
	}
	return matched
}

// reset drops the recorded calls so a test can count statements per window.
func (r *pgRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

type pgConn struct{ recorder *pgRecorder }

func (c *pgConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("pg recorder: prepared statements are not supported")
}

func (c *pgConn) Close() error { return nil }

func (c *pgConn) Begin() (driver.Tx, error) {
	return nil, errors.New("pg recorder: Begin is not supported; use BeginTx")
}

func (c *pgConn) Ping(context.Context) error { return nil }

func (c *pgConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.recorder.record(query, args)
	if isTrailCompact(query) && c.recorder.takeCompactFailure() {
		return nil, errors.New("deadlock detected")
	}
	return driver.RowsAffected(1), nil
}

// QueryContext answers trailCountQuery (the lazy seed) and nothing else; the
// trail append path issues no other reads.
func (c *pgConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.recorder.record(query, args)
	if strings.Contains(query, "count(*) FROM admin_action_trail") {
		return &pgCountRows{value: c.recorder.seedCount}, nil
	}
	return nil, fmt.Errorf("pg recorder: unexpected query %q", query)
}

type pgTx struct{ conn *pgConn }

func (tx *pgTx) Commit() error { return nil }

func (tx *pgTx) Rollback() error { return nil }

// pgCountRows is the one-column, one-row answer trailCountQuery scans.
type pgCountRows struct {
	value int64
	done  bool
}

func (r *pgCountRows) Columns() []string { return []string{"count"} }

func (r *pgCountRows) Close() error { return nil }

func (r *pgCountRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.value
	return nil
}

func isTrailCompact(query string) bool {
	return strings.Contains(query, "DELETE FROM admin_action_trail")
}

// openTrailRecorder registers a fresh recorder under a name unique to the
// test and returns a postgresBackend driving it. The backend is constructed
// directly (same package) because the recorder only answers trail statements:
// the snapshot Load/Save path this direction must not disturb is exercised by
// the DSN-gated tests.
func openTrailRecorder(t *testing.T, seedCount int64) (*postgresBackend, *pgRecorder) {
	t.Helper()
	recorder := &pgRecorder{seedCount: seedCount}
	name := "trailrecorder-" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	sql.Register(name, recorder)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &postgresBackend{db: db}, recorder
}

// appendTrailFact appends the n-th fact and reports the counter after the call.
func appendTrailFact(t *testing.T, backend *postgresBackend, n, trailCap int) int64 {
	t.Helper()
	if err := backend.AppendAdminFact(testCtx, trailFactN(n), trailCap); err != nil {
		t.Fatalf("append %d: %v", n, err)
	}
	return backend.trailCount
}

// TestPostgresTrailAppendAmortizesCompaction pins R3/R4/AC-4: the compaction
// DELETE runs once per headroom+1 appends with the watermark as its argument
// (never the cap), and the local counter resets to the watermark afterwards so
// the next window is free. Before the change the counter was never reset, so
// every append past the cap re-issued the DELETE with the cap.
func TestPostgresTrailAppendAmortizesCompaction(t *testing.T) {
	const cap, watermark, headroom = 24, 12, 12
	backend, recorder := openTrailRecorder(t, 0)

	// The lazy seed is one SELECT count(*) and one CREATE TABLE IF NOT
	// EXISTS; the seed count is 0 so the trail starts empty.
	for n := 1; n <= cap; n++ {
		appendTrailFact(t, backend, n, cap)
	}
	if got := recorder.callsMatching("DELETE FROM admin_action_trail"); len(got) != 0 {
		t.Fatalf("DELETE count=%d, want 0 while the trail is still filling", len(got))
	}
	if backend.trailCount != cap {
		t.Fatalf("trailCount=%d, want %d at the cap", backend.trailCount, cap)
	}

	// The next append trips compaction exactly once. Its single bound argument
	// is the watermark, not the cap — the parameter change this test pins.
	recorder.reset()
	appendTrailFact(t, backend, cap+1, cap)
	deletes := recorder.callsMatching("DELETE FROM admin_action_trail")
	if len(deletes) != 1 {
		t.Fatalf("DELETE count=%d, want exactly 1 on the overflowing append", len(deletes))
	}
	if len(deletes[0].args) != 1 {
		t.Fatalf("DELETE args=%v, want exactly one bound parameter", deletes[0].args)
	}
	if got := fmt.Sprint(deletes[0].args[0]); got != fmt.Sprint(watermark) {
		t.Fatalf("DELETE arg=%s, want the watermark %d (not the cap %d)", got, watermark, cap)
	}
	// The SQL text itself is unchanged, so the seq watermark still protects a
	// concurrent replica's newer insert (F-4).
	if deletes[0].query != trailCompactQuery {
		t.Fatalf("DELETE text changed:\n got %q\nwant %q", deletes[0].query, trailCompactQuery)
	}
	// The reset: the counter is back at the watermark, so headroom appends
	// fit before the next compaction.
	if backend.trailCount != watermark {
		t.Fatalf("trailCount=%d, want the watermark %d after compaction", backend.trailCount, watermark)
	}

	// A whole headroom of appends issues no DELETE at all.
	recorder.reset()
	for i := 0; i < headroom; i++ {
		appendTrailFact(t, backend, cap+2+i, cap)
	}
	if got := recorder.callsMatching("DELETE FROM admin_action_trail"); len(got) != 0 {
		t.Fatalf("DELETE count=%d, want 0 across the %d-append headroom", len(got), headroom)
	}
	// The (headroom+1)-th append trips it again.
	appendTrailFact(t, backend, cap+2+headroom, cap)
	if got := recorder.callsMatching("DELETE FROM admin_action_trail"); len(got) != 1 {
		t.Fatalf("DELETE count=%d, want 1 on the append after a full headroom", len(got))
	}
	if backend.trailCount != watermark {
		t.Fatalf("trailCount=%d, want the watermark %d after the second compaction", backend.trailCount, watermark)
	}
}

// TestPostgresTrailCompactionFailureRetries pins FM-D: a compaction error
// leaves the counter high, so the next append retries the DELETE instead of
// silently skipping compaction and letting the trail grow without bound.
func TestPostgresTrailCompactionFailureRetries(t *testing.T) {
	const cap = 24
	backend, recorder := openTrailRecorder(t, 0)
	for n := 1; n <= cap; n++ {
		appendTrailFact(t, backend, n, cap)
	}

	recorder.compactFailures = 1
	err := backend.AppendAdminFact(testCtx, trailFactN(cap+1), cap)
	if err == nil {
		t.Fatal("a failed compaction must fail the append closed")
	}
	if !strings.Contains(err.Error(), "compact admin action trail:") {
		t.Fatalf("err=%v, want it wrapped as 'compact admin action trail:'", err)
	}
	// The counter is left above the cap, so the next append retries.
	if backend.trailCount != cap+1 {
		t.Fatalf("trailCount=%d, want %d (no reset on a failed compaction)", backend.trailCount, cap+1)
	}
	recorder.reset()
	if err := backend.AppendAdminFact(testCtx, trailFactN(cap+2), cap); err != nil {
		t.Fatalf("retry after a failed compaction: %v", err)
	}
	if got := recorder.callsMatching("DELETE FROM admin_action_trail"); len(got) != 1 {
		t.Fatalf("DELETE count=%d, want 1 (the next append retries the compaction)", len(got))
	}
	if backend.trailCount != int64(trailWatermark(cap)) {
		t.Fatalf("trailCount=%d, want the watermark %d after the successful retry",
			backend.trailCount, trailWatermark(cap))
	}
}

// TestPostgresTrailAmortizationMatchesFileBackend is the cross-backend
// consistency proof (AC-6): both backends trim to the same watermark and
// therefore reach the same fact count after the same number of appends. The
// file side is asserted on the trail file, the postgres side on the local
// counter, which after a compaction equals the retained row count.
func TestPostgresTrailAmortizationMatchesFileBackend(t *testing.T) {
	const cap = 8
	backend, _ := openTrailRecorder(t, 0)
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	fileStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fileStore.Close() })

	for n := 1; n <= 32; n++ {
		fact := trailFactN(n)
		if err := backend.AppendAdminFact(testCtx, fact, cap); err != nil {
			t.Fatalf("postgres append %d: %v", n, err)
		}
		if err := fileStore.AppendAdminFact(testCtx, fact, 100, cap); err != nil {
			t.Fatalf("file append %d: %v", n, err)
		}
		fileCount := trailLines(t, path)
		if fileCount > cap {
			t.Fatalf("append %d: file trail lines=%d, want <= cap %d", n, fileCount, cap)
		}
		if backend.trailCount > cap {
			t.Fatalf("append %d: postgres trailCount=%d, want <= cap %d", n, backend.trailCount, cap)
		}
		// Between compactions both counters advance in lockstep.
		if n%trailHeadroom(cap) != 0 && backend.trailCount != fileCount {
			t.Fatalf("append %d: postgres trailCount=%d, file lines=%d (backends must trim alike)",
				n, backend.trailCount, fileCount)
		}
	}
	// Both ended on the same steady state: watermark + ((32-watermark) mod
	// (headroom+1)) = 4 + (28 mod 5) = 7.
	if got, want := backend.trailCount, int64(7); got != want {
		t.Fatalf("postgres trailCount=%d, want %d", got, want)
	}
	if got, want := trailLines(t, path), int64(7); got != want {
		t.Fatalf("file trail lines=%d, want %d", got, want)
	}
}
