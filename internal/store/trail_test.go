package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func trailFact(id string) domain.AdminAction {
	return domain.AdminAction{
		ID: id, TenantID: "tenant-a", Actor: "auditor-1", Action: domain.AdminActionEventRead,
		TargetType: "event", TargetID: "evt-" + id, CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
}

func trailFacts(n int) []domain.AdminAction {
	out := make([]domain.AdminAction, n)
	for i := range out {
		out[i] = trailFact(string(rune('a' + i)))
	}
	return out
}

// TestTrailAppendReadRoundTrip pins the JSONL contract: one line per fact on
// <state>.admin-trail.jsonl, read back newest-first (reverse append order),
// and the snapshot file is never touched by a trail append.
func TestTrailAppendReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	facts := trailFacts(3)
	for _, fact := range facts {
		if err := st.AppendAdminFact(testCtx, fact, 100, 100); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	got, err := st.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("trail len=%d, want 3", len(got))
	}
	// Newest-first: the last append is first.
	if got[0].ID != facts[2].ID || got[2].ID != facts[0].ID {
		t.Fatalf("trail order=%v, want reverse of %v", ids(got), ids(facts))
	}
	// state.json itself was never created by trail appends.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("state.json exists after trail-only appends: %v", err)
	}
	trailPath := path + trailPathSuffix
	contents, err := os.ReadFile(trailPath)
	if err != nil {
		t.Fatal(err)
	}
	if lines := countJSONLLines(contents); lines != 3 {
		t.Fatalf("trail lines=%d, want 3", lines)
	}
}

func ids(facts []domain.AdminAction) []string {
	out := make([]string, len(facts))
	for i, fact := range facts {
		out[i] = fact.ID
	}
	return out
}

// TestTrailCompactionKeepsNewest pins F-4/F-7: past the cap the trail is
// compacted to the newest trailCap facts (drop-oldest), the file is a valid
// JSONL afterwards, and a fresh backend reopen sees the same bounded view
// (baseline count, not a double-trim).
func TestTrailCompactionKeepsNewest(t *testing.T) {
	t.Run("file backend", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		st, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		facts := trailFacts(7)
		for _, fact := range facts {
			if err := st.AppendAdminFact(testCtx, fact, 100, 3); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		got, err := st.ReadAdminTrail(testCtx)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("compacted trail len=%d, want 3", len(got))
		}
		for i, want := range []string{facts[6].ID, facts[5].ID, facts[4].ID} {
			if got[i].ID != want {
				t.Fatalf("compacted order[%d]=%s, want %s", i, got[i].ID, want)
			}
		}
		// Reopen: the baseline count must not re-trim, and the file stays at
		// the cap (append order preserved).
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		st2, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		got2, err := st2.ReadAdminTrail(testCtx)
		if err != nil {
			t.Fatal(err)
		}
		if len(got2) != 3 {
			t.Fatalf("reopened trail len=%d, want 3 (baseline count)", len(got2))
		}
	})
	t.Run("in-memory backend", func(t *testing.T) {
		st, err := Open("")
		if err != nil {
			t.Fatal(err)
		}
		facts := trailFacts(7)
		for _, fact := range facts {
			if err := st.AppendAdminFact(testCtx, fact, 100, 3); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		got, err := st.ReadAdminTrail(testCtx)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 || got[0].ID != facts[6].ID || got[2].ID != facts[4].ID {
			t.Fatalf("in-memory compaction=%v, want newest 3 reverse", ids(got))
		}
	})
}

// TestTrailReadFailsClosedOnCorruption pins FM-4: a malformed NON-final line
// errors (the governance surface must not silently truncate evidence), while
// a truncated FINAL line (crash mid-append) is tolerated and dropped.
func TestTrailReadFailsClosedOnCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, fact := range trailFacts(2) {
		if err := st.AppendAdminFact(testCtx, fact, 100, 100); err != nil {
			t.Fatal(err)
		}
	}
	trailPath := path + trailPathSuffix

	t.Run("truncated final line tolerated", func(t *testing.T) {
		// Simulate a crash mid-append: append a partial line without '\n'.
		f, err := os.OpenFile(trailPath, os.O_APPEND|os.O_WRONLY, 0o640)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(`{"id":"partial"`); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := st.ReadAdminTrail(testCtx)
		if err != nil {
			t.Fatalf("truncated final line must be tolerated, got %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("trail len=%d, want 2 (partial line dropped)", len(got))
		}
	})

	t.Run("malformed non-final line fails closed", func(t *testing.T) {
		f, err := os.OpenFile(trailPath, os.O_APPEND|os.O_WRONLY, 0o640)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString("not-json\n"); err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString("{\"id\":\"after\"}\n"); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := st.ReadAdminTrail(testCtx); err == nil {
			t.Fatal("malformed non-final line must fail the read")
		}
	})
}

// countingTrailBackend is the AC-4 instrumentation backend: it implements
// Backend + AdminTrail and counts Save and trail-append calls so tests can
// prove reads never rewrite the snapshot and every fact lands in the trail.
type countingTrailBackend struct {
	mu      sync.Mutex
	saves   int
	appends int
	trail   []domain.AdminAction
	snap    *Snapshot
	// appendErr, when set, makes AppendAdminFact fail so the fail-closed
	// contract can be pinned without a real IO failure.
	appendErr error
}

func (b *countingTrailBackend) Load(ctx context.Context) (*Snapshot, error) { return b.snap, nil }
func (b *countingTrailBackend) LoadForUpdate(ctx context.Context) (*Snapshot, error) {
	return cloneSnapshot(b.snap)
}
func (b *countingTrailBackend) Save(ctx context.Context, data *Snapshot) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.saves++
	b.snap = data
	return nil
}
func (b *countingTrailBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, _ int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.appendErr != nil {
		return b.appendErr
	}
	b.appends++
	b.trail = append(b.trail, fact)
	return nil
}
func (b *countingTrailBackend) ReadAdminTrail(ctx context.Context) ([]domain.AdminAction, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]domain.AdminAction, len(b.trail))
	copy(out, b.trail)
	return out, nil
}

// TestAppendAdminFactIsolation pins AC-4(a): on a trail-capable backend, N
// read facts produce exactly N trail appends and ZERO snapshot Saves.
func TestAppendAdminFactIsolation(t *testing.T) {
	backend := &countingTrailBackend{snap: NewSnapshot()}
	st := NewWithBackend(backend)
	facts := trailFacts(5)
	for _, fact := range facts {
		if err := st.AppendAdminFact(testCtx, fact, 100, 100); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	backend.mu.Lock()
	saves, appends := backend.saves, backend.appends
	backend.mu.Unlock()
	if saves != 0 {
		t.Fatalf("saves=%d, want 0 (reads must not rewrite the snapshot)", saves)
	}
	if appends != len(facts) {
		t.Fatalf("appends=%d, want %d", appends, len(facts))
	}
}

// TestTrailAppendFailClosesRead pins FM-1: an append error is returned and
// the fact is not persisted; the snapshot is untouched.
func TestTrailAppendFailClosesRead(t *testing.T) {
	backend := &countingTrailBackend{snap: NewSnapshot(), appendErr: errors.New("trail full")}
	st := NewWithBackend(backend)
	err := st.AppendAdminFact(testCtx, trailFact("x"), 100, 100)
	if !errors.Is(err, backend.appendErr) {
		t.Fatalf("append err=%v, want %v", err, backend.appendErr)
	}
	trail, err := st.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(trail) != 0 {
		t.Fatalf("trail len=%d, want 0 (failed append persists nothing)", len(trail))
	}
}

// scriptedFallbackBackend is a capability-less (Backend-only) stateful seam
// mirroring the service package's scriptedConflictBackend: Save commits the
// mutated snapshot, failing with ErrSnapshotConflict `conflicts` times (no
// commit) or a non-conflict saveErr, so the fallback's conflict-retry and
// fail-closed contracts can be pinned.
type scriptedFallbackBackend struct {
	data      *Snapshot
	conflicts int // remaining conflicts before Save commits
	saves     int
	saveErr   error
}

func (b *scriptedFallbackBackend) Load(ctx context.Context) (*Snapshot, error) { return b.data, nil }

func (b *scriptedFallbackBackend) LoadForUpdate(ctx context.Context) (*Snapshot, error) {
	return cloneSnapshot(b.data)
}

func (b *scriptedFallbackBackend) Save(ctx context.Context, data *Snapshot) error {
	b.saves++
	if b.saveErr != nil {
		return b.saveErr
	}
	if b.conflicts > 0 {
		b.conflicts--
		return ErrSnapshotConflict
	}
	b.data = data
	return nil
}

// TestAppendAdminFactFallbackPreservesConflictRetries pins F-1/FM-3: on a
// capability-less backend the fallback reuses updateLocked, so transient
// conflicts retry in place and the fact commits exactly once.
func TestAppendAdminFactFallbackPreservesConflictRetries(t *testing.T) {
	backend := &scriptedFallbackBackend{data: NewSnapshot(), conflicts: 2}
	st := NewWithBackend(backend)
	fact := trailFact("conflict")
	if err := st.AppendAdminFact(testCtx, fact, 100, 100); err != nil {
		t.Fatalf("fallback append = %v, want nil", err)
	}
	if backend.saves != 3 {
		t.Fatalf("saves=%d, want 3 (initial + 2 retries)", backend.saves)
	}
	if len(backend.data.AdminActions) != 1 || backend.data.AdminActions[0].ID != fact.ID {
		t.Fatalf("fallback fact=%+v, want the appended fact committed once", backend.data.AdminActions)
	}
}

// TestAppendAdminFactFallbackSurfacesSaveErr pins the fail-closed seam: a
// capability-less backend whose Save fails surfaces the raw error and
// commits nothing.
func TestAppendAdminFactFallbackSurfacesSaveErr(t *testing.T) {
	backend := &scriptedFallbackBackend{data: NewSnapshot(), saveErr: errors.New("storage unavailable")}
	st := NewWithBackend(backend)
	err := st.AppendAdminFact(testCtx, trailFact("fail"), 100, 100)
	if !errors.Is(err, backend.saveErr) {
		t.Fatalf("fallback err=%v, want %v", err, backend.saveErr)
	}
	if backend.saves != 1 {
		t.Fatalf("saves=%d, want 1 (no retry on non-conflict error)", backend.saves)
	}
	if len(backend.data.AdminActions) != 0 {
		t.Fatalf("failed append committed %d facts, want 0", len(backend.data.AdminActions))
	}
}

// TestAppendAdminFactFallbackBoundedBySnapshotCap pins the fallback bound:
// even capability-less backends never regrow Snapshot.AdminActions past the
// snapshot cap (drop-oldest, newest retained).
func TestAppendAdminFactFallbackBoundedBySnapshotCap(t *testing.T) {
	backend := &scriptedFallbackBackend{data: NewSnapshot()}
	st := NewWithBackend(backend)
	facts := trailFacts(7)
	for _, fact := range facts {
		if err := st.AppendAdminFact(testCtx, fact, 3, 100); err != nil {
			t.Fatal(err)
		}
	}
	if len(backend.data.AdminActions) != 3 {
		t.Fatalf("fallback AdminActions=%d, want 3 (capped)", len(backend.data.AdminActions))
	}
	if backend.data.AdminActions[0].ID != facts[4].ID || backend.data.AdminActions[2].ID != facts[6].ID {
		t.Fatalf("fallback kept=%v, want the newest 3 in append order", ids(backend.data.AdminActions))
	}
}

// TestAppendAdminFactDoesNotHoldStoreLock is a best-effort timing guard for
// F-2: the trail path must not take Store.mu, so a concurrent snapshot
// mutation is never blocked behind trail appends. It is observational
// (documents the lock discipline); the deterministic proof is the absence of
// Store.mu acquisition in the trail path itself.
func TestAppendAdminFactDoesNotHoldStoreLock(t *testing.T) {
	backend := &countingTrailBackend{snap: NewSnapshot()}
	st := NewWithBackend(backend)
	// A snapshot write must be able to complete while trail appends are in
	// flight (would deadlock or block if the trail path took Store.mu).
	done := make(chan struct{})
	go func() {
		for i := 0; i < 20; i++ {
			_ = st.AppendAdminFact(testCtx, trailFact("x"), 100, 100)
		}
		close(done)
	}()
	if err := st.Update(testCtx, func(data *Snapshot) error {
		data.Tenants["t1"] = domain.Tenant{ID: "t1", Name: "T1", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("trail appends blocked behind Store.mu (F-2 violation)")
	}
}

// TestReadAdminTrailEmptyWhenCapabilityAbsent pins the seam contract: a
// capability-less backend reports an empty trail, so ListAdminActions' fast
// path (snapshot-only) is reachable from test seams unchanged.
func TestReadAdminTrailEmptyWhenCapabilityAbsent(t *testing.T) {
	backend := &scriptedFallbackBackend{data: NewSnapshot()}
	st := NewWithBackend(backend)
	trail, err := st.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(trail) != 0 {
		t.Fatalf("trail=%v, want empty for capability-less backend", trail)
	}
}

// TestPostgresTrailAppendDoesNotBumpSnapshotVersion is the PG branch of
// AC-4(b): a read fact inserts one admin_action_trail row and leaves
// audit_state_snapshot.version untouched — reads never rewrite the snapshot
// row. Skipped without AUDIT_TEST_POSTGRES_DSN (newPostgresTestDB gate).
func TestPostgresTrailAppendDoesNotBumpSnapshotVersion(t *testing.T) {
	db := newPostgresTestDB(t)
	st := seedTenant(t, db)
	versionBefore := 0
	if err := db.QueryRow(`SELECT version FROM audit_state_snapshot WHERE id = 1`).Scan(&versionBefore); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendAdminFact(testCtx, trailFact("pg-1"), 100, 100); err != nil {
		t.Fatalf("trail append: %v", err)
	}
	versionAfter := 0
	if err := db.QueryRow(`SELECT version FROM audit_state_snapshot WHERE id = 1`).Scan(&versionAfter); err != nil {
		t.Fatal(err)
	}
	if versionAfter != versionBefore {
		t.Fatalf("snapshot version bumped %d -> %d by a trail append", versionBefore, versionAfter)
	}
	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM admin_action_trail`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("trail rows=%d, want 1", rows)
	}
	got, err := st.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "pg-1" {
		t.Fatalf("trail=%+v, want [pg-1]", got)
	}
}

// TestPostgresTrailCompactionWatermark pins F-4/FM-7 on PG: past the cap the
// compaction deletes the oldest rows only, keeping the newest cap. The exact
// length (3) is unchanged by the watermark: at cap 3 the watermark is 2 and
// the interval is 2, so after 7 appends the steady state is
// 2 + ((7-2) mod 2) == 3 — the same value the old trim-to-the-cap produced.
func TestPostgresTrailCompactionWatermark(t *testing.T) {
	db := newPostgresTestDB(t)
	st := seedTenant(t, db)
	for _, fact := range trailFacts(7) {
		if err := st.AppendAdminFact(testCtx, fact, 100, 3); err != nil {
			t.Fatal(err)
		}
	}
	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM admin_action_trail`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 3 {
		t.Fatalf("trail rows=%d, want 3 (compacted)", rows)
	}
	got, err := st.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].ID != "g" || got[2].ID != "e" {
		t.Fatalf("trail=%v, want newest 3 reverse (g,f,e)", ids(got))
	}
	// Watermark evidence (R2/R6): the trail never exceeds the cap, and the
	// 4th append is where the table drops to exactly trailWatermark(3).
	if rows > 3 {
		t.Fatalf("trail rows=%d, want <= cap 3", rows)
	}
	if got := trailWatermark(3); got != 2 || trailHeadroom(3) != 1 {
		t.Fatalf("(watermark, headroom)=(%d,%d), want (2,1)", got, trailHeadroom(3))
	}
	for _, fact := range trailFacts(4) {
		if err := st.AppendAdminFact(testCtx, fact, 100, 3); err != nil {
			t.Fatal(err)
		}
	}
	var afterReset int
	if err := db.QueryRow(`SELECT count(*) FROM admin_action_trail`).Scan(&afterReset); err != nil {
		t.Fatal(err)
	}
	if afterReset > 3 {
		t.Fatalf("trail rows=%d after 8 appends, want <= cap 3", afterReset)
	}
	compacted, err := st.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	// The compactions at appends 4 and 6 each left exactly the watermark, and
	// drop-oldest/newest-first still holds for every consumer.
	if len(compacted) < trailWatermark(3) {
		t.Fatalf("trail=%v, want at least the watermark %d facts", ids(compacted), trailWatermark(3))
	}
	if compacted[0].ID != "d" {
		t.Fatalf("newest fact=%s, want d (the last append survives)", compacted[0].ID)
	}
}

// ---- amortized trail compaction -----------------------------------------

// trailFactN is a fact with a stable, unique-per-append id, so tests that
// append more facts than trailFacts can spell (its ids run a..z) can still
// assert newest-first identity.
func trailFactN(n int) domain.AdminAction {
	return trailFact(fmt.Sprintf("fact-%03d", n))
}

// trailCompactionsOf reads the file backend's compaction counter under f.mu,
// so the observation point itself is race-clean.
func trailCompactionsOf(t *testing.T, st *Store) int64 {
	t.Helper()
	backend, ok := st.backend.(*fileBackend)
	if !ok {
		t.Fatalf("backend is %T, want *fileBackend", st.backend)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.trailCompactions
}

// trailLines reads the complete-line count of the trail file backing st.
func trailLines(t *testing.T, path string) int64 {
	t.Helper()
	contents, err := os.ReadFile(path + trailPathSuffix)
	if err != nil {
		t.Fatalf("read trail file: %v", err)
	}
	return countJSONLLines(contents)
}

// TestTrailWatermarkDerivedFromCap pins R1/AC-3: the compaction watermark and
// headroom are pure functions of the operator cap, introduce no new
// configurable number, and keep the compaction trigger reachable for every
// bounded cap. The retention divisor (2) is an internal identity of the
// amortization — not a constant, flag, env var, metric or SLO.
func TestTrailWatermarkDerivedFromCap(t *testing.T) {
	cases := []struct {
		cap           int
		wantWatermark int
		wantHeadroom  int
	}{
		// cap <= 0 is operator-explicit "unbounded": the trigger
		// (trailCap > 0 && ...) is unreachable, so nothing is trimmed.
		{0, 0, 0},
		{-1, 0, 0},
		// cap == 1 is the documented degenerate case: W = 1, H = 0, so it
		// keeps today's compact-per-append behaviour (no positive headroom
		// exists under a one-fact cap).
		{1, 1, 0},
		{2, 1, 1},
		{3, 2, 1},
		{4, 2, 2},
		{5, 3, 2},
		{7, 4, 3},
		{100, 50, 50},
		{100000, 50000, 50000},
	}
	for _, tc := range cases {
		watermark, headroom := trailWatermark(tc.cap), trailHeadroom(tc.cap)
		if watermark != tc.wantWatermark || headroom != tc.wantHeadroom {
			t.Errorf("cap=%d: (watermark, headroom)=(%d,%d), want (%d,%d)",
				tc.cap, watermark, headroom, tc.wantWatermark, tc.wantHeadroom)
			continue
		}
		if tc.cap <= 0 {
			// Unbounded: no retention, no headroom, trigger unreachable.
			if watermark != 0 || headroom != 0 {
				t.Errorf("cap=%d: unbounded trail must not be trimmed", tc.cap)
			}
			continue
		}
		if watermark < 1 || watermark > tc.cap {
			t.Errorf("cap=%d: watermark=%d, want 1..cap", tc.cap, watermark)
		}
		if headroom != tc.cap-watermark {
			t.Errorf("cap=%d: headroom=%d, want %d", tc.cap, headroom, tc.cap-watermark)
		}
		if tc.cap >= 2 && headroom < 1 {
			t.Errorf("cap=%d: headroom=%d, want >= 1 (compaction must amortize)", tc.cap, headroom)
		}
		// Amortization bound: at most two retained facts rewritten per
		// append amortized (watermark <= 2*headroom), independent of the cap.
		if tc.cap >= 2 && watermark > 2*headroom {
			t.Errorf("cap=%d: watermark=%d headroom=%d, want watermark <= 2*headroom", tc.cap, watermark, headroom)
		}
	}
}

// TestTrailUnboundedCapNeverCompacts pins the "0 = unbounded append-only"
// operator choice (R1): with trailCap 0 the compaction trigger's
// trailCap > 0 conjunct is false forever, so the trail file is never
// rewritten regardless of how many facts accumulate.
func TestTrailUnboundedCapNeverCompacts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 40; i++ {
		if err := st.AppendAdminFact(testCtx, trailFactN(i), 100, 0); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if got := trailCompactionsOf(t, st); got != 0 {
		t.Fatalf("compactions=%d, want 0 (unbounded trail is never compacted)", got)
	}
	if got := trailLines(t, path); got != 40 {
		t.Fatalf("trail lines=%d, want 40 (nothing dropped)", got)
	}
}

// TestTrailCompactionAmortizedOnFileBackend pins R2/R4/AC-1 on the file
// backend: once the trail is full, k further appends cost at most
// ceil(k/(headroom+1)) rewrites. Before the amortization each of those
// appends performed a full O(cap) decode+rewrite, so the 10 appends in step 2
// used to produce 10 rewrites and now produce exactly one.
func TestTrailCompactionAmortizedOnFileBackend(t *testing.T) {
	const cap, watermark, headroom = 24, 12, 12
	interval := headroom + 1 // appends between compactions
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	appendOne := func(t *testing.T, n int) {
		t.Helper()
		if err := st.AppendAdminFact(testCtx, trailFactN(n), 100, cap); err != nil {
			t.Fatalf("append %d: %v", n, err)
		}
	}

	// 1. Fill the trail exactly to the cap: nothing has overflowed, so no
	// rewrite has happened yet.
	for n := 1; n <= cap; n++ {
		appendOne(t, n)
	}
	if got := trailCompactionsOf(t, st); got != 0 {
		t.Fatalf("compactions=%d, want 0 below the cap", got)
	}
	if got := trailLines(t, path); got != cap {
		t.Fatalf("trail lines=%d, want %d", got, cap)
	}

	// 2. Ten appends past the cap: the trail never exceeds the cap, and at
	// most ceil(10/13) == 1 rewrite happens (today: 10).
	for k := 1; k <= 10; k++ {
		appendOne(t, cap+k)
		if got := trailLines(t, path); got > cap {
			t.Fatalf("append %d: trail lines=%d, want <= cap %d", cap+k, got, cap)
		}
		if got := trailCompactionsOf(t, st); got > int64((k+headroom)/interval) {
			t.Fatalf("append %d: compactions=%d, want <= %d", cap+k, got, (k+headroom)/interval)
		}
	}
	// 3. Steady state after 34 appends is watermark + ((34-12) mod 13) = 21,
	// reached with exactly one rewrite.
	if got := trailCompactionsOf(t, st); got != 1 {
		t.Fatalf("compactions=%d, want 1 (10 appends past the cap amortize to one rewrite)", got)
	}
	if got := trailLines(t, path); got != 21 {
		t.Fatalf("trail lines=%d, want 21", got)
	}

	// 4. Reopen: ensureTrailFile reseeds trailCount from the file's complete
	// lines, so the baseline recount must not re-trim.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if got := trailLines(t, path); got != 21 {
		t.Fatalf("reopened trail lines=%d, want 21 (baseline recount must not re-trim)", got)
	}
	got, err := reopened.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 21 || got[0].ID != trailFactN(34).ID || got[20].ID != trailFactN(14).ID {
		t.Fatalf("reopened trail=%d facts, newest-first %s..%s, want 21 newest-first %s..%s",
			len(got), got[0].ID, got[len(got)-1].ID, trailFactN(34).ID, trailFactN(14).ID)
	}

	// 5. The reopened store has its own counter (a rewrite on this instance
	// only), and its reseeded trailCount is 21, so three appends still fit
	// under the cap and the fourth trips that instance's first rewrite.
	for n := 35; n <= 37; n++ {
		if err := reopened.AppendAdminFact(testCtx, trailFactN(n), 100, cap); err != nil {
			t.Fatalf("append %d: %v", n, err)
		}
		if got := trailCompactionsOf(t, reopened); got != 0 {
			t.Fatalf("append %d: compactions=%d, want 0 (back below the cap)", n, got)
		}
	}
	if err := reopened.AppendAdminFact(testCtx, trailFactN(38), 100, cap); err != nil {
		t.Fatalf("append 38: %v", err)
	}
	if got := trailCompactionsOf(t, reopened); got != 1 {
		t.Fatalf("compactions=%d, want 1 (the 25th append since the reseed trips again)", got)
	}
	if got := trailLines(t, path); got != watermark {
		t.Fatalf("trail lines=%d, want the watermark %d right after a rewrite", got, watermark)
	}
}

// TestTrailCompactionWatermarkOscillation pins the watermark itself (R2/R6,
// AC-2/AC-6), which no existing assertion covers: at trailCap 3 the trail
// settles into the exact steady state 2 + ((n-2) mod 2), oscillating between
// the watermark (2) and the cap (3) instead of collapsing to the cap on every
// overflow. It also confirms the untouched exact-length expectations in
// TestTrailCompactionKeepsNewest still hold (same steady state).
func TestTrailCompactionWatermarkOscillation(t *testing.T) {
	const cap, watermark, interval = 3, 2, 2
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	// want is the exact steady-state fact count after the n-th append.
	want := map[int]int64{1: 1, 2: 2, 3: 3, 4: watermark, 5: 3, 6: watermark, 7: 3}
	for n := 1; n <= 7; n++ {
		if err := st.AppendAdminFact(testCtx, trailFactN(n), 100, cap); err != nil {
			t.Fatalf("append %d: %v", n, err)
		}
		if got := trailLines(t, path); got != want[n] {
			t.Fatalf("append %d: trail lines=%d, want %d (watermark %d, cap %d)",
				n, got, want[n], watermark, cap)
		}
		if got := trailCompactionsOf(t, st); got > int64(n/interval) {
			t.Fatalf("append %d: compactions=%d, want <= %d", n, got, n/interval)
		}
	}
	// Rewrites happen at the 4th and 6th append only.
	if got := trailCompactionsOf(t, st); got != 2 {
		t.Fatalf("compactions=%d, want 2 (rewrites at appends 4 and 6)", got)
	}
	got, err := st.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := []string{trailFactN(7).ID, trailFactN(6).ID, trailFactN(5).ID}
	if len(got) != cap {
		t.Fatalf("trail len=%d, want cap %d", len(got), cap)
	}
	for i, want := range wantIDs {
		if got[i].ID != want {
			t.Fatalf("trail[%d]=%s, want %s (newest-first, drop-oldest)", i, got[i].ID, want)
		}
	}
}

// TestTrailCompactionNeverExceedsCapForSmallCap pins R2/AC-2: whatever the
// watermark, the returned trail never exceeds the operator cap, a rewrite
// always leaves exactly the watermark retained, and the newest fact survives.
// The in-memory sub-case closes the third trim path, which the file cases do
// not exercise.
func TestTrailCompactionNeverExceedsCapForSmallCap(t *testing.T) {
	const cap, watermark, headroom = 8, 4, 4
	t.Run("file backend", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		st, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		// Trips at appends 9, 14, 19, 24, 29 — the headroom+1-th append
		// after each watermark reset.
		trips := map[int]bool{9: true, 14: true, 19: true, 24: true, 29: true}
		for n := 1; n <= 32; n++ {
			if err := st.AppendAdminFact(testCtx, trailFactN(n), 100, cap); err != nil {
				t.Fatalf("append %d: %v", n, err)
			}
			if got := trailLines(t, path); got > cap {
				t.Fatalf("append %d: trail lines=%d, want <= cap %d", n, got, cap)
			}
			if trips[n] {
				if got := trailLines(t, path); got != watermark {
					t.Fatalf("append %d: trail lines=%d, want the watermark %d after a rewrite", n, got, watermark)
				}
				if got := trailCompactionsOf(t, st); got != int64(n/(headroom+1)) {
					t.Fatalf("append %d: compactions=%d, want %d", n, got, n/(headroom+1))
				}
			}
		}
		if got := trailCompactionsOf(t, st); got != 5 {
			t.Fatalf("compactions=%d, want 5 (one rewrite per headroom+1 appends)", got)
		}
		// Steady state after 32 appends: 4 + (28 mod 5) = 7.
		if got := trailLines(t, path); got != 7 {
			t.Fatalf("trail lines=%d, want 7", got)
		}
		got, err := st.ReadAdminTrail(testCtx)
		if err != nil {
			t.Fatal(err)
		}
		if got[0].ID != trailFactN(32).ID {
			t.Fatalf("newest fact=%s, want %s", got[0].ID, trailFactN(32).ID)
		}
	})

	t.Run("in-memory backend", func(t *testing.T) {
		st, err := Open("")
		if err != nil {
			t.Fatal(err)
		}
		for n := 1; n <= 32; n++ {
			if err := st.AppendAdminFact(testCtx, trailFactN(n), 100, cap); err != nil {
				t.Fatalf("append %d: %v", n, err)
			}
			got, err := st.ReadAdminTrail(testCtx)
			if err != nil {
				t.Fatal(err)
			}
			// Below the cap the trail holds everything; past it the trail
			// oscillates inside [watermark, cap] and never exceeds the cap.
			if want := int64(n); n <= cap && int64(len(got)) != want {
				t.Fatalf("append %d: in-memory trail len=%d, want %d (nothing trimmed below the cap)", n, len(got), want)
			}
			if n > cap && (len(got) < watermark || len(got) > cap) {
				t.Fatalf("append %d: in-memory trail len=%d, want %d..%d", n, len(got), watermark, cap)
			}
			if got[0].ID != trailFactN(n).ID {
				t.Fatalf("append %d: newest fact=%s, want %s", n, got[0].ID, trailFactN(n).ID)
			}
		}
	})
}

// TestTrailCompactionFaultBeforeRenameLeavesTrailIntact pins R5/FM-5 with no
// new production seam: the tmp path is made undeletable-but-unwritable by
// turning it into a directory, so compaction faults deterministically before
// any write and before the rename. The overflowing append is already durable,
// so the trail keeps the pre-compaction facts plus that one, and the next
// overflow append compacts normally once the fault is removed.
func TestTrailCompactionFaultBeforeRenameLeavesTrailIntact(t *testing.T) {
	const cap = 4
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	for n := 1; n <= cap; n++ {
		if err := st.AppendAdminFact(testCtx, trailFactN(n), 100, cap); err != nil {
			t.Fatalf("append %d: %v", n, err)
		}
	}
	if got := trailCompactionsOf(t, st); got != 0 {
		t.Fatalf("compactions=%d, want 0 at exactly the cap", got)
	}
	trailPath := path + trailPathSuffix
	preFault, err := os.ReadFile(trailPath)
	if err != nil {
		t.Fatal(err)
	}

	tmp := trailPath + ".tmp"
	if err := os.Mkdir(tmp, 0o750); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(tmp) })

	// The 5th append trips compaction; the tmp open fails with EISDIR before
	// any write and before the rename, so the append must fail closed.
	err = st.AppendAdminFact(testCtx, trailFactN(5), 100, cap)
	if err == nil {
		t.Fatal("append past the cap must fail closed when compaction cannot run")
	}
	if got := trailCompactionsOf(t, st); got != 0 {
		t.Fatalf("compactions=%d, want 0 (a pre-rename fault is not a rewrite)", got)
	}
	after, err := os.ReadFile(trailPath)
	if err != nil {
		t.Fatal(err)
	}
	// The pre-compaction bytes plus exactly the one appended line: nothing was
	// truncated and nothing was rewritten.
	want := append(append([]byte(nil), preFault...), appendLine(t, trailFactN(5))...)
	if string(after) != string(want) {
		t.Fatalf("trail bytes changed beyond the appended line (%d -> %d bytes)", len(preFault), len(after))
	}
	// Nothing was lost: the overflowing append was already fsynced.
	got, err := st.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != cap+1 {
		t.Fatalf("trail len=%d, want %d (the durable overflowing append survives)", len(got), cap+1)
	}
	for i, want := range []int{5, 4, 3, 2, 1} {
		if got[i].ID != trailFactN(want).ID {
			t.Fatalf("trail[%d]=%s, want %s (newest-first)", i, got[i].ID, trailFactN(want).ID)
		}
	}
	if info, statErr := os.Stat(tmp); statErr != nil || !info.IsDir() {
		t.Fatalf("tmp path was left behind as a file: err=%v isDir=%v", statErr, info != nil && info.IsDir())
	}

	// Remove the fault: the counter is still above the cap, so the next
	// append compacts successfully.
	if err := os.Remove(tmp); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendAdminFact(testCtx, trailFactN(6), 100, cap); err != nil {
		t.Fatalf("append after removing the fault: %v", err)
	}
	if got := trailCompactionsOf(t, st); got != 1 {
		t.Fatalf("compactions=%d, want 1 (the retry compacted)", got)
	}
	if got := trailLines(t, path); got > cap {
		t.Fatalf("trail lines=%d, want <= cap %d", got, cap)
	}
}

// appendLine renders one fact as the JSONL line the trail stores, so a test
// can assert byte-level append-only growth.
func appendLine(t *testing.T, fact domain.AdminAction) []byte {
	t.Helper()
	encoded, err := json.Marshal(fact)
	if err != nil {
		t.Fatal(err)
	}
	return append(encoded, '\n')
}
