package store

import (
	"errors"
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
		if err := st.AppendAdminFact(fact, 100, 100); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	got, err := st.ReadAdminTrail()
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
			if err := st.AppendAdminFact(fact, 100, 3); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		got, err := st.ReadAdminTrail()
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
		got2, err := st2.ReadAdminTrail()
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
			if err := st.AppendAdminFact(fact, 100, 3); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		got, err := st.ReadAdminTrail()
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
		if err := st.AppendAdminFact(fact, 100, 100); err != nil {
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
		got, err := st.ReadAdminTrail()
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
		if _, err := st.ReadAdminTrail(); err == nil {
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

func (b *countingTrailBackend) Load() (*Snapshot, error) { return b.snap, nil }
func (b *countingTrailBackend) LoadForUpdate() (*Snapshot, error) {
	return cloneSnapshot(b.snap)
}
func (b *countingTrailBackend) Save(data *Snapshot) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.saves++
	b.snap = data
	return nil
}
func (b *countingTrailBackend) AppendAdminFact(fact domain.AdminAction, _ int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.appendErr != nil {
		return b.appendErr
	}
	b.appends++
	b.trail = append(b.trail, fact)
	return nil
}
func (b *countingTrailBackend) ReadAdminTrail() ([]domain.AdminAction, error) {
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
		if err := st.AppendAdminFact(fact, 100, 100); err != nil {
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
	err := st.AppendAdminFact(trailFact("x"), 100, 100)
	if !errors.Is(err, backend.appendErr) {
		t.Fatalf("append err=%v, want %v", err, backend.appendErr)
	}
	trail, err := st.ReadAdminTrail()
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

func (b *scriptedFallbackBackend) Load() (*Snapshot, error) { return b.data, nil }

func (b *scriptedFallbackBackend) LoadForUpdate() (*Snapshot, error) {
	return cloneSnapshot(b.data)
}

func (b *scriptedFallbackBackend) Save(data *Snapshot) error {
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
	if err := st.AppendAdminFact(fact, 100, 100); err != nil {
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
	err := st.AppendAdminFact(trailFact("fail"), 100, 100)
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
		if err := st.AppendAdminFact(fact, 3, 100); err != nil {
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
			_ = st.AppendAdminFact(trailFact("x"), 100, 100)
		}
		close(done)
	}()
	if err := st.Update(func(data *Snapshot) error {
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
	trail, err := st.ReadAdminTrail()
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
	if err := st.AppendAdminFact(trailFact("pg-1"), 100, 100); err != nil {
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
	got, err := st.ReadAdminTrail()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "pg-1" {
		t.Fatalf("trail=%+v, want [pg-1]", got)
	}
}

// TestPostgresTrailCompactionWatermark pins F-4/FM-7 on PG: past the cap the
// compaction deletes the oldest rows only, keeping the newest cap.
func TestPostgresTrailCompactionWatermark(t *testing.T) {
	db := newPostgresTestDB(t)
	st := seedTenant(t, db)
	for _, fact := range trailFacts(7) {
		if err := st.AppendAdminFact(fact, 100, 3); err != nil {
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
	got, err := st.ReadAdminTrail()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].ID != "g" || got[2].ID != "e" {
		t.Fatalf("trail=%v, want newest 3 reverse (g,f,e)", ids(got))
	}
}
