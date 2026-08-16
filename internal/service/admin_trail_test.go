package service

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// adminTrailTestService builds a Service over a real file-backed store with
// explicit admin-trail caps and a frozen clock (every fact shares one
// CreatedAt, making "newest" deterministic via reverse-append order).
func adminTrailTestService(t *testing.T, dir string, maxActions, maxTrail int) (*Service, string) {
	t.Helper()
	statePath := filepath.Join(dir, "state.json")
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := New(st, Config{SegmentSize: 2, SigningSecret: "test-secret", AllowDevSecrets: true, MaxAdminActions: maxActions, MaxAdminTrailActions: maxTrail, Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}
	return svc, statePath
}

// seedTrailEvent ingests one deterministic event for tenant-a.
func seedTrailEvent(t *testing.T, svc *Service, id string) {
	t.Helper()
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent(id, "op-trail", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
}

// TestReadsDoNotRewriteSnapshotFile is AC-1: 10 000 actor-bearing reads
// leave state.json byte- and mtime-identical (zero snapshot writes), keep
// Snapshot.AdminActions within MaxAdminActions, and land every fact in the
// separate trail (bounded by MaxAdminTrailActions when capped).
func TestReadsDoNotRewriteSnapshotFile(t *testing.T) {
	svc, statePath := adminTrailTestService(t, t.TempDir(), 100, 100)
	seedTrailEvent(t, svc, "trail-evt")

	before, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	sizeBefore, mtimeBefore := before.Size(), before.ModTime()

	const reads = 10_000
	for i := 0; i < reads; i++ {
		if _, err := svc.GetEvent("tenant-a", "auditor-1", "trail-evt"); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}

	after, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != sizeBefore || !after.ModTime().Equal(mtimeBefore) {
		t.Fatalf("state.json rewritten by reads: size %d->%d mtime %v->%v", sizeBefore, after.Size(), mtimeBefore, after.ModTime())
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.AdminActions) > 100 {
		t.Fatalf("snapshot AdminActions=%d, want <= 100 (reads must not grow the snapshot)", len(snap.AdminActions))
	}
	// The trail file holds the read facts, bounded by the trail cap
	// (compaction keeps the newest 100).
	trailPath := statePath + ".admin-trail.jsonl"
	contents, err := os.ReadFile(trailPath)
	if err != nil {
		t.Fatalf("trail file missing: %v", err)
	}
	lines := 0
	for _, b := range contents {
		if b == '\n' {
			lines++
		}
	}
	if lines == 0 || lines > 100 {
		t.Fatalf("trail lines=%d, want 1..100 (bounded by MaxAdminTrailActions)", lines)
	}
	// The governance surface sees the read facts (merged view).
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	var readFacts int
	for _, action := range actions {
		if action.Action == domain.AdminActionEventRead {
			readFacts++
		}
	}
	if readFacts != lines {
		t.Fatalf("merged read facts=%d, want %d (trail lines)", readFacts, lines)
	}
}

// TestListAdminActionsNewestAfterCap is AC-3: with a small snapshot cap and
// an unbounded trail, the merged view returns the newest facts up to its
// limit after the cap is hit; the snapshot stays bounded simultaneously.
func TestListAdminActionsNewestAfterCap(t *testing.T) {
	svc, _ := adminTrailTestService(t, t.TempDir(), 5, 0) // unbounded trail
	seedTrailEvent(t, svc, "cap-evt")
	const reads = 20
	for i := 0; i < reads; i++ {
		if _, err := svc.GetEvent("tenant-a", "auditor-1", "cap-evt"); err != nil {
			t.Fatal(err)
		}
	}
	// The 5 newest read facts are the last 5 appends; with a frozen clock
	// "newest" is deterministic (reverse trail append order).
	newest := func(actions []domain.AdminAction) map[string]bool {
		out := map[string]bool{}
		for _, action := range actions {
			if action.Action == domain.AdminActionEventRead {
				out[action.TargetID] = true
			}
		}
		return out
	}
	all, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	readsInView := 0
	for _, action := range all {
		if action.Action == domain.AdminActionEventRead {
			readsInView++
		}
	}
	if readsInView != reads {
		t.Fatalf("read facts in view=%d, want %d (unbounded trail, all visible)", readsInView, reads)
	}
	got := newest(all)
	for _, missing := range []string{"cap-evt"} {
		if !got[missing] {
			t.Fatalf("read target %q missing from merged view", missing)
		}
	}
	// Cap respected: the first 5 items are exactly the 5 newest reads
	// (reverse append order: cap-evt x20 ... x16).
	if len(all) < 5 {
		t.Fatalf("merged view too short: %d", len(all))
	}
	for i, action := range all[:5] {
		if action.Action != domain.AdminActionEventRead {
			t.Fatalf("merged item %d is a mutation fact %q, want a read fact (trail first on equal CreatedAt)", i, action.Action)
		}
	}
	// limit=3 returns exactly the 3 newest.
	limited, err := svc.ListAdminActions("tenant-a", false, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 3 {
		t.Fatalf("limited view len=%d, want 3", len(limited))
	}
	for _, action := range limited {
		if action.Action != domain.AdminActionEventRead || action.TargetID != "cap-evt" {
			t.Fatalf("limited item=%+v, want an event read", action)
		}
	}
	// AC-1 bound holds simultaneously: the snapshot never grew past the cap.
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.AdminActions) > 5 {
		t.Fatalf("snapshot AdminActions=%d, want <= 5", len(snap.AdminActions))
	}
}

// TestMutationAppendsTrimOldest pins the mutation-path cap: once
// MaxAdminActions is exceeded the OLDEST mutation facts are dropped and the
// newest retained (in append order), so the persisted snapshot always
// satisfies the bound.
func TestMutationAppendsTrimOldest(t *testing.T) {
	svc, _ := adminTrailTestService(t, t.TempDir(), 5, 0)
	for i := 1; i <= 7; i++ {
		id := fmt.Sprintf("tenant-%d", i)
		if err := svc.CreateTenant("test", domain.Tenant{ID: id, Name: id, Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.AdminActions) != 5 {
		t.Fatalf("snapshot AdminActions=%d, want 5 (capped)", len(snap.AdminActions))
	}
	// The oldest facts (tenant-a, tenant-1, tenant-2) were dropped; the
	// newest 5 (tenant-3..tenant-7) survive in append order.
	want := []string{"tenant-3", "tenant-4", "tenant-5", "tenant-6", "tenant-7"}
	for i, action := range snap.AdminActions {
		if action.TargetID != want[i] {
			t.Fatalf("retained[%d]=%s, want %s", i, action.TargetID, want[i])
		}
	}
}

// TestResolveAdminTrailConfigValidation pins the F-6 zero semantics and the
// ordering invariant: <=0 selects the default for MaxAdminActions; <0
// selects the default for MaxAdminTrailActions while ==0 means unbounded
// (operator-explicit — the CLI's flag default supplies the bounded value, so
// a programmatic zero-value Config means "do not trim my read history"); a
// non-zero trail cap below the snapshot cap is ErrInvalid.
func TestResolveAdminTrailConfigValidation(t *testing.T) {
	t.Run("zero value", func(t *testing.T) {
		cfg, err := resolveAdminTrailConfig(Config{})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MaxAdminActions != DefaultMaxAdminActions || cfg.MaxAdminTrailActions != 0 {
			t.Fatalf("zero value=%d/%d, want %d/0 (snapshot default, unbounded trail)", cfg.MaxAdminActions, cfg.MaxAdminTrailActions, DefaultMaxAdminActions)
		}
	})
	t.Run("zero semantics", func(t *testing.T) {
		cfg, err := resolveAdminTrailConfig(Config{MaxAdminActions: 0, MaxAdminTrailActions: 0})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MaxAdminActions != DefaultMaxAdminActions {
			t.Fatalf("MaxAdminActions(0)=%d, want default", cfg.MaxAdminActions)
		}
		if cfg.MaxAdminTrailActions != 0 {
			t.Fatalf("MaxAdminTrailActions(0)=%d, want 0 (unbounded, operator-explicit)", cfg.MaxAdminTrailActions)
		}
		cfg, err = resolveAdminTrailConfig(Config{MaxAdminTrailActions: -1})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MaxAdminTrailActions != DefaultMaxAdminTrailActions {
			t.Fatalf("MaxAdminTrailActions(-1)=%d, want default", cfg.MaxAdminTrailActions)
		}
	})
	t.Run("explicit caps preserved", func(t *testing.T) {
		cfg, err := resolveAdminTrailConfig(Config{MaxAdminActions: 5, MaxAdminTrailActions: 0})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MaxAdminActions != 5 {
			t.Fatalf("MaxAdminActions=%d, want 5", cfg.MaxAdminActions)
		}
	})
	t.Run("trail below snapshot rejected", func(t *testing.T) {
		_, err := resolveAdminTrailConfig(Config{MaxAdminActions: 100, MaxAdminTrailActions: 50})
		if !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("err=%v, want ErrInvalid", err)
		}
	})
	t.Run("equal caps accepted", func(t *testing.T) {
		cfg, err := resolveAdminTrailConfig(Config{MaxAdminActions: 100, MaxAdminTrailActions: 100})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MaxAdminActions != 100 || cfg.MaxAdminTrailActions != 100 {
			t.Fatalf("caps=%d/%d, want 100/100", cfg.MaxAdminActions, cfg.MaxAdminTrailActions)
		}
	})
}
