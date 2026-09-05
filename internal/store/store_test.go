package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestFileBackendPersistAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Update(func(data *Snapshot) error {
		data.Tenants["demo"] = domain.Tenant{ID: "demo", Name: "Demo", Active: true}
		data.AdminActions = append(data.AdminActions, domain.AdminAction{ID: "a1", Actor: "test", Action: "tenant.created", TargetType: "tenant", TargetID: "demo"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("state file not persisted: %v", err)
	}

	// 新实例必须从磁盘恢复（先 Close 释放 <path>.lock 上的排他 flock；
	// 真实重启时内核会在进程退出时自动释放）。
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var tenant domain.Tenant
	if err := second.Read(func(data *Snapshot) error {
		var ok bool
		tenant, ok = data.Tenants["demo"]
		if !ok {
			t.Fatal("tenant missing after reload")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if tenant.Name != "Demo" {
		t.Fatalf("tenant name=%q", tenant.Name)
	}
	if len(second.MustSnapshot().AdminActions) != 1 {
		t.Fatal("admin actions not restored")
	}
}

func TestEventCanonicalIdentitySurvivesFileReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var nilMap map[string]any
	var nilSlice []any
	event := domain.Event{EventID: "canonical-reload", TenantID: "tenant-a", OccurredAt: time.Unix(10, 0).UTC(), ChangedFields: map[string]domain.FieldChange{
		"amount":      {Before: json.Number("9007199254740992.1"), After: json.Number("9007199254740992.2")},
		"collections": {Before: map[string]any{}, After: []any{}},
		"nulls":       {Before: nil, After: nil},
	}, Payload: map[string]any{"nil_map": nilMap, "nil_slice": nilSlice, "empty_map": map[string]any{}, "empty_slice": []any{}, "amount": json.Number("9007199254740992.1")}}
	if err := st.Update(func(data *Snapshot) error {
		data.Events[EventKey(event.TenantID, event.EventID)] = event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var beforeBytes []byte
	var beforeDigest string
	if err := st.Read(func(data *Snapshot) error {
		stored := data.Events[EventKey(event.TenantID, event.EventID)]
		beforeBytes, err = domain.CanonicalJSON(stored)
		if err != nil {
			return err
		}
		beforeDigest, err = domain.EventContentDigest(stored)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Read(func(data *Snapshot) error {
		stored := data.Events[EventKey(event.TenantID, event.EventID)]
		afterBytes, err := domain.CanonicalJSON(stored)
		if err != nil {
			return err
		}
		if string(afterBytes) != string(beforeBytes) {
			t.Fatalf("canonical bytes changed across reload: %s != %s", afterBytes, beforeBytes)
		}
		afterDigest, err := domain.EventContentDigest(stored)
		if err != nil {
			return err
		}
		if afterDigest != beforeDigest {
			t.Fatalf("content digest changed across reload: %s != %s", afterDigest, beforeDigest)
		}
		if got := stored.Payload["amount"]; got != json.Number("9007199254740992.1") {
			t.Fatalf("precise number after reload = %#v, want exact json.Number", got)
		}
		changes := stored.ChangedFields
		if got := changes["amount"].Before; got != json.Number("9007199254740992.1") {
			t.Fatalf("changed-field precise number after reload = %#v", got)
		}
		if got := changes["amount"].After; got != json.Number("9007199254740992.2") {
			t.Fatalf("changed-field precise number after reload = %#v", got)
		}
		if got, ok := changes["collections"].Before.(map[string]any); !ok || got == nil {
			t.Fatalf("empty changed-field map lost its identity: %#v", changes["collections"].Before)
		}
		if got, ok := changes["collections"].After.([]any); !ok || got == nil {
			t.Fatalf("empty changed-field slice lost its identity: %#v", changes["collections"].After)
		}
		if _, ok := changes["nulls"]; !ok {
			t.Fatalf("changed-field entry itself was lost across reload")
		}
		if _, ok := stored.Payload["nil_map"]; !ok || stored.Payload["nil_map"] != nil {
			t.Fatalf("typed-nil map must reload as JSON null: %#v", stored.Payload["nil_map"])
		}
		if _, ok := stored.Payload["nil_slice"]; !ok || stored.Payload["nil_slice"] != nil {
			t.Fatalf("typed-nil slice must reload as JSON null: %#v", stored.Payload["nil_slice"])
		}
		if _, ok := stored.Payload["empty_map"].(map[string]any); !ok || stored.Payload["empty_map"] == nil {
			t.Fatalf("empty map lost its collection identity: %#v", stored.Payload["empty_map"])
		}
		if _, ok := stored.Payload["empty_slice"].([]any); !ok || stored.Payload["empty_slice"] == nil {
			t.Fatalf("empty slice lost its collection identity: %#v", stored.Payload["empty_slice"])
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLedgeredOutboxPersistsAndLegacySnapshotsNormalize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{EventID: "outbox-event-1", TenantID: "tenant-a", Sequence: 7, Hash: "hash-7"}
	if err := first.Update(func(data *Snapshot) error {
		data.LedgeredOutbox[EventKey(event.TenantID, event.EventID)] = event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reloaded, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := reloaded.Read(func(data *Snapshot) error {
		if got := data.LedgeredOutbox[EventKey(event.TenantID, event.EventID)]; got.EventID != event.EventID || got.Hash != event.Hash {
			t.Fatalf("outbox event did not round-trip: %+v", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := reloaded.Close(); err != nil {
		t.Fatal(err)
	}

	legacyPath := filepath.Join(t.TempDir(), "legacy.json")
	if err := os.WriteFile(legacyPath, []byte(`{"tenants":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	legacy, err := Open(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	if err := legacy.Read(func(data *Snapshot) error {
		if data.LedgeredOutbox == nil {
			t.Fatal("legacy snapshot outbox was not normalized")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFileBackendUpdateRollbackOnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Update(func(data *Snapshot) error {
		data.Tenants["keep"] = domain.Tenant{ID: "keep", Name: "Keep", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// 闭包报错时，内存与磁盘都必须保持原状。
	err = st.Update(func(data *Snapshot) error {
		data.Tenants["ghost"] = domain.Tenant{ID: "ghost", Name: "Ghost", Active: true}
		return errors.New("boom")
	})
	if err == nil {
		t.Fatal("expected update error")
	}
	if err := st.Read(func(data *Snapshot) error {
		if _, ok := data.Tenants["ghost"]; ok {
			t.Fatal("failed update leaked into snapshot")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// 重开后 ghost 也不在磁盘上（先 Close 释放排他 flock）。
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Read(func(data *Snapshot) error {
		if _, ok := data.Tenants["ghost"]; ok {
			t.Fatal("failed update leaked to disk")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryStoreNeverPersists(t *testing.T) {
	st, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Update(func(data *Snapshot) error {
		data.Tenants["mem"] = domain.Tenant{ID: "mem", Name: "Mem", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	// 内存模式不写任何文件（路径为空时 Save 为 no-op）。
	reopened, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Read(func(data *Snapshot) error {
		if len(data.Tenants) != 0 {
			t.Fatal("memory store leaked between instances")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotDeadLettersRoundTrip pins the persisted dead-letter state
// (FM-1): entries survive a save/reload cycle, and a legacy snapshot without
// the field normalizes to an empty map (no migration).
func TestSnapshotDeadLettersRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	dead := domain.DeadLetter{TenantID: "tenant-a", EventID: "evt-x", StreamID: "tenant-a:aggregate:invoice:inv-1", Sequence: 4, Reason: "archive_key_too_long", ErrorMessage: "archive key exceeds the destination length limit", At: time.Unix(1_700_000_000, 0).UTC()}
	if err := st.Update(func(data *Snapshot) error {
		data.DeadLetters[EventKey("tenant-a", "evt-x")] = dead
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil { // release the state-file lock before reopening
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Read(func(data *Snapshot) error {
		got, ok := data.DeadLetters[EventKey("tenant-a", "evt-x")]
		if !ok || got.Reason != dead.Reason || got.ErrorMessage != dead.ErrorMessage || !got.At.Equal(dead.At) {
			t.Fatalf("dead letter after reload = %+v, want %+v", got, dead)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// A legacy snapshot without the field normalizes to an empty map.
	legacy := filepath.Join(t.TempDir(), "legacy.json")
	if err := os.WriteFile(legacy, []byte(`{"tenants":{}}`), 0o640); err != nil {
		t.Fatal(err)
	}
	legacyStore, err := Open(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyStore.Read(func(data *Snapshot) error {
		if data.DeadLetters == nil {
			t.Fatal("normalize did not fill DeadLetters")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotNormalizeLegacyJSON(t *testing.T) {
	// 旧版本快照缺少新字段（admin_actions、restore_runs 等），读取后必须可用。
	legacy := `{"tenants":{"demo":{"id":"demo"}},"events":null,"admin_actions":null}`
	path := filepath.Join(t.TempDir(), "legacy.json")
	if err := os.WriteFile(path, []byte(legacy), 0o640); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Read(func(data *Snapshot) error {
		if data.AdminActions == nil || data.RestoreRuns == nil || data.Streams == nil {
			t.Fatal("normalize did not fill missing collections")
		}
		if _, ok := data.Tenants["demo"]; !ok {
			t.Fatal("legacy tenant lost")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCloneSnapshotIsDeep(t *testing.T) {
	data := NewSnapshot()
	data.Tenants["demo"] = domain.Tenant{ID: "demo", Name: "Demo", Active: true}
	clone, err := cloneSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	clone.Tenants["demo"] = domain.Tenant{ID: "demo", Name: "Mutated", Active: false}
	if data.Tenants["demo"].Name != "Demo" {
		t.Fatal("clone shares tenant map with source")
	}
}

func TestKeySeparatorRoundTrip(t *testing.T) {
	if TenantKey("t") != "t" {
		t.Fatal("tenant key must be identity")
	}
	for _, key := range []string{
		SourceKey("t", "s"),
		SchemaKey("t", "s", 1),
		EventKey("t", "e"),
		StreamKey("t", "stream"),
	} {
		if !strings.Contains(key, KeySeparator) {
			t.Fatalf("key %q missing separator", key)
		}
		if strings.Contains(key, "\x00") {
			t.Fatalf("key %q must not contain NUL (jsonb rejects \\u0000)", key)
		}
	}
}

func (s *Store) MustSnapshot() *Snapshot {
	snapshot, err := s.Snapshot()
	if err != nil {
		panic(err)
	}
	return snapshot
}

func TestCheckFileToPostgresMigrationHazard(t *testing.T) {
	dir := t.TempDir()

	// Missing file: no hazard (fresh deployment).
	if err := CheckFileToPostgresMigrationHazard(filepath.Join(dir, "nope.json")); err != nil {
		t.Fatalf("missing file: %v", err)
	}
	// Empty file: no hazard.
	empty := filepath.Join(dir, "empty.json")
	if err := os.WriteFile(empty, []byte(""), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CheckFileToPostgresMigrationHazard(empty); err != nil {
		t.Fatalf("empty file: %v", err)
	}
	// Empty ledger snapshot: no hazard.
	blank := filepath.Join(dir, "blank.json")
	encoded, err := json.Marshal(NewSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blank, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CheckFileToPostgresMigrationHazard(blank); err != nil {
		t.Fatalf("blank snapshot: %v", err)
	}
	// Ledger with events: hazard reported.
	withEvents := filepath.Join(dir, "events.json")
	snapshot := NewSnapshot()
	snapshot.Events["t\x1fe"] = domain.Event{EventID: "e", TenantID: "t"}
	encoded, err = json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(withEvents, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	err = CheckFileToPostgresMigrationHazard(withEvents)
	if err == nil {
		t.Fatal("expected migration hazard for non-empty ledger, got nil")
	}
	if !strings.Contains(err.Error(), "AUDIT_ALLOW_PG_EMPTY_LEDGER") {
		t.Fatalf("error must name the opt-out flag: %v", err)
	}
	// An archived-and-evicted event still has a receipt and therefore remains
	// ledger evidence. The migration guard must not mistake zero hot payloads
	// for an empty ledger.
	evicted := filepath.Join(dir, "evicted.json")
	archived := NewSnapshot()
	archived.Receipts[EventKey("t", "e")] = domain.EventReceipt{EventID: "e", TenantID: "t", Status: domain.StatusArchived}
	encoded, err = json.Marshal(archived)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(evicted, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CheckFileToPostgresMigrationHazard(evicted); err == nil {
		t.Fatal("expected migration hazard for archived receipt without hot event")
	}
	// A v2 control file is intentionally empty of receipts/events; its
	// sibling tenant/ledger files are the migration evidence instead.
	v2 := filepath.Join(dir, "control.json")
	control := NewSnapshot()
	control.LayoutVersion = hotColdLayoutVersion
	encoded, err = json.Marshal(control)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(v2, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	ledgerPath := filepath.Join(dir, "ledger", "tenant-a.jsonl")
	if err := os.MkdirAll(filepath.Dir(ledgerPath), 0o750); err != nil {
		t.Fatal(err)
	}
	record := LedgerRecord{TenantID: "tenant-a", RecordType: LedgerReceipt, Key: EventKey("tenant-a", "evt-1"), Version: 1, Receipt: &domain.EventReceipt{EventID: "evt-1", TenantID: "tenant-a", Status: domain.StatusArchived}}
	line, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ledgerPath, append(line, '\n'), 0o640); err != nil {
		t.Fatal(err)
	}
	err = CheckFileToPostgresMigrationHazard(v2)
	if err == nil || !strings.Contains(err.Error(), "cold ledger records") {
		t.Fatalf("expected v2 ledger migration hazard, got %v", err)
	}
}

// syncRecorder records every directory sync performed through the
// fileBackend seam and can fault-inject failures. Because Save's chain sync
// runs after the rename and before f.data is replaced, the recorded sync log
// is also a publish log: every entry provably happened after the rename.
type syncRecorder struct {
	mu       sync.Mutex
	syncs    []string
	fail     map[string]bool // paths whose sync must fail
	failN    int             // number of sync calls to fail, then succeed
	failed   int
	sentinel error // when set, injected failures wrap it so errors.Is reaches it
}

func (r *syncRecorder) record(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.syncs = append(r.syncs, path)
	if r.fail != nil && r.fail[path] {
		return r.injected(path)
	}
	if r.failN > 0 && r.failed < r.failN {
		r.failed++
		return r.injected(path)
	}
	return nil
}

func (r *syncRecorder) injected(path string) error {
	if r.sentinel != nil {
		return fmt.Errorf("injected sync failure for %s: %w", path, r.sentinel)
	}
	return fmt.Errorf("injected sync failure for %s", path)
}

// TestFileBackendSaveSyncsParentDirectoryChain is AC-1: a successful Save
// fsyncs every directory from the renamed entry's immediate parent up to the
// deepest pre-existing ancestor (the snapshot has no configured root, unlike
// archive's FileStore.Dir), leaf-to-root, before returning nil — and only
// the directories this Save actually created or touched.
func TestFileBackendSaveSyncsParentDirectoryChain(t *testing.T) {
	t.Run("pre-existing parent collapses to one sync", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		rec := &syncRecorder{}
		f := &fileBackend{data: NewSnapshot(), path: path, syncDir: rec.record}
		if err := f.Save(NewSnapshot()); err != nil {
			t.Fatal(err)
		}
		want := []string{filepath.Dir(path)}
		if !reflect.DeepEqual(rec.syncs, want) {
			t.Fatalf("sync order = %v, want %v", rec.syncs, want)
		}
	})

	t.Run("freshly created nested parent syncs leaf-to-root", func(t *testing.T) {
		base := t.TempDir()
		path := filepath.Join(base, "ledger", "state.json")
		rec := &syncRecorder{}
		f := &fileBackend{data: NewSnapshot(), path: path, syncDir: rec.record}
		if err := f.Save(NewSnapshot()); err != nil {
			t.Fatal(err)
		}
		want := []string{filepath.Join(base, "ledger"), base}
		if !reflect.DeepEqual(rec.syncs, want) {
			t.Fatalf("sync order = %v, want %v", rec.syncs, want)
		}
	})

	t.Run("second save collapses to the existing parent", func(t *testing.T) {
		base := t.TempDir()
		path := filepath.Join(base, "ledger", "state.json")
		rec := &syncRecorder{}
		f := &fileBackend{data: NewSnapshot(), path: path, syncDir: rec.record}
		if err := f.Save(NewSnapshot()); err != nil {
			t.Fatal(err)
		}
		rec.syncs = nil
		if err := f.Save(NewSnapshot()); err != nil {
			t.Fatal(err)
		}
		want := []string{filepath.Join(base, "ledger")}
		if !reflect.DeepEqual(rec.syncs, want) {
			t.Fatalf("sync order = %v, want %v", rec.syncs, want)
		}
	})

	t.Run("chain sync runs after rename and publishes committed content", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ledger", "state.json")
		rec := &syncRecorder{}
		renamedAtSync := false
		hook := func(p string) error {
			if _, err := os.Stat(path); err == nil {
				renamedAtSync = true
			}
			return rec.record(p)
		}
		f := &fileBackend{data: NewSnapshot(), path: path, syncDir: hook}
		data := NewSnapshot()
		data.Tenants["demo"] = domain.Tenant{ID: "demo", Name: "Demo", Active: true}
		if err := f.Save(data); err != nil {
			t.Fatal(err)
		}
		if !renamedAtSync {
			t.Fatal("chain sync did not run after the rename")
		}
		// The in-memory authority is the committed snapshot.
		loaded, err := f.Load()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := loaded.Tenants["demo"]; !ok {
			t.Fatal("committed tenant missing from in-memory snapshot")
		}
		// The durable rename published exactly the committed snapshot.
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var disk Snapshot
		if err := decodeSnapshot(raw, &disk); err != nil {
			t.Fatalf("target file does not decode: %v", err)
		}
		if _, ok := disk.Tenants["demo"]; !ok {
			t.Fatal("committed tenant missing from the published target file")
		}
	})
}

// TestFileBackendSaveDirSyncFailureKeepsPreviousAuthoritative is AC-2: a
// post-rename chain-sync failure fails Save, keeps the previously persisted
// snapshot authoritative in memory AND on disk (best-effort restore, "no
// rename visible" to a fresh Open), and leaves no .tmp behind.
func TestFileBackendSaveDirSyncFailureKeepsPreviousAuthoritative(t *testing.T) {
	sentinel := errors.New("injected sync failure sentinel")
	path := filepath.Join(t.TempDir(), "ledger", "state.json")
	rec := &syncRecorder{sentinel: sentinel}
	f := &fileBackend{data: NewSnapshot(), path: path, syncDir: rec.record}

	seed := NewSnapshot()
	seed.Tenants["keep"] = domain.Tenant{ID: "keep", Name: "Keep", Active: true}
	if err := f.Save(seed); err != nil {
		t.Fatal(err)
	}
	// Non-vacuity: the seed recorded the leaf (immediate parent) first.
	if len(rec.syncs) == 0 || rec.syncs[0] != filepath.Dir(path) {
		t.Fatalf("seed save must sync the immediate parent first, got %v", rec.syncs)
	}

	rec.failN = 1
	mutated := NewSnapshot()
	mutated.Tenants["keep"] = domain.Tenant{ID: "keep", Name: "Keep", Active: true}
	mutated.Tenants["ghost"] = domain.Tenant{ID: "ghost", Name: "Ghost", Active: true}
	err := f.Save(mutated)
	if err == nil {
		t.Fatal("Save must fail when the parent-directory chain cannot be synced")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error %v must wrap the injected sentinel", err)
	}
	if !strings.Contains(err.Error(), "sync directory chain for") {
		t.Fatalf("error %v must name the chain-sync step", err)
	}
	// In-memory authority: the previous snapshot stays authoritative.
	loaded, err := f.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := loaded.Tenants["keep"]; !ok {
		t.Fatal("previous snapshot lost from in-memory authority")
	}
	if _, ok := loaded.Tenants["ghost"]; ok {
		t.Fatal("failed save leaked into in-memory authority")
	}
	// "No rename visible": a fresh Open loads the previously persisted
	// snapshot (the restore rewrote it over the target).
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Read(func(data *Snapshot) error {
		if _, ok := data.Tenants["keep"]; !ok {
			t.Fatal("previous snapshot lost on disk after failed save")
		}
		if _, ok := data.Tenants["ghost"]; ok {
			t.Fatal("failed save leaked to disk")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Temp hygiene: no .tmp remains after the failure path.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf(".tmp left behind after failed save: %v", err)
	}
	// The parent-directory sync was attempted (the recorded attempt is what
	// distinguishes the restore path from a pre-fix save that would leave
	// the new content at the target).
	if len(rec.syncs) <= 0 {
		t.Fatalf("chain sync must have been attempted, got %v", rec.syncs)
	}
}

// crashDrop models a power failure after a successful Save: a directory's
// dentry survives only if it was synced after its child was created. Because
// Save's chain sync runs after the rename, "recorded as synced" implies
// "after the rename". The renamed state file (whose dentry lives in the
// first chain member) survives iff every member from the immediate parent up
// to the chain root was synced; any unsynced member loses its whole subtree.
func crashDrop(recorded, chain []string) []string {
	set := make(map[string]bool, len(recorded))
	for _, p := range recorded {
		set[p] = true
	}
	var dropped []string
	for _, d := range chain {
		if !set[d] {
			dropped = append(dropped, d)
		}
	}
	return dropped
}

// TestFileBackendSaveSurvivesSimulatedCrash is AC-3: the deterministic replay
// model (no privileges, no tmpfs mount) shows the full chain is required for
// the committed snapshot to survive a power failure, and that the model is
// non-vacuous — the pre-fix no-sync save and a root-only sync both
// demonstrably lose the ledger subtree.
func TestFileBackendSaveSurvivesSimulatedCrash(t *testing.T) {
	commit := func(t *testing.T, path string, rec *syncRecorder) {
		t.Helper()
		f := &fileBackend{data: NewSnapshot(), path: path, syncDir: rec.record}
		data := NewSnapshot()
		data.Events[EventKey("t", "evt-1")] = domain.Event{TenantID: "t", EventID: "evt-1"}
		if err := f.Save(data); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("full chain survives", func(t *testing.T) {
		base := t.TempDir()
		path := filepath.Join(base, "ledger", "state.json")
		rec := &syncRecorder{}
		commit(t, path, rec)
		chain := []string{filepath.Join(base, "ledger"), base}
		if dropped := crashDrop(rec.syncs, chain); len(dropped) != 0 {
			t.Fatalf("full chain must survive the crash, dropped %v", dropped)
		}
		reopened, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := reopened.Read(func(data *Snapshot) error {
			if _, ok := data.Events[EventKey("t", "evt-1")]; !ok {
				t.Fatal("committed event lost after simulated crash")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("pre-fix no sync loses the ledger (non-vacuous)", func(t *testing.T) {
		base := t.TempDir()
		path := filepath.Join(base, "ledger", "state.json")
		rec := &syncRecorder{}
		commit(t, path, rec)
		chain := []string{filepath.Join(base, "ledger"), base}
		// Pre-fix behavior recorded no directory syncs at all.
		dropped := crashDrop(nil, chain)
		if len(dropped) != 2 {
			t.Fatalf("no-sync save must drop both chain members, got %v", dropped)
		}
		for _, d := range dropped {
			_ = os.RemoveAll(d)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("model must drop the state file when nothing was synced")
		}
		reopened, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := reopened.Read(func(data *Snapshot) error {
			if len(data.Events) != 0 {
				t.Fatal("committed event must be gone after the simulated crash")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("root-only sync still loses the leaf (non-vacuous)", func(t *testing.T) {
		base := t.TempDir()
		path := filepath.Join(base, "ledger", "state.json")
		rec := &syncRecorder{}
		commit(t, path, rec)
		chain := []string{filepath.Join(base, "ledger"), base}
		// Only the chain root (deepest pre-existing ancestor) was synced:
		// the freshly created intermediate ledger/ was not, so its whole
		// subtree — including the state file — vanishes on power failure.
		dropped := crashDrop([]string{base}, chain)
		if !reflect.DeepEqual(dropped, []string{filepath.Join(base, "ledger")}) {
			t.Fatalf("root-only sync must drop the unsynced intermediate, got %v", dropped)
		}
		for _, d := range dropped {
			_ = os.RemoveAll(d)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("leaf sync is required: state file must be gone when the intermediate was not synced")
		}
	})
}
