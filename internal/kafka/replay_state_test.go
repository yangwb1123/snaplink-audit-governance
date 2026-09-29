package kafka

// Tests for the persisted replay mark set: the on-disk replay-log-v1 format,
// crash and torn-tail healing, concurrent and multi-process writers, the
// per-mark cost, and the lock failure path. The rounds that consume these marks
// are in replay_test.go, mirroring internal/kafka/replay_state.go.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Shared dimensions for the AC-1/AC-4 concurrency tests: W writers × K
// distinct IDs plus a shared overlapping core, all on one state path.
const (
	replayConcurrentWriters   = 8
	replayConcurrentPerWriter = 50
	replayConcurrentShared    = 10
)

// AC-1 (REQ-4): the rewrite path (first write / legacy conversion) fsyncs
// the unique temp contents BEFORE the rename and the parent directory AFTER
// the rename; the append path (steady-state log) fsyncs the state file after
// the append — no rename, no dir sync. The recorded-hook proof is
// deterministic (no privileges, no power-loss simulation): the syncFile hook
// must observe the file existing on disk, the syncDir hook must observe the
// TARGET file existing and decoding to the just-marked event (so the rename
// provably ran between the two hooks), and the recorded sequence is exactly
// [file:<unique temp>, dir:<parent>] on the rewrite path and [file:<state>]
// on the append path. Fails pre-fix: pre-fix Mark used the deterministic
// <state>.tmp name and performed zero dir syncs.
func TestReplayStateMarkSyncsFileBeforeRenameAndDirAfterRename(t *testing.T) {
	t.Run("first write: content fsync before rename, dir fsync after", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		var order []string
		state.syncFile = func(p string) error {
			if p == path || p == path+".tmp" {
				t.Errorf("syncFile path=%s: the rewrite path must sync the UNIQUE temp, never the target or the deterministic name", p)
			}
			if _, err := os.Stat(p); err != nil {
				t.Errorf("syncFile must observe the temp file existing: %v", err)
			}
			order = append(order, "file:"+filepath.Base(p))
			return nil
		}
		state.syncDir = func(p string) error {
			if p != filepath.Dir(path) {
				t.Errorf("syncDir path=%s, want parent %s", p, filepath.Dir(path))
			}
			disk, err := LoadReplayState(path)
			if err != nil {
				t.Errorf("syncDir must observe a decodable target: %v", err)
			}
			if err == nil && !disk.Replayed["evt-1"] {
				t.Error("syncDir must observe the target holding the just-marked event")
			}
			order = append(order, "dir:"+filepath.Base(p))
			return nil
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		if len(order) != 2 || !strings.HasPrefix(order[0], "file:") || !strings.HasPrefix(order[1], "dir:") {
			t.Fatalf("sync order = %v, want exactly [file:<unique temp>, dir:<parent>] (file fsync before rename, dir fsync after)", order)
		}
		if order[0] == "file:state.json" || order[0] == "file:state.json.tmp" {
			t.Fatalf("sync order = %v: rewrite path must sync the unique temp, never the target or the deterministic name", order)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-1"] {
			t.Fatal("marked event lost after reload")
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") {
				t.Fatalf(".tmp left behind after successful Mark: %s", entry.Name())
			}
		}
	})
	t.Run("append path: file fsync after append, no rename, no dir sync", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		var order []string
		state.syncFile = func(p string) error {
			if p != path {
				t.Errorf("syncFile path=%s, want the state file %s (append path fsyncs the target, no temp)", p, path)
			}
			if _, err := os.Stat(p); err != nil {
				t.Errorf("syncFile must observe the state file existing: %v", err)
			}
			order = append(order, "file:"+filepath.Base(p))
			return nil
		}
		state.syncDir = func(p string) error {
			t.Errorf("append path must never sync a directory, got %s", p)
			return nil
		}
		if err := state.Mark("evt-2"); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(order, []string{"file:state.json"}) {
			t.Fatalf("sync order = %v, want exactly [file:state.json] (append → file fsync; no rename, no dir sync)", order)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-1"] || !reloaded.Replayed["evt-2"] {
			t.Fatalf("both marks must survive the append path: %v", reloaded.Replayed)
		}
	})
	t.Run("production defaults", func(t *testing.T) {
		// Nil hooks resolve to the real fsutil.SyncFile/fsutil.SyncDir on a
		// real tempdir (mirrors TestSyncDirChainNilSyncDir).
		realPath := filepath.Join(t.TempDir(), "state.json")
		realState, err := LoadReplayState(realPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := realState.Mark("evt-2"); err != nil {
			t.Fatalf("Mark with default hooks: %v", err)
		}
		reloaded, err := LoadReplayState(realPath)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-2"] {
			t.Fatal("marked event lost with default hooks")
		}
	})
}

// AC-2 (REQ-5): LoadReplayState tolerates every crash leftover a crashed
// Mark can leave. A stale .tmp is ignored — never read, parsed, or deleted —
// alongside a valid state file; a zero-length or whitespace-only target is
// an empty state; a missing target is an empty state.
func TestLoadReplayStateToleratesCrashLeftovers(t *testing.T) {
	t.Run("stale garbage tmp alongside valid state", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(`{"replayed":{"evt-a":true}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+".tmp", []byte("\x00garbage-not-json{"), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("stale .tmp must not fail the load: %v", err)
		}
		if !state.Replayed["evt-a"] {
			t.Fatalf("valid state lost alongside stale .tmp: %v", state.Replayed)
		}
		raw, err := os.ReadFile(path + ".tmp")
		if err != nil {
			t.Fatalf("stale .tmp must not be deleted: %v", err)
		}
		if string(raw) != "\x00garbage-not-json{" {
			t.Fatalf("stale .tmp content modified: %q", raw)
		}
	})
	t.Run("stale unique temp and lock file alongside a log state", func(t *testing.T) {
		// The new write path leaves unique-named temps (crash residue) and a
		// sibling .lock file; neither may be read, parsed, or deleted by the
		// loader (REQ-6(c), FM-12).
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte("{\"format\":\"replay-log-v1\"}\n{\"event_id\":\"evt-a\"}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+".lock", nil, 0o600); err != nil {
			t.Fatal(err)
		}
		stale := filepath.Join(filepath.Dir(path), "state.json-999999.tmp")
		if err := os.WriteFile(stale, []byte("\x00partial-write-garbage"), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("stale temp/lock must not fail the load: %v", err)
		}
		if !state.Replayed["evt-a"] {
			t.Fatalf("log state lost alongside stale artifacts: %v", state.Replayed)
		}
		if _, err := os.Stat(stale); err != nil {
			t.Fatalf("stale unique temp must not be deleted: %v", err)
		}
		if _, err := os.Stat(path + ".lock"); err != nil {
			t.Fatalf("lock file must not be touched: %v", err)
		}
	})
	t.Run("zero-length target is empty state", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("zero-length target must load: %v", err)
		}
		if len(state.Replayed) != 0 {
			t.Fatalf("zero-length target must be an empty map, got %v", state.Replayed)
		}
	})
	t.Run("whitespace-only target is empty state", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(" \n\t"), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("whitespace target must load: %v", err)
		}
		if len(state.Replayed) != 0 {
			t.Fatalf("whitespace target must be an empty map, got %v", state.Replayed)
		}
	})
	t.Run("missing target is empty state", func(t *testing.T) {
		state, err := LoadReplayState(filepath.Join(t.TempDir(), "absent.json"))
		if err != nil {
			t.Fatalf("missing target must load: %v", err)
		}
		if len(state.Replayed) != 0 {
			t.Fatalf("missing target must be an empty map, got %v", state.Replayed)
		}
	})
}

// AC-3 (REQ-4/REQ-6(c) crash model): the deterministic replay model shows
// every crash position of the log write paths leaves a DECODABLE state file:
//   - append + file fsync complete → the log holds {evt-1, evt-2};
//   - crash between append and file fsync → the append may revert → the
//     target holds {evt-1} (last durable content), still decodable;
//   - crash before the first write's temp fsync → the write never happened
//     → target absent, or zeroed by a power failure → empty state.
//
// All decode with nil error (boot proceeds; the next round re-scans —
// at-least-once, tolerated). The non-vacuous control: a torn byte prefix —
// the pre-fix F1 state — is a decode error, proving the model can produce an
// undecodable file and that the fsync ordering is what prevents it. A torn
// TRAILING log line (crash mid-append) is skipped, never a mark, never an
// error (REQ-6(c)).
func TestReplayStateMarkSurvivesSimulatedCrash(t *testing.T) {
	t.Run("fully durable marks decode after every step", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		// Real fsutil.SyncFile/SyncDir defaults: the full protocol runs.
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-2"); err != nil {
			t.Fatal(err)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("fully durable target must decode: %v", err)
		}
		if !reloaded.Replayed["evt-1"] || !reloaded.Replayed["evt-2"] {
			t.Fatalf("both marks must survive the crash: %v", reloaded.Replayed)
		}
	})
	t.Run("crash between append and file sync may revert the append", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		// Mark evt-1 durably (real syncs) and capture its bytes: those are
		// what the target reverts to when evt-2's append is not durable.
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		durableAfterEvt1, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// The second mark's file fsync is dropped (crash cuts the process
		// between the append write and the fsync): Mark reports the failure
		// (FM-3) while the append's bytes are already in the file.
		state.syncFile = func(string) error { return errors.New("injected file sync failure") }
		if err := state.Mark("evt-2"); err == nil {
			t.Fatal("Mark must fail when the file sync fails")
		}
		if state.Replayed["evt-2"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		// Outcome A — the unsynced append reverts on power failure: the
		// target is the last durable content {evt-1}. Still decodable.
		if err := os.WriteFile(path, durableAfterEvt1, 0o600); err != nil {
			t.Fatal(err)
		}
		reverted, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("reverted target must decode with nil error: %v", err)
		}
		if !reverted.Replayed["evt-1"] || reverted.Replayed["evt-2"] {
			t.Fatalf("reverted target must hold only evt-1: %v", reverted.Replayed)
		}
		// Outcome B — the append survived: the target holds {evt-1, evt-2}
		// and also decodes with nil error (both states decode).
		state.syncFile = nil
		if err := state.Mark("evt-2"); err != nil {
			t.Fatal(err)
		}
		survived, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("survived-append target must decode with nil error: %v", err)
		}
		if !survived.Replayed["evt-1"] || !survived.Replayed["evt-2"] {
			t.Fatalf("survived-append target must hold both marks: %v", survived.Replayed)
		}
	})
	t.Run("crash before the first write's temp sync loses the write", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		// The first write's temp-file fsync is dropped: Mark fails (FM-4),
		// the temp is removed, and no target was ever created.
		state.syncFile = func(string) error { return errors.New("injected file sync failure") }
		if err := state.Mark("evt-1"); err == nil {
			t.Fatal("Mark must fail when the first write's temp sync fails")
		}
		if state.Replayed["evt-1"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("target must not exist after a failed first write: %v", err)
		}
		missing, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("absent target must decode with nil error: %v", err)
		}
		if len(missing.Replayed) != 0 {
			t.Fatalf("absent target must be an empty map, got %v", missing.Replayed)
		}
		// Crash outcome — the un-flushed contents are lost entirely and the
		// target comes back zeroed: an empty state, NOT a decode error
		// (boot proceeds; the next round re-scans — at-least-once).
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		zeroed, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("zeroed target must decode with nil error: %v", err)
		}
		if len(zeroed.Replayed) != 0 {
			t.Fatalf("zeroed target must be an empty map, got %v", zeroed.Replayed)
		}
	})
	t.Run("torn byte prefix is a decode error (non-vacuous)", func(t *testing.T) {
		// The pre-fix F1 state: an un-fsynced write can leave a torn byte
		// prefix after power loss. This is the ONLY crash outcome that fails
		// to decode, and it is exactly what the fsync-before-rename ordering
		// removes. The sub-check proves the model is non-vacuous: not every
		// crash outcome decodes, so the assertions above are meaningful.
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(`{"replayed":{"evt-1":tru`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadReplayState(path); err == nil {
			t.Fatal("torn target must be a decode error (fail-loud, pinned by REQ-5)")
		}
	})
	t.Run("torn trailing log line is skipped, never a mark or error", func(t *testing.T) {
		// A crash mid-append leaves a partial trailing line: the loader
		// skips it (REQ-6(c)) — never a mark, never a decode failure — and
		// the next Mark repairs the tail before appending.
		path := filepath.Join(t.TempDir(), "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte(`{"event_id":"evt-2`)); err != nil {
			file.Close()
			t.Fatal(err)
		}
		file.Close()
		loaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("torn trailing line must not fail the load: %v", err)
		}
		if !loaded.Replayed["evt-1"] || loaded.Replayed["evt-2"] {
			t.Fatalf("torn trailing line must be skipped, never a mark: %v", loaded.Replayed)
		}
	})
}

// REQ-3/REQ-7 (FM-1…FM-7): every Mark failure path removes only its own
// temp file best-effort and preserves the authoritative state — the target
// stays untouched when the failure precedes the rename (rewrite-path file
// sync failure, write failure), KEEPS the new content when the failure
// follows it (dir-sync failure, D4: both old and new content decode), and
// decodes as a SUPERSET when the failure is a post-append file fsync (the
// append itself succeeded). Under the amended REQ-7 (security F1) the
// in-memory set is updated only after a successful persist, so a failed
// Mark never suppresses the record's retry. Deterministic: injected hook
// failures plus real-filesystem failure cases (ENOTDIR lock path, directory
// target) that need no privileges.
func TestReplayStateMarkErrorPathsCleanTempAndPreserveTarget(t *testing.T) {
	t.Run("legacy conversion file sync failure: temp removed, target untouched", func(t *testing.T) {
		// The failing Mark is a legacy→log conversion (a rewrite path): the
		// temp-file sync failure precedes the rename, so the target stays
		// byte-identical to the authoritative legacy file.
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(`{"replayed":{"evt-1":true}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		state.syncFile = func(string) error { return errors.New("injected file sync failure") }
		if err := state.Mark("evt-2"); err == nil {
			t.Fatal("Mark must fail when the temp-file sync fails (FM-4)")
		}
		if state.Replayed["evt-2"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(after, before) {
			t.Fatalf("target changed after file-sync failure: %s", after)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-1"] || reloaded.Replayed["evt-2"] {
			t.Fatalf("target must hold the authoritative pre-failure state: %v", reloaded.Replayed)
		}
	})
	t.Run("append file sync failure: error, no temp, target decodes as a superset", func(t *testing.T) {
		// On the append path the line is written before the file fsync, so a
		// failed fsync leaves a decodable SUPERSET (the new REQ-7 invariant:
		// "target = previous lines + new line", replacing the old
		// byte-identical-target invariant). The in-memory set lacks the mark
		// (security F1), so the next round re-reads the DLQ record as wanted
		// and retries until the mark is durable — a DLQ offset is never
		// committed without the durable mark.
		path := filepath.Join(t.TempDir(), "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		state.syncFile = func(string) error { return errors.New("injected file sync failure") }
		if err := state.Mark("evt-2"); err == nil {
			t.Fatal("Mark must fail when the post-append file sync fails (FM-3)")
		}
		if state.Replayed["evt-2"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("superset target must decode: %v", err)
		}
		if !reloaded.Replayed["evt-1"] || !reloaded.Replayed["evt-2"] {
			t.Fatalf("superset target must hold both marks: %v", reloaded.Replayed)
		}
		entries, err := os.ReadDir(filepath.Dir(path))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") {
				t.Fatalf(".tmp left behind after append failure: %s", entry.Name())
			}
		}
	})
	t.Run("dir sync failure after legacy conversion: target keeps the new content", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(`{"replayed":{"evt-1":true}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		state.syncDir = func(string) error { return errors.New("injected dir sync failure") }
		if err := state.Mark("evt-2"); err == nil {
			t.Fatal("Mark must fail when the parent-dir sync fails (FM-6)")
		}
		if state.Replayed["evt-2"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		// D4: the rename already succeeded, so the target holds the NEW log
		// content — removing it would lose committed marks; the next Mark
		// retries the durability step. Both old and new content decode.
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("target after dir-sync failure must decode: %v", err)
		}
		if !reloaded.Replayed["evt-1"] || !reloaded.Replayed["evt-2"] {
			t.Fatalf("target must keep the new content after dir-sync failure: %v", reloaded.Replayed)
		}
	})
	t.Run("lock path under a regular file: no temp or lock artifact anywhere", func(t *testing.T) {
		dir := t.TempDir()
		blocker := filepath.Join(dir, "blocker")
		if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		// The state file's parent is a regular file: the lock open fails
		// with ENOTDIR (REQ-1 — lock failure is a persist error).
		// LoadReplayState would fail the same way, so construct the state
		// directly (same package).
		state := &ReplayState{Replayed: map[string]bool{"evt-1": true}, path: filepath.Join(blocker, "state.json")}
		if err := state.Mark("evt-2"); err == nil {
			t.Fatal("Mark must fail when the lock path hits ENOTDIR")
		}
		if state.Replayed["evt-2"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") || strings.HasSuffix(entry.Name(), ".lock") {
				t.Fatalf("artifact left behind after ENOTDIR failure: %s", entry.Name())
			}
		}
	})
	t.Run("stale deterministic temp residue does not block a Mark", func(t *testing.T) {
		// Pre-fix writers left <state>.tmp artifacts, and a crashed write
		// could even leave a directory squatting on that name. The log
		// writer never touches the deterministic name (REQ-3), so stale
		// residue is harmless and must be left untouched.
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.Mkdir(path+".tmp", 0o700); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatalf("Mark must succeed despite stale <state>.tmp residue: %v", err)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-1"] {
			t.Fatalf("mark lost: %v", reloaded.Replayed)
		}
		info, err := os.Stat(path + ".tmp")
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() {
			t.Fatal("stale residue must be left untouched")
		}
	})
	t.Run("directory target: no temp left behind, target untouched", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		// The target path is an existing directory: classification fails
		// reading it (EISDIR), so the Mark fails before any temp exists
		// (FM-5); the directory is untouched.
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		state := &ReplayState{Replayed: map[string]bool{"evt-1": true}, path: path}
		if err := state.Mark("evt-2"); err == nil {
			t.Fatal("Mark must fail when the target is a directory")
		}
		if state.Replayed["evt-2"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") {
				t.Fatalf(".tmp left behind after directory-target failure: %s", entry.Name())
			}
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() {
			t.Fatal("target must stay untouched after directory-target failure")
		}
	})
}

// Rolling-restart and mixed-version contract: the on-disk bytes are part of
// the compatibility surface, so pin the exact schema. A future format
// change breaks these tests loudly instead of silently invalidating the
// "zero data migration" claim. Also pins the 0o600 permission contract,
// the legacy-format load path (REQ-6(a)), the JSON-aware format
// discriminator (protocol F-1: unknown/malformed formats fail loudly), and
// torn-header self-healing (FM-10).
func TestReplayStateOnDiskFormatIsStable(t *testing.T) {
	const golden = "{\"format\":\"replay-log-v1\"}\n{\"event_id\":\"evt-1\"}\n"
	t.Run("Mark writes exactly the golden bytes", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != golden {
			t.Fatalf("persisted bytes = %q, want golden %q", raw, golden)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Fatalf("state file mode = %o, want 600", perm)
		}
	})
	t.Run("golden bytes load back", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(golden), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(state.Replayed) != 1 || !state.Replayed["evt-1"] {
			t.Fatalf("golden bytes must decode to exactly {evt-1}: %v", state.Replayed)
		}
	})
	t.Run("legacy file loads and converts in place on the next Mark", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(`{"replayed":{"evt-1":true}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(state.Replayed) != 1 || !state.Replayed["evt-1"] {
			t.Fatalf("legacy file must decode to {evt-1}: %v", state.Replayed)
		}
		if err := state.Mark("evt-2"); err != nil {
			t.Fatalf("Mark must convert the legacy file in place: %v", err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		want := "{\"format\":\"replay-log-v1\"}\n{\"event_id\":\"evt-1\"}\n{\"event_id\":\"evt-2\"}\n"
		if string(raw) != want {
			t.Fatalf("converted bytes = %q, want %q", raw, want)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-1"] || !reloaded.Replayed["evt-2"] {
			t.Fatalf("converted file must hold both marks: %v", reloaded.Replayed)
		}
	})
	t.Run("unknown replay format fails loudly", func(t *testing.T) {
		// protocol F-1: a future format must never decode as an empty legacy
		// state (which would let the next Mark destroy its marks silently).
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte("{\"format\":\"replay-log-v2\"}\n{\"event_id\":\"x\"}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadReplayState(path)
		if err == nil || !strings.Contains(err.Error(), "unknown replay state format") {
			t.Fatalf("unknown format must fail loudly, got %v", err)
		}
	})
	t.Run("malformed v1 header fails loudly", func(t *testing.T) {
		// A valid-JSON first line with format=replay-log-v1 but extra fields
		// is never produced by this writer: fail loudly instead of healing
		// it silently (protocol F-1).
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte("{\"format\":\"replay-log-v1\",\"x\":1}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadReplayState(path)
		if err == nil || !strings.Contains(err.Error(), "invalid replay state header") {
			t.Fatalf("malformed header must fail loudly, got %v", err)
		}
	})
	t.Run("torn header fails loudly at load and self-heals on the next Mark", func(t *testing.T) {
		// FM-10: a never-fsynced torn first write fails loud at boot (the
		// same fail-loud outcome as today's torn-target pin); the next Mark
		// classifies it as torn and rewrites a fresh log, publishing the
		// union without losing anything (the torn content was never durable).
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte("{\"format\":\"replay-log-v1"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadReplayState(path); err == nil {
			t.Fatal("torn header must fail loudly at load (FM-10)")
		}
		state := &ReplayState{Replayed: map[string]bool{}, path: path}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatalf("next Mark must self-heal the torn header: %v", err)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-1"] {
			t.Fatalf("healed file must hold the mark: %v", reloaded.Replayed)
		}
	})
}

// AC-1 (REQ-1/REQ-2/REQ-8): a concurrent Mark storm on one state path yields
// a decodable file with the union of every writer's marks. Separate
// instances model separate processes with stale boot-time views; the shared
// sub-case exercises one instance from many goroutines (race-clean via mu).
// Fails pre-fix: last-writer-wins drops marks, and the shared sub-case is a
// data race.
func TestReplayStateConcurrentMarksUnion(t *testing.T) {
	t.Run("separate instances", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		instances := make([]*ReplayState, replayConcurrentWriters)
		for w := range instances {
			s, err := LoadReplayState(path)
			if err != nil {
				t.Fatal(err)
			}
			instances[w] = s
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for w, s := range instances {
			wg.Add(1)
			go func(w int, s *ReplayState) {
				defer wg.Done()
				<-start
				for i := 0; i < replayConcurrentPerWriter; i++ {
					if err := s.Mark(fmt.Sprintf("w%d-e%d", w, i)); err != nil {
						t.Errorf("writer %d: %v", w, err)
						return
					}
				}
				for i := 0; i < replayConcurrentShared; i++ {
					if err := s.Mark(fmt.Sprintf("core-%d", i)); err != nil {
						t.Errorf("writer %d core: %v", w, err)
						return
					}
				}
			}(w, s)
		}
		close(start)
		wg.Wait()
		assertReplayUnion(t, path)
	})
	t.Run("shared instance", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < replayConcurrentWriters; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start
				for i := 0; i < replayConcurrentPerWriter; i++ {
					if err := state.Mark(fmt.Sprintf("w%d-e%d", w, i)); err != nil {
						t.Errorf("writer %d: %v", w, err)
						return
					}
				}
				for i := 0; i < replayConcurrentShared; i++ {
					if err := state.Mark(fmt.Sprintf("core-%d", i)); err != nil {
						t.Errorf("writer %d core: %v", w, err)
						return
					}
				}
			}(w)
		}
		close(start)
		wg.Wait()
		assertReplayUnion(t, path)
	})
}

// assertReplayUnion asserts the state file decodes and holds the union of
// every concurrent writer's distinct IDs plus the shared core.
func assertReplayUnion(t *testing.T, path string) {
	t.Helper()
	state, err := LoadReplayState(path)
	if err != nil {
		t.Fatalf("union must decode: %v", err)
	}
	for w := 0; w < replayConcurrentWriters; w++ {
		for i := 0; i < replayConcurrentPerWriter; i++ {
			if !state.Replayed[fmt.Sprintf("w%d-e%d", w, i)] {
				t.Errorf("missing mark w%d-e%d (union incomplete)", w, i)
			}
		}
	}
	for i := 0; i < replayConcurrentShared; i++ {
		if !state.Replayed[fmt.Sprintf("core-%d", i)] {
			t.Errorf("missing shared mark core-%d (union incomplete)", i)
		}
	}
}

// AC-2 (REQ-3/REQ-6(c)): no two writers ever share one temp inode; the
// deterministic <state>.tmp name is never used; no *.tmp residue remains
// after a storm; stale temps (crash residue) and torn append tails are never
// interpreted as marks and never break a later Mark.
func TestReplayStateUniqueTempPerWrite(t *testing.T) {
	t.Run("first writes use distinct unique temps", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		var temps []string
		state.newTemp = func(dir, pattern string) (*os.File, error) {
			file, err := os.CreateTemp(dir, pattern)
			if err == nil {
				temps = append(temps, file.Name())
			}
			return file, err
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		// Delete the state file between marks: every Mark re-triggers the
		// first-write rewrite, forcing two rewriteFull attempts on one path
		// — each must use its own unique temp (async F1).
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-2"); err != nil {
			t.Fatal(err)
		}
		if len(temps) != 2 {
			t.Fatalf("temps=%d, want 2 (one per first write)", len(temps))
		}
		if temps[0] == temps[1] {
			t.Fatal("two rewrites shared one temp inode")
		}
		for _, temp := range temps {
			if temp == path+".tmp" {
				t.Fatal("deterministic <state>.tmp name must never be used")
			}
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") {
				t.Fatalf("*.tmp residue after Mark: %s", entry.Name())
			}
		}
	})
	t.Run("concurrent storm leaves no temp residue", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < replayConcurrentWriters; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start
				for i := 0; i < replayConcurrentPerWriter; i++ {
					if err := state.Mark(fmt.Sprintf("w%d-e%d", w, i)); err != nil {
						t.Errorf("writer %d: %v", w, err)
						return
					}
				}
			}(w)
		}
		close(start)
		wg.Wait()
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") {
				t.Fatalf("*.tmp residue after the storm: %s", entry.Name())
			}
		}
	})
	t.Run("crash residue temp is ignored and a later Mark publishes the union", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		// A crashed writer's unique temp survives next to the state file
		// (simulated crash residue — no cleanup ran).
		stale := filepath.Join(dir, "state.json-1234567890.tmp")
		if err := os.WriteFile(stale, []byte("\x00partial-write-garbage"), 0o600); err != nil {
			t.Fatal(err)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("stale temp must not fail the load: %v", err)
		}
		if !reloaded.Replayed["evt-1"] {
			t.Fatal("state lost alongside stale temp")
		}
		if err := state.Mark("evt-2"); err != nil {
			t.Fatalf("Mark must succeed despite stale temp: %v", err)
		}
		final, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !final.Replayed["evt-1"] || !final.Replayed["evt-2"] {
			t.Fatalf("union must be published despite stale temp: %v", final.Replayed)
		}
		if _, err := os.Stat(stale); err != nil {
			t.Fatalf("loader must never delete a stale temp: %v", err)
		}
	})
	t.Run("torn append tail is never a mark and the next Mark self-heals", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		// Crash mid-append: a partial line without its terminating newline.
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte(`{"event_id":"evt-torn`)); err != nil {
			file.Close()
			t.Fatal(err)
		}
		file.Close()
		loaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("torn tail must not fail the load: %v", err)
		}
		if !loaded.Replayed["evt-1"] || loaded.Replayed["evt-torn"] {
			t.Fatalf("torn tail must be skipped, never a mark: %v", loaded.Replayed)
		}
		if err := state.Mark("evt-2"); err != nil {
			t.Fatalf("next Mark must repair the tail: %v", err)
		}
		final, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !final.Replayed["evt-1"] || !final.Replayed["evt-2"] || final.Replayed["evt-torn"] {
			t.Fatalf("repair must publish the union and drop the torn fragment: %v", final.Replayed)
		}
	})
}

// AC-3 (REQ-5): the persistence cost of one Mark is not O(replayed-set
// size). The bytes-written bound is deterministic (a size-recording syncFile
// hook): after seeding M=10,000 marks, K=1,000 more marks must write O(M+K)
// bytes total — pre-fix, every Mark rewrote the whole map (≈10.5 MB for
// M=10,000, K=1,000 vs the ≈176 KB bound). Growth-independence via
// AllocsPerRun: per-Mark allocations at M=10,000 must be ≤ 4× those at
// M=100 (pre-fix, json.Marshal of the full map scales with the set size).
func TestReplayStateMarkCostNotOOfSetSize(t *testing.T) {
	t.Run("total bytes written is O(M+K)", func(t *testing.T) {
		const m = 10_000
		const k = 1_000
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		var sizes []int64
		state.syncFile = func(p string) error {
			info, err := os.Stat(p)
			if err != nil {
				t.Fatal(err)
			}
			sizes = append(sizes, info.Size())
			return nil
		}
		for i := 0; i < m; i++ {
			if err := state.Mark(fmt.Sprintf("seed-%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if len(sizes) != m {
			t.Fatalf("seed syncs=%d, want %d (one sync per Mark)", len(sizes), m)
		}
		base := sizes[len(sizes)-1]
		for i := 0; i < k; i++ {
			if err := state.Mark(fmt.Sprintf("mark-%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if len(sizes) != m+k {
			t.Fatalf("syncs=%d, want %d", len(sizes), m+k)
		}
		total := sizes[len(sizes)-1] - base
		if bound := 16 * int64(m+k); total > bound {
			t.Fatalf("bytes written for %d marks = %d, want ≤ %d (O(M+K)); per-mark cost is not O(set size)", k, total, bound)
		}
	})
	t.Run("growth-independence: per-Mark allocations do not scale with M", func(t *testing.T) {
		allocsAt := func(m int) float64 {
			dir := t.TempDir()
			path := filepath.Join(dir, "state.json")
			state, err := LoadReplayState(path)
			if err != nil {
				t.Fatal(err)
			}
			state.syncFile = func(string) error { return nil } // isolate the append path, not fsync
			for i := 0; i < m; i++ {
				if err := state.Mark(fmt.Sprintf("seed-%d", i)); err != nil {
					t.Fatal(err)
				}
			}
			id := 0
			return testing.AllocsPerRun(100, func() {
				if err := state.Mark(fmt.Sprintf("mark-%d", id)); err != nil {
					t.Fatal(err)
				}
				id++
			})
		}
		small := allocsAt(100)
		large := allocsAt(10_000)
		if large > 4*small {
			t.Fatalf("allocs/op at M=10,000 (%v) > 4× M=100 (%v): per-Mark cost scales with the set size", large, small)
		}
	})
}

// BenchmarkReplayStateMark reports per-Mark ns/op and B/op over seeded set
// sizes {100, 1,000, 10,000}; a flat profile across M is the supporting
// evidence for the append-only log (constant per line). No-op syncs isolate
// the append path's own cost from filesystem fsync latency.
func BenchmarkReplayStateMark(b *testing.B) {
	for _, m := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("M=%d", m), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "state.json")
			state, err := LoadReplayState(path)
			if err != nil {
				b.Fatal(err)
			}
			state.syncFile = func(string) error { return nil }
			state.syncDir = func(string) error { return nil }
			for i := 0; i < m; i++ {
				if err := state.Mark(fmt.Sprintf("seed-%d", i)); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := state.Mark(fmt.Sprintf("mark-%d", i)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// AC-4 (REQ-1/REQ-2): daemon and -once writers run as separate PROCESSES
// against one state path; the file stays decodable and holds the union of
// every child's marks. Uses the standard helper-process pattern: the test
// binary re-executes itself as N children (inheriting -race when the parent
// runs under it), each simulating one replay round (load → Mark → exit).
func TestReplayStateTwoProcessConcurrentRounds(t *testing.T) {
	if os.Getenv("REPLAY_STATE_HELPER") == "1" {
		runReplayStateHelper()
		return
	}
	path := filepath.Join(t.TempDir(), "state.json")
	errs := make(chan error, replayConcurrentWriters)
	var wg sync.WaitGroup
	for w := 0; w < replayConcurrentWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=^TestReplayStateTwoProcessConcurrentRounds$")
			cmd.Env = append(os.Environ(),
				"REPLAY_STATE_HELPER=1",
				"REPLAY_STATE_PATH="+path,
				fmt.Sprintf("REPLAY_STATE_WRITER=%d", w),
			)
			out, err := cmd.CombinedOutput()
			if err != nil {
				errs <- fmt.Errorf("helper %d: %v\n%s", w, err, out)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	assertReplayUnion(t, path)
}

// runReplayStateHelper is the child side of the two-process test: load the
// shared state, mark one writer's distinct IDs plus the shared core, exit.
func runReplayStateHelper() {
	path := os.Getenv("REPLAY_STATE_PATH")
	writer, err := strconv.Atoi(os.Getenv("REPLAY_STATE_WRITER"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad writer:", err)
		os.Exit(1)
	}
	state, err := LoadReplayState(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(1)
	}
	for i := 0; i < replayConcurrentPerWriter; i++ {
		if err := state.Mark(fmt.Sprintf("w%d-e%d", writer, i)); err != nil {
			fmt.Fprintln(os.Stderr, "mark:", err)
			os.Exit(1)
		}
	}
	for i := 0; i < replayConcurrentShared; i++ {
		if err := state.Mark(fmt.Sprintf("core-%d", i)); err != nil {
			fmt.Fprintln(os.Stderr, "mark core:", err)
			os.Exit(1)
		}
	}
}

// REQ-1: a lock-acquisition failure is a persist error — the round aborts,
// the previous state stays authoritative, nothing is marked in memory
// (security F1), and no temp or lock artifact is left behind. flock
// auto-releases on process death, so the only failure classes are the open
// failing (ENOTDIR etc.) and a contended lock timing out (a hung-but-alive
// peer cannot stall recovery forever).
func TestReplayStateLockFailureAbortsPersist(t *testing.T) {
	t.Run("lock path under a regular file fails with a wrapped error", func(t *testing.T) {
		dir := t.TempDir()
		blocker := filepath.Join(dir, "blocker")
		if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		state := &ReplayState{Replayed: map[string]bool{}, path: filepath.Join(blocker, "state.json")}
		err := state.Mark("evt-1")
		if err == nil {
			t.Fatal("Mark must fail when the lock cannot be opened")
		}
		if !strings.Contains(err.Error(), "replay state lock") {
			t.Fatalf("error must wrap the lock failure: %v", err)
		}
		if state.Replayed["evt-1"] {
			t.Fatal("in-memory mark must not be set when the persist failed (security F1)")
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") || strings.HasSuffix(entry.Name(), ".lock") {
				t.Fatalf("artifact left behind after lock failure: %s", entry.Name())
			}
		}
	})
	t.Run("contended lock times out with a wrapped error", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		lock, err := lockReplayState(path, 0) // hold the flock in this process
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		state.lockWait = 30 * time.Millisecond
		err = state.Mark("evt-1")
		if err == nil {
			t.Fatal("Mark must fail while another writer holds the lock")
		}
		if !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("error must report the lock timeout: %v", err)
		}
		if state.Replayed["evt-1"] {
			t.Fatal("in-memory mark must not be set when the persist failed (security F1)")
		}
	})
}
