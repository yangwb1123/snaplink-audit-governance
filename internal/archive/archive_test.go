package archive

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestFileStorePutGetRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	store := &FileStore{Dir: dir}
	key := "events/demo/stream/00000000000000000001-evt.json"
	payload := []byte(`{"event_id":"evt"}`)
	if err := store.Put(context.Background(), key, payload); err != nil {
		t.Fatal(err)
	}
	// 幂等：内容一致的重复 Put 返回 nil 且不覆盖。
	if err := store.Put(context.Background(), key, payload); err != nil {
		t.Fatal(err)
	}
	// 篡改内容：必须报错，不能静默视为已归档。
	if err := store.Put(context.Background(), key, []byte(`tampered`)); err == nil {
		t.Fatal("Put with mismatched content must fail instead of reporting archived")
	}
	data, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(payload) {
		t.Fatalf("object overwritten by idempotent Put: %s", data)
	}
	// 只读权限（WORM 语义）。
	info, err := os.Stat(filepath.Join(dir, "events", "demo", "stream", "00000000000000000001-evt.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o222 != 0 {
		t.Fatalf("archive object must be read-only, mode=%v", info.Mode())
	}
}

// TestFileStorePutRejectsNonRegularPreExisting is AC-1: a path that already
// exists at the key but is not a regular file (directory, symlink) must be
// rejected instead of silently treated as "already archived".
func TestFileStorePutRejectsNonRegularPreExisting(t *testing.T) {
	key := "events/demo/stream/00000000000000000001-evt.json"
	payload := []byte(`{"event_id":"evt"}`)

	t.Run("directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "archive")
		target := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.MkdirAll(target, 0o750); err != nil {
			t.Fatal(err)
		}
		store := &FileStore{Dir: dir}
		if err := store.Put(context.Background(), key, payload); err == nil {
			t.Fatal("Put must reject a pre-existing directory at the key")
		}
	})

	t.Run("symlink", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "archive")
		target := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			t.Fatal(err)
		}
		// A dangling symlink is enough: it is not a regular file.
		if err := os.Symlink(filepath.Join(dir, "elsewhere"), target); err != nil {
			t.Skipf("cannot create symlink: %v", err)
		}
		store := &FileStore{Dir: dir}
		if err := store.Put(context.Background(), key, payload); err == nil {
			t.Fatal("Put must reject a pre-existing symlink at the key")
		}
	})
}

// TestFileStorePutVerifiesExistingContent is AC-2/AC-3: an existing regular
// file is only "already archived" when it is byte-identical; a mismatch or
// an unverifiable file is an error, and the pre-existing object is never
// modified or removed (WORM).
func TestFileStorePutVerifiesExistingContent(t *testing.T) {
	key := "events/demo/stream/00000000000000000001-evt.json"
	payload := []byte(`{"event_id":"evt"}`)

	t.Run("identical", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "archive")
		target := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, payload, 0o440); err != nil {
			t.Fatal(err)
		}
		store := &FileStore{Dir: dir}
		if err := store.Put(context.Background(), key, payload); err != nil {
			t.Fatalf("byte-identical retry must succeed: %v", err)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "archive")
		target := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(`tampered`), 0o440); err != nil {
			t.Fatal(err)
		}
		store := &FileStore{Dir: dir}
		if err := store.Put(context.Background(), key, payload); err == nil {
			t.Fatal("Put must fail on content mismatch")
		}
		// WORM: the pre-existing object is preserved, not removed or rewritten.
		got, err := os.ReadFile(target)
		if err != nil || string(got) != `tampered` {
			t.Fatalf("pre-existing object modified: %v %q", err, got)
		}
	})

	t.Run("unreadable", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "archive")
		target := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(`tampered`), 0o000); err != nil {
			t.Fatal(err)
		}
		// Root and other privileged processes bypass permission bits; probe
		// readability instead of asserting a fake failure for them.
		if _, err := os.ReadFile(target); err == nil {
			t.Skip("file permissions are not enforced for this process")
		}
		store := &FileStore{Dir: dir}
		if err := store.Put(context.Background(), key, payload); err == nil {
			t.Fatal("Put must fail when the existing object cannot be verified")
		}
		// WORM: the unreadable pre-existing object is still not removed.
		if _, err := os.Stat(target); err != nil {
			t.Fatalf("pre-existing object removed: %v", err)
		}
	})
}

// TestFileStorePutReadOnlyDirLeavesNoObject is AC-3: when the destination
// directory is read-only, Put fails and no object exists at the key
// afterwards. Portable fallback of the RLIMIT_FSIZE write-fault leg (see
// archive_unix_test.go).
func TestFileStorePutReadOnlyDirLeavesNoObject(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	target := filepath.Join(dir, "events", "demo", "stream", "00000000000000000001-evt.json")
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		t.Fatal(err)
	}
	// The directory that must hold the object is read-only, so object
	// creation itself fails before any partial data can exist.
	if err := os.Chmod(filepath.Dir(target), 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Dir(target), 0o700) })
	store := &FileStore{Dir: dir}
	putErr := store.Put(context.Background(), "events/demo/stream/00000000000000000001-evt.json", []byte(`{"event_id":"evt"}`))
	if putErr == nil {
		// A privileged process (e.g. euid 0) bypasses the permission bits,
		// so the leg cannot be exercised; the object it created is cleaned
		// up with the temp dir.
		t.Skip("directory permissions are not enforced for this process")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("object exists at key after failed Put: %v", err)
	}
}

func TestFileStoreGetMissing(t *testing.T) {
	store := &FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
	if _, err := store.Get(context.Background(), "nope.json"); !os.IsNotExist(err) {
		t.Fatalf("missing object err=%v, want IsNotExist", err)
	}
}

func TestFileStoreReady(t *testing.T) {
	store := &FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
	if err := store.Ready(context.Background()); err != nil {
		t.Fatalf("Ready: %v", err)
	}
	// 空目录配置 → 不可用。
	if err := (&FileStore{Dir: ""}).Ready(context.Background()); err == nil {
		t.Fatal("empty dir must not be ready")
	}
}

func TestFileStorePutRejectsEmptyDir(t *testing.T) {
	// AC-3: an empty-Dir FileStore must fail before any filesystem access
	// instead of silently writing into the process working directory.
	cwd := t.TempDir()
	t.Chdir(cwd)
	store := &FileStore{}
	putErr := store.Put(context.Background(), "events/a.json", []byte(`{}`))
	if putErr == nil {
		t.Fatal("empty dir Put must fail")
	}
	entries, err := os.ReadDir(cwd)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("empty-dir Put wrote into the working directory: %v", entries)
	}
	// Ready rejects the same configuration with the byte-identical message.
	readyErr := store.Ready(context.Background())
	if readyErr == nil || readyErr.Error() != putErr.Error() {
		t.Fatalf("Put/Ready error mismatch: put=%v ready=%v", putErr, readyErr)
	}
}

// unknownStore pins the Configured default: implementations the predicate
// does not know are treated as configured because the caller injected them
// deliberately.
type unknownStore struct{}

func (unknownStore) Put(context.Context, string, []byte) error { return nil }

func (unknownStore) Get(context.Context, string) ([]byte, error) { return nil, nil }

func (unknownStore) Ready(context.Context) error { return nil }

func TestConfigured(t *testing.T) {
	cases := []struct {
		name  string
		store Store
		want  bool
	}{
		{"nil", nil, false},
		{"typed nil file store", (*FileStore)(nil), false},
		{"typed nil s3 store", (*S3Store)(nil), false},
		{"empty dir file store", &FileStore{}, false},
		{"file store with dir", &FileStore{Dir: "/x"}, true},
		{"s3 store", &S3Store{}, true},
		{"unknown store", unknownStore{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Configured(tc.store); got != tc.want {
				t.Fatalf("Configured(%T) = %v, want %v", tc.store, got, tc.want)
			}
		})
	}
}

func TestFileStorePutMissingData(t *testing.T) {
	store := &FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
	if err := store.Put(context.Background(), "a/b/c.json", []byte("x")); err != nil {
		t.Fatalf("nested Put: %v", err)
	}
}

// syncRecorder records every directory sync performed through the FileStore
// seam and can fault-inject failures. Because Put's chain sync is its final
// step, the recorded sync log is also a creation log: every entry provably
// happened after the object file was created and closed.
type syncRecorder struct {
	mu     sync.Mutex
	syncs  []string
	fail   map[string]bool // paths whose sync must fail
	failN  int             // number of sync calls to fail, then succeed
	failed int
}

func (r *syncRecorder) record(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.syncs = append(r.syncs, path)
	if r.fail != nil && r.fail[path] {
		return fmt.Errorf("injected sync failure for %s", path)
	}
	if r.failN > 0 && r.failed < r.failN {
		r.failed++
		return fmt.Errorf("injected sync failure for %s", path)
	}
	return nil
}

func (r *syncRecorder) synced(path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.syncs {
		if p == path {
			return true
		}
	}
	return false
}

// TestFileStorePutSyncsFullDirectoryChain is AC-1: a successful Put fsyncs
// every directory from the object's immediate parent up to the archive root,
// leaf-to-root, before returning nil.
func TestFileStorePutSyncsFullDirectoryChain(t *testing.T) {
	key := "events/demo/stream/00000000000000000001-evt.json"
	payload := []byte(`{"event_id":"evt"}`)

	t.Run("nested key", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "archive")
		rec := &syncRecorder{}
		store := &FileStore{Dir: dir, syncDir: rec.record}
		if err := store.Put(context.Background(), key, payload); err != nil {
			t.Fatal(err)
		}
		want := []string{
			filepath.Join(dir, "events", "demo", "stream"),
			filepath.Join(dir, "events", "demo"),
			filepath.Join(dir, "events"),
			dir,
		}
		if !reflect.DeepEqual(rec.syncs, want) {
			t.Fatalf("sync order = %v, want %v", rec.syncs, want)
		}
	})

	t.Run("flat exports key", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "archive")
		rec := &syncRecorder{}
		store := &FileStore{Dir: dir, syncDir: rec.record}
		if err := store.Put(context.Background(), "exports/job.jsonl", payload); err != nil {
			t.Fatal(err)
		}
		want := []string{filepath.Join(dir, "exports"), dir}
		if !reflect.DeepEqual(rec.syncs, want) {
			t.Fatalf("sync order = %v, want %v", rec.syncs, want)
		}
	})

	t.Run("pre-existing parent chain", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "archive")
		if err := os.MkdirAll(filepath.Join(dir, "events", "demo", "stream"), 0o750); err != nil {
			t.Fatal(err)
		}
		rec := &syncRecorder{}
		store := &FileStore{Dir: dir, syncDir: rec.record}
		if err := store.Put(context.Background(), key, payload); err != nil {
			t.Fatal(err)
		}
		want := []string{
			filepath.Join(dir, "events", "demo", "stream"),
			filepath.Join(dir, "events", "demo"),
			filepath.Join(dir, "events"),
			dir,
		}
		if !reflect.DeepEqual(rec.syncs, want) {
			t.Fatalf("sync order = %v, want %v", rec.syncs, want)
		}
	})
}

// crashDrop models a power failure after a successful Put: a directory's
// dentry survives only if it was synced after its child was created. Because
// Put's chain sync is its final step, "recorded as synced" implies "after
// creation". The object (whose dentry lives in the first chain member)
// survives iff every member from the immediate parent up to the archive root
// was synced; any unsynced member loses its whole subtree.
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

// TestFileStorePutSurvivesSimulatedCrash is AC-2: the deterministic replay
// model (no privileges, no tmpfs mount) shows the full chain is required for
// the object to survive a power failure, and that the model is non-vacuous —
// the pre-fix root-only sync demonstrably loses the subtree.
func TestFileStorePutSurvivesSimulatedCrash(t *testing.T) {
	key := "events/demo/stream/00000000000000000001-evt.json"
	payload := []byte(`{"event_id":"evt"}`)

	chain := func(dir string) []string {
		return []string{
			filepath.Join(dir, "events", "demo", "stream"),
			filepath.Join(dir, "events", "demo"),
			filepath.Join(dir, "events"),
			dir,
		}
	}

	t.Run("full chain survives", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "archive")
		rec := &syncRecorder{}
		store := &FileStore{Dir: dir, syncDir: rec.record}
		if err := store.Put(context.Background(), key, payload); err != nil {
			t.Fatal(err)
		}
		for _, d := range crashDrop(rec.syncs, chain(dir)) {
			_ = os.RemoveAll(d)
		}
		got, err := store.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("object lost after simulated crash: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("object corrupted after simulated crash: %q", got)
		}
	})

	t.Run("root-only sync loses the subtree (non-vacuous)", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "archive")
		target := filepath.Join(dir, "events", "demo", "stream", "obj.json")
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, payload, 0o440); err != nil {
			t.Fatal(err)
		}
		// Pre-fix behavior synced only the archive root.
		dropped := crashDrop([]string{dir}, chain(dir))
		if len(dropped) != 3 {
			t.Fatalf("root-only sync must drop the 3 chain members above root, got %v", dropped)
		}
		for _, d := range dropped {
			_ = os.RemoveAll(d)
		}
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Fatal("model must drop the object when only the root was synced")
		}
	})
}

// TestFileStorePutSyncFailureLeavesNoObject is AC-3: a chain-sync failure at
// the immediate parent, any intermediate ancestor, or the archive root must
// fail the Put and leave no object at the key.
func TestFileStorePutSyncFailureLeavesNoObject(t *testing.T) {
	key := "events/demo/stream/00000000000000000001-evt.json"
	payload := []byte(`{"event_id":"evt"}`)
	targets := []struct {
		name string
		path string
	}{
		{"immediate parent", filepath.Join("events", "demo", "stream")},
		{"intermediate ancestor", filepath.Join("events", "demo")},
		{"archive root", "."},
	}
	for _, tc := range targets {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "archive")
			rec := &syncRecorder{fail: map[string]bool{filepath.Join(dir, filepath.FromSlash(tc.path)): true}}
			store := &FileStore{Dir: dir, syncDir: rec.record}
			if err := store.Put(context.Background(), key, payload); err == nil {
				t.Fatal("Put must fail when a chain member cannot be synced")
			}
			target := filepath.Join(dir, filepath.FromSlash(key))
			if _, err := os.Stat(target); !os.IsNotExist(err) {
				t.Fatalf("object left at key after failed chain sync: %v", err)
			}
		})
	}
}

// TestFileStorePutIdempotentRetryPerformsNoDirectorySyncs pins REQ-3: a
// byte-identical retry takes the verify-existing path and performs zero new
// directory syncs (the seam makes this observable).
func TestFileStorePutIdempotentRetryPerformsNoDirectorySyncs(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	key := "events/demo/stream/00000000000000000001-evt.json"
	payload := []byte(`{"event_id":"evt"}`)
	rec := &syncRecorder{}
	store := &FileStore{Dir: dir, syncDir: rec.record}
	if err := store.Put(context.Background(), key, payload); err != nil {
		t.Fatal(err)
	}
	first := len(rec.syncs)
	if first == 0 {
		t.Fatal("first Put must sync the directory chain")
	}
	if err := store.Put(context.Background(), key, payload); err != nil {
		t.Fatal(err)
	}
	if got := len(rec.syncs); got != first {
		t.Fatalf("idempotent retry performed %d directory syncs, want 0 new", got-first)
	}
}

// TestFileStorePutKeyEscapingRootRejectedBeforeWrite is SEC-1: keys that
// would escape the archive root (.. traversal, absolute, empty) are rejected
// before any filesystem mutation, and Get is guarded symmetrically.
func TestFileStorePutKeyEscapingRootRejectedBeforeWrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	rec := &syncRecorder{}
	store := &FileStore{Dir: dir, syncDir: rec.record}
	for _, key := range []string{"../escape.json", "a/../../escape.json", "/abs/escape.json", ""} {
		if err := store.Put(context.Background(), key, []byte("boom")); err == nil {
			t.Fatalf("Put(%q) must be rejected before any write", key)
		}
		if len(rec.syncs) != 0 {
			t.Fatalf("Put(%q) must not reach the sync step", key)
		}
	}
	// Nothing may exist outside the archive root.
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "escape.json")); !os.IsNotExist(err) {
		t.Fatal("escaping key wrote an object outside the archive root")
	}
	// Get is guarded symmetrically.
	for _, key := range []string{"../escape.json", "a/../../escape.json", "/abs/escape.json"} {
		if _, err := store.Get(context.Background(), key); err == nil {
			t.Fatalf("Get(%q) must be rejected", key)
		}
	}
}

// TestFileStorePutConcurrentSameKeyNoLostArchive is SEC-2 pin: a failing Put
// must never remove an object that a concurrent byte-identical retry already
// verified as durable. The per-store mutex serializes create→sync→remove, so
// the interleaving is impossible; the hammer asserts the "nil ⇒ durable"
// invariant under -race.
func TestFileStorePutConcurrentSameKeyNoLostArchive(t *testing.T) {
	key := "events/demo/stream/00000000000000000001-evt.json"
	payload := []byte(`{"event_id":"evt"}`)
	for i := 0; i < 20; i++ {
		dir := filepath.Join(t.TempDir(), "archive")
		// Fail the sync of the archive root: the failing Put removes its
		// object only after the other chain members were synced, widening
		// any (now impossible) race window for a concurrent retry.
		rec := &syncRecorder{fail: map[string]bool{dir: true}}
		store := &FileStore{Dir: dir, syncDir: rec.record}
		var wg sync.WaitGroup
		results := make([]error, 2)
		for j := 0; j < 2; j++ {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				results[j] = store.Put(context.Background(), key, payload)
			}(j)
		}
		wg.Wait()
		data, err := store.Get(context.Background(), key)
		if err == nil {
			if !bytes.Equal(data, payload) {
				t.Fatalf("iteration %d: object corrupted: %q", i, data)
			}
			continue
		}
		for j, res := range results {
			if res == nil {
				t.Fatalf("iteration %d: caller %d got nil but the object is gone", i, j)
			}
		}
	}
}

// TestFileStoreGetDoesNotObservePartialObject is a W1 pin: with the mutex, a
// Get cannot observe a torn object while a Put is mid-write; a successful
// Get returns the complete payload.
func TestFileStoreGetDoesNotObservePartialObject(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	store := &FileStore{Dir: dir}
	key := "events/demo/stream/00000000000000000001-evt.json"
	payload := bytes.Repeat([]byte("x"), 512*1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = store.Put(context.Background(), key, payload)
	}()
	for attempts := 0; attempts < 500; attempts++ {
		data, err := store.Get(context.Background(), key)
		if err == nil {
			if !bytes.Equal(data, payload) {
				t.Fatalf("torn read: got %d bytes, want %d", len(data), len(payload))
			}
			<-done
			return
		}
	}
	<-done
	t.Fatal("Get never observed the completed object")
}
