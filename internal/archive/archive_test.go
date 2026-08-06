package archive

import (
	"context"
	"os"
	"path/filepath"
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
