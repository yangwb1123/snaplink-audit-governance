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
	if err := store.Put(context.Background(), "events/demo/stream/00000000000000000001-evt.json", []byte(`{"event_id":"evt"}`)); err != nil {
		t.Fatal(err)
	}
	// 幂等：重复 Put 返回 nil 且不覆盖。
	if err := store.Put(context.Background(), "events/demo/stream/00000000000000000001-evt.json", []byte(`tampered`)); err != nil {
		t.Fatal(err)
	}
	data, err := store.Get(context.Background(), "events/demo/stream/00000000000000000001-evt.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"event_id":"evt"}` {
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
