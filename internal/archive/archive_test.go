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

func TestFileStorePutMissingData(t *testing.T) {
	store := &FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
	if err := store.Put(context.Background(), "a/b/c.json", []byte("x")); err != nil {
		t.Fatalf("nested Put: %v", err)
	}
}
