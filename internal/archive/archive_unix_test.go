//go:build unix

package archive

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
)

// TestFileStorePutRemovesPartialObjectOnWriteFailure is AC-3: a write-path
// fault after the object was created (EFBIG from RLIMIT_FSIZE) must remove
// the partial object, so the invariant "error ⇒ no object at key" holds.
func TestFileStorePutRemovesPartialObjectOnWriteFailure(t *testing.T) {
	// SIGXFSZ would terminate the process by default on some platforms;
	// ignore it so the oversized write surfaces as EFBIG instead.
	signal.Ignore(syscall.SIGXFSZ)
	defer signal.Reset(syscall.SIGXFSZ)

	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Skipf("Getrlimit: %v", err)
	}
	oldCur := limit.Cur
	limit.Cur = 4 // smaller than the payload below
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Skipf("Setrlimit: %v", err)
	}
	defer func() {
		limit.Cur = oldCur
		_ = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit)
	}()

	dir := filepath.Join(t.TempDir(), "archive")
	store := &FileStore{Dir: dir}
	key := "events/demo/stream/00000000000000000001-evt.json"
	if err := store.Put(context.Background(), key, []byte(`{"event_id":"evt","payload":"exceeds the file size limit"}`)); err == nil {
		t.Fatal("Put must fail when the write exceeds RLIMIT_FSIZE")
	}
	target := filepath.Join(dir, filepath.FromSlash(key))
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("partial object left at key after failed Put: %v", err)
	}
}
