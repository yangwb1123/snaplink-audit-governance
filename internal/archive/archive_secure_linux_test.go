//go:build linux

package archive

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestFileStoreRejectsSymlinkedRootAndIntermediate(t *testing.T) {
	key := "events/demo/object.json"
	payload := []byte("secret")

	t.Run("root", func(t *testing.T) {
		base := t.TempDir()
		outside := filepath.Join(base, "outside")
		if err := os.Mkdir(outside, 0o750); err != nil {
			t.Fatal(err)
		}
		root := filepath.Join(base, "archive")
		if err := os.Symlink(outside, root); err != nil {
			t.Skipf("cannot create symlink: %v", err)
		}
		store := &FileStore{Dir: root}
		if err := store.Put(context.Background(), key, payload); err == nil {
			t.Fatal("Put followed a symlinked root")
		}
		if _, err := store.Get(context.Background(), key); err == nil {
			t.Fatal("Get followed a symlinked root")
		}
		if _, err := os.Stat(filepath.Join(outside, "events")); !os.IsNotExist(err) {
			t.Fatalf("operation created data outside root: %v", err)
		}
	})

	t.Run("intermediate", func(t *testing.T) {
		base := t.TempDir()
		root := filepath.Join(base, "archive")
		outside := filepath.Join(base, "outside")
		if err := os.MkdirAll(outside, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(root, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "events")); err != nil {
			t.Skipf("cannot create symlink: %v", err)
		}
		store := &FileStore{Dir: root}
		if err := store.Put(context.Background(), key, payload); err == nil {
			t.Fatal("Put followed a symlinked intermediate")
		}
		if _, err := store.Get(context.Background(), key); err == nil {
			t.Fatal("Get followed a symlinked intermediate")
		}
		if _, err := os.Stat(filepath.Join(outside, "demo")); !os.IsNotExist(err) {
			t.Fatalf("operation created data outside root: %v", err)
		}
	})
}

// TestFileStoreRejectsNonDirectoryRootAndIntermediate pins R2/R3 for
// non-symlink path components. O_DIRECTORY is important here: a regular file
// at either position must not be accepted as an archive directory.
func TestFileStoreRejectsNonDirectoryRootAndIntermediate(t *testing.T) {
	key := "events/demo/object.json"
	payload := []byte("payload")

	t.Run("root regular file", func(t *testing.T) {
		base := t.TempDir()
		root := filepath.Join(base, "archive")
		if err := os.WriteFile(root, []byte("root sentinel"), 0o600); err != nil {
			t.Fatal(err)
		}
		store := &FileStore{Dir: root}
		if err := store.Put(context.Background(), key, payload); err == nil {
			t.Fatal("Put accepted a regular file as the configured root")
		}
		if data, err := store.Get(context.Background(), key); err == nil || data != nil {
			t.Fatalf("Get accepted a regular file root: data=%q err=%v", data, err)
		}
		data, err := os.ReadFile(root)
		if err != nil || string(data) != "root sentinel" {
			t.Fatalf("root sentinel changed: %v %q", err, data)
		}
	})

	t.Run("intermediate regular file", func(t *testing.T) {
		base := t.TempDir()
		root := filepath.Join(base, "archive")
		if err := os.MkdirAll(root, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "events"), []byte("intermediate sentinel"), 0o600); err != nil {
			t.Fatal(err)
		}
		store := &FileStore{Dir: root}
		if err := store.Put(context.Background(), key, payload); err == nil {
			t.Fatal("Put accepted a regular file as an intermediate directory")
		}
		if data, err := store.Get(context.Background(), key); err == nil || data != nil {
			t.Fatalf("Get accepted a regular intermediate: data=%q err=%v", data, err)
		}
		data, err := os.ReadFile(filepath.Join(root, "events"))
		if err != nil || string(data) != "intermediate sentinel" {
			t.Fatalf("intermediate sentinel changed: %v %q", err, data)
		}
	})

	t.Run("missing root is created beneath its parent", func(t *testing.T) {
		base := t.TempDir()
		root := filepath.Join(base, "missing", "archive")
		store := &FileStore{Dir: root}
		if err := store.Put(context.Background(), key, payload); err != nil {
			t.Fatal(err)
		}
		if info, err := os.Stat(root); err != nil || !info.IsDir() {
			t.Fatalf("created root = %v, want directory: %v", info, err)
		}
		data, err := store.Get(context.Background(), key)
		if err != nil || !bytes.Equal(data, payload) {
			t.Fatalf("created-root round trip data=%q err=%v", data, err)
		}
	})
}

// TestFileStorePublishedReplacementSurvivesSyncFailure exercises the only
// test-only cleanup path. A replacement installed after Linkat must survive a
// post-publication sync failure; production never removes a pathname here.
func TestFileStorePublishedReplacementSurvivesSyncFailure(t *testing.T) {
	root := filepath.Join(t.TempDir(), "archive")
	key := "events/object.json"
	parent := filepath.Join(root, "events")
	target := filepath.Join(parent, "object.json")
	original := target + ".original"
	replacement := []byte("replacement")
	sentinel := errors.New("injected post-publication sync failure")
	var hookErr error

	store := &FileStore{Dir: root}
	store.syncDir = func(string) error {
		if hookErr == nil {
			if err := os.Rename(target, original); err != nil {
				hookErr = err
			} else if err := os.WriteFile(target, replacement, 0o440); err != nil {
				hookErr = err
			}
		}
		if hookErr != nil {
			return hookErr
		}
		return sentinel
	}
	if err := store.Put(context.Background(), key, []byte("original")); !errors.Is(err, sentinel) {
		t.Fatalf("Put err=%v, want injected sync failure", err)
	}
	if hookErr != nil {
		t.Fatalf("replacement setup failed: %v", hookErr)
	}
	data, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(data, replacement) {
		t.Fatalf("replacement was removed or changed: %v %q", err, data)
	}
}

// exchangeArchivePaths atomically toggles a real directory and a dangling
// symlink. RENAME_EXCHANGE avoids an absent-path window in which a Put could
// legitimately create a fresh component during the stress test.
func exchangeArchivePaths(left, right string) error {
	return unix.Renameat2(unix.AT_FDCWD, left, unix.AT_FDCWD, right, unix.RENAME_EXCHANGE)
}

func runReplacementStress(t *testing.T, root, key, outside, component, parked string) {
	t.Helper()
	payload := []byte("inside archive")
	seed := &FileStore{Dir: root}
	if err := seed.Put(context.Background(), key, payload); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, parked); err != nil {
		t.Fatal(err)
	}

	const workers = 4
	const rounds = 80
	const exchanges = 120
	start := make(chan struct{})
	failures := make(chan error, 1)
	var wg sync.WaitGroup
	var successfulPuts atomic.Int32
	report := func(err error) {
		select {
		case failures <- err:
		default:
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < exchanges; i++ {
			if err := exchangeArchivePaths(component, parked); err != nil {
				report(err)
				return
			}
			if err := exchangeArchivePaths(component, parked); err != nil {
				report(err)
				return
			}
		}
	}()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store := &FileStore{Dir: root}
			<-start
			for j := 0; j < rounds; j++ {
				if err := store.Put(context.Background(), key, payload); err == nil {
					successfulPuts.Add(1)
				}
				data, err := store.Get(context.Background(), key)
				if err == nil && !bytes.Equal(data, payload) {
					report(errors.New("Get returned data different from the in-root payload"))
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	select {
	case err := <-failures:
		t.Fatalf("replacement stress failed: %v", err)
	default:
	}
	if successfulPuts.Load() == 0 {
		t.Fatal("replacement stress produced no successful Put")
	}
	if _, err := os.Lstat(outside); !os.IsNotExist(err) {
		t.Fatalf("operation created the external target %s: %v", outside, err)
	}
	data, err := (&FileStore{Dir: root}).Get(context.Background(), key)
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("final in-root object data=%q err=%v", data, err)
	}
}

// TestFileStoreConcurrentRootAndIntermediateReplacement is T5: independent
// FileStore instances run Put/Get while a real directory is atomically
// exchanged with a dangling symlink. Successful operations remain bound to
// the opened directory inode; no operation can create or read the target.
func TestFileStoreConcurrentRootAndIntermediateReplacement(t *testing.T) {
	t.Run("configured root", func(t *testing.T) {
		base := t.TempDir()
		root := filepath.Join(base, "archive")
		if err := os.Mkdir(root, 0o750); err != nil {
			t.Fatal(err)
		}
		runReplacementStress(t, root, "events/object.json", filepath.Join(base, "outside-root"), root, filepath.Join(base, "archive-parked"))
	})

	t.Run("intermediate component", func(t *testing.T) {
		base := t.TempDir()
		root := filepath.Join(base, "archive")
		component := filepath.Join(root, "events")
		if err := os.MkdirAll(component, 0o750); err != nil {
			t.Fatal(err)
		}
		runReplacementStress(t, root, "events/object.json", filepath.Join(base, "outside-intermediate"), component, filepath.Join(base, "events-parked"))
	})
}

func TestFileStoreSpecialFileReturnsPromptly(t *testing.T) {
	root := filepath.Join(t.TempDir(), "archive")
	parent := filepath.Join(root, "events", "demo")
	if err := os.MkdirAll(parent, 0o750); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(parent, "object.json")
	if err := syscall.Mkfifo(name, 0o600); err != nil {
		t.Skipf("cannot create FIFO: %v", err)
	}
	store := &FileStore{Dir: root}
	calls := []struct {
		name string
		fn   func() error
	}{
		{"Get", func() error { _, err := store.Get(context.Background(), "events/demo/object.json"); return err }},
		{"Put", func() error { return store.Put(context.Background(), "events/demo/object.json", []byte("x")) }},
	}
	for _, call := range calls {
		t.Run(call.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- call.fn() }()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("operation accepted a FIFO")
				}
			case <-time.After(time.Second):
				t.Fatal("operation blocked on a FIFO")
			}
		})
	}
}
