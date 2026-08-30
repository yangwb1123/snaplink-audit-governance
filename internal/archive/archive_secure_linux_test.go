//go:build linux

package archive

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
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
