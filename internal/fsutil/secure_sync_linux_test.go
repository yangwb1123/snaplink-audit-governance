//go:build linux

package fsutil

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDataDirSyncChainSyncsPinnedParentExactlyOnce(t *testing.T) {
	root := t.TempDir()
	dir, err := OpenDataDir(root, false)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	calls := 0
	if err := syncDataDirChain(root, root, func() error {
		calls++
		return dir.Sync() // count the real directory fsync, not an elapsed-time proxy
	}, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("pinned parent fsyncs=%d, want exactly one", calls)
	}
}

func TestDataDirSyncChainKeepsFullOrderAndStopsOnFailure(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "new", "deep")
	dir, err := OpenDataDir(parent, true)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	want := []string{"pinned", parent, filepath.Dir(parent), root}
	for failAt := -1; failAt < len(want); failAt++ {
		var seen []string
		failure := errors.New("directory sync failure")
		record := func(path string) error {
			seen = append(seen, path)
			if len(seen)-1 == failAt {
				return failure
			}
			return nil
		}
		err := syncDataDirChain(root, parent, func() error {
			if err := record("pinned"); err != nil {
				return err
			}
			return dir.Sync()
		}, func(path string) error {
			if err := record(path); err != nil {
				return err
			}
			return SyncDir(path)
		})
		expected := want
		if failAt >= 0 {
			expected = want[:failAt+1]
			if !errors.Is(err, failure) {
				t.Fatalf("failed directory sync was hidden at boundary %d", failAt)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(seen, expected) {
			t.Fatalf("sync order=%v, want %v", seen, expected)
		}
	}
}

func TestDataDirSyncChainActualClosedDescriptorFailsBeforeHook(t *testing.T) {
	root := t.TempDir()
	dir, err := OpenDataDir(root, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := dir.Close(); err != nil {
		t.Fatal(err)
	}
	called := false
	err = dir.SyncChain(root, func(string) error { called = true; return nil })
	if err == nil || called {
		t.Fatal("hook bypassed a failed real pinned-directory fsync")
	}
}

func TestAtomicDataFileDirectoryFailureRetainsPublishedUnknownResult(t *testing.T) {
	for _, boundary := range []int{0, 1, 2} {
		root := t.TempDir()
		path := filepath.Join(root, "new", "deep", "state")
		calls := 0
		failure := errors.New("ancestor sync failure")
		err := AtomicDataFile(path, []byte("complete-original"), PrivateFileMode, func(path string) error {
			current := calls
			calls++
			if current == boundary {
				return failure
			}
			return SyncDir(path)
		})
		if !errors.Is(err, ErrDataDirectorySync) || !errors.Is(err, failure) || calls != boundary+1 {
			t.Fatal("post-rename failure lost its unknown-result classification or continued syncing")
		}
		raw, err := ReadDataFile(path)
		if err != nil || string(raw) != "complete-original" {
			t.Fatal("post-rename failure erased or corrupted the complete published inode")
		}
		if _, err := os.Lstat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("successful rename left a temporary sibling")
		}
		// An explicit later write must establish its own full durability; no
		// cache may infer that the failed prior chain was durable.
		if err := AtomicDataFile(path, raw, PrivateFileMode, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDataDirSyncChainDefaultRejectsReplacedParent(t *testing.T) {
	for _, kind := range []string{"symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			parent := filepath.Join(root, "parent")
			dir, err := OpenDataDir(parent, true)
			if err != nil {
				t.Fatal(err)
			}
			defer dir.Close()
			if err := os.Rename(parent, filepath.Join(root, "moved")); err != nil {
				t.Fatal(err)
			}
			if kind == "symlink" {
				err = os.Symlink(t.TempDir(), parent)
			} else {
				err = os.Mkdir(parent, 0o700)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := dir.SyncChain(root, nil); !errors.Is(err, ErrUnsafeFile) {
				t.Fatal("replaced parent path bypassed the safe post-rename boundary")
			}
		})
	}
}

func TestDataDirSyncChainKeepsActualParentAfterReplacement(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "parent")
	dir, err := OpenDataDir(parent, true)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	moved := filepath.Join(root, "moved")
	if err := os.Rename(parent, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), parent); err != nil {
		t.Fatal(err)
	}
	var seen []string
	if err := dir.SyncChain(root, func(path string) error {
		seen = append(seen, path)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, []string{parent, root}) {
		t.Fatal("parent replacement changed the captured chain labels")
	}
}
