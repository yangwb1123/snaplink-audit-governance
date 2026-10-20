//go:build linux

package fsutil

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestDataFileRejectsSymlinksHardLinksAndSpecialFilesBeforeMutation(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "fifo", "directory"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			victim, path := filepath.Join(root, "victim"), filepath.Join(root, "target")
			if err := os.WriteFile(victim, []byte("unchanged"), 0o600); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "symlink":
				err = os.Symlink(victim, path)
			case "hardlink":
				err = os.Link(victim, path)
			case "fifo":
				err = unix.Mkfifo(path, 0o600)
			case "directory":
				err = os.Mkdir(path, 0o700)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, flags := range []int{os.O_RDONLY, os.O_WRONLY | os.O_TRUNC, os.O_RDWR | os.O_APPEND} {
				result := make(chan error, 1)
				go func() {
					file, err := OpenDataFile(path, flags, 0)
					if file != nil {
						err = errors.Join(err, file.Close())
					}
					result <- err
				}()
				select {
				case err := <-result:
					if !errors.Is(err, ErrUnsafeFile) {
						t.Fatalf("kind=%s flags=%d err=%v", kind, flags, err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("special file blocked before inode validation")
				}
			}
			raw, err := os.ReadFile(victim)
			if err != nil || string(raw) != "unchanged" {
				t.Fatalf("unsafe open changed another inode: %q %v", raw, err)
			}
		})
	}
}

func TestDataDirectoryRejectsEverySymlinkAncestor(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	for _, relative := range []string{"root-link/file", "nested/link/deeper/file"} {
		path := filepath.Join(root, relative)
		link := filepath.Join(root, "root-link")
		if relative != "root-link/file" {
			if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
				t.Fatal(err)
			}
			link = filepath.Join(root, "nested", "link")
		}
		if err := os.Symlink(outside, link); err != nil {
			t.Fatal(err)
		}
		if err := AtomicDataFile(path, []byte("private"), PrivateFileMode, nil); !errors.Is(err, ErrUnsafeFile) {
			t.Fatalf("symlinked ancestor accepted: %v", err)
		}
		if _, err := ReadDataFile(path); !errors.Is(err, ErrUnsafeFile) {
			t.Fatalf("symlinked read accepted: %v", err)
		}
		if err := SyncDir(filepath.Dir(path)); !errors.Is(err, ErrUnsafeFile) {
			t.Fatalf("symlinked sync accepted: %v", err)
		}
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("created data outside pinned directory: %v %v", entries, err)
	}
}

func TestAtomicDataFilePreservesUnownedTempAndRejectsUnsafeTarget(t *testing.T) {
	for _, kind := range []string{"regular", "symlink", "hardlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			path, victim := filepath.Join(root, "state"), filepath.Join(root, "victim")
			if err := AtomicDataFile(path, []byte("previous"), PrivateFileMode, nil); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(victim, []byte("unknown-temp-evidence"), 0o600); err != nil {
				t.Fatal(err)
			}
			temp := path + ".tmp"
			var err error
			switch kind {
			case "regular":
				err = os.WriteFile(temp, []byte("unknown-temp-evidence"), 0o600)
			case "symlink":
				err = os.Symlink(victim, temp)
			case "hardlink":
				err = os.Link(victim, temp)
			case "directory":
				err = os.Mkdir(temp, 0o700)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := AtomicDataFile(path, []byte("new"), PrivateFileMode, nil); err == nil {
				t.Fatal("existing temp accepted")
			}
			if _, err := os.Lstat(temp); err != nil {
				t.Fatal("deleted an unowned temp")
			}
			for target, expected := range map[string]string{path: "previous", victim: "unknown-temp-evidence"} {
				raw, err := os.ReadFile(target)
				if err != nil || string(raw) != expected {
					t.Fatalf("modified %s: %q %v", target, raw, err)
				}
			}
		})
	}
	root := t.TempDir()
	victim, target := filepath.Join(root, "victim"), filepath.Join(root, "target")
	if err := os.WriteFile(victim, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, target); err != nil {
		t.Fatal(err)
	}
	if err := AtomicDataFile(target, []byte("replacement"), PrivateFileMode, nil); !errors.Is(err, ErrUnsafeFile) {
		t.Fatalf("unsafe existing target accepted: %v", err)
	}
}

func TestDataDirectoryDescriptorSurvivesParentSymlinkReplacement(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	path, moved := filepath.Join(root, "data"), filepath.Join(root, "moved")
	dir, err := OpenDataDir(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	// A deterministic real replacement AFTER descriptor capture, not a
	// sleep/race assumption. All operations must remain on the original inode.
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	file, err := dir.OpenFile("owned.tmp", os.O_CREATE|os.O_EXCL|os.O_WRONLY, PrivateFileMode)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("inside"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := dir.Rename("owned.tmp", "committed"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(moved, "committed"))
	if err != nil || string(raw) != "inside" {
		t.Fatalf("descriptor lost its inode: %q %v", raw, err)
	}
	if err := dir.Remove("committed"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../victim", "/absolute", "", ".", ".."} {
		if err := dir.Remove(name); !errors.Is(err, ErrUnsafeFile) {
			t.Fatalf("unsafe cleanup name accepted: %q %v", name, err)
		}
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("replacement redirected descriptor operations: %v %v", entries, err)
	}
}

func TestAtomicDataFilePrivateModesAndFullSyncChain(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "new", "deep", "state")
	var synced []string
	if err := AtomicDataFile(path, []byte("first"), PrivateFileMode, func(path string) error {
		synced = append(synced, path)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Dir(path), filepath.Join(root, "new"), root}
	if !reflect.DeepEqual(synced, want) {
		t.Fatalf("sync order=%v want=%v", synced, want)
	}
	for path, mode := range map[string]os.FileMode{path: 0o600, filepath.Dir(path): 0o700} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatalf("private creation mode %s: %v %v", path, info, err)
		}
	}
	// Legacy group-readable inodes are not silently chmod'd while reading.
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadDataFile(path); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0o640 {
		t.Fatal("read changed legacy permissions")
	}
	if err := AtomicDataFile(path, []byte("second"), PrivateFileMode, nil); err != nil {
		t.Fatal(err)
	}
	info, _ = os.Stat(path)
	if info.Mode().Perm() != 0o600 {
		t.Fatal("new replacement did not get owner-only permissions")
	}
}
