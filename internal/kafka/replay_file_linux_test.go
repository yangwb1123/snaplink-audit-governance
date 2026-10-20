//go:build linux

package kafka

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/fsutil"
	"golang.org/x/sys/unix"
)

func TestReplayFileRejectsUnsafeStateAndLockWithoutMarking(t *testing.T) {
	for _, target := range []string{"state", "lock"} {
		for _, kind := range []string{"symlink", "hardlink", "fifo", "directory"} {
			t.Run(target+"/"+kind, func(t *testing.T) {
				root := t.TempDir()
				path := filepath.Join(root, "state")
				seed := []byte(replayLogHeader + "{\"event_id\":\"seed\"}\n")
				if err := os.WriteFile(path, seed, 0o600); err != nil {
					t.Fatal(err)
				}
				state, err := LoadReplayState(path)
				if err != nil {
					t.Fatal(err)
				}
				victim := filepath.Join(t.TempDir(), "outside")
				if err := os.WriteFile(victim, seed, 0o600); err != nil {
					t.Fatal(err)
				}
				unsafe := path + ".lock"
				if target == "state" {
					unsafe = path
					if err := os.Rename(path, path+"-preserved"); err != nil {
						t.Fatal(err)
					}
				}
				switch kind {
				case "symlink":
					err = os.Symlink(victim, unsafe)
				case "hardlink":
					err = os.Link(victim, unsafe)
				case "fifo":
					err = unix.Mkfifo(unsafe, 0o600)
				case "directory":
					err = os.Mkdir(unsafe, 0o700)
				}
				if err != nil {
					t.Fatal(err)
				}
				result := make(chan error, 1)
				go func() {
					if target == "state" {
						if _, err := LoadReplayState(path); !errors.Is(err, fsutil.ErrUnsafeFile) {
							result <- errors.New("unsafe loader returned marks")
							return
						}
						if _, err := loadLegacyMarks(path); !errors.Is(err, fsutil.ErrUnsafeFile) {
							result <- errors.New("unsafe legacy loader accepted file")
							return
						}
					}
					for _, mark := range []func() error{
						func() error { return state.Mark("new") },
						func() error { return state.MarkTenant("tenant-a", "new") },
					} {
						if err := mark(); !errors.Is(err, fsutil.ErrUnsafeFile) {
							result <- errors.New("unsafe mark did not fail closed")
							return
						}
					}
					result <- nil
				}()
				select {
				case err := <-result:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("special replay file blocked at open/flock boundary")
				}
				if state.marked("new") || state.Marked("tenant-a", "new") {
					t.Fatal("unsafe persist advanced memory")
				}
				raw, err := os.ReadFile(victim)
				if err != nil || string(raw) != string(seed) {
					t.Fatalf("unsafe persist modified another inode: %q %v", raw, err)
				}
			})
		}
	}
}

func TestReplayFileRejectsSymlinkAncestorBeforeLoadOrPersist(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	link := filepath.Join(root, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(link, "state")
	if _, err := LoadReplayState(path); !errors.Is(err, fsutil.ErrUnsafeFile) {
		t.Fatalf("unsafe missing-file load became empty success: %v", err)
	}
	state := &ReplayState{path: path}
	if err := state.MarkTenant("tenant-a", "new"); !errors.Is(err, fsutil.ErrUnsafeFile) {
		t.Fatalf("unsafe ancestor allowed persist: %v", err)
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("created state/lock outside scope: %v %v", entries, err)
	}
}

func TestReplayRewriteParentReplacementPinsRenameAndCleanup(t *testing.T) {
	for _, failBeforeRename := range []bool{false, true} {
		root, outside := t.TempDir(), t.TempDir()
		dir, moved := filepath.Join(root, "data"), filepath.Join(root, "moved")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "state")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		var tempName string
		state.newTemp = func(dir, pattern string) (*os.File, error) {
			file, err := os.CreateTemp(dir, pattern)
			if err != nil {
				return nil, err
			}
			tempName = filepath.Base(file.Name())
			if err := os.Rename(dir, moved); err != nil {
				return nil, closeReplayFile(file, err)
			}
			if err := os.Symlink(outside, dir); err != nil {
				return nil, closeReplayFile(file, err)
			}
			if err := os.WriteFile(filepath.Join(outside, tempName), []byte("foreign-temp"), 0o600); err != nil {
				return nil, closeReplayFile(file, err)
			}
			return file, nil
		}
		primary := errors.New("injected file sync failure after actual parent replacement")
		state.syncFile = func(string) error {
			if failBeforeRename {
				return primary
			}
			return nil
		}
		err = state.MarkTenant("tenant-a", "new")
		if (failBeforeRename && !errors.Is(err, primary)) || (!failBeforeRename && !errors.Is(err, fsutil.ErrUnsafeFile)) {
			t.Fatalf("replacement did not return expected failure: %v", err)
		}
		if state.Marked("tenant-a", "new") {
			t.Fatal("unknown result advanced in-memory mark")
		}
		raw, err := os.ReadFile(filepath.Join(outside, tempName))
		if err != nil || string(raw) != "foreign-temp" {
			t.Fatalf("cleanup deleted foreign temp: %q %v", raw, err)
		}
		if _, err := os.Stat(filepath.Join(outside, "state")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("rename escaped pinned directory")
		}
		if _, err := os.Stat(filepath.Join(moved, tempName)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("owned temp not cleaned/renamed in captured directory")
		}
		if !failBeforeRename {
			loaded, err := LoadReplayState(filepath.Join(moved, "state"))
			if err != nil || !loaded.Marked("tenant-a", "new") {
				t.Fatalf("post-rename failure pretended disk rollback: %v", err)
			}
		}
	}
}

func TestReplayRejectsTargetSymlinkAddedAtContentSyncBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	state, err := LoadReplayState(path)
	if err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(victim, []byte("do-not-overwrite"), 0o600); err != nil {
		t.Fatal(err)
	}
	state.syncFile = func(string) error { return os.Symlink(victim, path) }
	if err := state.Mark("new"); !errors.Is(err, fsutil.ErrUnsafeFile) || state.marked("new") {
		t.Fatalf("unsafe new target accepted: %v", err)
	}
	raw, err := os.ReadFile(victim)
	if err != nil || string(raw) != "do-not-overwrite" {
		t.Fatalf("sync boundary changed foreign content: %q %v", raw, err)
	}
}

func TestReplayNewStateAndLockPrivateWithoutLegacyChmod(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	state, err := LoadReplayState(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.MarkTenant("tenant-a", "seed"); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{path, path + ".lock"} {
		info, err := os.Stat(file)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("new replay file not private: %v %v", info, err)
		}
		if err := os.Chmod(file, 0o640); err != nil {
			t.Fatal(err)
		}
	}
	if err := state.MarkTenant("tenant-b", "seed"); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{path, path + ".lock"} {
		info, _ := os.Stat(file)
		if info.Mode().Perm() != 0o640 {
			t.Fatal("legacy append/flock silently chmod'd")
		}
	}
	// Existing lock fault seam still returns a real flock with close-only
	// ownership, not a directory wrapper that would weaken early release.
	lock, err := lockReplayState(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := lock.(*os.File); !ok {
		t.Fatal("lock no longer owns the actual flock descriptor")
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}
