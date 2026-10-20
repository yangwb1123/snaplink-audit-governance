//go:build linux

package fsutil

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDataTempUniquePrivateAndPinnedToCapturedParent(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	path, moved := filepath.Join(root, "data"), filepath.Join(root, "moved")
	dir, err := OpenDataDir(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if err := os.WriteFile(filepath.Join(path, "state-old.tmp"), []byte("unknown"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		file, err := dir.CreateTemp("state-*.tmp")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(file.Name())
		if seen[name] || !strings.HasPrefix(name, "state-") || !strings.HasSuffix(name, ".tmp") {
			t.Fatalf("non-unique/unexpected temp %q", name)
		}
		seen[name] = true
		info, err := file.Stat()
		if err != nil || info.Mode().Perm() != 0o600 || info.Size() != 0 {
			t.Fatalf("unsafe temp inode: %v %v", info, err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(moved, name)); err != nil {
			t.Fatal("creation escaped the captured parent")
		}
		if err := dir.Remove(name); err != nil {
			t.Fatal(err)
		}
	}
	for _, pattern := range []string{"../*.tmp", "/absolute", "", "..", "nested/file", "nul\x00name"} {
		if file, err := dir.CreateTemp(pattern); !errors.Is(err, ErrUnsafeFile) {
			if file != nil {
				_ = file.Close()
			}
			t.Fatalf("unsafe temp pattern accepted: %q %v", pattern, err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(moved, "state-old.tmp"))
	if err != nil || string(raw) != "unknown" {
		t.Fatalf("unknown temp changed: %q %v", raw, err)
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("wrote outside captured directory: %v %v", entries, err)
	}
}
