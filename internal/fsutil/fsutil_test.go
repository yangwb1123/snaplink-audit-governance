package fsutil

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestSyncDirChainLeafToRoot pins the chain order: leaf first, archive root
// last, every intermediate directory in between. A single directory (root ==
// dir) collapses to exactly one sync, preserving the historical SyncDir
// behavior for flat keys.
func TestSyncDirChainLeafToRoot(t *testing.T) {
	cases := []struct {
		name string
		root string
		dir  string
		want []string
	}{
		{
			name: "nested chain",
			root: "/archive",
			dir:  "/archive/events/demo/stream",
			want: []string{"/archive/events/demo/stream", "/archive/events/demo", "/archive/events", "/archive"},
		},
		{
			name: "flat key parent is direct child",
			root: "/archive",
			dir:  "/archive/exports",
			want: []string{"/archive/exports", "/archive"},
		},
		{
			name: "root equals dir",
			root: "/archive",
			dir:  "/archive",
			want: []string{"/archive"},
		},
		{
			name: "trailing slashes are normalized",
			root: "/archive/",
			dir:  "/archive/events/",
			want: []string{"/archive/events", "/archive"},
		},
		{
			name: "relative root and dir",
			root: "archive",
			dir:  "archive/events/demo",
			want: []string{"archive/events/demo", "archive/events", "archive"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			err := SyncDirChain(tc.root, tc.dir, func(path string) error {
				got = append(got, path)
				return nil
			})
			if err != nil {
				t.Fatalf("SyncDirChain: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("sync order = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSyncDirChainFailurePropagates pins abort-on-first-failure: the walk
// stops at the failing member (leaf-to-root, so members below the failure
// are synced first) and the injected error is reachable via errors.Is.
func TestSyncDirChainFailurePropagates(t *testing.T) {
	sentinel := errors.New("injected sync failure")
	called := 0
	err := SyncDirChain("/a", "/a/b/c", func(path string) error {
		called++
		if path == "/a/b" {
			return sentinel
		}
		return nil
	})
	if err == nil {
		t.Fatal("SyncDirChain must propagate the sync error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error %v must wrap the injected sentinel", err)
	}
	if called != 2 {
		t.Fatalf("walk aborted after %d syncs, want 2 (leaf then failing parent)", called)
	}
}

// TestSyncDirChainRootNotAncestor pins the defensive guard: a dir outside
// the root is a configuration error, not a silent no-op.
func TestSyncDirChainRootNotAncestor(t *testing.T) {
	called := 0
	err := SyncDirChain("/a", "/b/c", func(string) error {
		called++
		return nil
	})
	if err == nil {
		t.Fatal("SyncDirChain must reject a dir outside the root")
	}
	if called != 0 {
		t.Fatalf("no sync must run for an invalid chain, got %d", called)
	}
}

// TestDeepestExistingAncestor pins the shared probe (REQ-1): it returns the
// deepest directory on the path that already exists, walking upward from the
// requested dir, so callers can extend a post-MkdirAll sync chain to cover
// every directory they themselves created. The filesystem root always
// exists and terminates the walk.
func TestDeepestExistingAncestor(t *testing.T) {
	t.Run("existing dir returns itself", func(t *testing.T) {
		dir := t.TempDir()
		if got := DeepestExistingAncestor(dir); got != dir {
			t.Fatalf("DeepestExistingAncestor(%q) = %q, want %q", dir, got, dir)
		}
	})

	t.Run("missing nested path returns deepest existing ancestor", func(t *testing.T) {
		base := t.TempDir()
		got := DeepestExistingAncestor(filepath.Join(base, "missing", "deeper"))
		if got != base {
			t.Fatalf("DeepestExistingAncestor = %q, want %q", got, base)
		}
	})

	t.Run("multi-level missing returns existing ancestor", func(t *testing.T) {
		base := t.TempDir()
		got := DeepestExistingAncestor(filepath.Join(base, "a", "b", "c"))
		if got != base {
			t.Fatalf("DeepestExistingAncestor = %q, want %q", got, base)
		}
	})

	t.Run("bare missing name under existing dir", func(t *testing.T) {
		base := t.TempDir()
		got := DeepestExistingAncestor(filepath.Join(base, "missing"))
		if got != base {
			t.Fatalf("DeepestExistingAncestor = %q, want %q", got, base)
		}
	})

	t.Run("relative path walk terminates at cwd", func(t *testing.T) {
		base := t.TempDir()
		t.Chdir(base)
		got := DeepestExistingAncestor(filepath.Join("missing", "deeper"))
		if got != "." {
			t.Fatalf("DeepestExistingAncestor = %q, want %q", got, ".")
		}
	})
}

// TestSyncDirChainNilSyncDir covers the production default: a nil hook falls
// back to SyncDir and a real directory chain (including a freshly created
// intermediate) syncs without error.
func TestSyncDirChainNilSyncDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), "archive")
	if err := os.MkdirAll(filepath.Join(root, "events", "demo"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := SyncDirChain(root, filepath.Join(root, "events", "demo"), nil); err != nil {
		t.Fatalf("SyncDirChain with nil hook: %v", err)
	}
}
