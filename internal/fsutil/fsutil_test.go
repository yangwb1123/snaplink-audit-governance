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
