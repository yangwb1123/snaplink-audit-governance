// Package fsutil holds the shared durability helpers for file-backed
// persistence: fsync of a written file and of the directory that contains a
// renamed file. A rename is not durable until the parent directory entry is
// synced; without these, a crash right after rename can lose the last
// committed state (snapshot, replay marks, archive objects).
package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// SyncFile flushes file contents to stable storage. The caller must have
// written and closed (or be about to close) the file; Sync returns an error
// when the underlying device reports a failure, so a silently lost write is
// surfaced instead of being reported as committed.
func SyncFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s for sync: %w", path, err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", path, err)
	}
	return nil
}

// SyncDir flushes the directory entry of path so a completed rename survives
// a crash. Opening a directory read-only and syncing it is the standard
// pattern; it is a no-op on filesystems without directory fsync support.
func SyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory %s for sync: %w", path, err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync directory %s: %w", path, err)
	}
	return nil
}

// SyncDirChain flushes, leaf-to-root, every directory on the path from dir up
// to and including root. A file created at a nested key is only fully durable
// when each ancestor directory's entry for its child has been synced: syncing
// just the archive root (or just the immediate parent) leaves a crash window
// in which a freshly created intermediate directory — and everything under it
// — can vanish after a power failure. dir must be root or a descendant of
// root; syncDir is the per-directory fsync operation and defaults to SyncDir
// when nil. The first failure aborts the walk and is returned wrapped, so
// callers can distinguish a chain-sync failure from a pre-existing one via
// errors.Is.
func SyncDirChain(root, dir string, syncDir func(string) error) error {
	if syncDir == nil {
		syncDir = SyncDir
	}
	root = filepath.Clean(root)
	var chain []string
	for d := filepath.Clean(dir); ; d = filepath.Dir(d) {
		chain = append(chain, d)
		if d == root {
			break
		}
		if filepath.Dir(d) == d {
			// Walked past the root without finding it: the caller's path
			// is not under the configured root (defensive; cannot happen
			// for paths built with filepath.Join(root, key)).
			return fmt.Errorf("sync chain: %s is not under root %s", dir, root)
		}
	}
	for i := 0; i < len(chain); i++ {
		if err := syncDir(chain[i]); err != nil {
			return fmt.Errorf("sync directory chain %s: %w", chain[i], err)
		}
	}
	return nil
}

// DeepestExistingAncestor returns the deepest directory on the path to dir
// that already exists (walking upward with os.Stat; the filesystem root
// always exists and terminates the walk). Only these directories have
// durable dentries already; every directory between the returned root and
// dir is created by the caller's MkdirAll and must itself be synced after
// creation. A Stat error other than "confirmed present" is treated as
// not-existing: over-syncing a pre-existing ancestor is harmless (one extra
// fsync), while under-syncing a freshly created intermediate would reopen
// the M-12 window — the probe can therefore never compromise durability.
func DeepestExistingAncestor(dir string) string {
	for d := filepath.Clean(dir); ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil {
			return d
		}
		parent := filepath.Dir(d)
		if parent == d {
			return d // filesystem root always exists
		}
	}
}
